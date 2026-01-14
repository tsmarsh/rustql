# Translate fts3.c and fts3_write.c - FTS3 Core

## Overview
Translate FTS3 (older full-text search) core implementation. FTS3 is simpler than FTS5 but still widely used.

## Source Reference
- `sqlite3/ext/fts3/fts3.c` - 6,206 lines
- `sqlite3/ext/fts3/fts3_write.c` - 5,834 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### FTS3 Table
```rust
/// FTS3 virtual table
pub struct Fts3Table {
    /// Database connection
    db: *mut Connection,
    /// Table name
    name: String,
    /// Schema name
    schema: String,
    /// Column names
    columns: Vec<String>,
    /// Tokenizer
    tokenizer: Box<dyn Fts3Tokenizer>,
    /// Has languageid column
    has_langid: bool,
    /// Has content= option
    has_content: bool,
    /// Content table name
    content_table: Option<String>,
    /// Prefix indexes
    prefixes: Vec<i32>,
}

/// FTS3 cursor
pub struct Fts3Cursor {
    /// Base cursor
    base: VtabCursor,
    /// Current expression
    expr: Option<Fts3Expr>,
    /// Doc list iterator
    doclist: Option<DoclistIter>,
    /// Current rowid
    rowid: i64,
    /// Is at EOF
    eof: bool,
}
```

### FTS3 Index Structure
```rust
/// FTS3 uses a simpler single-segment index
pub struct Fts3Index {
    /// Segment table
    segments: String,
    /// Segdir table
    segdir: String,
    /// Stat table
    stat: String,
}

/// Segment directory entry
pub struct Fts3Segdir {
    /// Level
    level: i32,
    /// Index within level
    idx: i32,
    /// Start block
    start_block: i64,
    /// Leaves end block
    leaves_end_block: i64,
    /// End block
    end_block: i64,
    /// Root page data
    root: Vec<u8>,
}
```

### Doclist Format
```rust
/// FTS3 doclist (different format from FTS5)
pub struct Fts3Doclist {
    /// Encoded data
    data: Vec<u8>,
}

impl Fts3Doclist {
    pub fn encode(entries: &[DoclistEntry]) -> Self {
        let mut data = Vec::new();
        let mut prev_rowid = 0i64;

        for entry in entries {
            // Delta-encode rowid
            let delta = entry.rowid - prev_rowid;
            sqlite3_put_varint(&mut data, delta as u64);
            prev_rowid = entry.rowid;

            // Encode positions
            let mut prev_pos = 0;
            for pos in &entry.positions {
                let delta = pos.offset - prev_pos + 2; // +2 to handle column markers
                sqlite3_put_varint(&mut data, delta as u64);
                prev_pos = pos.offset;
            }

            // End of position list marker
            data.push(0);
        }

        Self { data }
    }

    pub fn iter(&self) -> DoclistIter {
        DoclistIter::new(&self.data)
    }
}

pub struct DoclistIter<'a> {
    data: &'a [u8],
    pos: usize,
    rowid: i64,
}

impl<'a> DoclistIter<'a> {
    pub fn next(&mut self) -> Option<(i64, Vec<i32>)> {
        if self.pos >= self.data.len() {
            return None;
        }

        // Read rowid delta
        let (delta, n) = sqlite3_get_varint(&self.data[self.pos..]);
        self.pos += n;
        self.rowid += delta as i64;

        // Read positions
        let mut positions = Vec::new();
        let mut prev_pos = 0;

        loop {
            if self.pos >= self.data.len() {
                break;
            }

            let (val, n) = sqlite3_get_varint(&self.data[self.pos..]);
            self.pos += n;

            if val == 0 {
                break; // End of position list
            }

            let pos = prev_pos + (val as i32) - 2;
            positions.push(pos);
            prev_pos = pos;
        }

        Some((self.rowid, positions))
    }
}
```

## FTS3 Write Operations

```rust
impl Fts3Table {
    /// Insert a document
    pub fn insert(&mut self, rowid: i64, values: &[&str]) -> Result<()> {
        let mut pending = PendingTerms::new();

        for (col_idx, value) in values.iter().enumerate() {
            let tokens = self.tokenize(value)?;

            for (pos, token) in tokens.into_iter().enumerate() {
                pending.add(&token, rowid, col_idx as i32, pos as i32);
            }
        }

        // Write pending to index
        self.flush_pending(pending)?;

        Ok(())
    }

    /// Delete a document
    pub fn delete(&mut self, rowid: i64) -> Result<()> {
        // Read content
        let content = self.read_content(rowid)?;

        // Build delete terms
        let mut pending = PendingTerms::new();

        for (col_idx, value) in content.iter().enumerate() {
            let tokens = self.tokenize(value)?;

            for (pos, token) in tokens.into_iter().enumerate() {
                pending.add_delete(&token, rowid);
            }
        }

        // Apply delete markers
        self.flush_pending(pending)?;

        Ok(())
    }

    fn flush_pending(&mut self, pending: PendingTerms) -> Result<()> {
        // Sort terms
        let mut terms: Vec<_> = pending.terms.into_iter().collect();
        terms.sort_by(|a, b| a.0.cmp(&b.0));

        // Build leaf nodes
        let mut leaves = Vec::new();
        let mut current_leaf = LeafNode::new();

        for (term, doclist) in terms {
            if current_leaf.size() + term.len() + doclist.len() > LEAF_SIZE {
                leaves.push(current_leaf);
                current_leaf = LeafNode::new();
            }
            current_leaf.add_term(&term, &doclist);
        }

        if !current_leaf.is_empty() {
            leaves.push(current_leaf);
        }

        // Write leaves to segment table
        let segment_id = self.allocate_segment()?;

        for (i, leaf) in leaves.iter().enumerate() {
            self.write_leaf(segment_id, i as i64, leaf.data())?;
        }

        // Update segment directory
        self.write_segdir(segment_id, 0, leaves.len())?;

        // Maybe merge segments
        self.maybe_merge()?;

        Ok(())
    }
}

struct PendingTerms {
    terms: HashMap<Vec<u8>, Vec<u8>>,
}

impl PendingTerms {
    fn new() -> Self {
        Self { terms: HashMap::new() }
    }

    fn add(&mut self, term: &str, rowid: i64, col: i32, pos: i32) {
        let key = term.as_bytes().to_vec();
        let doclist = self.terms.entry(key).or_insert_with(Vec::new);

        // Append to doclist
        let mut buf = [0u8; 9];
        let n = sqlite3_put_varint(&mut buf, rowid as u64);
        doclist.extend_from_slice(&buf[..n]);

        // Position encoding
        let pos_val = (col << 10) + pos + 2;
        let n = sqlite3_put_varint(&mut buf, pos_val as u64);
        doclist.extend_from_slice(&buf[..n]);
    }
}
```

## FTS3 Query

```rust
impl Fts3Table {
    /// Execute a full-text query
    pub fn query(&self, expr: &str) -> Result<Fts3Cursor> {
        let parsed = self.parse_query(expr)?;

        let doclist = self.evaluate_expr(&parsed)?;

        Ok(Fts3Cursor {
            base: VtabCursor::new(),
            expr: Some(parsed),
            doclist: Some(doclist),
            rowid: 0,
            eof: false,
        })
    }

    fn evaluate_expr(&self, expr: &Fts3Expr) -> Result<DoclistIter> {
        match expr {
            Fts3Expr::Term(term) => {
                self.lookup_term(term)
            }
            Fts3Expr::And(left, right) => {
                let l = self.evaluate_expr(left)?;
                let r = self.evaluate_expr(right)?;
                Ok(DoclistIter::intersect(l, r))
            }
            Fts3Expr::Or(left, right) => {
                let l = self.evaluate_expr(left)?;
                let r = self.evaluate_expr(right)?;
                Ok(DoclistIter::union(l, r))
            }
            Fts3Expr::Not(left, right) => {
                let l = self.evaluate_expr(left)?;
                let r = self.evaluate_expr(right)?;
                Ok(DoclistIter::except(l, r))
            }
            Fts3Expr::Phrase(terms) => {
                self.lookup_phrase(terms)
            }
            _ => Err(Error::with_message(ErrorCode::Error, "unsupported expression")),
        }
    }

    fn lookup_term(&self, term: &str) -> Result<DoclistIter> {
        // Search all segments
        let mut results = Vec::new();

        // Query segment directory
        let sql = format!(
            "SELECT root, start_block, leaves_end_block FROM '{}' ORDER BY level DESC, idx ASC",
            self.segdir_table()
        );

        let mut stmt = self.db.prepare(&sql)?;
        while stmt.step()? == StepResult::Row {
            let root = stmt.column_blob(0)?;
            let start = stmt.column_int64(1)?;
            let leaves_end = stmt.column_int64(2)?;

            // Search segment
            if let Some(doclist) = self.search_segment(root, start, leaves_end, term.as_bytes())? {
                results.push(doclist);
            }
        }

        // Merge results
        Ok(DoclistIter::merge(results))
    }
}
```

## Segment Merge

```rust
impl Fts3Table {
    fn maybe_merge(&mut self) -> Result<()> {
        // Check if merge needed
        let level_counts = self.count_segments_by_level()?;

        for (level, count) in level_counts {
            if count >= MERGE_THRESHOLD {
                self.merge_level(level)?;
            }
        }

        Ok(())
    }

    fn merge_level(&mut self, level: i32) -> Result<()> {
        // Get segments at this level
        let segments = self.get_segments_at_level(level)?;

        // Create merge cursor
        let mut merged = MergeIter::new(segments)?;

        // Write merged segment at next level
        let new_segment = self.allocate_segment()?;
        let mut leaves = Vec::new();
        let mut current_leaf = LeafNode::new();

        while let Some((term, doclist)) = merged.next()? {
            if current_leaf.size() + term.len() + doclist.len() > LEAF_SIZE {
                leaves.push(current_leaf);
                current_leaf = LeafNode::new();
            }
            current_leaf.add_term(&term, &doclist);
        }

        if !current_leaf.is_empty() {
            leaves.push(current_leaf);
        }

        // Write new segment
        for (i, leaf) in leaves.iter().enumerate() {
            self.write_leaf(new_segment, i as i64, leaf.data())?;
        }

        // Update directories
        self.write_segdir(new_segment, level + 1, leaves.len())?;

        // Delete old segments
        for seg in segments {
            self.delete_segment(seg)?;
        }

        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] FTS3 virtual table creation
- [ ] Shadow tables (_content, _segments, _segdir, _stat)
- [ ] Document insertion
- [ ] Document deletion
- [ ] Term lookup
- [ ] Phrase lookup
- [ ] Boolean queries (AND, OR, NOT)
- [ ] Doclist encoding/decoding
- [ ] Segment structure
- [ ] Segment merging
- [ ] Prefix indexes
- [ ] Content table option

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `fts3.test` - Core FTS3 functionality
- `fts3a.test` - FTS3 basic queries
- `fts3b.test` - FTS3 boolean operators
- `fts3c.test` - FTS3 phrase queries
- `fts3d.test` - FTS3 prefix queries
- `fts3e.test` - FTS3 NEAR queries
- `fts3f.test` - FTS3 advanced features
- `fts3_write.test` - FTS3 write operations
