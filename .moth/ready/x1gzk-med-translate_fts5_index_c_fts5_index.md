# Translate fts5_index.c - FTS5 Index Implementation

## Overview
Translate FTS5 full-text search index implementation for managing the inverted index structure.

## Source Reference
- `sqlite3/ext/fts5/fts5_index.c` - 9,539 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### FTS5 Index
```rust
/// FTS5 index structure
pub struct Fts5Index {
    /// Database connection
    db: *mut Connection,
    /// Configuration
    config: Fts5Config,
    /// Content table info
    content: ContentInfo,
    /// B-tree for term lookup
    data: Arc<BtShared>,
    /// Write state
    writer: Option<Fts5Writer>,
    /// Read iterators
    readers: Vec<Fts5Iter>,
}

/// FTS5 configuration
pub struct Fts5Config {
    /// Column names
    pub columns: Vec<String>,
    /// Content table mode
    pub content_mode: ContentMode,
    /// Tokenizer name and args
    pub tokenizer: (String, Vec<String>),
    /// Prefix lengths for prefix indexes
    pub prefix: Vec<i32>,
    /// Column for rank()
    pub rank_column: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum ContentMode {
    /// Normal content storage
    Normal,
    /// External content
    External,
    /// Contentless
    Contentless,
}
```

### Index Structure
```rust
/// FTS5 uses a segmented index structure
pub struct Fts5Structure {
    /// Structure cookie
    cookie: i64,
    /// Write counter
    write_counter: u64,
    /// Levels
    levels: Vec<Fts5Level>,
}

pub struct Fts5Level {
    /// Segments at this level
    segments: Vec<Fts5Segment>,
    /// Is merge in progress?
    merging: bool,
}

pub struct Fts5Segment {
    /// Segment ID
    id: i64,
    /// First leaf page
    first_leaf: i64,
    /// Last leaf page
    last_leaf: i64,
    /// First term (for routing)
    first_term: Vec<u8>,
}
```

### Doclist Format
```rust
/// Document list entry
pub struct DoclistEntry {
    /// Row ID
    pub rowid: i64,
    /// Position lists by column
    pub positions: Vec<Vec<i32>>,
}

/// Position within a document
pub struct Fts5Position {
    /// Column index
    pub col: i32,
    /// Token offset within column
    pub offset: i32,
}
```

## Index Operations

### Building the Index
```rust
impl Fts5Index {
    /// Insert document into index
    pub fn insert(&mut self, rowid: i64, values: &[&str]) -> Result<()> {
        let writer = self.writer.get_or_insert_with(|| Fts5Writer::new());

        for (col_idx, value) in values.iter().enumerate() {
            // Tokenize the column
            let tokens = self.tokenize(value, col_idx)?;

            for (pos, token) in tokens.into_iter().enumerate() {
                writer.add_token(&token, rowid, col_idx as i32, pos as i32)?;
            }
        }

        // Check if we need to flush
        if writer.pending_size() > self.config.write_limit {
            self.flush()?;
        }

        Ok(())
    }

    /// Delete document from index
    pub fn delete(&mut self, rowid: i64, values: &[&str]) -> Result<()> {
        // In FTS5, delete is typically handled by:
        // 1. For contentless: mark as deleted
        // 2. For content: re-tokenize and mark terms as deleted

        let writer = self.writer.get_or_insert_with(|| Fts5Writer::new());

        for (col_idx, value) in values.iter().enumerate() {
            let tokens = self.tokenize(value, col_idx)?;

            for (pos, token) in tokens.into_iter().enumerate() {
                writer.delete_token(&token, rowid)?;
            }
        }

        Ok(())
    }

    /// Flush pending writes to disk
    pub fn flush(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            // Create new segment from pending data
            let segment = writer.to_segment()?;

            // Add to level 0
            self.structure.levels[0].segments.push(segment);

            // Check if merge needed
            if self.structure.levels[0].segments.len() > self.config.merge_trigger {
                self.merge(0)?;
            }
        }

        Ok(())
    }
}
```

### Segment Merge
```rust
impl Fts5Index {
    /// Merge segments at a level
    fn merge(&mut self, level: usize) -> Result<()> {
        let segments_to_merge = self.structure.levels[level].segments.drain(..).collect::<Vec<_>>();

        if segments_to_merge.len() < 2 {
            return Ok(());
        }

        // Create merge iterator
        let mut iterators: Vec<SegmentIter> = segments_to_merge.iter()
            .map(|s| SegmentIter::new(s))
            .collect::<Result<Vec<_>>>()?;

        // Merge into new segment
        let mut new_segment = SegmentBuilder::new();

        loop {
            // Find minimum term
            let min_term = iterators.iter()
                .filter(|it| !it.is_eof())
                .map(|it| it.term())
                .min();

            let min_term = match min_term {
                Some(t) => t.to_vec(),
                None => break,
            };

            // Collect doclists for this term from all iterators
            let mut merged_doclist = Vec::new();
            for it in &mut iterators {
                if !it.is_eof() && it.term() == min_term {
                    merged_doclist.extend(it.doclist());
                    it.next()?;
                }
            }

            // Sort by rowid and remove duplicates
            merged_doclist.sort_by_key(|d| d.rowid);
            merged_doclist.dedup_by_key(|d| d.rowid);

            new_segment.add_term(&min_term, &merged_doclist)?;
        }

        // Add new segment to next level
        let next_level = level + 1;
        while self.structure.levels.len() <= next_level {
            self.structure.levels.push(Fts5Level {
                segments: Vec::new(),
                merging: false,
            });
        }

        self.structure.levels[next_level].segments.push(new_segment.finish()?);

        Ok(())
    }
}
```

### Querying
```rust
impl Fts5Index {
    /// Look up a term
    pub fn lookup_term(&self, term: &str) -> Result<Fts5Iter> {
        let term_bytes = term.as_bytes();

        // Search all segments
        let mut segment_iters = Vec::new();

        for level in &self.structure.levels {
            for segment in &level.segments {
                if let Some(iter) = self.search_segment(segment, term_bytes)? {
                    segment_iters.push(iter);
                }
            }
        }

        Ok(Fts5Iter::merge(segment_iters))
    }

    /// Search for prefix
    pub fn lookup_prefix(&self, prefix: &str) -> Result<Fts5Iter> {
        // Similar to term lookup but matches prefix
        let prefix_bytes = prefix.as_bytes();

        let mut segment_iters = Vec::new();

        for level in &self.structure.levels {
            for segment in &level.segments {
                if let Some(iter) = self.search_prefix(segment, prefix_bytes)? {
                    segment_iters.push(iter);
                }
            }
        }

        Ok(Fts5Iter::merge(segment_iters))
    }

    fn search_segment(&self, segment: &Fts5Segment, term: &[u8]) -> Result<Option<SegmentIter>> {
        // Binary search in segment's leaf pages
        let mut lo = segment.first_leaf;
        let mut hi = segment.last_leaf;

        while lo < hi {
            let mid = (lo + hi) / 2;
            let page = self.read_leaf(mid)?;

            match page.first_term().cmp(term) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    return Ok(Some(SegmentIter::from_page(page, term)?));
                }
            }
        }

        // Check final page
        let page = self.read_leaf(lo)?;
        if page.contains_term(term) {
            Ok(Some(SegmentIter::from_page(page, term)?))
        } else {
            Ok(None)
        }
    }
}
```

### Iterator
```rust
pub struct Fts5Iter {
    /// Current rowid
    rowid: i64,
    /// Position list
    positions: Vec<Fts5Position>,
    /// Underlying iterators (for merge)
    sources: Vec<Box<dyn TermIter>>,
    /// Is at end
    eof: bool,
}

impl Fts5Iter {
    pub fn rowid(&self) -> i64 {
        self.rowid
    }

    pub fn positions(&self) -> &[Fts5Position] {
        &self.positions
    }

    pub fn next(&mut self) -> Result<()> {
        // Advance to next document
        if self.sources.is_empty() {
            self.eof = true;
            return Ok(());
        }

        // Find minimum rowid among sources
        let min_rowid = self.sources.iter()
            .filter(|s| !s.is_eof())
            .map(|s| s.rowid())
            .min();

        match min_rowid {
            Some(rid) => {
                self.rowid = rid;
                self.positions.clear();

                // Collect positions from all sources with this rowid
                for source in &mut self.sources {
                    if !source.is_eof() && source.rowid() == rid {
                        self.positions.extend(source.positions());
                        source.next()?;
                    }
                }
            }
            None => {
                self.eof = true;
            }
        }

        Ok(())
    }

    pub fn eof(&self) -> bool {
        self.eof
    }
}
```

## Acceptance Criteria
- [ ] Inverted index structure (term -> doclist)
- [ ] Segmented index with levels
- [ ] Segment merge algorithm
- [ ] Term lookup
- [ ] Prefix lookup (for prefix queries)
- [ ] Document insertion
- [ ] Document deletion
- [ ] Position list encoding
- [ ] Iterator merging
- [ ] Incremental indexing
- [ ] Rebuild/optimize
- [ ] Integrity check

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `fts5aa.test` - FTS5 basic functionality
- `fts5ab.test` - FTS5 index operations
- `fts5ac.test` - FTS5 segment merging
- `fts5ad.test` - FTS5 prefix indexes
- `fts5ae.test` - FTS5 delete operations
- `fts5corrupt.test` - FTS5 corruption handling
- `fts5fault.test` - FTS5 fault injection
