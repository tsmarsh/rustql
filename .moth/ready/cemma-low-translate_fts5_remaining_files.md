# Translate fts5 Remaining Files

## Overview
Translate remaining FTS5 auxiliary components including vocab, hash, buffer, and configuration.

## Source Reference
- `sqlite3/ext/fts5/fts5_aux.c` - Auxiliary functions
- `sqlite3/ext/fts5/fts5_hash.c` - Hash table for pending terms
- `sqlite3/ext/fts5/fts5_vocab.c` - Vocabulary virtual table
- `sqlite3/ext/fts5/fts5_buffer.c` - Buffer utilities
- `sqlite3/ext/fts5/fts5_config.c` - Configuration parsing

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### FTS5 Buffer
```rust
/// Dynamic buffer for FTS5
pub struct Fts5Buffer {
    data: Vec<u8>,
}

impl Fts5Buffer {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn with_capacity(n: usize) -> Self {
        Self { data: Vec::with_capacity(n) }
    }

    pub fn append(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    pub fn append_varint(&mut self, value: u64) {
        let mut buf = [0u8; 9];
        let len = sqlite3_put_varint(&mut buf, value);
        self.data.extend_from_slice(&buf[..len]);
    }

    pub fn append_blob(&mut self, data: &[u8]) {
        self.append_varint(data.len() as u64);
        self.data.extend_from_slice(data);
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }
}
```

### FTS5 Hash (Pending Terms)
```rust
/// Hash table for terms pending write
pub struct Fts5Hash {
    slots: Vec<Option<Box<Fts5HashEntry>>>,
    n_slot: usize,
    n_entry: usize,
}

struct Fts5HashEntry {
    term: Vec<u8>,
    doclist: Fts5Buffer,
    next: Option<Box<Fts5HashEntry>>,
}

impl Fts5Hash {
    pub fn new() -> Self {
        Self {
            slots: vec![None; 1024],
            n_slot: 1024,
            n_entry: 0,
        }
    }

    pub fn insert(&mut self, term: &[u8], rowid: i64, col: i32, pos: i32) {
        let hash = self.hash(term);
        let idx = hash % self.n_slot;

        // Find or create entry
        let entry = self.find_or_create(idx, term);

        // Append to doclist
        entry.doclist.append_varint(rowid as u64);
        entry.doclist.append_varint(col as u64);
        entry.doclist.append_varint(pos as u64);
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.slots.iter()
            .filter_map(|slot| slot.as_ref())
            .flat_map(|entry| {
                std::iter::once(entry.as_ref())
                    .chain(std::iter::successors(
                        entry.next.as_ref().map(|b| b.as_ref()),
                        |e| e.next.as_ref().map(|b| b.as_ref())
                    ))
            })
            .map(|e| (e.term.as_slice(), e.doclist.data()))
    }

    fn hash(&self, term: &[u8]) -> usize {
        let mut h: u32 = 0;
        for &b in term {
            h = h.wrapping_mul(31).wrapping_add(b as u32);
        }
        h as usize
    }

    fn find_or_create(&mut self, idx: usize, term: &[u8]) -> &mut Fts5HashEntry {
        // Search existing chain
        let mut current = &mut self.slots[idx];
        while let Some(ref mut entry) = current {
            if entry.term == term {
                return entry;
            }
            current = &mut entry.next;
        }

        // Create new entry
        *current = Some(Box::new(Fts5HashEntry {
            term: term.to_vec(),
            doclist: Fts5Buffer::new(),
            next: None,
        }));
        self.n_entry += 1;

        current.as_mut().unwrap()
    }
}
```

### FTS5 Vocab Virtual Table
```rust
/// fts5vocab virtual table - inspect index contents
pub struct Fts5VocabTable {
    fts_table: String,
    mode: VocabMode,
}

#[derive(Debug, Clone, Copy)]
pub enum VocabMode {
    /// One row per term per column
    Col,
    /// One row per term
    Row,
    /// One row per term instance
    Instance,
}

pub struct Fts5VocabCursor {
    table: Arc<Fts5VocabTable>,
    iter: Option<VocabIter>,
    current: Option<VocabRow>,
    eof: bool,
}

struct VocabRow {
    term: String,
    col: Option<String>,
    doc_count: i64,
    term_count: i64,
}

impl VirtualTable for Fts5VocabTable {
    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        info.estimated_cost = 1000000.0;
        Ok(())
    }

    fn open(&self) -> Result<Box<dyn Cursor>> {
        Ok(Box::new(Fts5VocabCursor {
            table: Arc::new(self.clone()),
            iter: None,
            current: None,
            eof: false,
        }))
    }
}

impl Cursor for Fts5VocabCursor {
    fn filter(&mut self, _idx_num: i32, _idx_str: Option<&str>, _args: &[&Value]) -> Result<()> {
        // Start iteration over vocabulary
        self.iter = Some(VocabIter::new(&self.table.fts_table)?);
        self.next()?;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        if let Some(ref mut iter) = self.iter {
            self.current = iter.next()?;
            self.eof = self.current.is_none();
        } else {
            self.eof = true;
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, idx: i32) -> Result<Value> {
        let row = self.current.as_ref().ok_or(Error::with_code(ErrorCode::Error))?;

        Ok(match idx {
            0 => Value::Text(row.term.clone()),
            1 => row.col.clone().map(Value::Text).unwrap_or(Value::Null),
            2 => Value::Integer(row.doc_count),
            3 => Value::Integer(row.term_count),
            _ => Value::Null,
        })
    }

    fn rowid(&self) -> Result<i64> {
        Ok(0) // Virtual rowid
    }
}
```

### Configuration Parsing
```rust
impl Fts5Config {
    pub fn parse(args: &[&str]) -> Result<Self> {
        let mut config = Self::default();

        // First arg is table name (already handled)
        // Parse remaining args

        let mut i = 0;
        while i < args.len() {
            let arg = args[i];

            if arg.contains('=') {
                // key=value option
                let parts: Vec<&str> = arg.splitn(2, '=').collect();
                let key = parts[0].trim();
                let value = parts[1].trim();

                match key {
                    "content" => {
                        config.content_mode = if value.is_empty() {
                            ContentMode::Contentless
                        } else {
                            ContentMode::External
                        };
                        config.content_table = Some(value.to_string());
                    }
                    "content_rowid" => {
                        config.content_rowid = Some(value.to_string());
                    }
                    "tokenize" => {
                        let tok_args: Vec<&str> = value.split_whitespace().collect();
                        if !tok_args.is_empty() {
                            config.tokenizer = (
                                tok_args[0].to_string(),
                                tok_args[1..].iter().map(|s| s.to_string()).collect()
                            );
                        }
                    }
                    "prefix" => {
                        config.prefix = value.split(',')
                            .filter_map(|s| s.trim().parse().ok())
                            .collect();
                    }
                    "columnsize" => {
                        config.column_size = value != "0";
                    }
                    "detail" => {
                        config.detail = match value {
                            "full" => DetailMode::Full,
                            "column" => DetailMode::Column,
                            "none" => DetailMode::None,
                            _ => return Err(Error::with_message(
                                ErrorCode::Error,
                                format!("unknown detail mode: {}", value)
                            )),
                        };
                    }
                    _ => {
                        // Unknown option - might be column definition
                    }
                }
            } else {
                // Column definition
                let col_name = arg.trim();
                if !col_name.is_empty() {
                    config.columns.push(col_name.to_string());
                }
            }

            i += 1;
        }

        // Default tokenizer
        if config.tokenizer.0.is_empty() {
            config.tokenizer = ("unicode61".to_string(), Vec::new());
        }

        Ok(config)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum DetailMode {
    #[default]
    Full,
    Column,
    None,
}
```

## Auxiliary Functions Implementation

```rust
/// Auxiliary function API
pub struct Fts5ExtensionApi {
    /// Get phrase count
    pub phrase_count: fn(&Fts5AuxContext) -> i32,
    /// Get phrase hit count
    pub phrase_size: fn(&Fts5AuxContext, i32) -> i32,
    /// Iterate phrase instances
    pub phrase_inst_iter: fn(&Fts5AuxContext, i32) -> Box<dyn Iterator<Item = (i32, i32, i32)>>,
    /// Get column text
    pub column_text: fn(&Fts5AuxContext, i32) -> Result<String>,
    /// Get column size
    pub column_size: fn(&Fts5AuxContext, i32) -> i32,
    /// Get total column count
    pub column_total_size: fn(&Fts5AuxContext, i32) -> i64,
    /// Get row count
    pub row_count: fn(&Fts5AuxContext) -> i64,
    /// Get average column size
    pub column_avg_size: fn(&Fts5AuxContext, i32) -> f64,
}

/// Register custom auxiliary function
pub fn fts5_create_aux(
    db: &mut Connection,
    name: &str,
    user_data: Option<*mut ()>,
    func: Fts5AuxFunc,
) -> Result<()> {
    // Store in connection's FTS5 aux function registry
    Ok(())
}
```

## Acceptance Criteria
- [ ] FTS5 buffer utilities
- [ ] FTS5 hash for pending terms
- [ ] fts5vocab virtual table (row mode)
- [ ] fts5vocab virtual table (col mode)
- [ ] fts5vocab virtual table (instance mode)
- [ ] Configuration parsing
- [ ] Content/contentless options
- [ ] Tokenizer configuration
- [ ] Prefix index configuration
- [ ] Detail mode (full/column/none)
- [ ] Auxiliary function API
- [ ] Custom auxiliary function registration

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `fts5vocab.test` - FTS5 vocabulary virtual table
- `fts5vocab2.test` - Additional vocab tests
- `fts5config.test` - FTS5 configuration parsing
- `fts5config2.test` - Additional config tests
- `fts5aux.test` - FTS5 auxiliary functions
- `fts5hash.test` - FTS5 hash table tests
- `fts5detail.test` - FTS5 detail modes
