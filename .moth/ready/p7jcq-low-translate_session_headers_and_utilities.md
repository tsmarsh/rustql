# Translate Session Headers and Utilities

## Overview
Translate session extension headers and utility functions.

## Source Reference
- `sqlite3/ext/session/sqlite3session.h` - Public API header
- `sqlite3/ext/session/test_session.c` - Test utilities

## Design Fidelity
- SQLite's "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite's observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Public API Types
```rust
/// Session handle (opaque)
pub struct SqliteSession {
    inner: Session,
}

/// Changeset iterator handle
pub struct SqliteChangesetIter<'a> {
    inner: ChangesetIter<'a>,
}

/// Rebaser handle for changeset combination
pub struct SqliteRebaser {
    /// Base changeset for rebasing
    base: Changeset,
    /// Mapping of PK values
    pk_map: HashMap<Vec<u8>, Vec<u8>>,
}

/// Changegroup for combining changesets
pub struct SqliteChangegroup {
    /// Combined changes by table
    tables: HashMap<String, TableChanges>,
}

struct TableChanges {
    n_col: i32,
    pk_cols: Vec<bool>,
    changes: HashMap<Vec<u8>, SessionChange>,
}
```

### Configuration Structures
```rust
/// Session configuration
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Size threshold for patchset (vs changeset)
    pub patchset_threshold: usize,
    /// Enable change statistics
    pub enable_stats: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            patchset_threshold: 0,
            enable_stats: false,
        }
    }
}

/// Changeset statistics
#[derive(Debug, Default)]
pub struct ChangesetStats {
    /// Number of INSERT operations
    pub inserts: i32,
    /// Number of UPDATE operations
    pub updates: i32,
    /// Number of DELETE operations
    pub deletes: i32,
    /// Total changeset size in bytes
    pub size: usize,
}
```

## Changegroup Operations
```rust
impl SqliteChangegroup {
    /// Create new changegroup
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Add changeset to group
    pub fn add(&mut self, changeset: &Changeset) -> Result<()> {
        let mut iter = changeset.iter();

        while let Some(change) = iter.next()? {
            let table = self.tables.entry(change.table.clone())
                .or_insert_with(|| TableChanges {
                    n_col: change.n_col,
                    pk_cols: change.pk.clone(),
                    changes: HashMap::new(),
                });

            // Compute PK value
            let pk_value = compute_pk_value(&change);

            // Merge with existing change
            self.merge_change(table, pk_value, change)?;
        }

        Ok(())
    }

    fn merge_change(
        &mut self,
        table: &mut TableChanges,
        pk: Vec<u8>,
        change: ChangesetChange,
    ) -> Result<()> {
        if let Some(existing) = table.changes.get(&pk) {
            // Combine changes
            let combined = combine_changes(existing, &change)?;
            if let Some(c) = combined {
                table.changes.insert(pk, c);
            } else {
                // Changes cancel out
                table.changes.remove(&pk);
            }
        } else {
            table.changes.insert(pk, SessionChange {
                op: change.op,
                old: change.old,
                new: change.new,
                indirect: change.indirect,
            });
        }

        Ok(())
    }

    /// Output combined changeset
    pub fn output(&self) -> Result<Changeset> {
        let mut data = Vec::new();

        for (name, table) in &self.tables {
            if table.changes.is_empty() {
                continue;
            }

            // Write table header
            write_table_header(&mut data, name, table.n_col, &table.pk_cols)?;

            // Write changes
            for (_, change) in &table.changes {
                write_change(&mut data, table.n_col, &table.pk_cols, change)?;
            }
        }

        Ok(Changeset { data })
    }
}

/// Combine two changes on the same row
fn combine_changes(
    first: &SessionChange,
    second: &ChangesetChange,
) -> Result<Option<SessionChange>> {
    match (first.op, second.op) {
        (ChangeOp::Insert, ChangeOp::Delete) => {
            // INSERT then DELETE = no change
            Ok(None)
        }
        (ChangeOp::Insert, ChangeOp::Update) => {
            // INSERT then UPDATE = INSERT with new values
            Ok(Some(SessionChange {
                op: ChangeOp::Insert,
                old: None,
                new: second.new.clone(),
                indirect: first.indirect || second.indirect,
            }))
        }
        (ChangeOp::Update, ChangeOp::Update) => {
            // UPDATE then UPDATE = UPDATE
            Ok(Some(SessionChange {
                op: ChangeOp::Update,
                old: first.old.clone(),
                new: second.new.clone(),
                indirect: first.indirect || second.indirect,
            }))
        }
        (ChangeOp::Update, ChangeOp::Delete) => {
            // UPDATE then DELETE = DELETE
            Ok(Some(SessionChange {
                op: ChangeOp::Delete,
                old: first.old.clone(),
                new: None,
                indirect: first.indirect || second.indirect,
            }))
        }
        (ChangeOp::Delete, ChangeOp::Insert) => {
            // DELETE then INSERT = UPDATE
            Ok(Some(SessionChange {
                op: ChangeOp::Update,
                old: first.old.clone(),
                new: second.new.clone(),
                indirect: first.indirect || second.indirect,
            }))
        }
        _ => Err(Error::with_message(
            ErrorCode::Error,
            "invalid change combination",
        )),
    }
}
```

## Rebaser Operations
```rust
impl SqliteRebaser {
    /// Create rebaser from base changeset
    pub fn new(base: Changeset) -> Result<Self> {
        Ok(Self {
            base,
            pk_map: HashMap::new(),
        })
    }

    /// Configure rebaser (map old PK to new PK)
    pub fn configure(&mut self, table: &str, old_pk: &[Value], new_pk: &[Value]) -> Result<()> {
        let old_key = encode_pk_values(old_pk);
        let new_key = encode_pk_values(new_pk);
        self.pk_map.insert(old_key, new_key);
        Ok(())
    }

    /// Rebase changeset against base
    pub fn rebase(&self, changeset: &Changeset) -> Result<Changeset> {
        let mut data = Vec::new();
        let mut iter = changeset.iter();

        while let Some(mut change) = iter.next()? {
            // Check if PK needs remapping
            let pk_key = compute_pk_key(&change);
            if let Some(new_pk) = self.pk_map.get(&pk_key) {
                // Update PK in change
                change = remap_pk(change, new_pk)?;
            }

            // Check for conflicts with base
            if self.conflicts_with_base(&change)? {
                // Skip conflicting change
                continue;
            }

            encode_change(&mut data, &change)?;
        }

        Ok(Changeset { data })
    }

    fn conflicts_with_base(&self, change: &ChangesetChange) -> Result<bool> {
        let mut iter = self.base.iter();

        while let Some(base_change) = iter.next()? {
            if base_change.table != change.table {
                continue;
            }

            let base_pk = compute_pk_key(&base_change);
            let change_pk = compute_pk_key(change);

            if base_pk == change_pk {
                // Same row - check for conflict
                match (base_change.op, change.op) {
                    (ChangeOp::Delete, ChangeOp::Update) => return Ok(true),
                    (ChangeOp::Delete, ChangeOp::Delete) => return Ok(true),
                    (ChangeOp::Update, _) => {
                        // Check if change.old matches base.old
                        if !values_match(&base_change.old, &change.old) {
                            return Ok(true);
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(false)
    }
}
```

## Stream API
```rust
/// Streaming changeset input
pub struct ChangesetInputStream<R: Read> {
    reader: R,
    buffer: Vec<u8>,
}

impl<R: Read> ChangesetInputStream<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: Vec::with_capacity(4096),
        }
    }

    /// Apply changeset from stream
    pub fn apply(
        &mut self,
        db: &mut Connection,
        filter: Option<&dyn Fn(&str) -> bool>,
        conflict: Option<&dyn Fn(&ChangesetChange, ConflictType) -> ConflictAction>,
    ) -> Result<()> {
        loop {
            // Read next change
            match self.read_next_change()? {
                Some(change) => {
                    if let Some(f) = filter {
                        if !f(&change.table) {
                            continue;
                        }
                    }
                    apply_single_change(db, &change)?;
                }
                None => break,
            }
        }
        Ok(())
    }

    fn read_next_change(&mut self) -> Result<Option<ChangesetChange>> {
        // Read from stream into buffer as needed
        // Parse next change from buffer
        todo!()
    }
}

/// Streaming changeset output
pub struct ChangesetOutputStream<W: Write> {
    writer: W,
    current_table: Option<String>,
}

impl<W: Write> ChangesetOutputStream<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            current_table: None,
        }
    }

    /// Write table header if needed
    pub fn write_table_header(&mut self, name: &str, n_col: i32, pk: &[bool]) -> Result<()> {
        if self.current_table.as_deref() == Some(name) {
            return Ok(());
        }

        // Write header to stream
        let mut buf = Vec::new();
        sqlite3_put_varint(&mut buf, name.len() as u64);
        buf.extend_from_slice(name.as_bytes());
        sqlite3_put_varint(&mut buf, n_col as u64);
        for &is_pk in pk {
            buf.push(if is_pk { 1 } else { 0 });
        }

        self.writer.write_all(&buf)?;
        self.current_table = Some(name.to_string());
        Ok(())
    }

    /// Write change
    pub fn write_change(&mut self, change: &SessionChange, n_col: i32, pk: &[bool]) -> Result<()> {
        let mut buf = Vec::new();
        write_change(&mut buf, n_col, pk, change)?;
        self.writer.write_all(&buf)?;
        Ok(())
    }
}
```

## Utility Functions
```rust
/// Check if changeset is empty
pub fn changeset_is_empty(changeset: &Changeset) -> bool {
    changeset.data.is_empty()
}

/// Get changeset size in bytes
pub fn changeset_size(changeset: &Changeset) -> usize {
    changeset.data.len()
}

/// Concatenate changesets
pub fn changeset_concat(a: &Changeset, b: &Changeset) -> Result<Changeset> {
    let mut group = SqliteChangegroup::new();
    group.add(a)?;
    group.add(b)?;
    group.output()
}

/// Diff two changesets
pub fn changeset_diff(
    db: &mut Connection,
    table: &str,
    from_db: &str,
    to_db: &str,
) -> Result<Changeset> {
    // Compare rows in two databases
    let sql = format!(
        "SELECT * FROM \"{}\".\"{}\" EXCEPT SELECT * FROM \"{}\".\"{}\"",
        to_db, table, from_db, table
    );

    // Build changeset from differences
    todo!()
}

/// Encode value for changeset
fn encode_value(data: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => data.push(0),
        Value::Integer(i) => {
            data.push(1);
            sqlite3_put_varint(data, *i as u64);
        }
        Value::Float(f) => {
            data.push(2);
            data.extend_from_slice(&f.to_be_bytes());
        }
        Value::Text(s) => {
            data.push(3);
            sqlite3_put_varint(data, s.len() as u64);
            data.extend_from_slice(s.as_bytes());
        }
        Value::Blob(b) => {
            data.push(4);
            sqlite3_put_varint(data, b.len() as u64);
            data.extend_from_slice(b);
        }
    }
}

/// Decode value from changeset
fn decode_value(data: &[u8], pos: &mut usize) -> Result<Value> {
    let type_byte = data[*pos];
    *pos += 1;

    match type_byte {
        0 => Ok(Value::Null),
        1 => {
            let (v, n) = sqlite3_get_varint(&data[*pos..]);
            *pos += n;
            Ok(Value::Integer(v as i64))
        }
        2 => {
            let f = f64::from_be_bytes(data[*pos..*pos+8].try_into().unwrap());
            *pos += 8;
            Ok(Value::Float(f))
        }
        3 => {
            let (len, n) = sqlite3_get_varint(&data[*pos..]);
            *pos += n;
            let s = String::from_utf8(data[*pos..*pos+len as usize].to_vec())?;
            *pos += len as usize;
            Ok(Value::Text(s))
        }
        4 => {
            let (len, n) = sqlite3_get_varint(&data[*pos..]);
            *pos += n;
            let b = data[*pos..*pos+len as usize].to_vec();
            *pos += len as usize;
            Ok(Value::Blob(b))
        }
        _ => Err(Error::with_message(ErrorCode::Error, "invalid value type")),
    }
}
```

## Acceptance Criteria
- [ ] Public API type definitions
- [ ] Changegroup creation and merging
- [ ] Changeset concatenation
- [ ] Rebaser for conflict resolution
- [ ] PK remapping support
- [ ] Streaming input API
- [ ] Streaming output API
- [ ] Value encoding/decoding utilities
- [ ] Changeset statistics
- [ ] Empty check utility
- [ ] Size calculation
- [ ] Changeset diff between databases
