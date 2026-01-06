# Translate vdbeblob.c - BLOB Handling

## Overview
Translate the incremental BLOB I/O API which allows reading and writing portions of BLOB values without loading the entire BLOB into memory.

## Source Reference
- `sqlite3/src/vdbeblob.c` - 540 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Blob Handle
```rust
pub struct Blob {
    /// Database connection
    db: Arc<Connection>,

    /// VDBE for access
    stmt: Statement,

    /// B-tree cursor
    cursor: BtCursor,

    /// Writable flag
    writable: bool,

    /// Database index (0 = main, 1 = temp, 2+ = attached)
    db_idx: i32,

    /// Table name
    table: String,

    /// Column name
    column: String,

    /// Current rowid
    rowid: i64,

    /// Column index in table
    col_idx: i32,

    /// Size of blob in bytes
    size: i32,

    /// Offset to blob data within record
    offset: i32,
}
```

## Key Functions

### Opening BLOB

```rust
/// Open a BLOB for incremental I/O
/// sqlite3_blob_open()
pub fn blob_open(
    db: &Connection,
    db_name: &str,
    table: &str,
    column: &str,
    rowid: i64,
    writable: bool,
) -> Result<Blob> {
    // Look up database
    let db_idx = db.find_db_index(db_name)?;

    // Look up table
    let table_info = db.schema(db_idx).get_table(table)?;

    // Look up column
    let col_idx = table_info.find_column(column)?;
    let col = &table_info.columns[col_idx];

    // Verify column allows BLOB
    if col.affinity != Affinity::Blob && col.affinity != Affinity::Text {
        return Err(Error::with_message(
            ErrorCode::Error,
            "cannot open value of that type",
        ));
    }

    // Open cursor on table
    let btree = db.get_btree(db_idx)?;
    let cursor = btree.cursor(table_info.root_page, writable, None)?;

    // Seek to row
    cursor.move_to(rowid)?;

    // Check row exists
    if !cursor.valid() || cursor.rowid()? != rowid {
        return Err(Error::new(ErrorCode::NotFound));
    }

    // Get blob info
    let record = cursor.payload()?;
    let (size, offset) = get_blob_info(&record, col_idx)?;

    Ok(Blob {
        db: db.clone(),
        stmt: Statement::dummy(),
        cursor,
        writable,
        db_idx: db_idx as i32,
        table: table.to_string(),
        column: column.to_string(),
        rowid,
        col_idx: col_idx as i32,
        size,
        offset,
    })
}

/// Get blob size and offset from record
fn get_blob_info(record: &[u8], col_idx: usize) -> Result<(i32, i32)> {
    // Parse record header
    let (types, header_end) = Vdbe::decode_record_header(record)?;

    // Calculate offset to column
    let mut offset = header_end;
    for i in 0..col_idx {
        offset += types[i].size();
    }

    // Get size of this column
    let size = types[col_idx].size() as i32;

    Ok((size, offset as i32))
}
```

### Reading BLOB

```rust
impl Blob {
    /// Read bytes from BLOB
    /// sqlite3_blob_read()
    pub fn read(&self, buf: &mut [u8], offset: i32) -> Result<()> {
        // Validate offset and size
        let n = buf.len() as i32;
        if offset < 0 || n < 0 || offset + n > self.size {
            return Err(Error::new(ErrorCode::Error));
        }

        // Read from B-tree cursor
        let data = self.cursor.payload()?;
        let start = (self.offset + offset) as usize;
        let end = start + n as usize;

        buf.copy_from_slice(&data[start..end]);

        Ok(())
    }

    /// Read entire BLOB into new Vec
    pub fn read_all(&self) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; self.size as usize];
        self.read(&mut buf, 0)?;
        Ok(buf)
    }
}
```

### Writing BLOB

```rust
impl Blob {
    /// Write bytes to BLOB
    /// sqlite3_blob_write()
    pub fn write(&mut self, data: &[u8], offset: i32) -> Result<()> {
        // Check writable
        if !self.writable {
            return Err(Error::new(ErrorCode::ReadOnly));
        }

        // Validate offset and size
        let n = data.len() as i32;
        if offset < 0 || n < 0 || offset + n > self.size {
            return Err(Error::new(ErrorCode::Error));
        }

        // Get current record
        let mut record = self.cursor.payload()?.to_vec();

        // Modify in place
        let start = (self.offset + offset) as usize;
        let end = start + n as usize;
        record[start..end].copy_from_slice(data);

        // Write back
        self.cursor.update_payload(&record)?;

        Ok(())
    }
}
```

### Repositioning

```rust
impl Blob {
    /// Move BLOB handle to different row
    /// sqlite3_blob_reopen()
    pub fn reopen(&mut self, rowid: i64) -> Result<()> {
        // Seek to new row
        self.cursor.move_to(rowid)?;

        // Check row exists
        if !self.cursor.valid() || self.cursor.rowid()? != rowid {
            return Err(Error::new(ErrorCode::NotFound));
        }

        // Update info
        self.rowid = rowid;

        // Recalculate blob info (size might be different)
        let record = self.cursor.payload()?;
        let (size, offset) = get_blob_info(&record, self.col_idx as usize)?;
        self.size = size;
        self.offset = offset;

        Ok(())
    }
}
```

### Query Functions

```rust
impl Blob {
    /// Get BLOB size in bytes
    /// sqlite3_blob_bytes()
    pub fn bytes(&self) -> i32 {
        self.size
    }
}
```

### Closing

```rust
impl Blob {
    /// Close BLOB handle
    /// sqlite3_blob_close()
    pub fn close(self) -> Result<()> {
        // Cursor cleanup handled by Drop
        Ok(())
    }
}

impl Drop for Blob {
    fn drop(&mut self) {
        // Close cursor
        // Any errors ignored in drop
    }
}
```

## Public API

```rust
/// Open BLOB for reading/writing
pub fn sqlite3_blob_open(
    db: &Connection,
    db_name: &str,
    table: &str,
    column: &str,
    rowid: i64,
    flags: i32,
) -> Result<Blob>;

/// Close BLOB handle
pub fn sqlite3_blob_close(blob: Blob) -> Result<()>;

/// Read from BLOB
pub fn sqlite3_blob_read(blob: &Blob, buf: &mut [u8], offset: i32) -> Result<()>;

/// Write to BLOB
pub fn sqlite3_blob_write(blob: &mut Blob, data: &[u8], offset: i32) -> Result<()>;

/// Get BLOB size
pub fn sqlite3_blob_bytes(blob: &Blob) -> i32;

/// Reposition to different row
pub fn sqlite3_blob_reopen(blob: &mut Blob, rowid: i64) -> Result<()>;
```

## Usage Example

```rust
// Open blob for reading
let mut blob = blob_open(&db, "main", "images", "data", 42, false)?;

// Get size
let size = blob.bytes();

// Read first 1KB
let mut header = vec![0u8; 1024];
blob.read(&mut header, 0)?;

// Read rest
let mut rest = vec![0u8; size as usize - 1024];
blob.read(&mut rest, 1024)?;

// Close
blob.close()?;
```

## Overflow Page Handling

For large BLOBs that span multiple pages:

```rust
impl Blob {
    /// Read potentially spanning overflow pages
    fn read_overflow(&self, buf: &mut [u8], offset: i32) -> Result<()> {
        // If blob spans overflow pages, need to follow chain
        // The cursor's payload() method handles this
        let data = self.cursor.payload()?;
        let start = (self.offset + offset) as usize;
        let end = start + buf.len();
        buf.copy_from_slice(&data[start..end]);
        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] Blob struct with cursor and position info
- [ ] blob_open() with table/column/row lookup
- [ ] read() for partial reads
- [ ] write() for partial writes
- [ ] bytes() for size query
- [ ] reopen() for repositioning
- [ ] close() with proper cleanup
- [ ] Handle overflow pages for large BLOBs
- [ ] Proper error handling for invalid access
