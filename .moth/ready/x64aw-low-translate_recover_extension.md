# Translate Recover Extension

## Overview
Translate database recovery extension for salvaging data from corrupt databases.

## Source Reference
- `sqlite3/ext/recover/sqlite3recover.c` - Recovery implementation
- `sqlite3/ext/recover/sqlite3recover.h` - Recovery API header
- `sqlite3/ext/recover/dbdata.c` - Raw database data access

## Design Fidelity
- SQLite's "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite's observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Recovery Handle
```rust
/// Database recovery handle
pub struct SqliteRecover {
    /// Source database (corrupt)
    source_db: Connection,
    /// Source database name
    source_name: String,
    /// Destination database
    dest_db: Connection,
    /// Current state
    state: RecoverState,
    /// Configuration
    config: RecoverConfig,
    /// Error info
    error: Option<RecoverError>,
    /// Lost and found table name
    lost_found_table: Option<String>,
    /// Recovered row count
    rows_recovered: i64,
}

#[derive(Debug, Clone)]
pub struct RecoverConfig {
    /// Recover freelist pages
    recover_freelist: bool,
    /// Recover row IDs
    recover_rowid: bool,
    /// Slow mode (more thorough)
    slow_mode: bool,
    /// Lost and found table name
    lost_found: Option<String>,
}

impl Default for RecoverConfig {
    fn default() -> Self {
        Self {
            recover_freelist: false,
            recover_rowid: true,
            slow_mode: false,
            lost_found: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RecoverState {
    Init,
    ReadSchema,
    RecoverData,
    RecoverFreelist,
    WriteOutput,
    Done,
}

#[derive(Debug)]
pub struct RecoverError {
    pub code: i32,
    pub message: String,
}
```

### Page Analysis
```rust
/// Raw page data access
pub struct DbData {
    /// Database file path
    path: String,
    /// Page size
    page_size: u32,
    /// Total pages
    n_pages: u32,
}

/// Page types
#[derive(Debug, Clone, Copy)]
pub enum PageType {
    /// B-tree internal page
    BtreeInternal,
    /// B-tree leaf page
    BtreeLeaf,
    /// Overflow page
    Overflow,
    /// Freelist trunk
    FreelistTrunk,
    /// Freelist leaf
    FreelistLeaf,
    /// Lock byte page
    LockByte,
    /// Pointer map page (auto-vacuum)
    PtrMap,
    /// Unknown/corrupt
    Unknown,
}

impl DbData {
    pub fn open(path: &str) -> Result<Self> {
        // Read database header
        let mut file = File::open(path)?;
        let mut header = [0u8; 100];
        file.read_exact(&mut header)?;

        // Parse page size
        let page_size = u16::from_be_bytes([header[16], header[17]]) as u32;
        let page_size = if page_size == 1 { 65536 } else { page_size };

        // Calculate total pages
        let file_size = file.metadata()?.len();
        let n_pages = (file_size / page_size as u64) as u32;

        Ok(Self {
            path: path.to_string(),
            page_size,
            n_pages,
        })
    }

    pub fn read_page(&self, pgno: u32) -> Result<Vec<u8>> {
        let mut file = File::open(&self.path)?;
        let offset = (pgno - 1) as u64 * self.page_size as u64;
        file.seek(SeekFrom::Start(offset))?;

        let mut data = vec![0u8; self.page_size as usize];
        file.read_exact(&mut data)?;

        Ok(data)
    }

    pub fn page_type(&self, data: &[u8]) -> PageType {
        let header_offset = if data.len() > 100 { 0 } else { 100 }; // Page 1 has file header
        let first_byte = data[header_offset];

        match first_byte {
            0x02 => PageType::BtreeInternal,
            0x05 => PageType::BtreeLeaf,
            0x0a => PageType::BtreeInternal, // Index internal
            0x0d => PageType::BtreeLeaf,     // Index leaf
            0x00 => {
                // Could be overflow, freelist, or corrupt
                PageType::Unknown
            }
            _ => PageType::Unknown,
        }
    }
}
```

### Cell Parser
```rust
/// Parse cells from B-tree page
pub struct CellParser<'a> {
    data: &'a [u8],
    page_type: PageType,
    cell_count: u16,
    current_cell: u16,
}

impl<'a> CellParser<'a> {
    pub fn new(data: &'a [u8], page_type: PageType) -> Self {
        let header_offset = if data.len() > 100 { 0 } else { 100 };
        let cell_count = u16::from_be_bytes([
            data[header_offset + 3],
            data[header_offset + 4],
        ]);

        Self {
            data,
            page_type,
            cell_count,
            current_cell: 0,
        }
    }

    pub fn next_cell(&mut self) -> Option<RecoveredCell> {
        if self.current_cell >= self.cell_count {
            return None;
        }

        // Get cell pointer
        let ptr_offset = self.header_size() + (self.current_cell as usize * 2);
        let cell_offset = u16::from_be_bytes([
            self.data[ptr_offset],
            self.data[ptr_offset + 1],
        ]) as usize;

        self.current_cell += 1;

        // Parse cell based on page type
        match self.page_type {
            PageType::BtreeLeaf => self.parse_table_leaf_cell(cell_offset),
            PageType::BtreeInternal => self.parse_table_interior_cell(cell_offset),
            _ => None,
        }
    }

    fn parse_table_leaf_cell(&self, offset: usize) -> Option<RecoveredCell> {
        let mut pos = offset;

        // Payload size (varint)
        let (payload_size, n) = sqlite3_get_varint(&self.data[pos..]);
        pos += n;

        // Row ID (varint)
        let (rowid, n) = sqlite3_get_varint(&self.data[pos..]);
        pos += n;

        // Payload
        let payload = self.data[pos..pos + payload_size as usize].to_vec();

        Some(RecoveredCell {
            rowid: rowid as i64,
            payload,
        })
    }

    fn parse_table_interior_cell(&self, offset: usize) -> Option<RecoveredCell> {
        // Interior cells have child page number + key
        let child_page = u32::from_be_bytes([
            self.data[offset],
            self.data[offset + 1],
            self.data[offset + 2],
            self.data[offset + 3],
        ]);

        let (key, _) = sqlite3_get_varint(&self.data[offset + 4..]);

        Some(RecoveredCell {
            rowid: key as i64,
            payload: child_page.to_le_bytes().to_vec(),
        })
    }

    fn header_size(&self) -> usize {
        match self.page_type {
            PageType::BtreeLeaf => 8,
            PageType::BtreeInternal => 12,
            _ => 0,
        }
    }
}

pub struct RecoveredCell {
    pub rowid: i64,
    pub payload: Vec<u8>,
}
```

### Record Parser
```rust
/// Parse SQLite record format
pub fn parse_record(payload: &[u8]) -> Result<Vec<Value>> {
    let mut pos = 0;

    // Header size (varint)
    let (header_size, n) = sqlite3_get_varint(&payload[pos..]);
    pos += n;

    let header_end = header_size as usize;
    let mut types = Vec::new();

    // Read serial types from header
    while pos < header_end {
        let (serial_type, n) = sqlite3_get_varint(&payload[pos..]);
        pos += n;
        types.push(serial_type);
    }

    // Read values
    let mut data_pos = header_end;
    let mut values = Vec::new();

    for serial_type in types {
        let (value, size) = parse_serial_value(&payload[data_pos..], serial_type);
        values.push(value);
        data_pos += size;
    }

    Ok(values)
}

fn parse_serial_value(data: &[u8], serial_type: u64) -> (Value, usize) {
    match serial_type {
        0 => (Value::Null, 0),
        1 => {
            let v = data[0] as i8 as i64;
            (Value::Integer(v), 1)
        }
        2 => {
            let v = i16::from_be_bytes([data[0], data[1]]) as i64;
            (Value::Integer(v), 2)
        }
        3 => {
            let v = i32::from_be_bytes([0, data[0], data[1], data[2]]) as i64;
            (Value::Integer(v >> 8), 3)
        }
        4 => {
            let v = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as i64;
            (Value::Integer(v), 4)
        }
        5 => {
            let mut bytes = [0u8; 8];
            bytes[2..8].copy_from_slice(&data[0..6]);
            let v = i64::from_be_bytes(bytes) >> 16;
            (Value::Integer(v), 6)
        }
        6 => {
            let v = i64::from_be_bytes(data[0..8].try_into().unwrap());
            (Value::Integer(v), 8)
        }
        7 => {
            let v = f64::from_be_bytes(data[0..8].try_into().unwrap());
            (Value::Float(v), 8)
        }
        8 => (Value::Integer(0), 0),
        9 => (Value::Integer(1), 0),
        n if n >= 12 && n % 2 == 0 => {
            // Blob
            let len = ((n - 12) / 2) as usize;
            (Value::Blob(data[..len].to_vec()), len)
        }
        n if n >= 13 && n % 2 == 1 => {
            // Text
            let len = ((n - 13) / 2) as usize;
            let s = String::from_utf8_lossy(&data[..len]).to_string();
            (Value::Text(s), len)
        }
        _ => (Value::Null, 0),
    }
}
```

## Recovery Operations

### Main Recovery Loop
```rust
impl SqliteRecover {
    pub fn new(source: &Connection, source_name: &str, dest: &Connection) -> Result<Self> {
        Ok(Self {
            source_db: source.clone(),
            source_name: source_name.to_string(),
            dest_db: dest.clone(),
            state: RecoverState::Init,
            config: RecoverConfig::default(),
            error: None,
            lost_found_table: None,
            rows_recovered: 0,
        })
    }

    pub fn config(&mut self, config: RecoverConfig) {
        self.config = config;
        self.lost_found_table = config.lost_found.clone();
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            match self.step()? {
                true => continue,
                false => break,
            }
        }
        Ok(())
    }

    pub fn step(&mut self) -> Result<bool> {
        match self.state {
            RecoverState::Init => {
                self.init_recovery()?;
                self.state = RecoverState::ReadSchema;
                Ok(true)
            }
            RecoverState::ReadSchema => {
                self.recover_schema()?;
                self.state = RecoverState::RecoverData;
                Ok(true)
            }
            RecoverState::RecoverData => {
                let more = self.recover_data_step()?;
                if !more {
                    self.state = if self.config.recover_freelist {
                        RecoverState::RecoverFreelist
                    } else {
                        RecoverState::WriteOutput
                    };
                }
                Ok(true)
            }
            RecoverState::RecoverFreelist => {
                self.recover_freelist()?;
                self.state = RecoverState::WriteOutput;
                Ok(true)
            }
            RecoverState::WriteOutput => {
                self.finalize_output()?;
                self.state = RecoverState::Done;
                Ok(false)
            }
            RecoverState::Done => Ok(false),
        }
    }

    fn recover_schema(&mut self) -> Result<()> {
        // Try to read schema from sqlite_master
        let sql = "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY rowid";

        match self.source_db.prepare(sql) {
            Ok(mut stmt) => {
                while stmt.step()? == StepResult::Row {
                    let sql = stmt.column_text(3)?;
                    if !sql.is_empty() {
                        // Recreate in destination
                        let _ = self.dest_db.execute(&sql);
                    }
                }
            }
            Err(_) => {
                // Schema corrupt - try to recover from raw pages
                self.recover_schema_from_raw()?;
            }
        }

        Ok(())
    }

    fn recover_data_step(&mut self) -> Result<bool> {
        // Scan all pages for recoverable data
        let dbdata = DbData::open(&self.source_name)?;

        for pgno in 1..=dbdata.n_pages {
            let data = match dbdata.read_page(pgno) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let page_type = dbdata.page_type(&data);
            if !matches!(page_type, PageType::BtreeLeaf) {
                continue;
            }

            let mut parser = CellParser::new(&data, page_type);
            while let Some(cell) = parser.next_cell() {
                if let Ok(values) = parse_record(&cell.payload) {
                    self.insert_recovered_row(cell.rowid, &values)?;
                    self.rows_recovered += 1;
                }
            }
        }

        Ok(false)
    }

    fn insert_recovered_row(&mut self, rowid: i64, values: &[Value]) -> Result<()> {
        // Try to insert into lost_found table if configured
        if let Some(table) = &self.lost_found_table {
            let placeholders: Vec<_> = (0..values.len()).map(|_| "?").collect();
            let sql = format!(
                "INSERT INTO \"{}\" VALUES (?, {})",
                table,
                placeholders.join(", ")
            );

            let mut stmt = self.dest_db.prepare(&sql)?;
            stmt.bind_int64(1, rowid)?;
            for (i, val) in values.iter().enumerate() {
                stmt.bind_value(i as i32 + 2, val)?;
            }
            stmt.step()?;
        }

        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] Recovery handle creation
- [ ] Schema recovery from sqlite_master
- [ ] Raw page scanning for corrupt databases
- [ ] B-tree leaf cell parsing
- [ ] Record format parsing
- [ ] Lost and found table support
- [ ] Freelist recovery option
- [ ] Row ID recovery
- [ ] Step-based recovery API
- [ ] Error handling for corrupt data
- [ ] Progress reporting
- [ ] Configuration options

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `recover.test` - Database recovery tests
- `recover2.test` - Additional recovery tests
- `dbdata.test` - Raw database data access
