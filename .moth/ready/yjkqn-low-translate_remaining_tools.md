# Translate Remaining Tools

## Overview
Translate remaining SQLite command-line tools: showdb, showwal, dbhash, and others.

## Source Reference
- `sqlite3/tool/showdb.c` - Database file inspector
- `sqlite3/tool/showwal.c` - WAL file inspector
- `sqlite3/tool/showjournal.c` - Journal file inspector
- `sqlite3/tool/dbhash.c` - Database content hash
- `sqlite3/tool/mksourceid.c` - Source ID generator
- `sqlite3/tool/showshm.c` - Shared memory inspector

## Design Fidelity
- SQLite's "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite's observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## showdb - Database Inspector

### Key Structures
```rust
/// Database file inspector
pub struct ShowDb {
    /// Database file path
    path: String,
    /// File handle
    file: File,
    /// Page size
    page_size: u32,
    /// Total pages
    n_pages: u32,
}

impl ShowDb {
    pub fn open(path: &str) -> Result<Self> {
        let mut file = File::open(path)?;

        // Read header
        let mut header = [0u8; 100];
        file.read_exact(&mut header)?;

        let page_size = u16::from_be_bytes([header[16], header[17]]) as u32;
        let page_size = if page_size == 1 { 65536 } else { page_size };

        let file_size = file.metadata()?.len();
        let n_pages = (file_size / page_size as u64) as u32;

        Ok(Self {
            path: path.to_string(),
            file,
            page_size,
            n_pages,
        })
    }

    /// Show database header info
    pub fn show_header(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; 100];
        self.file.read_exact(&mut header)?;

        println!("Database: {}", self.path);
        println!("Magic header: {}", String::from_utf8_lossy(&header[0..16]));
        println!("Page size: {}", self.page_size);
        println!("Total pages: {}", self.n_pages);
        println!("File format write version: {}", header[18]);
        println!("File format read version: {}", header[19]);
        println!("Reserved space: {}", header[20]);
        println!("Max embedded payload fraction: {}", header[21]);
        println!("Min embedded payload fraction: {}", header[22]);
        println!("Leaf payload fraction: {}", header[23]);
        println!("File change counter: {}", u32::from_be_bytes([
            header[24], header[25], header[26], header[27]
        ]));
        println!("Database size in pages: {}", u32::from_be_bytes([
            header[28], header[29], header[30], header[31]
        ]));
        println!("First freelist trunk page: {}", u32::from_be_bytes([
            header[32], header[33], header[34], header[35]
        ]));
        println!("Total freelist pages: {}", u32::from_be_bytes([
            header[36], header[37], header[38], header[39]
        ]));
        println!("Schema cookie: {}", u32::from_be_bytes([
            header[40], header[41], header[42], header[43]
        ]));
        println!("Schema format: {}", u32::from_be_bytes([
            header[44], header[45], header[46], header[47]
        ]));
        println!("Default cache size: {}", u32::from_be_bytes([
            header[48], header[49], header[50], header[51]
        ]));
        println!("Auto-vacuum mode: {}", u32::from_be_bytes([
            header[52], header[53], header[54], header[55]
        ]));
        println!("Text encoding: {}", match u32::from_be_bytes([
            header[56], header[57], header[58], header[59]
        ]) {
            1 => "UTF-8",
            2 => "UTF-16le",
            3 => "UTF-16be",
            _ => "Unknown",
        });
        println!("User version: {}", u32::from_be_bytes([
            header[60], header[61], header[62], header[63]
        ]));
        println!("Incremental vacuum: {}", u32::from_be_bytes([
            header[64], header[65], header[66], header[67]
        ]));
        println!("Application ID: {}", u32::from_be_bytes([
            header[68], header[69], header[70], header[71]
        ]));
        println!("Version valid for: {}", u32::from_be_bytes([
            header[92], header[93], header[94], header[95]
        ]));
        println!("SQLite version: {}", u32::from_be_bytes([
            header[96], header[97], header[98], header[99]
        ]));

        Ok(())
    }

    /// Show page info
    pub fn show_page(&mut self, pgno: u32) -> Result<()> {
        if pgno < 1 || pgno > self.n_pages {
            return Err(Error::with_message(ErrorCode::Error, "invalid page number"));
        }

        let offset = (pgno as u64 - 1) * self.page_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let mut page = vec![0u8; self.page_size as usize];
        self.file.read_exact(&mut page)?;

        let header_offset = if pgno == 1 { 100 } else { 0 };
        let page_type = page[header_offset];

        println!("Page {}: ", pgno);
        println!("  Type: {}", match page_type {
            0x02 => "Interior index b-tree",
            0x05 => "Interior table b-tree",
            0x0a => "Leaf index b-tree",
            0x0d => "Leaf table b-tree",
            0x00 => "Overflow or free",
            _ => "Unknown",
        });

        if page_type == 0x02 || page_type == 0x05 || page_type == 0x0a || page_type == 0x0d {
            let first_freeblock = u16::from_be_bytes([
                page[header_offset + 1], page[header_offset + 2]
            ]);
            let cell_count = u16::from_be_bytes([
                page[header_offset + 3], page[header_offset + 4]
            ]);
            let cell_content_start = u16::from_be_bytes([
                page[header_offset + 5], page[header_offset + 6]
            ]);
            let fragmented_free = page[header_offset + 7];

            println!("  First freeblock: {}", first_freeblock);
            println!("  Cell count: {}", cell_count);
            println!("  Cell content start: {}", cell_content_start);
            println!("  Fragmented free bytes: {}", fragmented_free);

            if page_type == 0x02 || page_type == 0x05 {
                let right_child = u32::from_be_bytes([
                    page[header_offset + 8], page[header_offset + 9],
                    page[header_offset + 10], page[header_offset + 11]
                ]);
                println!("  Right child: {}", right_child);
            }
        }

        Ok(())
    }

    /// Hex dump of page
    pub fn dump_page(&mut self, pgno: u32) -> Result<()> {
        if pgno < 1 || pgno > self.n_pages {
            return Err(Error::with_message(ErrorCode::Error, "invalid page number"));
        }

        let offset = (pgno as u64 - 1) * self.page_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let mut page = vec![0u8; self.page_size as usize];
        self.file.read_exact(&mut page)?;

        for (i, chunk) in page.chunks(16).enumerate() {
            print!("{:08x}: ", i * 16);

            // Hex
            for byte in chunk {
                print!("{:02x} ", byte);
            }

            // Padding
            for _ in chunk.len()..16 {
                print!("   ");
            }

            // ASCII
            print!(" |");
            for byte in chunk {
                let c = if *byte >= 0x20 && *byte < 0x7f {
                    *byte as char
                } else {
                    '.'
                };
                print!("{}", c);
            }
            println!("|");
        }

        Ok(())
    }
}
```

## showwal - WAL Inspector

```rust
/// WAL file inspector
pub struct ShowWal {
    path: String,
    file: File,
    page_size: u32,
}

impl ShowWal {
    pub fn open(path: &str) -> Result<Self> {
        let mut file = File::open(path)?;

        // Read WAL header
        let mut header = [0u8; 32];
        file.read_exact(&mut header)?;

        let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        if magic != 0x377f0682 && magic != 0x377f0683 {
            return Err(Error::with_message(ErrorCode::Error, "not a WAL file"));
        }

        let page_size = u32::from_be_bytes([
            header[8], header[9], header[10], header[11]
        ]);

        Ok(Self {
            path: path.to_string(),
            file,
            page_size,
        })
    }

    /// Show WAL header
    pub fn show_header(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; 32];
        self.file.read_exact(&mut header)?;

        let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        println!("WAL: {}", self.path);
        println!("Magic: 0x{:08x} ({})", magic,
            if magic == 0x377f0682 { "big-endian" } else { "little-endian" });
        println!("Format version: {}", u32::from_be_bytes([
            header[4], header[5], header[6], header[7]
        ]));
        println!("Page size: {}", self.page_size);
        println!("Checkpoint sequence: {}", u32::from_be_bytes([
            header[12], header[13], header[14], header[15]
        ]));
        println!("Salt-1: {}", u32::from_be_bytes([
            header[16], header[17], header[18], header[19]
        ]));
        println!("Salt-2: {}", u32::from_be_bytes([
            header[20], header[21], header[22], header[23]
        ]));
        println!("Checksum-1: {}", u32::from_be_bytes([
            header[24], header[25], header[26], header[27]
        ]));
        println!("Checksum-2: {}", u32::from_be_bytes([
            header[28], header[29], header[30], header[31]
        ]));

        Ok(())
    }

    /// Show WAL frames
    pub fn show_frames(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(32))?;

        let frame_size = 24 + self.page_size;
        let mut frame_header = [0u8; 24];
        let mut frame_num = 0;

        loop {
            if self.file.read_exact(&mut frame_header).is_err() {
                break;
            }
            frame_num += 1;

            let pgno = u32::from_be_bytes([
                frame_header[0], frame_header[1], frame_header[2], frame_header[3]
            ]);
            let db_size = u32::from_be_bytes([
                frame_header[4], frame_header[5], frame_header[6], frame_header[7]
            ]);
            let salt1 = u32::from_be_bytes([
                frame_header[8], frame_header[9], frame_header[10], frame_header[11]
            ]);
            let salt2 = u32::from_be_bytes([
                frame_header[12], frame_header[13], frame_header[14], frame_header[15]
            ]);

            println!("Frame {}: page={}, db_size={}, salt=({}, {})",
                frame_num, pgno, db_size, salt1, salt2);

            // Skip page data
            self.file.seek(SeekFrom::Current(self.page_size as i64))?;
        }

        println!("Total frames: {}", frame_num);
        Ok(())
    }
}
```

## dbhash - Content Hash

```rust
/// Database content hash
pub fn dbhash(path: &str) -> Result<String> {
    let db = Connection::open(path)?;

    let mut hasher = Sha1::new();

    // Get table list
    let mut stmt = db.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )?;

    let mut tables = Vec::new();
    while stmt.step()? == StepResult::Row {
        tables.push(stmt.column_text(0)?);
    }

    // Hash each table's content
    for table in &tables {
        // Hash table name
        hasher.update(table.as_bytes());

        // Hash all rows
        let sql = format!("SELECT * FROM \"{}\" ORDER BY rowid", table);
        let mut stmt = db.prepare(&sql)?;

        while stmt.step()? == StepResult::Row {
            let n_cols = stmt.column_count();
            for i in 0..n_cols {
                let val = stmt.column_value(i)?;
                match val {
                    Value::Null => hasher.update(b"N"),
                    Value::Integer(i) => hasher.update(&i.to_le_bytes()),
                    Value::Float(f) => hasher.update(&f.to_le_bytes()),
                    Value::Text(s) => hasher.update(s.as_bytes()),
                    Value::Blob(b) => hasher.update(&b),
                }
            }
        }
    }

    let hash = hasher.finalize();
    Ok(hex::encode(hash))
}

/// Compare hashes of two databases
pub fn dbhash_compare(path1: &str, path2: &str) -> Result<bool> {
    let hash1 = dbhash(path1)?;
    let hash2 = dbhash(path2)?;
    Ok(hash1 == hash2)
}
```

## showshm - Shared Memory Inspector

```rust
/// SHM file inspector
pub struct ShowShm {
    path: String,
    data: Vec<u8>,
}

impl ShowShm {
    pub fn open(path: &str) -> Result<Self> {
        let data = std::fs::read(path)?;
        Ok(Self {
            path: path.to_string(),
            data,
        })
    }

    pub fn show(&self) -> Result<()> {
        println!("SHM: {}", self.path);
        println!("Size: {} bytes", self.data.len());

        if self.data.len() < 136 {
            println!("(too small for WAL index header)");
            return Ok(());
        }

        // WAL index header
        println!("\nWAL Index Header:");
        println!("  Version: {}", u32::from_le_bytes(
            self.data[0..4].try_into().unwrap()
        ));
        println!("  Unused: {}", u32::from_le_bytes(
            self.data[4..8].try_into().unwrap()
        ));
        println!("  Change counter: {}", u32::from_le_bytes(
            self.data[8..12].try_into().unwrap()
        ));
        println!("  Initialized: {}", self.data[12]);
        println!("  Big-endian checksum: {}", self.data[13]);

        // Read/write lock info
        println!("\nLock Info:");
        println!("  Read mark 0: {}", u32::from_le_bytes(
            self.data[16..20].try_into().unwrap()
        ));
        println!("  Read mark 1: {}", u32::from_le_bytes(
            self.data[20..24].try_into().unwrap()
        ));
        println!("  Read mark 2: {}", u32::from_le_bytes(
            self.data[24..28].try_into().unwrap()
        ));
        println!("  Read mark 3: {}", u32::from_le_bytes(
            self.data[28..32].try_into().unwrap()
        ));
        println!("  Read mark 4: {}", u32::from_le_bytes(
            self.data[32..36].try_into().unwrap()
        ));

        Ok(())
    }
}
```

## showjournal - Journal Inspector

```rust
/// Journal file inspector
pub struct ShowJournal {
    path: String,
    file: File,
}

impl ShowJournal {
    pub fn open(path: &str) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            path: path.to_string(),
            file,
        })
    }

    pub fn show(&mut self) -> Result<()> {
        let mut header = [0u8; 28];
        self.file.read_exact(&mut header)?;

        // Check magic
        let magic = &header[0..8];
        let expected_magic = [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7];

        println!("Journal: {}", self.path);

        if magic != expected_magic {
            println!("Invalid magic header");
            return Ok(());
        }

        let page_count = u32::from_be_bytes([
            header[8], header[9], header[10], header[11]
        ]);
        let random_nonce = u32::from_be_bytes([
            header[12], header[13], header[14], header[15]
        ]);
        let initial_size = u32::from_be_bytes([
            header[16], header[17], header[18], header[19]
        ]);
        let sector_size = u32::from_be_bytes([
            header[20], header[21], header[22], header[23]
        ]);
        let page_size = u32::from_be_bytes([
            header[24], header[25], header[26], header[27]
        ]);

        println!("Page count: {}", page_count);
        println!("Random nonce: {}", random_nonce);
        println!("Initial DB size: {} pages", initial_size);
        println!("Sector size: {} bytes", sector_size);
        println!("Page size: {} bytes", page_size);

        // Show page records
        let record_size = 4 + page_size + 4; // pgno + data + checksum
        let mut record_num = 0;

        loop {
            let mut record = vec![0u8; record_size as usize];
            if self.file.read_exact(&mut record).is_err() {
                break;
            }
            record_num += 1;

            let pgno = u32::from_be_bytes([
                record[0], record[1], record[2], record[3]
            ]);

            if pgno == 0 {
                break;
            }

            println!("Record {}: page {}", record_num, pgno);
        }

        println!("Total records: {}", record_num);
        Ok(())
    }
}
```

## CLI Entry Points

```rust
pub fn showdb_main(args: &[String]) -> Result<i32> {
    let path = args.get(1).ok_or_else(|| {
        Error::with_message(ErrorCode::Error, "usage: showdb DATABASE [PAGE]")
    })?;

    let mut db = ShowDb::open(path)?;

    if let Some(page_str) = args.get(2) {
        let pgno: u32 = page_str.parse()?;
        db.show_page(pgno)?;
        db.dump_page(pgno)?;
    } else {
        db.show_header()?;
    }

    Ok(0)
}

pub fn showwal_main(args: &[String]) -> Result<i32> {
    let path = args.get(1).ok_or_else(|| {
        Error::with_message(ErrorCode::Error, "usage: showwal WAL-FILE")
    })?;

    let mut wal = ShowWal::open(path)?;
    wal.show_header()?;
    wal.show_frames()?;

    Ok(0)
}

pub fn dbhash_main(args: &[String]) -> Result<i32> {
    let path = args.get(1).ok_or_else(|| {
        Error::with_message(ErrorCode::Error, "usage: dbhash DATABASE")
    })?;

    let hash = dbhash(path)?;
    println!("{}", hash);

    Ok(0)
}
```

## Acceptance Criteria
- [ ] showdb database header display
- [ ] showdb page inspection
- [ ] showdb hex dump
- [ ] showwal WAL header display
- [ ] showwal frame listing
- [ ] showjournal journal inspection
- [ ] showshm shared memory inspection
- [ ] dbhash content hashing
- [ ] dbhash comparison
- [ ] CLI argument handling
- [ ] Error handling for corrupt files

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `showdb.test` - Database inspector tests (if exists)
- `showwal.test` - WAL inspector tests (if exists)
- `dbhash.test` - Database hash tests (if exists)
