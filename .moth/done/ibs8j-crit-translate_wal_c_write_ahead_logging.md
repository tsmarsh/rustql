# Translate wal.c - Write-Ahead Logging

## Overview
Translate the Write-Ahead Logging (WAL) implementation which provides improved concurrency and performance compared to rollback journal mode. WAL allows concurrent readers and a single writer.

## Source Reference
- `sqlite3/src/wal.c` - 4,621 lines
- `sqlite3/src/wal.h` - 160 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Wal
Main WAL connection structure:
```rust
pub struct Wal {
    // File handles
    vfs: Arc<dyn Vfs>,              // Virtual file system
    db_fd: Arc<dyn VfsFile>,        // Database file
    wal_fd: Option<Box<dyn VfsFile>>, // WAL file

    // Identity
    db_path: String,                 // Database path

    // WAL state
    read_lock: i16,                  // Read lock index (-1 = none)
    write_lock: bool,                // Holding write lock
    ckpt_lock: bool,                 // Holding checkpoint lock

    // Size tracking
    header: WalIndexHdr,             // WAL-index header
    max_frame: u32,                  // Max valid frame number
    n_ckpt: u32,                     // Checkpoint counter

    // Page size
    page_size: u32,                  // Database page size

    // Checksum
    checksum_init: [u32; 2],         // Checksum seed values

    // Shared memory (WAL-index)
    shm: WalShm,                     // Shared memory mapping

    // Callbacks
    busy_handler: Option<Box<dyn Fn() -> bool>>,
}
```

### WalIndexHdr
WAL index header (in shared memory):
```rust
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct WalIndexHdr {
    pub version: u32,           // WAL-index format version
    pub unused: u32,            // Unused padding
    pub change: u32,            // Change counter
    pub is_init: u8,            // True after initialization
    pub big_endian_cksum: u8,   // Checksum is big-endian
    pub page_size: u16,         // Database page size
    pub max_frame: u32,         // Last valid frame
    pub n_page: u32,            // Database size in pages
    pub frame_cksum: [u32; 2],  // Checksum of last frame
    pub salt: [u32; 2],         // Salt values (random)
    pub cksum: [u32; 2],        // Checksum of this header
}
```

### WalFrame
Individual WAL frame (24-byte header + page data):
```rust
#[repr(C)]
pub struct WalFrameHdr {
    pub pgno: u32,              // Page number
    pub n_truncate: u32,        // Database size after commit (or 0)
    pub salt: [u32; 2],         // Salt values (must match WAL header)
    pub checksum: [u32; 2],     // Cumulative checksum
}
```

### WalShm
Shared memory for WAL index:
```rust
pub struct WalShm {
    regions: Vec<ShmRegion>,     // Shared memory regions
    n_region: usize,             // Number of regions
    read_marks: [u32; WAL_NREADER], // Read marks for each reader
}

const WAL_NREADER: usize = 5;    // Max concurrent readers + 1
```

## Key Functions

### Initialization
- `sqlite3WalOpen()` - Open WAL for a database
- `sqlite3WalClose()` - Close WAL connection
- `walIndexRecover()` - Recover WAL index from WAL file

### Reading
- `sqlite3WalBeginReadTransaction()` - Start read transaction
- `sqlite3WalEndReadTransaction()` - End read transaction
- `sqlite3WalFindFrame()` - Find frame for a page
- `sqlite3WalReadFrame()` - Read frame content

### Writing
- `sqlite3WalBeginWriteTransaction()` - Start write transaction
- `sqlite3WalEndWriteTransaction()` - End write transaction
- `sqlite3WalFrames()` - Write frames to WAL
- `sqlite3WalUndo()` - Undo write transaction

### Checkpointing
- `sqlite3WalCheckpoint()` - Run checkpoint
- `walCheckpoint()` - Internal checkpoint implementation
- `walIteratorInit()` - Initialize page iterator for checkpoint

## WAL File Format

### WAL Header (32 bytes)
```
Offset  Size  Description
0       4     Magic number (0x377f0682 or 0x377f0683)
4       4     File format version (3007000)
8       4     Database page size
12      4     Checkpoint sequence number
16      4     Salt-1 (random)
20      4     Salt-2 (random)
24      4     Checksum-1 of header
28      4     Checksum-2 of header
```

### Frame Format (24 byte header + page)
```
Offset  Size  Description
0       4     Page number
4       4     Commit marker (db size after commit, else 0)
8       4     Salt-1 (must match header)
12      4     Salt-2 (must match header)
16      4     Checksum-1 (cumulative)
20      4     Checksum-2 (cumulative)
24      N     Page data (page_size bytes)
```

## WAL Index (Shared Memory)

### Structure
```
Region 0: Header + hash table 0
  - Two copies of WalIndexHdr (for lock-free reading)
  - Hash table mapping page numbers to frames

Region 1-N: Additional hash tables
  - Each region covers 4096 frames
  - Hash table: page_number -> frame_number
```

### Hash Table
- 8192 entries per region (HASHTABLE_NSLOT)
- Uses page number as key
- Linear probing for collisions
- Stores frame numbers

## Checkpoint Modes

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointMode {
    Passive = 0,   // Checkpoint without blocking
    Full = 1,      // Wait for readers, checkpoint all
    Restart = 2,   // Full + reset WAL file
    Truncate = 3,  // Full + truncate WAL to zero
}
```

## Concurrency Model

1. **Readers** acquire read lock on WAL-index
   - Can proceed without blocking writers
   - See snapshot at their read mark

2. **Writer** acquires write lock
   - Only one writer at a time
   - Appends to WAL file

3. **Checkpointer** copies WAL frames to database
   - Can run concurrently with readers
   - Blocks on pages still being read

## Rust Translation Considerations

### Shared Memory
- Use platform-specific shared memory APIs
- Consider memory-mapped file as fallback
- Handle lock-free header reads with atomics

### Checksums
- Implement SQLite's checksum algorithm exactly
- Handle both big and little endian modes

### Locking
- WAL uses shared memory locks
- Multiple reader slots
- Exclusive write and checkpoint locks

### Recovery
- Must handle partial WAL writes
- Validate checksums during recovery
- Rebuild index from WAL file

## Dependencies
- `pager.rs` - Pager integration
- `os/*.rs` - VFS and shared memory

## Acceptance Criteria
- [ ] Wal struct with shared memory support
- [ ] WAL file creation and header writing
- [ ] Frame writing with checksums
- [ ] Read transaction with snapshots
- [ ] Write transaction support
- [ ] Checkpoint (passive, full, restart, truncate)
- [ ] Recovery from crash
- [ ] Concurrent readers working
- [ ] WAL-index hash table lookups
