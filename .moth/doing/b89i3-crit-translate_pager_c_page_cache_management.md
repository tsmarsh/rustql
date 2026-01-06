# Translate pager.c - Page Cache Management

## Overview
Translate the Pager module which manages the page cache, handles database I/O, journaling, and transaction durability. This is the layer between the B-tree and the OS file system.

## Source Reference
- `sqlite3/src/pager.c` - 7,830 lines
- `sqlite3/src/pager.h` - 263 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Pager
Main pager object managing a database file:
```rust
pub struct Pager {
    // File handles
    fd: Box<dyn VfsFile>,           // Database file
    jfd: Option<Box<dyn VfsFile>>,  // Journal file

    // State
    state: PagerState,              // Current state
    lock: LockLevel,                // Current lock level
    journal_mode: JournalMode,      // DELETE, TRUNCATE, PERSIST, WAL, etc.

    // Page management
    page_size: u32,                 // Database page size
    db_size: Pgno,                  // Database size in pages
    db_file_size: Pgno,             // Actual file size in pages
    cache: PCache,                  // Page cache

    // Journal state
    journal_offset: i64,            // Current position in journal
    journal_header: i64,            // Start of current header
    n_rec: u32,                     // Records in current journal segment

    // WAL state (if WAL mode)
    wal: Option<Wal>,               // Write-ahead log

    // Stats and options
    n_read: u32,                    // Pages read
    n_write: u32,                   // Pages written
    sync_flags: SyncFlags,          // Sync mode
    temp_file: bool,                // Is this a temp database
    no_sync: bool,                  // Disable syncs (unsafe)

    // Savepoints
    savepoints: Vec<Savepoint>,     // Active savepoints
}
```

### PagerState
Pager state machine states:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PagerState {
    Open = 0,          // No lock held
    Reader = 1,        // Shared lock, can read
    Writer = 2,        // Reserved lock, writing
    WriterLocked = 3,  // Exclusive, committing
    WriterFinished = 4, // Committed, releasing
    Error = 5,         // Error occurred
}
```

### JournalMode
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    Delete = 0,     // Delete journal after commit
    Persist = 1,    // Zero journal header
    Off = 2,        // No journal (unsafe)
    Truncate = 3,   // Truncate journal to zero
    Memory = 4,     // In-memory journal
    Wal = 5,        // Write-ahead logging
}
```

### PgHdr
Page header for cached pages:
```rust
pub struct PgHdr {
    pub pgno: Pgno,              // Page number
    pub data: Vec<u8>,           // Page content
    pub flags: PgFlags,          // Page state flags
    pub dirty: bool,             // Page has been modified
    pub n_ref: u32,              // Reference count
    pub extra: Option<Box<MemPage>>, // B-tree page info
}

bitflags! {
    pub struct PgFlags: u8 {
        const CLEAN = 0x00;
        const DIRTY = 0x01;
        const DONT_WRITE = 0x02;
        const NEED_SYNC = 0x04;
        const WRITEABLE = 0x08;
    }
}
```

## Key Functions

### Initialization
- `sqlite3PagerOpen()` - Open pager on database file
- `sqlite3PagerClose()` - Close pager and release resources
- `sqlite3PagerSetPagesize()` - Set/change page size
- `sqlite3PagerReadFileheader()` - Read database header

### Page Acquisition
- `sqlite3PagerGet()` - Get a page (read from disk if needed)
- `sqlite3PagerLookup()` - Get page only if cached
- `sqlite3PagerRef()` - Increment page reference
- `sqlite3PagerUnref()` - Decrement page reference
- `sqlite3PagerWrite()` - Mark page as writable

### Transaction Control
- `sqlite3PagerBegin()` - Begin a write transaction
- `sqlite3PagerCommitPhaseOne()` - Sync journal
- `sqlite3PagerCommitPhaseTwo()` - Finalize commit
- `sqlite3PagerRollback()` - Rollback transaction
- `sqlite3PagerSavepoint()` - Create savepoint
- `sqlite3PagerSavepointUndo()` - Rollback to savepoint

### Journal Operations
- `pager_open_journal()` - Create journal file
- `pager_write_pagelist()` - Write dirty pages to journal
- `pager_playback()` - Replay journal on recovery
- `pager_truncate()` - Truncate database file

### Sync and Durability
- `sqlite3PagerSync()` - Sync database to disk
- `pager_wait_on_lock()` - Wait for lock
- `pagerUnlockDb()` - Release database lock

## Page Lifecycle

```
   [Not Cached]
        |
        v (PagerGet)
   [Clean/Cached] <----+
        |              |
        v (PagerWrite) | (Commit/Rollback)
   [Dirty/Cached] -----+
        |
        v (Write to journal)
   [In Journal]
        |
        v (Commit)
   [Written to DB]
```

## Journal Format

### Rollback Journal Header (28 bytes)
```
Offset  Size  Description
0       8     Magic number
8       4     Page count in this segment
12      4     Random nonce for checksum
16      4     Initial database page count
20      4     Disk sector size
24      4     Page size
```

### Journal Page Record
```
Offset  Size  Description
0       4     Page number
4       N     Page content (page size bytes)
N+4     4     Checksum
```

## Rust Translation Considerations

### File Operations
- Use VFS trait for all file I/O
- Handle partial reads/writes properly
- Implement proper fsync behavior

### Memory Management
- Page cache should use LRU eviction
- Reference counting for page lifetime
- Consider memory-mapped I/O option

### Concurrency
- Lock management must match SQLite semantics
- Handle lock contention with busy handler
- Support concurrent readers with WAL

### Error Handling
- Pager can enter ERROR state
- Recovery path must work from ERROR state
- Proper cleanup on errors

## Dependencies
- `pcache.rs` - Underlying page cache
- `wal.rs` - Write-ahead logging support
- `os/*.rs` - VFS implementation

## Acceptance Criteria
- [ ] Pager struct with all necessary fields
- [ ] Page acquisition (get/lookup/ref/unref) working
- [ ] Transaction begin/commit/rollback working
- [ ] Rollback journal creation and playback
- [ ] Savepoint support
- [ ] Lock management (shared/reserved/exclusive)
- [ ] WAL mode integration point
- [ ] Proper sync behavior for durability
- [ ] Recovery from crash with journal
