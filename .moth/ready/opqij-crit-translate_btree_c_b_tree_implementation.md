# Translate btree.c - B-tree Implementation

## Overview
Translate the core B-tree implementation which is the fundamental data structure for SQLite's storage engine. This handles all database page organization, indexing, and record storage.

## Source Reference
- `sqlite3/src/btree.c` - 11,544 lines
- `sqlite3/src/btree.h` - Public interface (425 lines)

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### BtShared
Shared B-tree state across all connections to the same database file:
```rust
pub struct BtShared {
    pager: Pager,              // Page cache manager
    db: Weak<Connection>,      // Database connection
    cursor_list: Vec<BtCursor>, // All open cursors
    page1: Option<MemPage>,    // First page of database
    page_size: u32,            // Database page size
    usable_size: u32,          // Usable bytes per page
    n_transaction: i32,        // Number of open transactions
    in_transaction: TransState, // Transaction state
    schema_cookie: u32,        // Schema version
    file_format: u8,           // File format version
    // ... more fields
}
```

### BtCursor
B-tree cursor for traversing and modifying data:
```rust
pub struct BtCursor {
    btree: Arc<Btree>,         // B-tree this cursor belongs to
    root_page: Pgno,           // Root page of table/index
    page_stack: Vec<MemPage>,  // Stack of pages from root to current
    idx_stack: Vec<u16>,       // Cell index on each page
    state: CursorState,        // CURSOR_INVALID, CURSOR_VALID, etc.
    hints: CursorHints,        // Optimization hints
    key: Option<Vec<u8>>,      // Saved key for repositioning
    // ... more fields
}
```

### MemPage
In-memory representation of a database page:
```rust
pub struct MemPage {
    pgno: Pgno,                // Page number
    data: Vec<u8>,             // Raw page data
    is_init: bool,             // True if initialized
    is_leaf: bool,             // True for leaf pages
    is_intkey: bool,           // True for integer key tables
    n_cell: u16,               // Number of cells on page
    cell_offset: u16,          // Offset to cell pointers
    free_bytes: u16,           // Free bytes on page
    n_overflow: u8,            // Number of overflow cells
    // ... more fields
}
```

## Key Functions to Translate

### Initialization & Configuration
- `sqlite3BtreeOpen()` - Open a database file
- `sqlite3BtreeClose()` - Close a database
- `sqlite3BtreeSetPageSize()` - Set page size
- `sqlite3BtreeGetPageSize()` - Get page size
- `sqlite3BtreeSetAutoVacuum()` - Configure auto-vacuum

### Transaction Management
- `sqlite3BtreeBeginTrans()` - Begin a transaction
- `sqlite3BtreeCommit()` - Commit transaction
- `sqlite3BtreeRollback()` - Rollback transaction
- `sqlite3BtreeSavepoint()` - Create savepoint
- `sqlite3BtreeIncrVacuum()` - Incremental vacuum

### Cursor Operations
- `sqlite3BtreeCursor()` - Create a cursor
- `sqlite3BtreeCloseCursor()` - Close cursor
- `sqlite3BtreeFirst()` - Move to first entry
- `sqlite3BtreeLast()` - Move to last entry
- `sqlite3BtreeNext()` - Move to next entry
- `sqlite3BtreePrevious()` - Move to previous entry
- `sqlite3BtreeMovetoUnpacked()` - Seek to specific key

### Data Operations
- `sqlite3BtreeInsert()` - Insert a record
- `sqlite3BtreeDelete()` - Delete current record
- `sqlite3BtreeKey()` - Get current key
- `sqlite3BtreeData()` - Get current data
- `sqlite3BtreePayload()` - Get payload bytes

### Table/Index Management
- `sqlite3BtreeCreateTable()` - Create new table
- `sqlite3BtreeDropTable()` - Drop a table
- `sqlite3BtreeClearTable()` - Delete all rows

### Internal Page Operations
- `allocateBtreePage()` - Allocate a new page
- `freePage()` - Free a page
- `balance()` - Rebalance pages after insert/delete
- `fillInCell()` - Format a cell for insertion
- `dropCell()` - Remove a cell from a page
- `insertCell()` - Insert a cell into a page

## Rust Translation Considerations

### Memory Safety
- Replace raw pointers with `Arc<>`, `Rc<>`, `RefCell<>`
- Use slices instead of pointer arithmetic for page data
- Implement proper RAII for cursor cleanup

### Error Handling
- Convert all error codes to `Result<T, Error>`
- Propagate errors with `?` operator
- Handle OOM conditions properly

### Concurrency
- Use `RwLock` for shared B-tree state
- Implement proper lock ordering to avoid deadlocks
- Consider lock-free cursors where possible

### Page Layout
SQLite page format must be preserved exactly:
- Page header (8 or 12 bytes)
- Cell pointer array
- Unallocated space
- Cell content area
- Reserved region

## Dependencies
- `pager.rs` - Page cache management
- `pcache.rs` - Page cache implementation
- `wal.rs` - Write-ahead logging (for WAL mode)

## Testing
- Unit tests for page manipulation
- B-tree invariant checks
- Cursor navigation tests
- Insert/delete/balance tests
- Concurrent access tests

## Acceptance Criteria
- [ ] BtShared, BtCursor, MemPage structs defined
- [ ] Open/close database working
- [ ] Transaction begin/commit/rollback working
- [ ] Cursor creation and navigation working
- [ ] Insert and delete operations working
- [ ] Page balancing after modifications
- [ ] Overflow page handling
- [ ] Compatible with SQLite database format
