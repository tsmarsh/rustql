# Implement BTREE_PREFORMAT Cell Support in Insert

## Problem

The current insert function (btree.rs lines 2511-2512) explicitly rejects the BTREE_PREFORMAT flag:

```rust
if flags.contains(BtreeInsertFlags::PREFORMAT) {
    return Err(Error::with_message(ErrorCode::Internal, "PREFORMAT not implemented"));
}
```

SQLite uses PREFORMAT for bulk loading and index building where the cell is pre-constructed, avoiding redundant serialization.

## SQLite3 Reference

### Key Files
- `sqlite3/src/btree.c` - sqlite3BtreeInsert() (~9370)
- `sqlite3/src/btree.h` - BTREE_PREFORMAT flag definition

### BTREE_PREFORMAT Flag (btree.h ~300)
```c
#define BTREE_PREFORMAT  0x80  /* Cell is already formatted */
```

### Insert with PREFORMAT (btree.c ~9400)
```c
int sqlite3BtreeInsert(
  BtCursor *pCur,
  const BtreePayload *pX,  /* Content of the cell */
  int flags,               /* BTREE_APPEND, BTREE_PREFORMAT, etc */
  int seekResult           /* Hint from prior seek operation */
){
  // ...
  if( flags & BTREE_PREFORMAT ){
    /* Cell is pre-formatted in pCur->pPage->aData at offset pCur->info.nLocal */
    pCell = pCur->pPage->aData + pCur->info.nLocal;
    nCell = pCur->info.nSize;
    // Skip serialization, use pre-formatted cell directly
  }else{
    // Normal path: build cell from payload
    pCell = aSpace;
    nCell = buildCell(...);
  }
  // ...
}
```

### Use Cases
1. **Bulk index creation**: CREATE INDEX builds cells in bulk
2. **Index updates**: After UPDATE, index cells can be pre-formatted
3. **REINDEX**: Rebuilds entire index efficiently
4. **Optimized inserts**: When caller already has serialized data

### Pre-formatting Location
The pre-formatted cell is stored at:
- `pCur->pPage->aData + pCur->info.nLocal` - Start of cell data
- `pCur->info.nSize` - Cell size

## Current Rust Implementation

```rust
// src/storage/btree.rs lines 2504-2682
pub fn insert(..., flags: BtreeInsertFlags, ...) -> Result<()> {
    if flags.contains(BtreeInsertFlags::PREFORMAT) {
        return Err(Error::with_message(ErrorCode::Internal, "PREFORMAT not implemented"));
    }
    // ... normal cell building
}
```

## Required Changes

### 1. Add Preformatted Cell Storage to Cursor
```rust
pub struct BtCursor {
    // ... existing fields
    preformat_cell: Option<Vec<u8>>,  // Pre-formatted cell data
}
```

### 2. Add Method to Set Preformatted Cell
```rust
impl BtCursor {
    pub fn set_preformat_cell(&mut self, cell: Vec<u8>) {
        self.preformat_cell = Some(cell);
    }
}
```

### 3. Modify Insert to Use Preformatted Cell
```rust
pub fn insert(..., flags: BtreeInsertFlags, ...) -> Result<()> {
    let cell = if flags.contains(BtreeInsertFlags::PREFORMAT) {
        // Use pre-formatted cell from cursor
        cursor.preformat_cell.take()
            .ok_or_else(|| Error::with_message(
                ErrorCode::Internal,
                "PREFORMAT flag set but no preformatted cell"
            ))?
    } else {
        // Build cell normally
        self.build_cell(page, key, data)?
    };

    self.insert_cell(page, idx, &cell)?;
    Ok(())
}
```

## Unit Tests Required

### Test 1: Basic preformat insert
```rust
#[test]
fn test_preformat_insert_basic() {
    let mut btree = setup_btree();
    let mut cursor = btree.cursor();

    // Build cell manually
    let cell = build_table_cell(rowid: 1, data: b"hello");
    cursor.set_preformat_cell(cell.clone());

    // Insert with PREFORMAT
    btree.insert(&mut cursor, 1, &[], BtreeInsertFlags::PREFORMAT, 0).unwrap();

    // Verify data
    cursor.move_to(1).unwrap();
    assert_eq!(cursor.data().unwrap(), b"hello");
}
```

### Test 2: Preformat without cell fails
```rust
#[test]
fn test_preformat_without_cell_fails() {
    let mut btree = setup_btree();
    let mut cursor = btree.cursor();

    // Don't set preformat cell
    let result = btree.insert(&mut cursor, 1, &[], BtreeInsertFlags::PREFORMAT, 0);
    assert!(result.is_err());
    assert!(result.unwrap_err().message().contains("no preformatted cell"));
}
```

### Test 3: Bulk index creation
```rust
#[test]
fn test_preformat_bulk_index() {
    let mut btree = setup_index_btree();
    let mut cursor = btree.cursor();

    // Simulate CREATE INDEX bulk loading
    let keys = vec![b"alice", b"bob", b"charlie"];
    for key in keys {
        let cell = build_index_cell(key, rowid: 1);
        cursor.set_preformat_cell(cell);
        btree.insert(&mut cursor, key, &[], BtreeInsertFlags::PREFORMAT, 0).unwrap();
    }

    // Verify all entries
    cursor.first().unwrap();
    assert_eq!(cursor.key().unwrap(), b"alice");
}
```

### Test 4: Preformat with overflow
```rust
#[test]
fn test_preformat_with_overflow() {
    let mut btree = setup_btree();
    let mut cursor = btree.cursor();

    // Build cell with overflow pages already allocated
    let large_data = vec![0u8; 10000];
    let cell = build_cell_with_overflow(&mut btree, 1, &large_data);
    cursor.set_preformat_cell(cell);

    btree.insert(&mut cursor, 1, &[], BtreeInsertFlags::PREFORMAT, 0).unwrap();

    // Verify large data readable
    cursor.move_to(1).unwrap();
    assert_eq!(cursor.data().unwrap(), large_data);
}
```

### Test 5: Cell consumed after insert
```rust
#[test]
fn test_preformat_cell_consumed() {
    let mut btree = setup_btree();
    let mut cursor = btree.cursor();

    let cell = build_table_cell(1, b"data");
    cursor.set_preformat_cell(cell);

    btree.insert(&mut cursor, 1, &[], BtreeInsertFlags::PREFORMAT, 0).unwrap();

    // Cell should be consumed (taken)
    assert!(cursor.preformat_cell.is_none());
}
```

## Acceptance Criteria

- [ ] PREFORMAT flag no longer returns error
- [ ] Pre-formatted cells can be set on cursor
- [ ] Insert uses pre-formatted cell when flag set
- [ ] Cell is consumed after insert
- [ ] Works with overflow pages
- [ ] All unit tests pass
