# Implement Auto-Vacuum and Incremental Vacuum

## Problem

SQLite supports auto-vacuum and incremental vacuum modes to automatically reclaim space from deleted pages. Without this, databases grow monotonically - freed pages are reused but the file never shrinks.

Currently rustql has no vacuum support.

## SQLite3 Reference

### Key Files
- `sqlite3/src/btree.c` - Vacuum implementation (~4000-4400)
- `sqlite3/src/btreeInt.h` - Pointer map structures
- `sqlite3/src/vacuum.c` - VACUUM command

### Database Header Fields (offset)
- **52**: Auto-vacuum mode (0=none, 1=full, 2=incremental)
- **64**: Incremental vacuum mode flag

### Auto-Vacuum Modes
```c
#define BTREE_AUTOVACUUM_NONE 0      /* No auto-vacuum */
#define BTREE_AUTOVACUUM_FULL 1      /* Full auto-vacuum on commit */
#define BTREE_AUTOVACUUM_INCR 2      /* Incremental (manual trigger) */
```

### Pointer Map (btreeInt.h ~280)
Auto-vacuum requires a "pointer map" that tracks the parent of each page:
```
For each page in the database (except page 1), there is a 5-byte entry:
- Byte 0: Page type (PTRMAP_ROOTPAGE, PTRMAP_FREEPAGE, PTRMAP_OVERFLOW1, etc.)
- Bytes 1-4: Parent page number (for btree pages) or previous overflow page

Pointer map pages occur at regular intervals:
  First ptrmap page: page 2
  Subsequent: every (usableSize/5 + 1) pages
```

### Page Types in Pointer Map
```c
#define PTRMAP_ROOTPAGE    1   /* Root page of a btree */
#define PTRMAP_FREEPAGE    2   /* Page on the freelist */
#define PTRMAP_OVERFLOW1   3   /* First page of an overflow chain */
#define PTRMAP_OVERFLOW2   4   /* Subsequent overflow page */
#define PTRMAP_BTREE       5   /* Non-root btree page */
```

### Key Functions

#### incrVacuumStep() (btree.c ~4100)
```c
/*
** Perform a single step of incremental vacuum.
** Move one page from the end of the file to a free slot earlier in the file.
** Returns SQLITE_DONE when vacuum is complete.
*/
static int incrVacuumStep(BtShared *pBt, Pgno nFin, Pgno iLastPg, int bCommit){
  Pgno nFreeList = get4byte(&pBt->pPage1->aData[36]);  /* Free page count */

  if( nFreeList==0 ){
    return SQLITE_DONE;  /* Nothing to vacuum */
  }

  /* Find a page to move (last page in file) */
  Pgno iLastPg = btreePagecount(pBt);

  /* Get page type from pointer map */
  u8 eType;
  Pgno iPtrPage;
  ptrmapGet(pBt, iLastPg, &eType, &iPtrPage);

  if( eType==PTRMAP_FREEPAGE ){
    /* Last page is free - just truncate */
    // ...
  }else{
    /* Move page content to a free page earlier in file */
    Pgno iFreePg = get4byte(&pBt->pPage1->aData[32]);  /* First free page */
    relocatePage(pBt, iLastPg, eType, iPtrPage, iFreePg);
    // Update pointer map
    // Update parent pointers
  }

  /* Truncate file */
  sqlite3PagerTruncateImage(pBt->pPager, iLastPg-1);
  return SQLITE_OK;
}
```

#### autoVacuumCommit() (btree.c ~4200)
```c
/*
** Called during commit if auto-vacuum is enabled.
** Relocates all pages to fill gaps, then truncates file.
*/
static int autoVacuumCommit(BtShared *pBt){
  Pgno nFreeList = get4byte(&pBt->pPage1->aData[36]);
  Pgno nPtrmap = /* calculate ptrmap pages */;
  Pgno nFin = nOrig - nFreeList - nPtrmap;

  while( btreePagecount(pBt) > nFin ){
    rc = incrVacuumStep(pBt, nFin, 0, 1);
    if( rc!=SQLITE_OK ) break;
  }

  return sqlite3PagerTruncateImage(pBt->pPager, nFin);
}
```

#### relocatePage() (btree.c ~3900)
```c
/*
** Move page iDbPage to page iFreePage.
** Update all pointers (parent, children, overflow chains).
*/
static int relocatePage(
  BtShared *pBt,
  Pgno iDbPage,       /* Page to move */
  u8 eType,           /* Type from pointer map */
  Pgno iPtrPage,      /* Parent page */
  Pgno iFreePage      /* Destination */
){
  // 1. Copy page content
  // 2. Update parent's child pointer
  // 3. Update children's parent pointers (for interior pages)
  // 4. Update overflow chain links
  // 5. Update pointer map
  // 6. Free old page
}
```

## Current Rust Implementation

No vacuum support. No pointer map. Freed pages tracked only in memory.

## Required Changes

### 1. Add Auto-Vacuum Mode to Database Header
```rust
pub struct DbHeader {
    // ... existing fields
    auto_vacuum: u32,        // Offset 52: 0=none, 1=full, 2=incremental
    incr_vacuum: u32,        // Offset 64: incremental mode flag
}
```

### 2. Implement Pointer Map
```rust
pub struct PointerMap {
    page_size: u32,
}

impl PointerMap {
    /// Get pointer map page number for a given page
    pub fn ptrmap_pageno(&self, pgno: Pgno) -> Pgno {
        let entries_per_page = (self.page_size / 5) as Pgno;
        let ptrmap_pages = (pgno - 1) / entries_per_page;
        2 + ptrmap_pages * (entries_per_page + 1)
    }

    /// Read entry for a page
    pub fn get(&self, pgno: Pgno) -> Result<(u8, Pgno)>;

    /// Write entry for a page
    pub fn put(&mut self, pgno: Pgno, ptype: u8, parent: Pgno) -> Result<()>;
}
```

### 3. Implement Incremental Vacuum Step
```rust
impl Btree {
    /// Perform one step of incremental vacuum.
    /// Returns true if more work remains, false if done.
    pub fn incr_vacuum_step(&mut self) -> Result<bool> {
        // 1. Check if any free pages
        // 2. Find last page in file
        // 3. Get page type from pointer map
        // 4. If free page, just truncate
        // 5. Otherwise, relocate to earlier free slot
        // 6. Update pointer map
        // 7. Truncate file
    }
}
```

### 4. Implement Auto-Vacuum on Commit
```rust
impl Btree {
    pub fn commit(&mut self) -> Result<()> {
        if self.auto_vacuum == AUTOVACUUM_FULL {
            self.auto_vacuum_commit()?;
        }
        self.pager.commit()
    }

    fn auto_vacuum_commit(&mut self) -> Result<()> {
        while self.has_free_pages() {
            if !self.incr_vacuum_step()? {
                break;
            }
        }
        Ok(())
    }
}
```

### 5. Implement PRAGMA Commands
```rust
// PRAGMA auto_vacuum = NONE | FULL | INCREMENTAL
// PRAGMA incremental_vacuum(N) - run N steps
```

## Unit Tests Required

### Test 1: Pointer map page calculation
```rust
#[test]
fn test_ptrmap_pageno() {
    let pm = PointerMap::new(4096);

    // Page 2 is first ptrmap page
    assert_eq!(pm.ptrmap_pageno(3), 2);

    // Calculate entries per page: 4096/5 = 819
    // So ptrmap page 2 covers pages 3-821
    // Page 822 is next ptrmap page
    assert_eq!(pm.ptrmap_pageno(822), 822);
}
```

### Test 2: Pointer map read/write
```rust
#[test]
fn test_ptrmap_read_write() {
    let mut btree = setup_btree_with_autovacuum();
    let mut pm = btree.pointer_map();

    pm.put(5, PTRMAP_BTREE, 2).unwrap();  // Page 5 is child of page 2
    let (ptype, parent) = pm.get(5).unwrap();
    assert_eq!(ptype, PTRMAP_BTREE);
    assert_eq!(parent, 2);
}
```

### Test 3: Incremental vacuum reduces file size
```rust
#[test]
fn test_incr_vacuum_shrinks_db() {
    let path = temp_db_path();
    {
        let mut db = Database::create_with_autovacuum(&path, AUTOVACUUM_INCR).unwrap();
        db.execute("CREATE TABLE t(x)").unwrap();

        // Insert lots of data
        for i in 0..1000 {
            db.execute(&format!("INSERT INTO t VALUES('{}')", "x".repeat(1000))).unwrap();
        }
        db.close().unwrap();
    }

    let size_after_insert = fs::metadata(&path).unwrap().len();

    {
        let mut db = Database::open(&path).unwrap();
        // Delete everything
        db.execute("DELETE FROM t").unwrap();

        // Run incremental vacuum
        db.execute("PRAGMA incremental_vacuum(1000)").unwrap();
        db.close().unwrap();
    }

    let size_after_vacuum = fs::metadata(&path).unwrap().len();
    assert!(size_after_vacuum < size_after_insert / 2, "File should shrink significantly");
}
```

### Test 4: Auto-vacuum on commit
```rust
#[test]
fn test_auto_vacuum_on_commit() {
    let path = temp_db_path();

    let mut db = Database::create_with_autovacuum(&path, AUTOVACUUM_FULL).unwrap();
    db.execute("CREATE TABLE t(x)").unwrap();

    for i in 0..100 {
        db.execute(&format!("INSERT INTO t VALUES({})", i)).unwrap();
    }

    let size_before_delete = db.page_count();

    // Delete all - should auto-vacuum on commit
    db.execute("DELETE FROM t").unwrap();
    db.execute("COMMIT").unwrap();

    let size_after_commit = db.page_count();
    assert!(size_after_commit < size_before_delete);
}
```

### Test 5: Page relocation updates pointers
```rust
#[test]
fn test_page_relocation() {
    let mut btree = setup_btree_with_autovacuum();

    // Create multi-level tree
    for i in 0..1000i64 {
        btree.insert(i, &[0u8; 100]).unwrap();
    }

    // Delete to create free pages in middle
    for i in (0..500i64).step_by(2) {
        btree.delete(i).unwrap();
    }

    // Vacuum should relocate pages
    while btree.incr_vacuum_step().unwrap() {}

    // Tree should still be valid
    for i in (1..500i64).step_by(2) {
        assert!(btree.find(i).unwrap().is_some());
    }
}
```

### Test 6: Overflow page relocation
```rust
#[test]
fn test_overflow_relocation() {
    let mut btree = setup_btree_with_autovacuum();

    // Insert large values that create overflow pages
    btree.insert(1, &[0u8; 10000]).unwrap();
    btree.insert(2, &[0u8; 10000]).unwrap();

    // Delete first to create free pages
    btree.delete(1).unwrap();

    // Vacuum should relocate overflow pages from row 2
    while btree.incr_vacuum_step().unwrap() {}

    // Row 2 should still be readable
    let data = btree.find(2).unwrap().unwrap();
    assert_eq!(data.len(), 10000);
}
```

### Test 7: SQLite compatibility
```rust
#[test]
fn test_vacuum_sqlite_compatible() {
    let path = temp_db_path();

    // Create and vacuum with rustql
    {
        let mut db = Database::create_with_autovacuum(&path, AUTOVACUUM_FULL).unwrap();
        db.execute("CREATE TABLE t(x)").unwrap();
        db.execute("INSERT INTO t VALUES(1)").unwrap();
        db.close().unwrap();
    }

    // Open with sqlite3, verify pointer map valid
    let output = run_sqlite(&path, "PRAGMA integrity_check;");
    assert_eq!(output.trim(), "ok");
}
```

## Acceptance Criteria

- [ ] Database header stores auto-vacuum mode at offset 52
- [ ] Pointer map correctly tracks page types and parents
- [ ] PRAGMA auto_vacuum works (NONE, FULL, INCREMENTAL)
- [ ] PRAGMA incremental_vacuum(N) runs N vacuum steps
- [ ] Auto-vacuum mode shrinks file on commit
- [ ] Page relocation updates all pointers correctly
- [ ] Overflow chain relocation works
- [ ] Pointer map pages calculated correctly
- [ ] SQLite database compatibility maintained
- [ ] All unit tests pass
