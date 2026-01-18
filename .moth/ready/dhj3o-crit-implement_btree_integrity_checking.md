# Implement B-tree Integrity Checking

## Problem

The current `integrity_check()` function (btree.rs lines 385-392) immediately returns an error - it's not implemented. This means there's no way to detect database corruption, which is critical for data safety.

SQLite's PRAGMA integrity_check is a key diagnostic tool.

## SQLite3 Reference

### Key Files
- `sqlite3/src/btree.c` - sqlite3BtreeIntegrityCheck() (~10800)
- `sqlite3/src/pragma.c` - PRAGMA integrity_check implementation

### Main Function (btree.c ~10800)
```c
char *sqlite3BtreeIntegrityCheck(
  sqlite3 *db,         /* Database connection */
  Btree *p,            /* The btree to check */
  Pgno *aRoot,         /* Root pages of tables/indexes to check */
  int nRoot,           /* Number of root pages */
  int mxErr,           /* Stop after this many errors */
  int *pnErr           /* OUT: Number of errors found */
);
```

### Checks Performed (btree.c ~10800-11200)
1. **Page structure validation**
   - Valid page type flags
   - Cell count within bounds
   - Cell pointers don't overlap
   - Free block list consistency

2. **Tree structure validation**
   - Child page numbers valid
   - Parent-child relationships correct
   - No cycles in tree

3. **Key ordering validation**
   - Keys in ascending order within page
   - Keys between pages properly ordered
   - Index keys match table rowids

4. **Freelist validation**
   - Trunk page chain valid
   - Leaf page counts correct
   - No pages both in tree and freelist

5. **Overflow chain validation**
   - Overflow pointers valid
   - Chain lengths match payload size

### Helper Functions
- `checkTreePage()` - Recursive page checker (btree.c ~10500)
- `checkList()` - Freelist checker (btree.c ~10300)
- `checkPtrmap()` - Pointer map checker for auto-vacuum (btree.c ~10400)

## Current Rust Implementation

```rust
// src/storage/btree.rs lines 385-392
pub fn integrity_check() -> Result<IntegrityCheckResult> {
    Err(Error::new(ErrorCode::Internal))
}
```

## Required Changes

### 1. Define Result Structure
```rust
pub struct IntegrityCheckResult {
    pub errors: Vec<String>,
    pub pages_checked: u32,
    pub is_ok: bool,
}
```

### 2. Implement Main Check Function
```rust
pub fn integrity_check(&self, max_errors: usize) -> Result<IntegrityCheckResult> {
    let mut result = IntegrityCheckResult::new();

    // Check header
    self.check_header(&mut result)?;

    // Check freelist
    self.check_freelist(&mut result)?;

    // Check each btree
    for root in self.get_root_pages()? {
        self.check_tree(root, &mut result)?;
    }

    Ok(result)
}
```

### 3. Implement Sub-checks
```rust
fn check_header(&self, result: &mut IntegrityCheckResult) -> Result<()>;
fn check_freelist(&self, result: &mut IntegrityCheckResult) -> Result<()>;
fn check_tree(&self, root: Pgno, result: &mut IntegrityCheckResult) -> Result<()>;
fn check_page(&self, pgno: Pgno, result: &mut IntegrityCheckResult) -> Result<()>;
fn check_cell(&self, page: &Page, idx: usize, result: &mut IntegrityCheckResult) -> Result<()>;
fn check_overflow_chain(&self, first: Pgno, expected_len: usize, result: &mut IntegrityCheckResult) -> Result<()>;
```

## Unit Tests Required

### Test 1: Valid database passes
```rust
#[test]
fn test_integrity_check_valid_db() {
    let db = setup_db();
    db.execute("CREATE TABLE t(x, y, z)").unwrap();
    db.execute("CREATE INDEX i ON t(y)").unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO t VALUES({}, {}, {})", i, i*2, i*3)).unwrap();
    }

    let result = db.integrity_check(100).unwrap();
    assert!(result.is_ok, "Valid database should pass: {:?}", result.errors);
}
```

### Test 2: Corrupted page header detected
```rust
#[test]
fn test_integrity_check_corrupted_header() {
    let path = temp_db_path();
    create_test_db(&path);

    // Corrupt the page type flag
    let mut file = File::open(&path).unwrap();
    file.seek(SeekFrom::Start(PAGE_SIZE as u64)).unwrap(); // Page 2
    file.write_all(&[0xFF]).unwrap(); // Invalid page type

    let db = Database::open(&path).unwrap();
    let result = db.integrity_check(100).unwrap();
    assert!(!result.is_ok);
    assert!(result.errors.iter().any(|e| e.contains("invalid page type")));
}
```

### Test 3: Out-of-order keys detected
```rust
#[test]
fn test_integrity_check_key_order() {
    // Manually construct a page with out-of-order keys
    // Verify integrity check catches it
}
```

### Test 4: Freelist corruption detected
```rust
#[test]
fn test_integrity_check_freelist_cycle() {
    // Create freelist with a cycle (trunk points to itself)
    // Verify integrity check catches infinite loop
}
```

### Test 5: Overflow chain corruption
```rust
#[test]
fn test_integrity_check_broken_overflow() {
    // Insert large blob, then corrupt overflow pointer
    // Verify integrity check catches broken chain
}
```

### Test 6: Cross-reference with SQLite
```rust
#[test]
fn test_integrity_check_matches_sqlite() {
    let path = temp_db_path();
    create_test_db(&path);

    // Run rustql integrity check
    let rustql_result = rustql_integrity_check(&path);

    // Run sqlite3 PRAGMA integrity_check
    let sqlite_result = run_sqlite(&path, "PRAGMA integrity_check;");

    // Both should report same status
    assert_eq!(rustql_result.is_ok, sqlite_result == "ok");
}
```

### Test 7: Max errors limit respected
```rust
#[test]
fn test_integrity_check_max_errors() {
    // Create database with many corruptions
    // Verify check stops after max_errors
    let result = db.integrity_check(5).unwrap();
    assert!(result.errors.len() <= 5);
}
```

## Error Messages

Follow SQLite's error message format for compatibility:
- `"Page N: cell M extends past end of page"`
- `"Page N: freelist count is M but should be K"`
- `"Page N: child page M has parent N2 instead of N"`
- `"Freelist: trunk page N has invalid next pointer"`
- `"rowid N missing from index I"`

## Acceptance Criteria

- [ ] integrity_check() returns meaningful results
- [ ] Detects corrupted page headers
- [ ] Detects out-of-order keys
- [ ] Detects freelist corruption (cycles, invalid pointers)
- [ ] Detects broken overflow chains
- [ ] Respects max_errors limit
- [ ] Error messages match SQLite format
- [ ] All unit tests pass
- [ ] PRAGMA integrity_check works from SQL
