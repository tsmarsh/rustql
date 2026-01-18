# Implement Persistent Freelist for Freed Pages

## Problem

Currently, freed pages are stored in a `Vec<Pgno>` in memory (`src/storage/btree.rs` line 196). When the database is closed and reopened, this list is lost, causing the database to grow indefinitely even when rows are deleted.

SQLite maintains a persistent freelist on disk that survives restarts.

## SQLite3 Reference

### Key Files
- `sqlite3/src/btree.c` - Freelist management functions
- `sqlite3/src/btreeInt.h` - Internal structures

### Key Functions to Study
- `btreeGetUnusedPage()` - Get page from freelist (btree.c ~6000)
- `freePage2()` - Add page to freelist (btree.c ~7870)
- `allocateBtreePage()` - Allocate new page or reuse from freelist (btree.c ~7600)
- `sqlite3PagerLookup()` - Pager integration

### Freelist Structure (btree.c comments ~200-250)
```
The freelist is stored as a linked list of "trunk" pages. Each trunk page contains:
- Bytes 0-3: Page number of next trunk page (0 if last)
- Bytes 4-7: Number of leaf pages on this trunk
- Bytes 8+: Array of leaf page numbers (up to usable_size/4 - 2 entries)

When freelist overflows a trunk page, a new trunk is allocated.
```

### Database Header (offset 32-39)
- Offset 32-35: Total number of freelist pages
- Offset 36-39: First freelist trunk page number

## Current Rust Implementation

```rust
// src/storage/btree.rs line 196
free_pages: Vec<Pgno>,

// allocate_page() - line ~2200
fn allocate_page(&mut self) -> Result<Pgno> {
    if let Some(pgno) = self.free_pages.pop() {
        return Ok(pgno);
    }
    // Fall back to extending database
    Ok(self.db_size + 1)
}
```

## Required Changes

1. **Read freelist on open**: Parse trunk pages from header offset 36-39
2. **Write freelist on close/commit**: Persist trunk/leaf structure to disk
3. **Update header**: Maintain free page count at offset 32-35
4. **Trunk page management**: Implement trunk page allocation when freelist grows

## Unit Tests Required

### Test 1: Freelist persistence across restart
```rust
#[test]
fn test_freelist_persists_across_restart() {
    let path = temp_db_path();

    // Create database, insert rows, delete them
    {
        let mut db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t(x)").unwrap();
        for i in 0..1000 {
            db.execute(&format!("INSERT INTO t VALUES({})", i)).unwrap();
        }
        let size_after_insert = db.page_count();

        db.execute("DELETE FROM t").unwrap();
        db.close().unwrap();
    }

    // Reopen and verify freelist was persisted
    {
        let mut db = Database::open(&path).unwrap();
        let freelist_count = db.freelist_count(); // Read from header offset 32
        assert!(freelist_count > 0, "Freelist should be persisted");

        // Insert new rows - should reuse freed pages
        for i in 0..500 {
            db.execute(&format!("INSERT INTO t VALUES({})", i)).unwrap();
        }
        let size_after_reinsert = db.page_count();
        // Size should not have grown much since we reused freed pages
    }
}
```

### Test 2: Trunk page overflow
```rust
#[test]
fn test_freelist_trunk_overflow() {
    // Delete enough pages to overflow a single trunk page
    // Verify multiple trunk pages are created and linked
}
```

### Test 3: Header consistency
```rust
#[test]
fn test_freelist_header_matches_actual() {
    // Verify offset 32-35 count matches actual freelist traversal
}
```

### Test 4: SQLite compatibility
```rust
#[test]
fn test_freelist_sqlite_compatible() {
    // Create DB with rustql, open with sqlite3, verify freelist readable
    // Create DB with sqlite3, open with rustql, verify freelist readable
}
```

## Acceptance Criteria

- [ ] Freelist persists across database close/reopen
- [ ] Database header offsets 32-39 correctly maintained
- [ ] Trunk page structure matches SQLite format
- [ ] All unit tests pass
- [ ] Cross-compatible with SQLite3 databases
