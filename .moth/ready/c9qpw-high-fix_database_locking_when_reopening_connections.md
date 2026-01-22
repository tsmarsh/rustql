# Fix Database Locking When Reopening Connections

## Problem
When a test closes and reopens a database connection, subsequent operations fail with:
```
Error: unable to open database: database is locked
```

This causes cascading failures in index.test (2% pass rate) - after one lock error,
all subsequent tests fail with "invalid command name db".

## Root Cause Analysis

The TCL test harness does:
```tcl
db close
sqlite3 db test.db
```

The lock persists because:
1. Connection close doesn't fully release file locks
2. Or: Shared resources aren't properly cleaned up
3. Or: File handle isn't being closed properly

## SQLite Locking Algorithm

SQLite uses these lock states (in order):
1. **UNLOCKED** - No lock, file can be read/written by others
2. **SHARED** - Reading, others can also read
3. **RESERVED** - Preparing to write, others can still read
4. **PENDING** - Waiting for readers to finish
5. **EXCLUSIVE** - Writing, no other access allowed

Lock transitions:
```
UNLOCKED -> SHARED (to read)
SHARED -> RESERVED (to prepare write)
RESERVED -> EXCLUSIVE (to write)
EXCLUSIVE -> UNLOCKED (after commit/rollback)
```

On `close()`, the connection must:
1. Rollback any pending transaction (if not committed)
2. Release all locks (transition to UNLOCKED)
3. Close file handles (fcntl unlock + close())
4. Free associated memory

## Implementation Details

### Unix File Locking
SQLite uses `fcntl()` with `F_SETLK` for POSIX advisory locks:
- Lock regions on the file indicate lock state
- SHARED_LOCK: shared lock on SHARED_BYTE range
- EXCLUSIVE_LOCK: exclusive lock on entire lock region

### Close Sequence
```rust
fn sqlite3_close(db: &mut Connection) -> Result<()> {
    // 1. Check for unfinalized statements
    if db.has_active_statements() {
        return Err(SQLITE_BUSY);
    }

    // 2. Rollback any active transaction
    if db.in_transaction() {
        db.rollback()?;
    }

    // 3. Close all btree connections (releases locks)
    for btree in db.btrees.drain(..) {
        btree.close()?;  // Must call pager->close() which releases locks
    }

    // 4. Close file handles
    db.vfs.close()?;

    Ok(())
}
```

## Files to Modify

- `src/api/connection.rs` - Connection close logic
- `src/pager/mod.rs` - Pager close and lock release
- `src/vfs/` - File handle closing

## Test Command
```bash
make test-index
```

## Success Criteria
After `db close`, a new `sqlite3 db test.db` should succeed without locking errors.
All index.test tests should run without cascading "invalid command name db" errors.

## Regression Tests (Required)

Add these Rust unit tests to prevent regression:

### 1. `src/api/tests/connection_lifecycle_tests.rs`
```rust
#[cfg(test)]
mod connection_lifecycle_tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_close_releases_lock_for_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Open, write, close
        {
            let db = Connection::open(&path).unwrap();
            db.execute("CREATE TABLE t1(a INT)").unwrap();
            db.execute("INSERT INTO t1 VALUES(1)").unwrap();
            db.close().unwrap();
        }

        // Reopen should succeed without "database is locked"
        {
            let db = Connection::open(&path).unwrap();
            let rows: Vec<i32> = db.query("SELECT a FROM t1").unwrap();
            assert_eq!(rows, vec![1]);
        }
    }

    #[test]
    fn test_close_with_pending_transaction_rollback() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        {
            let db = Connection::open(&path).unwrap();
            db.execute("CREATE TABLE t1(a INT)").unwrap();
            db.execute("BEGIN").unwrap();
            db.execute("INSERT INTO t1 VALUES(1)").unwrap();
            // Don't commit - close should rollback
            db.close().unwrap();
        }

        // Reopen and verify rollback happened
        {
            let db = Connection::open(&path).unwrap();
            let count: i32 = db.query_row("SELECT COUNT(*) FROM t1").unwrap();
            assert_eq!(count, 0); // Insert was rolled back
        }
    }

    #[test]
    fn test_multiple_close_reopen_cycles() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        for i in 0..10 {
            let db = Connection::open(&path).unwrap();
            if i == 0 {
                db.execute("CREATE TABLE t1(a INT)").unwrap();
            }
            db.execute(&format!("INSERT INTO t1 VALUES({})", i)).unwrap();
            db.close().unwrap();
        }

        let db = Connection::open(&path).unwrap();
        let count: i32 = db.query_row("SELECT COUNT(*) FROM t1").unwrap();
        assert_eq!(count, 10);
    }

    #[test]
    fn test_drop_releases_lock() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");

        // Use drop instead of explicit close
        {
            let db = Connection::open(&path).unwrap();
            db.execute("CREATE TABLE t1(a INT)").unwrap();
            // db dropped here
        }

        // Should be able to reopen
        let db = Connection::open(&path).unwrap();
        assert!(db.execute("SELECT * FROM t1").is_ok());
    }
}
```

### 2. `src/pager/tests/lock_release_tests.rs`
```rust
#[test]
fn test_pager_close_releases_file_lock() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");

    let pager = Pager::open(&path).unwrap();
    pager.close().unwrap();

    // File should be unlocked - another open should work
    let pager2 = Pager::open(&path).unwrap();
    assert!(pager2.is_ok());
}
```

### Acceptance Criteria
- [ ] All tests in `connection_lifecycle_tests.rs` pass
- [ ] All tests in `lock_release_tests.rs` pass
- [ ] `make test-index` runs without cascading lock errors
- [ ] No regression in other test suites
