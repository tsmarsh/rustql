# Implement VDBE Search Count Tracking for Query Efficiency Tests

## Problem
The where.test uses a `count` proc that appends `sqlite_search_count` to query results:

```tcl
proc count sql {
  set ::sqlite_search_count 0
  return [concat [execsql $sql] $::sqlite_search_count]
}
```

Tests expect efficient index usage measured by low search counts:
- Expected: `[3 121 10 3]` (3 VDBE search operations)
- Got: `[3 121 10 0]` (search count not implemented)

## What sqlite_search_count Tracks

SQLite increments `sqlite_search_count` for these VDBE operations:
- **OP_SeekGE, OP_SeekGT, OP_SeekLE, OP_SeekLT** - Index/table seeks
- **OP_Next, OP_Prev** - Cursor movement
- **OP_MoveTo** - Direct rowid lookup

This counter proves that:
1. Index is being used (few seeks vs full scan)
2. Query is efficient (low operation count)

## Implementation

### 1. Add Global Counter
```rust
// In VDBE state
thread_local! {
    static SEARCH_COUNT: Cell<i32> = Cell::new(0);
}

pub fn reset_search_count() {
    SEARCH_COUNT.with(|c| c.set(0));
}

pub fn get_search_count() -> i32 {
    SEARCH_COUNT.with(|c| c.get())
}

fn increment_search_count() {
    SEARCH_COUNT.with(|c| c.set(c.get() + 1));
}
```

### 2. Increment in VDBE Operations
```rust
fn execute_op(&mut self, op: &VdbeOp) -> Result<()> {
    match op.opcode {
        Opcode::SeekGE | Opcode::SeekGT |
        Opcode::SeekLE | Opcode::SeekLT |
        Opcode::SeekRowid | Opcode::NotExists => {
            increment_search_count();
            // ... actual seek logic
        }
        Opcode::Next | Opcode::Prev => {
            increment_search_count();
            // ... cursor movement
        }
        // ...
    }
}
```

### 3. Expose in TCL Extension
```rust
// Add sqlite_search_count variable
unsafe extern "C" fn get_sqlite_search_count(...) {
    set_result_int(interp, get_search_count());
}

// Reset at start of each query
unsafe extern "C" fn execsql_cmd(...) {
    reset_search_count();
    // ... execute query
}
```

## Files to Modify
- `src/vdbe/execute.rs` - Add counter increments
- `src/tcl_ext.rs` - Expose sqlite_search_count variable
- `scripts/run_sqlite_test.tcl` - Ensure count proc uses the variable

## Test Command
```bash
make test-where
```

## Success Criteria
Tests like `where-1.1.1` should return correct search counts:
- With index: low count (3-10)
- Full scan: high count (~100 for 100 rows)

## Regression Tests (Required)

Add these Rust unit tests to prevent regression:

### 1. `src/vdbe/tests/search_count_tests.rs`
```rust
#[cfg(test)]
mod search_count_tests {
    use super::*;

    #[test]
    fn test_search_count_reset() {
        reset_search_count();
        assert_eq!(get_search_count(), 0);

        increment_search_count();
        increment_search_count();
        assert_eq!(get_search_count(), 2);

        reset_search_count();
        assert_eq!(get_search_count(), 0);
    }

    #[test]
    fn test_table_scan_increments_count() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT)").unwrap();
        for i in 0..100 {
            db.execute(&format!("INSERT INTO t1 VALUES({})", i)).unwrap();
        }

        reset_search_count();
        let _: Vec<i32> = db.query("SELECT * FROM t1").unwrap();

        // Full scan of 100 rows should have ~100 Next operations
        let count = get_search_count();
        assert!(count >= 100, "Expected >= 100, got {}", count);
    }

    #[test]
    fn test_index_seek_low_count() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT)").unwrap();
        db.execute("CREATE INDEX i1 ON t1(a)").unwrap();
        for i in 0..100 {
            db.execute(&format!("INSERT INTO t1 VALUES({})", i)).unwrap();
        }

        reset_search_count();
        let _: Vec<i32> = db.query("SELECT * FROM t1 WHERE a = 50").unwrap();

        // Index seek should have very few operations (seek + maybe 1-2 next)
        let count = get_search_count();
        assert!(count <= 10, "Expected <= 10, got {}", count);
    }

    #[test]
    fn test_range_query_partial_scan() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT)").unwrap();
        db.execute("CREATE INDEX i1 ON t1(a)").unwrap();
        for i in 0..100 {
            db.execute(&format!("INSERT INTO t1 VALUES({})", i)).unwrap();
        }

        reset_search_count();
        let _: Vec<i32> = db.query("SELECT * FROM t1 WHERE a > 90").unwrap();

        // Range returning ~10 rows should have ~10 operations
        let count = get_search_count();
        assert!(count >= 9 && count <= 15, "Expected 9-15, got {}", count);
    }

    #[test]
    fn test_count_increments_on_seek_ops() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT PRIMARY KEY)").unwrap();
        db.execute("INSERT INTO t1 VALUES(1)").unwrap();

        reset_search_count();
        let _: Vec<i32> = db.query("SELECT * FROM t1 WHERE a = 1").unwrap();

        // Primary key lookup should have exactly 1 seek
        let count = get_search_count();
        assert!(count >= 1, "Expected >= 1, got {}", count);
    }
}
```

### 2. Integration with TCL
```rust
// In src/tcl_ext.rs - test the variable is exposed
#[test]
fn test_sqlite_search_count_tcl_variable() {
    // Verify the variable can be read via Tcl
    let script = r#"
        sqlite3 db :memory:
        db eval {CREATE TABLE t1(a INT)}
        db eval {INSERT INTO t1 VALUES(1)}
        set ::sqlite_search_count 0
        db eval {SELECT * FROM t1}
        set ::sqlite_search_count
    "#;
    // Result should be > 0 after query
}
```

### Acceptance Criteria
- [ ] All tests in `search_count_tests.rs` pass
- [ ] `sqlite_search_count` variable accessible in TCL
- [ ] `count{}` tests in where.test return non-zero counts
- [ ] Index queries show lower counts than full scans
- [ ] No regression in other test suites
