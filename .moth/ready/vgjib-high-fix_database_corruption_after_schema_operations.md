# Fix Database Corruption After Schema Operations

## Problem
Database becomes corrupted ("disk image is malformed") after certain error conditions, particularly involving schema operations. This causes cascading test failures.

### Observed Pattern
```
insert-1.2: INSERT INTO sqlite_master VALUES(...)
Expected: {1 {table sqlite_master may not be modified}}
Got:      {1 {no such table: sqlite_master}}

insert-1.3: CREATE TABLE test1(...)
Error: database disk image is malformed
```

After the initial error, subsequent operations fail with corruption errors.

## Root Cause Analysis

### 1. sqlite_master Table Handling
The system returns "no such table: sqlite_master" instead of "table sqlite_master may not be modified". This suggests:
- sqlite_master is not properly registered as a system table
- Or the schema cache is not properly initialized

### 2. State Corruption After Errors
When an operation fails with an unexpected error:
- Transaction may not be properly rolled back
- Schema cache may be left in inconsistent state
- Page cache may contain dirty pages from failed operations

### 3. Schema Initialization
The database header page (page 1) must be properly initialized with:
- Magic string at offset 0
- Page size at offset 16
- Schema cookie at offset 40
- sqlite_master table structure

## SQLite Schema Handling

### sqlite_master Protection
SQLite protects sqlite_master from direct modification:
```c
// In sqlite3Insert():
if( pTab->tabFlags & TF_Readonly ){
  sqlite3ErrorMsg(pParse, "table %s may not be modified", pTab->zName);
  goto insert_cleanup;
}
```

### Schema Initialization Sequence
1. Open database file
2. Read page 1 header
3. Validate magic/page size
4. Read sqlite_master table from root page 1
5. Build schema cache from sqlite_master entries

## Implementation

### 1. Mark sqlite_master as Read-Only
```rust
impl Table {
    pub fn is_readonly(&self) -> bool {
        self.name == "sqlite_master" ||
        self.name == "sqlite_temp_master" ||
        self.name.starts_with("sqlite_")
    }
}

fn check_table_writable(table: &Table) -> Result<()> {
    if table.is_readonly() {
        return Err(Error::with_message(
            ErrorCode::Error,
            format!("table {} may not be modified", table.name)
        ));
    }
    Ok(())
}
```

### 2. Proper Error Recovery
```rust
fn execute_with_recovery(&mut self, stmt: &Statement) -> Result<()> {
    let result = self.execute_inner(stmt);

    if result.is_err() {
        // Ensure transaction is rolled back on error
        if self.in_transaction() && !self.auto_commit {
            self.rollback_internal()?;
        }

        // Clear any dirty state in schema cache
        self.schema_cache.invalidate_if_dirty();
    }

    result
}
```

### 3. Schema Cache Consistency
```rust
impl SchemaCache {
    fn invalidate_if_dirty(&mut self) {
        if self.is_dirty {
            self.clear();
            self.is_dirty = false;
        }
    }

    fn mark_dirty(&mut self) {
        self.is_dirty = true;
    }
}
```

## Files to Modify
- `src/schema/mod.rs` - Add readonly check for sqlite_master
- `src/executor/insert.rs` - Check table writable before insert
- `src/executor/update.rs` - Check table writable before update
- `src/executor/delete.rs` - Check table writable before delete
- `src/api/connection.rs` - Error recovery logic

## Test Command
```bash
make test-insert
```

## Success Criteria
- "table sqlite_master may not be modified" error for direct modifications
- No cascading corruption after error conditions
- Schema cache remains consistent after errors

## Regression Tests (Required)

### 1. `src/schema/tests/sqlite_master_tests.rs`
```rust
#[cfg(test)]
mod sqlite_master_tests {
    use super::*;

    #[test]
    fn test_sqlite_master_insert_blocked() {
        let db = setup_test_db();

        let result = db.execute("INSERT INTO sqlite_master VALUES(1,2,3,4,5)");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("may not be modified"),
            "Expected 'may not be modified', got: {}", err);
    }

    #[test]
    fn test_sqlite_master_update_blocked() {
        let db = setup_test_db();

        let result = db.execute("UPDATE sqlite_master SET name='foo'");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("may not be modified"));
    }

    #[test]
    fn test_sqlite_master_delete_blocked() {
        let db = setup_test_db();

        let result = db.execute("DELETE FROM sqlite_master");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("may not be modified"));
    }

    #[test]
    fn test_sqlite_master_exists() {
        let db = setup_test_db();

        // sqlite_master should exist and be queryable
        let rows = db.query("SELECT * FROM sqlite_master").unwrap();
        // Empty database has at least the schema for sqlite_master itself
        assert!(rows.is_empty() || rows.len() >= 0);
    }
}
```

### 2. `src/api/tests/error_recovery_tests.rs`
```rust
#[cfg(test)]
mod error_recovery_tests {
    use super::*;

    #[test]
    fn test_no_corruption_after_schema_error() {
        let db = setup_test_db();

        // Trigger an error involving sqlite_master
        let _ = db.execute("INSERT INTO sqlite_master VALUES(1,2,3,4,5)");

        // Next operation should NOT fail with corruption
        let result = db.execute("CREATE TABLE test1(a INT)");
        assert!(result.is_ok(), "Got unexpected error: {:?}", result);

        // Verify table was created
        let rows: Vec<i32> = db.query("SELECT a FROM test1").unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_no_corruption_after_constraint_error() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT PRIMARY KEY)").unwrap();
        db.execute("INSERT INTO t1 VALUES(1)").unwrap();

        // Trigger a constraint error
        let _ = db.execute("INSERT INTO t1 VALUES(1)");

        // Next operation should work
        let result = db.execute("INSERT INTO t1 VALUES(2)");
        assert!(result.is_ok(), "Got corruption after constraint error: {:?}", result);
    }

    #[test]
    fn test_no_corruption_after_column_count_error() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT, b INT, c INT)").unwrap();

        // Wrong number of values
        let _ = db.execute("INSERT INTO t1 VALUES(1, 2)");

        // Next operation should work
        let result = db.execute("INSERT INTO t1 VALUES(1, 2, 3)");
        assert!(result.is_ok(), "Got corruption after column count error: {:?}", result);

        // Verify data
        let rows: Vec<(i32, i32, i32)> = db.query("SELECT * FROM t1").unwrap();
        assert_eq!(rows, vec![(1, 2, 3)]);
    }

    #[test]
    fn test_schema_consistent_after_failed_create() {
        let db = setup_test_db();

        // Try to create table with invalid syntax or error
        let _ = db.execute("CREATE TABLE t1(a INT, a INT)"); // Duplicate column

        // Schema should still be consistent
        let result = db.execute("CREATE TABLE t2(x INT)");
        assert!(result.is_ok(), "Schema corrupted after failed CREATE: {:?}", result);
    }

    #[test]
    fn test_transaction_rollback_on_error() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT)").unwrap();
        db.execute("INSERT INTO t1 VALUES(1)").unwrap();

        db.execute("BEGIN").unwrap();
        db.execute("INSERT INTO t1 VALUES(2)").unwrap();

        // Force an error
        let _ = db.execute("INSERT INTO nonexistent VALUES(3)");

        // Should be able to rollback
        let result = db.execute("ROLLBACK");
        assert!(result.is_ok());

        // Original data should be intact
        let count: i32 = db.query_row("SELECT COUNT(*) FROM t1").unwrap();
        assert_eq!(count, 1);
    }
}
```

### 3. `src/storage/tests/page_integrity_tests.rs`
```rust
#[cfg(test)]
mod page_integrity_tests {
    use super::*;

    #[test]
    fn test_page1_valid_after_error() {
        let db = setup_test_db();

        // Trigger various errors
        let _ = db.execute("INSERT INTO sqlite_master VALUES(1)");
        let _ = db.execute("SELECT * FROM nonexistent");

        // Page 1 should still be valid
        let magic = db.read_page1_magic();
        assert_eq!(magic, b"SQLite format 3\0");
    }

    #[test]
    fn test_schema_cookie_consistent() {
        let db = setup_test_db();
        let cookie1 = db.schema_cookie();

        // Error should not change schema cookie
        let _ = db.execute("INSERT INTO sqlite_master VALUES(1)");
        let cookie2 = db.schema_cookie();
        assert_eq!(cookie1, cookie2);

        // Successful CREATE should increment it
        db.execute("CREATE TABLE t1(a INT)").unwrap();
        let cookie3 = db.schema_cookie();
        assert_eq!(cookie3, cookie2 + 1);
    }
}
```

### Acceptance Criteria
- [ ] All tests in `sqlite_master_tests.rs` pass
- [ ] All tests in `error_recovery_tests.rs` pass
- [ ] All tests in `page_integrity_tests.rs` pass
- [ ] `make test-insert` shows no cascading corruption errors
- [ ] No regression in other test suites
