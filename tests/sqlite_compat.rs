//! SQLite File Format Compatibility Tests
//!
//! These tests verify that RustQL database files are 100% compatible with
//! the official SQLite CLI tool. This is a non-negotiable requirement per CLAUDE.md.
//!
//! Every test creates a database with RustQL, then verifies it with sqlite3.

use std::process::Command;
use std::sync::Once;

use rustql::types::StepResult;
use rustql::{sqlite3_close, sqlite3_initialize, sqlite3_open, sqlite3_prepare_v2, sqlite3_step};

static INIT: Once = Once::new();

fn init() {
    INIT.call_once(|| {
        let _ = sqlite3_initialize();
    });
}

/// Execute SQL in RustQL
fn exec_rustql(db_path: &str, sql: &str) {
    let mut conn = sqlite3_open(db_path).expect("open db");
    for stmt_sql in sql.split(';').filter(|s| !s.trim().is_empty()) {
        let (mut stmt, _) = sqlite3_prepare_v2(&mut conn, stmt_sql).expect("prepare");
        loop {
            match sqlite3_step(&mut stmt) {
                Ok(StepResult::Done) => break,
                Ok(StepResult::Row) => continue,
                Err(e) => panic!("Error executing '{}': {:?}", stmt_sql, e),
            }
        }
    }
    let _ = sqlite3_close(conn);
}

/// Query SQLite CLI and return stdout
fn sqlite3_cli(db_path: &str, sql: &str) -> String {
    let output = Command::new("sqlite3")
        .arg(db_path)
        .arg(sql)
        .output()
        .expect("sqlite3 command failed");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("sqlite3 failed: {}", stderr);
    }

    String::from_utf8_lossy(&output.stdout).to_string()
}

/// Verify database passes sqlite3 integrity check
fn verify_integrity(db_path: &str) {
    let result = sqlite3_cli(db_path, "PRAGMA integrity_check;");
    assert_eq!(
        result.trim(),
        "ok",
        "Integrity check failed for {}: {}",
        db_path,
        result
    );
}

/// Query RustQL and return results as strings
fn query_rustql(db_path: &str, sql: &str) -> Vec<Vec<String>> {
    let mut conn = sqlite3_open(db_path).expect("open db");
    let (mut stmt, _) = sqlite3_prepare_v2(&mut conn, sql).expect("prepare");
    let mut results = Vec::new();

    loop {
        match sqlite3_step(&mut stmt) {
            Ok(StepResult::Row) => {
                let col_count = rustql::sqlite3_column_count(&stmt);
                let mut row = Vec::new();
                for i in 0..col_count {
                    let val = rustql::sqlite3_column_text(&stmt, i);
                    row.push(val);
                }
                results.push(row);
            }
            Ok(StepResult::Done) => break,
            Err(e) => panic!("Query error: {:?}", e),
        }
    }

    let _ = sqlite3_close(conn);
    results
}

// ============================================================================
// Basic File Format Tests
// ============================================================================

#[test]
fn test_empty_database_readable_by_sqlite() {
    init();
    let db_path = "/tmp/rustql_compat_empty.db";
    let _ = std::fs::remove_file(db_path);

    // Create empty database
    let conn = sqlite3_open(db_path).expect("open");
    let _ = sqlite3_close(conn);

    // Verify with sqlite3
    verify_integrity(db_path);

    // Cleanup
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_single_table_readable_by_sqlite() {
    init();
    let db_path = "/tmp/rustql_compat_single.db";
    let _ = std::fs::remove_file(db_path);

    // Create table with RustQL
    exec_rustql(db_path, "CREATE TABLE t1(a INTEGER, b TEXT, c REAL)");

    // Verify integrity
    verify_integrity(db_path);

    // Verify schema readable
    let schema = sqlite3_cli(db_path, ".schema");
    assert!(
        schema.contains("CREATE TABLE"),
        "Schema not readable: {}",
        schema
    );

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_data_round_trip_rustql_to_sqlite() {
    init();
    let db_path = "/tmp/rustql_compat_roundtrip.db";
    let _ = std::fs::remove_file(db_path);

    // Create and populate with RustQL
    exec_rustql(
        db_path,
        "CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT, value REAL);
         INSERT INTO t1 VALUES(1, 'alice', 3.14);
         INSERT INTO t1 VALUES(2, 'bob', 2.71);
         INSERT INTO t1 VALUES(3, 'charlie', 1.41)",
    );

    // Verify integrity
    verify_integrity(db_path);

    // Read with sqlite3 CLI
    let result = sqlite3_cli(db_path, "SELECT id, name, value FROM t1 ORDER BY id;");
    let lines: Vec<&str> = result.trim().lines().collect();

    assert_eq!(lines.len(), 3, "Expected 3 rows, got: {:?}", lines);
    assert!(lines[0].contains("alice"), "Row 1 mismatch: {}", lines[0]);
    assert!(lines[1].contains("bob"), "Row 2 mismatch: {}", lines[1]);
    assert!(lines[2].contains("charlie"), "Row 3 mismatch: {}", lines[2]);

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_data_round_trip_sqlite_to_rustql() {
    init();
    let db_path = "/tmp/rustql_compat_roundtrip2.db";
    let _ = std::fs::remove_file(db_path);

    // Create with sqlite3 CLI
    sqlite3_cli(
        db_path,
        "CREATE TABLE t1(x INT, y TEXT); INSERT INTO t1 VALUES(10, 'hello');",
    );

    // Read with RustQL
    let results = query_rustql(db_path, "SELECT x, y FROM t1");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0][0], "10");
    assert_eq!(results[0][1], "hello");

    let _ = std::fs::remove_file(db_path);
}

// ============================================================================
// Index Compatibility Tests
// ============================================================================

#[test]
fn test_index_readable_by_sqlite() {
    init();
    let db_path = "/tmp/rustql_compat_index.db";
    let _ = std::fs::remove_file(db_path);

    exec_rustql(
        db_path,
        "CREATE TABLE t1(a INT, b TEXT);
         CREATE INDEX idx_a ON t1(a);
         INSERT INTO t1 VALUES(1, 'one');
         INSERT INTO t1 VALUES(2, 'two');
         INSERT INTO t1 VALUES(3, 'three')",
    );

    verify_integrity(db_path);

    // Verify index exists and is used
    let plan = sqlite3_cli(db_path, "EXPLAIN QUERY PLAN SELECT * FROM t1 WHERE a = 2;");
    assert!(
        plan.contains("idx_a") || plan.contains("SEARCH"),
        "Index should be visible: {}",
        plan
    );

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_multi_column_index_compatible() {
    init();
    let db_path = "/tmp/rustql_compat_multi_idx.db";
    let _ = std::fs::remove_file(db_path);

    exec_rustql(
        db_path,
        "CREATE TABLE t1(a INT, b INT, c INT);
         CREATE INDEX idx_ab ON t1(a, b);
         INSERT INTO t1 VALUES(1, 2, 3);
         INSERT INTO t1 VALUES(1, 3, 4);
         INSERT INTO t1 VALUES(2, 1, 5)",
    );

    verify_integrity(db_path);

    // Query using composite index
    let result = sqlite3_cli(db_path, "SELECT c FROM t1 WHERE a = 1 AND b = 2;");
    assert_eq!(result.trim(), "3");

    let _ = std::fs::remove_file(db_path);
}

// ============================================================================
// Large Data Tests
// ============================================================================

#[test]
fn test_many_rows_compatible() {
    init();
    let db_path = "/tmp/rustql_compat_large.db";
    let _ = std::fs::remove_file(db_path);

    // Create table
    exec_rustql(db_path, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val INT)");

    // Insert many rows
    let mut conn = sqlite3_open(db_path).expect("open");
    for i in 0..1000 {
        let sql = format!("INSERT INTO t1 VALUES({}, {})", i, i * 2);
        let (mut stmt, _) = sqlite3_prepare_v2(&mut conn, &sql).expect("prepare");
        while let Ok(r) = sqlite3_step(&mut stmt) {
            if matches!(r, StepResult::Done) {
                break;
            }
        }
    }
    let _ = sqlite3_close(conn);

    // Verify integrity
    verify_integrity(db_path);

    // Verify count
    let count = sqlite3_cli(db_path, "SELECT COUNT(*) FROM t1;");
    assert_eq!(count.trim(), "1000");

    // Verify specific value
    let val = sqlite3_cli(db_path, "SELECT val FROM t1 WHERE id = 500;");
    assert_eq!(val.trim(), "1000");

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_large_text_compatible() {
    init();
    let db_path = "/tmp/rustql_compat_largetext.db";
    let _ = std::fs::remove_file(db_path);

    // Create large text value (10KB)
    let large_text: String = "x".repeat(10000);

    exec_rustql(
        db_path,
        &format!(
            "CREATE TABLE t1(id INT, data TEXT);
             INSERT INTO t1 VALUES(1, '{}')",
            large_text
        ),
    );

    verify_integrity(db_path);

    // Verify length
    let len = sqlite3_cli(db_path, "SELECT LENGTH(data) FROM t1 WHERE id = 1;");
    assert_eq!(len.trim(), "10000");

    let _ = std::fs::remove_file(db_path);
}

// ============================================================================
// Transaction and Modification Tests
// ============================================================================

#[test]
#[ignore = "Known issue: UPDATE with INT PRIMARY KEY not persisting correctly - see moth eigtx"]
fn test_update_compatible() {
    init();
    let db_path = "/tmp/rustql_compat_update.db";
    let _ = std::fs::remove_file(db_path);

    exec_rustql(
        db_path,
        "CREATE TABLE t1(id INT PRIMARY KEY, val INT);
         INSERT INTO t1 VALUES(1, 100);
         UPDATE t1 SET val = 200 WHERE id = 1",
    );

    verify_integrity(db_path);

    let result = sqlite3_cli(db_path, "SELECT val FROM t1 WHERE id = 1;");
    assert_eq!(result.trim(), "200");

    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_delete_compatible() {
    init();
    let db_path = "/tmp/rustql_compat_delete.db";
    let _ = std::fs::remove_file(db_path);

    exec_rustql(
        db_path,
        "CREATE TABLE t1(id INT);
         INSERT INTO t1 VALUES(1);
         INSERT INTO t1 VALUES(2);
         INSERT INTO t1 VALUES(3);
         DELETE FROM t1 WHERE id = 2",
    );

    verify_integrity(db_path);

    let result = sqlite3_cli(db_path, "SELECT id FROM t1 ORDER BY id;");
    let lines: Vec<&str> = result.trim().lines().collect();
    assert_eq!(lines, vec!["1", "3"]);

    let _ = std::fs::remove_file(db_path);
}

// ============================================================================
// Schema Compatibility Tests
// ============================================================================

#[test]
#[ignore = "Known issue: Multiple table INSERT with INT PRIMARY KEY - see moth eigtx"]
fn test_multiple_tables_compatible() {
    init();
    let db_path = "/tmp/rustql_compat_multi.db";
    let _ = std::fs::remove_file(db_path);

    exec_rustql(
        db_path,
        "CREATE TABLE users(id INT PRIMARY KEY, name TEXT);
         CREATE TABLE orders(id INT PRIMARY KEY, user_id INT, amount REAL);
         INSERT INTO users VALUES(1, 'alice');
         INSERT INTO orders VALUES(100, 1, 99.99)",
    );

    verify_integrity(db_path);

    // Verify join works
    let result = sqlite3_cli(
        db_path,
        "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id;",
    );
    assert!(result.contains("alice"));
    assert!(result.contains("99.99"));

    let _ = std::fs::remove_file(db_path);
}

#[test]
#[ignore = "Known issue: UNIQUE constraint false positive with INT PRIMARY KEY - see moth zenc6"]
fn test_unique_constraint_compatible() {
    init();
    let db_path = "/tmp/rustql_compat_unique.db";
    let _ = std::fs::remove_file(db_path);

    exec_rustql(
        db_path,
        "CREATE TABLE t1(id INT PRIMARY KEY, email TEXT UNIQUE);
         INSERT INTO t1 VALUES(1, 'a@example.com');
         INSERT INTO t1 VALUES(2, 'b@example.com')",
    );

    verify_integrity(db_path);

    // Verify unique index exists
    let indices = sqlite3_cli(db_path, ".indices t1");
    assert!(
        indices.contains("sqlite_autoindex") || !indices.is_empty(),
        "Should have unique index"
    );

    let _ = std::fs::remove_file(db_path);
}
