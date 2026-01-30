//! Debug test for REPLACE with partial unique indexes
//!
//! Tests the scenario where REPLACE needs to delete a row due to a partial
//! index constraint conflict.

use rustql::types::StepResult;
use rustql::{
    sqlite3_close, sqlite3_column_count, sqlite3_column_int64, sqlite3_column_text,
    sqlite3_column_type, sqlite3_finalize, sqlite3_initialize, sqlite3_open, sqlite3_prepare_v2,
    sqlite3_step, SqliteConnection,
};
use std::sync::Once;

static INIT: Once = Once::new();

fn init() {
    INIT.call_once(|| {
        let _ = sqlite3_initialize();
    });
}

fn run_sql(conn: &mut SqliteConnection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    println!("  SQL: {}", sql.trim());
    let (mut stmt, _tail) = match sqlite3_prepare_v2(conn, sql) {
        Ok(result) => result,
        Err(e) => {
            println!("  Prepare error: {:?}", e);
            return Err(format!("{:?}", e));
        }
    };

    let mut results = Vec::new();
    let col_count = sqlite3_column_count(&stmt);
    loop {
        match sqlite3_step(&mut stmt) {
            Ok(StepResult::Row) => {
                let mut row = Vec::new();
                for i in 0..col_count {
                    let col_type = sqlite3_column_type(&stmt, i);
                    let text = if col_type == rustql::types::ColumnType::Null {
                        "{}".to_string()
                    } else {
                        sqlite3_column_text(&stmt, i)
                    };
                    row.push(text);
                }
                println!("  Row: {:?}", row);
                results.push(row);
            }
            Ok(StepResult::Done) => {
                println!("  Done. Total rows: {}", results.len());
                break;
            }
            Err(e) => {
                let _ = sqlite3_finalize(stmt);
                return Err(format!("{:?}", e));
            }
        }
    }

    let _ = sqlite3_finalize(stmt);
    Ok(results)
}

fn run_sql_no_result(conn: &mut SqliteConnection, sql: &str) -> Result<(), String> {
    run_sql(conn, sql)?;
    Ok(())
}

fn run_sql_expect_error(conn: &mut SqliteConnection, sql: &str) -> Option<String> {
    match run_sql(conn, sql) {
        Ok(_) => None,
        Err(e) => Some(e),
    }
}

#[test]
fn test_debug_partial_index_setup() {
    init();
    println!("\n=== test_debug_partial_index_setup ===");

    let mut conn = sqlite3_open(":memory:").expect("Failed to open database");

    run_sql_no_result(
        &mut conn,
        "CREATE TABLE t3(a INTEGER PRIMARY KEY, b INT, c INT, d INT);",
    )
    .expect("create table");

    run_sql_no_result(&mut conn, "CREATE UNIQUE INDEX t3bpi ON t3(b) WHERE c<=d;")
        .expect("create partial index");

    // Check that the index was created
    println!("\n--- Checking sqlite_master ---");
    let schema = run_sql(&mut conn, "SELECT * FROM sqlite_master WHERE type='index';")
        .expect("schema query");
    for row in &schema {
        println!("  Index: {:?}", row);
    }

    // Insert a row that satisfies partial condition
    run_sql_no_result(&mut conn, "INSERT INTO t3(a,b,c,d) VALUES(3,4,5,6);").expect("insert");

    // Query the data
    println!("\n--- Initial data ---");
    let data = run_sql(&mut conn, "SELECT * FROM t3;").expect("select");
    for row in &data {
        println!("  Row: {:?}", row);
    }

    // Try DELETE then INSERT separately
    println!("\n--- DELETE row 3 ---");
    run_sql_no_result(&mut conn, "DELETE FROM t3 WHERE a=3;").expect("delete should work");

    let after_delete = run_sql(&mut conn, "SELECT * FROM t3;").expect("select");
    println!("  After delete: {} rows", after_delete.len());

    println!("\n--- INSERT new row with b=4 ---");
    run_sql_no_result(&mut conn, "INSERT INTO t3(a,b,c,d) VALUES(4,4,8,9);")
        .expect("insert after delete should work");

    let after_insert = run_sql(&mut conn, "SELECT * FROM t3;").expect("select");
    for row in &after_insert {
        println!("  Row: {:?}", row);
    }
    assert_eq!(after_insert.len(), 1);

    let _ = sqlite3_close(conn);
}

#[test]
fn test_replace_with_partial_index() {
    init();
    println!("\n=== test_replace_with_partial_index ===");

    let mut conn = sqlite3_open(":memory:").expect("Failed to open database");

    // Create table with partial unique index
    // t3bpi only includes rows where c <= d
    run_sql_no_result(
        &mut conn,
        "CREATE TABLE t3(a INTEGER PRIMARY KEY, b INT, c INT, d INT);",
    )
    .expect("create table");

    run_sql_no_result(&mut conn, "CREATE UNIQUE INDEX t3bpi ON t3(b) WHERE c<=d;")
        .expect("create partial index");

    // Insert test data:
    // (1, 1, 1, 1) - c<=d is TRUE (1<=1), so b=1 is in partial index
    // (2, 1, 3, 2) - c<=d is FALSE (3<=2), so NOT in partial index
    // (3, 4, 5, 6) - c<=d is TRUE (5<=6), so b=4 is in partial index
    run_sql_no_result(
        &mut conn,
        "INSERT INTO t3(a,b,c,d) VALUES(1,1,1,1),(2,1,3,2),(3,4,5,6);",
    )
    .expect("insert data");

    // Verify initial data
    println!("\n--- Initial state ---");
    let initial = run_sql(&mut conn, "SELECT a,b,c,d FROM t3 ORDER BY a;").expect("select");
    assert_eq!(initial.len(), 3);
    assert_eq!(initial[0], vec!["1", "1", "1", "1"]);
    assert_eq!(initial[1], vec!["2", "1", "3", "2"]);
    assert_eq!(initial[2], vec!["3", "4", "5", "6"]);

    // Debug: check what's in the table's indexes
    println!("\n--- Debug: query pragma_index_list ---");
    let idx_info = run_sql(&mut conn, "PRAGMA index_list(t3);");
    println!("  index_list result: {:?}", idx_info);

    // REPLACE INTO with new row that conflicts on partial index
    // New row (4, 4, 8, 9):
    // - c<=d is TRUE (8<=9), so it WOULD be in partial index
    // - b=4 conflicts with row 3's b=4 (which is also in partial index)
    // Expected: delete row 3, insert row 4
    println!("\n--- REPLACE ---");
    let result = run_sql(&mut conn, "REPLACE INTO t3(a,b,c,d) VALUES(4,4,8,9);");
    println!("  REPLACE result: {:?}", result);

    if result.is_err() {
        // If it failed, let's check the state of the table
        println!("\n--- Debug: state after failed REPLACE ---");
        let state = run_sql(&mut conn, "SELECT * FROM t3 ORDER BY a;");
        println!("  Current data: {:?}", state);
    }

    result.expect("REPLACE should succeed by deleting conflicting row 3");

    // Verify result
    println!("\n--- Final state ---");
    let final_data = run_sql(&mut conn, "SELECT a,b,c,d FROM t3 ORDER BY a;").expect("select");

    // Should have: (1,1,1,1), (2,1,3,2), (4,4,8,9)
    // Row 3 should be gone, replaced by row 4
    assert_eq!(final_data.len(), 3, "Should still have 3 rows");
    assert_eq!(final_data[0], vec!["1", "1", "1", "1"]);
    assert_eq!(final_data[1], vec!["2", "1", "3", "2"]);
    assert_eq!(
        final_data[2],
        vec!["4", "4", "8", "9"],
        "Row 3 should be replaced by row 4"
    );

    let _ = sqlite3_close(conn);
}

#[test]
fn test_replace_without_partial_condition_conflict() {
    init();
    println!("\n=== test_replace_without_partial_condition_conflict ===");

    let mut conn = sqlite3_open(":memory:").expect("Failed to open database");

    // Create table with partial unique index
    // This tests the case where the NEW row does NOT satisfy the partial index condition
    run_sql_no_result(
        &mut conn,
        "CREATE TABLE t3(a INTEGER PRIMARY KEY, b INT, c INT, d INT);",
    )
    .expect("create table");

    run_sql_no_result(&mut conn, "CREATE UNIQUE INDEX t3bpi ON t3(b) WHERE c<=d;")
        .expect("create partial index");

    // Row (3, 4, 5, 6) satisfies c<=d (5<=6), so b=4 is in partial index
    run_sql_no_result(&mut conn, "INSERT INTO t3(a,b,c,d) VALUES(3,4,5,6);").expect("insert");

    // REPLACE with new row that does NOT satisfy c<=d
    // New row (4, 4, 10, 5): c=10, d=5, so 10<=5 is FALSE
    // This should NOT trigger a partial index conflict because new row won't be in the index
    println!("\n--- REPLACE with non-matching partial condition ---");
    run_sql_no_result(&mut conn, "REPLACE INTO t3(a,b,c,d) VALUES(4,4,10,5);")
        .expect("REPLACE should succeed - no partial index conflict");

    let data = run_sql(&mut conn, "SELECT * FROM t3 ORDER BY a;").expect("select");
    // Should have both rows since there's no conflict on the partial index
    assert_eq!(data.len(), 2);
    assert_eq!(data[0], vec!["3", "4", "5", "6"]);
    assert_eq!(data[1], vec!["4", "4", "10", "5"]);

    let _ = sqlite3_close(conn);
}

#[test]
fn test_replace_simple_unique_index() {
    init();
    println!("\n=== test_replace_simple_unique_index ===");

    let mut conn = sqlite3_open(":memory:").expect("Failed to open database");

    // Create table with simple (non-partial) unique index
    run_sql_no_result(
        &mut conn,
        "CREATE TABLE t3(a INTEGER PRIMARY KEY, b INT, c INT, d INT);",
    )
    .expect("create table");

    run_sql_no_result(&mut conn, "CREATE UNIQUE INDEX t3b ON t3(b);")
        .expect("create simple unique index");

    // Insert row
    run_sql_no_result(&mut conn, "INSERT INTO t3(a,b,c,d) VALUES(3,4,5,6);").expect("insert");

    // Verify
    let before = run_sql(&mut conn, "SELECT * FROM t3;").expect("select");
    assert_eq!(before.len(), 1);

    // REPLACE with conflicting b value
    // This should delete row 3 and insert row 4
    println!("\n--- REPLACE with simple unique index conflict ---");
    run_sql_no_result(&mut conn, "REPLACE INTO t3(a,b,c,d) VALUES(4,4,8,9);")
        .expect("REPLACE should succeed by deleting conflicting row");

    let data = run_sql(&mut conn, "SELECT * FROM t3;").expect("select");
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0],
        vec!["4", "4", "8", "9"],
        "Row 3 should be replaced by row 4"
    );

    let _ = sqlite3_close(conn);
}

#[test]
fn test_basic_delete_with_partial_index() {
    init();
    println!("\n=== test_basic_delete_with_partial_index ===");

    let mut conn = sqlite3_open(":memory:").expect("Failed to open database");

    // Create table with partial unique index
    run_sql_no_result(
        &mut conn,
        "CREATE TABLE t3(a INTEGER PRIMARY KEY, b INT, c INT, d INT);",
    )
    .expect("create table");

    run_sql_no_result(&mut conn, "CREATE UNIQUE INDEX t3bpi ON t3(b) WHERE c<=d;")
        .expect("create partial index");

    // Insert a row that satisfies partial index condition
    run_sql_no_result(&mut conn, "INSERT INTO t3(a,b,c,d) VALUES(3,4,5,6);").expect("insert");

    // Verify row exists
    let before = run_sql(&mut conn, "SELECT * FROM t3;").expect("select");
    assert_eq!(before.len(), 1);

    // Delete the row
    run_sql_no_result(&mut conn, "DELETE FROM t3 WHERE a=3;").expect("delete");

    // Verify row is gone
    let after = run_sql(&mut conn, "SELECT * FROM t3;").expect("select");
    assert_eq!(after.len(), 0);

    // Now insert a row with the same b value - should succeed
    run_sql_no_result(&mut conn, "INSERT INTO t3(a,b,c,d) VALUES(4,4,8,9);")
        .expect("insert after delete should succeed");

    let final_data = run_sql(&mut conn, "SELECT * FROM t3;").expect("select");
    assert_eq!(final_data.len(), 1);
    assert_eq!(final_data[0], vec!["4", "4", "8", "9"]);

    let _ = sqlite3_close(conn);
}

#[test]
fn test_replace_with_delete_trigger_recheck() {
    init();
    println!("\n=== test_replace_with_delete_trigger_recheck ===");

    let mut conn = sqlite3_open(":memory:").expect("Failed to open database");

    // This tests the SQLite pattern where:
    // 1. REPLACE deletes conflicting row
    // 2. DELETE trigger fires and inserts a new conflicting row
    // 3. Recheck should find the conflict and abort

    run_sql_no_result(&mut conn, "PRAGMA recursive_triggers = true;").expect("pragma");

    run_sql_no_result(&mut conn, "CREATE TABLE t0(c0, c1);").expect("create table");

    run_sql_no_result(&mut conn, "CREATE UNIQUE INDEX i0 ON t0(c0);").expect("create index");

    // Insert initial row
    run_sql_no_result(&mut conn, "INSERT INTO t0(c0,c1) VALUES(123,1);").expect("insert");

    // Create trigger that re-inserts on delete
    run_sql_no_result(
        &mut conn,
        "CREATE TRIGGER tr0 AFTER DELETE ON t0 BEGIN INSERT INTO t0(c0,c1) VALUES(123,2); END;",
    )
    .expect("create trigger");

    // REPLACE should fail because trigger re-inserts conflicting row
    let err = run_sql_expect_error(&mut conn, "REPLACE INTO t0(c0,c1) VALUES(123,3);");

    assert!(
        err.is_some(),
        "REPLACE should fail due to trigger inserting conflicting row"
    );
    assert!(
        err.as_ref().unwrap().contains("UNIQUE constraint"),
        "Error should mention UNIQUE constraint: {:?}",
        err
    );

    // Verify original row is still there
    let data = run_sql(&mut conn, "SELECT * FROM t0;").expect("select");
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0],
        vec!["123", "1"],
        "Original row should remain after failed REPLACE"
    );

    let _ = sqlite3_close(conn);
}
