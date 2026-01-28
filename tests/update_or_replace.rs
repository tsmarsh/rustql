//! Debug test for UPDATE OR REPLACE functionality
//!
//! Tests the specific scenario where UPDATE OR REPLACE changes a row's
//! INTEGER PRIMARY KEY to conflict with another existing row.

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

#[test]
fn test_update_or_replace_ipk_conflict() {
    init();
    println!("\n=== test_update_or_replace_ipk_conflict ===");

    // Create in-memory database
    let mut conn = sqlite3_open(":memory:").expect("Failed to open database");

    // Create table with INTEGER PRIMARY KEY
    run_sql_no_result(&mut conn, "CREATE TABLE t2(a INTEGER PRIMARY KEY, b, c);")
        .expect("Failed to create table");

    // Insert test data
    // (1, 2, 'x'), (2, 3, 'x'), (3, 4, 'x'), (4, 5, 'x')
    run_sql_no_result(
        &mut conn,
        "INSERT INTO t2 VALUES(1, 2, 'x'), (2, 3, 'x'), (3, 4, 'x'), (4, 5, 'x');",
    )
    .expect("Failed to insert data");

    // Verify initial data
    println!("\n--- Initial state ---");
    let initial =
        run_sql(&mut conn, "SELECT a, b, c FROM t2 ORDER BY a;").expect("Failed to select");
    assert_eq!(initial.len(), 4);
    assert_eq!(initial[0], vec!["1", "2", "x"]);
    assert_eq!(initial[1], vec!["2", "3", "x"]);
    assert_eq!(initial[2], vec!["3", "4", "x"]);
    assert_eq!(initial[3], vec!["4", "5", "x"]);

    // UPDATE OR REPLACE: change a=2 to a=4 (conflicts with existing a=4)
    // SQLite behavior: delete the conflicting row (a=4), then update row (a=2) to (a=4)
    // Expected result: (1, 2, 'x'), (3, 4, 'x'), (4, 3, 'x')
    println!("\n--- After UPDATE OR REPLACE t2 SET a=4 WHERE a=2 ---");
    run_sql_no_result(&mut conn, "UPDATE OR REPLACE t2 SET a=4 WHERE a=2;")
        .expect("Failed to update");

    let after_update =
        run_sql(&mut conn, "SELECT a, b, c FROM t2 ORDER BY a;").expect("Failed to select");

    println!("\nResults after UPDATE OR REPLACE:");
    for row in &after_update {
        println!("  {:?}", row);
    }

    // Expected: 3 rows (a=1, a=3, a=4)
    // Row a=4 should have b=3 (from the updated row), not b=5 (from the deleted row)
    assert_eq!(
        after_update.len(),
        3,
        "Should have 3 rows after UPDATE OR REPLACE"
    );

    // Check row a=1 is unchanged
    assert_eq!(
        after_update[0],
        vec!["1", "2", "x"],
        "Row a=1 should be unchanged"
    );

    // Check row a=3 is unchanged
    assert_eq!(
        after_update[1],
        vec!["3", "4", "x"],
        "Row a=3 should be unchanged"
    );

    // THE KEY TEST: Row a=4 should have b=3 (from the original row a=2)
    // NOT b=5 (from the deleted row a=4)
    assert_eq!(
        after_update[2],
        vec!["4", "3", "x"],
        "Row a=4 should have b=3 from the updated row, not b=5 from the deleted row"
    );

    let _ = sqlite3_close(conn);
    println!("\n=== test_update_or_replace_ipk_conflict PASSED ===\n");
}

#[test]
fn test_update_or_replace_with_unique_index() {
    init();
    println!("\n=== test_update_or_replace_with_unique_index ===");

    let mut conn = sqlite3_open(":memory:").expect("Failed to open database");

    // Create table with UNIQUE constraint on column b
    run_sql_no_result(
        &mut conn,
        "CREATE TABLE t3(a INTEGER PRIMARY KEY, b UNIQUE, c);",
    )
    .expect("Failed to create table");

    // Insert test data
    run_sql_no_result(
        &mut conn,
        "INSERT INTO t3 VALUES(1, 10, 'one'), (2, 20, 'two'), (3, 30, 'three');",
    )
    .expect("Failed to insert data");

    // Verify initial state
    println!("\n--- Initial state ---");
    let initial = run_sql(&mut conn, "SELECT * FROM t3 ORDER BY a;").expect("Failed to select");
    assert_eq!(initial.len(), 3);

    // UPDATE OR REPLACE: set b=20 for row a=1 (conflicts with row a=2's b=20)
    // Expected: row a=2 is deleted, row a=1 gets b=20
    println!("\n--- After UPDATE OR REPLACE t3 SET b=20 WHERE a=1 ---");
    run_sql_no_result(&mut conn, "UPDATE OR REPLACE t3 SET b=20 WHERE a=1;")
        .expect("Failed to update");

    let after_update =
        run_sql(&mut conn, "SELECT * FROM t3 ORDER BY a;").expect("Failed to select");

    // Should have 2 rows: a=1 and a=3
    assert_eq!(
        after_update.len(),
        2,
        "Should have 2 rows after UPDATE OR REPLACE"
    );

    // Row a=1 should now have b=20
    assert_eq!(after_update[0][0], "1");
    assert_eq!(after_update[0][1], "20");

    // Row a=3 should be unchanged
    assert_eq!(after_update[1][0], "3");
    assert_eq!(after_update[1][1], "30");

    let _ = sqlite3_close(conn);
    println!("\n=== test_update_or_replace_with_unique_index PASSED ===\n");
}

#[test]
fn test_simple_delete() {
    init();
    println!("\n=== test_simple_delete ===");

    let mut conn = sqlite3_open(":memory:").expect("Failed to open database");

    run_sql_no_result(&mut conn, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b);")
        .expect("Failed to create table");

    run_sql_no_result(
        &mut conn,
        "INSERT INTO t1 VALUES(1, 'one'), (2, 'two'), (3, 'three');",
    )
    .expect("Failed to insert");

    println!("\n--- Before DELETE ---");
    let before = run_sql(&mut conn, "SELECT * FROM t1 ORDER BY a;").expect("Failed to select");
    assert_eq!(before.len(), 3);

    run_sql_no_result(&mut conn, "DELETE FROM t1 WHERE a=2;").expect("Failed to delete");

    println!("\n--- After DELETE ---");
    let after = run_sql(&mut conn, "SELECT * FROM t1 ORDER BY a;").expect("Failed to select");
    assert_eq!(after.len(), 2);
    assert_eq!(after[0][0], "1");
    assert_eq!(after[1][0], "3");

    let _ = sqlite3_close(conn);
    println!("\n=== test_simple_delete PASSED ===\n");
}

#[test]
fn test_simple_update() {
    init();
    println!("\n=== test_simple_update ===");

    let mut conn = sqlite3_open(":memory:").expect("Failed to open database");

    run_sql_no_result(&mut conn, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b);")
        .expect("Failed to create table");

    run_sql_no_result(&mut conn, "INSERT INTO t1 VALUES(1, 'one'), (2, 'two');")
        .expect("Failed to insert");

    run_sql_no_result(&mut conn, "UPDATE t1 SET b='TWO' WHERE a=2;").expect("Failed to update");

    let result = run_sql(&mut conn, "SELECT * FROM t1 ORDER BY a;").expect("Failed to select");
    assert_eq!(result.len(), 2);
    assert_eq!(result[1], vec!["2", "TWO"]);

    let _ = sqlite3_close(conn);
    println!("\n=== test_simple_update PASSED ===\n");
}
