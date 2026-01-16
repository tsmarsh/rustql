//! Debug test for coalesce and nested aggregate issues

use rustql::types::StepResult;
use rustql::{
    sqlite3_close, sqlite3_column_count, sqlite3_column_text, sqlite3_column_type,
    sqlite3_finalize, sqlite3_initialize, sqlite3_open, sqlite3_prepare_v2, sqlite3_step,
    SqliteConnection,
};
use std::sync::Once;

static INIT: Once = Once::new();

fn init() {
    INIT.call_once(|| {
        let _ = sqlite3_initialize();
    });
}

fn exec_sql(conn: &mut SqliteConnection, sql: &str) -> Vec<String> {
    let mut results = Vec::new();
    let (mut stmt, _) = sqlite3_prepare_v2(conn, sql).expect("prepare");
    let col_count = sqlite3_column_count(&stmt);
    loop {
        match sqlite3_step(&mut stmt) {
            Ok(StepResult::Row) => {
                for i in 0..col_count {
                    let col_type = sqlite3_column_type(&stmt, i);
                    let text = if col_type == rustql::types::ColumnType::Null {
                        "{}".to_string()
                    } else {
                        sqlite3_column_text(&stmt, i)
                    };
                    results.push(text);
                }
            }
            Ok(StepResult::Done) => break,
            Err(e) => {
                println!("Error: {:?}", e);
                break;
            }
        }
    }
    let _ = sqlite3_finalize(stmt);
    results
}

#[test]
fn test_coalesce_with_max() {
    init();
    let mut conn = sqlite3_open(":memory:").expect("open");

    // Create t3 like the test does
    exec_sql(&mut conn, "CREATE TABLE t3(a, b)");
    exec_sql(&mut conn, "INSERT INTO t3 VALUES('abc', NULL)");
    exec_sql(&mut conn, "INSERT INTO t3 VALUES(NULL, 'xyz')");
    exec_sql(&mut conn, "INSERT INTO t3 VALUES(11, 22)");
    exec_sql(&mut conn, "INSERT INTO t3 VALUES(33, 44)");

    // First test just max(a)
    println!("=== SELECT max(a) FROM t3 ===");
    let result = exec_sql(&mut conn, "SELECT max(a) FROM t3");
    println!("Result: {:?}", result);
    // Expected: "abc" (max of 'abc', NULL, 11, 33 - string 'abc' is max in SQLite)

    // Now test coalesce(max(a), 'xyzzy')
    println!("\n=== SELECT coalesce(max(a), 'xyzzy') FROM t3 ===");
    let result = exec_sql(&mut conn, "SELECT coalesce(max(a), 'xyzzy') FROM t3");
    println!("Result: {:?}", result);
    // Expected: "abc" (since max(a) returns 'abc', coalesce returns it)

    // Test with NULL max result
    println!("\n=== SELECT coalesce(max(a), 'xyzzy') FROM (SELECT NULL as a) ===");
    exec_sql(&mut conn, "CREATE TABLE empty(a)");
    let result = exec_sql(&mut conn, "SELECT coalesce(max(a), 'xyzzy') FROM empty");
    println!("Result: {:?}", result);
    // Expected: "xyzzy" (max on empty table returns NULL, coalesce returns 'xyzzy')

    let _ = sqlite3_close(conn);
}

#[test]
fn test_ifnull() {
    init();
    let mut conn = sqlite3_open(":memory:").expect("open");

    println!("=== SELECT ifnull(NULL, 'default') ===");
    let result = exec_sql(&mut conn, "SELECT ifnull(NULL, 'default')");
    println!("Result: {:?}", result);

    println!("\n=== SELECT ifnull('value', 'default') ===");
    let result = exec_sql(&mut conn, "SELECT ifnull('value', 'default')");
    println!("Result: {:?}", result);

    let _ = sqlite3_close(conn);
}
