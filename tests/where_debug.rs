//! Debug tests for WHERE clause issues

use rustql::types::StepResult;
use rustql::{
    sqlite3_close, sqlite3_column_count, sqlite3_column_text, sqlite3_finalize, sqlite3_initialize,
    sqlite3_open, sqlite3_prepare_v2, sqlite3_step, SqliteConnection,
};
use std::sync::Once;

// Tests to debug WHERE test failures - mimics where.test setup

static INIT: Once = Once::new();

fn init() {
    INIT.call_once(|| {
        let _ = sqlite3_initialize();
    });
}

fn exec(conn: &mut SqliteConnection, sql: &str) {
    let (mut stmt, _) = sqlite3_prepare_v2(conn, sql).unwrap();
    loop {
        match sqlite3_step(&mut stmt) {
            Ok(StepResult::Done) => break,
            Ok(StepResult::Row) => continue,
            Err(e) => {
                eprintln!("Error executing '{}': {:?}", sql, e);
                break;
            }
        }
    }
    let _ = sqlite3_finalize(stmt);
}

fn query(conn: &mut SqliteConnection, sql: &str) -> Vec<Vec<String>> {
    let (mut stmt, _) = sqlite3_prepare_v2(conn, sql).unwrap();
    let col_count = sqlite3_column_count(&stmt);
    let mut results = Vec::new();
    loop {
        match sqlite3_step(&mut stmt) {
            Ok(StepResult::Row) => {
                let mut row = Vec::new();
                for i in 0..col_count {
                    row.push(sqlite3_column_text(&stmt, i));
                }
                results.push(row);
            }
            Ok(StepResult::Done) => break,
            Err(e) => {
                eprintln!("Error querying '{}': {:?}", sql, e);
                break;
            }
        }
    }
    let _ = sqlite3_finalize(stmt);
    results
}

fn query_flat(conn: &mut SqliteConnection, sql: &str) -> Vec<String> {
    query(conn, sql).into_iter().flatten().collect()
}

#[test]
fn test_scalar_subquery_standalone() {
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // Setup
    exec(&mut conn, "CREATE TABLE t1(w int, x int, y int)");
    exec(&mut conn, "INSERT INTO t1 VALUES(1, 0, 4)");
    exec(&mut conn, "INSERT INTO t1 VALUES(2, 1, 9)");
    exec(&mut conn, "INSERT INTO t1 VALUES(3, 1, 16)");

    // Standalone scalar subquery should return max(y) = 16
    let result = query_flat(&mut conn, "SELECT max(y) FROM t1");
    println!("max(y) result: {:?}", result);
    assert_eq!(result, vec!["16"], "max(y) should be 16");

    let _ = sqlite3_close(conn);
}

#[test]
fn test_scalar_subquery_in_expression() {
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // Setup
    exec(&mut conn, "CREATE TABLE t1(w int, x int, y int)");
    exec(&mut conn, "INSERT INTO t1 VALUES(1, 0, 4)");
    exec(&mut conn, "INSERT INTO t1 VALUES(2, 1, 9)");
    exec(&mut conn, "INSERT INTO t1 VALUES(3, 1, 16)");

    // Scalar subquery in expression: (SELECT max(y) FROM t1) + 1 = 17
    let result = query_flat(&mut conn, "SELECT (SELECT max(y) FROM t1) + 1");
    println!("(SELECT max(y)) + 1 result: {:?}", result);
    assert_eq!(result, vec!["17"], "(SELECT max(y)) + 1 should be 17");

    let _ = sqlite3_close(conn);
}

#[test]
fn test_insert_select_with_scalar_subquery() {
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // Setup
    exec(&mut conn, "CREATE TABLE t1(w int, x int, y int)");
    exec(&mut conn, "INSERT INTO t1 VALUES(1, 0, 4)");
    exec(&mut conn, "INSERT INTO t1 VALUES(2, 1, 9)");
    exec(&mut conn, "INSERT INTO t1 VALUES(3, 1, 16)");

    // First, confirm max(y) = 16
    let max_result = query_flat(&mut conn, "SELECT max(y) FROM t1");
    println!("max(y) = {:?}", max_result);
    assert_eq!(max_result, vec!["16"]);

    // First, test a simpler INSERT...SELECT without subquery
    exec(&mut conn, "CREATE TABLE t_simple(a int, b int)");
    exec(&mut conn, "INSERT INTO t_simple SELECT w, y FROM t1");
    let simple_results = query(&mut conn, "SELECT a, b FROM t_simple ORDER BY a");
    println!("Simple INSERT...SELECT results: {:?}", simple_results);
    assert_eq!(
        simple_results.len(),
        3,
        "Simple INSERT...SELECT should work"
    );

    // Test just the SELECT with scalar subquery
    let sub_results = query(&mut conn, "SELECT w, (SELECT max(y) FROM t1) - y FROM t1");
    println!("SELECT with scalar subquery: {:?}", sub_results);
    assert_eq!(
        sub_results.len(),
        3,
        "SELECT with scalar subquery should return 3 rows"
    );

    // INSERT...SELECT with scalar subquery
    // max(y) = 16
    // For each row: (SELECT max(y) FROM t1) - y
    // Row 1: 16 - 4 = 12
    // Row 2: 16 - 9 = 7
    // Row 3: 16 - 16 = 0
    exec(&mut conn, "CREATE TABLE t2(a int, b int)");
    exec(
        &mut conn,
        "INSERT INTO t2 SELECT w, (SELECT max(y) FROM t1) - y FROM t1",
    );

    // Check results
    let results = query(&mut conn, "SELECT a, b FROM t2 ORDER BY a");
    println!("Results: {:?}", results);

    assert_eq!(results.len(), 3, "Should have 3 rows");
    assert_eq!(
        results[0],
        vec!["1", "12"],
        "Row 1: w=1, max(y)-y = 16-4 = 12"
    );
    assert_eq!(
        results[1],
        vec!["2", "7"],
        "Row 2: w=2, max(y)-y = 16-9 = 7"
    );
    assert_eq!(
        results[2],
        vec!["3", "0"],
        "Row 3: w=3, max(y)-y = 16-16 = 0"
    );

    let _ = sqlite3_close(conn);
}

#[test]
fn test_where_test_setup() {
    // This test mimics the where.test setup from SQLite
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // Create tables
    exec(&mut conn, "CREATE TABLE t1(w int, x int, y int)");
    exec(&mut conn, "CREATE TABLE t2(p int, q int, r int, s int)");

    // Insert data into t1 - 5 rows for simplicity
    // w, x, y where y = w*w + 2*w + 1 = (w+1)^2
    exec(&mut conn, "INSERT INTO t1 VALUES(1, 0, 4)"); // y = 4
    exec(&mut conn, "INSERT INTO t1 VALUES(2, 1, 9)"); // y = 9
    exec(&mut conn, "INSERT INTO t1 VALUES(3, 1, 16)"); // y = 16
    exec(&mut conn, "INSERT INTO t1 VALUES(4, 2, 25)"); // y = 25
    exec(&mut conn, "INSERT INTO t1 VALUES(5, 2, 36)"); // y = 36

    // Check max(y)
    let max_y = query_flat(&mut conn, "SELECT max(y) FROM t1");
    println!("max(y) = {:?}", max_y);
    assert_eq!(max_y, vec!["36"]);

    // INSERT INTO t2 SELECT 101-w, x, (SELECT max(y) FROM t1)+1-y, y FROM t1
    // For row (1, 0, 4): (100, 0, 37-4, 4) = (100, 0, 33, 4)
    // For row (2, 1, 9): (99, 1, 37-9, 9) = (99, 1, 28, 9)
    // For row (3, 1, 16): (98, 1, 37-16, 16) = (98, 1, 21, 16)
    // For row (4, 2, 25): (97, 2, 37-25, 25) = (97, 2, 12, 25)
    // For row (5, 2, 36): (96, 2, 37-36, 36) = (96, 2, 1, 36)
    exec(
        &mut conn,
        "INSERT INTO t2 SELECT 101-w, x, (SELECT max(y) FROM t1)+1-y, y FROM t1",
    );

    // Check t2 contents
    let t2_rows = query(&mut conn, "SELECT p, q, r, s FROM t2 ORDER BY p");
    println!("t2 rows: {:?}", t2_rows);

    assert_eq!(t2_rows.len(), 5, "t2 should have 5 rows");
    assert_eq!(t2_rows[0], vec!["96", "2", "1", "36"]);
    assert_eq!(t2_rows[1], vec!["97", "2", "12", "25"]);
    assert_eq!(t2_rows[2], vec!["98", "1", "21", "16"]);
    assert_eq!(t2_rows[3], vec!["99", "1", "28", "9"]);
    assert_eq!(t2_rows[4], vec!["100", "0", "33", "4"]);

    let _ = sqlite3_close(conn);
}
