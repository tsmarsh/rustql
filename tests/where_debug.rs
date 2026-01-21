//! Debug tests for WHERE clause issues

use rustql::types::StepResult;
use rustql::vdbe::{get_sort_flag, reset_search_count, reset_sort_flag};
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
fn test_insert_select_arithmetic_simple() {
    // Test INSERT...SELECT with simple arithmetic (no subqueries)
    // This mimics where-6.1 setup: INSERT INTO t3 SELECT w, 101-w, y FROM t1
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // Setup t1
    exec(&mut conn, "CREATE TABLE t1(w int, x int, y int)");
    exec(&mut conn, "INSERT INTO t1 VALUES(1, 0, 4)");
    exec(&mut conn, "INSERT INTO t1 VALUES(2, 1, 9)");
    exec(&mut conn, "INSERT INTO t1 VALUES(3, 1, 16)");

    // Create t3 with arithmetic expression 101-w
    exec(&mut conn, "CREATE TABLE t3(a int, b int, c int)");
    exec(&mut conn, "INSERT INTO t3 SELECT w, 101-w, y FROM t1");

    // For w=1: a=1, b=100, c=4
    // For w=2: a=2, b=99, c=9
    // For w=3: a=3, b=98, c=16
    let results = query(&mut conn, "SELECT a, b, c FROM t3 ORDER BY a");
    println!("t3 results: {:?}", results);

    assert_eq!(results.len(), 3, "t3 should have 3 rows");
    assert_eq!(
        results[0],
        vec!["1", "100", "4"],
        "Row 1: a=1, b=101-1=100, c=4"
    );
    assert_eq!(
        results[1],
        vec!["2", "99", "9"],
        "Row 2: a=2, b=101-2=99, c=9"
    );
    assert_eq!(
        results[2],
        vec!["3", "98", "16"],
        "Row 3: a=3, b=101-3=98, c=16"
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

#[test]
fn test_left_join_empty_table() {
    // Test LEFT JOIN with empty right table (where-18.1)
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    exec(&mut conn, "CREATE TABLE t181(a)");
    exec(&mut conn, "CREATE TABLE t182(b, c)");
    exec(&mut conn, "INSERT INTO t181 VALUES(1)");
    // Note: t182 is empty

    // First, verify basic query works
    let basic_result = query(&mut conn, "SELECT a FROM t181");
    println!("Basic SELECT result: {:?}", basic_result);
    assert_eq!(basic_result, vec![vec!["1"]], "Basic SELECT should work");

    // LEFT JOIN should return the left row with NULLs for the right side
    // We select *, to see all columns
    let result_star = query(&mut conn, "SELECT * FROM t181 LEFT JOIN t182 ON a=b");
    println!("LEFT JOIN SELECT * result: {:?}", result_star);

    // Now select just a
    let result = query(&mut conn, "SELECT a FROM t181 LEFT JOIN t182 ON a=b");
    println!("LEFT JOIN SELECT a result: {:?}", result);
    assert_eq!(result.len(), 1, "Should have one row");
    assert_eq!(result[0][0], "1", "First column should be 1");

    let _ = sqlite3_close(conn);
}

#[test]
fn test_sqlite_search_count() {
    // Test that sqlite_search_count() function works
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // Setup
    exec(&mut conn, "CREATE TABLE t1(w int, x int, y int)");
    exec(&mut conn, "CREATE INDEX i1w ON t1(w)");
    exec(&mut conn, "INSERT INTO t1 VALUES(1, 0, 4)");
    exec(&mut conn, "INSERT INTO t1 VALUES(2, 1, 9)");
    exec(&mut conn, "INSERT INTO t1 VALUES(3, 1, 16)");

    // Reset the search count before our test query
    reset_search_count();

    // Run a query that should use the index
    let result = query_flat(&mut conn, "SELECT x, y, w FROM t1 WHERE w=2");
    println!("Query result: {:?}", result);
    assert_eq!(result, vec!["1", "9", "2"]);

    // Check the search count using the SQL function
    let count_result = query_flat(&mut conn, "SELECT sqlite_search_count()");
    println!("Search count: {:?}", count_result);

    // The search count should be > 0 (includes seek and possibly next operations)
    let count: i64 = count_result[0].parse().unwrap();
    assert!(count > 0, "Search count should be > 0, got {}", count);

    let _ = sqlite3_close(conn);
}

#[test]
fn test_join_with_non_empty_tables() {
    // Test joins where both tables have data
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // Setup - similar to where.test where-2.x
    exec(&mut conn, "CREATE TABLE t1(w int, x int, y int)");
    exec(&mut conn, "CREATE TABLE t2(p int, q int, r int, s int)");
    exec(&mut conn, "INSERT INTO t1 VALUES(1, 0, 4)");
    exec(&mut conn, "INSERT INTO t1 VALUES(2, 1, 9)");
    exec(&mut conn, "INSERT INTO t1 VALUES(3, 1, 16)");
    exec(&mut conn, "INSERT INTO t2 VALUES(100, 0, 10197, 4)");
    exec(&mut conn, "INSERT INTO t2 VALUES(99, 1, 10192, 9)");
    exec(&mut conn, "INSERT INTO t2 VALUES(98, 1, 10185, 16)");

    // Simple inner join
    let result = query(
        &mut conn,
        "SELECT w, p FROM t1, t2 WHERE x=q AND y=s ORDER BY w",
    );
    println!("Join result: {:?}", result);
    assert!(
        result.len() >= 1,
        "Should have at least one join result, got {:?}",
        result
    );

    let _ = sqlite3_close(conn);
}

#[test]
fn test_left_join_with_data() {
    // Test LEFT JOIN where right table has matching data
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    exec(&mut conn, "CREATE TABLE t181(a)");
    exec(&mut conn, "CREATE TABLE t182(b, c)");
    exec(&mut conn, "INSERT INTO t181 VALUES(1)");
    exec(&mut conn, "INSERT INTO t181 VALUES(2)");
    exec(&mut conn, "INSERT INTO t182 VALUES(1, 'match')");
    // No match for a=2

    // Check data
    let t181_data = query(&mut conn, "SELECT * FROM t181");
    println!("t181 data: {:?}", t181_data);
    let t182_data = query(&mut conn, "SELECT * FROM t182");
    println!("t182 data: {:?}", t182_data);

    // Try simpler query first: LEFT JOIN without ORDER BY
    let simple_result = query(&mut conn, "SELECT a, b, c FROM t181 LEFT JOIN t182 ON a=b");
    println!("LEFT JOIN without ORDER BY: {:?}", simple_result);

    // LEFT JOIN should return both rows from t181
    let result = query(
        &mut conn,
        "SELECT a, b, c FROM t181 LEFT JOIN t182 ON a=b ORDER BY a",
    );
    println!("LEFT JOIN with data result: {:?}", result);
    assert_eq!(result.len(), 2, "Should have 2 rows");
    // Row 1: a=1 should match b=1
    assert_eq!(result[0][0], "1");
    assert_eq!(result[0][1], "1");
    assert_eq!(result[0][2], "match");
    // Row 2: a=2 should have NULLs for b and c
    assert_eq!(result[1][0], "2");
    // b and c should be NULL (empty string in our representation)
    assert!(
        result[1][1] == "" || result[1][1] == "NULL",
        "b should be NULL for unmatched row"
    );

    let _ = sqlite3_close(conn);
}

#[test]
fn test_three_way_join() {
    // Test 3-way join - similar to where-3.x
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    exec(&mut conn, "CREATE TABLE t1(w int, x int, y int)");
    exec(&mut conn, "INSERT INTO t1 VALUES(1, 0, 4)");
    exec(&mut conn, "INSERT INTO t1 VALUES(2, 1, 9)");
    exec(&mut conn, "INSERT INTO t1 VALUES(3, 1, 16)");

    // 3-way self-join
    let result = query(
        &mut conn,
        "SELECT A.w, B.w, C.w FROM t1 as A, t1 as B, t1 as C
         WHERE A.w = B.w AND B.w = C.w ORDER BY A.w",
    );
    println!("3-way join result: {:?}", result);
    assert_eq!(result.len(), 3, "Should have 3 rows (one for each match)");
    assert_eq!(result[0], vec!["1", "1", "1"]);
    assert_eq!(result[1], vec!["2", "2", "2"]);
    assert_eq!(result[2], vec!["3", "3", "3"]);

    let _ = sqlite3_close(conn);
}

#[test]
fn test_sort_flag_with_order_by_expression() {
    // Test that the sort flag is set when ORDER BY requires sorting
    // (i.e., when ORDER BY expression doesn't match an index)
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // Setup
    exec(&mut conn, "CREATE TABLE t3(a int, b int, c int)");
    exec(&mut conn, "CREATE INDEX i3a ON t3(a)");
    exec(&mut conn, "INSERT INTO t3 VALUES(1, 100, 4)");
    exec(&mut conn, "INSERT INTO t3 VALUES(2, 99, 9)");
    exec(&mut conn, "INSERT INTO t3 VALUES(3, 98, 16)");

    // ORDER BY a should NOT require sorting (index can provide order)
    reset_sort_flag();
    let result = query_flat(&mut conn, "SELECT * FROM t3 ORDER BY a LIMIT 3");
    println!("ORDER BY a result: {:?}", result);
    let sort_for_order_a = get_sort_flag();
    println!("Sort flag for ORDER BY a: {}", sort_for_order_a);

    // ORDER BY a+1 SHOULD require sorting (expression doesn't match index)
    reset_sort_flag();
    let result2 = query_flat(&mut conn, "SELECT * FROM t3 ORDER BY a+1 LIMIT 3");
    println!("ORDER BY a+1 result: {:?}", result2);
    let sort_for_order_a_plus_1 = get_sort_flag();
    println!("Sort flag for ORDER BY a+1: {}", sort_for_order_a_plus_1);

    // The key test: ORDER BY a+1 should trigger a sort, ORDER BY a should not
    // (assuming the query planner can use the index for ORDER BY a)
    assert!(
        sort_for_order_a_plus_1,
        "ORDER BY a+1 should require sorting"
    );

    let _ = sqlite3_close(conn);
}

#[test]
fn test_correlated_subquery_no_from() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();

    // Create test table
    exec(&mut conn, "CREATE TABLE t1(a,b)");
    exec(&mut conn, "INSERT INTO t1 VALUES(1,2)");
    exec(&mut conn, "INSERT INTO t1 VALUES(3,4)");

    // Test simple correlated subquery with no FROM clause
    let result = query(&mut conn, "SELECT a, (SELECT a) FROM t1");
    println!("(SELECT a) result: {:?}", result);

    // Expected: [(1, 1), (3, 3)]
    assert_eq!(result.len(), 2, "Should return 2 rows");
    assert_eq!(result[0], vec!["1", "1"], "Row 1 should be (1, 1)");
    assert_eq!(result[1], vec!["3", "3"], "Row 2 should be (3, 3)");

    let _ = sqlite3_close(conn);
}

#[test]
fn test_correlated_subquery_with_other_column() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();

    // Create test table
    exec(&mut conn, "CREATE TABLE t1(a,b)");
    exec(&mut conn, "INSERT INTO t1 VALUES(1,2)");
    exec(&mut conn, "INSERT INTO t1 VALUES(3,4)");

    // Test correlated subquery AND another column from same table
    let result = query(&mut conn, "SELECT (SELECT a), b FROM t1");
    println!("(SELECT a), b result: {:?}", result);

    // Expected: [(1, 2), (3, 4)]
    assert_eq!(result.len(), 2, "Should return 2 rows");
    assert_eq!(result[0], vec!["1", "2"], "Row 1 should be (1, 2)");
    assert_eq!(result[1], vec!["3", "4"], "Row 2 should be (3, 4)");

    let _ = sqlite3_close(conn);
}
