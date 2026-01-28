//! Debug tests for WHERE clause issues

use rustql::types::StepResult;
use rustql::vdbe::{get_sort_flag, reset_search_count, reset_sort_count};
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
fn test_ambiguous_column_resolution() {
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    exec(&mut conn, "CREATE TABLE t1(x int)");
    exec(&mut conn, "CREATE TABLE t2(x int)");
    exec(&mut conn, "INSERT INTO t1 VALUES(1)");
    exec(&mut conn, "INSERT INTO t2 VALUES(2)");

    let err = sqlite3_prepare_v2(&mut conn, "SELECT x FROM t1, t2")
        .err()
        .expect("expected prepare to fail for ambiguous column");
    assert!(
        err.to_string().contains("ambiguous"),
        "expected ambiguous column error, got: {}",
        err
    );

    let results = query(&mut conn, "SELECT t1.x, t2.x FROM t1, t2");
    assert_eq!(results, vec![vec!["1".to_string(), "2".to_string()]]);

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
#[ignore] // TODO: Sort flag not set for ORDER BY expression - needs investigation
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
    reset_sort_count();
    let result = query_flat(&mut conn, "SELECT * FROM t3 ORDER BY a LIMIT 3");
    println!("ORDER BY a result: {:?}", result);
    let sort_for_order_a = get_sort_flag();
    println!("Sort flag for ORDER BY a: {}", sort_for_order_a);

    // ORDER BY a+1 SHOULD require sorting (expression doesn't match index)
    reset_sort_count();
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

#[test]
fn test_expr_transaction() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();

    // Setup like expr.test
    exec(
        &mut conn,
        "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)",
    );
    exec(
        &mut conn,
        "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')",
    );

    // Test multi-statement with transaction
    let result = query(
        &mut conn,
        "BEGIN; UPDATE test1 SET i1=10, i2=20; SELECT i1-i2 FROM test1; ROLLBACK;",
    );
    println!("Transaction test result: {:?}", result);

    // Also test individual statements
    exec(&mut conn, "BEGIN");
    exec(&mut conn, "UPDATE test1 SET i1=10, i2=20");
    let result2 = query(&mut conn, "SELECT i1-i2 FROM test1");
    println!("After UPDATE result: {:?}", result2);
    exec(&mut conn, "ROLLBACK");

    let _ = sqlite3_close(conn);
}

#[test]
fn test_expr_minus_vs_plus() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();

    exec(&mut conn, "CREATE TABLE test1(i1 int, i2 int)");
    exec(&mut conn, "INSERT INTO test1 VALUES(1,2)");

    // Test simple addition and subtraction
    let plus = query(&mut conn, "SELECT i1+i2 FROM test1");
    println!("i1+i2: {:?}", plus);

    let minus = query(&mut conn, "SELECT i1-i2 FROM test1");
    println!("i1-i2: {:?}", minus);

    // Test after update
    exec(&mut conn, "UPDATE test1 SET i1=10, i2=20");
    let plus_after = query(&mut conn, "SELECT i1+i2 FROM test1");
    println!("After update i1+i2: {:?}", plus_after);

    let minus_after = query(&mut conn, "SELECT i1-i2 FROM test1");
    println!("After update i1-i2: {:?}", minus_after);

    let _ = sqlite3_close(conn);
}

#[test]
fn test_expr_multi_statement_rollback() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();

    exec(&mut conn, "CREATE TABLE test1(i1 int, i2 int)");
    exec(&mut conn, "INSERT INTO test1 VALUES(1,2)");

    // Simulate what test_expr does
    exec(&mut conn, "BEGIN");
    exec(&mut conn, "UPDATE test1 SET i1=10, i2=20");
    let r1 = query(&mut conn, "SELECT i1+i2 FROM test1");
    println!("Test 1 (i1+i2): {:?}", r1);
    exec(&mut conn, "ROLLBACK");

    // Now try second "test"
    exec(&mut conn, "BEGIN");
    exec(&mut conn, "UPDATE test1 SET i1=10, i2=20");
    let r2 = query(&mut conn, "SELECT i1-i2 FROM test1");
    println!("Test 2 (i1-i2): {:?}", r2);
    exec(&mut conn, "ROLLBACK");

    // And third
    exec(&mut conn, "BEGIN");
    exec(&mut conn, "UPDATE test1 SET i1=10, i2=20");
    let r3 = query(&mut conn, "SELECT i1*i2 FROM test1");
    println!("Test 3 (i1*i2): {:?}", r3);
    exec(&mut conn, "ROLLBACK");

    // Check table still exists with original data
    let check = query(&mut conn, "SELECT * FROM test1");
    println!("Final table state: {:?}", check);

    let _ = sqlite3_close(conn);
}

#[test]
fn test_rollback_behavior() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();

    exec(&mut conn, "CREATE TABLE test1(i1 int, i2 int)");
    exec(&mut conn, "INSERT INTO test1 VALUES(1,2)");

    let before = query(&mut conn, "SELECT * FROM test1");
    println!("Before BEGIN: {:?}", before);
    assert_eq!(before, vec![vec!["1".to_string(), "2".to_string()]]);

    exec(&mut conn, "BEGIN");
    let in_txn = query(&mut conn, "SELECT * FROM test1");
    println!("In transaction (no changes): {:?}", in_txn);
    assert_eq!(in_txn, vec![vec!["1".to_string(), "2".to_string()]]);

    exec(&mut conn, "UPDATE test1 SET i1=10, i2=20");
    let after_update = query(&mut conn, "SELECT * FROM test1");
    println!("After UPDATE: {:?}", after_update);
    assert_eq!(after_update, vec![vec!["10".to_string(), "20".to_string()]]);

    exec(&mut conn, "ROLLBACK");
    let after_rollback = query(&mut conn, "SELECT * FROM test1");
    println!("After ROLLBACK: {:?}", after_rollback);
    // CRITICAL: After ROLLBACK, data should be restored to pre-BEGIN state
    assert_eq!(
        after_rollback,
        vec![vec!["1".to_string(), "2".to_string()]],
        "ROLLBACK should restore data to pre-transaction state"
    );

    let _ = sqlite3_close(conn);
}

#[test]
fn test_simple_update_no_txn() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();

    exec(&mut conn, "CREATE TABLE test1(i1 int, i2 int)");
    exec(&mut conn, "INSERT INTO test1 VALUES(1,2)");

    let before = query(&mut conn, "SELECT * FROM test1");
    println!("Before UPDATE: {:?}", before);

    // Simple UPDATE without explicit transaction
    exec(&mut conn, "UPDATE test1 SET i1=10, i2=20");

    let after = query(&mut conn, "SELECT * FROM test1");
    println!("After UPDATE: {:?}", after);

    let _ = sqlite3_close(conn);
}

/// Test cursor stability when DELETE occurs during iteration.
/// This tests that when a nested DELETE modifies a table being iterated,
/// the outer cursor detects the modification and handles it correctly.
#[test]
fn test_cursor_stability_delete_during_iteration() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();

    // Setup tables
    exec(&mut conn, "CREATE TABLE t5(x, y)");
    exec(&mut conn, "CREATE TABLE t6(c, d)");
    exec(&mut conn, "INSERT INTO t5 VALUES(1, 2)");
    exec(&mut conn, "INSERT INTO t5 VALUES(3, 4)");
    exec(&mut conn, "INSERT INTO t5 VALUES(5, 6)");
    exec(&mut conn, "INSERT INTO t6 VALUES('a', 'b')");
    exec(&mut conn, "INSERT INTO t6 VALUES('c', 'd')");

    // Query that would normally return 6 rows (3 t5 rows * 2 t6 rows)
    // But we'll delete t5 after seeing row 2, which should affect results
    let rows = query(&mut conn, "SELECT t5.rowid AS r, c, d FROM t5, t6");
    println!("Cross join without delete: {:?}", rows);
    assert_eq!(rows.len(), 6, "Cross join should return 6 rows");

    // Now test that after deleting t5, subsequent queries see empty table
    exec(&mut conn, "DELETE FROM t5");
    let after_delete = query(&mut conn, "SELECT * FROM t5");
    assert!(after_delete.is_empty(), "t5 should be empty after DELETE");

    let _ = sqlite3_close(conn);
}

/// Test that cursor detects when page becomes empty during iteration.
/// This is a lower-level test of the btree cursor staleness detection.
#[test]
fn test_cursor_page_refresh_on_stale() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();

    exec(&mut conn, "CREATE TABLE t1(a)");
    exec(&mut conn, "INSERT INTO t1 VALUES(1)");
    exec(&mut conn, "INSERT INTO t1 VALUES(2)");
    exec(&mut conn, "INSERT INTO t1 VALUES(3)");

    // First, verify the table has 3 rows
    let rows = query(&mut conn, "SELECT * FROM t1");
    assert_eq!(rows.len(), 3);

    // Delete all and verify empty
    exec(&mut conn, "DELETE FROM t1");
    let rows = query(&mut conn, "SELECT * FROM t1");
    assert!(rows.is_empty());

    // Re-insert and verify
    exec(&mut conn, "INSERT INTO t1 VALUES(10)");
    let rows = query(&mut conn, "SELECT * FROM t1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "10");

    let _ = sqlite3_close(conn);
}

/// Test ORDER BY uses index scan when index matches the ORDER BY column.
/// This is important for cursor stability during DELETE operations.
#[test]
fn test_order_by_uses_index_scan() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();

    // Create table with index on column 'a'
    exec(&mut conn, "CREATE TABLE t1(a, b)");
    exec(&mut conn, "CREATE INDEX i1 ON t1(a)");

    // Insert data in non-sorted order
    exec(&mut conn, "INSERT INTO t1 VALUES(3, 'three')");
    exec(&mut conn, "INSERT INTO t1 VALUES(1, 'one')");
    exec(&mut conn, "INSERT INTO t1 VALUES(2, 'two')");

    // Query with ORDER BY a - should use index scan
    let rows = query(&mut conn, "SELECT a, b FROM t1 ORDER BY a");
    println!("ORDER BY a results: {:?}", rows);

    // Results should be in order by 'a'
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[1][0], "2");
    assert_eq!(rows[2][0], "3");

    let _ = sqlite3_close(conn);
}

/// Test ORDER BY with cross join uses index scan on the correct table.
/// This mimics the delete-9 test structure.
#[test]
fn test_order_by_cross_join_index_scan() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();

    // Create tables like delete-9 tests
    exec(&mut conn, "CREATE TABLE t5(a, b)");
    exec(&mut conn, "CREATE TABLE t6(c, d)");
    exec(&mut conn, "CREATE INDEX i5 ON t5(a)");

    // Insert data (t5.a in non-sorted order)
    exec(&mut conn, "INSERT INTO t5 VALUES(3, 'three')");
    exec(&mut conn, "INSERT INTO t5 VALUES(1, 'one')");
    exec(&mut conn, "INSERT INTO t5 VALUES(2, 'two')");
    exec(&mut conn, "INSERT INTO t6 VALUES('x', 'X')");
    exec(&mut conn, "INSERT INTO t6 VALUES('y', 'Y')");

    // Cross join with ORDER BY a - should use index scan on t5
    let rows = query(
        &mut conn,
        "SELECT t5.rowid AS r, a, c FROM t5, t6 ORDER BY a",
    );
    println!("Cross join ORDER BY a results: {:?}", rows);

    // Results should be 6 rows (3 * 2), ordered by 'a'
    // Each 'a' value should appear twice (once for each t6 row)
    assert_eq!(rows.len(), 6);

    // First two rows should have a=1
    assert_eq!(rows[0][1], "1");
    assert_eq!(rows[1][1], "1");
    // Next two rows should have a=2
    assert_eq!(rows[2][1], "2");
    assert_eq!(rows[3][1], "2");
    // Last two rows should have a=3
    assert_eq!(rows[4][1], "3");
    assert_eq!(rows[5][1], "3");

    let _ = sqlite3_close(conn);
}

/// Test delete-9.2 scenario: DELETE ALL during cross join iteration with index scan
/// This tests cursor stability with ORDER BY index scans
#[test]
fn test_delete_all_during_cross_join_index_scan() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();

    // Setup exactly like delete-9
    exec(&mut conn, "CREATE TABLE t5(a, b)");
    exec(&mut conn, "CREATE TABLE t6(c, d)");
    exec(&mut conn, "INSERT INTO t5 VALUES(1, 2)");
    exec(&mut conn, "INSERT INTO t5 VALUES(3, 4)");
    exec(&mut conn, "INSERT INTO t5 VALUES(5, 6)");
    exec(&mut conn, "INSERT INTO t6 VALUES('a', 'b')");
    exec(&mut conn, "INSERT INTO t6 VALUES('c', 'd')");
    exec(&mut conn, "CREATE INDEX i5 ON t5(a)");
    exec(&mut conn, "CREATE INDEX i6 ON t6(c)");

    // Run query with DELETE mid-iteration
    // SELECT t5.rowid AS r, c, d FROM t5, t6 ORDER BY a
    // When r==2, DELETE FROM t5
    let (mut stmt, _) = sqlite3_prepare_v2(
        &mut conn,
        "SELECT t5.rowid AS r, c, d FROM t5, t6 ORDER BY a",
    )
    .unwrap();

    let mut results = Vec::new();
    loop {
        match sqlite3_step(&mut stmt) {
            Ok(StepResult::Row) => {
                let r = sqlite3_column_text(&stmt, 0);
                let c = sqlite3_column_text(&stmt, 1);
                let d = sqlite3_column_text(&stmt, 2);
                println!("Got row: r={}, c={}, d={}", r, c, d);

                // When r==2, execute DELETE FROM t5
                if r == "2" {
                    println!("Deleting all from t5...");
                    exec(&mut conn, "DELETE FROM t5");
                }

                results.push((r, c, d));
            }
            Ok(StepResult::Done) => break,
            Err(e) => {
                println!("Error: {:?}", e);
                break;
            }
        }
    }
    let _ = sqlite3_finalize(stmt);

    println!("Final results: {:?}", results);

    // Expected: After DELETE, subsequent reads should return NULL for t5 columns
    // Row 1: r=1, c=a, d=b (rowid 1, a=1)
    // Row 2: r=1, c=c, d=d (rowid 1, a=1, second t6 row)
    // Row 3: r=2, c=a, d=b (rowid 2, a=3) - DELETE happens here
    // Row 4: r="", c=c, d=d (t5 empty, r should be NULL/empty)
    assert_eq!(
        results.len(),
        4,
        "Should have 4 rows (iteration stops after DELETE)"
    );
    assert_eq!(results[0].0, "1");
    assert_eq!(results[1].0, "1");
    assert_eq!(results[2].0, "2"); // DELETE happens after reading this
                                   // After DELETE, the fourth row's r should be NULL (empty string in our output)
    assert!(
        results[3].0.is_empty() || results[3].0 == "",
        "After DELETE, t5.rowid should be NULL but got: {}",
        results[3].0
    );

    let _ = sqlite3_close(conn);
}

/// Test delete-9.3 scenario: DELETE single row during cross join iteration
/// Should continue iterating to remaining rows after single-row delete
#[test]
fn test_delete_single_row_during_cross_join_index_scan() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();

    // Setup exactly like delete-9
    exec(&mut conn, "CREATE TABLE t5(a, b)");
    exec(&mut conn, "CREATE TABLE t6(c, d)");
    exec(&mut conn, "INSERT INTO t5 VALUES(1, 2)");
    exec(&mut conn, "INSERT INTO t5 VALUES(3, 4)");
    exec(&mut conn, "INSERT INTO t5 VALUES(5, 6)");
    exec(&mut conn, "INSERT INTO t6 VALUES('a', 'b')");
    exec(&mut conn, "INSERT INTO t6 VALUES('c', 'd')");
    exec(&mut conn, "CREATE INDEX i5 ON t5(a)");
    exec(&mut conn, "CREATE INDEX i6 ON t6(c)");

    // Run query with DELETE of single row mid-iteration
    // SELECT t5.rowid AS r, c, d FROM t5, t6 ORDER BY a
    // When r==2, DELETE FROM t5 WHERE rowid = 2
    let (mut stmt, _) = sqlite3_prepare_v2(
        &mut conn,
        "SELECT t5.rowid AS r, c, d FROM t5, t6 ORDER BY a",
    )
    .unwrap();

    let mut results = Vec::new();
    loop {
        match sqlite3_step(&mut stmt) {
            Ok(StepResult::Row) => {
                let r = sqlite3_column_text(&stmt, 0);
                let c = sqlite3_column_text(&stmt, 1);
                let d = sqlite3_column_text(&stmt, 2);
                println!("Got row: r={}, c={}, d={}", r, c, d);

                // When r==2, execute DELETE FROM t5 WHERE rowid = 2
                if r == "2" {
                    println!("Deleting rowid 2 from t5...");
                    exec(&mut conn, "DELETE FROM t5 WHERE rowid = 2");
                }

                results.push((r, c, d));
            }
            Ok(StepResult::Done) => break,
            Err(e) => {
                println!("Error: {:?}", e);
                break;
            }
        }
    }
    let _ = sqlite3_finalize(stmt);

    println!("Final results: {:?}", results);

    // Expected: 6 rows total
    // Rows 1-2: r=1, c=a/c, d=b/d (rowid 1, a=1)
    // Rows 3-4: r=2, c=a/c, d=b/d (rowid 2, a=3) - DELETE happens on row 3
    //           Row 4 should have r=NULL since rowid 2 is deleted
    // Rows 5-6: r=3, c=a/c, d=b/d (rowid 3, a=5) - should still iterate
    assert_eq!(
        results.len(),
        6,
        "Should have 6 rows (iteration continues after single delete)"
    );
    assert_eq!(results[0].0, "1");
    assert_eq!(results[1].0, "1");
    assert_eq!(results[2].0, "2"); // DELETE happens after reading this
                                   // After DELETE, the fourth row's r should be NULL (deleted row)
    assert!(
        results[3].0.is_empty(),
        "After DELETE, row 4 should have NULL rowid but got: {}",
        results[3].0
    );
    // Rows 5-6 should be from rowid 3
    assert_eq!(results[4].0, "3", "Row 5 should have rowid 3");
    assert_eq!(results[5].0, "3", "Row 6 should have rowid 3");

    let _ = sqlite3_close(conn);
}

/// Test bulk DELETE from large table (delete-6.5.1, delete-6.5.2, delete-6.6)
/// - Insert 3000 rows
/// - DELETE WHERE f1>7 should delete 2993 rows
/// - Only 7 rows should remain
#[test]
fn test_bulk_delete_large_table() {
    init();
    let mut conn = sqlite3_open(":memory:").expect("Failed to open database");

    exec(&mut conn, "CREATE TABLE table1(f1, f2)");

    // Testing threshold: 504 rows works, 505 fails
    let total_rows = 505;
    exec(&mut conn, "BEGIN");
    for i in 1..=total_rows {
        exec(
            &mut conn,
            &format!("INSERT INTO table1 VALUES({}, {})", i, i * i),
        );
    }
    exec(&mut conn, "COMMIT");

    // Verify rows inserted
    let rows = query(&mut conn, "SELECT count(*) FROM table1");
    assert_eq!(
        rows[0][0],
        total_rows.to_string(),
        "Should have {} rows after insert",
        total_rows
    );

    // First check SELECT WHERE f1>7 to see if cursor iteration works
    let rows = query(&mut conn, "SELECT count(*) FROM table1 WHERE f1>7");
    println!("Rows with f1>7 before DELETE: {}", rows[0][0]);
    let expected_to_delete = total_rows - 7;
    assert_eq!(
        rows[0][0],
        expected_to_delete.to_string(),
        "Should have {} rows with f1>7 before DELETE",
        expected_to_delete
    );

    // Check rows 1-10 before DELETE
    let rows = query(&mut conn, "SELECT f1 FROM table1 WHERE f1<=10 ORDER BY f1");
    let first_10: Vec<String> = rows.iter().map(|r| r[0].clone()).collect();
    println!("First 10 rows before DELETE: {:?}", first_10);

    // Try manual iteration with debugging
    // First, let's see what a SELECT iteration looks like
    let rows = query(
        &mut conn,
        "SELECT rowid, f1 FROM table1 ORDER BY rowid LIMIT 20",
    );
    println!("First 20 rows before DELETE (rowid, f1):");
    for row in &rows {
        println!("  rowid={}, f1={}", row[0], row[1]);
    }

    // Now do the DELETE
    exec(&mut conn, "DELETE FROM table1 WHERE f1>7");

    // Check what's left
    let rows = query(&mut conn, "SELECT rowid, f1 FROM table1 ORDER BY rowid");
    println!("All remaining rows after DELETE (rowid, f1):");
    for row in &rows {
        println!("  rowid={}, f1={}", row[0], row[1]);
    }

    let remaining: Vec<String> = rows.iter().map(|r| r[1].clone()).collect();
    println!("Remaining f1 values: {:?}", remaining);

    // Verify first 7 rows remain after DELETE f1>7
    let expected_rows: Vec<String> = (1..=7).map(|i| i.to_string()).collect();
    assert_eq!(
        remaining, expected_rows,
        "Only rows 1-7 should remain after DELETE f1>7"
    );

    let _ = sqlite3_close(conn);
}

/// Test INSERT after DELETE (delete-6.8, delete-6.10)
/// After clearing a table with DELETE, INSERT should work
#[test]
fn test_insert_after_delete() {
    init();
    let mut conn = sqlite3_open(":memory:").expect("Failed to open database");

    exec(&mut conn, "CREATE TABLE table1(f1, f2)");

    // Insert some rows
    exec(&mut conn, "INSERT INTO table1 VALUES(1, 2)");
    exec(&mut conn, "INSERT INTO table1 VALUES(3, 4)");
    exec(&mut conn, "INSERT INTO table1 VALUES(5, 6)");

    // Verify rows exist
    let rows = query(&mut conn, "SELECT count(*) FROM table1");
    assert_eq!(rows[0][0], "3", "Should have 3 rows");

    // DELETE all rows
    exec(&mut conn, "DELETE FROM table1");

    // Verify table is empty
    let rows = query(&mut conn, "SELECT count(*) FROM table1");
    assert_eq!(rows[0][0], "0", "Table should be empty after DELETE");

    // INSERT new row - this is what delete-6.8 tests
    exec(&mut conn, "INSERT INTO table1 VALUES(2, 3)");

    // Verify the new row
    let rows = query(&mut conn, "SELECT f1 FROM table1");
    assert_eq!(rows.len(), 1, "Should have 1 row after INSERT");
    assert_eq!(rows[0][0], "2", "Row should have f1=2");

    let _ = sqlite3_close(conn);
}

/// Mimics the exact TCL test delete-6.8 flow
#[test]
fn test_insert_after_bulk_delete() {
    init();
    let mut conn = sqlite3_open(":memory:").expect("Failed to open database");

    // Setup like delete.test
    exec(&mut conn, "CREATE TABLE table1(f1, f2)");

    // Insert 3000 rows like the TCL test
    exec(&mut conn, "BEGIN");
    for i in 1..=3000 {
        exec(
            &mut conn,
            &format!("INSERT INTO table1 VALUES({}, {})", i, i * i),
        );
    }
    exec(&mut conn, "COMMIT");
    println!("Inserted 3000 rows");

    // Verify count
    let rows = query(&mut conn, "SELECT count(*) FROM table1");
    println!("Count after insert: {}", rows[0][0]);
    assert_eq!(rows[0][0], "3000", "Should have 3000 rows");

    // delete-6.5.1: DELETE WHERE f1>7
    exec(&mut conn, "DELETE FROM table1 WHERE f1>7");
    println!("Deleted rows where f1>7");

    // delete-6.5.2: Verify only 7 rows remain
    let rows = query(&mut conn, "SELECT f1 FROM table1 ORDER BY f1");
    let vals: Vec<String> = rows.iter().map(|r| r[0].clone()).collect();
    println!("Remaining rows: {:?}", vals);
    assert_eq!(
        vals,
        vec!["1", "2", "3", "4", "5", "6", "7"],
        "Should have rows 1-7"
    );

    // delete-6.7: DELETE all remaining rows
    exec(&mut conn, "DELETE FROM table1");
    println!("Deleted all remaining rows");

    // Verify empty
    let rows = query(&mut conn, "SELECT count(*) FROM table1");
    println!("Count after delete all: {}", rows[0][0]);
    let rows = query(&mut conn, "SELECT f1 FROM table1");
    assert!(rows.is_empty(), "Table should be empty");

    // delete-6.8: INSERT new row
    println!("About to INSERT...");
    exec(&mut conn, "INSERT INTO table1 VALUES(2, 3)");
    println!("INSERT completed");

    // Verify the insert worked
    let rows = query(&mut conn, "SELECT f1 FROM table1");
    assert_eq!(rows.len(), 1, "Should have 1 row after INSERT");
    assert_eq!(rows[0][0], "2", "Row should have f1=2");

    let _ = sqlite3_close(conn);
}

/// Test to isolate the INSERT after bulk DELETE issue
#[test]
fn test_insert_after_bulk_delete_variations() {
    init();

    // Test 1: Just DELETE all at once
    {
        let mut conn = sqlite3_open(":memory:").expect("Failed to open database");
        exec(&mut conn, "CREATE TABLE t1(a, b)");
        exec(&mut conn, "BEGIN");
        for i in 1..=3000 {
            exec(
                &mut conn,
                &format!("INSERT INTO t1 VALUES({}, {})", i, i * i),
            );
        }
        exec(&mut conn, "COMMIT");
        println!("Test 1: Created 3000 rows");

        exec(&mut conn, "DELETE FROM t1");
        println!("Test 1: Deleted all rows");

        exec(&mut conn, "INSERT INTO t1 VALUES(1, 2)");
        let rows = query(&mut conn, "SELECT count(*) FROM t1");
        println!("Test 1: Count after insert = {}", rows[0][0]);
        let _ = sqlite3_close(conn);
    }

    // Test 2: DELETE with WHERE, then DELETE remaining
    {
        let mut conn = sqlite3_open(":memory:").expect("Failed to open database");
        exec(&mut conn, "CREATE TABLE t2(a, b)");
        exec(&mut conn, "BEGIN");
        for i in 1..=3000 {
            exec(
                &mut conn,
                &format!("INSERT INTO t2 VALUES({}, {})", i, i * i),
            );
        }
        exec(&mut conn, "COMMIT");
        println!("Test 2: Created 3000 rows");

        exec(&mut conn, "DELETE FROM t2 WHERE a > 7");
        println!("Test 2: Deleted rows where a > 7");

        exec(&mut conn, "DELETE FROM t2");
        println!("Test 2: Deleted remaining rows");

        exec(&mut conn, "INSERT INTO t2 VALUES(1, 2)");
        let rows = query(&mut conn, "SELECT count(*) FROM t2");
        println!("Test 2: Count after insert = {}", rows[0][0]);
        let _ = sqlite3_close(conn);
    }

    // Test 3: What if we don't do the second DELETE?
    {
        let mut conn = sqlite3_open(":memory:").expect("Failed to open database");
        exec(&mut conn, "CREATE TABLE t3(a, b)");
        exec(&mut conn, "BEGIN");
        for i in 1..=3000 {
            exec(
                &mut conn,
                &format!("INSERT INTO t3 VALUES({}, {})", i, i * i),
            );
        }
        exec(&mut conn, "COMMIT");
        println!("Test 3: Created 3000 rows");

        exec(&mut conn, "DELETE FROM t3 WHERE a > 7");
        println!("Test 3: Deleted rows where a > 7");

        // Skip the second DELETE, just insert
        exec(&mut conn, "INSERT INTO t3 VALUES(8, 64)");
        let rows = query(&mut conn, "SELECT count(*) FROM t3");
        println!("Test 3: Count = {} (expected 8)", rows[0][0]);
        let _ = sqlite3_close(conn);
    }

    // Test 4: Small table DELETE all + INSERT
    {
        let mut conn = sqlite3_open(":memory:").expect("Failed to open database");
        exec(&mut conn, "CREATE TABLE t4(a, b)");
        for i in 1..=10 {
            exec(
                &mut conn,
                &format!("INSERT INTO t4 VALUES({}, {})", i, i * i),
            );
        }
        println!("Test 4: Created 10 rows");

        exec(&mut conn, "DELETE FROM t4");
        println!("Test 4: Deleted all rows");

        exec(&mut conn, "INSERT INTO t4 VALUES(1, 2)");
        let rows = query(&mut conn, "SELECT count(*) FROM t4");
        println!("Test 4: Count after insert = {}", rows[0][0]);
        let _ = sqlite3_close(conn);
    }

    // Test 5: Find the threshold where DELETE all starts failing
    for total_rows in [10, 100, 200, 300, 400, 500, 1000, 2000, 3000] {
        let mut conn = sqlite3_open(":memory:").expect("Failed to open database");
        exec(&mut conn, "CREATE TABLE t5(a, b)");
        exec(&mut conn, "BEGIN");
        for i in 1..=total_rows {
            exec(
                &mut conn,
                &format!("INSERT INTO t5 VALUES({}, {})", i, i * i),
            );
        }
        exec(&mut conn, "COMMIT");

        exec(&mut conn, "DELETE FROM t5");
        exec(&mut conn, "INSERT INTO t5 VALUES(1, 2)");
        let rows = query(&mut conn, "SELECT count(*) FROM t5");
        let ok = rows[0][0] == "1";
        println!(
            "Test 5: {} rows, INSERT after DELETE all: {}",
            total_rows,
            if ok { "OK" } else { "FAIL" }
        );
        let _ = sqlite3_close(conn);
    }
}

/// Debug test: stepwise delete to trace cursor behavior
#[test]
fn test_bulk_delete_stepwise() {
    init();
    let mut conn = sqlite3_open(":memory:").expect("Failed to open database");

    exec(&mut conn, "CREATE TABLE table1(f1, f2)");

    let total_rows = 505;
    exec(&mut conn, "BEGIN");
    for i in 1..=total_rows {
        exec(
            &mut conn,
            &format!("INSERT INTO table1 VALUES({}, {})", i, i * i),
        );
    }
    exec(&mut conn, "COMMIT");

    // Check rows 1-10 before any deletes
    let rows = query(
        &mut conn,
        "SELECT rowid, f1 FROM table1 WHERE rowid <= 10 ORDER BY rowid",
    );
    println!("Rows 1-10 before DELETE:");
    for row in &rows {
        println!("  rowid={}, f1={}", row[0], row[1]);
    }

    // This is what works: batched deletes from the end
    let mut batch_start = 400;
    while batch_start > 7 {
        let batch_end = batch_start + 50;
        exec(
            &mut conn,
            &format!(
                "DELETE FROM table1 WHERE rowid >= {} AND rowid < {}",
                batch_start, batch_end
            ),
        );
        batch_start -= 50;
    }

    // Delete remaining rows 8-50
    exec(&mut conn, "DELETE FROM table1 WHERE rowid > 7");

    let remaining = query(
        &mut conn,
        "SELECT rowid FROM table1 WHERE rowid <= 7 ORDER BY rowid",
    );
    let remaining_ids: Vec<String> = remaining.iter().map(|r| r[0].clone()).collect();
    println!("After batched delete, rows 1-7: {:?}", remaining_ids);

    // Final check
    let all_remaining = query(&mut conn, "SELECT rowid, f1 FROM table1 ORDER BY rowid");
    println!("All remaining rows:");
    for row in &all_remaining {
        println!("  rowid={}, f1={}", row[0], row[1]);
    }

    let _ = sqlite3_close(conn);

    assert_eq!(remaining_ids.len(), 7, "Rows 1-7 should remain");
}

/// Debug test: find minimum number of rows that triggers the bug
#[test]
fn test_bulk_delete_threshold() {
    init();

    // Test around known problem areas
    // 335-345 (around where 341 might be significant)
    // 395-410 (around 400)
    // 500-515 (around 505)
    let test_ranges = [(335, 345), (395, 410), (500, 515)];

    for (start, end) in test_ranges {
        println!("\n--- Testing range {}-{} ---", start, end);
        for total_rows in start..=end {
            let mut conn = sqlite3_open(":memory:").expect("Failed to open database");
            exec(&mut conn, "CREATE TABLE table1(f1, f2)");
            exec(&mut conn, "BEGIN");
            for i in 1..=total_rows {
                exec(
                    &mut conn,
                    &format!("INSERT INTO table1 VALUES({}, {})", i, i * i),
                );
            }
            exec(&mut conn, "COMMIT");

            // Single bulk DELETE WHERE f1 > 7
            exec(&mut conn, "DELETE FROM table1 WHERE f1 > 7");

            let remaining = query(&mut conn, "SELECT f1 FROM table1 ORDER BY f1");
            let remaining_vals: Vec<String> = remaining.iter().map(|r| r[0].clone()).collect();

            let expected: Vec<String> = (1..=7).map(|i| i.to_string()).collect();
            let ok = remaining_vals == expected;

            if !ok {
                // Also show extra/missing values
                let extra: Vec<_> = remaining_vals
                    .iter()
                    .filter(|v| v.parse::<i32>().unwrap_or(0) > 7)
                    .collect();
                let missing: Vec<i32> = (1..=7)
                    .filter(|i| !remaining_vals.contains(&i.to_string()))
                    .collect();
                println!(
                    "total_rows={}: FAIL - extra={:?}, missing={:?}",
                    total_rows, extra, missing
                );
            }

            let _ = sqlite3_close(conn);
        }
    }
}
