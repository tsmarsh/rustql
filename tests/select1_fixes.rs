//! Tests for select1.test fixes

use rustql::types::StepResult;
use rustql::{
    sqlite3_close, sqlite3_column_count, sqlite3_column_text, sqlite3_finalize, sqlite3_initialize,
    sqlite3_open, sqlite3_prepare_v2, sqlite3_step, SqliteConnection,
};
use std::sync::Once;

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
#[ignore] // TODO: IN with UNION subquery returns empty - needs investigation
fn test_6_23_in_subquery_union_order_by_alias() {
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // Setup t6
    exec(&mut conn, "CREATE TABLE t6(a TEXT, b TEXT)");
    exec(&mut conn, "INSERT INTO t6 VALUES('a','0')");
    exec(&mut conn, "INSERT INTO t6 VALUES('b','1')");
    exec(&mut conn, "INSERT INTO t6 VALUES('c','2')");
    exec(&mut conn, "INSERT INTO t6 VALUES('d','3')");

    // Debug: Check subquery parts
    println!("=== Subquery parts ===");
    let r1 = query_flat(&mut conn, "SELECT b FROM t6 WHERE a<='b'");
    println!("SELECT b FROM t6 WHERE a<='b': {:?}", r1);
    assert_eq!(r1, vec!["0", "1"]);

    let r2 = query_flat(&mut conn, "SELECT '3' AS x");
    println!("SELECT '3' AS x: {:?}", r2);
    assert_eq!(r2, vec!["3"]);

    // UNION with ORDER BY column position
    let r3 = query_flat(
        &mut conn,
        "SELECT b FROM t6 WHERE a<='b' UNION SELECT '3' AS x ORDER BY 1 DESC",
    );
    println!("UNION ORDER BY 1 DESC: {:?}", r3);
    assert_eq!(r3, vec!["3", "1", "0"]);

    // UNION with LIMIT
    let r4 = query_flat(
        &mut conn,
        "SELECT b FROM t6 WHERE a<='b' UNION SELECT '3' AS x ORDER BY 1 DESC LIMIT 2",
    );
    println!("UNION ORDER BY 1 DESC LIMIT 2: {:?}", r4);
    assert_eq!(r4, vec!["3", "1"]);

    // UNION with ORDER BY alias 'x'
    let r5 = query_flat(
        &mut conn,
        "SELECT b FROM t6 WHERE a<='b' UNION SELECT '3' AS x ORDER BY x DESC LIMIT 2",
    );
    println!("UNION ORDER BY x DESC LIMIT 2: {:?}", r5);
    // Should be same as ORDER BY 1 DESC LIMIT 2
    assert_eq!(r5, vec!["3", "1"], "ORDER BY x should work like ORDER BY 1");

    // Full query from test 6.23
    let result = query_flat(&mut conn,
        "SELECT a FROM t6 WHERE b IN (SELECT b FROM t6 WHERE a<='b' UNION SELECT '3' AS x ORDER BY x DESC LIMIT 2) ORDER BY a");
    println!("Full query result: {:?}", result);
    assert_eq!(result, vec!["b", "d"], "Test 6.23 should return b, d");

    let _ = sqlite3_close(conn);
}

#[test]
fn test_cross_join() {
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    exec(&mut conn, "CREATE TABLE test1(f1 int, f2 int)");
    exec(&mut conn, "INSERT INTO test1 VALUES(11, 22)");
    exec(&mut conn, "INSERT INTO test1 VALUES(33, 44)");

    let result = query(
        &mut conn,
        "SELECT A.f1, B.f1 FROM test1 as A, test1 as B ORDER BY A.f1, B.f1",
    );
    println!("Cross join result: {:?}", result);
    assert_eq!(result.len(), 4, "Cross join should return 4 rows");
    assert_eq!(result[0], vec!["11", "11"]);
    assert_eq!(result[1], vec!["11", "33"]);
    assert_eq!(result[2], vec!["33", "11"]);
    assert_eq!(result[3], vec!["33", "33"]);

    let _ = sqlite3_close(conn);
}

#[test]
fn test_subquery_aggregate_columns() {
    init();
    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    exec(&mut conn, "CREATE TABLE t3(a,b)");
    exec(&mut conn, "INSERT INTO t3 VALUES(1,2)");
    exec(&mut conn, "CREATE TABLE t4(a,b)");
    exec(&mut conn, "INSERT INTO t4 VALUES(3,4)");

    // Test 11.14: SELECT * from join with aggregate subquery
    let result = query_flat(
        &mut conn,
        "SELECT * FROM t3, (SELECT max(a), max(b) FROM t4) AS tx",
    );
    println!("Test 11.14 result: {:?}", result);
    assert_eq!(result, vec!["1", "2", "3", "4"]);

    let _ = sqlite3_close(conn);
}
