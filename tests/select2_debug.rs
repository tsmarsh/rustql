//! Debug test for aggregate and INSERT...SELECT issues

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
                println!("  Step error: {:?}", e);
                let _ = sqlite3_finalize(stmt);
                return Err(format!("{:?}", e));
            }
        }
    }
    let _ = sqlite3_finalize(stmt);
    Ok(results)
}

#[test]
fn test_count_aggregate() {
    init();

    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // Setup test1 table like SQLite tests
    println!("=== Creating test1 table ===");
    run_sql(&mut conn, "CREATE TABLE test1(f1, f2)").unwrap();
    run_sql(&mut conn, "INSERT INTO test1 VALUES(11, 22)").unwrap();
    run_sql(&mut conn, "INSERT INTO test1 VALUES(33, 44)").unwrap();

    println!("\n=== Verify test1 has 2 rows ===");
    let rows = run_sql(&mut conn, "SELECT * FROM test1").unwrap();
    assert_eq!(rows.len(), 2, "test1 should have 2 rows");

    println!("\n=== Test COUNT(f1) ===");
    let count_rows = run_sql(&mut conn, "SELECT COUNT(f1) FROM test1").unwrap();
    println!("COUNT(f1) returned: {:?}", count_rows);

    println!("\n=== Test COUNT(*) ===");
    let count_star = run_sql(&mut conn, "SELECT COUNT(*) FROM test1").unwrap();
    println!("COUNT(*) returned: {:?}", count_star);

    let _ = sqlite3_close(conn);

    // Verify COUNT returns 2
    assert_eq!(count_rows.len(), 1, "Should have 1 result row");
    assert_eq!(count_rows[0][0], "2", "COUNT(f1) should be 2");
}

#[test]
fn test_insert_select_chain() {
    init();

    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // Replicate select1-2.0 test setup
    println!("=== Creating test1 table ===");
    run_sql(&mut conn, "CREATE TABLE test1(f1, f2)").unwrap();
    run_sql(&mut conn, "INSERT INTO test1 VALUES(11, 22)").unwrap();
    run_sql(&mut conn, "INSERT INTO test1 VALUES(33, 44)").unwrap();

    println!("\n=== Creating t3 and inserting data ===");
    run_sql(&mut conn, "CREATE TABLE t3(a, b)").unwrap();
    run_sql(&mut conn, "INSERT INTO t3 VALUES('abc', NULL)").unwrap();
    run_sql(&mut conn, "INSERT INTO t3 VALUES(NULL, 'xyz')").unwrap();

    println!("\n=== INSERT INTO t3 SELECT * FROM test1 ===");
    run_sql(&mut conn, "INSERT INTO t3 SELECT * FROM test1").unwrap();

    println!("\n=== SELECT * FROM t3 ===");
    let t3_rows = run_sql(&mut conn, "SELECT * FROM t3").unwrap();
    println!("t3 has {} rows", t3_rows.len());

    let _ = sqlite3_close(conn);

    // Expected: abc,NULL  NULL,xyz  11,22  33,44
    assert_eq!(t3_rows.len(), 4, "t3 should have 4 rows");
}
