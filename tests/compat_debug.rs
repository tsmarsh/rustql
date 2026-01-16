//! Debug test to trace compat test failures

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

fn exec_sql(conn: &mut SqliteConnection, sql: &str) -> Result<Vec<String>, String> {
    let mut all_results = Vec::new();
    let mut remaining = sql.trim();

    while !remaining.is_empty() {
        remaining = remaining.trim_start();

        // Skip comments
        if remaining.starts_with("--") {
            if let Some(pos) = remaining.find('\n') {
                remaining = &remaining[pos + 1..];
                continue;
            } else {
                break;
            }
        }

        let (mut stmt, tail) = match sqlite3_prepare_v2(conn, remaining) {
            Ok(result) => result,
            Err(e) => {
                println!("  PREPARE ERROR: {:?}", e);
                println!("  SQL: {:.60}...", remaining.replace('\n', " "));
                return Err(format!("{:?}", e));
            }
        };

        if stmt.sql().is_empty() {
            remaining = tail;
            continue;
        }

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
                        all_results.push(text);
                    }
                }
                Ok(StepResult::Done) => break,
                Err(e) => {
                    println!("  STEP ERROR: {:?}", e);
                    let _ = sqlite3_finalize(stmt);
                    return Err(format!("{:?}", e));
                }
            }
        }

        let _ = sqlite3_finalize(stmt);
        remaining = tail;
    }

    Ok(all_results)
}

#[test]
fn test_select1_2_0_exact() {
    init();

    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // Run the 4 setup commands that come before select1-2.0 (line 102)
    println!("=== Setup command 1 (line 27): CREATE TABLE test1 ===");
    match exec_sql(&mut conn, "CREATE TABLE test1(f1 int, f2 int)") {
        Ok(_) => println!("  OK"),
        Err(e) => println!("  FAILED: {}", e),
    }

    println!("\n=== Setup command 2 (line 38): INSERT INTO test1 ===");
    match exec_sql(&mut conn, "INSERT INTO test1(f1,f2) VALUES(11,22)") {
        Ok(_) => println!("  OK"),
        Err(e) => println!("  FAILED: {}", e),
    }

    println!("\n=== Setup command 3 (line 68): CREATE TABLE test2 ===");
    match exec_sql(&mut conn, "CREATE TABLE test2(r1 real, r2 real)") {
        Ok(_) => println!("  OK"),
        Err(e) => println!("  FAILED: {}", e),
    }

    println!("\n=== Setup command 4 (line 69): INSERT INTO test2 ===");
    match exec_sql(&mut conn, "INSERT INTO test2(r1,r2) VALUES(1.1,2.2)") {
        Ok(_) => println!("  OK"),
        Err(e) => println!("  FAILED: {}", e),
    }

    // Verify setup
    println!("\n=== Verify test1 ===");
    match exec_sql(&mut conn, "SELECT * FROM test1") {
        Ok(r) => println!("  test1: {:?}", r),
        Err(e) => println!("  FAILED: {}", e),
    }

    println!("\n=== Verify test2 ===");
    match exec_sql(&mut conn, "SELECT * FROM test2") {
        Ok(r) => println!("  test2: {:?}", r),
        Err(e) => println!("  FAILED: {}", e),
    }

    // Now run select1-2.0 test SQL (multi-statement)
    println!("\n=== Running select1-2.0 SQL ===");
    let sql = r#"
    DROP TABLE test2;
    DELETE FROM test1;
    INSERT INTO test1 VALUES(11,22);
    INSERT INTO test1 VALUES(33,44);
    CREATE TABLE t3(a,b);
    INSERT INTO t3 VALUES('abc',NULL);
    INSERT INTO t3 VALUES(NULL,'xyz');
    INSERT INTO t3 SELECT * FROM test1;
    SELECT * FROM t3;
    "#;

    match exec_sql(&mut conn, sql) {
        Ok(results) => {
            let result_str = results.join(" ");
            println!("  Result: {}", result_str);
            println!("  Expected: abc {{}} {{}} xyz 11 22 33 44");
            assert_eq!(result_str, "abc {} {} xyz 11 22 33 44");
        }
        Err(e) => {
            println!("  FAILED: {}", e);
            panic!("select1-2.0 failed");
        }
    }

    let _ = sqlite3_close(conn);
}

#[test]
fn test_select1_2_2_count() {
    init();

    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // Setup: create test1 with 2 rows (as select1-2.0 leaves it)
    println!("=== Setup ===");
    exec_sql(&mut conn, "CREATE TABLE test1(f1 int, f2 int)").unwrap();
    exec_sql(&mut conn, "INSERT INTO test1 VALUES(11,22)").unwrap();
    exec_sql(&mut conn, "INSERT INTO test1 VALUES(33,44)").unwrap();

    // Verify
    let rows = exec_sql(&mut conn, "SELECT * FROM test1").unwrap();
    println!("test1 rows: {:?}", rows);
    assert_eq!(rows.len(), 4); // 2 rows x 2 columns

    // Test: SELECT count(f1) FROM test1
    // Expected: "0 2" (0 = no error, 2 = count result)
    println!("\n=== Test COUNT(f1) ===");
    let result = exec_sql(&mut conn, "SELECT count(f1) FROM test1").unwrap();
    println!("COUNT(f1) result: {:?}", result);

    // The compat test expects "0 2" where 0 is the catch result code
    // Our exec_sql just returns the values, so we expect ["2"]
    assert_eq!(result, vec!["2"], "COUNT(f1) should return 2 for 2 rows");

    let _ = sqlite3_close(conn);
}

#[test]
fn test_aggregate_sum_max_min() {
    init();

    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    exec_sql(&mut conn, "CREATE TABLE test1(f1 int, f2 int)").unwrap();
    exec_sql(&mut conn, "INSERT INTO test1 VALUES(11,22)").unwrap();
    exec_sql(&mut conn, "INSERT INTO test1 VALUES(33,44)").unwrap();

    println!("=== Test SUM(f1) ===");
    let result = exec_sql(&mut conn, "SELECT sum(f1) FROM test1").unwrap();
    println!("SUM(f1): {:?}", result);
    // 11 + 33 = 44
    assert_eq!(result, vec!["44"], "SUM should be 44");

    println!("\n=== Test MAX(f1) ===");
    let result = exec_sql(&mut conn, "SELECT max(f1) FROM test1").unwrap();
    println!("MAX(f1): {:?}", result);
    assert_eq!(result, vec!["33"], "MAX should be 33");

    println!("\n=== Test MIN(f1) ===");
    let result = exec_sql(&mut conn, "SELECT min(f1) FROM test1").unwrap();
    println!("MIN(f1): {:?}", result);
    assert_eq!(result, vec!["11"], "MIN should be 11");

    let _ = sqlite3_close(conn);
}
