//! Debug test for multi-statement SQL

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

fn execute_multi_stmt(conn: &mut SqliteConnection, sql: &str) -> Result<Vec<Vec<String>>, String> {
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

        println!("  Preparing: {:.50}...", remaining.replace('\n', " "));

        let (mut stmt, tail) = match sqlite3_prepare_v2(conn, remaining) {
            Ok(result) => result,
            Err(e) => {
                println!("  Prepare ERROR: {:?}", e);
                return Err(format!("{:?}", e));
            }
        };

        if stmt.sql().is_empty() {
            remaining = tail;
            continue;
        }

        // Step through statement
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
                    println!("    Row: {:?}", row);
                    all_results.push(row);
                }
                Ok(StepResult::Done) => {
                    println!("    Done");
                    break;
                }
                Err(e) => {
                    println!("    Step ERROR: {:?}", e);
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
fn test_select1_2_0_multi_stmt() {
    init();

    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // First, set up test1 and test2 tables (initial setup from the test file)
    println!("=== Initial setup ===");
    execute_multi_stmt(&mut conn, "CREATE TABLE test1(f1 int, f2 int)").unwrap();
    execute_multi_stmt(&mut conn, "INSERT INTO test1 VALUES(11,22)").unwrap();
    execute_multi_stmt(&mut conn, "CREATE TABLE test2(r1 real, r2 real)").unwrap();
    execute_multi_stmt(&mut conn, "INSERT INTO test2 VALUES(1.1,2.2)").unwrap();

    // Now run the actual multi-statement block from select1-2.0
    println!("\n=== Running select1-2.0 multi-statement block ===");
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

    match execute_multi_stmt(&mut conn, sql) {
        Ok(results) => {
            println!("\n=== Final results ===");
            for row in &results {
                println!("  {:?}", row);
            }
            // Expected: 4 rows - abc/NULL, NULL/xyz, 11/22, 33/44
            assert_eq!(results.len(), 4, "Should have 4 rows from t3");
        }
        Err(e) => {
            println!("ERROR: {}", e);
            panic!("Multi-statement block failed: {}", e);
        }
    }

    let _ = sqlite3_close(conn);
}

#[test]
fn test_drop_table() {
    init();

    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    println!("=== Test DROP TABLE ===");

    // Create a table
    execute_multi_stmt(&mut conn, "CREATE TABLE foo(x)").unwrap();

    // Insert data
    execute_multi_stmt(&mut conn, "INSERT INTO foo VALUES(1)").unwrap();

    // Try to drop it
    println!("\n=== DROP TABLE foo ===");
    match execute_multi_stmt(&mut conn, "DROP TABLE foo") {
        Ok(_) => println!("DROP succeeded"),
        Err(e) => {
            println!("DROP failed: {}", e);
            panic!("DROP TABLE should work");
        }
    }

    // Verify it's gone
    println!("\n=== Try SELECT from dropped table ===");
    match execute_multi_stmt(&mut conn, "SELECT * FROM foo") {
        Ok(_) => panic!("Should fail - table was dropped"),
        Err(e) => println!("Expected error: {}", e),
    }

    let _ = sqlite3_close(conn);
}

#[test]
fn test_delete_from() {
    init();

    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    println!("=== Test DELETE FROM ===");

    // Create a table with data
    execute_multi_stmt(&mut conn, "CREATE TABLE bar(x)").unwrap();
    execute_multi_stmt(&mut conn, "INSERT INTO bar VALUES(1)").unwrap();
    execute_multi_stmt(&mut conn, "INSERT INTO bar VALUES(2)").unwrap();

    // Verify data
    let rows = execute_multi_stmt(&mut conn, "SELECT * FROM bar").unwrap();
    println!("Before DELETE: {} rows", rows.len());
    assert_eq!(rows.len(), 2);

    // Delete all rows
    println!("\n=== DELETE FROM bar ===");
    execute_multi_stmt(&mut conn, "DELETE FROM bar").unwrap();

    // Verify empty
    let rows = execute_multi_stmt(&mut conn, "SELECT * FROM bar").unwrap();
    println!("After DELETE: {} rows", rows.len());
    assert_eq!(rows.len(), 0, "Table should be empty after DELETE");

    let _ = sqlite3_close(conn);
}
