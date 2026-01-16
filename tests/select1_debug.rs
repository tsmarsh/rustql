//! Debug test for INSERT...SELECT issue

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
fn test_insert_select() {
    init();

    let mut conn = sqlite3_open(":memory:").expect("open memory db");

    // Create source table with data
    println!("=== Creating source table ===");
    run_sql(&mut conn, "CREATE TABLE source(a, b)").unwrap();
    run_sql(&mut conn, "INSERT INTO source VALUES(1, 2)").unwrap();
    run_sql(&mut conn, "INSERT INTO source VALUES(3, 4)").unwrap();

    println!("\n=== Verifying source table ===");
    let source_rows = run_sql(&mut conn, "SELECT * FROM source").unwrap();
    assert_eq!(source_rows.len(), 2, "Source should have 2 rows");

    // Create destination table
    println!("\n=== Creating dest table ===");
    run_sql(&mut conn, "CREATE TABLE dest(x, y)").unwrap();

    // First, test a simple SELECT to make sure it works
    println!("\n=== Testing simple SELECT from source ===");
    let simple = run_sql(&mut conn, "SELECT * FROM source").unwrap();
    println!("Simple SELECT returned {} rows", simple.len());

    // Insert with SELECT
    println!("\n=== Running INSERT INTO dest SELECT * FROM source ===");
    match run_sql(&mut conn, "INSERT INTO dest SELECT * FROM source") {
        Ok(_) => println!("INSERT...SELECT succeeded"),
        Err(e) => println!("INSERT...SELECT error: {}", e),
    }

    // Check dest table
    println!("\n=== Checking dest table ===");
    let dest_rows = run_sql(&mut conn, "SELECT * FROM dest").unwrap();
    println!(
        "Dest table has {} rows after INSERT...SELECT",
        dest_rows.len()
    );

    let _ = sqlite3_close(conn);

    // Verify
    assert_eq!(
        dest_rows.len(),
        2,
        "Dest should have 2 rows after INSERT...SELECT"
    );
}
