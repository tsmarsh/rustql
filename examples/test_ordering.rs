//! Test result ordering

use rustql::api::{
    sqlite3_close, sqlite3_column_count, sqlite3_column_text, sqlite3_finalize, sqlite3_open,
    sqlite3_prepare_v2, sqlite3_step,
};
use rustql::types::StepResult;

fn exec_sql(db: &mut rustql::api::SqliteConnection, sql: &str) -> Vec<String> {
    println!("SQL: {}", sql);
    match sqlite3_prepare_v2(db, sql) {
        Ok((mut stmt, _)) => {
            let mut results = Vec::new();
            loop {
                match sqlite3_step(&mut stmt) {
                    Ok(StepResult::Row) => {
                        let col_count = sqlite3_column_count(&stmt);
                        for i in 0..col_count {
                            results.push(sqlite3_column_text(&stmt, i));
                        }
                    }
                    Ok(StepResult::Done) => break,
                    Err(e) => {
                        println!("  Error: {:?}", e);
                        break;
                    }
                }
            }
            let _ = sqlite3_finalize(stmt);
            println!("  Results: {:?}", results);
            results
        }
        Err(e) => {
            println!("  Prepare error: {:?}", e);
            Vec::new()
        }
    }
}

fn main() {
    let mut db = sqlite3_open(":memory:").unwrap();

    // Create multiple tables
    exec_sql(&mut db, "CREATE TABLE test2(a)");
    exec_sql(&mut db, "CREATE TABLE test3(b)");
    exec_sql(&mut db, "CREATE TABLE test1(c)");

    // Query sqlite_master - check ordering
    println!("\n--- Testing sqlite_master ordering ---");
    exec_sql(&mut db, "SELECT name FROM sqlite_master WHERE type='table'");
    exec_sql(
        &mut db,
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
    );

    let _ = sqlite3_close(db);
}
