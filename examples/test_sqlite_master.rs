//! Test sqlite_master queries

use rustql::api::{
    sqlite3_close, sqlite3_column_count, sqlite3_column_text, sqlite3_finalize, sqlite3_open,
    sqlite3_prepare_v2, sqlite3_step,
};
use rustql::types::StepResult;

fn main() {
    println!("Testing sqlite_master...\n");

    let mut db = match sqlite3_open(":memory:") {
        Ok(conn) => conn,
        Err(e) => {
            println!("Failed to open database: {:?}", e);
            return;
        }
    };

    // Create a table
    let sql = "CREATE TABLE test1(a, b)";
    println!("Executing: {}", sql);
    if let Ok((mut stmt, _)) = sqlite3_prepare_v2(&mut db, sql) {
        let _ = sqlite3_step(&mut stmt);
        let _ = sqlite3_finalize(stmt);
    }
    println!("CREATE TABLE done\n");

    // Simple query - SELECT * FROM sqlite_master
    let sql = "SELECT * FROM sqlite_master";
    println!("Executing: {}", sql);
    match sqlite3_prepare_v2(&mut db, sql) {
        Ok((mut stmt, _)) => {
            println!("Column count: {}", sqlite3_column_count(&stmt));
            let mut count = 0;
            loop {
                println!("  Calling step...");
                match sqlite3_step(&mut stmt) {
                    Ok(StepResult::Row) => {
                        count += 1;
                        let type_ = sqlite3_column_text(&stmt, 0);
                        let name = sqlite3_column_text(&stmt, 1);
                        let sql_col = sqlite3_column_text(&stmt, 4);
                        println!(
                            "  Row {}: type='{}', name='{}', sql='{}'",
                            count, type_, name, sql_col
                        );
                        if count > 10 {
                            println!("  Safety limit reached");
                            break;
                        }
                    }
                    Ok(StepResult::Done) => {
                        println!("  Done, {} rows", count);
                        break;
                    }
                    Ok(other) => {
                        println!("  Other result: {:?}", other);
                        break;
                    }
                    Err(e) => {
                        println!("  Error: {:?}", e);
                        break;
                    }
                }
            }
            let _ = sqlite3_finalize(stmt);
        }
        Err(e) => println!("Prepare failed: {:?}", e),
    }

    // Query with WHERE clause
    let sql = "SELECT name FROM sqlite_master WHERE type='table'";
    println!("\nExecuting: {}", sql);

    // Debug: print bytecode
    eprintln!("Bytecode for query:");
    match sqlite3_prepare_v2(&mut db, sql) {
        Ok((mut stmt, _)) => {
            println!("Column count: {}", sqlite3_column_count(&stmt));
            let mut count = 0;
            loop {
                println!("  Calling step...");
                match sqlite3_step(&mut stmt) {
                    Ok(StepResult::Row) => {
                        count += 1;
                        let name = sqlite3_column_text(&stmt, 0);
                        println!("  Row {}: name='{}'", count, name);
                        if count > 10 {
                            println!("  Safety limit reached");
                            break;
                        }
                    }
                    Ok(StepResult::Done) => {
                        println!("  Done, {} rows", count);
                        break;
                    }
                    Ok(other) => {
                        println!("  Other result: {:?}", other);
                        break;
                    }
                    Err(e) => {
                        println!("  Error: {:?}", e);
                        break;
                    }
                }
            }
            let _ = sqlite3_finalize(stmt);
        }
        Err(e) => println!("Prepare failed: {:?}", e),
    }

    let _ = sqlite3_close(db);
    println!("\nDone");
}
