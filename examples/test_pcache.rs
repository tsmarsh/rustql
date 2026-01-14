//! Test page cache integration

use rustql::api::{
    sqlite3_close, sqlite3_column_count, sqlite3_column_int, sqlite3_column_text,
    sqlite3_column_type, sqlite3_finalize, sqlite3_open, sqlite3_prepare_v2, sqlite3_step,
};
use rustql::types::StepResult;

fn main() {
    println!("Testing page cache integration...\n");

    // Open database
    let mut db = match sqlite3_open(":memory:") {
        Ok(conn) => conn,
        Err(e) => {
            println!("Failed to open database: {:?}", e);
            return;
        }
    };
    println!("Database opened successfully");

    // Create table
    let sql = "CREATE TABLE t1(a, b)";
    println!("\nExecuting: {}", sql);
    match sqlite3_prepare_v2(&mut db, sql) {
        Ok((mut stmt, _tail)) => {
            match sqlite3_step(&mut stmt) {
                Ok(result) => println!("CREATE TABLE result: {:?}", result),
                Err(e) => println!("CREATE TABLE step failed: {:?}", e),
            }
            let _ = sqlite3_finalize(stmt);
        }
        Err(e) => println!("CREATE TABLE prepare failed: {:?}", e),
    }

    // Insert data
    let sql = "INSERT INTO t1 VALUES(1, 2)";
    println!("\nExecuting: {}", sql);
    match sqlite3_prepare_v2(&mut db, sql) {
        Ok((mut stmt, _tail)) => {
            match sqlite3_step(&mut stmt) {
                Ok(result) => println!("INSERT result: {:?}", result),
                Err(e) => println!("INSERT step failed: {:?}", e),
            }
            let _ = sqlite3_finalize(stmt);
        }
        Err(e) => println!("INSERT prepare failed: {:?}", e),
    }

    // Select data - use explicit column names
    let sql = "SELECT a, b FROM t1";
    println!("\nExecuting: {}", sql);
    match sqlite3_prepare_v2(&mut db, sql) {
        Ok((mut stmt, _tail)) => {
            println!("PREPARE succeeded");
            println!("Column count: {}", sqlite3_column_count(&stmt));

            // Step through results
            println!("Stepping through results...");
            let mut row_count = 0;
            loop {
                match sqlite3_step(&mut stmt) {
                    Ok(StepResult::Row) => {
                        row_count += 1;
                        let type0 = sqlite3_column_type(&stmt, 0);
                        let type1 = sqlite3_column_type(&stmt, 1);
                        let a_int = sqlite3_column_int(&stmt, 0);
                        let b_int = sqlite3_column_int(&stmt, 1);
                        let a = sqlite3_column_text(&stmt, 0);
                        let b = sqlite3_column_text(&stmt, 1);
                        println!("  Row {}: type0={:?}, type1={:?}", row_count, type0, type1);
                        println!("  Row {}: a_int={}, b_int={}", row_count, a_int, b_int);
                        println!("  Row {}: a='{}', b='{}'", row_count, a, b);
                    }
                    Ok(StepResult::Done) => {
                        println!("Query complete, {} rows returned", row_count);
                        break;
                    }
                    Err(e) => {
                        println!("STEP error: {:?}", e);
                        break;
                    }
                }
            }
            let _ = sqlite3_finalize(stmt);
        }
        Err(e) => println!("SELECT prepare failed: {:?}", e),
    }

    // Also test SELECT *
    let sql = "SELECT * FROM t1";
    println!("\nExecuting: {}", sql);
    match sqlite3_prepare_v2(&mut db, sql) {
        Ok((mut stmt, _tail)) => {
            println!("PREPARE succeeded");
            println!("Column count: {}", sqlite3_column_count(&stmt));

            let mut row_count = 0;
            loop {
                match sqlite3_step(&mut stmt) {
                    Ok(StepResult::Row) => {
                        row_count += 1;
                        let a = sqlite3_column_text(&stmt, 0);
                        let b = sqlite3_column_text(&stmt, 1);
                        println!("  Row {}: a='{}', b='{}'", row_count, a, b);
                    }
                    Ok(StepResult::Done) => {
                        println!("Query complete, {} rows returned", row_count);
                        break;
                    }
                    Err(e) => {
                        println!("STEP error: {:?}", e);
                        break;
                    }
                }
            }
            let _ = sqlite3_finalize(stmt);
        }
        Err(e) => println!("SELECT * prepare failed: {:?}", e),
    }

    // Close database
    let _ = sqlite3_close(db);
    println!("\nDatabase closed");
}
