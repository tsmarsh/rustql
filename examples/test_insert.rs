//! Test INSERT operations

use rustql::api::{
    sqlite3_close, sqlite3_column_count, sqlite3_column_text, sqlite3_finalize, sqlite3_open,
    sqlite3_prepare_v2, sqlite3_step,
};
use rustql::types::StepResult;

fn main() {
    println!("Testing INSERT operations...\n");

    let mut db = match sqlite3_open(":memory:") {
        Ok(conn) => conn,
        Err(e) => {
            println!("Failed to open database: {:?}", e);
            return;
        }
    };

    // Create a table
    println!("1. CREATE TABLE t1(a, b)");
    if let Ok((mut stmt, _)) = sqlite3_prepare_v2(&mut db, "CREATE TABLE t1(a, b)") {
        match sqlite3_step(&mut stmt) {
            Ok(result) => println!("   Step result: {:?}", result),
            Err(e) => println!("   Step error: {:?}", e),
        }
        let _ = sqlite3_finalize(stmt);
    }

    // Insert data
    println!("\n2. INSERT INTO t1 VALUES(1, 2)");
    match sqlite3_prepare_v2(&mut db, "INSERT INTO t1 VALUES(1, 2)") {
        Ok((mut stmt, _)) => {
            match sqlite3_step(&mut stmt) {
                Ok(result) => println!("   Step result: {:?}", result),
                Err(e) => println!("   Step error: {:?}", e),
            }
            let _ = sqlite3_finalize(stmt);
        }
        Err(e) => println!("   Prepare error: {:?}", e),
    }

    // Insert more data
    println!("\n3. INSERT INTO t1 VALUES(3, 4)");
    match sqlite3_prepare_v2(&mut db, "INSERT INTO t1 VALUES(3, 4)") {
        Ok((mut stmt, _)) => {
            match sqlite3_step(&mut stmt) {
                Ok(result) => println!("   Step result: {:?}", result),
                Err(e) => println!("   Step error: {:?}", e),
            }
            let _ = sqlite3_finalize(stmt);
        }
        Err(e) => println!("   Prepare error: {:?}", e),
    }

    // Select to verify
    println!("\n4. SELECT * FROM t1");
    match sqlite3_prepare_v2(&mut db, "SELECT * FROM t1") {
        Ok((mut stmt, _)) => {
            println!("   Column count: {}", sqlite3_column_count(&stmt));
            let mut row_count = 0;
            loop {
                match sqlite3_step(&mut stmt) {
                    Ok(StepResult::Row) => {
                        row_count += 1;
                        let a = sqlite3_column_text(&stmt, 0);
                        let b = sqlite3_column_text(&stmt, 1);
                        println!("   Row {}: a='{}', b='{}'", row_count, a, b);
                    }
                    Ok(StepResult::Done) => {
                        println!("   Done, {} rows total", row_count);
                        break;
                    }
                    Err(e) => {
                        println!("   Step error: {:?}", e);
                        break;
                    }
                }
            }
            let _ = sqlite3_finalize(stmt);
        }
        Err(e) => println!("   Prepare error: {:?}", e),
    }

    let _ = sqlite3_close(db);
    println!("\nDone");
}
