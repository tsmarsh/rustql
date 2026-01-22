use std::sync::Once;

use rustql::types::StepResult;
use rustql::{
    sqlite3_close, sqlite3_column_text, sqlite3_enable_shared_cache, sqlite3_finalize,
    sqlite3_initialize, sqlite3_open, sqlite3_prepare_v2, sqlite3_step, SqliteConnection,
};
use tempfile::tempdir;

static INIT: Once = Once::new();

fn init() {
    INIT.call_once(|| {
        let _ = sqlite3_initialize();
        let _ = sqlite3_enable_shared_cache(1);
    });
}

fn exec(conn: &mut SqliteConnection, sql: &str) -> rustql::Result<()> {
    let (mut stmt, _) = sqlite3_prepare_v2(conn, sql)?;
    loop {
        match sqlite3_step(&mut stmt) {
            Ok(StepResult::Done) => break,
            Ok(StepResult::Row) => continue,
            Err(err) => {
                let _ = sqlite3_finalize(stmt);
                return Err(err);
            }
        }
    }
    let _ = sqlite3_finalize(stmt);
    Ok(())
}

fn query_flat(conn: &mut SqliteConnection, sql: &str) -> Vec<String> {
    let (mut stmt, _) = sqlite3_prepare_v2(conn, sql).unwrap();
    let mut results = Vec::new();
    loop {
        match sqlite3_step(&mut stmt) {
            Ok(StepResult::Row) => results.push(sqlite3_column_text(&stmt, 0)),
            Ok(StepResult::Done) => break,
            Err(err) => panic!("query failed for '{}': {}", sql, err),
        }
    }
    let _ = sqlite3_finalize(stmt);
    results
}

#[test]
fn test_shared_cache_schema_visibility() {
    init();
    let dir = tempdir().unwrap();
    let path = dir.path().join("shared.db");
    let path_str = path.to_str().unwrap();

    let mut db1 = sqlite3_open(path_str).unwrap();
    let mut db2 = sqlite3_open(path_str).unwrap();

    exec(&mut db1, "CREATE TABLE t(x INT)").unwrap();

    let tables = query_flat(
        &mut db2,
        "SELECT name FROM sqlite_master WHERE type='table' AND name='t'",
    );
    assert_eq!(tables, vec!["t"]);

    sqlite3_close(db1).unwrap();
    sqlite3_close(db2).unwrap();
}

#[test]
fn test_shared_cache_table_write_lock() {
    init();
    let dir = tempdir().unwrap();
    let path = dir.path().join("shared_lock.db");
    let path_str = path.to_str().unwrap();

    let mut db1 = sqlite3_open(path_str).unwrap();
    let mut db2 = sqlite3_open(path_str).unwrap();

    exec(&mut db1, "CREATE TABLE t(x INT)").unwrap();
    exec(&mut db1, "BEGIN").unwrap();
    exec(&mut db1, "INSERT INTO t VALUES(1)").unwrap();

    let err = exec(&mut db2, "INSERT INTO t VALUES(2)").unwrap_err();
    assert_eq!(err.code, rustql::error::ErrorCode::Locked);

    exec(&mut db1, "ROLLBACK").unwrap();
    sqlite3_close(db1).unwrap();
    sqlite3_close(db2).unwrap();
}
