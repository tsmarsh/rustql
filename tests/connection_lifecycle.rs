use std::sync::Once;

use rustql::types::StepResult;
use rustql::{
    sqlite3_close, sqlite3_column_text, sqlite3_finalize, sqlite3_initialize, sqlite3_open,
    sqlite3_prepare_v2, sqlite3_step,
};
use tempfile::tempdir;

static INIT: Once = Once::new();

fn open_db(path: &str) -> rustql::Result<Box<rustql::SqliteConnection>> {
    INIT.call_once(|| {
        sqlite3_initialize().unwrap();
    });
    sqlite3_open(path)
}

fn exec(conn: &mut rustql::SqliteConnection, sql: &str) {
    let (mut stmt, _) = sqlite3_prepare_v2(conn, sql).unwrap();
    loop {
        match sqlite3_step(&mut stmt) {
            Ok(StepResult::Done) => break,
            Ok(StepResult::Row) => continue,
            Err(err) => panic!("exec failed for '{}': {}", sql, err),
        }
    }
    let _ = sqlite3_finalize(stmt);
}

fn query_flat(conn: &mut rustql::SqliteConnection, sql: &str) -> Vec<String> {
    let (mut stmt, _) = sqlite3_prepare_v2(conn, sql).unwrap();
    let mut results = Vec::new();
    loop {
        match sqlite3_step(&mut stmt) {
            Ok(StepResult::Row) => {
                results.push(sqlite3_column_text(&stmt, 0));
            }
            Ok(StepResult::Done) => break,
            Err(err) => panic!("query failed for '{}': {}", sql, err),
        }
    }
    let _ = sqlite3_finalize(stmt);
    results
}

#[test]
fn test_close_releases_lock_for_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let path_str = path.to_str().unwrap();

    {
        let mut db = open_db(path_str).unwrap();
        exec(&mut db, "CREATE TABLE t1(a INT)");
        exec(&mut db, "INSERT INTO t1 VALUES(1)");
        sqlite3_close(db).unwrap();
    }

    {
        let mut db = open_db(path_str).unwrap();
        let rows = query_flat(&mut db, "SELECT a FROM t1");
        assert_eq!(rows, vec!["1"]);
        sqlite3_close(db).unwrap();
    }
}

#[test]
fn test_close_rolls_back_open_transaction() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let path_str = path.to_str().unwrap();

    {
        let mut db = open_db(path_str).unwrap();
        exec(&mut db, "CREATE TABLE t1(a INT)");
        exec(&mut db, "BEGIN");
        exec(&mut db, "INSERT INTO t1 VALUES(1)");
        sqlite3_close(db).unwrap();
    }

    {
        let mut db = open_db(path_str).unwrap();
        let rows = query_flat(&mut db, "SELECT count(*) FROM t1");
        assert_eq!(rows, vec!["0"]);
        sqlite3_close(db).unwrap();
    }
}

#[test]
fn test_multiple_close_reopen_cycles() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.db");
    let path_str = path.to_str().unwrap();

    for i in 0..5 {
        let mut db = open_db(path_str).unwrap();
        if i == 0 {
            exec(&mut db, "CREATE TABLE t1(a INT)");
        }
        exec(&mut db, &format!("INSERT INTO t1 VALUES({})", i));
        sqlite3_close(db).unwrap();
    }

    let mut db = open_db(path_str).unwrap();
    let rows = query_flat(&mut db, "SELECT count(*) FROM t1");
    assert_eq!(rows, vec!["5"]);
    sqlite3_close(db).unwrap();
}
