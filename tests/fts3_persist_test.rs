//! FTS3 persistence tests.

#![cfg(feature = "fts3")]

use rustql::types::StepResult;
use rustql::{
    sqlite3_close, sqlite3_column_count, sqlite3_column_text, sqlite3_finalize, sqlite3_initialize,
    sqlite3_open, sqlite3_prepare_v2, sqlite3_step, PreparedStmt, SqliteConnection,
};
use std::fs;
use std::sync::Once;

fn exec_sql(conn: &mut SqliteConnection, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let mut all_rows = Vec::new();
    let mut remaining = sql.trim();

    while !remaining.is_empty() {
        remaining = remaining.trim_start();
        if remaining.starts_with("--") {
            if let Some(pos) = remaining.find('\n') {
                remaining = &remaining[pos + 1..];
                continue;
            }
            break;
        }

        let (mut stmt, tail) =
            sqlite3_prepare_v2(conn, remaining).map_err(|e| e.sqlite_errmsg())?;

        if stmt.sql().is_empty() {
            remaining = tail;
            continue;
        }

        let rows = step_and_collect(&mut stmt)?;
        all_rows.extend(rows);
        let _ = sqlite3_finalize(stmt);
        remaining = tail;
    }

    Ok(all_rows)
}

fn step_and_collect(stmt: &mut PreparedStmt) -> Result<Vec<Vec<String>>, String> {
    let mut rows = Vec::new();
    let col_count = sqlite3_column_count(stmt);

    loop {
        match sqlite3_step(stmt) {
            Ok(StepResult::Row) => {
                let mut row = Vec::new();
                for i in 0..col_count {
                    row.push(sqlite3_column_text(stmt, i));
                }
                rows.push(row);
            }
            Ok(StepResult::Done) => break,
            Err(e) => return Err(e.sqlite_errmsg()),
        }
    }

    Ok(rows)
}

fn init_sqlite() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        sqlite3_initialize().expect("sqlite3_initialize");
    });
}

#[test]
fn test_fts3_persists_internal_content() {
    init_sqlite();
    let path = "/tmp/rustql_fts3_persist.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}-journal", path));
    let _ = fs::remove_file(format!("{}-wal", path));
    let _ = fs::remove_file(format!("{}-shm", path));

    let mut conn = sqlite3_open(path).expect("open db");
    exec_sql(
        &mut conn,
        "CREATE VIRTUAL TABLE docs USING fts3(title, body);
         INSERT INTO docs(rowid, title, body) VALUES(1, 'hello', 'world');
         INSERT INTO docs(rowid, title, body) VALUES(2, 'goodbye', 'moon');",
    )
    .expect("setup");
    sqlite3_close(conn).expect("close");

    let mut conn = sqlite3_open(path).expect("reopen");
    let rows = exec_sql(
        &mut conn,
        "SELECT rowid FROM docs WHERE docs MATCH 'hello';",
    )
    .expect("query");
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    let rows = exec_sql(
        &mut conn,
        "SELECT snippet(docs) FROM docs WHERE docs MATCH 'hello';",
    )
    .expect("snippet");
    assert!(!rows.is_empty(), "snippet returns rows");
    assert!(!rows[0][0].is_empty(), "snippet returns text");
    sqlite3_close(conn).expect("close");
}

#[test]
fn test_fts3_persists_external_content() {
    init_sqlite();
    let path = "/tmp/rustql_fts3_external_content.db";
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}-journal", path));
    let _ = fs::remove_file(format!("{}-wal", path));
    let _ = fs::remove_file(format!("{}-shm", path));

    let mut conn = sqlite3_open(path).expect("open db");
    exec_sql(
        &mut conn,
        "CREATE TABLE content(docid INTEGER PRIMARY KEY, title, body);
         CREATE VIRTUAL TABLE docs USING fts3(title, body, content=content);
         INSERT INTO docs(rowid, title, body) VALUES(1, 'alpha', 'beta');",
    )
    .expect("setup");
    let rows = exec_sql(&mut conn, "SELECT count(*) FROM content;").expect("content read");
    assert_eq!(rows, vec![vec!["1".to_string()]]);
    sqlite3_close(conn).expect("close");

    let mut conn = sqlite3_open(path).expect("reopen");
    let rows = exec_sql(
        &mut conn,
        "SELECT rowid FROM docs WHERE docs MATCH 'alpha';",
    )
    .expect("query");
    assert_eq!(rows, vec![vec!["1".to_string()]]);
    sqlite3_close(conn).expect("close");
}
