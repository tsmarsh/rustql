use std::sync::Once;

use rustql::types::StepResult;
use rustql::{
    sqlite3_close, sqlite3_column_text, sqlite3_finalize, sqlite3_initialize, sqlite3_open,
    sqlite3_prepare_v2, sqlite3_step, SqliteConnection,
};

static INIT: Once = Once::new();

fn init() {
    INIT.call_once(|| {
        let _ = sqlite3_initialize();
    });
}

fn exec(conn: &mut SqliteConnection, sql: &str) {
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

fn explain_details(conn: &mut SqliteConnection, sql: &str) -> Vec<String> {
    let (mut stmt, _) = sqlite3_prepare_v2(conn, sql).unwrap();
    let mut details = Vec::new();
    loop {
        match sqlite3_step(&mut stmt) {
            Ok(StepResult::Row) => {
                details.push(sqlite3_column_text(&stmt, 3));
            }
            Ok(StepResult::Done) => break,
            Err(err) => panic!("explain failed for '{}': {}", sql, err),
        }
    }
    let _ = sqlite3_finalize(stmt);
    details
}

#[test]
fn test_single_column_equality_uses_index() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();
    exec(&mut conn, "CREATE TABLE t1(a INT, b INT)");
    exec(&mut conn, "CREATE INDEX i1 ON t1(a)");

    let details = explain_details(&mut conn, "EXPLAIN QUERY PLAN SELECT * FROM t1 WHERE a = 5");
    assert!(details.iter().any(|d| d.contains("USING INDEX i1")));
    assert!(details.iter().any(|d| d.contains("a=?")));

    sqlite3_close(conn).unwrap();
}

#[test]
fn test_multi_column_index_partial_match() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();
    exec(&mut conn, "CREATE TABLE t1(a INT, b INT, c INT)");
    exec(&mut conn, "CREATE INDEX i1 ON t1(a, b)");

    let details = explain_details(&mut conn, "EXPLAIN QUERY PLAN SELECT * FROM t1 WHERE a = 5");
    assert!(details.iter().any(|d| d.contains("USING INDEX i1")));

    sqlite3_close(conn).unwrap();
}

#[test]
fn test_no_index_when_no_match() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();
    exec(&mut conn, "CREATE TABLE t1(a INT, b INT)");
    exec(&mut conn, "CREATE INDEX i1 ON t1(a)");

    let details = explain_details(&mut conn, "EXPLAIN QUERY PLAN SELECT * FROM t1 WHERE b = 5");
    // Changed from "SCAN TABLE t1" to "SCAN t1" to match SQLite format
    assert!(details.iter().any(|d| d.contains("SCAN t1")));

    sqlite3_close(conn).unwrap();
}

#[test]
fn test_covering_index_preferred() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();
    exec(&mut conn, "CREATE TABLE t1(a INT, b INT, c INT)");
    exec(&mut conn, "CREATE INDEX i1 ON t1(a, b)");

    let details = explain_details(
        &mut conn,
        "EXPLAIN QUERY PLAN SELECT a, b FROM t1 WHERE a = 5",
    );
    assert!(details.iter().any(|d| d.contains("COVERING INDEX i1")));

    sqlite3_close(conn).unwrap();
}

#[test]
fn test_range_query_uses_index() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();
    exec(&mut conn, "CREATE TABLE t1(a INT)");
    exec(&mut conn, "CREATE INDEX i1 ON t1(a)");

    let details = explain_details(&mut conn, "EXPLAIN QUERY PLAN SELECT * FROM t1 WHERE a > 5");
    assert!(details.iter().any(|d| d.contains("USING INDEX i1")));

    sqlite3_close(conn).unwrap();
}

#[test]
fn test_index_with_multiple_conditions() {
    init();
    let mut conn = sqlite3_open(":memory:").unwrap();
    exec(&mut conn, "CREATE TABLE t1(a INT, b INT)");
    exec(&mut conn, "CREATE INDEX i1 ON t1(a, b)");

    let details = explain_details(
        &mut conn,
        "EXPLAIN QUERY PLAN SELECT * FROM t1 WHERE a = 5 AND b = 10",
    );
    assert!(details.iter().any(|d| d.contains("USING INDEX i1")));
    assert!(details.iter().any(|d| d.contains("a=?")));
    assert!(details.iter().any(|d| d.contains("b=?")));

    sqlite3_close(conn).unwrap();
}
