//! ANALYZE command execution
//!
//! Translates sqlite3/src/analyze.c behavior into RustQL's execution model.

use std::sync::Arc;

use crate::error::{Error, ErrorCode, Result};
use crate::parser::ast::QualifiedName;
use crate::schema::{Affinity, Column, Index, Schema, Stat1Row, Table};
use crate::SqliteConnection;

const ENABLE_STAT4: bool = false;

/// Execute an ANALYZE statement.
pub fn execute_analyze(conn: &mut SqliteConnection, target: Option<QualifiedName>) -> Result<()> {
    match target {
        None => analyze_all_databases(conn),
        Some(name) => analyze_target(conn, name),
    }
}

fn analyze_all_databases(conn: &mut SqliteConnection) -> Result<()> {
    let db_names: Vec<String> = conn.dbs.iter().map(|db| db.name.clone()).collect();
    for name in db_names {
        analyze_database(conn, &name, false)?;
    }
    Ok(())
}

fn analyze_target(conn: &mut SqliteConnection, target: QualifiedName) -> Result<()> {
    if let Some(schema) = target.schema.as_ref() {
        return analyze_table(conn, schema, &target.name);
    }

    if conn.find_db(&target.name).is_some() {
        return analyze_database(conn, &target.name, false);
    }

    if let Some(schema_name) = find_schema_for_table(conn, &target.name) {
        return analyze_table(conn, &schema_name, &target.name);
    }

    Err(Error::with_message(
        ErrorCode::Error,
        format!("no such table: {}", target.name),
    ))
}

fn analyze_database(
    conn: &mut SqliteConnection,
    schema_name: &str,
    include_system: bool,
) -> Result<()> {
    let schema_arc = schema_for_db(conn, schema_name)?;
    let table_names: Vec<String> = {
        let schema = schema_arc
            .read()
            .map_err(|_| Error::new(ErrorCode::Internal))?;
        schema
            .tables
            .values()
            .filter_map(|table| {
                if !include_system && is_system_table(&table.name) {
                    None
                } else {
                    Some(table.name.clone())
                }
            })
            .collect()
    };

    for table_name in table_names {
        analyze_table(conn, schema_name, &table_name)?;
    }

    Ok(())
}

fn analyze_table(conn: &mut SqliteConnection, schema_name: &str, table_name: &str) -> Result<()> {
    let schema_arc = schema_for_db(conn, schema_name)?;
    let (table, indexes) = {
        let schema = schema_arc
            .read()
            .map_err(|_| Error::new(ErrorCode::Internal))?;
        let table = schema.table(table_name).ok_or_else(|| {
            Error::with_message(ErrorCode::Error, format!("no such table: {}", table_name))
        })?;
        let indexes = schema
            .indexes
            .values()
            .filter(|idx| idx.table.eq_ignore_ascii_case(&table.name))
            .cloned()
            .collect::<Vec<Arc<Index>>>();
        (table, indexes)
    };

    let row_count = estimate_table_rows(&table);

    let mut schema = schema_arc
        .write()
        .map_err(|_| Error::new(ErrorCode::Internal))?;
    ensure_stat1_table(&mut schema, schema_name)?;
    if ENABLE_STAT4 {
        ensure_stat4_table(&mut schema, schema_name)?;
    }
    schema.clear_stat1_for_table(&table.name);
    schema.set_stat1(Stat1Row {
        tbl: table.name.clone(),
        idx: None,
        stat: row_count.to_string(),
    })?;

    for index in indexes {
        let stat = index_stat_string(row_count, index.columns.len());
        schema.set_stat1(Stat1Row {
            tbl: table.name.clone(),
            idx: Some(index.name.clone()),
            stat,
        })?;
    }

    Ok(())
}

fn schema_for_db(
    conn: &mut SqliteConnection,
    schema_name: &str,
) -> Result<Arc<std::sync::RwLock<Schema>>> {
    let db = conn
        .find_db(schema_name)
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "unknown database".to_string()))?;
    db.schema
        .as_ref()
        .cloned()
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "schema unavailable".to_string()))
}

fn find_schema_for_table(conn: &SqliteConnection, table_name: &str) -> Option<String> {
    for db in &conn.dbs {
        if let Some(schema_arc) = db.schema.as_ref() {
            if let Ok(schema) = schema_arc.read() {
                if schema.table(table_name).is_some() {
                    return Some(db.name.clone());
                }
            }
        }
    }
    None
}

fn is_system_table(name: &str) -> bool {
    name.to_lowercase().starts_with("sqlite_")
}

fn estimate_table_rows(_table: &Table) -> i64 {
    // Storage is not wired into VDBE yet; return zero until btree integration.
    0
}

fn index_stat_string(row_count: i64, column_count: usize) -> String {
    let mut parts = Vec::with_capacity(column_count + 1);
    parts.push(row_count.to_string());
    let avg = if row_count > 0 { row_count } else { 0 };
    for _ in 0..column_count {
        parts.push(avg.to_string());
    }
    parts.join(" ")
}

fn ensure_stat1_table(schema: &mut Schema, schema_name: &str) -> Result<()> {
    if schema.table_exists("sqlite_stat1") {
        return Ok(());
    }

    let mut table = Table::new("sqlite_stat1");
    table.db_idx = database_idx(schema_name);
    table.columns = vec![stat_column("tbl"), stat_column("idx"), stat_column("stat")];
    table.sql = Some("CREATE TABLE sqlite_stat1(tbl,idx,stat)".to_string());

    schema
        .tables
        .insert("sqlite_stat1".to_string(), Arc::new(table));

    Ok(())
}

fn stat_column(name: &str) -> Column {
    Column {
        name: name.to_string(),
        type_name: Some("TEXT".to_string()),
        affinity: Affinity::Text,
        ..Default::default()
    }
}

fn stat_blob_column(name: &str) -> Column {
    Column {
        name: name.to_string(),
        type_name: Some("BLOB".to_string()),
        affinity: Affinity::Blob,
        ..Default::default()
    }
}

fn database_idx(schema_name: &str) -> i32 {
    if schema_name.eq_ignore_ascii_case("main") {
        0
    } else if schema_name.eq_ignore_ascii_case("temp") {
        1
    } else {
        2
    }
}

fn ensure_stat4_table(schema: &mut Schema, schema_name: &str) -> Result<()> {
    if schema.table_exists("sqlite_stat4") {
        return Ok(());
    }

    let mut table = Table::new("sqlite_stat4");
    table.db_idx = database_idx(schema_name);
    table.columns = vec![
        stat_column("tbl"),
        stat_column("idx"),
        stat_blob_column("nlt"),
        stat_blob_column("ndlt"),
        stat_blob_column("neq"),
        stat_blob_column("sample"),
    ];
    table.sql = Some("CREATE TABLE sqlite_stat4(tbl,idx,nlt,ndlt,neq,sample)".to_string());

    schema
        .tables
        .insert("sqlite_stat4".to_string(), Arc::new(table));

    Ok(())
}
