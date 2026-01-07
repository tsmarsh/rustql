//! PRAGMA handling
//!
//! This module provides minimal PRAGMA execution support using the
//! connection schema and configuration state.

use std::sync::{Arc, RwLock};

use crate::api::{AutoVacuum, DbInfo, SafetyLevel, SqliteConnection};
use crate::error::{Error, ErrorCode, Result};
use crate::parser::ast::{Expr, Literal, PragmaStmt, PragmaValue};
use crate::schema::{DefaultValue, Schema};
use crate::storage::pager::JournalMode;
use crate::types::{ColumnType, Value};

pub struct PragmaResult {
    pub columns: Vec<String>,
    pub types: Vec<ColumnType>,
    pub rows: Vec<Vec<Value>>,
}

pub fn pragma_columns(pragma: &PragmaStmt) -> Option<(Vec<String>, Vec<ColumnType>)> {
    let name = pragma.name.to_lowercase();
    let (columns, types) = match name.as_str() {
        "database_list" => (
            vec!["seq", "name", "file"],
            vec![ColumnType::Integer, ColumnType::Text, ColumnType::Text],
        ),
        "table_info" => (
            vec!["cid", "name", "type", "notnull", "dflt_value", "pk"],
            vec![
                ColumnType::Integer,
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Integer,
                ColumnType::Text,
                ColumnType::Integer,
            ],
        ),
        "index_list" => (
            vec!["seq", "name", "unique", "origin", "partial"],
            vec![
                ColumnType::Integer,
                ColumnType::Text,
                ColumnType::Integer,
                ColumnType::Text,
                ColumnType::Integer,
            ],
        ),
        "index_info" => (
            vec!["seqno", "cid", "name"],
            vec![ColumnType::Integer, ColumnType::Integer, ColumnType::Text],
        ),
        "foreign_key_list" => (
            vec![
                "id",
                "seq",
                "table",
                "from",
                "to",
                "on_update",
                "on_delete",
                "match",
            ],
            vec![
                ColumnType::Integer,
                ColumnType::Integer,
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Text,
            ],
        ),
        "foreign_key_check" => (
            vec!["table", "rowid", "parent", "fkid"],
            vec![
                ColumnType::Text,
                ColumnType::Integer,
                ColumnType::Text,
                ColumnType::Integer,
            ],
        ),
        "wal_checkpoint" => (
            vec!["busy", "log", "checkpointed"],
            vec![
                ColumnType::Integer,
                ColumnType::Integer,
                ColumnType::Integer,
            ],
        ),
        _ => return None,
    };

    let columns = columns.into_iter().map(|c| c.to_string()).collect();
    Some((columns, types))
}

pub fn execute_pragma(conn: &mut SqliteConnection, pragma: &PragmaStmt) -> Result<PragmaResult> {
    let name = pragma.name.to_lowercase();
    let schema_name = pragma.schema.as_deref().unwrap_or("main");

    match name.as_str() {
        "database_list" => pragma_database_list(conn),
        "table_info" => pragma_table_info(conn, schema_name, pragma),
        "index_list" => pragma_index_list(conn, schema_name, pragma),
        "index_info" => pragma_index_info(conn, schema_name, pragma),
        "foreign_key_list" => pragma_foreign_key_list(conn, schema_name, pragma),
        "foreign_key_check" => pragma_foreign_key_check(conn, schema_name, pragma),
        "page_size" => pragma_page_size(conn, schema_name, pragma),
        "page_count" => pragma_page_count(conn),
        "cache_size" => pragma_cache_size(conn, schema_name, pragma),
        "synchronous" => pragma_synchronous(conn, schema_name, pragma),
        "foreign_keys" => pragma_foreign_keys(conn, pragma),
        "recursive_triggers" => pragma_recursive_triggers(conn, pragma),
        "journal_mode" => pragma_journal_mode(conn, schema_name, pragma),
        "wal_checkpoint" => pragma_wal_checkpoint(),
        "auto_vacuum" => pragma_auto_vacuum(conn, pragma),
        "encoding" => pragma_encoding(conn, pragma),
        _ => Err(Error::with_message(
            ErrorCode::Error,
            format!("unknown pragma: {}", pragma.name),
        )),
    }
}

fn pragma_database_list(conn: &SqliteConnection) -> Result<PragmaResult> {
    let mut rows = Vec::new();
    for (idx, db) in conn.dbs.iter().enumerate() {
        let file = db
            .path
            .as_ref()
            .map(|p| Value::Text(p.clone()))
            .unwrap_or(Value::Null);
        rows.push(vec![
            Value::Integer(idx as i64),
            Value::Text(db.name.clone()),
            file,
        ]);
    }

    Ok(PragmaResult {
        columns: vec!["seq".into(), "name".into(), "file".into()],
        types: vec![ColumnType::Integer, ColumnType::Text, ColumnType::Text],
        rows,
    })
}

fn pragma_table_info(
    conn: &SqliteConnection,
    schema_name: &str,
    pragma: &PragmaStmt,
) -> Result<PragmaResult> {
    let table_name = pragma_arg_string(pragma)?;
    let schema = lookup_schema(conn, schema_name)?;
    let schema = schema.read().unwrap();
    let table = schema
        .table(&table_name)
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "no such table".to_string()))?;

    let pk_list = table.primary_key.clone().unwrap_or_default();
    let mut rows = Vec::new();
    for (idx, col) in table.columns.iter().enumerate() {
        let pk_pos = pk_list
            .iter()
            .position(|&cidx| cidx == idx)
            .map(|pos| (pos + 1) as i64)
            .unwrap_or(if col.is_primary_key { 1 } else { 0 });
        let dflt = col
            .default_value
            .as_ref()
            .map(default_value_to_text)
            .map(Value::Text)
            .unwrap_or(Value::Null);
        rows.push(vec![
            Value::Integer(idx as i64),
            Value::Text(col.name.clone()),
            Value::Text(col.type_name.clone().unwrap_or_default()),
            Value::Integer(i64::from(col.not_null)),
            dflt,
            Value::Integer(pk_pos),
        ]);
    }

    Ok(PragmaResult {
        columns: vec![
            "cid".into(),
            "name".into(),
            "type".into(),
            "notnull".into(),
            "dflt_value".into(),
            "pk".into(),
        ],
        types: vec![
            ColumnType::Integer,
            ColumnType::Text,
            ColumnType::Text,
            ColumnType::Integer,
            ColumnType::Text,
            ColumnType::Integer,
        ],
        rows,
    })
}

fn pragma_index_list(
    conn: &SqliteConnection,
    schema_name: &str,
    pragma: &PragmaStmt,
) -> Result<PragmaResult> {
    let table_name = pragma_arg_string(pragma)?;
    let schema = lookup_schema(conn, schema_name)?;
    let schema = schema.read().unwrap();
    let table = schema
        .table(&table_name)
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "no such table".to_string()))?;

    let mut rows = Vec::new();
    for (idx, index) in table.indexes.iter().enumerate() {
        let origin = if index.is_primary_key { "pk" } else { "c" };
        rows.push(vec![
            Value::Integer(idx as i64),
            Value::Text(index.name.clone()),
            Value::Integer(i64::from(index.unique)),
            Value::Text(origin.to_string()),
            Value::Integer(i64::from(index.partial.is_some())),
        ]);
    }

    Ok(PragmaResult {
        columns: vec![
            "seq".into(),
            "name".into(),
            "unique".into(),
            "origin".into(),
            "partial".into(),
        ],
        types: vec![
            ColumnType::Integer,
            ColumnType::Text,
            ColumnType::Integer,
            ColumnType::Text,
            ColumnType::Integer,
        ],
        rows,
    })
}

fn pragma_index_info(
    conn: &SqliteConnection,
    schema_name: &str,
    pragma: &PragmaStmt,
) -> Result<PragmaResult> {
    let index_name = pragma_arg_string(pragma)?;
    let schema = lookup_schema(conn, schema_name)?;
    let schema = schema.read().unwrap();
    let index = schema
        .index(&index_name)
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "no such index".to_string()))?;

    let table = schema.table(&index.table);
    let mut rows = Vec::new();
    for (seqno, col) in index.columns.iter().enumerate() {
        let (cid, name) = match (col.column_idx, table.as_deref()) {
            (idx, Some(table)) if idx >= 0 => {
                let idx = idx as usize;
                let name = table
                    .columns
                    .get(idx)
                    .map(|col| col.name.clone())
                    .unwrap_or_default();
                (Value::Integer(idx as i64), Value::Text(name))
            }
            _ => (Value::Integer(-1), Value::Null),
        };
        rows.push(vec![Value::Integer(seqno as i64), cid, name]);
    }

    Ok(PragmaResult {
        columns: vec!["seqno".into(), "cid".into(), "name".into()],
        types: vec![ColumnType::Integer, ColumnType::Integer, ColumnType::Text],
        rows,
    })
}

fn pragma_foreign_key_list(
    conn: &SqliteConnection,
    schema_name: &str,
    pragma: &PragmaStmt,
) -> Result<PragmaResult> {
    let table_name = pragma_arg_string(pragma)?;
    let schema = lookup_schema(conn, schema_name)?;
    let schema = schema.read().unwrap();
    let table = schema
        .table(&table_name)
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "no such table".to_string()))?;

    let mut rows = Vec::new();
    for (id, fk) in table.foreign_keys.iter().enumerate() {
        let from_cols = fk
            .columns
            .iter()
            .map(|idx| table.columns.get(*idx).map(|c| c.name.clone()))
            .collect::<Vec<_>>();
        let to_cols = fk.ref_columns.clone().unwrap_or_default();

        for (seq, from) in from_cols.iter().enumerate() {
            let from = from.clone().unwrap_or_default();
            let to = to_cols.get(seq).cloned().unwrap_or_default();
            rows.push(vec![
                Value::Integer(id as i64),
                Value::Integer(seq as i64),
                Value::Text(fk.ref_table.clone()),
                Value::Text(from),
                Value::Text(to),
                Value::Text(fk_action_name(fk.on_update)),
                Value::Text(fk_action_name(fk.on_delete)),
                Value::Text("NONE".to_string()),
            ]);
        }
    }

    Ok(PragmaResult {
        columns: vec![
            "id".into(),
            "seq".into(),
            "table".into(),
            "from".into(),
            "to".into(),
            "on_update".into(),
            "on_delete".into(),
            "match".into(),
        ],
        types: vec![
            ColumnType::Integer,
            ColumnType::Integer,
            ColumnType::Text,
            ColumnType::Text,
            ColumnType::Text,
            ColumnType::Text,
            ColumnType::Text,
            ColumnType::Text,
        ],
        rows,
    })
}

fn pragma_foreign_key_check(
    conn: &SqliteConnection,
    schema_name: &str,
    pragma: &PragmaStmt,
) -> Result<PragmaResult> {
    let schema = lookup_schema(conn, schema_name)?;
    let schema_guard = schema
        .read()
        .map_err(|_| Error::with_message(ErrorCode::Busy, "schema lock"))?;

    // Get table name if specified (optional - checks single table)
    let table_name = pragma.value.as_ref().and_then(|v| match v {
        PragmaValue::Set(Expr::Literal(Literal::String(s))) => Some(s.clone()),
        PragmaValue::Set(Expr::Column(col_ref)) => Some(col_ref.column.clone()),
        PragmaValue::Call(Expr::Literal(Literal::String(s))) => Some(s.clone()),
        PragmaValue::Call(Expr::Column(col_ref)) => Some(col_ref.column.clone()),
        _ => None,
    });

    // Try to get btree for FK checking
    let btree = conn.dbs.first().and_then(|db| db.btree.clone());

    let violations = if let Some(ref btree) = btree {
        crate::executor::fkey::foreign_key_check(&schema_guard, btree, table_name.as_deref())?
    } else {
        Vec::new()
    };

    let rows: Vec<Vec<Value>> = violations
        .iter()
        .map(|v| {
            vec![
                Value::Text(v.table.clone()),
                Value::Integer(v.rowid),
                Value::Text(v.parent.clone()),
                Value::Integer(v.fkid as i64),
            ]
        })
        .collect();

    Ok(PragmaResult {
        columns: vec![
            "table".into(),
            "rowid".into(),
            "parent".into(),
            "fkid".into(),
        ],
        types: vec![
            ColumnType::Text,
            ColumnType::Integer,
            ColumnType::Text,
            ColumnType::Integer,
        ],
        rows,
    })
}

fn pragma_page_size(
    conn: &mut SqliteConnection,
    schema_name: &str,
    pragma: &PragmaStmt,
) -> Result<PragmaResult> {
    let db = lookup_db_mut(conn, schema_name)?;
    if let Some(value) = pragma_value_i64(pragma) {
        if value > 0 {
            db.page_size = value as u32;
        }
    }
    if pragma.value.is_none() {
        return Ok(single_int_result(db.page_size as i64));
    }
    Ok(empty_result())
}

fn pragma_page_count(_conn: &SqliteConnection) -> Result<PragmaResult> {
    Ok(single_int_result(0))
}

fn pragma_cache_size(
    conn: &mut SqliteConnection,
    schema_name: &str,
    pragma: &PragmaStmt,
) -> Result<PragmaResult> {
    let db = lookup_db_mut(conn, schema_name)?;
    if let Some(value) = pragma_value_i64(pragma) {
        db.cache_size = value;
    }
    if pragma.value.is_none() {
        return Ok(single_int_result(db.cache_size));
    }
    Ok(empty_result())
}

fn pragma_synchronous(
    conn: &mut SqliteConnection,
    schema_name: &str,
    pragma: &PragmaStmt,
) -> Result<PragmaResult> {
    let db = lookup_db_mut(conn, schema_name)?;
    if let Some(value) = pragma_value_string(pragma) {
        let level = safety_level_from_str(&value, db.safety_level);
        db.safety_level = level;
    }
    if pragma.value.is_none() {
        return Ok(single_int_result(db.safety_level as i64));
    }
    Ok(empty_result())
}

fn pragma_foreign_keys(conn: &mut SqliteConnection, pragma: &PragmaStmt) -> Result<PragmaResult> {
    if let Some(value) = pragma_value_i64(pragma) {
        conn.db_config.enable_fkey = value != 0;
    }
    if pragma.value.is_none() {
        return Ok(single_int_result(i64::from(conn.db_config.enable_fkey)));
    }
    Ok(empty_result())
}

fn pragma_recursive_triggers(
    conn: &mut SqliteConnection,
    pragma: &PragmaStmt,
) -> Result<PragmaResult> {
    if let Some(value) = pragma_value_i64(pragma) {
        conn.db_config.recursive_triggers = value != 0;
    }
    if pragma.value.is_none() {
        return Ok(single_int_result(i64::from(
            conn.db_config.recursive_triggers,
        )));
    }
    Ok(empty_result())
}

fn pragma_journal_mode(
    conn: &mut SqliteConnection,
    schema_name: &str,
    pragma: &PragmaStmt,
) -> Result<PragmaResult> {
    let db = lookup_db_mut(conn, schema_name)?;
    if let Some(value) = pragma_value_string(pragma) {
        if let Some(mode) = journal_mode_from_str(&value) {
            db.journal_mode = mode;
        }
    }
    if pragma.value.is_none() {
        let mode = journal_mode_name(db.journal_mode);
        return Ok(single_text_result(mode.to_string()));
    }
    Ok(empty_result())
}

fn pragma_wal_checkpoint() -> Result<PragmaResult> {
    let rows = vec![vec![
        Value::Integer(0),
        Value::Integer(0),
        Value::Integer(0),
    ]];
    Ok(PragmaResult {
        columns: vec!["busy".into(), "log".into(), "checkpointed".into()],
        types: vec![
            ColumnType::Integer,
            ColumnType::Integer,
            ColumnType::Integer,
        ],
        rows,
    })
}

fn pragma_auto_vacuum(conn: &mut SqliteConnection, pragma: &PragmaStmt) -> Result<PragmaResult> {
    if let Some(value) = pragma_value_i64(pragma) {
        conn.auto_vacuum = match value {
            1 => AutoVacuum::Full,
            2 => AutoVacuum::Incremental,
            _ => AutoVacuum::None,
        };
    }
    if pragma.value.is_none() {
        return Ok(single_int_result(conn.auto_vacuum as i64));
    }
    Ok(empty_result())
}

fn pragma_encoding(conn: &mut SqliteConnection, pragma: &PragmaStmt) -> Result<PragmaResult> {
    if let Some(value) = pragma_value_string(pragma) {
        let encoding = match value.to_lowercase().as_str() {
            "utf8" | "utf-8" => crate::schema::Encoding::Utf8,
            "utf16le" | "utf-16le" => crate::schema::Encoding::Utf16le,
            "utf16be" | "utf-16be" => crate::schema::Encoding::Utf16be,
            _ => conn.encoding,
        };
        conn.encoding = encoding;
    }
    if pragma.value.is_none() {
        let name = match conn.encoding {
            crate::schema::Encoding::Utf8 => "UTF-8",
            crate::schema::Encoding::Utf16le => "UTF-16LE",
            crate::schema::Encoding::Utf16be => "UTF-16BE",
        };
        return Ok(single_text_result(name.to_string()));
    }
    Ok(empty_result())
}

fn single_int_result(value: i64) -> PragmaResult {
    PragmaResult {
        columns: vec![],
        types: vec![ColumnType::Integer],
        rows: vec![vec![Value::Integer(value)]],
    }
}

fn single_text_result(value: String) -> PragmaResult {
    PragmaResult {
        columns: vec![],
        types: vec![ColumnType::Text],
        rows: vec![vec![Value::Text(value)]],
    }
}

fn empty_result() -> PragmaResult {
    PragmaResult {
        columns: Vec::new(),
        types: Vec::new(),
        rows: Vec::new(),
    }
}

fn pragma_arg_string(pragma: &PragmaStmt) -> Result<String> {
    let expr = match pragma.value.as_ref() {
        Some(PragmaValue::Set(expr)) => expr,
        Some(PragmaValue::Call(expr)) => expr,
        None => {
            return Err(Error::with_message(
                ErrorCode::Error,
                "missing pragma argument".to_string(),
            ))
        }
    };
    expr_to_string(expr)
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "invalid pragma argument".to_string()))
}

fn pragma_value_i64(pragma: &PragmaStmt) -> Option<i64> {
    let expr = match pragma.value.as_ref() {
        Some(PragmaValue::Set(expr)) => expr,
        Some(PragmaValue::Call(expr)) => expr,
        None => return None,
    };
    expr_to_value(expr).map(|v| v.to_i64())
}

fn pragma_value_string(pragma: &PragmaStmt) -> Option<String> {
    let expr = match pragma.value.as_ref() {
        Some(PragmaValue::Set(expr)) => expr,
        Some(PragmaValue::Call(expr)) => expr,
        None => return None,
    };
    expr_to_string(expr)
}

fn expr_to_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Literal(Literal::Null) => Some(Value::Null),
        Expr::Literal(Literal::Integer(v)) => Some(Value::Integer(*v)),
        Expr::Literal(Literal::Float(v)) => Some(Value::Real(*v)),
        Expr::Literal(Literal::String(v)) => Some(Value::Text(v.clone())),
        Expr::Literal(Literal::Blob(v)) => Some(Value::Blob(v.clone())),
        Expr::Literal(Literal::Bool(v)) => Some(Value::Integer(i64::from(*v))),
        Expr::Column(col) => Some(Value::Text(col.column.clone())),
        Expr::Unary { op, expr } => {
            if let Some(value) = expr_to_value(expr) {
                match op {
                    crate::parser::ast::UnaryOp::Neg => Some(Value::Integer(-value.to_i64())),
                    crate::parser::ast::UnaryOp::Pos => Some(Value::Integer(value.to_i64())),
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

fn expr_to_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(Literal::String(v)) => Some(v.clone()),
        Expr::Literal(Literal::Integer(v)) => Some(v.to_string()),
        Expr::Literal(Literal::Float(v)) => Some(v.to_string()),
        Expr::Literal(Literal::Bool(v)) => Some(if *v { "1" } else { "0" }.to_string()),
        Expr::Column(col) => Some(col.column.clone()),
        _ => None,
    }
}

fn lookup_schema(conn: &SqliteConnection, schema_name: &str) -> Result<Arc<RwLock<Schema>>> {
    let db = conn.find_db(schema_name).ok_or_else(|| {
        Error::with_message(
            ErrorCode::Error,
            format!("unknown database {}", schema_name),
        )
    })?;
    db.schema
        .as_ref()
        .cloned()
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "schema unavailable".to_string()))
}

fn lookup_db_mut<'a>(conn: &'a mut SqliteConnection, schema_name: &str) -> Result<&'a mut DbInfo> {
    conn.find_db_mut(schema_name).ok_or_else(|| {
        Error::with_message(
            ErrorCode::Error,
            format!("unknown database {}", schema_name),
        )
    })
}

fn default_value_to_text(value: &DefaultValue) -> String {
    match value {
        DefaultValue::Null => "NULL".to_string(),
        DefaultValue::Integer(v) => v.to_string(),
        DefaultValue::Float(v) => v.to_string(),
        DefaultValue::String(v) => format!("'{}'", v),
        DefaultValue::Blob(_) => "X''".to_string(),
        DefaultValue::Expr(_) => "".to_string(),
        DefaultValue::CurrentTime => "CURRENT_TIME".to_string(),
        DefaultValue::CurrentDate => "CURRENT_DATE".to_string(),
        DefaultValue::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
    }
}

fn fk_action_name(action: crate::schema::FkAction) -> String {
    match action {
        crate::schema::FkAction::SetNull => "SET NULL",
        crate::schema::FkAction::SetDefault => "SET DEFAULT",
        crate::schema::FkAction::Cascade => "CASCADE",
        crate::schema::FkAction::Restrict => "RESTRICT",
        crate::schema::FkAction::NoAction => "NO ACTION",
    }
    .to_string()
}

fn safety_level_from_str(value: &str, default: SafetyLevel) -> SafetyLevel {
    let lower = value.to_lowercase();
    match lower.as_str() {
        "off" | "0" => SafetyLevel::Off,
        "on" | "normal" | "1" => SafetyLevel::Normal,
        "full" | "2" => SafetyLevel::Full,
        "extra" | "3" => SafetyLevel::Extra,
        _ => default,
    }
}

fn journal_mode_from_str(value: &str) -> Option<JournalMode> {
    match value.to_lowercase().as_str() {
        "delete" => Some(JournalMode::Delete),
        "persist" => Some(JournalMode::Persist),
        "off" => Some(JournalMode::Off),
        "truncate" => Some(JournalMode::Truncate),
        "memory" => Some(JournalMode::Memory),
        "wal" => Some(JournalMode::Wal),
        _ => None,
    }
}

fn journal_mode_name(mode: JournalMode) -> &'static str {
    match mode {
        JournalMode::Delete => "delete",
        JournalMode::Persist => "persist",
        JournalMode::Off => "off",
        JournalMode::Truncate => "truncate",
        JournalMode::Memory => "memory",
        JournalMode::Wal => "wal",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, Table};

    #[test]
    fn test_database_list() {
        let conn = SqliteConnection::new();
        let result = pragma_database_list(&conn).unwrap();
        assert!(result.rows.len() >= 2);
    }

    #[test]
    fn test_table_info() {
        let mut conn = SqliteConnection::new();
        let schema = conn.main_db().schema.as_ref().unwrap().clone();
        let mut schema = schema.write().unwrap();
        let mut table = Table::new("t");
        table.columns.push(Column::new("id"));
        schema.tables.insert("t".to_string(), Arc::new(table));
        drop(schema);

        let pragma = PragmaStmt {
            schema: None,
            name: "table_info".to_string(),
            value: Some(PragmaValue::Call(Expr::Column(
                crate::parser::ast::ColumnRef {
                    database: None,
                    table: None,
                    column: "t".to_string(),
                    column_index: None,
                },
            ))),
        };

        let result = execute_pragma(&mut conn, &pragma).unwrap();
        assert_eq!(result.rows.len(), 1);
    }
}
