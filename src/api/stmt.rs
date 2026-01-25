//! Prepared statement handling
//!
//! This module implements sqlite3_stmt (prepared statement) and related functions.

use std::sync::atomic::Ordering;

use crate::error::{Error, ErrorCode, Result};
use crate::executor::analyze::execute_analyze;
use crate::executor::pragma::{execute_pragma, pragma_columns};
use crate::executor::prepare::{
    compile_sql, compile_sql_with_config, compile_sql_with_schema, CompiledStmt, StmtType,
};
use crate::parser::ast::{AttachStmt, Expr, Literal, QualifiedName, Variable};
use crate::types::{ColumnType, StepResult, Value};
use crate::vdbe::engine::Vdbe;
use crate::vdbe::ops::VdbeOp;
use crate::vdbe::reset_sort_count;

use super::connection::SqliteConnection;

// ============================================================================
// Prepared Statement
// ============================================================================

/// Prepared statement (sqlite3_stmt)
pub struct PreparedStmt {
    /// SQL text
    sql: String,
    /// Remaining SQL (tail after parsing)
    tail: String,
    /// Column names
    column_names: Vec<String>,
    /// Column types (declared or inferred)
    column_types: Vec<ColumnType>,
    /// Parameter values (1-indexed internally)
    params: Vec<Value>,
    /// Parameter names (for named parameters)
    param_names: Vec<Option<String>>,
    /// Number of parameters
    param_count: i32,
    /// Current row values
    row_values: Vec<Value>,
    /// Has been stepped
    stepped: bool,
    /// Execution complete
    done: bool,
    /// Is read-only statement
    read_only: bool,
    /// Is EXPLAIN statement
    explain: i32,
    /// Expanded SQL (with bound parameters)
    expanded_sql: Option<String>,
    /// Compiled VDBE bytecode
    ops: Vec<VdbeOp>,
    /// Statement type
    stmt_type: Option<StmtType>,
    /// VDBE virtual machine (created on first step)
    vdbe: Option<Vdbe>,
    /// PRAGMA statement (if applicable)
    pragma: Option<crate::parser::ast::PragmaStmt>,
    /// PRAGMA execution state
    pragma_state: Option<PragmaState>,
    /// ANALYZE target (if applicable)
    analyze_target: Option<QualifiedName>,
    /// ATTACH statement (if applicable)
    attach_stmt: Option<AttachStmt>,
    /// DETACH schema name (if applicable)
    detach_name: Option<String>,
    /// Connection pointer for PRAGMA/ANALYZE execution
    conn_ptr: Option<*mut SqliteConnection>,
    /// Statement has been expired due to schema change
    expired: bool,
    /// Schema generation when statement was prepared
    schema_generation: u64,
}

impl PreparedStmt {
    /// Create a new prepared statement
    pub fn new(sql: &str) -> Self {
        Self {
            sql: sql.to_string(),
            tail: String::new(),
            column_names: Vec::new(),
            column_types: Vec::new(),
            params: Vec::new(),
            param_names: Vec::new(),
            param_count: 0,
            row_values: Vec::new(),
            stepped: false,
            done: false,
            read_only: true,
            explain: 0,
            expanded_sql: None,
            ops: Vec::new(),
            stmt_type: None,
            vdbe: None,
            pragma: None,
            pragma_state: None,
            analyze_target: None,
            attach_stmt: None,
            detach_name: None,
            conn_ptr: None,
            expired: false,
            schema_generation: 0,
        }
    }

    /// Create from a compiled statement
    pub fn from_compiled(sql: &str, compiled: CompiledStmt, tail: &str) -> Self {
        Self {
            sql: sql.to_string(),
            tail: tail.to_string(),
            column_names: compiled.column_names,
            column_types: compiled.column_types,
            params: vec![Value::Null; compiled.param_count as usize],
            param_names: compiled.param_names,
            param_count: compiled.param_count,
            row_values: Vec::new(),
            stepped: false,
            done: false,
            read_only: compiled.read_only,
            explain: match compiled.stmt_type {
                StmtType::Explain => 1,
                StmtType::ExplainQueryPlan => 2,
                _ => 0,
            },
            expanded_sql: None,
            ops: compiled.ops,
            stmt_type: Some(compiled.stmt_type),
            vdbe: None,
            pragma: None,
            pragma_state: None,
            analyze_target: None,
            attach_stmt: None,
            detach_name: None,
            conn_ptr: None,
            expired: false,
            schema_generation: 0,
        }
    }

    /// Create from compiled statement with schema generation
    pub fn from_compiled_with_generation(
        sql: &str,
        compiled: CompiledStmt,
        tail: &str,
        schema_generation: u64,
    ) -> Self {
        let mut stmt = Self::from_compiled(sql, compiled, tail);
        stmt.schema_generation = schema_generation;
        stmt
    }

    /// Mark this statement as expired
    pub fn expire(&mut self) {
        self.expired = true;
    }

    /// Check if statement is expired
    pub fn is_expired(&self) -> bool {
        self.expired
    }

    /// Get schema generation this statement was prepared with
    pub fn schema_generation(&self) -> u64 {
        self.schema_generation
    }

    /// Set schema generation
    pub fn set_schema_generation(&mut self, gen: u64) {
        self.schema_generation = gen;
    }

    /// Get the compiled bytecode
    pub fn ops(&self) -> &[VdbeOp] {
        &self.ops
    }

    /// Get statement type
    pub fn stmt_type(&self) -> Option<StmtType> {
        self.stmt_type
    }

    /// Get SQL text
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Get tail SQL
    pub fn tail(&self) -> &str {
        &self.tail
    }

    /// Set the tail SQL
    pub fn set_tail(&mut self, tail: &str) {
        self.tail = tail.to_string();
    }

    /// Set column information
    pub fn set_columns(&mut self, names: Vec<String>, types: Vec<ColumnType>) {
        self.column_names = names;
        self.column_types = types;
    }

    /// Set parameter count
    pub fn set_param_count(&mut self, count: i32) {
        self.param_count = count;
        self.params = vec![Value::Null; count as usize];
        self.param_names = vec![None; count as usize];
    }

    /// Set parameter name
    pub fn set_param_name(&mut self, idx: i32, name: &str) {
        if idx >= 1 && idx <= self.param_count {
            self.param_names[(idx - 1) as usize] = Some(name.to_string());
        }
    }

    /// Check if read-only
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Set read-only status
    pub fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    /// Get explain mode
    pub fn explain_mode(&self) -> i32 {
        self.explain
    }

    /// Set explain mode
    pub fn set_explain(&mut self, mode: i32) {
        self.explain = mode;
    }

    /// Set row values
    pub fn set_row(&mut self, values: Vec<Value>) {
        self.row_values = values;
        self.stepped = true;
    }

    /// Mark as done
    pub fn set_done(&mut self) {
        self.done = true;
    }

    /// Reset for re-execution
    pub fn reset(&mut self) {
        self.stepped = false;
        self.done = false;
        self.row_values.clear();

        // Reset VDBE if present
        if let Some(vdbe) = &mut self.vdbe {
            vdbe.reset();
        }
    }

    /// Clear all bindings
    pub fn clear_bindings(&mut self) {
        for param in &mut self.params {
            *param = Value::Null;
        }
        self.expanded_sql = None;
    }
}

#[derive(Debug, Clone)]
struct PragmaState {
    rows: Vec<Vec<Value>>,
    idx: usize,
}

// ============================================================================
// Prepare Functions
// ============================================================================

/// sqlite3_prepare - Prepare a statement (deprecated)
pub fn sqlite3_prepare<'a>(
    conn: &mut SqliteConnection,
    sql: &'a str,
) -> Result<(Box<PreparedStmt>, &'a str)> {
    sqlite3_prepare_v2(conn, sql)
}

/// sqlite3_prepare_v2 - Prepare a statement
///
/// Compiles SQL into a prepared statement. Returns the statement and
/// any remaining SQL text (tail).
pub fn sqlite3_prepare_v2<'a>(
    conn: &mut SqliteConnection,
    sql: &'a str,
) -> Result<(Box<PreparedStmt>, &'a str)> {
    conn.clear_error();

    // Skip leading whitespace
    let trimmed = sql.trim_start();
    if trimmed.is_empty() {
        // Empty statement - return stub
        return Ok((Box::new(PreparedStmt::new("")), ""));
    }

    let parsed_stmt = crate::executor::prepare::parse_sql(sql).ok();
    let parsed_pragma = parsed_stmt.as_ref().and_then(|stmt| match stmt {
        crate::parser::ast::Stmt::Pragma(pragma) => Some(pragma.clone()),
        _ => None,
    });
    let parsed_analyze = parsed_stmt.as_ref().and_then(|stmt| match stmt {
        crate::parser::ast::Stmt::Analyze(target) => Some(target.clone()),
        _ => None,
    });
    let parsed_attach = parsed_stmt.as_ref().and_then(|stmt| match stmt {
        crate::parser::ast::Stmt::Attach(attach) => Some(attach.clone()),
        _ => None,
    });
    let parsed_detach = parsed_stmt.as_ref().and_then(|stmt| match stmt {
        crate::parser::ast::Stmt::Detach(name) => Some(name.clone()),
        _ => None,
    });

    // Compile the SQL to VDBE bytecode with schema access for name resolution
    // Pass column naming PRAGMA settings for result column name formatting
    let short_column_names = conn.db_config.short_column_names;
    let full_column_names = conn.db_config.full_column_names;
    let compile_result = if let Some(ref schema_arc) = conn.main_db().schema {
        if let Ok(schema) = schema_arc.read() {
            compile_sql_with_config(sql, &schema, short_column_names, full_column_names)
        } else {
            compile_sql(sql)
        }
    } else {
        compile_sql(sql)
    };

    match compile_result {
        Ok((compiled, tail)) => {
            let mut stmt = PreparedStmt::from_compiled(sql, compiled, tail);
            // Always set connection pointer so VDBE has access to btree and schema
            stmt.conn_ptr = Some(conn as *mut SqliteConnection);
            // Capture schema generation for statement invalidation
            stmt.set_schema_generation(conn.get_schema_generation());
            if let Some(pragma) = parsed_pragma {
                if let Some((names, types)) = pragma_columns(&pragma) {
                    stmt.set_columns(names, types);
                } else {
                    stmt.set_columns(vec![pragma.name.clone()], vec![ColumnType::Text]);
                }
                stmt.pragma = Some(pragma);
            }
            if let Some(analyze_target) = parsed_analyze {
                stmt.analyze_target = analyze_target;
            }
            if let Some(attach_stmt) = parsed_attach {
                stmt.attach_stmt = Some(attach_stmt);
            }
            if let Some(detach_name) = parsed_detach {
                stmt.detach_name = Some(detach_name);
            }
            let stmt = Box::new(stmt);
            // Calculate actual tail position in original string
            let tail_start = if tail.is_empty() {
                sql.len()
            } else {
                sql.len() - tail.len()
            };
            Ok((stmt, &sql[tail_start..]))
        }
        Err(e) => {
            conn.set_error(e.code, e.message.as_deref().unwrap_or("error"));
            Err(e)
        }
    }
}

/// sqlite3_prepare_v3 - Prepare with flags
pub fn sqlite3_prepare_v3<'a>(
    conn: &mut SqliteConnection,
    sql: &'a str,
    _flags: u32,
) -> Result<(Box<PreparedStmt>, &'a str)> {
    // Flags can include SQLITE_PREPARE_PERSISTENT, SQLITE_PREPARE_NORMALIZE, etc.
    // For now, ignore flags and use v2
    sqlite3_prepare_v2(conn, sql)
}

/// sqlite3_prepare16 - Prepare with UTF-16 SQL
pub fn sqlite3_prepare16(
    conn: &mut SqliteConnection,
    sql: &[u16],
) -> Result<(Box<PreparedStmt>, Vec<u16>)> {
    let sql_str = String::from_utf16_lossy(sql);
    let (stmt, tail) = sqlite3_prepare_v2(conn, &sql_str)?;
    let tail_utf16: Vec<u16> = tail.encode_utf16().collect();
    Ok((stmt, tail_utf16))
}

// ============================================================================
// Step and Execute
// ============================================================================

/// sqlite3_step - Execute one step
///
/// Returns Row if a row is available, Done if finished, or an error.
pub fn sqlite3_step(stmt: &mut PreparedStmt) -> Result<StepResult> {
    use crate::vdbe::engine::ExecResult;
    use std::sync::atomic::Ordering;

    if stmt.done {
        return Ok(StepResult::Done);
    }

    // Check if statement is expired due to schema change
    if stmt.is_expired() {
        return Err(Error::with_message(
            ErrorCode::Schema,
            "statement expired: schema has changed",
        ));
    }

    // Check if schema generation has changed since statement was prepared
    if let Some(conn_ptr) = stmt.conn_ptr {
        let conn = unsafe { &*conn_ptr };
        let current_gen = conn.get_schema_generation();
        if stmt.schema_generation != current_gen {
            // Mark as expired and return schema error
            stmt.expire();
            return Err(Error::with_message(
                ErrorCode::Schema,
                "statement expired: schema has changed",
            ));
        }
    }

    if stmt.stmt_type == Some(StmtType::Pragma) && stmt.pragma.is_some() {
        return pragma_step(stmt);
    }
    if stmt.stmt_type == Some(StmtType::Analyze) {
        return analyze_step(stmt);
    }
    if stmt.stmt_type == Some(StmtType::Attach) {
        return attach_step(stmt);
    }
    if stmt.stmt_type == Some(StmtType::Detach) {
        return detach_step(stmt);
    }

    // Create VDBE if not already created
    if stmt.vdbe.is_none() {
        if stmt.ops.is_empty() {
            // No bytecode - this is an empty or stub statement
            stmt.set_done();
            return Ok(StepResult::Done);
        }

        // Reset the sort count before executing a new query
        reset_sort_count();

        // Create VDBE from compiled bytecode
        let mut vdbe = Vdbe::from_ops(stmt.ops.clone());

        // Set up btree and schema from connection
        if let Some(conn_ptr) = stmt.conn_ptr {
            // SAFETY: conn_ptr is valid for the lifetime of the statement
            let conn = unsafe { &*conn_ptr };

            // Reset the changes counter only for write statements
            // (SELECT and other read-only statements should not reset it)
            if !stmt.read_only {
                conn.changes.store(0, Ordering::SeqCst);
            }

            if let Some(main_db) = conn.find_db("main") {
                if let Some(ref btree) = main_db.btree {
                    vdbe.set_btree(btree.clone());

                    // In autocommit mode, start a write transaction for write statements
                    // This is necessary because individual statements don't include
                    // Transaction opcodes in their bytecode
                    if !stmt.read_only && conn.get_autocommit() {
                        let _ = btree.begin_trans(true);
                    }
                }
                if let Some(ref schema) = main_db.schema {
                    vdbe.set_schema(schema.clone());
                }
            }
            vdbe.set_connection(conn_ptr);
        }

        // Set up parameters
        vdbe.ensure_vars(stmt.param_count);
        for (i, param) in stmt.params.iter().enumerate() {
            let _ = vdbe.bind_value((i + 1) as i32, param);
        }

        // Set column names
        vdbe.set_column_names(stmt.column_names.clone());

        stmt.vdbe = Some(vdbe);
    }

    // Execute one step
    let vdbe = stmt.vdbe.as_mut().unwrap();
    match vdbe.step() {
        Ok(ExecResult::Row) => {
            // Copy result row to statement
            let col_count = vdbe.column_count();
            let mut row = Vec::with_capacity(col_count as usize);
            for i in 0..col_count {
                row.push(vdbe.column_value(i));
            }
            stmt.set_row(row);
            stmt.stepped = true;
            Ok(StepResult::Row)
        }
        Ok(ExecResult::Done) => {
            // In autocommit mode, commit the implicit write transaction
            if !stmt.read_only {
                if let Some(conn_ptr) = stmt.conn_ptr {
                    let conn = unsafe { &*conn_ptr };
                    if conn.get_autocommit() {
                        if let Some(main_db) = conn.find_db("main") {
                            if let Some(ref btree) = main_db.btree {
                                let _ = btree.commit();
                            }
                        }
                    }
                }
            }
            stmt.set_done();
            Ok(StepResult::Done)
        }
        Ok(ExecResult::Continue) => {
            // Internal state - keep stepping
            sqlite3_step(stmt)
        }
        Err(e) => {
            // Rollback on error - both in autocommit mode and explicit transactions
            // This matches SQLite behavior where errors within a transaction cause
            // automatic rollback to maintain database consistency
            if let Some(conn_ptr) = stmt.conn_ptr {
                let conn = unsafe { &*conn_ptr };
                if let Some(main_db) = conn.find_db("main") {
                    if let Some(ref btree) = main_db.btree {
                        let _ = btree.rollback(0, false);
                    }
                }
                // If we were in an explicit transaction, reset to autocommit mode
                // so subsequent BEGIN statements will work
                if !conn.get_autocommit() {
                    conn.autocommit
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    // Call rollback hook if configured
                    if let Some(hook) = conn.rollback_hook.as_ref() {
                        hook();
                    }
                }
            }
            stmt.set_done();
            Err(e)
        }
    }
}

fn pragma_step(stmt: &mut PreparedStmt) -> Result<StepResult> {
    let conn_ptr = stmt
        .conn_ptr
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "missing connection".to_string()))?;
    let pragma = stmt
        .pragma
        .clone()
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "missing pragma".to_string()))?;

    if stmt.pragma_state.is_none() {
        let conn = unsafe { &mut *conn_ptr };
        let result = execute_pragma(conn, &pragma)?;
        stmt.pragma_state = Some(PragmaState {
            rows: result.rows,
            idx: 0,
        });
        if !result.columns.is_empty() {
            stmt.column_names = result.columns;
            stmt.column_types = result.types;
        } else if !result.types.is_empty() {
            stmt.column_names = vec![pragma.name.clone()];
            stmt.column_types = result.types;
        }
    }

    let state = stmt
        .pragma_state
        .as_mut()
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "pragma state missing".to_string()))?;

    if state.idx >= state.rows.len() {
        stmt.set_done();
        return Ok(StepResult::Done);
    }

    let row = state.rows[state.idx].clone();
    state.idx += 1;
    stmt.set_row(row);
    Ok(StepResult::Row)
}

fn analyze_step(stmt: &mut PreparedStmt) -> Result<StepResult> {
    let conn_ptr = stmt
        .conn_ptr
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "missing connection".to_string()))?;
    let target = stmt.analyze_target.clone();
    let conn = unsafe { &mut *conn_ptr };
    execute_analyze(conn, target)?;
    stmt.set_done();
    Ok(StepResult::Done)
}

fn attach_step(stmt: &mut PreparedStmt) -> Result<StepResult> {
    let conn_ptr = stmt
        .conn_ptr
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "missing connection".to_string()))?;
    let attach = stmt
        .attach_stmt
        .clone()
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "missing attach".to_string()))?;
    let filename_val = eval_attach_expr(stmt, &attach.expr)?;
    let filename = filename_val.to_text();
    let conn = unsafe { &mut *conn_ptr };
    conn.attach_database(&filename, &attach.schema)?;
    stmt.set_done();
    Ok(StepResult::Done)
}

fn detach_step(stmt: &mut PreparedStmt) -> Result<StepResult> {
    let conn_ptr = stmt
        .conn_ptr
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "missing connection".to_string()))?;
    let detach_name = stmt
        .detach_name
        .clone()
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "missing detach".to_string()))?;
    let conn = unsafe { &mut *conn_ptr };
    conn.detach_database(&detach_name)?;
    stmt.set_done();
    Ok(StepResult::Done)
}

fn eval_attach_expr(stmt: &PreparedStmt, expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Literal(literal) => literal_to_value(literal),
        Expr::Variable(var) => resolve_attach_param(stmt, var),
        _ => Err(Error::with_message(
            ErrorCode::Error,
            "unsupported ATTACH expression",
        )),
    }
}

fn literal_to_value(literal: &Literal) -> Result<Value> {
    Ok(match literal {
        Literal::Null => Value::Null,
        Literal::Integer(i) => Value::Integer(*i),
        Literal::Float(f) => Value::Real(*f),
        Literal::String(s) => Value::Text(s.clone()),
        Literal::Blob(b) => Value::Blob(b.clone()),
        Literal::Bool(b) => Value::Integer(i64::from(*b)),
        Literal::CurrentTime => Value::Text("12:00:00".to_string()),
        Literal::CurrentDate => Value::Text("2024-01-01".to_string()),
        Literal::CurrentTimestamp => Value::Text("2024-01-01 12:00:00".to_string()),
    })
}

fn resolve_attach_param(stmt: &PreparedStmt, var: &Variable) -> Result<Value> {
    let idx = match var {
        Variable::Numbered(Some(num)) => (*num - 1) as usize,
        Variable::Numbered(None) => stmt
            .param_names
            .iter()
            .position(|name| name.is_none())
            .ok_or_else(|| Error::with_message(ErrorCode::Range, "parameter index out of range"))?,
        Variable::Named { prefix, name } => {
            let full_name = format!("{}{}", prefix, name);
            stmt.param_names
                .iter()
                .position(|param| param.as_deref() == Some(full_name.as_str()))
                .ok_or_else(|| {
                    Error::with_message(ErrorCode::Range, "parameter index out of range")
                })?
        }
    };
    stmt.params
        .get(idx)
        .cloned()
        .ok_or_else(|| Error::with_message(ErrorCode::Range, "parameter index out of range"))
}

/// sqlite3_reset - Reset statement for re-execution
pub fn sqlite3_reset(stmt: &mut PreparedStmt) -> Result<()> {
    stmt.reset();
    Ok(())
}

/// sqlite3_finalize - Destroy a prepared statement
pub fn sqlite3_finalize(_stmt: Box<PreparedStmt>) -> Result<()> {
    // Statement is dropped when Box goes out of scope
    Ok(())
}

// ============================================================================
// Binding Functions
// ============================================================================

/// sqlite3_bind_null - Bind NULL to parameter
pub fn sqlite3_bind_null(stmt: &mut PreparedStmt, idx: i32) -> Result<()> {
    if idx < 1 || idx > stmt.param_count {
        return Err(Error::new(ErrorCode::Range));
    }
    stmt.params[(idx - 1) as usize] = Value::Null;
    stmt.expanded_sql = None;
    Ok(())
}

/// sqlite3_bind_int - Bind i32 to parameter
pub fn sqlite3_bind_int(stmt: &mut PreparedStmt, idx: i32, value: i32) -> Result<()> {
    sqlite3_bind_int64(stmt, idx, value as i64)
}

/// sqlite3_bind_int64 - Bind i64 to parameter
pub fn sqlite3_bind_int64(stmt: &mut PreparedStmt, idx: i32, value: i64) -> Result<()> {
    if idx < 1 || idx > stmt.param_count {
        return Err(Error::new(ErrorCode::Range));
    }
    stmt.params[(idx - 1) as usize] = Value::Integer(value);
    stmt.expanded_sql = None;
    Ok(())
}

/// sqlite3_bind_double - Bind f64 to parameter
pub fn sqlite3_bind_double(stmt: &mut PreparedStmt, idx: i32, value: f64) -> Result<()> {
    if idx < 1 || idx > stmt.param_count {
        return Err(Error::new(ErrorCode::Range));
    }
    stmt.params[(idx - 1) as usize] = Value::Real(value);
    stmt.expanded_sql = None;
    Ok(())
}

/// sqlite3_bind_text - Bind text to parameter
pub fn sqlite3_bind_text(stmt: &mut PreparedStmt, idx: i32, value: &str) -> Result<()> {
    if idx < 1 || idx > stmt.param_count {
        return Err(Error::new(ErrorCode::Range));
    }
    stmt.params[(idx - 1) as usize] = Value::Text(value.to_string());
    stmt.expanded_sql = None;
    Ok(())
}

/// sqlite3_bind_text16 - Bind UTF-16 text to parameter
pub fn sqlite3_bind_text16(stmt: &mut PreparedStmt, idx: i32, value: &[u16]) -> Result<()> {
    let text = String::from_utf16_lossy(value);
    sqlite3_bind_text(stmt, idx, &text)
}

/// sqlite3_bind_blob - Bind blob to parameter
pub fn sqlite3_bind_blob(stmt: &mut PreparedStmt, idx: i32, value: &[u8]) -> Result<()> {
    if idx < 1 || idx > stmt.param_count {
        return Err(Error::new(ErrorCode::Range));
    }
    stmt.params[(idx - 1) as usize] = Value::Blob(value.to_vec());
    stmt.expanded_sql = None;
    Ok(())
}

/// sqlite3_bind_blob64 - Bind large blob to parameter
pub fn sqlite3_bind_blob64(stmt: &mut PreparedStmt, idx: i32, value: &[u8]) -> Result<()> {
    sqlite3_bind_blob(stmt, idx, value)
}

/// sqlite3_bind_zeroblob - Bind zero-filled blob
pub fn sqlite3_bind_zeroblob(stmt: &mut PreparedStmt, idx: i32, size: i32) -> Result<()> {
    if idx < 1 || idx > stmt.param_count {
        return Err(Error::new(ErrorCode::Range));
    }
    stmt.params[(idx - 1) as usize] = Value::Blob(vec![0u8; size as usize]);
    stmt.expanded_sql = None;
    Ok(())
}

/// sqlite3_bind_zeroblob64 - Bind large zero-filled blob
pub fn sqlite3_bind_zeroblob64(stmt: &mut PreparedStmt, idx: i32, size: u64) -> Result<()> {
    if idx < 1 || idx > stmt.param_count {
        return Err(Error::new(ErrorCode::Range));
    }
    stmt.params[(idx - 1) as usize] = Value::Blob(vec![0u8; size as usize]);
    stmt.expanded_sql = None;
    Ok(())
}

/// sqlite3_bind_value - Bind Value to parameter
pub fn sqlite3_bind_value(stmt: &mut PreparedStmt, idx: i32, value: &Value) -> Result<()> {
    if idx < 1 || idx > stmt.param_count {
        return Err(Error::new(ErrorCode::Range));
    }
    stmt.params[(idx - 1) as usize] = value.clone();
    stmt.expanded_sql = None;
    Ok(())
}

/// sqlite3_bind_parameter_count - Get parameter count
pub fn sqlite3_bind_parameter_count(stmt: &PreparedStmt) -> i32 {
    stmt.param_count
}

/// sqlite3_bind_parameter_name - Get parameter name
pub fn sqlite3_bind_parameter_name(stmt: &PreparedStmt, idx: i32) -> Option<&str> {
    if idx < 1 || idx > stmt.param_count {
        return None;
    }
    stmt.param_names[(idx - 1) as usize].as_deref()
}

/// sqlite3_bind_parameter_index - Get parameter index by name
pub fn sqlite3_bind_parameter_index(stmt: &PreparedStmt, name: &str) -> i32 {
    for (i, param_name) in stmt.param_names.iter().enumerate() {
        if let Some(n) = param_name {
            if n == name {
                return (i + 1) as i32;
            }
        }
    }
    0 // Not found
}

/// sqlite3_clear_bindings - Clear all parameter bindings
pub fn sqlite3_clear_bindings(stmt: &mut PreparedStmt) -> Result<()> {
    stmt.clear_bindings();
    Ok(())
}

// ============================================================================
// Column Functions
// ============================================================================

/// sqlite3_column_count - Get number of result columns
pub fn sqlite3_column_count(stmt: &PreparedStmt) -> i32 {
    // If we have explicit column names, use that count
    if !stmt.column_names.is_empty() {
        return stmt.column_names.len() as i32;
    }
    // Otherwise check if VDBE has a result (e.g., from count_changes)
    if let Some(ref vdbe) = stmt.vdbe {
        return vdbe.column_count();
    }
    0
}

/// sqlite3_column_name - Get column name
pub fn sqlite3_column_name(stmt: &PreparedStmt, idx: i32) -> Option<&str> {
    stmt.column_names.get(idx as usize).map(|s| s.as_str())
}

/// sqlite3_column_name16 - Get column name as UTF-16
pub fn sqlite3_column_name16(stmt: &PreparedStmt, idx: i32) -> Option<Vec<u16>> {
    stmt.column_names
        .get(idx as usize)
        .map(|s| s.encode_utf16().collect())
}

/// sqlite3_column_type - Get column type for current row
pub fn sqlite3_column_type(stmt: &PreparedStmt, idx: i32) -> ColumnType {
    stmt.row_values
        .get(idx as usize)
        .map(|v| v.column_type())
        .unwrap_or(ColumnType::Null)
}

/// sqlite3_column_decltype - Get declared type
pub fn sqlite3_column_decltype(stmt: &PreparedStmt, idx: i32) -> Option<&str> {
    // Return the declared type string if available
    // For now, map ColumnType to string
    stmt.column_types.get(idx as usize).map(|t| match t {
        ColumnType::Integer => "INTEGER",
        ColumnType::Float => "REAL",
        ColumnType::Text => "TEXT",
        ColumnType::Blob => "BLOB",
        ColumnType::Null => "",
    })
}

/// sqlite3_column_int - Get column as i32
pub fn sqlite3_column_int(stmt: &PreparedStmt, idx: i32) -> i32 {
    sqlite3_column_int64(stmt, idx) as i32
}

/// sqlite3_column_int64 - Get column as i64
pub fn sqlite3_column_int64(stmt: &PreparedStmt, idx: i32) -> i64 {
    stmt.row_values
        .get(idx as usize)
        .map(|v| v.to_i64())
        .unwrap_or(0)
}

/// sqlite3_column_double - Get column as f64
pub fn sqlite3_column_double(stmt: &PreparedStmt, idx: i32) -> f64 {
    stmt.row_values
        .get(idx as usize)
        .map(|v| v.to_f64())
        .unwrap_or(0.0)
}

/// sqlite3_column_text - Get column as text
pub fn sqlite3_column_text(stmt: &PreparedStmt, idx: i32) -> String {
    stmt.row_values
        .get(idx as usize)
        .map(|v| v.to_text())
        .unwrap_or_default()
}

/// sqlite3_column_text16 - Get column as UTF-16 text
pub fn sqlite3_column_text16(stmt: &PreparedStmt, idx: i32) -> Vec<u16> {
    sqlite3_column_text(stmt, idx).encode_utf16().collect()
}

/// sqlite3_column_blob - Get column as blob
pub fn sqlite3_column_blob(stmt: &PreparedStmt, idx: i32) -> Vec<u8> {
    stmt.row_values
        .get(idx as usize)
        .map(|v| v.to_blob())
        .unwrap_or_default()
}

/// sqlite3_column_bytes - Get column byte length
pub fn sqlite3_column_bytes(stmt: &PreparedStmt, idx: i32) -> i32 {
    stmt.row_values
        .get(idx as usize)
        .map(|v| v.bytes() as i32)
        .unwrap_or(0)
}

/// sqlite3_column_bytes16 - Get column byte length (UTF-16)
pub fn sqlite3_column_bytes16(stmt: &PreparedStmt, idx: i32) -> i32 {
    stmt.row_values
        .get(idx as usize)
        .map(|v| v.to_text().encode_utf16().count() as i32 * 2)
        .unwrap_or(0)
}

/// sqlite3_column_value - Get column as Value
pub fn sqlite3_column_value(stmt: &PreparedStmt, idx: i32) -> Value {
    stmt.row_values
        .get(idx as usize)
        .cloned()
        .unwrap_or(Value::Null)
}

// ============================================================================
// Statement Info
// ============================================================================

/// sqlite3_sql - Get SQL text
pub fn sqlite3_sql(stmt: &PreparedStmt) -> &str {
    stmt.sql()
}

/// sqlite3_expanded_sql - Get SQL with bound parameters
///
/// Returns the SQL text with bound parameters replaced by their actual values.
/// This is useful for logging and debugging.
pub fn sqlite3_expanded_sql(stmt: &PreparedStmt) -> Option<String> {
    if stmt.params.is_empty() {
        return Some(stmt.sql.clone());
    }

    let mut result = String::with_capacity(stmt.sql.len() * 2);
    let mut param_idx = 0usize;
    let bytes = stmt.sql.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let c = bytes[i];

        // Handle string literals - don't replace ? inside strings
        if c == b'\'' || c == b'"' {
            let quote = c;
            result.push(c as char);
            i += 1;
            while i < bytes.len() {
                let cc = bytes[i];
                result.push(cc as char);
                i += 1;
                if cc == quote {
                    // Check for escaped quote
                    if i < bytes.len() && bytes[i] == quote {
                        result.push(quote as char);
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }

        // Handle positional parameter ?
        if c == b'?' {
            i += 1;
            // Check for numbered parameter ?N
            let mut num = 0i32;
            let mut has_num = false;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                has_num = true;
                num = num * 10 + (bytes[i] - b'0') as i32;
                i += 1;
            }

            let idx = if has_num {
                (num - 1) as usize
            } else {
                let idx = param_idx;
                param_idx += 1;
                idx
            };

            if let Some(value) = stmt.params.get(idx) {
                result.push_str(&value_to_sql_literal(value));
            } else {
                result.push('?');
                if has_num {
                    result.push_str(&num.to_string());
                }
            }
            continue;
        }

        // Handle named parameters :name, $name, @name
        if c == b':' || c == b'$' || c == b'@' {
            let start = i;
            i += 1;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let name = &stmt.sql[start..i];

            // Look up by name
            let mut found = false;
            for (idx, param_name) in stmt.param_names.iter().enumerate() {
                if let Some(n) = param_name {
                    if n == name {
                        if let Some(value) = stmt.params.get(idx) {
                            result.push_str(&value_to_sql_literal(value));
                            found = true;
                        }
                        break;
                    }
                }
            }
            if !found {
                result.push_str(name);
            }
            continue;
        }

        result.push(c as char);
        i += 1;
    }

    Some(result)
}

/// Convert a Value to SQL literal representation
fn value_to_sql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Real(r) => {
            // Ensure proper float representation
            if r.fract() == 0.0 {
                format!("{}.0", r)
            } else {
                r.to_string()
            }
        }
        Value::Text(s) => {
            // Escape single quotes by doubling them
            let escaped = s.replace('\'', "''");
            format!("'{}'", escaped)
        }
        Value::Blob(b) => {
            // Convert to X'...' hex literal
            let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
            format!("X'{}'", hex)
        }
    }
}

/// sqlite3_normalized_sql - Get normalized SQL
pub fn sqlite3_normalized_sql(stmt: &PreparedStmt) -> Option<String> {
    // TODO: Implement SQL normalization
    Some(stmt.sql.clone())
}

/// sqlite3_stmt_readonly - Check if statement is read-only
pub fn sqlite3_stmt_readonly(stmt: &PreparedStmt) -> bool {
    stmt.is_read_only()
}

/// sqlite3_stmt_isexplain - Check if EXPLAIN statement
pub fn sqlite3_stmt_isexplain(stmt: &PreparedStmt) -> i32 {
    stmt.explain_mode()
}

/// sqlite3_stmt_busy - Check if statement is busy
pub fn sqlite3_stmt_busy(stmt: &PreparedStmt) -> bool {
    stmt.stepped && !stmt.done
}

/// sqlite3_data_count - Get number of columns with data
pub fn sqlite3_data_count(stmt: &PreparedStmt) -> i32 {
    if stmt.stepped && !stmt.done {
        stmt.row_values.len() as i32
    } else {
        0
    }
}

// ============================================================================
// Statement Status
// ============================================================================

/// Statement status counter operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum StmtStatusOp {
    /// Number of full table scans
    FullscanStep = 1,
    /// Number of sort operations
    Sort = 2,
    /// Number of auto-index creations
    AutoIndex = 3,
    /// Number of VM steps executed
    VmStep = 4,
    /// Number of statement reprepares
    Reprepare = 5,
    /// Number of times statement was run
    Run = 6,
    /// Memory used by statement
    MemUsed = 99,
}

impl TryFrom<i32> for StmtStatusOp {
    type Error = ();

    fn try_from(value: i32) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(StmtStatusOp::FullscanStep),
            2 => Ok(StmtStatusOp::Sort),
            3 => Ok(StmtStatusOp::AutoIndex),
            4 => Ok(StmtStatusOp::VmStep),
            5 => Ok(StmtStatusOp::Reprepare),
            6 => Ok(StmtStatusOp::Run),
            99 => Ok(StmtStatusOp::MemUsed),
            _ => Err(()),
        }
    }
}

/// sqlite3_stmt_status - Get statement status counter
///
/// Returns the value of a status counter for the statement.
/// If reset is true, the counter is reset to zero after reading.
pub fn sqlite3_stmt_status(stmt: &mut PreparedStmt, op: i32, reset: bool) -> i32 {
    let op = match StmtStatusOp::try_from(op) {
        Ok(op) => op,
        Err(_) => return 0,
    };

    // Get value from VDBE if present
    let value = if let Some(vdbe) = &stmt.vdbe {
        match op {
            StmtStatusOp::FullscanStep => 0,       // Would track full scans
            StmtStatusOp::Sort => 0,               // Would track sorts
            StmtStatusOp::AutoIndex => 0,          // Would track auto-index
            StmtStatusOp::VmStep => vdbe.get_pc(), // Approximate VM steps
            StmtStatusOp::Reprepare => 0,          // Would track reprepares
            StmtStatusOp::Run => {
                if stmt.stepped {
                    1
                } else {
                    0
                }
            }
            StmtStatusOp::MemUsed => 0, // Would track memory
        }
    } else {
        0
    };

    // Reset not yet implemented (would need mutable status in Vdbe)
    let _ = reset;

    value
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Find the end of the first statement (semicolon handling)
fn find_statement_end(sql: &str) -> &str {
    // Simple semicolon finder (doesn't handle strings/comments properly)
    // A full implementation would use the tokenizer
    let bytes = sql.as_bytes();
    let mut in_string = false;
    let mut string_char = b'\0';

    for (i, &c) in bytes.iter().enumerate() {
        if in_string {
            if c == string_char {
                in_string = false;
            }
        } else {
            match c {
                b'\'' | b'"' => {
                    in_string = true;
                    string_char = c;
                }
                b';' => {
                    // Found statement end
                    return &sql[i + 1..];
                }
                _ => {}
            }
        }
    }

    // No semicolon found
    ""
}

/// Count parameter placeholders in SQL
fn count_parameters(sql: &str) -> i32 {
    // Simple ? counter (doesn't handle strings/comments properly)
    let bytes = sql.as_bytes();
    let mut count = 0;
    let mut in_string = false;
    let mut string_char = b'\0';

    for &c in bytes {
        if in_string {
            if c == string_char {
                in_string = false;
            }
        } else {
            match c {
                b'\'' | b'"' => {
                    in_string = true;
                    string_char = c;
                }
                b'?' => count += 1,
                _ => {}
            }
        }
    }

    count
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepare_simple() {
        let mut conn = SqliteConnection::new();
        let (stmt, tail) = sqlite3_prepare_v2(&mut conn, "SELECT 1").unwrap();
        assert!(tail.is_empty());
        assert!(stmt.is_read_only());
    }

    #[test]
    fn test_prepare_with_tail() {
        let mut conn = SqliteConnection::new();
        let (_, tail) = sqlite3_prepare_v2(&mut conn, "SELECT 1; SELECT 2").unwrap();
        assert_eq!(tail.trim(), "SELECT 2");
    }

    #[test]
    fn test_attach_detach_statement() {
        let mut conn = SqliteConnection::new();
        let (mut attach_stmt, _) =
            sqlite3_prepare_v2(&mut conn, "ATTACH ':memory:' AS aux").unwrap();
        assert!(matches!(
            sqlite3_step(&mut attach_stmt).unwrap(),
            StepResult::Done
        ));
        assert!(conn.find_db("aux").is_some());

        let (mut detach_stmt, _) = sqlite3_prepare_v2(&mut conn, "DETACH aux").unwrap();
        assert!(matches!(
            sqlite3_step(&mut detach_stmt).unwrap(),
            StepResult::Done
        ));
        assert!(conn.find_db("aux").is_none());
    }

    #[test]
    fn test_bind_parameters() {
        let mut stmt = PreparedStmt::new("SELECT ?");
        stmt.set_param_count(1);

        sqlite3_bind_int(&mut stmt, 1, 42).unwrap();
        assert_eq!(stmt.params[0], Value::Integer(42));

        sqlite3_bind_text(&mut stmt, 1, "hello").unwrap();
        assert_eq!(stmt.params[0], Value::Text("hello".to_string()));

        sqlite3_bind_null(&mut stmt, 1).unwrap();
        assert_eq!(stmt.params[0], Value::Null);
    }

    #[test]
    fn test_bind_range_error() {
        let mut stmt = PreparedStmt::new("SELECT ?");
        stmt.set_param_count(1);

        assert!(sqlite3_bind_int(&mut stmt, 0, 42).is_err());
        assert!(sqlite3_bind_int(&mut stmt, 2, 42).is_err());
    }

    #[test]
    fn test_parameter_count() {
        let mut stmt = PreparedStmt::new("SELECT ?, ?, ?");
        stmt.set_param_count(3);
        assert_eq!(sqlite3_bind_parameter_count(&stmt), 3);
    }

    #[test]
    fn test_column_access() {
        let mut stmt = PreparedStmt::new("SELECT 1, 'hello'");
        stmt.set_columns(
            vec!["a".to_string(), "b".to_string()],
            vec![ColumnType::Integer, ColumnType::Text],
        );
        stmt.set_row(vec![Value::Integer(1), Value::Text("hello".to_string())]);

        assert_eq!(sqlite3_column_count(&stmt), 2);
        assert_eq!(sqlite3_column_name(&stmt, 0), Some("a"));
        assert_eq!(sqlite3_column_int(&stmt, 0), 1);
        assert_eq!(sqlite3_column_text(&stmt, 1), "hello");
    }

    #[test]
    fn test_reset_and_clear() {
        let mut stmt = PreparedStmt::new("SELECT ?");
        stmt.set_param_count(1);
        stmt.set_row(vec![Value::Integer(1)]);
        stmt.set_done();

        assert!(stmt.done);

        sqlite3_reset(&mut stmt).unwrap();
        assert!(!stmt.done);
        assert!(!stmt.stepped);

        sqlite3_bind_int(&mut stmt, 1, 42).unwrap();
        sqlite3_clear_bindings(&mut stmt).unwrap();
        assert_eq!(stmt.params[0], Value::Null);
    }

    #[test]
    fn test_find_statement_end() {
        assert_eq!(find_statement_end("SELECT 1; SELECT 2"), " SELECT 2");
        assert_eq!(find_statement_end("SELECT 1"), "");
        assert_eq!(find_statement_end("SELECT ';'"), ""); // In string
    }

    #[test]
    fn test_count_parameters() {
        assert_eq!(count_parameters("SELECT ?"), 1);
        assert_eq!(count_parameters("SELECT ?, ?"), 2);
        assert_eq!(count_parameters("SELECT '?'"), 0); // In string
    }

    #[test]
    fn test_expanded_sql_no_params() {
        let stmt = PreparedStmt::new("SELECT 1, 2, 3");
        let expanded = sqlite3_expanded_sql(&stmt).unwrap();
        assert_eq!(expanded, "SELECT 1, 2, 3");
    }

    #[test]
    fn test_expanded_sql_positional() {
        let mut stmt = PreparedStmt::new("SELECT ?, ?");
        stmt.set_param_count(2);
        sqlite3_bind_int(&mut stmt, 1, 42).unwrap();
        sqlite3_bind_text(&mut stmt, 2, "hello").unwrap();

        let expanded = sqlite3_expanded_sql(&stmt).unwrap();
        assert_eq!(expanded, "SELECT 42, 'hello'");
    }

    #[test]
    fn test_expanded_sql_numbered() {
        let mut stmt = PreparedStmt::new("SELECT ?2, ?1");
        stmt.set_param_count(2);
        sqlite3_bind_int(&mut stmt, 1, 10).unwrap();
        sqlite3_bind_int(&mut stmt, 2, 20).unwrap();

        let expanded = sqlite3_expanded_sql(&stmt).unwrap();
        assert_eq!(expanded, "SELECT 20, 10");
    }

    #[test]
    fn test_expanded_sql_null() {
        let mut stmt = PreparedStmt::new("SELECT ?");
        stmt.set_param_count(1);
        sqlite3_bind_null(&mut stmt, 1).unwrap();

        let expanded = sqlite3_expanded_sql(&stmt).unwrap();
        assert_eq!(expanded, "SELECT NULL");
    }

    #[test]
    fn test_expanded_sql_blob() {
        let mut stmt = PreparedStmt::new("SELECT ?");
        stmt.set_param_count(1);
        sqlite3_bind_blob(&mut stmt, 1, &[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();

        let expanded = sqlite3_expanded_sql(&stmt).unwrap();
        assert_eq!(expanded, "SELECT X'DEADBEEF'");
    }

    #[test]
    fn test_expanded_sql_escaping() {
        let mut stmt = PreparedStmt::new("SELECT ?");
        stmt.set_param_count(1);
        sqlite3_bind_text(&mut stmt, 1, "it's a test").unwrap();

        let expanded = sqlite3_expanded_sql(&stmt).unwrap();
        assert_eq!(expanded, "SELECT 'it''s a test'");
    }

    #[test]
    fn test_expanded_sql_in_string() {
        let mut stmt = PreparedStmt::new("SELECT '?', ?");
        stmt.set_param_count(1);
        sqlite3_bind_int(&mut stmt, 1, 42).unwrap();

        let expanded = sqlite3_expanded_sql(&stmt).unwrap();
        assert_eq!(expanded, "SELECT '?', 42");
    }

    #[test]
    fn test_stmt_status_run() {
        let mut stmt = PreparedStmt::new("SELECT 1");
        // Before stepping, run count is 0 (no VDBE = 0)
        let run_count = sqlite3_stmt_status(&mut stmt, StmtStatusOp::Run as i32, false);
        assert_eq!(run_count, 0);

        // Unknown status op returns 0
        let unknown = sqlite3_stmt_status(&mut stmt, 999, false);
        assert_eq!(unknown, 0);
    }

    #[test]
    fn test_stmt_status_enum() {
        assert_eq!(StmtStatusOp::try_from(1), Ok(StmtStatusOp::FullscanStep));
        assert_eq!(StmtStatusOp::try_from(2), Ok(StmtStatusOp::Sort));
        assert_eq!(StmtStatusOp::try_from(3), Ok(StmtStatusOp::AutoIndex));
        assert_eq!(StmtStatusOp::try_from(4), Ok(StmtStatusOp::VmStep));
        assert_eq!(StmtStatusOp::try_from(5), Ok(StmtStatusOp::Reprepare));
        assert_eq!(StmtStatusOp::try_from(6), Ok(StmtStatusOp::Run));
        assert_eq!(StmtStatusOp::try_from(99), Ok(StmtStatusOp::MemUsed));
        assert!(StmtStatusOp::try_from(100).is_err());
    }

    #[test]
    fn test_value_to_sql_literal() {
        assert_eq!(value_to_sql_literal(&Value::Null), "NULL");
        assert_eq!(value_to_sql_literal(&Value::Integer(42)), "42");
        assert_eq!(value_to_sql_literal(&Value::Real(3.14)), "3.14");
        assert_eq!(value_to_sql_literal(&Value::Real(5.0)), "5.0");
        assert_eq!(
            value_to_sql_literal(&Value::Text("test".to_string())),
            "'test'"
        );
        assert_eq!(
            value_to_sql_literal(&Value::Blob(vec![1, 2, 3])),
            "X'010203'"
        );
    }

    #[test]
    fn test_statement_expiration_manual() {
        let mut stmt = PreparedStmt::new("SELECT 1");
        assert!(!stmt.is_expired());

        stmt.expire();
        assert!(stmt.is_expired());
    }

    #[test]
    fn test_schema_generation_captured_on_prepare() {
        let mut conn = SqliteConnection::new();
        assert_eq!(conn.get_schema_generation(), 0);

        let (stmt, _) = sqlite3_prepare_v2(&mut conn, "SELECT 1").unwrap();
        assert_eq!(stmt.schema_generation(), 0);

        // Increment generation
        conn.increment_schema_generation();
        assert_eq!(conn.get_schema_generation(), 1);

        // New statement should have new generation
        let (stmt2, _) = sqlite3_prepare_v2(&mut conn, "SELECT 2").unwrap();
        assert_eq!(stmt2.schema_generation(), 1);
    }

    #[test]
    fn test_expired_statement_returns_schema_error() {
        let mut conn = SqliteConnection::new();
        let (mut stmt, _) = sqlite3_prepare_v2(&mut conn, "SELECT 1").unwrap();

        // Manually expire
        stmt.expire();

        let result = sqlite3_step(&mut stmt);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::Schema);
    }

    #[test]
    fn test_schema_generation_mismatch_expires_statement() {
        let mut conn = SqliteConnection::new();
        let (mut stmt, _) = sqlite3_prepare_v2(&mut conn, "SELECT 1").unwrap();
        assert_eq!(stmt.schema_generation(), 0);
        assert!(!stmt.is_expired());

        // Simulate schema change by incrementing generation
        conn.increment_schema_generation();

        // Statement should now fail with schema error
        let result = sqlite3_step(&mut stmt);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::Schema);

        // Statement should now be marked as expired
        assert!(stmt.is_expired());
    }
}
