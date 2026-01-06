//! Prepared statement handling
//!
//! This module implements sqlite3_stmt (prepared statement) and related functions.

use crate::error::{Error, ErrorCode, Result};
use crate::types::{ColumnType, StepResult, Value};

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
        }
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
    }

    /// Clear all bindings
    pub fn clear_bindings(&mut self) {
        for param in &mut self.params {
            *param = Value::Null;
        }
        self.expanded_sql = None;
    }
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

    // TODO: Actually parse and compile the SQL
    // For now, create a stub statement

    let mut stmt = Box::new(PreparedStmt::new(sql));

    // Find the first statement boundary (semicolon)
    let tail = find_statement_end(sql);
    stmt.set_tail(tail);

    // Count parameters (? placeholders)
    let param_count = count_parameters(sql);
    stmt.set_param_count(param_count);

    // Determine if read-only
    let trimmed = sql.trim().to_uppercase();
    stmt.set_read_only(
        trimmed.starts_with("SELECT")
            || trimmed.starts_with("EXPLAIN")
            || trimmed.starts_with("PRAGMA")
            || trimmed.starts_with("BEGIN")
            || trimmed.starts_with("COMMIT")
            || trimmed.starts_with("ROLLBACK"),
    );

    // Check for EXPLAIN
    if trimmed.starts_with("EXPLAIN QUERY PLAN") {
        stmt.set_explain(2);
    } else if trimmed.starts_with("EXPLAIN") {
        stmt.set_explain(1);
    }

    Ok((stmt, tail))
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
    if stmt.done {
        return Ok(StepResult::Done);
    }

    // TODO: Actually execute the VDBE program
    // For now, just return Done

    stmt.set_done();
    Ok(StepResult::Done)
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
    stmt.column_names.len() as i32
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
pub fn sqlite3_expanded_sql(stmt: &PreparedStmt) -> Option<String> {
    // TODO: Implement parameter expansion
    Some(stmt.sql.clone())
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
}
