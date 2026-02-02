//! Echo virtual table module for testing
//!
//! The echo module is a test virtual table that mirrors a real table.
//! It is used extensively in SQLite's TCL test suite to test virtual
//! table functionality.
//!
//! Usage:
//!   CREATE VIRTUAL TABLE echo_t1 USING echo(t1);
//!
//! This creates a virtual table echo_t1 that mirrors real table t1.

use std::ffi::CString;
use std::sync::Arc;

use crate::api::{
    sqlite3_bind_double, sqlite3_bind_int64, sqlite3_bind_null, sqlite3_bind_text,
    sqlite3_prepare_v2, sqlite3_step, PreparedStmt,
};
use crate::error::{Error, ErrorCode, Result};
use crate::schema::Affinity;
use crate::types::Value;
use crate::vdbe::mem::Mem;
use crate::vtab::{
    ConstraintValue, DbContext, IndexInfo, VtabCursor, VtabModule, VtabTable,
    SQLITE_INDEX_CONSTRAINT_EQ, SQLITE_INDEX_CONSTRAINT_GE, SQLITE_INDEX_CONSTRAINT_GLOB,
    SQLITE_INDEX_CONSTRAINT_GT, SQLITE_INDEX_CONSTRAINT_LE, SQLITE_INDEX_CONSTRAINT_LIKE,
    SQLITE_INDEX_CONSTRAINT_LT, SQLITE_INDEX_CONSTRAINT_MATCH,
};

use super::ffi::{Tcl_SetVar, TCL_APPEND_VALUE, TCL_GLOBAL_ONLY, TCL_LIST_ELEMENT};
use super::{CONNECTIONS, CURRENT_INTERP};

/// Append a string to the ::echo_module TCL variable
///
/// This mirrors SQLite's appendToEchoModule function in test8.c.
/// It appends to the global echo_module variable as a list element.
fn append_to_echo_module(arg: &str) {
    CURRENT_INTERP.with(|interp_cell| {
        let interp_opt = interp_cell.borrow();
        if let Some(interp) = *interp_opt {
            let flags = TCL_APPEND_VALUE | TCL_LIST_ELEMENT | TCL_GLOBAL_ONLY;
            if let Ok(var_name) = CString::new("echo_module") {
                if let Ok(value) = CString::new(arg) {
                    unsafe {
                        Tcl_SetVar(interp, var_name.as_ptr(), value.as_ptr(), flags);
                    }
                }
            }
        }
    });
}

/// Echo virtual table module
pub struct EchoModule;

impl VtabModule for EchoModule {
    fn name(&self) -> &str {
        "echo"
    }

    fn create(
        &self,
        _db_name: &str,
        _table_name: &str,
        _args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        // This method is not used when create_with_ctx is available
        // Return an error indicating that DbContext is required
        Err(Error::with_message(
            ErrorCode::Error,
            "echo module requires database context (create_with_ctx)",
        ))
    }

    fn create_with_ctx(
        &self,
        db_name: &str,
        table_name: &str,
        args: &[String],
        ctx: &dyn DbContext,
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        // Log the xCreate callback
        append_to_echo_module("xCreate");
        append_to_echo_module("echo");
        append_to_echo_module(db_name);
        append_to_echo_module(table_name);
        for arg in args {
            append_to_echo_module(arg);
        }

        // First argument should be the name of the real table to mirror
        // If no args, return empty schema to trigger "did not declare schema" error
        let real_table = match args.first() {
            Some(name) => name,
            None => {
                // Return empty schema - VCreate will return "did not declare schema" error
                let table = EchoTable {
                    name: table_name.to_string(),
                    db_name: db_name.to_string(),
                    real_table: String::new(),
                    columns: Vec::new(),
                };
                return Ok((String::new(), Arc::new(table)));
            }
        };

        // If a second argument is provided, it's a log table name
        // If that table already exists, return an error (matches SQLite's echo behavior)
        if let Some(log_table) = args.get(1) {
            let check_sql = format!(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='{}'",
                log_table
            );
            if let Ok(rows) = ctx.query(&check_sql) {
                if !rows.is_empty() {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        format!("table '{}' already exists", log_table),
                    ));
                }
            }
        }

        // Query the schema for the real table using DbContext
        let sql = format!(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='{}'",
            real_table
        );

        let rows = ctx.query(&sql)?;

        if let Some(row) = rows.first() {
            if let Some(Value::Text(create_sql)) = row.first() {
                // Parse column info from the CREATE statement
                let columns = parse_columns_from_sql(create_sql);

                // The schema for sqlite_master is the CREATE statement itself
                // but we need just the column definitions part
                let schema = extract_column_defs(create_sql);

                let table = EchoTable {
                    name: table_name.to_string(),
                    db_name: db_name.to_string(),
                    real_table: real_table.to_string(),
                    columns,
                };

                return Ok((schema, Arc::new(table)));
            }
        }

        Err(Error::with_message(
            ErrorCode::Error,
            format!("table {} does not exist", real_table),
        ))
    }

    fn connect(
        &self,
        db_name: &str,
        table_name: &str,
        args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        // For echo, connect is the same as create
        // But connect doesn't have DbContext, so we need CONNECTIONS fallback
        self.create(db_name, table_name, args)
    }

    fn connect_with_ctx(
        &self,
        db_name: &str,
        table_name: &str,
        args: &[String],
        ctx: &dyn DbContext,
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        // Log the xConnect callback
        append_to_echo_module("xConnect");
        append_to_echo_module("echo");
        append_to_echo_module(db_name);
        append_to_echo_module(table_name);
        for arg in args {
            append_to_echo_module(arg);
        }

        // For echo, connect creates the same table structure but doesn't initialize storage
        // Parse the real table schema just like create
        let real_table = match args.first() {
            Some(name) => name,
            None => {
                let table = EchoTable {
                    name: table_name.to_string(),
                    db_name: db_name.to_string(),
                    real_table: String::new(),
                    columns: Vec::new(),
                };
                return Ok((String::new(), Arc::new(table)));
            }
        };

        let sql = format!(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='{}'",
            real_table
        );

        let rows = ctx.query(&sql)?;

        if let Some(row) = rows.first() {
            if let Some(Value::Text(create_sql)) = row.first() {
                let columns = parse_columns_from_sql(create_sql);
                let schema = extract_column_defs(create_sql);

                let table = EchoTable {
                    name: table_name.to_string(),
                    db_name: db_name.to_string(),
                    real_table: real_table.to_string(),
                    columns,
                };

                return Ok((schema, Arc::new(table)));
            }
        }

        Err(Error::with_message(
            ErrorCode::Error,
            format!("table {} does not exist", real_table),
        ))
    }

    fn destroy(&self, _table: Arc<dyn VtabTable>) -> Result<()> {
        // Log the xDestroy callback
        append_to_echo_module("xDestroy");
        Ok(())
    }
}

/// Column information for echo table
#[derive(Debug, Clone)]
struct EchoColumn {
    name: String,
    affinity: Affinity,
    is_rowid_alias: bool,
}

/// Echo virtual table instance
struct EchoTable {
    name: String,
    db_name: String,
    real_table: String,
    columns: Vec<EchoColumn>,
}

impl VtabTable for EchoTable {
    fn table_name(&self) -> &str {
        &self.name
    }

    fn db_name(&self) -> &str {
        &self.db_name
    }

    fn column_count(&self) -> usize {
        self.columns.len()
    }

    fn column_name(&self, col_idx: usize) -> &str {
        self.columns
            .get(col_idx)
            .map(|c| c.name.as_str())
            .unwrap_or("")
    }

    fn column_affinity(&self, col_idx: usize) -> Affinity {
        self.columns
            .get(col_idx)
            .map(|c| c.affinity)
            .unwrap_or(Affinity::Blob)
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // Build a SQL query based on constraints
        // The echo module can use indices on the underlying table

        let mut sql = format!("SELECT rowid, * FROM {}", self.real_table);
        let mut where_parts = Vec::new();
        let mut arg_idx = 1;

        // First pass: collect usable constraints (avoid borrow issues)
        let mut usable_constraints: Vec<(usize, String, bool)> = Vec::new();

        for (idx, constraint) in info.constraints.iter().enumerate() {
            if !constraint.usable {
                continue;
            }

            let col_name = if constraint.col_idx < 0 {
                "rowid".to_string()
            } else if let Some(col) = self.columns.get(constraint.col_idx as usize) {
                col.name.clone()
            } else {
                continue;
            };

            let op = match constraint.op {
                SQLITE_INDEX_CONSTRAINT_EQ => Some("="),
                SQLITE_INDEX_CONSTRAINT_GT => Some(">"),
                SQLITE_INDEX_CONSTRAINT_LT => Some("<"),
                SQLITE_INDEX_CONSTRAINT_GE => Some(">="),
                SQLITE_INDEX_CONSTRAINT_LE => Some("<="),
                SQLITE_INDEX_CONSTRAINT_LIKE => Some("LIKE"),
                SQLITE_INDEX_CONSTRAINT_GLOB => Some("GLOB"),
                SQLITE_INDEX_CONSTRAINT_MATCH => Some("LIKE"), // Convert MATCH to LIKE
                _ => None,
            };

            if let Some(op_str) = op {
                let is_match = constraint.op == SQLITE_INDEX_CONSTRAINT_MATCH;
                if is_match {
                    // For MATCH, wrap with %
                    where_parts.push(format!("{} LIKE '%' || ? || '%'", col_name));
                } else {
                    where_parts.push(format!("{} {} ?", col_name, op_str));
                }
                usable_constraints.push((idx, col_name, is_match));
            }
        }

        // Second pass: mark constraints as used
        for (idx, _, _) in &usable_constraints {
            info.use_constraint(*idx, arg_idx, true);
            arg_idx += 1;
        }

        if !where_parts.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_parts.join(" AND "));
        }

        // Handle ORDER BY if it's on an indexed column
        if info.order_by.len() == 1 {
            let order = &info.order_by[0];
            let col_name = if order.col_idx < 0 {
                "rowid"
            } else if let Some(col) = self.columns.get(order.col_idx as usize) {
                &col.name
            } else {
                ""
            };

            if !col_name.is_empty() {
                sql.push_str(&format!(
                    " ORDER BY {} {}",
                    col_name,
                    if order.desc { "DESC" } else { "ASC" }
                ));
                info.order_by_consumed = true;
            }
        }

        // Store the SQL in idx_str
        info.idx_str = Some(sql.clone());
        info.idx_num = hash_string(&sql);

        // Estimate cost
        if where_parts.is_empty() {
            info.estimated_cost = 1_000_000.0;
            info.estimated_rows = 1_000_000;
        } else {
            info.estimated_cost = 10.0;
            info.estimated_rows = 10;
        }

        Ok(())
    }

    fn open_cursor(&self) -> Result<Box<dyn VtabCursor>> {
        Ok(Box::new(EchoCursor {
            real_table: self.real_table.clone(),
            columns: self.columns.clone(),
            rows: Vec::new(),
            position: 0,
        }))
    }

    fn disconnect(&self) -> Result<()> {
        // Log the xDisconnect callback
        append_to_echo_module("xDisconnect");
        Ok(())
    }

    fn update(
        &self,
        rowid: Option<i64>,
        new_rowid: Option<i64>,
        columns: &[Option<Mem>],
    ) -> Result<i64> {
        CONNECTIONS.with(|connections| -> Result<i64> {
            let mut connections = connections.borrow_mut();
            let conn = connections.values_mut().next().ok_or_else(|| {
                Error::with_message(ErrorCode::Error, "no database connection available")
            })?;

            if rowid.is_none() && !columns.is_empty() {
                // INSERT
                let col_names: Vec<&str> = self.columns.iter().map(|c| c.name.as_str()).collect();
                let placeholders: Vec<String> =
                    (1..=columns.len()).map(|i| format!("?{}", i)).collect();

                let sql = if let Some(rid) = new_rowid {
                    format!(
                        "INSERT INTO {} (rowid, {}) VALUES ({}, {})",
                        self.real_table,
                        col_names.join(", "),
                        rid,
                        placeholders.join(", ")
                    )
                } else {
                    format!(
                        "INSERT INTO {} ({}) VALUES ({})",
                        self.real_table,
                        col_names.join(", "),
                        placeholders.join(", ")
                    )
                };

                let (mut stmt, _) = sqlite3_prepare_v2(conn, &sql)?;
                bind_mem_values(&mut stmt, columns)?;
                sqlite3_step(&mut stmt)?;

                Ok(crate::api::sqlite3_last_insert_rowid(conn))
            } else if let Some(old_rowid) = rowid {
                if columns.is_empty() {
                    // DELETE
                    let sql = format!("DELETE FROM {} WHERE rowid = ?1", self.real_table);
                    let (mut stmt, _) = sqlite3_prepare_v2(conn, &sql)?;
                    sqlite3_bind_int64(&mut stmt, 1, old_rowid)?;
                    sqlite3_step(&mut stmt)?;
                    Ok(old_rowid)
                } else {
                    // UPDATE
                    let mut set_parts = Vec::new();
                    for (i, col) in self.columns.iter().enumerate() {
                        if i < columns.len() && columns[i].is_some() {
                            set_parts.push(format!("{} = ?{}", col.name, i + 1));
                        }
                    }

                    let sql = if let Some(new_rid) = new_rowid {
                        if new_rid != old_rowid {
                            set_parts.insert(0, format!("rowid = {}", new_rid));
                        }
                        format!(
                            "UPDATE {} SET {} WHERE rowid = {}",
                            self.real_table,
                            set_parts.join(", "),
                            old_rowid
                        )
                    } else {
                        format!(
                            "UPDATE {} SET {} WHERE rowid = {}",
                            self.real_table,
                            set_parts.join(", "),
                            old_rowid
                        )
                    };

                    let (mut stmt, _) = sqlite3_prepare_v2(conn, &sql)?;
                    bind_mem_values(&mut stmt, columns)?;
                    sqlite3_step(&mut stmt)?;

                    Ok(new_rowid.unwrap_or(old_rowid))
                }
            } else {
                Err(Error::with_message(
                    ErrorCode::Error,
                    "invalid update parameters",
                ))
            }
        })
    }
}

/// Bind Mem values to a prepared statement
fn bind_mem_values(stmt: &mut PreparedStmt, values: &[Option<Mem>]) -> Result<()> {
    for (i, val) in values.iter().enumerate() {
        let idx = (i + 1) as i32;
        if let Some(mem) = val {
            bind_mem_to_stmt(stmt, idx, mem)?;
        } else {
            sqlite3_bind_null(stmt, idx)?;
        }
    }
    Ok(())
}

/// Bind a single Mem value to a prepared statement
fn bind_mem_to_stmt(stmt: &mut PreparedStmt, idx: i32, mem: &Mem) -> Result<()> {
    if mem.is_null() {
        sqlite3_bind_null(stmt, idx)
    } else if mem.is_int() {
        sqlite3_bind_int64(stmt, idx, mem.to_int())
    } else if mem.is_real() {
        sqlite3_bind_double(stmt, idx, mem.to_real())
    } else if mem.is_str() {
        sqlite3_bind_text(stmt, idx, &mem.to_str())
    } else if mem.is_blob() {
        crate::api::sqlite3_bind_blob(stmt, idx, &mem.to_blob())
    } else {
        sqlite3_bind_null(stmt, idx)
    }
}

/// Echo cursor for iterating over mirrored table
struct EchoCursor {
    real_table: String,
    columns: Vec<EchoColumn>,
    rows: Vec<(i64, Vec<Value>)>,
    position: usize,
}

// Echo cursor is not Send because it references CONNECTIONS thread-local
// But VtabCursor requires Send+Sync, so we use unsafe impl
// This is safe because TCL is single-threaded and the cursor only accesses
// CONNECTIONS from the same thread that created it
unsafe impl Send for EchoCursor {}
unsafe impl Sync for EchoCursor {}

impl VtabCursor for EchoCursor {
    fn filter(
        &mut self,
        _index_num: i32,
        _index_str: Option<&str>,
        _constraints: &[ConstraintValue],
    ) -> Result<()> {
        // This method is not used when filter_with_ctx is available
        // Return an error indicating that DbContext is required
        Err(Error::with_message(
            ErrorCode::Error,
            "echo cursor requires database context (filter_with_ctx)",
        ))
    }

    fn filter_with_ctx(
        &mut self,
        _index_num: i32,
        index_str: Option<&str>,
        _constraints: &[ConstraintValue],
        ctx: &dyn DbContext,
    ) -> Result<()> {
        self.rows.clear();
        self.position = 0;

        // Get the SQL from idx_str, or use default
        let sql = index_str
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("SELECT rowid, * FROM {}", self.real_table));

        // Execute query using DbContext
        let query_rows = ctx.query(&sql)?;

        for row in query_rows {
            // First column is rowid, rest are data columns
            let rowid = match row.first() {
                Some(Value::Integer(i)) => *i,
                _ => 0,
            };

            let row_data: Vec<Value> = row.into_iter().skip(1).collect();
            self.rows.push((rowid, row_data));
        }

        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.position += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.position >= self.rows.len()
    }

    fn rowid(&self) -> Result<i64> {
        if self.position < self.rows.len() {
            Ok(self.rows[self.position].0)
        } else {
            Ok(0)
        }
    }

    fn column(&self, col_idx: usize) -> Result<Mem> {
        if self.position < self.rows.len() {
            let row = &self.rows[self.position].1;
            if col_idx < row.len() {
                return Ok(value_to_mem(&row[col_idx]));
            }
        }
        Ok(Mem::new())
    }
}

/// Bind a ConstraintValue to a prepared statement
fn bind_constraint_value(stmt: &mut PreparedStmt, idx: i32, cv: &ConstraintValue) -> Result<()> {
    bind_mem_to_stmt(stmt, idx, &cv.value)
}

/// Convert Value to Mem
fn value_to_mem(value: &Value) -> Mem {
    match value {
        Value::Null => Mem::new(),
        Value::Integer(i) => Mem::from_int(*i),
        Value::Real(f) => Mem::from_real(*f),
        Value::Text(s) => Mem::from_str(s),
        Value::Blob(b) => Mem::from_blob(b),
    }
}

/// Simple hash function for SQL string (matches SQLite's echo module)
fn hash_string(s: &str) -> i32 {
    let mut val: u32 = 0;
    for b in s.bytes() {
        val = (val << 3).wrapping_add(b as u32);
    }
    (val & 0x7fffffff) as i32
}

/// Parse column definitions from CREATE TABLE SQL
fn parse_columns_from_sql(sql: &str) -> Vec<EchoColumn> {
    let mut columns = Vec::new();

    // Find the part between parentheses
    if let Some(start) = sql.find('(') {
        if let Some(end) = sql.rfind(')') {
            let cols_str = &sql[start + 1..end];

            // Split by comma, being careful about nested parentheses
            let mut depth = 0;
            let mut current = String::new();
            let mut col_defs = Vec::new();

            for c in cols_str.chars() {
                match c {
                    '(' => {
                        depth += 1;
                        current.push(c);
                    }
                    ')' => {
                        depth -= 1;
                        current.push(c);
                    }
                    ',' if depth == 0 => {
                        col_defs.push(current.trim().to_string());
                        current.clear();
                    }
                    _ => current.push(c),
                }
            }
            if !current.trim().is_empty() {
                col_defs.push(current.trim().to_string());
            }

            for col_def in col_defs {
                // Skip constraints (PRIMARY KEY, UNIQUE, etc.)
                let upper = col_def.to_uppercase();
                if upper.starts_with("PRIMARY KEY")
                    || upper.starts_with("UNIQUE")
                    || upper.starts_with("CHECK")
                    || upper.starts_with("FOREIGN KEY")
                    || upper.starts_with("CONSTRAINT")
                {
                    continue;
                }

                // Parse column name and type
                let parts: Vec<&str> = col_def.split_whitespace().collect();
                if let Some(name) = parts.first() {
                    let name = name.trim_matches(|c| c == '"' || c == '\'' || c == '[' || c == ']');

                    // Determine affinity from type
                    let type_str = parts.get(1).map(|s| s.to_uppercase()).unwrap_or_default();
                    let affinity = if type_str.contains("INT") {
                        Affinity::Integer
                    } else if type_str.contains("TEXT")
                        || type_str.contains("CHAR")
                        || type_str.contains("CLOB")
                    {
                        Affinity::Text
                    } else if type_str.contains("BLOB") || type_str.is_empty() {
                        Affinity::Blob
                    } else if type_str.contains("REAL")
                        || type_str.contains("FLOA")
                        || type_str.contains("DOUB")
                    {
                        Affinity::Real
                    } else {
                        Affinity::Numeric
                    };

                    // Check if this is a rowid alias (INTEGER PRIMARY KEY)
                    let is_rowid_alias = upper.contains("INTEGER")
                        && upper.contains("PRIMARY")
                        && upper.contains("KEY")
                        && !upper.contains("WITHOUT ROWID");

                    columns.push(EchoColumn {
                        name: name.to_string(),
                        affinity,
                        is_rowid_alias,
                    });
                }
            }
        }
    }

    columns
}

/// Extract column definitions for vtab schema declaration
fn extract_column_defs(sql: &str) -> String {
    if let Some(start) = sql.find('(') {
        if let Some(end) = sql.rfind(')') {
            return sql[start..=end].to_string();
        }
    }
    "(x)".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_columns() {
        let sql = "CREATE TABLE t1(a INTEGER, b TEXT, c REAL)";
        let cols = parse_columns_from_sql(sql);
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "a");
        assert_eq!(cols[1].name, "b");
        assert_eq!(cols[2].name, "c");
    }

    #[test]
    fn test_parse_columns_with_constraints() {
        let sql = "CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT, UNIQUE(name))";
        let cols = parse_columns_from_sql(sql);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert!(cols[0].is_rowid_alias);
        assert_eq!(cols[1].name, "name");
    }

    #[test]
    fn test_hash_string() {
        let h1 = hash_string("SELECT * FROM t1");
        let h2 = hash_string("SELECT * FROM t1");
        assert_eq!(h1, h2);

        let h3 = hash_string("SELECT * FROM t2");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_extract_column_defs() {
        let sql = "CREATE TABLE t1(a, b, c)";
        let defs = extract_column_defs(sql);
        assert_eq!(defs, "(a, b, c)");
    }
}
