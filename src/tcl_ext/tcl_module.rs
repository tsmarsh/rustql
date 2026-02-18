//! TCL virtual table module for testing
//!
//! The tcl module is a test virtual table whose methods (xCreate, xConnect,
//! xBestIndex, xFilter, xNext, etc.) are implemented as TCL scripts.
//!
//! Usage:
//!   register_tcl_module db
//!   CREATE VIRTUAL TABLE t1 USING tcl(tcl_script)
//!
//! The tcl_script argument is evaluated as a TCL command for each vtab
//! callback. The first argument to the script is the method name.

use std::ffi::CString;
use std::sync::Arc;

use crate::error::{Error, ErrorCode, Result};
use crate::schema::Affinity;
use crate::types::Value;
use crate::vdbe::mem::Mem;
use crate::vtab::{ConstraintValue, DbContext, IndexInfo, VtabCursor, VtabModule, VtabTable};

use super::ffi::{Tcl_Eval, Tcl_GetObjResult, Tcl_GetStringFromObj, Tcl_Interp, TCL_OK};
use super::CURRENT_INTERP;

/// Evaluate a TCL script and return the result as a String.
/// Returns None if the interpreter is not set or evaluation fails.
fn eval_tcl(script: &str) -> Option<String> {
    CURRENT_INTERP.with(|interp_cell| {
        let interp_opt = interp_cell.borrow();
        let interp = (*interp_opt)?;
        if interp.is_null() {
            return None;
        }
        eval_tcl_with_interp(interp, script)
    })
}

/// Evaluate a TCL script with a given interpreter.
fn eval_tcl_with_interp(interp: *mut Tcl_Interp, script: &str) -> Option<String> {
    unsafe {
        let cmd = CString::new(script).ok()?;
        let result = Tcl_Eval(interp, cmd.as_ptr());
        if result == TCL_OK {
            let result_obj = Tcl_GetObjResult(interp);
            if !result_obj.is_null() {
                let mut len: std::os::raw::c_int = 0;
                let ptr = Tcl_GetStringFromObj(result_obj, &mut len);
                if !ptr.is_null() {
                    let slice = std::slice::from_raw_parts(ptr as *const u8, len as usize);
                    return Some(String::from_utf8_lossy(slice).to_string());
                }
            }
        }
        None
    }
}

/// TCL virtual table module
pub struct TclModule;

impl VtabModule for TclModule {
    fn name(&self) -> &str {
        "tcl"
    }

    fn create(
        &self,
        _db_name: &str,
        _table_name: &str,
        args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        self.create_or_connect(args)
    }

    fn create_with_ctx(
        &self,
        _db_name: &str,
        _table_name: &str,
        args: &[String],
        _ctx: &dyn DbContext,
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        self.create_or_connect(args)
    }

    fn connect(
        &self,
        _db_name: &str,
        _table_name: &str,
        args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        self.create_or_connect(args)
    }

    fn connect_with_ctx(
        &self,
        _db_name: &str,
        _table_name: &str,
        args: &[String],
        _ctx: &dyn DbContext,
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        self.create_or_connect(args)
    }

    fn destroy(&self, _table: Arc<dyn VtabTable>) -> Result<()> {
        Ok(())
    }
}

impl TclModule {
    /// Common implementation for create and connect.
    ///
    /// The first argument is expected to be a TCL script. We call it with
    /// "xConnect" to get the schema declaration.
    fn create_or_connect(&self, args: &[String]) -> Result<(String, Arc<dyn VtabTable>)> {
        // The first arg is the TCL script that handles callbacks
        let tcl_script = args.first().cloned().unwrap_or_default();

        if tcl_script.is_empty() {
            return Err(Error::with_message(
                ErrorCode::Error,
                "tcl module requires a script argument",
            ));
        }

        // Call the script with "xConnect" to get the schema
        // In SQLite's test suite: the script returns a list where the first
        // element is the declare_vtab SQL and subsequent elements are ignored
        let connect_result = eval_tcl(&format!("{} xConnect", tcl_script));
        let schema_sql = connect_result.unwrap_or_else(|| "(x)".to_string());

        // Parse the result - it should be a CREATE TABLE statement or column defs
        // If the TCL script returns "CREATE TABLE ... (...)", extract the column part
        let schema = if schema_sql.to_uppercase().starts_with("CREATE ") {
            // Extract the part from first ( to last )
            if let Some(start) = schema_sql.find('(') {
                if let Some(end) = schema_sql.rfind(')') {
                    schema_sql[start..=end].to_string()
                } else {
                    "(x)".to_string()
                }
            } else {
                "(x)".to_string()
            }
        } else {
            // Assume it's just column definitions, wrap in parens if needed
            if schema_sql.starts_with('(') {
                schema_sql
            } else {
                format!("({})", schema_sql)
            }
        };

        // Parse column names from schema
        let columns = parse_schema_columns(&schema);

        let table = TclTable {
            script: tcl_script,
            columns,
        };

        Ok((schema, Arc::new(table)))
    }
}

/// Parse column names from a schema string like "(a TEXT, b INTEGER, c)"
fn parse_schema_columns(schema: &str) -> Vec<String> {
    let mut columns = Vec::new();
    let trimmed = schema.trim();

    // Remove outer parens
    let inner = if trimmed.starts_with('(') && trimmed.ends_with(')') {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };

    // Split by comma, handle nested parens
    let mut depth = 0;
    let mut current = String::new();
    for c in inner.chars() {
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
                let col_name = extract_col_name(current.trim());
                if !col_name.is_empty() {
                    columns.push(col_name);
                }
                current.clear();
            }
            _ => current.push(c),
        }
    }
    let col_name = extract_col_name(current.trim());
    if !col_name.is_empty() {
        columns.push(col_name);
    }

    columns
}

/// Extract column name from a column definition like "a TEXT" -> "a"
fn extract_col_name(def: &str) -> String {
    let upper = def.to_uppercase();
    // Skip table constraints
    if upper.starts_with("PRIMARY KEY")
        || upper.starts_with("UNIQUE")
        || upper.starts_with("CHECK")
        || upper.starts_with("FOREIGN KEY")
        || upper.starts_with("CONSTRAINT")
    {
        return String::new();
    }
    // First word is the column name
    def.split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches(|c| c == '"' || c == '\'' || c == '[' || c == ']')
        .to_string()
}

/// TCL virtual table instance
struct TclTable {
    /// The TCL script that handles callbacks
    script: String,
    /// Column names parsed from schema
    columns: Vec<String>,
}

// TclTable is not truly Send+Sync because it relies on the TCL interpreter
// which is single-threaded, but the vtab trait requires Send+Sync.
// This is safe because TCL extension is always single-threaded.
unsafe impl Send for TclTable {}
unsafe impl Sync for TclTable {}

impl VtabTable for TclTable {
    fn table_name(&self) -> &str {
        "tcl"
    }

    fn db_name(&self) -> &str {
        "main"
    }

    fn column_count(&self) -> usize {
        self.columns.len()
    }

    fn column_name(&self, col_idx: usize) -> &str {
        self.columns.get(col_idx).map(|s| s.as_str()).unwrap_or("")
    }

    fn column_affinity(&self, _col_idx: usize) -> Affinity {
        Affinity::Blob
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // Call the TCL script with xBestIndex
        // For a basic implementation, just do a full table scan
        let result = eval_tcl(&format!("{} xBestIndex", self.script));
        if let Some(cost_str) = result {
            if let Ok(cost) = cost_str.parse::<f64>() {
                info.estimated_cost = cost;
            }
        }
        info.idx_num = 0;
        if info.estimated_cost == 0.0 {
            info.estimated_cost = 1_000_000.0;
        }
        info.estimated_rows = 1_000_000;
        Ok(())
    }

    fn open_cursor(&self) -> Result<Box<dyn VtabCursor>> {
        Ok(Box::new(TclCursor {
            script: self.script.clone(),
            columns: self.columns.clone(),
            rows: Vec::new(),
            position: 0,
        }))
    }

    fn disconnect(&self) -> Result<()> {
        let _ = eval_tcl(&format!("{} xDisconnect", self.script));
        Ok(())
    }
}

/// TCL cursor for iterating over the virtual table
struct TclCursor {
    script: String,
    columns: Vec<String>,
    rows: Vec<(i64, Vec<Value>)>,
    position: usize,
}

unsafe impl Send for TclCursor {}
unsafe impl Sync for TclCursor {}

impl VtabCursor for TclCursor {
    fn filter(
        &mut self,
        _index_num: i32,
        _index_str: Option<&str>,
        _constraints: &[ConstraintValue],
    ) -> Result<()> {
        self.rows.clear();
        self.position = 0;

        // Call the TCL script with xFilter
        // The script should return a list of rows, where each row is a list of values
        // Format: {rowid col0 col1 ...} {rowid col0 col1 ...} ...
        let result = eval_tcl(&format!("{} xFilter", self.script));

        if let Some(data) = result {
            // Parse the result as TCL list of rows
            // Each row line is separated by newlines or is a TCL list element
            self.parse_filter_result(&data);
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

impl TclCursor {
    /// Parse the result of xFilter into rows.
    ///
    /// The TCL script returns data in various formats. The most common
    /// format from the test suite is a list where each element is a row,
    /// and each row is a sub-list of values.
    fn parse_filter_result(&mut self, data: &str) {
        let trimmed = data.trim();
        if trimmed.is_empty() {
            return;
        }

        // Simple parsing: split by newlines, each line is a row
        // Each row has: rowid value1 value2 ...
        let mut rowid_counter = 1i64;

        for line in trimmed.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Parse the line as space-separated values
            // First value is rowid if it's a number, otherwise auto-assign
            let parts: Vec<&str> = split_tcl_list(line);
            if parts.is_empty() {
                continue;
            }

            let (rowid, values_start) = if let Ok(rid) = parts[0].parse::<i64>() {
                (rid, 1)
            } else {
                let rid = rowid_counter;
                rowid_counter += 1;
                (rid, 0)
            };

            let values: Vec<Value> = parts[values_start..]
                .iter()
                .map(|s| {
                    if let Ok(i) = s.parse::<i64>() {
                        Value::Integer(i)
                    } else if let Ok(f) = s.parse::<f64>() {
                        Value::Real(f)
                    } else {
                        Value::Text(s.to_string())
                    }
                })
                .collect();

            self.rows.push((rowid, values));
            rowid_counter = rowid + 1;
        }
    }
}

/// Simple TCL list splitting (handles braces and quotes)
fn split_tcl_list(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let s = s.trim();
    if s.is_empty() {
        return result;
    }

    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Skip whitespace
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }

        if bytes[i] == b'{' {
            // Find matching close brace
            let start = i + 1;
            let mut depth = 1;
            i += 1;
            while i < bytes.len() && depth > 0 {
                if bytes[i] == b'{' {
                    depth += 1;
                } else if bytes[i] == b'}' {
                    depth -= 1;
                }
                if depth > 0 {
                    i += 1;
                }
            }
            result.push(&s[start..i]);
            i += 1; // skip closing brace
        } else {
            // Unquoted word
            let start = i;
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            result.push(&s[start..i]);
        }
    }

    result
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_schema_columns() {
        let cols = parse_schema_columns("(a TEXT, b INTEGER, c)");
        assert_eq!(cols, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_schema_columns_with_constraints() {
        let cols = parse_schema_columns("(id INTEGER PRIMARY KEY, name TEXT, UNIQUE(name))");
        assert_eq!(cols, vec!["id", "name"]);
    }

    #[test]
    fn test_split_tcl_list() {
        let result = split_tcl_list("hello world");
        assert_eq!(result, vec!["hello", "world"]);

        let result = split_tcl_list("{hello world} foo");
        assert_eq!(result, vec!["hello world", "foo"]);
    }

    #[test]
    fn test_extract_col_name() {
        assert_eq!(extract_col_name("a TEXT"), "a");
        assert_eq!(extract_col_name("b"), "b");
        assert_eq!(extract_col_name("PRIMARY KEY(id)"), "");
    }
}
