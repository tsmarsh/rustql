//! sqlite3_declare_vtab implementation
//!
//! This module provides functionality for virtual table modules to declare
//! their schema, similar to SQLite's sqlite3_declare_vtab function.
//!
//! # Overview
//!
//! When a virtual table module is creating a table, it needs to tell SQLite
//! what columns the table has. In SQLite's C API, this is done by calling
//! `sqlite3_declare_vtab()` with a CREATE TABLE statement.
//!
//! # Example
//!
//! ```ignore
//! use crate::vtab::declare_vtab::{DeclaredSchema, DeclaredColumn};
//!
//! let schema = DeclaredSchema::parse("CREATE TABLE x(a TEXT, b INTEGER HIDDEN, c REAL)")?;
//! assert_eq!(schema.columns.len(), 3);
//! assert!(!schema.columns[0].hidden);
//! assert!(schema.columns[1].hidden);
//! ```

use crate::error::{Error, ErrorCode, Result};
use crate::schema::Affinity;

/// Derive affinity from a type name (SQLite rules from section 3.1 of docs)
fn affinity_from_type_name(type_name: &str) -> Affinity {
    let upper = type_name.to_uppercase();

    if upper.contains("INT") {
        Affinity::Integer
    } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        Affinity::Text
    } else if upper.contains("BLOB") || upper.is_empty() {
        Affinity::Blob
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        Affinity::Real
    } else {
        Affinity::Numeric
    }
}

/// A column declared via sqlite3_declare_vtab
#[derive(Debug, Clone)]
pub struct DeclaredColumn {
    /// Column name
    pub name: String,
    /// Column type (as declared, e.g., "TEXT", "INTEGER")
    pub type_name: Option<String>,
    /// Column affinity (derived from type_name)
    pub affinity: Affinity,
    /// Whether this is a hidden column
    ///
    /// Hidden columns are not included in `SELECT *` but can be
    /// explicitly referenced. They're useful for internal data
    /// like docid in FTS tables.
    pub hidden: bool,
    /// Column index (0-based)
    pub index: usize,
}

impl DeclaredColumn {
    /// Create a new column with defaults
    pub fn new(name: &str, index: usize) -> Self {
        Self {
            name: name.to_string(),
            type_name: None,
            affinity: Affinity::Blob,
            hidden: false,
            index,
        }
    }

    /// Set the type name and derive affinity
    pub fn with_type(mut self, type_name: &str) -> Self {
        self.affinity = affinity_from_type_name(type_name);
        self.type_name = Some(type_name.to_uppercase());
        self
    }

    /// Mark as hidden
    pub fn as_hidden(mut self) -> Self {
        self.hidden = true;
        self
    }
}

/// A schema declared via sqlite3_declare_vtab
#[derive(Debug, Clone, Default)]
pub struct DeclaredSchema {
    /// All columns (including hidden)
    pub columns: Vec<DeclaredColumn>,
    /// Whether the table is WITHOUT ROWID
    pub without_rowid: bool,
}

impl DeclaredSchema {
    /// Create an empty schema
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            without_rowid: false,
        }
    }

    /// Parse a CREATE TABLE statement to extract schema
    ///
    /// The statement should be in the form:
    /// `CREATE TABLE x(col1 TYPE, col2 TYPE HIDDEN, ...)`
    ///
    /// The table name is ignored - it's just for SQLite compatibility.
    pub fn parse(sql: &str) -> Result<Self> {
        let sql = sql.trim();

        // Must start with CREATE TABLE (case insensitive)
        let sql_upper = sql.to_uppercase();
        if !sql_upper.starts_with("CREATE TABLE") {
            return Err(Error::with_message(
                ErrorCode::Error,
                "declare_vtab: expected CREATE TABLE",
            ));
        }

        // Find the opening paren of the column list
        let paren_start = sql.find('(').ok_or_else(|| {
            Error::with_message(ErrorCode::Error, "declare_vtab: missing column list")
        })?;

        // Find the matching closing paren
        let paren_end = sql.rfind(')').ok_or_else(|| {
            Error::with_message(ErrorCode::Error, "declare_vtab: missing closing paren")
        })?;

        if paren_end <= paren_start {
            return Err(Error::with_message(
                ErrorCode::Error,
                "declare_vtab: invalid column list",
            ));
        }

        let columns_str = &sql[paren_start + 1..paren_end];

        // Check for WITHOUT ROWID after the closing paren
        let after_paren = sql[paren_end + 1..].trim().to_uppercase();
        let without_rowid = after_paren.contains("WITHOUT ROWID");

        // Parse columns
        let columns = Self::parse_columns(columns_str)?;

        Ok(Self {
            columns,
            without_rowid,
        })
    }

    /// Parse the column list portion
    fn parse_columns(columns_str: &str) -> Result<Vec<DeclaredColumn>> {
        let mut columns = Vec::new();
        let mut index = 0;

        // Split by commas, but be careful of nested parens
        let parts = Self::split_column_defs(columns_str);

        for part in parts {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            // Skip constraints (PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY)
            let part_upper = part.to_uppercase();
            if part_upper.starts_with("PRIMARY KEY")
                || part_upper.starts_with("UNIQUE")
                || part_upper.starts_with("CHECK")
                || part_upper.starts_with("FOREIGN KEY")
                || part_upper.starts_with("CONSTRAINT")
            {
                continue;
            }

            // Parse as column definition
            if let Some(col) = Self::parse_column_def(part, index)? {
                columns.push(col);
                index += 1;
            }
        }

        Ok(columns)
    }

    /// Split column definitions, respecting nested parentheses
    fn split_column_defs(s: &str) -> Vec<&str> {
        let mut parts = Vec::new();
        let mut depth: i32 = 0;
        let mut start = 0;

        for (i, c) in s.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                ',' if depth == 0 => {
                    parts.push(&s[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }

        // Don't forget the last part
        if start < s.len() {
            parts.push(&s[start..]);
        }

        parts
    }

    /// Parse a single column definition
    fn parse_column_def(def: &str, index: usize) -> Result<Option<DeclaredColumn>> {
        let tokens: Vec<&str> = def.split_whitespace().collect();

        if tokens.is_empty() {
            return Ok(None);
        }

        let name = tokens[0];
        let mut type_name: Option<&str> = None;
        let mut hidden = false;

        // Process remaining tokens
        let mut i = 1;
        while i < tokens.len() {
            let token = tokens[i].to_uppercase();

            if token == "HIDDEN" {
                hidden = true;
            } else if token == "PRIMARY" || token == "NOT" || token == "UNIQUE" || token == "CHECK"
            {
                // Skip constraint keywords and their arguments
                break;
            } else if token == "DEFAULT" || token == "COLLATE" || token == "REFERENCES" {
                // Skip these and their following argument
                i += 1;
            } else if type_name.is_none()
                && !matches!(
                    token.as_str(),
                    "NULL" | "CONSTRAINT" | "AUTOINCREMENT" | "GENERATED" | "AS"
                )
            {
                // This is likely a type name
                type_name = Some(tokens[i]);
            }

            i += 1;
        }

        let mut column = DeclaredColumn::new(name, index);
        if let Some(tn) = type_name {
            column = column.with_type(tn);
        }
        if hidden {
            column = column.as_hidden();
        }

        Ok(Some(column))
    }

    /// Get number of columns (including hidden)
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get number of visible columns (excluding hidden)
    pub fn visible_column_count(&self) -> usize {
        self.columns.iter().filter(|c| !c.hidden).count()
    }

    /// Get column by index
    pub fn column(&self, idx: usize) -> Option<&DeclaredColumn> {
        self.columns.get(idx)
    }

    /// Get column by name
    pub fn column_by_name(&self, name: &str) -> Option<&DeclaredColumn> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Iterate over visible columns
    pub fn visible_columns(&self) -> impl Iterator<Item = &DeclaredColumn> {
        self.columns.iter().filter(|c| !c.hidden)
    }

    /// Build a schema string for sqlite_master
    ///
    /// Returns just the column portion: "(col1 TYPE, col2 TYPE, ...)"
    pub fn to_schema_string(&self) -> String {
        let parts: Vec<String> = self
            .columns
            .iter()
            .map(|c| {
                let mut s = c.name.clone();
                if let Some(ref tn) = c.type_name {
                    s.push(' ');
                    s.push_str(tn);
                }
                if c.hidden {
                    s.push_str(" HIDDEN");
                }
                s
            })
            .collect();

        format!("({})", parts.join(", "))
    }
}

/// Helper function to declare a virtual table schema
///
/// This is the Rust equivalent of sqlite3_declare_vtab().
/// It parses a CREATE TABLE statement and returns the schema.
///
/// # Example
///
/// ```ignore
/// let schema = declare_vtab("CREATE TABLE x(a TEXT, b INTEGER HIDDEN)")?;
/// ```
pub fn declare_vtab(sql: &str) -> Result<DeclaredSchema> {
    DeclaredSchema::parse(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple() {
        let schema = DeclaredSchema::parse("CREATE TABLE x(a, b, c)").unwrap();
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].name, "a");
        assert_eq!(schema.columns[1].name, "b");
        assert_eq!(schema.columns[2].name, "c");
    }

    #[test]
    fn test_parse_with_types() {
        let schema = DeclaredSchema::parse("CREATE TABLE x(a TEXT, b INTEGER, c REAL)").unwrap();
        assert_eq!(schema.columns[0].type_name, Some("TEXT".to_string()));
        assert_eq!(schema.columns[0].affinity, Affinity::Text);
        assert_eq!(schema.columns[1].type_name, Some("INTEGER".to_string()));
        assert_eq!(schema.columns[1].affinity, Affinity::Integer);
        assert_eq!(schema.columns[2].type_name, Some("REAL".to_string()));
        assert_eq!(schema.columns[2].affinity, Affinity::Real);
    }

    #[test]
    fn test_parse_hidden() {
        let schema = DeclaredSchema::parse("CREATE TABLE x(a TEXT, b INTEGER HIDDEN, c)").unwrap();
        assert!(!schema.columns[0].hidden);
        assert!(schema.columns[1].hidden);
        assert!(!schema.columns[2].hidden);
        assert_eq!(schema.visible_column_count(), 2);
    }

    #[test]
    fn test_parse_without_rowid() {
        let schema =
            DeclaredSchema::parse("CREATE TABLE x(a TEXT PRIMARY KEY) WITHOUT ROWID").unwrap();
        assert!(schema.without_rowid);
    }

    #[test]
    fn test_parse_constraints() {
        let schema =
            DeclaredSchema::parse("CREATE TABLE x(a TEXT, b INTEGER, PRIMARY KEY(a), UNIQUE(b))")
                .unwrap();
        // Constraints should be skipped
        assert_eq!(schema.columns.len(), 2);
    }

    #[test]
    fn test_parse_complex_types() {
        let schema =
            DeclaredSchema::parse("CREATE TABLE x(a VARCHAR(255), b DECIMAL(10,2))").unwrap();
        assert_eq!(schema.columns.len(), 2);
    }

    #[test]
    fn test_to_schema_string() {
        let schema = DeclaredSchema::parse("CREATE TABLE x(a TEXT, b INTEGER HIDDEN)").unwrap();
        let s = schema.to_schema_string();
        assert!(s.contains("a TEXT"));
        assert!(s.contains("b INTEGER HIDDEN"));
    }

    #[test]
    fn test_column_by_name() {
        let schema = DeclaredSchema::parse("CREATE TABLE x(foo TEXT, bar INTEGER)").unwrap();
        assert_eq!(schema.column_by_name("FOO").unwrap().index, 0);
        assert_eq!(schema.column_by_name("bar").unwrap().index, 1);
        assert!(schema.column_by_name("baz").is_none());
    }

    #[test]
    fn test_error_invalid() {
        assert!(DeclaredSchema::parse("SELECT * FROM x").is_err());
        assert!(DeclaredSchema::parse("CREATE TABLE x").is_err());
    }
}
