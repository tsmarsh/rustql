//! Virtual table adapter for FTS3
//!
//! This module wraps the existing FTS3 implementation with the VtabModule,
//! VtabTable, and VtabCursor traits to integrate with the virtual table registry.

use std::sync::{Arc, Mutex};

use crate::error::{Error, ErrorCode, Result};
use crate::schema::Affinity;
use crate::vdbe::mem::Mem;
use crate::vtab::{
    ConstraintValue, IndexInfo, VtabCursor, VtabModule, VtabTable, SQLITE_INDEX_CONSTRAINT_MATCH,
};

use super::fts3::{Fts3Cursor, Fts3Table};
use super::registry::{get_table, register_table};
use super::tokenizer::create_tokenizer;

/// FTS3 virtual table module
///
/// This module creates FTS3 full-text search tables.
pub struct Fts3VtabModule;

/// FTS4 virtual table module - delegates to FTS3
pub struct Fts4VtabModule;

impl VtabModule for Fts3VtabModule {
    fn name(&self) -> &str {
        "fts3"
    }

    fn create(
        &self,
        db_name: &str,
        table_name: &str,
        args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        // Parse FTS3 arguments to get columns and tokenizer
        let (columns, tokenizer_name, tokenizer_args) = parse_fts3_args(args)?;

        // Create tokenizer
        let args_refs: Vec<&str> = tokenizer_args.iter().map(|s| s.as_str()).collect();
        let tokenizer = create_tokenizer(&tokenizer_name, &args_refs)?;

        // Build schema string for sqlite_master
        let schema = if columns.is_empty() {
            "(content TEXT)".to_string()
        } else {
            format!(
                "({})",
                columns
                    .iter()
                    .map(|c| format!("{} TEXT", c))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        // Create the FTS3 table
        let fts_columns = if columns.is_empty() {
            vec!["content".to_string()]
        } else {
            columns.clone()
        };

        let fts_table = Fts3Table::new(table_name, db_name, fts_columns.clone(), tokenizer);

        // Register in the global FTS3 registry (also returns the Arc)
        let table_arc = register_table(fts_table);

        // Create the adapter
        let adapter = Fts3VtabTableAdapter {
            name: table_name.to_string(),
            db_name: db_name.to_string(),
            columns: fts_columns,
            table: table_arc,
        };

        Ok((schema, Arc::new(adapter)))
    }

    fn connect(
        &self,
        db_name: &str,
        table_name: &str,
        args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        // For FTS3, connect is the same as create
        // (we reconnect to existing shadow tables on disk)
        self.create(db_name, table_name, args)
    }

    fn destroy(&self, _table: Arc<dyn VtabTable>) -> Result<()> {
        // TODO: Remove from global registry and drop shadow tables
        Ok(())
    }

    fn uses_shadow_tables(&self) -> bool {
        true
    }

    fn shadow_table_suffixes(&self) -> Vec<&'static str> {
        vec!["_content", "_segments", "_segdir", "_stat"]
    }
}

impl VtabModule for Fts4VtabModule {
    fn name(&self) -> &str {
        "fts4"
    }
    fn create(
        &self,
        db_name: &str,
        table_name: &str,
        args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        validate_fts4_args(args)?;
        Fts3VtabModule.create(db_name, table_name, args)
    }
    fn connect(
        &self,
        db_name: &str,
        table_name: &str,
        args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        validate_fts4_args(args)?;
        Fts3VtabModule.connect(db_name, table_name, args)
    }
    fn destroy(&self, table: Arc<dyn VtabTable>) -> Result<()> {
        Fts3VtabModule.destroy(table)
    }
    fn uses_shadow_tables(&self) -> bool {
        true
    }
    fn shadow_table_suffixes(&self) -> Vec<&'static str> {
        vec!["_content", "_segments", "_segdir", "_stat"]
    }
}

/// Known FTS4 option names (case-insensitive).
/// These are the recognized `key=value` parameters for FTS4.
const FTS4_KNOWN_OPTIONS: &[&str] = &[
    "matchinfo",
    "prefix",
    "compress",
    "uncompress",
    "order",
    "content",
    "languageid",
    "notindexed",
];

/// Validate FTS4 arguments.
///
/// In SQLite, FTS4 rejects unknown `key=value` parameters (unlike FTS3 which
/// treats them as column names). The first `tokenize=` is accepted; any
/// subsequent `tokenize=` or unrecognized `key=value` triggers an error.
fn validate_fts4_args(args: &[String]) -> Result<()> {
    let mut seen_tokenize = false;
    for arg in args {
        let arg = arg.trim();
        if arg.is_empty() {
            continue;
        }
        // Check for key=value pattern (contains '=' anywhere)
        if let Some(eq_pos) = arg.find('=') {
            let key = arg[..eq_pos].trim().to_ascii_lowercase();
            if key == "tokenize" {
                if seen_tokenize {
                    // Duplicate tokenize - unrecognized parameter
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        format!("unrecognized parameter: {}", arg),
                    ));
                }
                seen_tokenize = true;
            } else if !FTS4_KNOWN_OPTIONS.iter().any(|opt| *opt == key) {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("unrecognized parameter: {}", arg),
                ));
            }
        }
    }
    Ok(())
}

/// FTS3 virtual table instance adapter
pub struct Fts3VtabTableAdapter {
    name: String,
    db_name: String,
    columns: Vec<String>,
    table: Arc<Mutex<Fts3Table>>,
}

impl VtabTable for Fts3VtabTableAdapter {
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
        self.columns.get(col_idx).map(|s| s.as_str()).unwrap_or("")
    }

    fn column_affinity(&self, _col_idx: usize) -> Affinity {
        Affinity::Text
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // FTS3 supports MATCH queries on any column
        // Look for a MATCH constraint
        let mut has_match = false;
        for (idx, constraint) in info.constraints.iter().enumerate() {
            if constraint.usable && constraint.op == SQLITE_INDEX_CONSTRAINT_MATCH {
                // Use this MATCH constraint
                info.use_constraint(idx, 1, true); // arg_index=1, omit=true
                has_match = true;
                break;
            }
        }

        if has_match {
            // MATCH query - fast
            info.idx_num = 1; // 1 = MATCH query
            info.estimated_cost = 10.0;
            info.estimated_rows = 10;
        } else {
            // Full table scan - slower
            info.idx_num = 0; // 0 = full scan
            info.estimated_cost = 1_000_000.0;
            info.estimated_rows = 1_000_000;
        }

        Ok(())
    }

    fn open_cursor(&self) -> Result<Box<dyn VtabCursor>> {
        Ok(Box::new(Fts3VtabCursorAdapter {
            table: self.table.clone(),
            columns: self.columns.clone(),
            cursor: Fts3Cursor {
                expr: None,
                doclist: None,
                rowid: 0,
                eof: true,
            },
            current_rowids: Vec::new(),
            position: 0,
        }))
    }

    fn update(
        &self,
        rowid: Option<i64>,
        new_rowid: Option<i64>,
        columns: &[Option<Mem>],
    ) -> Result<i64> {
        let mut table = self
            .table
            .lock()
            .map_err(|_| Error::with_message(ErrorCode::Internal, "failed to lock FTS3 table"))?;

        if rowid.is_none() && new_rowid.is_some() {
            // INSERT
            let rowid = new_rowid.unwrap();
            let values: Vec<String> = columns
                .iter()
                .map(|c| c.as_ref().map(|m| m.to_str()).unwrap_or_default())
                .collect();
            let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
            table.insert(rowid, &values_refs)?;
            Ok(rowid)
        } else if let Some(old_rowid) = rowid {
            if columns.is_empty() {
                // DELETE - need old values for inverted index cleanup
                let old_values = table.content.get(&old_rowid).cloned().unwrap_or_default();
                let old_refs: Vec<&str> = old_values.iter().map(|s| s.as_str()).collect();
                table.delete(old_rowid, &old_refs)?;
                Ok(old_rowid)
            } else {
                // UPDATE = DELETE + INSERT
                let old_values = table.content.get(&old_rowid).cloned().unwrap_or_default();
                let old_refs: Vec<&str> = old_values.iter().map(|s| s.as_str()).collect();
                table.delete(old_rowid, &old_refs)?;
                let new_rid = new_rowid.unwrap_or(old_rowid);
                let values: Vec<String> = columns
                    .iter()
                    .map(|c| c.as_ref().map(|m| m.to_str()).unwrap_or_default())
                    .collect();
                let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
                table.insert(new_rid, &values_refs)?;
                Ok(new_rid)
            }
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                "invalid update parameters",
            ))
        }
    }
}

/// FTS3 cursor adapter
pub struct Fts3VtabCursorAdapter {
    table: Arc<Mutex<Fts3Table>>,
    columns: Vec<String>,
    cursor: Fts3Cursor,
    current_rowids: Vec<i64>,
    position: usize,
}

impl VtabCursor for Fts3VtabCursorAdapter {
    fn filter(
        &mut self,
        index_num: i32,
        _index_str: Option<&str>,
        constraints: &[ConstraintValue],
    ) -> Result<()> {
        let table = self
            .table
            .lock()
            .map_err(|_| Error::with_message(ErrorCode::Internal, "failed to lock FTS3 table"))?;

        if index_num == 1 {
            // MATCH query
            let query = constraints.first().map(|c| c.as_str()).unwrap_or_default();

            if query.is_empty() {
                self.current_rowids = Vec::new();
            } else {
                self.current_rowids = table.query_rowids(&query)?;
            }
        } else {
            // Full table scan - return all rowids
            self.current_rowids = table.content.keys().copied().collect();
            self.current_rowids.sort();
        }

        self.position = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.position += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.position >= self.current_rowids.len()
    }

    fn rowid(&self) -> Result<i64> {
        if self.position < self.current_rowids.len() {
            Ok(self.current_rowids[self.position])
        } else {
            Ok(0)
        }
    }

    fn column(&self, col_idx: usize) -> Result<Mem> {
        if self.position >= self.current_rowids.len() {
            return Ok(Mem::new());
        }

        let rowid = self.current_rowids[self.position];
        let table = self
            .table
            .lock()
            .map_err(|_| Error::with_message(ErrorCode::Internal, "failed to lock FTS3 table"))?;

        if let Some(content) = table.content.get(&rowid) {
            if col_idx < content.len() {
                return Ok(Mem::from_str(&content[col_idx]));
            }
        }

        Ok(Mem::new())
    }
}

/// Parse FTS3 CREATE VIRTUAL TABLE arguments
///
/// Arguments can be:
/// - Column names (just bare words)
/// - tokenize=<tokenizer> [args...]
/// - content=<table_name>
/// - prefix=<n>
fn parse_fts3_args(args: &[String]) -> Result<(Vec<String>, String, Vec<String>)> {
    let mut columns = Vec::new();
    let mut tokenizer_name = "simple".to_string();
    let mut tokenizer_args = Vec::new();

    for arg in args {
        let arg = arg.trim();
        if arg.is_empty() {
            continue;
        }

        if let Some(value) = arg.strip_prefix("tokenize=") {
            // Parse tokenizer specification
            let parts: Vec<&str> = value.split_whitespace().collect();
            if let Some(name) = parts.first() {
                tokenizer_name = name.to_string();
                tokenizer_args = parts[1..].iter().map(|s| s.to_string()).collect();
            }
        } else if arg.starts_with("content=") {
            // External content table - not fully supported yet
            // Just ignore for now
        } else if arg.starts_with("prefix=") {
            // Prefix indexes - not fully supported yet
            // Just ignore for now
        } else {
            // Assume it's a column name
            columns.push(arg.to_string());
        }
    }

    Ok((columns, tokenizer_name, tokenizer_args))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_fts3_args_empty() {
        let (columns, tokenizer, args) = parse_fts3_args(&[]).unwrap();
        assert!(columns.is_empty());
        assert_eq!(tokenizer, "simple");
        assert!(args.is_empty());
    }

    #[test]
    fn test_parse_fts3_args_columns() {
        let (columns, tokenizer, _) =
            parse_fts3_args(&["title".to_string(), "body".to_string()]).unwrap();
        assert_eq!(columns, vec!["title", "body"]);
        assert_eq!(tokenizer, "simple");
    }

    #[test]
    fn test_parse_fts3_args_tokenizer() {
        let (_, tokenizer, args) = parse_fts3_args(&["tokenize=porter".to_string()]).unwrap();
        assert_eq!(tokenizer, "porter");
        assert!(args.is_empty());
    }

    #[test]
    fn test_parse_fts3_args_tokenizer_with_args() {
        let (_, tokenizer, args) =
            parse_fts3_args(&["tokenize=unicode61 remove_diacritics=1".to_string()]).unwrap();
        assert_eq!(tokenizer, "unicode61");
        assert_eq!(args, vec!["remove_diacritics=1"]);
    }

    #[test]
    fn test_fts3_module_create() {
        let module = Fts3VtabModule;
        let (schema, table) = module
            .create(
                "main",
                "test_fts",
                &["title".to_string(), "body".to_string()],
            )
            .unwrap();

        assert!(schema.contains("title TEXT"));
        assert!(schema.contains("body TEXT"));
        assert_eq!(table.table_name(), "test_fts");
        assert_eq!(table.column_count(), 2);
        assert_eq!(table.column_name(0), "title");
        assert_eq!(table.column_name(1), "body");
    }
}
