//! Virtual table adapter for FTS5
//!
//! This module wraps the existing FTS5 implementation with the VtabModule,
//! VtabTable, and VtabCursor traits to integrate with the virtual table registry.

use std::sync::{Arc, Mutex};

use crate::error::{Error, ErrorCode, Result};
use crate::schema::Affinity;
use crate::vdbe::mem::Mem;
use crate::vtab::{
    ConstraintValue, IndexInfo, VtabCursor, VtabModule, VtabTable, SQLITE_INDEX_CONSTRAINT_MATCH,
};

use super::main::Fts5Table;
use super::registry::register_table;
use super::tokenizer::{create_tokenizer, SimpleTokenizer};

/// FTS5 virtual table module
///
/// This module creates FTS5 full-text search tables.
pub struct Fts5VtabModule;

impl VtabModule for Fts5VtabModule {
    fn name(&self) -> &str {
        "fts5"
    }

    fn create(
        &self,
        db_name: &str,
        table_name: &str,
        args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        // Parse FTS5 arguments to get columns and tokenizer
        let (columns, tokenizer_name, tokenizer_args, has_content, content_table) =
            parse_fts5_args(args)?;

        // Create tokenizer
        let args_refs: Vec<&str> = tokenizer_args.iter().map(|s| s.as_str()).collect();
        let tokenizer = create_tokenizer(&tokenizer_name, &args_refs)
            .unwrap_or_else(|_| Box::new(SimpleTokenizer::default()));

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

        // Create the FTS5 table
        let fts_columns = if columns.is_empty() {
            vec!["content".to_string()]
        } else {
            columns.clone()
        };

        let mut fts_table = Fts5Table::new(table_name, db_name, fts_columns.clone(), tokenizer);
        fts_table.has_content = has_content;
        fts_table.content_table = content_table;

        // Register in the global FTS5 registry
        let table_arc = register_table(fts_table);

        // Create the adapter
        let adapter = Fts5VtabTableAdapter {
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
        // For FTS5, connect is the same as create
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
        vec!["_content", "_data", "_idx", "_docsize", "_config"]
    }
}

/// FTS5 virtual table instance adapter
pub struct Fts5VtabTableAdapter {
    name: String,
    db_name: String,
    columns: Vec<String>,
    table: Arc<Mutex<Fts5Table>>,
}

impl VtabTable for Fts5VtabTableAdapter {
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
        // FTS5 supports MATCH queries on any column
        let mut has_match = false;
        for (idx, constraint) in info.constraints.iter().enumerate() {
            if constraint.usable && constraint.op == SQLITE_INDEX_CONSTRAINT_MATCH {
                info.use_constraint(idx, 1, true);
                has_match = true;
                break;
            }
        }

        if has_match {
            info.idx_num = 1; // 1 = MATCH query
            info.estimated_cost = 10.0;
            info.estimated_rows = 10;
        } else {
            info.idx_num = 0; // 0 = full scan
            info.estimated_cost = 1_000_000.0;
            info.estimated_rows = 1_000_000;
        }

        Ok(())
    }

    fn open_cursor(&self) -> Result<Box<dyn VtabCursor>> {
        Ok(Box::new(Fts5VtabCursorAdapter {
            table: self.table.clone(),
            columns: self.columns.clone(),
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
            .map_err(|_| Error::with_message(ErrorCode::Internal, "failed to lock FTS5 table"))?;

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

/// FTS5 cursor adapter
pub struct Fts5VtabCursorAdapter {
    table: Arc<Mutex<Fts5Table>>,
    columns: Vec<String>,
    current_rowids: Vec<i64>,
    position: usize,
}

impl VtabCursor for Fts5VtabCursorAdapter {
    fn filter(
        &mut self,
        index_num: i32,
        _index_str: Option<&str>,
        constraints: &[ConstraintValue],
    ) -> Result<()> {
        let table = self
            .table
            .lock()
            .map_err(|_| Error::with_message(ErrorCode::Internal, "failed to lock FTS5 table"))?;

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
            self.current_rowids = table.all_rowids();
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
            .map_err(|_| Error::with_message(ErrorCode::Internal, "failed to lock FTS5 table"))?;

        if let Some(content) = table.row_values(rowid) {
            if col_idx < content.len() {
                return Ok(Mem::from_str(&content[col_idx]));
            }
        }

        Ok(Mem::new())
    }
}

/// Parse FTS5 CREATE VIRTUAL TABLE arguments
///
/// Returns: (columns, tokenizer_name, tokenizer_args, has_content, content_table)
fn parse_fts5_args(
    args: &[String],
) -> Result<(Vec<String>, String, Vec<String>, bool, Option<String>)> {
    let mut columns = Vec::new();
    let mut tokenizer_name = "unicode61".to_string();
    let mut tokenizer_args = Vec::new();
    let mut has_content = true;
    let mut content_table = None;

    for arg in args {
        let arg = arg.trim();
        if arg.is_empty() {
            continue;
        }

        if let Some(value) = arg
            .strip_prefix("tokenize=")
            .or_else(|| arg.strip_prefix("TOKENIZE="))
        {
            // Parse tokenizer specification
            let parts: Vec<&str> = value.split_whitespace().collect();
            if let Some(name) = parts.first() {
                tokenizer_name = name.to_string();
                tokenizer_args = parts[1..].iter().map(|s| s.to_string()).collect();
            }
        } else if let Some(value) = arg
            .strip_prefix("content=")
            .or_else(|| arg.strip_prefix("CONTENT="))
        {
            // External content table or contentless
            let value = value.trim().trim_matches('"').trim_matches('\'');
            if value.eq_ignore_ascii_case("") {
                has_content = false;
                content_table = None;
            } else {
                has_content = true;
                content_table = Some(value.to_string());
            }
        } else if arg.starts_with("prefix=") || arg.starts_with("PREFIX=") {
            // Prefix indexes - parse but don't fully support yet
        } else if arg.starts_with("detail=") || arg.starts_with("DETAIL=") {
            // Detail mode - parse but don't fully support yet
        } else if arg.starts_with("columnsize=") || arg.starts_with("COLUMNSIZE=") {
            // Column size storage - parse but don't fully support yet
        } else if !arg.contains('=') {
            // Assume it's a column name
            columns.push(arg.to_string());
        }
    }

    Ok((
        columns,
        tokenizer_name,
        tokenizer_args,
        has_content,
        content_table,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_fts5_args_empty() {
        let (columns, tokenizer, args, has_content, content_table) = parse_fts5_args(&[]).unwrap();
        assert!(columns.is_empty());
        assert_eq!(tokenizer, "unicode61");
        assert!(args.is_empty());
        assert!(has_content);
        assert!(content_table.is_none());
    }

    #[test]
    fn test_parse_fts5_args_columns() {
        let (columns, tokenizer, _, _, _) =
            parse_fts5_args(&["title".to_string(), "body".to_string()]).unwrap();
        assert_eq!(columns, vec!["title", "body"]);
        assert_eq!(tokenizer, "unicode61");
    }

    #[test]
    fn test_parse_fts5_args_tokenizer() {
        let (_, tokenizer, args, _, _) = parse_fts5_args(&["tokenize=porter".to_string()]).unwrap();
        assert_eq!(tokenizer, "porter");
        assert!(args.is_empty());
    }

    #[test]
    fn test_parse_fts5_args_contentless() {
        let (_, _, _, has_content, content_table) =
            parse_fts5_args(&["content=".to_string()]).unwrap();
        assert!(!has_content);
        assert!(content_table.is_none());
    }

    #[test]
    fn test_fts5_module_create() {
        let module = Fts5VtabModule;
        let (schema, table) = module
            .create(
                "main",
                "test_fts5",
                &["title".to_string(), "body".to_string()],
            )
            .unwrap();

        assert!(schema.contains("title TEXT"));
        assert!(schema.contains("body TEXT"));
        assert_eq!(table.table_name(), "test_fts5");
        assert_eq!(table.column_count(), 2);
        assert_eq!(table.column_name(0), "title");
        assert_eq!(table.column_name(1), "body");
    }
}
