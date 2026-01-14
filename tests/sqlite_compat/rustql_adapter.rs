//! RustQL adapter implementing TestDatabase trait.
//!
//! This adapter connects the test runner to rustql's API.

use super::runner::{CatchResult, TestDatabase};
use rustql::types::{ColumnType, StepResult};
use rustql::{
    sqlite3_close, sqlite3_column_count, sqlite3_column_text, sqlite3_column_type,
    sqlite3_finalize, sqlite3_initialize, sqlite3_open, sqlite3_prepare_v2, sqlite3_step,
    PreparedStmt, SqliteConnection,
};
use std::fs;

/// RustQL database adapter for testing
pub struct RustqlTestDb {
    /// Path to the database file
    path: String,
    /// Database connection
    conn: Option<Box<SqliteConnection>>,
}

impl RustqlTestDb {
    /// Create a new test database
    pub fn new(path: &str) -> Result<Self, String> {
        // Initialize the library
        sqlite3_initialize().map_err(|e| format!("Failed to initialize: {}", e))?;

        // Remove existing test database
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}-journal", path));
        let _ = fs::remove_file(format!("{}-wal", path));
        let _ = fs::remove_file(format!("{}-shm", path));

        // Open connection
        let conn = sqlite3_open(path).map_err(|e| format!("Failed to open: {}", e))?;

        Ok(Self {
            path: path.to_string(),
            conn: Some(conn),
        })
    }

    /// Get mutable connection reference
    fn conn_mut(&mut self) -> Result<&mut SqliteConnection, String> {
        self.conn
            .as_mut()
            .map(|b| b.as_mut())
            .ok_or_else(|| "No connection".to_string())
    }

    /// Execute SQL and collect results
    fn execute_sql_internal(&mut self, sql: &str) -> Result<Vec<Vec<String>>, String> {
        let mut all_results = Vec::new();
        let mut remaining = sql.trim();

        while !remaining.is_empty() {
            // Skip comments and whitespace
            remaining = remaining.trim_start();
            if remaining.starts_with("--") {
                if let Some(pos) = remaining.find('\n') {
                    remaining = &remaining[pos + 1..];
                    continue;
                } else {
                    break;
                }
            }

            // Prepare statement
            let conn = self.conn_mut()?;
            let (mut stmt, tail) = match sqlite3_prepare_v2(conn, remaining) {
                Ok(result) => result,
                Err(e) => return Err(e.sqlite_errmsg()),
            };

            // If no statement was compiled (empty or comment-only), advance
            if stmt.sql().is_empty() {
                remaining = tail;
                continue;
            }

            // Execute and collect results
            let results = self.step_and_collect(&mut stmt)?;
            all_results.extend(results);

            // Clean up
            let _ = sqlite3_finalize(stmt);

            remaining = tail;
        }

        Ok(all_results)
    }

    /// Step through a statement and collect results
    fn step_and_collect(&mut self, stmt: &mut PreparedStmt) -> Result<Vec<Vec<String>>, String> {
        let mut rows = Vec::new();
        let col_count = sqlite3_column_count(stmt);

        // Safety limit to prevent infinite loops in incomplete implementations
        const MAX_ROWS: usize = 10000;
        let mut row_count = 0;

        loop {
            match sqlite3_step(stmt) {
                Ok(StepResult::Row) => {
                    let mut row = Vec::new();
                    for i in 0..col_count {
                        // Check if column is NULL and format for TCL compatibility
                        let col_type = sqlite3_column_type(stmt, i);
                        let text = if col_type == ColumnType::Null {
                            "{}".to_string() // TCL representation of NULL
                        } else {
                            sqlite3_column_text(stmt, i)
                        };
                        row.push(text);
                    }
                    rows.push(row);
                    row_count += 1;
                    if row_count >= MAX_ROWS {
                        return Err("Row limit exceeded".to_string());
                    }
                }
                Ok(StepResult::Done) => break,
                Err(e) => return Err(e.sqlite_errmsg()),
            }
        }

        Ok(rows)
    }
}

impl TestDatabase for RustqlTestDb {
    fn exec_sql(&mut self, sql: &str) -> Result<Vec<String>, String> {
        let rows = self.execute_sql_internal(sql)?;

        // Flatten rows to TCL-style result list
        let mut result = Vec::new();
        for row in rows {
            result.extend(row);
        }
        Ok(result)
    }

    fn catch_sql(&mut self, sql: &str) -> Result<CatchResult, String> {
        match self.execute_sql_internal(sql) {
            Ok(rows) => {
                // Flatten rows to TCL-style result list
                let mut result = Vec::new();
                for row in rows {
                    result.extend(row);
                }
                Ok(CatchResult::Success(result))
            }
            Err(e) => Ok(CatchResult::Error(e)),
        }
    }

    fn reopen(&mut self) -> Result<(), String> {
        // Close existing connection
        if let Some(conn) = self.conn.take() {
            let _ = sqlite3_close(conn);
        }

        // Reopen
        let conn = sqlite3_open(&self.path).map_err(|e| format!("Failed to reopen: {}", e))?;
        self.conn = Some(conn);
        Ok(())
    }

    fn path(&self) -> &str {
        &self.path
    }
}

impl Drop for RustqlTestDb {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let _ = sqlite3_close(conn);
        }
        // Optionally clean up test file
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_file(format!("{}-journal", &self.path));
        let _ = fs::remove_file(format!("{}-wal", &self.path));
        let _ = fs::remove_file(format!("{}-shm", &self.path));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_db() {
        let db = RustqlTestDb::new("/tmp/rustql_test_adapter.db");
        assert!(db.is_ok(), "Should create database");
    }
}
