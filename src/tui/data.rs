//! Data fetching and caching for the TUI browser.

use crate::types::{ColumnType, StepResult};
use crate::{
    sqlite3_column_count, sqlite3_column_name, sqlite3_column_text, sqlite3_column_type,
    sqlite3_finalize, sqlite3_prepare_v2, sqlite3_step, SqliteConnection,
};

use super::style;

/// Cached result set for display.
pub struct DataCache {
    pub columns: Vec<String>,
    pub col_widths: Vec<u16>,
    pub rows: Vec<Vec<String>>,
    /// Whether each cell is NULL (for styling)
    pub null_mask: Vec<Vec<bool>>,
    pub row_offset: usize,
    pub col_offset: usize,
    pub selected_row: usize,
    pub total_rows: Option<usize>,
    pub source_table: Option<String>,
    pub error: Option<String>,
}

impl DataCache {
    pub fn empty() -> Self {
        DataCache {
            columns: Vec::new(),
            col_widths: Vec::new(),
            rows: Vec::new(),
            null_mask: Vec::new(),
            row_offset: 0,
            col_offset: 0,
            selected_row: 0,
            total_rows: None,
            source_table: None,
            error: None,
        }
    }

    /// Load data from a table with LIMIT/OFFSET.
    pub fn from_table(
        conn: &mut SqliteConnection,
        name: &str,
        limit: usize,
        offset: usize,
    ) -> Self {
        // Get total row count
        let total = Self::count_rows(conn, name);

        let sql = format!(
            "SELECT * FROM \"{}\" LIMIT {} OFFSET {}",
            name.replace('"', "\"\""),
            limit,
            offset
        );
        let mut cache = Self::from_query(conn, &sql);
        cache.total_rows = total;
        cache.source_table = Some(name.to_string());
        cache
    }

    /// Execute arbitrary SQL and cache the results.
    pub fn from_query(conn: &mut SqliteConnection, sql: &str) -> Self {
        let (mut stmt, _tail) = match sqlite3_prepare_v2(conn, sql) {
            Ok(result) => result,
            Err(e) => {
                let mut cache = Self::empty();
                cache.error = Some(format!("{}", e));
                return cache;
            }
        };

        let col_count = sqlite3_column_count(&stmt);
        let columns: Vec<String> = (0..col_count)
            .map(|i| sqlite3_column_name(&stmt, i).unwrap_or("?").to_string())
            .collect();

        let mut rows = Vec::new();
        let mut null_mask = Vec::new();

        loop {
            if rows.len() >= style::BATCH_SIZE {
                break;
            }
            match sqlite3_step(&mut stmt) {
                Ok(StepResult::Row) => {
                    let mut row = Vec::with_capacity(col_count as usize);
                    let mut nulls = Vec::with_capacity(col_count as usize);
                    for i in 0..col_count {
                        let col_type = sqlite3_column_type(&stmt, i);
                        let is_null = col_type == ColumnType::Null;
                        nulls.push(is_null);
                        if is_null {
                            row.push(style::NULL_DISPLAY.to_string());
                        } else {
                            row.push(sqlite3_column_text(&stmt, i));
                        }
                    }
                    rows.push(row);
                    null_mask.push(nulls);
                }
                Ok(StepResult::Done) => break,
                Err(_) => break,
            }
        }

        let _ = sqlite3_finalize(stmt);

        let col_widths = compute_col_widths(&columns, &rows);

        DataCache {
            columns,
            col_widths,
            rows,
            null_mask,
            row_offset: 0,
            col_offset: 0,
            selected_row: 0,
            total_rows: None,
            source_table: None,
            error: None,
        }
    }

    fn count_rows(conn: &mut SqliteConnection, table: &str) -> Option<usize> {
        let sql = format!("SELECT COUNT(*) FROM \"{}\"", table.replace('"', "\"\""));
        let (mut stmt, _) = sqlite3_prepare_v2(conn, &sql).ok()?;
        if let Ok(StepResult::Row) = sqlite3_step(&mut stmt) {
            let text = sqlite3_column_text(&stmt, 0);
            let count = text.parse::<usize>().ok();
            let _ = sqlite3_finalize(stmt);
            count
        } else {
            let _ = sqlite3_finalize(stmt);
            None
        }
    }
}

/// Compute column widths based on header and data, clamped to MAX_COL_WIDTH.
fn compute_col_widths(columns: &[String], rows: &[Vec<String>]) -> Vec<u16> {
    let mut widths: Vec<u16> = columns
        .iter()
        .map(|c| c.len().max(style::MIN_COL_WIDTH as usize) as u16)
        .collect();

    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                let w = cell.len() as u16;
                if w > widths[i] {
                    widths[i] = w;
                }
            }
        }
    }

    // Clamp to max
    for w in &mut widths {
        if *w > style::MAX_COL_WIDTH {
            *w = style::MAX_COL_WIDTH;
        }
    }

    widths
}
