//! Application state for the TUI browser.

use crate::types::StepResult;
use crate::{
    sqlite3_column_text, sqlite3_finalize, sqlite3_prepare_v2, sqlite3_step, SqliteConnection,
};

use super::data::DataCache;
use super::style;

/// Which panel currently has focus.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FocusedPanel {
    TableList,
    DataView,
    QueryInput,
}

/// Current input mode.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AppMode {
    Normal,
    QueryInput,
}

/// Main application state.
pub struct BrowseApp {
    pub conn: Option<Box<SqliteConnection>>,
    pub db_path: String,

    // Table list
    pub tables: Vec<String>,
    pub table_cursor: usize,
    pub table_scroll: usize,

    // Focus and mode
    pub focus: FocusedPanel,
    pub mode: AppMode,

    // Data view
    pub data: DataCache,

    // Query input
    pub query_buffer: String,
    pub query_cursor: usize,

    // Status
    pub status_message: Option<String>,

    // Terminal
    pub term_width: u16,
    pub term_height: u16,
    pub should_quit: bool,
}

impl BrowseApp {
    pub fn new(conn: Box<SqliteConnection>, db_path: &str) -> Self {
        let (w, h) = crossterm::terminal::size().unwrap_or((80, 24));
        let mut app = BrowseApp {
            conn: Some(conn),
            db_path: db_path.to_string(),
            tables: Vec::new(),
            table_cursor: 0,
            table_scroll: 0,
            focus: FocusedPanel::TableList,
            mode: AppMode::Normal,
            data: DataCache::empty(),
            query_buffer: String::new(),
            query_cursor: 0,
            status_message: None,
            term_width: w,
            term_height: h,
            should_quit: false,
        };
        app.load_tables();
        // Auto-select first table if available
        if !app.tables.is_empty() {
            app.select_table(0);
        }
        app
    }

    /// Load the list of tables from sqlite_master.
    pub fn load_tables(&mut self) {
        let conn = match self.conn.as_mut() {
            Some(c) => c.as_mut(),
            None => return,
        };

        let sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name";
        let (mut stmt, _) = match sqlite3_prepare_v2(conn, sql) {
            Ok(r) => r,
            Err(_) => return,
        };

        let mut tables = Vec::new();
        loop {
            match sqlite3_step(&mut stmt) {
                Ok(StepResult::Row) => {
                    tables.push(sqlite3_column_text(&stmt, 0));
                }
                Ok(StepResult::Done) => break,
                _ => break,
            }
        }
        let _ = sqlite3_finalize(stmt);
        self.tables = tables;
    }

    /// Select a table by index and load its data.
    pub fn select_table(&mut self, idx: usize) {
        if idx >= self.tables.len() {
            return;
        }
        self.table_cursor = idx;
        let name = self.tables[idx].clone();

        let conn = match self.conn.as_mut() {
            Some(c) => c.as_mut(),
            None => return,
        };

        self.data = DataCache::from_table(conn, &name, style::BATCH_SIZE, 0);
        self.status_message = None;
    }

    /// Execute a SQL query and display results in the data panel.
    pub fn execute_query(&mut self, sql: &str) {
        let conn = match self.conn.as_mut() {
            Some(c) => c.as_mut(),
            None => return,
        };

        self.data = DataCache::from_query(conn, sql);
        if let Some(ref err) = self.data.error {
            self.status_message = Some(format!("Error: {}", err));
        } else {
            self.status_message = Some(format!(
                "Query returned {} row{}",
                self.data.rows.len(),
                if self.data.rows.len() == 1 { "" } else { "s" }
            ));
        }
    }

    /// Handle terminal resize.
    pub fn handle_resize(&mut self, width: u16, height: u16) {
        self.term_width = width;
        self.term_height = height;
    }

    /// Cycle focus forward: TableList -> DataView -> QueryInput -> TableList.
    pub fn cycle_focus_forward(&mut self) {
        self.focus = match self.focus {
            FocusedPanel::TableList => FocusedPanel::DataView,
            FocusedPanel::DataView => FocusedPanel::QueryInput,
            FocusedPanel::QueryInput => FocusedPanel::TableList,
        };
        // Enter/exit query mode based on focus
        self.mode = if self.focus == FocusedPanel::QueryInput {
            AppMode::QueryInput
        } else {
            AppMode::Normal
        };
    }

    /// Cycle focus backward.
    pub fn cycle_focus_backward(&mut self) {
        self.focus = match self.focus {
            FocusedPanel::TableList => FocusedPanel::QueryInput,
            FocusedPanel::DataView => FocusedPanel::TableList,
            FocusedPanel::QueryInput => FocusedPanel::DataView,
        };
        self.mode = if self.focus == FocusedPanel::QueryInput {
            AppMode::QueryInput
        } else {
            AppMode::Normal
        };
    }

    /// Consume self, returning the owned connection.
    pub fn into_connection(mut self) -> Option<Box<SqliteConnection>> {
        self.conn.take()
    }

    /// Compute the width of the table list panel.
    pub fn table_panel_width(&self) -> u16 {
        let longest = self.tables.iter().map(|t| t.len()).max().unwrap_or(6) as u16;
        let w = longest + style::TABLE_PANEL_PADDING;
        w.min(self.term_width / 4).min(style::TABLE_PANEL_MAX_WIDTH)
    }

    /// Refresh the current view (reload table data or re-run query).
    pub fn refresh(&mut self) {
        if let Some(ref table) = self.data.source_table.clone() {
            let conn = match self.conn.as_mut() {
                Some(c) => c.as_mut(),
                None => return,
            };
            self.data = DataCache::from_table(conn, table, style::BATCH_SIZE, 0);
        }
        self.load_tables();
    }
}
