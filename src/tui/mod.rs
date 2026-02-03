//! TUI database browser for RustQL.
//!
//! Activated via `.browse` from the REPL. Provides a Datasette-inspired
//! terminal UI for browsing tables and running queries.

mod app;
mod data;
mod input;
mod render;
mod style;

use crate::SqliteConnection;
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::{cursor, execute};
use std::io;

use app::BrowseApp;

/// RAII guard that enables raw mode and alternate screen on creation,
/// and restores the terminal on drop.
struct TerminalGuard;

impl TerminalGuard {
    fn new() -> io::Result<Self> {
        terminal::enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, cursor::Hide)?;
        Ok(TerminalGuard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = terminal::disable_raw_mode();
        let mut stdout = io::stdout();
        let _ = execute!(stdout, cursor::Show, LeaveAlternateScreen);
    }
}

/// Enter the TUI browser. Takes ownership of the connection and returns it when done.
///
/// # Arguments
/// * `conn` - The active database connection (ownership transferred)
/// * `db_path` - Path to the database file (for display)
///
/// # Returns
/// The connection back to the caller, or an error string.
pub fn browse(conn: Box<SqliteConnection>, db_path: &str) -> Result<Box<SqliteConnection>, String> {
    let _guard = TerminalGuard::new().map_err(|e| format!("Failed to initialize TUI: {}", e))?;

    let mut app = BrowseApp::new(conn, db_path);
    run_event_loop(&mut app);

    app.into_connection()
        .ok_or_else(|| "Connection lost during browse".to_string())
}

/// Main event loop: poll input, update state, render.
fn run_event_loop(app: &mut BrowseApp) {
    let mut stdout = io::stdout();

    // Initial render
    render::render(app, &mut stdout);

    while !app.should_quit {
        let had_event = input::handle_event(app);
        if had_event {
            render::render(app, &mut stdout);
        }
    }
}
