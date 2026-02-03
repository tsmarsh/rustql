//! Event handling for the TUI browser.

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use std::time::Duration;

use super::app::{AppMode, BrowseApp, FocusedPanel};

/// Poll for events and dispatch. Returns true if an event was handled.
pub fn handle_event(app: &mut BrowseApp) -> bool {
    if !event::poll(Duration::from_millis(50)).unwrap_or(false) {
        return false;
    }

    let evt = match event::read() {
        Ok(e) => e,
        Err(_) => return false,
    };

    match evt {
        Event::Key(key) => match app.mode {
            AppMode::Normal => handle_normal_key(app, key),
            AppMode::QueryInput => handle_query_key(app, key),
        },
        Event::Resize(w, h) => {
            app.handle_resize(w, h);
        }
        _ => {}
    }

    true
}

/// Handle key events in normal mode.
fn handle_normal_key(app: &mut BrowseApp, key: KeyEvent) {
    match key.code {
        // Quit
        KeyCode::Char('q') | KeyCode::Esc => {
            app.should_quit = true;
        }

        // Focus cycling
        KeyCode::Tab => {
            if key.modifiers.contains(KeyModifiers::SHIFT) {
                app.cycle_focus_backward();
            } else {
                app.cycle_focus_forward();
            }
        }
        KeyCode::BackTab => {
            app.cycle_focus_backward();
        }

        // Enter query mode
        KeyCode::Char(':') | KeyCode::Char('/') => {
            app.focus = FocusedPanel::QueryInput;
            app.mode = AppMode::QueryInput;
        }

        // Refresh
        KeyCode::Char('r') => {
            app.refresh();
        }

        // Navigation
        KeyCode::Char('j') | KeyCode::Down => match app.focus {
            FocusedPanel::TableList => navigate_table_list(app, 1),
            FocusedPanel::DataView => navigate_data_vertical(app, 1),
            _ => {}
        },
        KeyCode::Char('k') | KeyCode::Up => match app.focus {
            FocusedPanel::TableList => navigate_table_list(app, -1),
            FocusedPanel::DataView => navigate_data_vertical(app, -1),
            _ => {}
        },
        KeyCode::Char('h') | KeyCode::Left => {
            if app.focus == FocusedPanel::DataView && app.data.col_offset > 0 {
                app.data.col_offset -= 1;
            }
        }
        KeyCode::Char('l') | KeyCode::Right => {
            if app.focus == FocusedPanel::DataView {
                let max_col = app.data.columns.len().saturating_sub(1);
                if app.data.col_offset < max_col {
                    app.data.col_offset += 1;
                }
            }
        }

        // Select table
        KeyCode::Enter => {
            if app.focus == FocusedPanel::TableList {
                let idx = app.table_cursor;
                app.select_table(idx);
            }
        }

        // Jump to first/last
        KeyCode::Char('g') => match app.focus {
            FocusedPanel::TableList => {
                app.table_cursor = 0;
                app.table_scroll = 0;
            }
            FocusedPanel::DataView => {
                app.data.selected_row = 0;
                app.data.row_offset = 0;
            }
            _ => {}
        },
        KeyCode::Char('G') => match app.focus {
            FocusedPanel::TableList => {
                if !app.tables.is_empty() {
                    app.table_cursor = app.tables.len() - 1;
                }
            }
            FocusedPanel::DataView => {
                if !app.data.rows.is_empty() {
                    app.data.selected_row = app.data.rows.len() - 1;
                }
            }
            _ => {}
        },

        // Page down/up
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let page = visible_data_rows(app) / 2;
            match app.focus {
                FocusedPanel::DataView => navigate_data_vertical(app, page as i32),
                FocusedPanel::TableList => {
                    for _ in 0..page {
                        navigate_table_list(app, 1);
                    }
                }
                _ => {}
            }
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let page = visible_data_rows(app) / 2;
            match app.focus {
                FocusedPanel::DataView => navigate_data_vertical(app, -(page as i32)),
                FocusedPanel::TableList => {
                    for _ in 0..page {
                        navigate_table_list(app, -1);
                    }
                }
                _ => {}
            }
        }

        _ => {}
    }
}

/// Handle key events in query input mode.
fn handle_query_key(app: &mut BrowseApp, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.mode = AppMode::Normal;
            app.focus = FocusedPanel::DataView;
        }
        KeyCode::Enter => {
            let sql = app.query_buffer.clone();
            if !sql.trim().is_empty() {
                app.execute_query(&sql);
            }
            app.mode = AppMode::Normal;
            app.focus = FocusedPanel::DataView;
        }
        KeyCode::Backspace => {
            if app.query_cursor > 0 {
                app.query_cursor -= 1;
                app.query_buffer.remove(app.query_cursor);
            }
        }
        KeyCode::Delete => {
            if app.query_cursor < app.query_buffer.len() {
                app.query_buffer.remove(app.query_cursor);
            }
        }
        KeyCode::Left => {
            if app.query_cursor > 0 {
                app.query_cursor -= 1;
            }
        }
        KeyCode::Right => {
            if app.query_cursor < app.query_buffer.len() {
                app.query_cursor += 1;
            }
        }
        KeyCode::Home => {
            app.query_cursor = 0;
        }
        KeyCode::End => {
            app.query_cursor = app.query_buffer.len();
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            // Clear line
            app.query_buffer.clear();
            app.query_cursor = 0;
        }
        KeyCode::Char(c) => {
            app.query_buffer.insert(app.query_cursor, c);
            app.query_cursor += 1;
        }
        _ => {}
    }
}

/// Navigate the table list by delta rows.
fn navigate_table_list(app: &mut BrowseApp, delta: i32) {
    if app.tables.is_empty() {
        return;
    }
    let new = app.table_cursor as i32 + delta;
    app.table_cursor = new.clamp(0, app.tables.len() as i32 - 1) as usize;
}

/// Navigate the data view vertically by delta rows.
fn navigate_data_vertical(app: &mut BrowseApp, delta: i32) {
    if app.data.rows.is_empty() {
        return;
    }
    let new = app.data.selected_row as i32 + delta;
    app.data.selected_row = new.clamp(0, app.data.rows.len() as i32 - 1) as usize;

    // Adjust scroll to keep selection visible
    let visible = visible_data_rows(app);
    if app.data.selected_row < app.data.row_offset {
        app.data.row_offset = app.data.selected_row;
    } else if app.data.selected_row >= app.data.row_offset + visible {
        app.data.row_offset = app.data.selected_row + 1 - visible;
    }
}

/// Calculate how many data rows are visible in the data panel.
fn visible_data_rows(app: &BrowseApp) -> usize {
    // Total height minus: status bar (1) + column header (1) + separator (1) + query bar (1) + borders (2)
    let overhead: u16 = 6;
    if app.term_height > overhead {
        (app.term_height - overhead) as usize
    } else {
        1
    }
}
