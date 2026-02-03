//! Style constants for the TUI browser.

use crossterm::style::Color;

// Box-drawing characters
pub const BOX_HORIZONTAL: char = '─';
pub const BOX_VERTICAL: char = '│';
pub const BOX_TOP_LEFT: char = '┌';
pub const BOX_TOP_RIGHT: char = '┐';
pub const BOX_BOTTOM_LEFT: char = '└';
pub const BOX_BOTTOM_RIGHT: char = '┘';
pub const BOX_T_DOWN: char = '┬';
pub const BOX_T_UP: char = '┴';
pub const BOX_T_RIGHT: char = '├';
pub const BOX_T_LEFT: char = '┤';
pub const BOX_CROSS: char = '┼';

// Color scheme
pub const BORDER_FOCUSED: Color = Color::Cyan;
pub const BORDER_UNFOCUSED: Color = Color::DarkGrey;
pub const HEADER_BG: Color = Color::DarkBlue;
pub const HEADER_FG: Color = Color::White;
pub const SELECTED_BG: Color = Color::DarkCyan;
pub const SELECTED_FG: Color = Color::White;
pub const STATUS_BG: Color = Color::DarkBlue;
pub const STATUS_FG: Color = Color::White;
pub const DATA_FG: Color = Color::White;
pub const NULL_FG: Color = Color::DarkGrey;
pub const QUERY_PROMPT_FG: Color = Color::Cyan;
pub const ERROR_FG: Color = Color::Red;

// Display constants
pub const NULL_DISPLAY: &str = "NULL";
pub const QUERY_PROMPT: &str = "SQL> ";
pub const MAX_COL_WIDTH: u16 = 40;
pub const MIN_COL_WIDTH: u16 = 4;
pub const BATCH_SIZE: usize = 1000;
pub const TABLE_PANEL_MAX_WIDTH: u16 = 30;
pub const TABLE_PANEL_PADDING: u16 = 4;
