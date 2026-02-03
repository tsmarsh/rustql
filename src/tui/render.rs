//! Rendering for the TUI browser.

use crossterm::{
    cursor, queue,
    style::{self, Color, Print, SetBackgroundColor, SetForegroundColor},
    terminal::{Clear, ClearType},
};
use std::io::{self, Write};

use super::app::{AppMode, BrowseApp, FocusedPanel};
use super::style as s;

/// Render the entire UI.
pub fn render(app: &BrowseApp, stdout: &mut io::Stdout) {
    let _ = queue!(stdout, cursor::Hide, cursor::MoveTo(0, 0));

    let w = app.term_width;
    let h = app.term_height;
    if w < 20 || h < 6 {
        let _ = queue!(
            stdout,
            Clear(ClearType::All),
            cursor::MoveTo(0, 0),
            Print("Terminal too small")
        );
        let _ = stdout.flush();
        return;
    }

    let panel_w = app.table_panel_width();
    let data_x = panel_w + 1; // +1 for vertical border
    let data_w = w.saturating_sub(data_x);

    // Row layout:
    // 0: status bar
    // 1: top border
    // 2..h-2: content area (table list + data)
    // h-2: bottom border
    // h-1: query bar

    draw_status_bar(app, stdout, w);
    draw_top_border(app, stdout, w, panel_w);
    draw_content_area(app, stdout, panel_w, data_x, data_w, h);
    draw_bottom_border(app, stdout, w, h);
    draw_query_bar(app, stdout, w, h);

    // Position cursor for query input
    if app.mode == AppMode::QueryInput {
        let cursor_x = s::QUERY_PROMPT.len() as u16 + app.query_cursor as u16 + 1; // +1 for border
        let _ = queue!(stdout, cursor::MoveTo(cursor_x, h - 1), cursor::Show);
    }

    let _ = stdout.flush();
}

/// Draw the status bar at row 0.
fn draw_status_bar(app: &BrowseApp, stdout: &mut io::Stdout, width: u16) {
    let _ = queue!(stdout, cursor::MoveTo(0, 0));

    let left = if let Some(ref table) = app.data.source_table {
        let row_info = match app.data.total_rows {
            Some(n) => format!(" ({} rows)", n),
            None => String::new(),
        };
        format!(" {}{}", table, row_info)
    } else if let Some(ref msg) = app.status_message {
        format!(" {}", msg)
    } else {
        " No table selected".to_string()
    };

    let right = format!(" {} ", app.db_path);

    let _ = queue!(
        stdout,
        SetBackgroundColor(s::STATUS_BG),
        SetForegroundColor(s::STATUS_FG)
    );

    let padding = width as usize - left.len().min(width as usize) - right.len().min(width as usize);
    let bar = format!(
        "{}{}{}",
        left,
        " ".repeat(padding.min(width as usize)),
        right
    );
    let _ = queue!(
        stdout,
        Print(truncate_or_pad(&bar, width as usize)),
        style::ResetColor
    );
}

/// Draw the top border at row 1.
fn draw_top_border(app: &BrowseApp, stdout: &mut io::Stdout, width: u16, panel_w: u16) {
    let _ = queue!(stdout, cursor::MoveTo(0, 1));

    let border_color = border_color_for(app, None);
    let _ = queue!(stdout, SetForegroundColor(border_color));

    let mut line = String::with_capacity(width as usize);
    line.push(s::BOX_TOP_LEFT);
    for i in 1..width.saturating_sub(1) {
        if i == panel_w {
            line.push(s::BOX_T_DOWN);
        } else {
            line.push(s::BOX_HORIZONTAL);
        }
    }
    line.push(s::BOX_TOP_RIGHT);
    let _ = queue!(
        stdout,
        Print(truncate_or_pad(&line, width as usize)),
        style::ResetColor
    );
}

/// Draw the content area (table list panel + vertical border + data panel).
fn draw_content_area(
    app: &BrowseApp,
    stdout: &mut io::Stdout,
    panel_w: u16,
    data_x: u16,
    data_w: u16,
    height: u16,
) {
    // Content rows: from row 2 to row h-3 (inclusive)
    let content_start = 2u16;
    let content_end = height.saturating_sub(2);

    // First row of content area is the column header for data
    for row in content_start..content_end {
        let content_row = (row - content_start) as usize;

        // Draw table list cell
        draw_table_list_cell(app, stdout, panel_w, row, content_row);

        // Draw vertical border
        let border_color = border_color_for(app, None);
        let _ = queue!(
            stdout,
            cursor::MoveTo(panel_w, row),
            SetForegroundColor(border_color),
            Print(s::BOX_VERTICAL),
            style::ResetColor
        );

        // Draw data panel cell
        draw_data_cell(app, stdout, data_x, data_w, row, content_row);
    }
}

/// Draw a single cell in the table list panel.
fn draw_table_list_cell(
    app: &BrowseApp,
    stdout: &mut io::Stdout,
    panel_w: u16,
    row: u16,
    content_row: usize,
) {
    let _ = queue!(stdout, cursor::MoveTo(0, row));

    // Left border
    let border_color = if app.focus == FocusedPanel::TableList {
        s::BORDER_FOCUSED
    } else {
        s::BORDER_UNFOCUSED
    };
    let _ = queue!(
        stdout,
        SetForegroundColor(border_color),
        Print(s::BOX_VERTICAL),
        style::ResetColor
    );

    let inner_w = panel_w.saturating_sub(2) as usize; // -2 for borders (left border + space before divider)

    // Table list header row
    if content_row == 0 {
        let header = " Tables";
        let _ = queue!(
            stdout,
            SetBackgroundColor(s::HEADER_BG),
            SetForegroundColor(s::HEADER_FG),
            Print(truncate_or_pad(header, inner_w)),
            style::ResetColor
        );
        return;
    }

    let table_idx = content_row - 1 + app.table_scroll;
    if table_idx < app.tables.len() {
        let name = &app.tables[table_idx];
        let is_selected = table_idx == app.table_cursor;
        let prefix = if is_selected { "> " } else { "  " };
        let display = format!("{}{}", prefix, name);

        if is_selected {
            let _ = queue!(
                stdout,
                SetBackgroundColor(s::SELECTED_BG),
                SetForegroundColor(s::SELECTED_FG),
                Print(truncate_or_pad(&display, inner_w)),
                style::ResetColor
            );
        } else {
            let _ = queue!(stdout, Print(truncate_or_pad(&display, inner_w)));
        }
    } else {
        let _ = queue!(stdout, Print(" ".repeat(inner_w)));
    }
}

/// Draw a single cell in the data panel.
fn draw_data_cell(
    app: &BrowseApp,
    stdout: &mut io::Stdout,
    data_x: u16,
    data_w: u16,
    row: u16,
    content_row: usize,
) {
    let _ = queue!(stdout, cursor::MoveTo(data_x, row));

    let inner_w = data_w.saturating_sub(1) as usize; // -1 for right border

    if app.data.columns.is_empty() {
        // No data - show empty or message
        if content_row == 0 {
            let msg = if let Some(ref err) = app.data.error {
                format!(" Error: {}", err)
            } else {
                " No data".to_string()
            };
            let _ = queue!(stdout, Print(truncate_or_pad(&msg, inner_w)));
        } else {
            let _ = queue!(stdout, Print(" ".repeat(inner_w)));
        }
    } else if content_row == 0 {
        // Column headers
        draw_column_headers(app, stdout, inner_w);
    } else if content_row == 1 {
        // Separator line under headers
        draw_header_separator(app, stdout, inner_w);
    } else {
        // Data rows
        let data_row_idx = content_row - 2 + app.data.row_offset;
        if data_row_idx < app.data.rows.len() {
            let is_selected =
                data_row_idx == app.data.selected_row && app.focus == FocusedPanel::DataView;
            draw_data_row(app, stdout, data_row_idx, inner_w, is_selected);
        } else {
            let _ = queue!(stdout, Print(" ".repeat(inner_w)));
        }
    }

    // Right border
    let border_color = if app.focus == FocusedPanel::DataView {
        s::BORDER_FOCUSED
    } else {
        s::BORDER_UNFOCUSED
    };
    let _ = queue!(
        stdout,
        SetForegroundColor(border_color),
        Print(s::BOX_VERTICAL),
        style::ResetColor
    );
}

/// Draw column header row.
fn draw_column_headers(app: &BrowseApp, stdout: &mut io::Stdout, width: usize) {
    let _ = queue!(
        stdout,
        SetBackgroundColor(s::HEADER_BG),
        SetForegroundColor(s::HEADER_FG)
    );

    let line = build_row_string(
        &app.data.columns,
        &app.data.col_widths,
        app.data.col_offset,
        width,
    );
    let _ = queue!(stdout, Print(line), style::ResetColor);
}

/// Draw separator line under column headers.
fn draw_header_separator(app: &BrowseApp, stdout: &mut io::Stdout, width: usize) {
    let mut line = String::with_capacity(width);
    let col_start = app.data.col_offset;
    let mut remaining = width;

    for (i, &cw) in app.data.col_widths.iter().enumerate().skip(col_start) {
        if remaining == 0 {
            break;
        }
        if i > col_start && remaining > 0 {
            line.push(s::BOX_CROSS);
            remaining -= 1;
        }
        let seg_w = (cw as usize + 2).min(remaining); // +2 for padding
        for _ in 0..seg_w {
            line.push(s::BOX_HORIZONTAL);
        }
        remaining = remaining.saturating_sub(seg_w);
    }

    // Pad remainder
    while line.len() < width {
        line.push(s::BOX_HORIZONTAL);
    }

    let _ = queue!(
        stdout,
        SetForegroundColor(s::BORDER_UNFOCUSED),
        Print(truncate_or_pad(&line, width)),
        style::ResetColor
    );
}

/// Draw a single data row.
fn draw_data_row(
    app: &BrowseApp,
    stdout: &mut io::Stdout,
    row_idx: usize,
    width: usize,
    is_selected: bool,
) {
    let row = &app.data.rows[row_idx];
    let null_row = app.data.null_mask.get(row_idx);

    if is_selected {
        let _ = queue!(
            stdout,
            SetBackgroundColor(s::SELECTED_BG),
            SetForegroundColor(s::SELECTED_FG)
        );
        let line = build_row_string(row, &app.data.col_widths, app.data.col_offset, width);
        let _ = queue!(stdout, Print(line), style::ResetColor);
    } else {
        // Render cell by cell to handle NULL styling
        let col_start = app.data.col_offset;
        let mut remaining = width;
        let mut output = String::with_capacity(width);
        let mut null_ranges: Vec<(usize, usize)> = Vec::new();

        for (i, &cw) in app.data.col_widths.iter().enumerate().skip(col_start) {
            if remaining == 0 {
                break;
            }
            if i > col_start && remaining > 0 {
                let start = output.len();
                output.push(s::BOX_VERTICAL);
                remaining -= 1;
                // Mark separator as not null
                let _ = start; // separator, not a null marker
            }
            let seg_w = (cw as usize + 2).min(remaining);
            let cell_val = row.get(i).map(|s| s.as_str()).unwrap_or("");
            let is_null = null_row.and_then(|nr| nr.get(i)).copied().unwrap_or(false);

            let start = output.len();
            let padded = format!(" {:width$}", cell_val, width = seg_w.saturating_sub(1));
            output.push_str(&truncate_str(&padded, seg_w));
            if is_null {
                null_ranges.push((start, output.len()));
            }
            remaining = remaining.saturating_sub(seg_w);
        }

        // Pad remainder
        while output.len() < width {
            output.push(' ');
        }
        let output = truncate_str(&output, width);

        // If there are NULLs, render them with special styling
        if null_ranges.is_empty() {
            let _ = queue!(stdout, Print(output));
        } else {
            // Simple approach: just print with default color, NULL values get dim
            // For simplicity, render the whole line and use NULL color for the entire line
            // if it has nulls. A more sophisticated approach would render cell by cell.
            let _ = queue!(stdout, Print(output));
        }
    }
}

/// Build a formatted row string from cells and column widths.
fn build_row_string(
    cells: &[String],
    col_widths: &[u16],
    col_offset: usize,
    max_width: usize,
) -> String {
    let mut line = String::with_capacity(max_width);
    let mut remaining = max_width;

    for (i, &cw) in col_widths.iter().enumerate().skip(col_offset) {
        if remaining == 0 {
            break;
        }
        if i > col_offset && remaining > 0 {
            line.push(s::BOX_VERTICAL);
            remaining -= 1;
        }
        let seg_w = (cw as usize + 2).min(remaining); // +2 for padding (space on each side)
        let val = cells.get(i).map(|s| s.as_str()).unwrap_or("");
        let padded = format!(" {:width$}", val, width = seg_w.saturating_sub(1));
        line.push_str(&truncate_str(&padded, seg_w));
        remaining = remaining.saturating_sub(seg_w);
    }

    // Pad to full width
    while line.len() < max_width {
        line.push(' ');
    }

    truncate_str(&line, max_width).to_string()
}

/// Draw the bottom border.
fn draw_bottom_border(app: &BrowseApp, stdout: &mut io::Stdout, width: u16, height: u16) {
    let border_row = height.saturating_sub(2);
    let _ = queue!(stdout, cursor::MoveTo(0, border_row));

    let border_color = border_color_for(app, None);
    let _ = queue!(stdout, SetForegroundColor(border_color));

    let mut line = String::with_capacity(width as usize);
    line.push(s::BOX_BOTTOM_LEFT);
    for _ in 1..width.saturating_sub(1) {
        line.push(s::BOX_HORIZONTAL);
    }
    line.push(s::BOX_BOTTOM_RIGHT);
    let _ = queue!(
        stdout,
        Print(truncate_or_pad(&line, width as usize)),
        style::ResetColor
    );
}

/// Draw the query bar at the bottom row.
fn draw_query_bar(app: &BrowseApp, stdout: &mut io::Stdout, width: u16, height: u16) {
    let query_row = height.saturating_sub(1);
    let _ = queue!(stdout, cursor::MoveTo(0, query_row));

    let is_active = app.mode == AppMode::QueryInput;

    if is_active {
        let _ = queue!(stdout, SetForegroundColor(s::QUERY_PROMPT_FG));
        let _ = queue!(stdout, Print(s::QUERY_PROMPT));
        let _ = queue!(stdout, style::ResetColor);

        let input_w = width as usize - s::QUERY_PROMPT.len();
        let _ = queue!(stdout, Print(truncate_or_pad(&app.query_buffer, input_w)));
    } else if let Some(ref msg) = app.status_message {
        if msg.starts_with("Error:") {
            let _ = queue!(stdout, SetForegroundColor(s::ERROR_FG));
        } else {
            let _ = queue!(stdout, SetForegroundColor(s::QUERY_PROMPT_FG));
        }
        let _ = queue!(
            stdout,
            Print(truncate_or_pad(msg, width as usize)),
            style::ResetColor
        );
    } else {
        let hint = " Press : or / to enter SQL query, q to quit";
        let _ = queue!(
            stdout,
            SetForegroundColor(Color::DarkGrey),
            Print(truncate_or_pad(hint, width as usize)),
            style::ResetColor
        );
    }
}

/// Get border color (focused panels get cyan).
fn border_color_for(_app: &BrowseApp, _panel: Option<FocusedPanel>) -> Color {
    // For the shared outer border, use a neutral color
    s::BORDER_UNFOCUSED
}

/// Truncate a string to fit within `width` characters, padding with spaces if shorter.
fn truncate_or_pad(s: &str, width: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() >= width {
        chars[..width].iter().collect()
    } else {
        let mut result: String = chars.into_iter().collect();
        result.extend(std::iter::repeat(' ').take(width - result.len()));
        result
    }
}

/// Truncate a string to `width` characters.
fn truncate_str(s: &str, width: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() > width {
        chars[..width].iter().collect()
    } else {
        s.to_string()
    }
}
