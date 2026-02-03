# TUI Database Browser

RustQL includes a terminal-based database browser inspired by [Datasette](https://datasette.io/). It provides an interactive interface for exploring tables and running SQL queries without leaving the terminal.

## Building

The TUI is feature-gated behind `tui` and depends on [crossterm](https://crates.io/crates/crossterm):

```bash
# Build with TUI support
cargo build --features tui

# Release build
cargo build --release --features tui
```

The standard build (`cargo build` without `--features tui`) does not include the TUI or the crossterm dependency.

## Launching

From the RustQL REPL:

```
$ rustql mydb.sqlite
rustql> .browse
```

This enters the TUI browser. Press `q` or `Esc` to return to the REPL. The database connection is preserved across the transition.

If RustQL was built without the `tui` feature, `.browse` prints an error:

```
Error: .browse requires --features tui
```

## Layout

```
┌──────────────┬──────────────────────────────────────────┐
│ Tables       │ users (1,234 rows)              db.sqlite│  ← Status bar
├──────────────┼──────────────────────────────────────────┤
│ > users      │ id │ name    │ email          │ age      │  ← Column headers
│   posts      │────┼─────────┼────────────────┼──────────│
│   comments   │  1 │ Alice   │ alice@ex.com   │ 30       │  ← Data rows
│   sessions   │  2 │ Bob     │ bob@ex.com     │ 25       │
│              │  3 │ Charlie │ charlie@ex.com │ 35       │
├──────────────┴──────────────────────────────────────────┤
│ SQL> _                                                   │  ← Query bar
└─────────────────────────────────────────────────────────┘
```

The interface has three panels:

- **Table list** (left) — lists all tables in the database. The selected table is highlighted with `>`.
- **Data view** (main) — shows column headers and rows for the selected table or query result.
- **Query bar** (bottom) — for typing and executing arbitrary SQL.

The status bar at the top shows the current table name, row count, and database path.

## Key Bindings

### Navigation

| Key | Action |
|-----|--------|
| `j` / `↓` | Move down in focused panel |
| `k` / `↑` | Move up in focused panel |
| `h` / `←` | Scroll left in data view |
| `l` / `→` | Scroll right in data view |
| `g` | Jump to first row/table |
| `G` | Jump to last row/table |
| `Ctrl+D` | Page down (half screen) |
| `Ctrl+U` | Page up (half screen) |

### Focus

| Key | Action |
|-----|--------|
| `Tab` | Cycle focus: table list → data view → query bar |
| `Shift+Tab` | Cycle focus backward |

### Actions

| Key | Action |
|-----|--------|
| `Enter` | Select table (when table list is focused) |
| `:` or `/` | Enter SQL query mode |
| `r` | Refresh current view |
| `q` / `Esc` | Exit TUI, return to REPL |

### Query Input Mode

When the query bar is active (via `:`, `/`, or `Tab`):

| Key | Action |
|-----|--------|
| (any character) | Insert at cursor |
| `Backspace` | Delete character before cursor |
| `Delete` | Delete character at cursor |
| `←` / `→` | Move cursor |
| `Home` / `End` | Jump to start/end of input |
| `Ctrl+U` | Clear the entire line |
| `Enter` | Execute the SQL query |
| `Esc` | Cancel and return to normal mode |

Query results replace the data view. The status bar shows the number of rows returned or any error message.

## Examples

### Browse a database

```bash
cargo build --features tui
./target/debug/rustql chinook.db
```

```
rustql> .browse
```

Use `j`/`k` to navigate the table list, `Enter` to load a table, `h`/`l` to scroll wide tables horizontally.

### Run a query

Press `:` to activate the query bar, type SQL, and press `Enter`:

```
SQL> SELECT name, email FROM users WHERE age > 30
```

Results appear in the data view. Press `Esc` to return to normal navigation.

### Inspect a table then query it

1. Navigate to a table with `j`/`k` and press `Enter`
2. Browse the data with `j`/`k` (vertical) and `h`/`l` (horizontal)
3. Press `:` and type a filtered query
4. Press `r` to refresh after external changes

## Implementation Notes

- The TUI uses crossterm directly (no ratatui) for minimal dependencies.
- Rendering uses batched `queue!` writes with a single `flush()` per frame.
- Event polling uses a 50ms timeout to keep CPU usage low.
- Data is fetched in batches of 1,000 rows to avoid memory pressure on large tables.
- Column widths are auto-computed from data content, clamped to 40 characters max.
- NULL values are displayed as `NULL` in dim styling.
- The database connection is transferred into the TUI and returned when exiting, so no connection state is lost.

## Source Files

| File | Purpose |
|------|---------|
| `src/tui/mod.rs` | Entry point, `TerminalGuard`, `browse()` function |
| `src/tui/app.rs` | Application state (`BrowseApp`), table loading, query execution |
| `src/tui/data.rs` | Data fetching, caching, column width computation |
| `src/tui/render.rs` | Screen drawing (status bar, panels, query bar) |
| `src/tui/input.rs` | Keyboard event handling and dispatch |
| `src/tui/style.rs` | Color scheme and display constants |
