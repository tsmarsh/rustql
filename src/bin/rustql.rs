//! RustQL CLI - SQLite-compatible command-line interface
//!
//! Usage: rustql [DATABASE]
//!
//! Provides a shell interface compatible with SQLite's CLI for running
//! the TCL test suite.

use rustql::types::{ColumnType, StepResult};
use rustql::{
    sqlite3_close, sqlite3_column_count, sqlite3_column_name, sqlite3_column_text,
    sqlite3_column_type, sqlite3_finalize, sqlite3_initialize, sqlite3_open, sqlite3_prepare_v2,
    sqlite3_step, PreparedStmt, SqliteConnection,
};
use std::env;
use std::io::{self, BufRead, IsTerminal, Write};

/// Output mode for results
#[derive(Debug, Clone, Copy, PartialEq)]
enum OutputMode {
    List,   // Values separated by separator (default "|")
    Column, // Columnar output with headers
    Line,   // One value per line as "column = value"
    Csv,    // CSV format
    Tabs,   // Tab-separated
}

/// Shell state
struct Shell {
    conn: Option<Box<SqliteConnection>>,
    mode: OutputMode,
    separator: String,
    headers: bool,
    nullvalue: String,
    bail: bool,
    echo: bool,
    db_path: String,
}

impl Shell {
    fn new(db_path: &str) -> Result<Self, String> {
        // Initialize the library
        sqlite3_initialize().map_err(|e| format!("Failed to initialize: {}", e))?;

        // Open connection
        let conn = if db_path == ":memory:" || db_path.is_empty() {
            sqlite3_open(":memory:").map_err(|e| format!("Failed to open: {}", e))?
        } else {
            sqlite3_open(db_path).map_err(|e| format!("Failed to open: {}", e))?
        };

        Ok(Shell {
            conn: Some(conn),
            mode: OutputMode::List,
            separator: "|".to_string(),
            headers: false,
            nullvalue: String::new(),
            bail: false,
            echo: false,
            db_path: db_path.to_string(),
        })
    }

    fn conn_mut(&mut self) -> Result<&mut SqliteConnection, String> {
        self.conn
            .as_mut()
            .map(|b| b.as_mut())
            .ok_or_else(|| "No connection".to_string())
    }

    /// Process a dot command
    fn process_dot_command(&mut self, line: &str) -> Result<bool, String> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            return Ok(true);
        }

        let cmd = parts[0].to_lowercase();
        match cmd.as_str() {
            ".quit" | ".exit" | ".q" => {
                return Ok(false); // Signal to exit
            }
            ".help" => {
                self.print_help();
            }
            ".mode" => {
                if parts.len() > 1 {
                    self.mode = match parts[1].to_lowercase().as_str() {
                        "list" => OutputMode::List,
                        "column" => OutputMode::Column,
                        "line" => OutputMode::Line,
                        "csv" => OutputMode::Csv,
                        "tabs" => OutputMode::Tabs,
                        _ => {
                            eprintln!("Error: unknown mode: {}", parts[1]);
                            return Ok(true);
                        }
                    };
                } else {
                    println!("current mode: {:?}", self.mode);
                }
            }
            ".headers" => {
                if parts.len() > 1 {
                    self.headers = matches!(parts[1].to_lowercase().as_str(), "on" | "yes" | "1");
                } else {
                    println!("headers: {}", if self.headers { "on" } else { "off" });
                }
            }
            ".separator" => {
                if parts.len() > 1 {
                    self.separator = parts[1].to_string();
                } else {
                    println!("separator: {:?}", self.separator);
                }
            }
            ".nullvalue" => {
                if parts.len() > 1 {
                    self.nullvalue = parts[1..].join(" ");
                } else {
                    println!("nullvalue: {:?}", self.nullvalue);
                }
            }
            ".bail" => {
                if parts.len() > 1 {
                    self.bail = matches!(parts[1].to_lowercase().as_str(), "on" | "yes" | "1");
                } else {
                    println!("bail: {}", if self.bail { "on" } else { "off" });
                }
            }
            ".echo" => {
                if parts.len() > 1 {
                    self.echo = matches!(parts[1].to_lowercase().as_str(), "on" | "yes" | "1");
                } else {
                    println!("echo: {}", if self.echo { "on" } else { "off" });
                }
            }
            ".tables" => {
                self.list_tables()?;
            }
            ".schema" => {
                let table = parts.get(1).copied();
                self.show_schema(table)?;
            }
            ".databases" => {
                println!("main: {}", self.db_path);
            }
            ".dump" => {
                eprintln!("Error: .dump not yet implemented");
            }
            ".read" => {
                if parts.len() > 1 {
                    self.read_file(parts[1])?;
                } else {
                    eprintln!("Error: .read requires a filename");
                }
            }
            ".timeout" | ".width" | ".stats" | ".timer" => {
                // Accept but ignore these settings
            }
            ".version" => {
                println!("RustQL 0.1.0 (SQLite-compatible)");
            }
            _ => {
                eprintln!("Error: unknown command: {}", cmd);
            }
        }
        Ok(true)
    }

    fn print_help(&self) {
        println!(
            r#".bail ON|OFF           Stop after hitting an error
.databases             List databases
.dump ?TABLE?          Dump database in SQL text format
.echo ON|OFF           Turn command echo on or off
.exit                  Exit this program
.headers ON|OFF        Turn display of headers on or off
.help                  Show this message
.mode MODE             Set output mode (list, column, line, csv, tabs)
.nullvalue STRING      Use STRING in place of NULL values
.quit                  Exit this program
.read FILENAME         Execute SQL in FILENAME
.schema ?TABLE?        Show CREATE statements
.separator STRING      Set separator for list mode
.tables ?PATTERN?      List names of tables
.timeout MS            Set busy timeout
.version               Show version
.width NUM NUM ...     Set column widths for column mode"#
        );
    }

    fn list_tables(&mut self) -> Result<(), String> {
        self.execute_sql("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    }

    fn show_schema(&mut self, table: Option<&str>) -> Result<(), String> {
        let sql = if let Some(t) = table {
            format!(
                "SELECT sql FROM sqlite_master WHERE name='{}' AND sql IS NOT NULL",
                t
            )
        } else {
            "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name".to_string()
        };
        self.execute_sql(&sql)
    }

    fn read_file(&mut self, path: &str) -> Result<(), String> {
        let content = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let mut sql_buffer = String::new();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with("--") {
                continue;
            }
            if line.starts_with('.') && sql_buffer.is_empty() {
                self.process_dot_command(line)?;
            } else {
                if !sql_buffer.is_empty() {
                    sql_buffer.push(' ');
                }
                sql_buffer.push_str(line);
                if sql_buffer.trim_end().ends_with(';') {
                    self.execute_sql(&sql_buffer)?;
                    sql_buffer.clear();
                }
            }
        }
        if !sql_buffer.trim().is_empty() {
            self.execute_sql(&sql_buffer)?;
        }
        Ok(())
    }

    /// Execute SQL and display results
    fn execute_sql(&mut self, sql: &str) -> Result<(), String> {
        if self.echo {
            println!("{}", sql);
        }

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
                Err(e) => {
                    let msg = e.sqlite_errmsg();
                    eprintln!("Error: {}", msg);
                    if self.bail {
                        return Err(msg);
                    }
                    return Ok(());
                }
            };

            // If no statement was compiled, advance
            if stmt.sql().is_empty() {
                remaining = tail;
                continue;
            }

            // Get column info before stepping
            let col_count = sqlite3_column_count(&stmt);
            let col_names: Vec<String> = (0..col_count)
                .map(|i| sqlite3_column_name(&stmt, i).unwrap_or("").to_string())
                .collect();

            // Print headers if enabled
            if self.headers && col_count > 0 {
                self.print_header(&col_names);
            }

            // Step through results
            let mut row_count = 0;
            const MAX_ROWS: usize = 100000;
            let mut dynamic_col_count = col_count;

            loop {
                match sqlite3_step(&mut stmt) {
                    Ok(StepResult::Row) => {
                        // Re-check column count after step (may change for count_changes)
                        let new_col_count = sqlite3_column_count(&stmt);
                        if new_col_count > dynamic_col_count {
                            dynamic_col_count = new_col_count;
                        }
                        self.print_row(&stmt, dynamic_col_count, &col_names);
                        row_count += 1;
                        if row_count >= MAX_ROWS {
                            eprintln!("Warning: result limit reached");
                            break;
                        }
                    }
                    Ok(StepResult::Done) => break,
                    Err(e) => {
                        let msg = e.sqlite_errmsg();
                        eprintln!("Error: {}", msg);
                        if self.bail {
                            let _ = sqlite3_finalize(stmt);
                            return Err(msg);
                        }
                        break;
                    }
                    _ => break,
                }
            }

            let _ = sqlite3_finalize(stmt);
            remaining = tail;
        }

        Ok(())
    }

    fn print_header(&self, col_names: &[String]) {
        match self.mode {
            OutputMode::List => {
                println!("{}", col_names.join(&self.separator));
            }
            OutputMode::Csv => {
                println!("{}", col_names.join(","));
            }
            OutputMode::Tabs => {
                println!("{}", col_names.join("\t"));
            }
            _ => {}
        }
    }

    fn print_row(&self, stmt: &PreparedStmt, col_count: i32, col_names: &[String]) {
        let values: Vec<String> = (0..col_count).map(|i| self.format_value(stmt, i)).collect();

        match self.mode {
            OutputMode::List => {
                println!("{}", values.join(&self.separator));
            }
            OutputMode::Csv => {
                let csv_values: Vec<String> = values
                    .iter()
                    .map(|v| {
                        if v.contains(',') || v.contains('"') || v.contains('\n') {
                            format!("\"{}\"", v.replace('"', "\"\""))
                        } else {
                            v.clone()
                        }
                    })
                    .collect();
                println!("{}", csv_values.join(","));
            }
            OutputMode::Tabs => {
                println!("{}", values.join("\t"));
            }
            OutputMode::Line => {
                for (i, val) in values.iter().enumerate() {
                    let name = col_names.get(i as usize).map(|s| s.as_str()).unwrap_or("?");
                    println!("{} = {}", name, val);
                }
                println!();
            }
            OutputMode::Column => {
                println!("{}", values.join("  "));
            }
        }
    }

    fn format_value(&self, stmt: &PreparedStmt, col: i32) -> String {
        let col_type = sqlite3_column_type(stmt, col);

        match col_type {
            ColumnType::Null => self.nullvalue.clone(),
            _ => {
                let text = sqlite3_column_text(stmt, col);
                if text.is_empty() {
                    self.nullvalue.clone()
                } else {
                    text
                }
            }
        }
    }

    /// Run the shell in interactive mode
    fn run_interactive(&mut self) -> Result<(), String> {
        let stdin = io::stdin();
        let mut stdout = io::stdout();

        let mut sql_buffer = String::new();

        loop {
            // Print prompt
            let prompt = if sql_buffer.is_empty() {
                "rustql> "
            } else {
                "   ...> "
            };
            print!("{}", prompt);
            stdout.flush().ok();

            // Read line
            let mut line = String::new();
            match stdin.lock().read_line(&mut line) {
                Ok(0) => break, // EOF
                Ok(_) => {}
                Err(e) => return Err(e.to_string()),
            }

            let line = line.trim();

            // Handle dot commands (only when not in middle of SQL)
            if sql_buffer.is_empty() && line.starts_with('.') {
                if !self.process_dot_command(line)? {
                    break;
                }
                continue;
            }

            // Accumulate SQL
            if !sql_buffer.is_empty() {
                sql_buffer.push(' ');
            }
            sql_buffer.push_str(line);

            // Check if SQL is complete (ends with semicolon)
            if sql_buffer.trim_end().ends_with(';') {
                self.execute_sql(&sql_buffer)?;
                sql_buffer.clear();
            }
        }

        Ok(())
    }

    /// Run in non-interactive mode (read from stdin)
    fn run_stdin(&mut self) -> Result<(), String> {
        let stdin = io::stdin();
        let mut sql_buffer = String::new();

        for line in stdin.lock().lines() {
            let line = line.map_err(|e| e.to_string())?;
            let line = line.trim();

            // Handle dot commands
            if sql_buffer.is_empty() && line.starts_with('.') {
                self.process_dot_command(line)?;
                continue;
            }

            // Skip comments
            if line.starts_with("--") {
                continue;
            }

            // Accumulate SQL
            if !sql_buffer.is_empty() {
                sql_buffer.push(' ');
            }
            sql_buffer.push_str(line);

            // Execute when we see semicolon
            if sql_buffer.trim_end().ends_with(';') {
                self.execute_sql(&sql_buffer)?;
                sql_buffer.clear();
            }
        }

        // Execute any remaining SQL
        if !sql_buffer.trim().is_empty() {
            self.execute_sql(&sql_buffer)?;
        }

        Ok(())
    }
}

impl Drop for Shell {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let _ = sqlite3_close(conn);
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    // Parse arguments
    let mut db_path = ":memory:".to_string();
    let mut init_sql: Option<String> = None;
    let mut i = 1;

    while i < args.len() {
        match args[i].as_str() {
            "-help" | "--help" | "-?" => {
                println!("Usage: rustql [OPTIONS] [DATABASE]");
                println!();
                println!("Options:");
                println!("  -help              Show this help");
                println!("  -version           Show version");
                println!("  -cmd COMMAND       Run COMMAND before reading stdin");
                println!("  -init FILENAME     Read/execute FILENAME on startup");
                println!();
                println!("If DATABASE is omitted, an in-memory database is used.");
                return;
            }
            "-version" | "--version" => {
                println!("RustQL 0.1.0");
                return;
            }
            "-cmd" => {
                i += 1;
                if i < args.len() {
                    init_sql = Some(args[i].clone());
                }
            }
            "-init" => {
                i += 1;
                // Init file - would be handled later
            }
            arg if !arg.starts_with('-') => {
                db_path = arg.to_string();
            }
            _ => {
                // Ignore unknown flags for compatibility
            }
        }
        i += 1;
    }

    // Create shell
    let mut shell = match Shell::new(&db_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error: unable to open database \"{}\": {}", db_path, e);
            std::process::exit(1);
        }
    };

    // Run init SQL if provided
    if let Some(sql) = init_sql {
        if let Err(e) = shell.execute_sql(&sql) {
            eprintln!("Error in -cmd: {}", e);
            if shell.bail {
                std::process::exit(1);
            }
        }
    }

    // Determine if we're interactive
    let is_tty = std::io::stdin().is_terminal();

    let result = if is_tty {
        shell.run_interactive()
    } else {
        shell.run_stdin()
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
