//! Database connection management
//!
//! This module implements the sqlite3 connection type and related functions.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

use crate::error::{Error, ErrorCode, Result};
use crate::schema::{Encoding, Schema};
use crate::types::{OpenFlags, RowId};

use super::config::{sqlite3_initialize, DbConfigOption};

// ============================================================================
// Transaction State
// ============================================================================

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionState {
    /// No active transaction (autocommit mode)
    #[default]
    None,
    /// Read transaction active
    Read,
    /// Write transaction active
    Write,
}

/// Auto-vacuum mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum AutoVacuum {
    /// No auto-vacuum
    #[default]
    None = 0,
    /// Full auto-vacuum after each transaction
    Full = 1,
    /// Incremental vacuum on demand
    Incremental = 2,
}

/// Synchronous mode (PRAGMA synchronous)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum SafetyLevel {
    /// OFF - no syncs
    Off = 0,
    /// NORMAL - sync at critical moments
    #[default]
    Normal = 1,
    /// FULL - sync after each write
    Full = 2,
    /// EXTRA - sync more frequently
    Extra = 3,
}

// ============================================================================
// Attached Database
// ============================================================================

/// Information about an attached database
#[derive(Debug)]
pub struct DbInfo {
    /// Schema name ("main", "temp", or attached name)
    pub name: String,
    /// Path to database file
    pub path: Option<String>,
    /// Schema for this database
    pub schema: Option<Arc<RwLock<Schema>>>,
    /// Safety level
    pub safety_level: SafetyLevel,
    /// Is database busy (exclusive lock held)
    pub busy: bool,
}

impl DbInfo {
    /// Create a new database info
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            path: None,
            schema: Some(Arc::new(RwLock::new(Schema::new()))),
            safety_level: SafetyLevel::Normal,
            busy: false,
        }
    }
}

// ============================================================================
// Callback Types
// ============================================================================

/// Busy handler callback type
pub type BusyHandler = Box<dyn Fn(i32) -> bool + Send + Sync>;

/// Progress handler callback type
pub type ProgressHandler = Box<dyn Fn() -> bool + Send + Sync>;

/// Trace callback type
pub type TraceCallback = Box<dyn Fn(&str) + Send + Sync>;

/// Profile callback type
pub type ProfileCallback = Box<dyn Fn(&str, u64) + Send + Sync>;

/// Commit hook callback type
pub type CommitHook = Box<dyn Fn() -> bool + Send + Sync>;

/// Rollback hook callback type
pub type RollbackHook = Box<dyn Fn() + Send + Sync>;

/// Update hook callback type
pub type UpdateHook = Box<dyn Fn(i32, &str, &str, i64) + Send + Sync>;

/// Authorizer callback type
/// Returns SQLITE_OK (0), SQLITE_DENY (1), or SQLITE_IGNORE (2)
pub type Authorizer =
    Box<dyn Fn(i32, Option<&str>, Option<&str>, Option<&str>, Option<&str>) -> i32 + Send + Sync>;

// ============================================================================
// Connection
// ============================================================================

/// SQLite database connection (sqlite3)
pub struct SqliteConnection {
    /// Attached databases (main, temp, and user-attached)
    pub dbs: Vec<DbInfo>,
    /// Open flags
    pub flags: OpenFlags,
    /// Last error code
    pub err_code: ErrorCode,
    /// Extended error code
    pub err_code_ext: i32,
    /// Error message
    pub err_msg: Option<String>,
    /// Busy handler
    pub busy_handler: Option<BusyHandler>,
    /// Busy timeout in milliseconds
    pub busy_timeout: i32,
    /// Trace callback
    pub trace: Option<TraceCallback>,
    /// Profile callback
    pub profile: Option<ProfileCallback>,
    /// Progress handler
    pub progress_handler: Option<ProgressHandler>,
    /// Progress handler interval (VDBE instructions)
    pub progress_interval: i32,
    /// Authorizer callback
    pub authorizer: Option<Authorizer>,
    /// Commit hook
    pub commit_hook: Option<CommitHook>,
    /// Rollback hook
    pub rollback_hook: Option<RollbackHook>,
    /// Update hook
    pub update_hook: Option<UpdateHook>,
    /// Registered collations
    pub collations: HashMap<String, Arc<dyn Fn(&str, &str) -> std::cmp::Ordering + Send + Sync>>,
    /// Auto-vacuum mode
    pub auto_vacuum: AutoVacuum,
    /// Transaction state
    pub transaction_state: TransactionState,
    /// Savepoint stack
    pub savepoints: Vec<String>,
    /// Total changes since connection opened
    pub total_changes: AtomicI64,
    /// Changes from last statement
    pub changes: AtomicI64,
    /// Last insert rowid
    pub last_insert_rowid: AtomicI64,
    /// Interrupt flag
    pub interrupted: AtomicBool,
    /// In autocommit mode
    pub autocommit: AtomicBool,
    /// Text encoding
    pub encoding: Encoding,
    /// Per-connection configuration flags
    pub db_config: DbConfigFlags,
}

/// Per-connection configuration flags
#[derive(Debug, Default)]
pub struct DbConfigFlags {
    /// Enable foreign key constraints
    pub enable_fkey: bool,
    /// Enable triggers
    pub enable_trigger: bool,
    /// Enable views
    pub enable_view: bool,
    /// Defensive mode (restrict dangerous operations)
    pub defensive: bool,
    /// Allow writing to sqlite_schema
    pub writable_schema: bool,
    /// Enable double-quoted string literals in DML
    pub dqs_dml: bool,
    /// Enable double-quoted string literals in DDL
    pub dqs_ddl: bool,
    /// Trust schema
    pub trusted_schema: bool,
    /// Legacy ALTER TABLE behavior
    pub legacy_alter_table: bool,
    /// Legacy file format
    pub legacy_file_format: bool,
    /// No checkpoint on close
    pub no_ckpt_on_close: bool,
}

impl Default for SqliteConnection {
    fn default() -> Self {
        Self::new()
    }
}

impl SqliteConnection {
    /// Create a new connection (internal)
    pub fn new() -> Self {
        let mut conn = Self {
            dbs: Vec::new(),
            flags: OpenFlags::empty(),
            err_code: ErrorCode::Ok,
            err_code_ext: 0,
            err_msg: None,
            busy_handler: None,
            busy_timeout: 0,
            trace: None,
            profile: None,
            progress_handler: None,
            progress_interval: 0,
            authorizer: None,
            commit_hook: None,
            rollback_hook: None,
            update_hook: None,
            collations: HashMap::new(),
            auto_vacuum: AutoVacuum::None,
            transaction_state: TransactionState::None,
            savepoints: Vec::new(),
            total_changes: AtomicI64::new(0),
            changes: AtomicI64::new(0),
            last_insert_rowid: AtomicI64::new(0),
            interrupted: AtomicBool::new(false),
            autocommit: AtomicBool::new(true),
            encoding: Encoding::Utf8,
            db_config: DbConfigFlags {
                enable_fkey: false,
                enable_trigger: true,
                enable_view: true,
                defensive: false,
                writable_schema: false,
                dqs_dml: true,
                dqs_ddl: true,
                trusted_schema: true,
                legacy_alter_table: false,
                legacy_file_format: false,
                no_ckpt_on_close: false,
            },
        };

        // Add main and temp databases
        conn.dbs.push(DbInfo::new("main"));
        conn.dbs.push(DbInfo::new("temp"));

        // Register built-in collations
        conn.register_builtin_collations();

        conn
    }

    /// Register built-in collation sequences
    fn register_builtin_collations(&mut self) {
        // BINARY - bytewise comparison (default)
        self.collations
            .insert("BINARY".to_string(), Arc::new(|a: &str, b: &str| a.cmp(b)));

        // NOCASE - case-insensitive for ASCII
        self.collations.insert(
            "NOCASE".to_string(),
            Arc::new(|a: &str, b: &str| a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase())),
        );

        // RTRIM - ignore trailing spaces
        self.collations.insert(
            "RTRIM".to_string(),
            Arc::new(|a: &str, b: &str| a.trim_end().cmp(b.trim_end())),
        );
    }

    /// Set an error on this connection
    pub fn set_error(&mut self, code: ErrorCode, msg: &str) {
        self.err_code = code;
        self.err_msg = Some(msg.to_string());
    }

    /// Clear any pending error
    pub fn clear_error(&mut self) {
        self.err_code = ErrorCode::Ok;
        self.err_code_ext = 0;
        self.err_msg = None;
    }

    /// Find database by name
    pub fn find_db(&self, name: &str) -> Option<&DbInfo> {
        self.dbs
            .iter()
            .find(|db| db.name.eq_ignore_ascii_case(name))
    }

    /// Find database by name (mutable)
    pub fn find_db_mut(&mut self, name: &str) -> Option<&mut DbInfo> {
        self.dbs
            .iter_mut()
            .find(|db| db.name.eq_ignore_ascii_case(name))
    }

    /// Get the main database
    pub fn main_db(&self) -> &DbInfo {
        &self.dbs[0]
    }

    /// Get autocommit state
    pub fn get_autocommit(&self) -> bool {
        self.autocommit.load(Ordering::SeqCst)
    }
}

// ============================================================================
// Open/Close Functions
// ============================================================================

/// sqlite3_open - Open a database connection
///
/// Opens the database file specified and returns a connection handle.
/// If the file does not exist, it is created.
pub fn sqlite3_open(filename: &str) -> Result<Box<SqliteConnection>> {
    sqlite3_open_v2(filename, OpenFlags::READWRITE | OpenFlags::CREATE, None)
}

/// sqlite3_open_v2 - Open database with flags
///
/// Extended version of sqlite3_open that accepts flags and VFS name.
pub fn sqlite3_open_v2(
    filename: &str,
    flags: OpenFlags,
    _vfs_name: Option<&str>,
) -> Result<Box<SqliteConnection>> {
    // Ensure library is initialized
    sqlite3_initialize()?;

    // Parse URI if enabled
    let (path, uri_flags) = if flags.contains(OpenFlags::URI) {
        parse_uri(filename)?
    } else {
        (filename.to_string(), OpenFlags::empty())
    };

    let final_flags = flags | uri_flags;

    // Create the connection
    let mut conn = Box::new(SqliteConnection::new());
    conn.flags = final_flags;

    // Set up main database path
    if let Some(main_db) = conn.find_db_mut("main") {
        if path == ":memory:" || final_flags.contains(OpenFlags::MEMORY) {
            main_db.path = None; // In-memory database
        } else if !path.is_empty() {
            main_db.path = Some(path);
        }
    }

    // TODO: Actually open the database file via Btree
    // For now, we just set up the connection structure

    Ok(conn)
}

/// sqlite3_open16 - Open database with UTF-16 filename
pub fn sqlite3_open16(filename: &[u16]) -> Result<Box<SqliteConnection>> {
    let filename = String::from_utf16_lossy(filename);
    sqlite3_open(&filename)
}

/// sqlite3_close - Close a database connection
///
/// Closes the database connection and releases all resources.
/// Returns SQLITE_BUSY if there are unfinalized statements.
pub fn sqlite3_close(mut conn: Box<SqliteConnection>) -> Result<()> {
    // Check for pending statements
    // In a full implementation, we'd track active statements

    // Close all databases
    for db in &mut conn.dbs {
        // Close btree connections
        db.schema = None;
    }

    Ok(())
}

/// sqlite3_close_v2 - Close connection with deferred cleanup
///
/// Like sqlite3_close but marks the connection as unusable and
/// defers actual cleanup until all statements are finalized.
pub fn sqlite3_close_v2(conn: Box<SqliteConnection>) -> Result<()> {
    // For now, same as sqlite3_close
    sqlite3_close(conn)
}

// ============================================================================
// Error Functions
// ============================================================================

/// sqlite3_errcode - Get error code
pub fn sqlite3_errcode(conn: &SqliteConnection) -> ErrorCode {
    conn.err_code
}

/// sqlite3_extended_errcode - Get extended error code
pub fn sqlite3_extended_errcode(conn: &SqliteConnection) -> i32 {
    if conn.err_code_ext != 0 {
        conn.err_code_ext
    } else {
        conn.err_code as i32
    }
}

/// sqlite3_errmsg - Get error message
pub fn sqlite3_errmsg(conn: &SqliteConnection) -> &str {
    conn.err_msg
        .as_deref()
        .unwrap_or_else(|| sqlite3_errstr(conn.err_code))
}

/// sqlite3_errmsg16 - Get error message as UTF-16
pub fn sqlite3_errmsg16(conn: &SqliteConnection) -> Vec<u16> {
    sqlite3_errmsg(conn).encode_utf16().collect()
}

/// sqlite3_errstr - Get error string for code
pub fn sqlite3_errstr(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::Ok => "not an error",
        ErrorCode::Error => "SQL logic error",
        ErrorCode::Internal => "internal error",
        ErrorCode::Perm => "access permission denied",
        ErrorCode::Abort => "query aborted",
        ErrorCode::Busy => "database is locked",
        ErrorCode::Locked => "database table is locked",
        ErrorCode::NoMem => "out of memory",
        ErrorCode::ReadOnly => "attempt to write a readonly database",
        ErrorCode::Interrupt => "interrupted",
        ErrorCode::IoErr => "disk I/O error",
        ErrorCode::Corrupt => "database disk image is malformed",
        ErrorCode::NotFound => "unknown operation",
        ErrorCode::Full => "database or disk is full",
        ErrorCode::CantOpen => "unable to open database file",
        ErrorCode::Protocol => "locking protocol",
        ErrorCode::Empty => "empty",
        ErrorCode::Schema => "database schema has changed",
        ErrorCode::TooBig => "string or blob too big",
        ErrorCode::Constraint => "constraint failed",
        ErrorCode::Mismatch => "datatype mismatch",
        ErrorCode::Misuse => "bad parameter or other API misuse",
        ErrorCode::NoLfs => "large file support is disabled",
        ErrorCode::Auth => "authorization denied",
        ErrorCode::Format => "file format error",
        ErrorCode::Range => "column index out of range",
        ErrorCode::NotADb => "file is not a database",
        ErrorCode::Notice => "notification message",
        ErrorCode::Warning => "warning message",
        ErrorCode::Row => "another row available",
        ErrorCode::Done => "no more rows available",
    }
}

// ============================================================================
// State and Info Functions
// ============================================================================

/// sqlite3_changes - Rows changed by last statement
pub fn sqlite3_changes(conn: &SqliteConnection) -> i32 {
    conn.changes.load(Ordering::SeqCst) as i32
}

/// sqlite3_changes64 - Rows changed by last statement (64-bit)
pub fn sqlite3_changes64(conn: &SqliteConnection) -> i64 {
    conn.changes.load(Ordering::SeqCst)
}

/// sqlite3_total_changes - Total rows changed since connection opened
pub fn sqlite3_total_changes(conn: &SqliteConnection) -> i32 {
    conn.total_changes.load(Ordering::SeqCst) as i32
}

/// sqlite3_total_changes64 - Total rows changed (64-bit)
pub fn sqlite3_total_changes64(conn: &SqliteConnection) -> i64 {
    conn.total_changes.load(Ordering::SeqCst)
}

/// sqlite3_last_insert_rowid - Get last insert rowid
pub fn sqlite3_last_insert_rowid(conn: &SqliteConnection) -> RowId {
    conn.last_insert_rowid.load(Ordering::SeqCst)
}

/// sqlite3_set_last_insert_rowid - Set last insert rowid
pub fn sqlite3_set_last_insert_rowid(conn: &SqliteConnection, rowid: RowId) {
    conn.last_insert_rowid.store(rowid, Ordering::SeqCst);
}

/// sqlite3_get_autocommit - Check autocommit mode
pub fn sqlite3_get_autocommit(conn: &SqliteConnection) -> bool {
    conn.get_autocommit()
}

/// sqlite3_db_filename - Get filename for database
pub fn sqlite3_db_filename<'a>(conn: &'a SqliteConnection, db_name: &str) -> Option<&'a str> {
    conn.find_db(db_name).and_then(|db| db.path.as_deref())
}

/// sqlite3_db_readonly - Check if database is read-only
pub fn sqlite3_db_readonly(conn: &SqliteConnection, db_name: &str) -> i32 {
    if conn.find_db(db_name).is_none() {
        return -1; // Database not found
    }
    if conn.flags.contains(OpenFlags::READONLY) {
        1
    } else {
        0
    }
}

// ============================================================================
// Interrupt and Busy
// ============================================================================

/// sqlite3_interrupt - Interrupt a long-running query
pub fn sqlite3_interrupt(conn: &SqliteConnection) {
    conn.interrupted.store(true, Ordering::SeqCst);
}

/// sqlite3_is_interrupted - Check if interrupted
pub fn sqlite3_is_interrupted(conn: &SqliteConnection) -> bool {
    conn.interrupted.load(Ordering::SeqCst)
}

/// sqlite3_busy_handler - Set busy handler callback
pub fn sqlite3_busy_handler(
    conn: &mut SqliteConnection,
    handler: Option<BusyHandler>,
) -> Result<()> {
    conn.busy_handler = handler;
    Ok(())
}

/// sqlite3_busy_timeout - Set busy timeout
pub fn sqlite3_busy_timeout(conn: &mut SqliteConnection, ms: i32) -> Result<()> {
    conn.busy_timeout = ms;
    // Clear any custom busy handler when setting timeout
    conn.busy_handler = None;
    Ok(())
}

// ============================================================================
// Callbacks
// ============================================================================

/// sqlite3_trace - Set trace callback (deprecated)
pub fn sqlite3_trace(conn: &mut SqliteConnection, callback: Option<TraceCallback>) {
    conn.trace = callback;
}

/// sqlite3_profile - Set profile callback (deprecated)
pub fn sqlite3_profile(conn: &mut SqliteConnection, callback: Option<ProfileCallback>) {
    conn.profile = callback;
}

/// sqlite3_progress_handler - Set progress handler
pub fn sqlite3_progress_handler(
    conn: &mut SqliteConnection,
    n_ops: i32,
    handler: Option<ProgressHandler>,
) {
    conn.progress_interval = n_ops;
    conn.progress_handler = handler;
}

/// sqlite3_set_authorizer - Set authorizer callback
pub fn sqlite3_set_authorizer(
    conn: &mut SqliteConnection,
    authorizer: Option<Authorizer>,
) -> Result<()> {
    conn.authorizer = authorizer;
    Ok(())
}

/// sqlite3_commit_hook - Set commit hook
pub fn sqlite3_commit_hook(
    conn: &mut SqliteConnection,
    hook: Option<CommitHook>,
) -> Option<CommitHook> {
    std::mem::replace(&mut conn.commit_hook, hook)
}

/// sqlite3_rollback_hook - Set rollback hook
pub fn sqlite3_rollback_hook(
    conn: &mut SqliteConnection,
    hook: Option<RollbackHook>,
) -> Option<RollbackHook> {
    std::mem::replace(&mut conn.rollback_hook, hook)
}

/// sqlite3_update_hook - Set update hook
pub fn sqlite3_update_hook(
    conn: &mut SqliteConnection,
    hook: Option<UpdateHook>,
) -> Option<UpdateHook> {
    std::mem::replace(&mut conn.update_hook, hook)
}

// ============================================================================
// Database Configuration
// ============================================================================

/// sqlite3_db_config - Configure connection
pub fn sqlite3_db_config(
    conn: &mut SqliteConnection,
    option: DbConfigOption,
    value: i32,
) -> Result<i32> {
    let old_value = match option {
        DbConfigOption::EnableFKey => {
            let old = conn.db_config.enable_fkey as i32;
            if value >= 0 {
                conn.db_config.enable_fkey = value != 0;
            }
            old
        }
        DbConfigOption::EnableTrigger => {
            let old = conn.db_config.enable_trigger as i32;
            if value >= 0 {
                conn.db_config.enable_trigger = value != 0;
            }
            old
        }
        DbConfigOption::EnableView => {
            let old = conn.db_config.enable_view as i32;
            if value >= 0 {
                conn.db_config.enable_view = value != 0;
            }
            old
        }
        DbConfigOption::Defensive => {
            let old = conn.db_config.defensive as i32;
            if value >= 0 {
                conn.db_config.defensive = value != 0;
            }
            old
        }
        DbConfigOption::WritableSchema => {
            let old = conn.db_config.writable_schema as i32;
            if value >= 0 {
                conn.db_config.writable_schema = value != 0;
            }
            old
        }
        DbConfigOption::DqsDml => {
            let old = conn.db_config.dqs_dml as i32;
            if value >= 0 {
                conn.db_config.dqs_dml = value != 0;
            }
            old
        }
        DbConfigOption::DqsDdl => {
            let old = conn.db_config.dqs_ddl as i32;
            if value >= 0 {
                conn.db_config.dqs_ddl = value != 0;
            }
            old
        }
        DbConfigOption::TrustedSchema => {
            let old = conn.db_config.trusted_schema as i32;
            if value >= 0 {
                conn.db_config.trusted_schema = value != 0;
            }
            old
        }
        DbConfigOption::LegacyAlterTable => {
            let old = conn.db_config.legacy_alter_table as i32;
            if value >= 0 {
                conn.db_config.legacy_alter_table = value != 0;
            }
            old
        }
        DbConfigOption::LegacyFileFormat => {
            let old = conn.db_config.legacy_file_format as i32;
            if value >= 0 {
                conn.db_config.legacy_file_format = value != 0;
            }
            old
        }
        DbConfigOption::NoCkptOnClose => {
            let old = conn.db_config.no_ckpt_on_close as i32;
            if value >= 0 {
                conn.db_config.no_ckpt_on_close = value != 0;
            }
            old
        }
        _ => {
            return Err(Error::new(ErrorCode::Error));
        }
    };

    Ok(old_value)
}

// ============================================================================
// URI Parsing
// ============================================================================

/// Parse a URI filename
fn parse_uri(uri: &str) -> Result<(String, OpenFlags)> {
    // Simple URI parsing
    // Full format: file:path?mode=ro&cache=shared
    let mut flags = OpenFlags::empty();

    let path = if uri.starts_with("file:") {
        let rest = &uri[5..];
        if let Some(query_start) = rest.find('?') {
            let (path_part, query) = rest.split_at(query_start);
            let query = &query[1..]; // Skip '?'

            // Parse query parameters
            for param in query.split('&') {
                if let Some((key, value)) = param.split_once('=') {
                    match key {
                        "mode" => match value {
                            "ro" => flags.insert(OpenFlags::READONLY),
                            "rw" => flags.insert(OpenFlags::READWRITE),
                            "rwc" => {
                                flags.insert(OpenFlags::READWRITE);
                                flags.insert(OpenFlags::CREATE);
                            }
                            "memory" => flags.insert(OpenFlags::MEMORY),
                            _ => {}
                        },
                        "cache" => match value {
                            "shared" => flags.insert(OpenFlags::SHAREDCACHE),
                            "private" => flags.insert(OpenFlags::PRIVATECACHE),
                            _ => {}
                        },
                        "nolock" => {
                            // Handle nolock parameter if needed
                        }
                        "immutable" => {
                            // Handle immutable parameter if needed
                        }
                        _ => {}
                    }
                }
            }
            path_part.to_string()
        } else {
            rest.to_string()
        }
    } else {
        uri.to_string()
    };

    Ok((path, flags))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_memory() {
        let conn = sqlite3_open(":memory:").unwrap();
        assert_eq!(conn.dbs.len(), 2); // main and temp
        assert_eq!(conn.dbs[0].name, "main");
    }

    #[test]
    fn test_error_messages() {
        assert_eq!(sqlite3_errstr(ErrorCode::Ok), "not an error");
        assert_eq!(sqlite3_errstr(ErrorCode::Error), "SQL logic error");
        assert_eq!(sqlite3_errstr(ErrorCode::Busy), "database is locked");
    }

    #[test]
    fn test_parse_uri() {
        let (path, flags) = parse_uri("file:test.db?mode=ro").unwrap();
        assert_eq!(path, "test.db");
        assert!(flags.contains(OpenFlags::READONLY));

        let (path, flags) = parse_uri("file:test.db?mode=rwc").unwrap();
        assert_eq!(path, "test.db");
        assert!(flags.contains(OpenFlags::READWRITE));
        assert!(flags.contains(OpenFlags::CREATE));
    }

    #[test]
    fn test_db_config() {
        let mut conn = SqliteConnection::new();

        // Check default
        assert!(!conn.db_config.enable_fkey);

        // Enable foreign keys
        let old = sqlite3_db_config(&mut conn, DbConfigOption::EnableFKey, 1).unwrap();
        assert_eq!(old, 0);
        assert!(conn.db_config.enable_fkey);

        // Query without changing
        let current = sqlite3_db_config(&mut conn, DbConfigOption::EnableFKey, -1).unwrap();
        assert_eq!(current, 1);
    }

    #[test]
    fn test_interrupt() {
        let conn = SqliteConnection::new();
        assert!(!sqlite3_is_interrupted(&conn));

        sqlite3_interrupt(&conn);
        assert!(sqlite3_is_interrupted(&conn));
    }

    #[test]
    fn test_autocommit() {
        let conn = SqliteConnection::new();
        assert!(sqlite3_get_autocommit(&conn));
    }
}
