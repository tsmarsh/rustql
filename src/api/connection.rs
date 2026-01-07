//! Database connection management
//!
//! This module implements the sqlite3 connection type and related functions.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

use crate::error::{Error, ErrorCode, Result};
use crate::functions::{get_aggregate_function, get_scalar_function, AggregateInfo, ScalarFunc};
use crate::schema::{Encoding, Schema};
use crate::storage::btree::{Btree, BtreeOpenFlags};
use crate::storage::pager::{JournalMode, LockingMode, DEFAULT_PAGE_SIZE};
use crate::types::{
    AccessFlags, DbOffset, DeviceCharacteristics, LockLevel, OpenFlags, RowId, SyncFlags, Value,
    Vfs, VfsFile,
};

use super::config::{sqlite3_initialize, DbConfigOption};

// ============================================================================
// Stub VFS Implementation
// ============================================================================

/// Stub VFS file for pager/btree (temporary until full VFS integration)
pub struct StubVfsFile;

impl VfsFile for StubVfsFile {
    fn read(&mut self, buf: &mut [u8], _offset: DbOffset) -> Result<usize> {
        buf.fill(0);
        Ok(buf.len())
    }

    fn write(&mut self, _buf: &[u8], _offset: DbOffset) -> Result<()> {
        Ok(())
    }

    fn truncate(&mut self, _size: DbOffset) -> Result<()> {
        Ok(())
    }

    fn sync(&mut self, _flags: SyncFlags) -> Result<()> {
        Ok(())
    }

    fn file_size(&self) -> Result<DbOffset> {
        Ok(0)
    }

    fn lock(&mut self, _level: LockLevel) -> Result<()> {
        Ok(())
    }

    fn unlock(&mut self, _level: LockLevel) -> Result<()> {
        Ok(())
    }

    fn check_reserved_lock(&self) -> Result<bool> {
        Ok(false)
    }

    fn sector_size(&self) -> i32 {
        4096
    }

    fn device_characteristics(&self) -> DeviceCharacteristics {
        DeviceCharacteristics::empty()
    }
}

/// Stub VFS for btree/pager (temporary until full VFS integration)
pub struct StubVfs;

impl Vfs for StubVfs {
    type File = StubVfsFile;

    fn open(&self, _path: &str, _flags: OpenFlags) -> Result<Self::File> {
        Ok(StubVfsFile)
    }

    fn delete(&self, _path: &str, _sync_dir: bool) -> Result<()> {
        Ok(())
    }

    fn access(&self, _path: &str, _flags: AccessFlags) -> Result<bool> {
        Ok(true)
    }

    fn full_pathname(&self, path: &str) -> Result<String> {
        Ok(path.to_string())
    }

    fn randomness(&self, buf: &mut [u8]) -> i32 {
        for (i, byte) in buf.iter_mut().enumerate() {
            *byte = (i as u8).wrapping_mul(17).wrapping_add(3);
        }
        buf.len() as i32
    }

    fn sleep(&self, microseconds: i32) -> i32 {
        std::thread::sleep(std::time::Duration::from_micros(microseconds as u64));
        microseconds
    }

    fn current_time(&self) -> f64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        // Convert to Julian day number (days since -4713-11-24 12:00:00)
        2440587.5 + (duration.as_secs_f64() / 86400.0)
    }

    fn current_time_i64(&self) -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        duration.as_millis() as i64
    }
}

// ============================================================================
// Transaction State
// ============================================================================

// ============================================================================
// Authorization
// ============================================================================

/// Authorization actions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthAction {
    CreateIndex = 1,
    CreateTable = 2,
    CreateTempIndex = 3,
    CreateTempTable = 4,
    CreateTempTrigger = 5,
    CreateTempView = 6,
    CreateTrigger = 7,
    CreateView = 8,
    Delete = 9,
    DropIndex = 10,
    DropTable = 11,
    DropTempIndex = 12,
    DropTempTable = 13,
    DropTempTrigger = 14,
    DropTempView = 15,
    DropTrigger = 16,
    DropView = 17,
    Insert = 18,
    Pragma = 19,
    Read = 20,
    Select = 21,
    Transaction = 22,
    Update = 23,
    Attach = 24,
    Detach = 25,
    AlterTable = 26,
    Reindex = 27,
    Analyze = 28,
    CreateVtable = 29,
    DropVtable = 30,
    Function = 31,
    Savepoint = 32,
    Recursive = 33,
}

/// Authorization callback result
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthResult {
    Ok = 0,
    Deny = 1,
    Ignore = 2,
}

impl AuthResult {
    fn from_code(code: i32) -> Self {
        match code {
            1 => AuthResult::Deny,
            2 => AuthResult::Ignore,
            _ => AuthResult::Ok,
        }
    }
}

// ============================================================================
// Function Registry
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FuncKey {
    pub name: String,
    pub n_arg: i32,
    pub encoding: Encoding,
}

impl FuncKey {
    pub fn new(name: &str, n_arg: i32, encoding: Encoding) -> Self {
        Self {
            name: name.to_lowercase(),
            n_arg,
            encoding,
        }
    }
}

pub struct AggregateContext;

pub type AggStep = fn(&mut AggregateContext, &[Value]) -> Result<()>;
pub type AggFinal = fn(&AggregateContext) -> Result<Value>;

#[derive(Clone)]
pub struct FunctionDef {
    pub name: String,
    pub n_arg: i32,
    pub x_func: Option<ScalarFunc>,
    pub x_step: Option<AggStep>,
    pub x_final: Option<AggFinal>,
    pub x_inverse: Option<AggStep>,
    pub x_value: Option<AggFinal>,
}

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
    /// Page size in bytes
    pub page_size: u32,
    /// Cache size in pages
    pub cache_size: i64,
    /// Journal mode
    pub journal_mode: JournalMode,
    /// Locking mode
    pub locking_mode: LockingMode,
    /// B-tree storage
    pub btree: Option<Arc<Btree>>,
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
            page_size: DEFAULT_PAGE_SIZE,
            cache_size: 0,
            journal_mode: JournalMode::Delete,
            locking_mode: LockingMode::Normal,
            btree: None,
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

/// Collation needed callback type
pub type CollationNeeded = Box<dyn Fn(&mut SqliteConnection, &str) + Send + Sync>;

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
    /// Function registry (user-defined)
    pub functions: HashMap<FuncKey, FunctionDef>,
    /// Commit hook
    pub commit_hook: Option<CommitHook>,
    /// Rollback hook
    pub rollback_hook: Option<RollbackHook>,
    /// Update hook
    pub update_hook: Option<UpdateHook>,
    /// Registered collations
    pub collations: HashMap<String, Arc<dyn Fn(&str, &str) -> std::cmp::Ordering + Send + Sync>>,
    /// Collation needed callback
    pub collation_needed: Option<CollationNeeded>,
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
    /// Enable recursive triggers
    pub recursive_triggers: bool,
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
            functions: HashMap::new(),
            commit_hook: None,
            rollback_hook: None,
            update_hook: None,
            collations: HashMap::new(),
            collation_needed: None,
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
                recursive_triggers: false,
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
        self.create_collation("BINARY", |a, b| a.cmp(b));

        // NOCASE - case-insensitive for ASCII
        self.create_collation("NOCASE", |a, b| {
            a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase())
        });

        // RTRIM - ignore trailing spaces
        self.create_collation("RTRIM", |a, b| a.trim_end().cmp(b.trim_end()));
    }

    /// Register a collation sequence
    pub fn create_collation<F>(&mut self, name: &str, cmp: F)
    where
        F: Fn(&str, &str) -> std::cmp::Ordering + Send + Sync + 'static,
    {
        self.collations.insert(name.to_uppercase(), Arc::new(cmp));
    }

    /// Find a collation by name, invoking the collation-needed callback if set.
    pub fn find_collation(
        &mut self,
        name: &str,
    ) -> Option<Arc<dyn Fn(&str, &str) -> std::cmp::Ordering + Send + Sync>> {
        if let Some(collation) = self.collations.get(&name.to_uppercase()) {
            return Some(Arc::clone(collation));
        }

        if self.collation_needed.is_some() {
            let callback = self.collation_needed.take().unwrap();
            callback(self, name);
            self.collation_needed = Some(callback);
            return self.collations.get(&name.to_uppercase()).map(Arc::clone);
        }

        None
    }

    /// Set the collation-needed callback
    pub fn set_collation_needed(&mut self, callback: Option<CollationNeeded>) {
        self.collation_needed = callback;
    }

    /// Register a scalar function
    pub fn create_function(&mut self, name: &str, n_arg: i32, func: ScalarFunc) {
        let key = FuncKey::new(name, n_arg, self.encoding);
        let def = FunctionDef {
            name: name.to_string(),
            n_arg,
            x_func: Some(func),
            x_step: None,
            x_final: None,
            x_inverse: None,
            x_value: None,
        };
        self.functions.insert(key, def);
    }

    /// Register an aggregate function
    pub fn create_aggregate(&mut self, name: &str, n_arg: i32, step: AggStep, finalizer: AggFinal) {
        let key = FuncKey::new(name, n_arg, self.encoding);
        let def = FunctionDef {
            name: name.to_string(),
            n_arg,
            x_func: None,
            x_step: Some(step),
            x_final: Some(finalizer),
            x_inverse: None,
            x_value: None,
        };
        self.functions.insert(key, def);
    }

    /// Register a window function
    pub fn create_window_function(
        &mut self,
        name: &str,
        n_arg: i32,
        step: AggStep,
        finalizer: AggFinal,
        value: AggFinal,
        inverse: AggStep,
    ) {
        let key = FuncKey::new(name, n_arg, self.encoding);
        let def = FunctionDef {
            name: name.to_string(),
            n_arg,
            x_func: None,
            x_step: Some(step),
            x_final: Some(finalizer),
            x_inverse: Some(inverse),
            x_value: Some(value),
        };
        self.functions.insert(key, def);
    }

    /// Find a function by name and argument count
    pub fn find_function(&self, name: &str, n_arg: i32) -> Option<FunctionDef> {
        let key = FuncKey::new(name, n_arg, self.encoding);
        if let Some(def) = self.functions.get(&key) {
            return Some(def.clone());
        }
        let any_key = FuncKey::new(name, -1, self.encoding);
        if let Some(def) = self.functions.get(&any_key) {
            return Some(def.clone());
        }
        if let Some(func) = get_scalar_function(name) {
            return Some(FunctionDef {
                name: name.to_string(),
                n_arg,
                x_func: Some(func),
                x_step: None,
                x_final: None,
                x_inverse: None,
                x_value: None,
            });
        }
        if let Some(aggregate) = get_aggregate_function(name) {
            if matches_arg_count(&aggregate, n_arg) {
                return Some(FunctionDef {
                    name: aggregate.name,
                    n_arg,
                    x_func: None,
                    x_step: None,
                    x_final: None,
                    x_inverse: None,
                    x_value: None,
                });
            }
        }
        None
    }

    /// Invoke the authorizer callback
    pub fn authorize(
        &self,
        action: AuthAction,
        arg1: Option<&str>,
        arg2: Option<&str>,
        arg3: Option<&str>,
        arg4: Option<&str>,
    ) -> AuthResult {
        match &self.authorizer {
            Some(authorizer) => {
                AuthResult::from_code(authorizer(action as i32, arg1, arg2, arg3, arg4))
            }
            None => AuthResult::Ok,
        }
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

    /// Attach a database file under a schema name
    pub fn attach_database(&mut self, filename: &str, schema_name: &str) -> Result<()> {
        let schema_lower = schema_name.to_lowercase();
        if schema_lower.is_empty() || schema_lower == "main" || schema_lower == "temp" {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("cannot attach database {}", schema_name),
            ));
        }
        if schema_lower.starts_with("sqlite_") {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("cannot attach database {}", schema_name),
            ));
        }
        if self
            .dbs
            .iter()
            .any(|db| db.name.eq_ignore_ascii_case(schema_name))
        {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("database {} is already in use", schema_name),
            ));
        }

        if self.dbs.len() >= crate::schema::MAX_ATTACHED + 2 {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!(
                    "too many attached databases - max {}",
                    crate::schema::MAX_ATTACHED
                ),
            ));
        }

        if self.transaction_state != TransactionState::None {
            return Err(Error::with_message(
                ErrorCode::Error,
                "cannot ATTACH database within transaction",
            ));
        }

        let auth = self.authorize(
            AuthAction::Attach,
            Some(filename),
            None,
            Some(schema_name),
            None,
        );
        if auth != AuthResult::Ok {
            return Err(Error::with_message(ErrorCode::Auth, "authorization denied"));
        }

        let mut db = DbInfo::new(schema_name);
        if !filename.is_empty() && filename != ":memory:" {
            db.path = Some(filename.to_string());
        }
        self.dbs.push(db);
        Ok(())
    }

    /// Detach a database by schema name
    pub fn detach_database(&mut self, schema_name: &str) -> Result<()> {
        if schema_name.eq_ignore_ascii_case("main") || schema_name.eq_ignore_ascii_case("temp") {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("cannot detach database {}", schema_name),
            ));
        }

        let idx = self
            .dbs
            .iter()
            .position(|db| db.name.eq_ignore_ascii_case(schema_name))
            .ok_or_else(|| {
                Error::with_message(
                    ErrorCode::Error,
                    format!("no such database: {}", schema_name),
                )
            })?;

        if self.dbs[idx].busy || self.transaction_state != TransactionState::None {
            return Err(Error::with_message(
                ErrorCode::Busy,
                format!("database {} is locked", schema_name),
            ));
        }

        let auth = self.authorize(AuthAction::Detach, Some(schema_name), None, None, None);
        if auth != AuthResult::Ok {
            return Err(Error::with_message(ErrorCode::Auth, "authorization denied"));
        }

        self.dbs.remove(idx);
        Ok(())
    }
}

fn matches_arg_count(info: &AggregateInfo, n_arg: i32) -> bool {
    if n_arg < 0 {
        return true;
    }
    let count = n_arg as usize;
    count >= info.min_args && count <= info.max_args
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

    // Set up main database path and open btree
    if let Some(main_db) = conn.find_db_mut("main") {
        let is_memory = path == ":memory:" || final_flags.contains(OpenFlags::MEMORY);
        if is_memory {
            main_db.path = None; // In-memory database
        } else if !path.is_empty() {
            main_db.path = Some(path.clone());
        }

        // Create btree open flags
        let mut btree_flags = BtreeOpenFlags::empty();
        if is_memory {
            btree_flags |= BtreeOpenFlags::MEMORY;
        }

        // Open the btree
        let vfs = StubVfs;
        let btree_path = if is_memory { "" } else { &path };
        match Btree::open(&vfs, btree_path, None, btree_flags, final_flags) {
            Ok(btree) => {
                main_db.btree = Some(btree);
            }
            Err(e) => {
                // For new/empty databases, continue without btree for now
                // The btree will be created when the first table is created
                if !is_memory && !path.is_empty() {
                    // Log error but continue - allows creating new databases
                    eprintln!("Warning: Failed to open btree: {}", e);
                }
            }
        }
    }

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
        db.btree = None;
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
    crate::util::general::sqlite3_errstr(code)
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
    conn.busy_timeout = 0;
    Ok(())
}

/// sqlite3_busy_timeout - Set busy timeout
pub fn sqlite3_busy_timeout(conn: &mut SqliteConnection, ms: i32) -> Result<()> {
    conn.busy_timeout = ms;
    if ms > 0 {
        let timeout = ms as i64;
        conn.busy_handler = Some(Box::new(move |count| {
            let delay = if count < 12 {
                (count + 1) * (count + 1)
            } else {
                100
            } as i64;
            std::thread::sleep(std::time::Duration::from_millis(delay as u64));
            (count as i64 * delay) < timeout
        }));
    } else {
        conn.busy_handler = None;
    }
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

/// sqlite3_create_function - Register a scalar function
pub fn sqlite3_create_function(
    conn: &mut SqliteConnection,
    name: &str,
    n_arg: i32,
    func: ScalarFunc,
) -> Result<()> {
    conn.create_function(name, n_arg, func);
    Ok(())
}

/// sqlite3_create_aggregate - Register an aggregate function
pub fn sqlite3_create_aggregate(
    conn: &mut SqliteConnection,
    name: &str,
    n_arg: i32,
    step: AggStep,
    finalizer: AggFinal,
) -> Result<()> {
    conn.create_aggregate(name, n_arg, step, finalizer);
    Ok(())
}

/// sqlite3_create_window_function - Register a window function
pub fn sqlite3_create_window_function(
    conn: &mut SqliteConnection,
    name: &str,
    n_arg: i32,
    step: AggStep,
    finalizer: AggFinal,
    value: AggFinal,
    inverse: AggStep,
) -> Result<()> {
    conn.create_window_function(name, n_arg, step, finalizer, value, inverse);
    Ok(())
}

/// sqlite3_create_collation - Register a collation sequence
pub fn sqlite3_create_collation<F>(conn: &mut SqliteConnection, name: &str, cmp: F) -> Result<()>
where
    F: Fn(&str, &str) -> std::cmp::Ordering + Send + Sync + 'static,
{
    conn.create_collation(name, cmp);
    Ok(())
}

/// sqlite3_collation_needed - Register a collation-needed callback
pub fn sqlite3_collation_needed(
    conn: &mut SqliteConnection,
    callback: Option<CollationNeeded>,
) -> Result<()> {
    conn.set_collation_needed(callback);
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

    #[test]
    fn test_function_registry() {
        fn custom_func(args: &[Value]) -> Result<Value> {
            Ok(Value::Integer(args.len() as i64))
        }

        let mut conn = SqliteConnection::new();
        conn.create_function("custom", 1, custom_func);

        let def = conn
            .find_function("custom", 1)
            .expect("function registered");
        assert!(def.x_func.is_some());

        let builtin = conn.find_function("abs", 1).expect("builtin lookup");
        assert!(builtin.x_func.is_some());
    }

    #[test]
    fn test_collation_needed_callback() {
        let mut conn = SqliteConnection::new();
        conn.set_collation_needed(Some(Box::new(|conn, name| {
            if name.eq_ignore_ascii_case("CUSTOM") {
                conn.create_collation("CUSTOM", |a, b| a.len().cmp(&b.len()));
            }
        })));

        let coll = conn.find_collation("CUSTOM");
        assert!(coll.is_some());
    }

    #[test]
    fn test_authorizer_wrapper() {
        let mut conn = SqliteConnection::new();
        sqlite3_set_authorizer(
            &mut conn,
            Some(Box::new(|action, _, _, _, _| {
                if action == AuthAction::Attach as i32 {
                    1
                } else {
                    0
                }
            })),
        )
        .unwrap();

        assert_eq!(
            conn.authorize(AuthAction::Attach, None, None, None, None),
            AuthResult::Deny
        );
    }
}
