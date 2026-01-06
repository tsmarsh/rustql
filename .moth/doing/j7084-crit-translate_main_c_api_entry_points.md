# Translate main.c - API Entry Points

## Overview
Translate the main SQLite API including connection management, configuration, and initialization.

## Source Reference
- `sqlite3/src/main.c` - 5,159 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Connection
```rust
/// SQLite database connection
pub struct Connection {
    /// Database files (main + attached)
    pub dbs: Vec<DbInfo>,
    /// Number of attached databases
    pub n_db: i32,
    /// Open flags
    pub flags: OpenFlags,
    /// Last error code
    pub err_code: ErrorCode,
    /// Extended error code
    pub err_code_ext: i32,
    /// Error message
    pub err_msg: Option<String>,
    /// Schema information
    pub schema: Arc<RwLock<Schema>>,
    /// Mutex for thread safety
    pub mutex: Box<dyn Mutex>,
    /// Busy handler callback
    pub busy_handler: Option<BusyHandler>,
    /// Busy timeout in ms
    pub busy_timeout: i32,
    /// Trace callback
    pub trace: Option<TraceCallback>,
    /// Profile callback
    pub profile: Option<ProfileCallback>,
    /// Progress handler
    pub progress_handler: Option<ProgressHandler>,
    /// Authorizer callback
    pub authorizer: Option<Authorizer>,
    /// Collations
    pub collations: HashMap<String, Arc<Collation>>,
    /// User-defined functions
    pub functions: HashMap<String, FuncDef>,
    /// Virtual table modules
    pub modules: HashMap<String, Arc<dyn VirtualTableModule>>,
    /// Auto-vacuum mode
    pub auto_vacuum: AutoVacuum,
    /// Transaction nesting
    pub transaction_state: TransactionState,
    /// Savepoints
    pub savepoints: Vec<Savepoint>,
    /// Lookaside memory allocator
    pub lookaside: Option<Lookaside>,
    /// Total changes count
    pub total_changes: i64,
    /// Changes from last statement
    pub changes: i64,
    /// Last insert rowid
    pub last_insert_rowid: i64,
    /// Soft heap limit
    pub soft_heap_limit: i64,
    /// VFS to use
    pub vfs: Arc<dyn Vfs>,
    /// Currently executing statement
    pub current_stmt: Option<*mut Vdbe>,
    /// Interrupt flag
    pub interrupted: AtomicBool,
    /// Safe mode (restrict operations)
    pub safe_mode: bool,
}

/// Attached database info
pub struct DbInfo {
    /// Schema name ("main", "temp", or attached name)
    pub name: String,
    /// B-tree for this database
    pub btree: Option<Arc<BtShared>>,
    /// Schema object
    pub schema: Option<Arc<Schema>>,
    /// Safety level
    pub safety_level: SafetyLevel,
    /// Is busy (exclusive lock held)
    pub busy: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum TransactionState {
    None,
    Read,
    Write,
}

#[derive(Debug, Clone, Copy)]
pub enum AutoVacuum {
    None = 0,
    Full = 1,
    Incremental = 2,
}

#[derive(Debug, Clone, Copy)]
pub enum SafetyLevel {
    Off = 0,
    Normal = 1,
    Full = 2,
    Extra = 3,
}
```

### Configuration
```rust
/// Global configuration
pub struct GlobalConfig {
    /// Initialized flag
    pub is_init: AtomicBool,
    /// In initialization
    pub in_progress: AtomicBool,
    /// Threading mode
    pub threading_mode: ThreadingMode,
    /// Memory allocator
    pub mem_methods: Box<dyn MemMethods>,
    /// Mutex methods
    pub mutex_methods: Box<dyn MutexMethods>,
    /// Page cache size
    pub page_cache_size: i32,
    /// Default page size
    pub page_size: i32,
    /// Memory status tracking
    pub mem_status: bool,
    /// Lookaside config
    pub lookaside_size: i32,
    pub lookaside_count: i32,
    /// Soft heap limit
    pub soft_heap_limit: i64,
    /// URI filenames
    pub uri: bool,
    /// Covering index scan
    pub covering_index_scan: bool,
    /// Small malloc threshold
    pub small_malloc: i32,
    /// Statement journal spill threshold
    pub stmtjrnl_spill: i32,
    /// Sorter reference size
    pub sorter_ref_size: i32,
    /// Memory map size
    pub mmap_size: i64,
    /// Default mmap size
    pub default_mmap_size: i64,
}

lazy_static! {
    pub static ref GLOBAL_CONFIG: RwLock<GlobalConfig> = RwLock::new(GlobalConfig::default());
}
```

## Core API Functions

### Initialization
```rust
/// Initialize SQLite library
pub fn sqlite3_initialize() -> Result<()> {
    let mut config = GLOBAL_CONFIG.write().unwrap();

    if config.is_init.load(Ordering::SeqCst) {
        return Ok(());
    }

    if config.in_progress.swap(true, Ordering::SeqCst) {
        return Err(Error::with_code(ErrorCode::Misuse));
    }

    // Initialize subsystems
    os_init()?;
    // mem_init()?;
    // mutex_init()?;

    config.is_init.store(true, Ordering::SeqCst);
    config.in_progress.store(false, Ordering::SeqCst);

    Ok(())
}

/// Shutdown SQLite library
pub fn sqlite3_shutdown() -> Result<()> {
    let mut config = GLOBAL_CONFIG.write().unwrap();

    if !config.is_init.load(Ordering::SeqCst) {
        return Ok(());
    }

    // Shutdown subsystems
    os_end()?;

    config.is_init.store(false, Ordering::SeqCst);

    Ok(())
}
```

### Connection Management
```rust
/// Open a database connection
pub fn sqlite3_open(filename: &str) -> Result<Box<Connection>> {
    sqlite3_open_v2(filename, OpenFlags::READWRITE | OpenFlags::CREATE, None)
}

pub fn sqlite3_open_v2(
    filename: &str,
    flags: OpenFlags,
    vfs_name: Option<&str>,
) -> Result<Box<Connection>> {
    // Ensure initialized
    sqlite3_initialize()?;

    // Find VFS
    let vfs = vfs_find(vfs_name)
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "VFS not found"))?;

    // Parse URI if applicable
    let (path, uri_flags) = if flags.contains(OpenFlags::URI) {
        parse_uri(filename)?
    } else {
        (filename.to_string(), OpenFlags::empty())
    };

    let final_flags = flags | uri_flags;

    // Create connection
    let mut conn = Box::new(Connection {
        dbs: Vec::new(),
        n_db: 0,
        flags: final_flags,
        err_code: ErrorCode::Ok,
        err_code_ext: 0,
        err_msg: None,
        schema: Arc::new(RwLock::new(Schema::new())),
        mutex: mutex_alloc(MutexType::Recursive),
        busy_handler: None,
        busy_timeout: 0,
        trace: None,
        profile: None,
        progress_handler: None,
        authorizer: None,
        collations: HashMap::new(),
        functions: HashMap::new(),
        modules: HashMap::new(),
        auto_vacuum: AutoVacuum::None,
        transaction_state: TransactionState::None,
        savepoints: Vec::new(),
        lookaside: None,
        total_changes: 0,
        changes: 0,
        last_insert_rowid: 0,
        soft_heap_limit: 0,
        vfs: vfs.clone(),
        current_stmt: None,
        interrupted: AtomicBool::new(false),
        safe_mode: false,
    });

    // Open main database
    conn.open_database(&path, "main", final_flags)?;

    // Register built-in functions
    conn.register_builtin_functions();

    // Register built-in collations
    conn.register_builtin_collations();

    Ok(conn)
}

/// Close a database connection
pub fn sqlite3_close(conn: Box<Connection>) -> Result<()> {
    // Check for busy statements
    if conn.current_stmt.is_some() {
        return Err(Error::with_code(ErrorCode::Busy));
    }

    // Close all databases
    for db in &conn.dbs {
        if let Some(ref btree) = db.btree {
            btree.close()?;
        }
    }

    Ok(())
}

impl Connection {
    fn open_database(&mut self, path: &str, name: &str, flags: OpenFlags) -> Result<()> {
        // Open B-tree
        let btree = BtShared::open(&self.vfs, path, flags)?;

        let db_info = DbInfo {
            name: name.to_string(),
            btree: Some(Arc::new(btree)),
            schema: None,
            safety_level: SafetyLevel::Full,
            busy: false,
        };

        self.dbs.push(db_info);
        self.n_db += 1;

        Ok(())
    }
}
```

### Statement Execution
```rust
/// Execute a SQL statement
pub fn sqlite3_exec(
    conn: &mut Connection,
    sql: &str,
    callback: Option<ExecCallback>,
    user_data: *mut (),
) -> Result<()> {
    let mut tail = sql;

    while !tail.trim().is_empty() {
        let mut stmt = std::ptr::null_mut();
        let (remaining, prepared) = sqlite3_prepare_v2(conn, tail)?;
        tail = remaining;

        if prepared.is_null() {
            continue;
        }

        stmt = prepared;

        loop {
            let rc = sqlite3_step(unsafe { &mut *stmt });

            match rc {
                StepResult::Row => {
                    if let Some(ref cb) = callback {
                        let ncol = sqlite3_column_count(unsafe { &*stmt });
                        let mut values = Vec::with_capacity(ncol as usize);
                        let mut names = Vec::with_capacity(ncol as usize);

                        for i in 0..ncol {
                            values.push(sqlite3_column_text(unsafe { &*stmt }, i));
                            names.push(sqlite3_column_name(unsafe { &*stmt }, i));
                        }

                        if cb(user_data, ncol, &values, &names) != 0 {
                            break;
                        }
                    }
                }
                StepResult::Done => break,
                StepResult::Error(e) => {
                    sqlite3_finalize(unsafe { Box::from_raw(stmt) });
                    return Err(e);
                }
            }
        }

        sqlite3_finalize(unsafe { Box::from_raw(stmt) });
    }

    Ok(())
}

pub type ExecCallback = fn(*mut (), i32, &[&str], &[&str]) -> i32;
```

### Error Handling
```rust
impl Connection {
    pub fn set_error(&mut self, code: ErrorCode, msg: &str) {
        self.err_code = code;
        self.err_msg = Some(msg.to_string());
    }

    pub fn clear_error(&mut self) {
        self.err_code = ErrorCode::Ok;
        self.err_msg = None;
    }
}

pub fn sqlite3_errcode(conn: &Connection) -> ErrorCode {
    conn.err_code
}

pub fn sqlite3_errmsg(conn: &Connection) -> &str {
    conn.err_msg.as_deref().unwrap_or("not an error")
}

pub fn sqlite3_extended_errcode(conn: &Connection) -> i32 {
    conn.err_code_ext
}

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
        ErrorCode::Schema => "database schema has changed",
        ErrorCode::TooBig => "string or blob too big",
        ErrorCode::Constraint => "constraint failed",
        ErrorCode::Mismatch => "datatype mismatch",
        ErrorCode::Misuse => "bad parameter or other API misuse",
        _ => "unknown error",
    }
}
```

### Utility Functions
```rust
pub fn sqlite3_changes(conn: &Connection) -> i64 {
    conn.changes
}

pub fn sqlite3_total_changes(conn: &Connection) -> i64 {
    conn.total_changes
}

pub fn sqlite3_last_insert_rowid(conn: &Connection) -> i64 {
    conn.last_insert_rowid
}

pub fn sqlite3_interrupt(conn: &Connection) {
    conn.interrupted.store(true, Ordering::SeqCst);
}

pub fn sqlite3_busy_timeout(conn: &mut Connection, ms: i32) -> Result<()> {
    conn.busy_timeout = ms;
    Ok(())
}

pub fn sqlite3_busy_handler(
    conn: &mut Connection,
    handler: Option<BusyHandler>,
) -> Result<()> {
    conn.busy_handler = handler;
    Ok(())
}

pub type BusyHandler = fn(*mut (), i32) -> i32;

pub fn sqlite3_libversion() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub fn sqlite3_libversion_number() -> i32 {
    // Format: major * 1000000 + minor * 1000 + patch
    let version = env!("CARGO_PKG_VERSION");
    // Parse and compute
    3046000  // Example: 3.46.0
}

pub fn sqlite3_threadsafe() -> i32 {
    match get_threading_mode() {
        ThreadingMode::SingleThread => 0,
        ThreadingMode::MultiThread => 1,
        ThreadingMode::Serialized => 2,
    }
}
```

## Acceptance Criteria
- [ ] sqlite3_initialize/shutdown
- [ ] sqlite3_open/open_v2/close
- [ ] sqlite3_exec
- [ ] sqlite3_prepare_v2/step/finalize/reset
- [ ] sqlite3_bind_* functions
- [ ] sqlite3_column_* functions
- [ ] Error handling (errcode, errmsg, errstr)
- [ ] sqlite3_changes/total_changes
- [ ] sqlite3_last_insert_rowid
- [ ] sqlite3_interrupt
- [ ] sqlite3_busy_handler/timeout
- [ ] sqlite3_trace/profile
- [ ] sqlite3_progress_handler
- [ ] sqlite3_set_authorizer
- [ ] sqlite3_config
- [ ] sqlite3_db_config
- [ ] Version information
