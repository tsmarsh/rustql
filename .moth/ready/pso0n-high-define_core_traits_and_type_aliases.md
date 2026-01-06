# Define Core Traits and Type Aliases

## Overview
Define the foundational types, traits, and type aliases that will be used throughout the SQLite translation.

## Source Reference
- `sqlite3/src/sqliteInt.h` - Core type definitions (Pgno, i64, etc.)
- `sqlite3/src/sqlite.h.in` - Public API template (generates `sqlite3.h`)
- `sqlite3/src/os.h` - VFS types

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Tasks

### 1. Numeric Type Aliases
```rust
/// Page number type (u32 in SQLite)
pub type Pgno = u32;

/// Row ID type (i64 in SQLite)
pub type RowId = i64;

/// Database file offset
pub type DbOffset = i64;

/// Byte count type
pub type ByteCount = usize;

/// Transaction counter
pub type TxnId = u32;
```

### 2. Database Connection Trait
```rust
/// Trait for database connection operations
pub trait Connection {
    /// Execute SQL without returning rows
    fn execute(&mut self, sql: &str) -> Result<()>;

    /// Prepare a statement for execution
    fn prepare(&mut self, sql: &str) -> Result<Box<dyn Statement>>;

    /// Get the rowid of last INSERT
    fn last_insert_rowid(&self) -> RowId;

    /// Number of rows changed by last statement
    fn changes(&self) -> i32;

    /// Total rows changed since connection opened
    fn total_changes(&self) -> i64;

    /// Check if autocommit mode is on
    fn get_autocommit(&self) -> bool;

    /// Interrupt a long-running query
    fn interrupt(&self);
}
```

### 3. Statement Trait
```rust
/// Prepared statement execution states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepResult {
    Row,   // SQLITE_ROW - another row available
    Done,  // SQLITE_DONE - statement finished
}

/// Column data types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Integer = 1,  // SQLITE_INTEGER
    Float = 2,    // SQLITE_FLOAT
    Text = 3,     // SQLITE_TEXT
    Blob = 4,     // SQLITE_BLOB
    Null = 5,     // SQLITE_NULL
}

/// Trait for prepared statement operations
pub trait Statement {
    /// Execute one step, returns Row or Done
    fn step(&mut self) -> Result<StepResult>;

    /// Reset statement to re-execute
    fn reset(&mut self) -> Result<()>;

    /// Finalize and release resources
    fn finalize(self: Box<Self>) -> Result<()>;

    /// Clear all parameter bindings
    fn clear_bindings(&mut self) -> Result<()>;

    // Parameter binding (1-indexed)
    fn bind_null(&mut self, idx: i32) -> Result<()>;
    fn bind_i64(&mut self, idx: i32, value: i64) -> Result<()>;
    fn bind_f64(&mut self, idx: i32, value: f64) -> Result<()>;
    fn bind_text(&mut self, idx: i32, value: &str) -> Result<()>;
    fn bind_blob(&mut self, idx: i32, value: &[u8]) -> Result<()>;
    fn bind_value(&mut self, idx: i32, value: &Value) -> Result<()>;

    // Column access (0-indexed)
    fn column_count(&self) -> i32;
    fn column_name(&self, idx: i32) -> &str;
    fn column_type(&self, idx: i32) -> ColumnType;
    fn column_i64(&self, idx: i32) -> i64;
    fn column_f64(&self, idx: i32) -> f64;
    fn column_text(&self, idx: i32) -> &str;
    fn column_blob(&self, idx: i32) -> &[u8];
    fn column_value(&self, idx: i32) -> Value;
}
```

### 4. Virtual Filesystem Traits
```rust
use bitflags::bitflags;

bitflags! {
    /// File open flags (SQLITE_OPEN_*)
    pub struct OpenFlags: u32 {
        const READONLY       = 0x00000001;
        const READWRITE      = 0x00000002;
        const CREATE         = 0x00000004;
        const DELETEONCLOSE  = 0x00000008;
        const EXCLUSIVE      = 0x00000010;
        const AUTOPROXY      = 0x00000020;
        const URI            = 0x00000040;
        const MEMORY         = 0x00000080;
        const MAIN_DB        = 0x00000100;
        const TEMP_DB        = 0x00000200;
        const TRANSIENT_DB   = 0x00000400;
        const MAIN_JOURNAL   = 0x00000800;
        const TEMP_JOURNAL   = 0x00001000;
        const SUBJOURNAL     = 0x00002000;
        const SUPER_JOURNAL  = 0x00004000;
        const NOMUTEX        = 0x00008000;
        const FULLMUTEX      = 0x00010000;
        const SHAREDCACHE    = 0x00020000;
        const PRIVATECACHE   = 0x00040000;
        const WAL            = 0x00080000;
        const NOFOLLOW       = 0x01000000;
    }

    /// Sync flags for VfsFile::sync()
    pub struct SyncFlags: u32 {
        const NORMAL   = 0x00002;
        const FULL     = 0x00003;
        const DATAONLY = 0x00010;
    }

    /// Access check flags
    pub struct AccessFlags: u32 {
        const EXISTS    = 0;
        const READWRITE = 1;
        const READ      = 2;
    }
}

/// Lock levels for file locking
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockLevel {
    None = 0,
    Shared = 1,
    Reserved = 2,
    Pending = 3,
    Exclusive = 4,
}

/// Virtual filesystem abstraction (sqlite3_vfs)
pub trait Vfs: Send + Sync {
    type File: VfsFile;

    /// Open a file
    fn open(&self, path: &str, flags: OpenFlags) -> Result<Self::File>;

    /// Delete a file
    fn delete(&self, path: &str, sync_dir: bool) -> Result<()>;

    /// Check file accessibility
    fn access(&self, path: &str, flags: AccessFlags) -> Result<bool>;

    /// Convert to full pathname
    fn full_pathname(&self, path: &str) -> Result<String>;

    /// Fill buffer with random bytes
    fn randomness(&self, buf: &mut [u8]) -> i32;

    /// Sleep for microseconds
    fn sleep(&self, microseconds: i32) -> i32;

    /// Current time as Julian day number
    fn current_time(&self) -> f64;

    /// Current time with millisecond precision
    fn current_time_i64(&self) -> i64;
}

/// File operations trait (sqlite3_file)
pub trait VfsFile: Send {
    fn read(&mut self, buf: &mut [u8], offset: DbOffset) -> Result<usize>;
    fn write(&mut self, buf: &[u8], offset: DbOffset) -> Result<()>;
    fn truncate(&mut self, size: DbOffset) -> Result<()>;
    fn sync(&mut self, flags: SyncFlags) -> Result<()>;
    fn file_size(&self) -> Result<DbOffset>;
    fn lock(&mut self, level: LockLevel) -> Result<()>;
    fn unlock(&mut self, level: LockLevel) -> Result<()>;
    fn check_reserved_lock(&self) -> Result<bool>;
    fn sector_size(&self) -> i32;
    fn device_characteristics(&self) -> u32;
}
```

### 5. SQLite Value Type
```rust
/// Dynamic SQLite value (sqlite3_value)
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    /// Get the type affinity of this value
    pub fn column_type(&self) -> ColumnType {
        match self {
            Value::Null => ColumnType::Null,
            Value::Integer(_) => ColumnType::Integer,
            Value::Real(_) => ColumnType::Float,
            Value::Text(_) => ColumnType::Text,
            Value::Blob(_) => ColumnType::Blob,
        }
    }

    /// Convert to i64 with SQLite coercion rules
    pub fn to_i64(&self) -> i64 { ... }

    /// Convert to f64 with SQLite coercion rules
    pub fn to_f64(&self) -> f64 { ... }

    /// Convert to string with SQLite coercion rules
    pub fn to_text(&self) -> String { ... }

    /// Convert to bytes with SQLite coercion rules
    pub fn to_blob(&self) -> Vec<u8> { ... }

    /// Check if value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl Default for Value {
    fn default() -> Self { Value::Null }
}
```

### 6. Function Context Trait
```rust
/// Context for user-defined functions (sqlite3_context)
pub trait FunctionContext {
    fn result_null(&mut self);
    fn result_i64(&mut self, value: i64);
    fn result_f64(&mut self, value: f64);
    fn result_text(&mut self, value: &str);
    fn result_blob(&mut self, value: &[u8]);
    fn result_error(&mut self, msg: &str);
    fn result_error_code(&mut self, code: ErrorCode);
    fn result_value(&mut self, value: &Value);

    /// Get auxiliary data for this invocation
    fn get_auxdata(&self, n: i32) -> Option<&dyn std::any::Any>;
    fn set_auxdata(&mut self, n: i32, data: Box<dyn std::any::Any>);
}
```

## Dependencies
- `bitflags = "2"` crate for flag definitions

## Acceptance Criteria
- [ ] All numeric type aliases defined (Pgno, RowId, DbOffset, etc.)
- [ ] Connection trait with execute/prepare/changes methods
- [ ] Statement trait with bind/column/step methods
- [ ] Vfs and VfsFile traits with all file operations
- [ ] OpenFlags, SyncFlags, AccessFlags, LockLevel defined
- [ ] Value enum with type conversions and coercion
- [ ] FunctionContext trait for UDFs
- [ ] All traits are Send + Sync where appropriate
