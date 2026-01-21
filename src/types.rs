//! Core type aliases and traits for RustQL
//!
//! This module defines the foundational types, traits, and type aliases
//! that will be used throughout the SQLite translation.

use std::any::Any;

use bitflags::bitflags;

use crate::error::{ErrorCode, Result};

// ============================================================================
// Numeric Type Aliases
// ============================================================================

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

// ============================================================================
// Statement Execution Types
// ============================================================================

/// Prepared statement execution states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepResult {
    /// SQLITE_ROW - another row available
    Row,
    /// SQLITE_DONE - statement finished
    Done,
}

/// Column data types (SQLITE_INTEGER, SQLITE_FLOAT, etc.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ColumnType {
    /// SQLITE_INTEGER = 1
    Integer = 1,
    /// SQLITE_FLOAT = 2
    Float = 2,
    /// SQLITE_TEXT = 3
    Text = 3,
    /// SQLITE_BLOB = 4
    Blob = 4,
    /// SQLITE_NULL = 5
    Null = 5,
}

// ============================================================================
// SQLite Value Type
// ============================================================================

/// Dynamic SQLite value (sqlite3_value)
///
/// Represents a value that can be stored in or retrieved from SQLite.
/// Implements SQLite's type affinity and coercion rules.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Value {
    /// NULL value
    #[default]
    Null,
    /// Integer value (64-bit signed)
    Integer(i64),
    /// Real/float value (64-bit IEEE 754)
    Real(f64),
    /// Text value (UTF-8 string)
    Text(String),
    /// Binary large object
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
    ///
    /// - NULL -> 0
    /// - Integer -> value
    /// - Real -> truncated to integer
    /// - Text -> parsed as integer, or 0 if invalid
    /// - Blob -> 0
    pub fn to_i64(&self) -> i64 {
        match self {
            Value::Null => 0,
            Value::Integer(i) => *i,
            Value::Real(f) => *f as i64,
            Value::Text(s) => s.parse().unwrap_or(0),
            Value::Blob(_) => 0,
        }
    }

    /// Convert to f64 with SQLite coercion rules
    ///
    /// - NULL -> 0.0
    /// - Integer -> converted to float
    /// - Real -> value
    /// - Text -> parsed as float, or 0.0 if invalid
    /// - Blob -> 0.0
    pub fn to_f64(&self) -> f64 {
        match self {
            Value::Null => 0.0,
            Value::Integer(i) => *i as f64,
            Value::Real(f) => *f,
            Value::Text(s) => s.parse().unwrap_or(0.0),
            Value::Blob(_) => 0.0,
        }
    }

    /// Convert to string with SQLite coercion rules
    ///
    /// - NULL -> empty string
    /// - Integer -> decimal representation
    /// - Real -> decimal representation
    /// - Text -> value
    /// - Blob -> interpreted as UTF-8 (lossy)
    pub fn to_text(&self) -> String {
        match self {
            Value::Null => String::new(),
            Value::Integer(i) => i.to_string(),
            Value::Real(f) => {
                // SQLite displays floats with decimal point even for whole numbers
                // e.g., 1.0 not 1, to distinguish from integers
                let s = f.to_string();
                if !s.contains('.') && !s.contains('e') && !s.contains('E') {
                    format!("{}.0", s)
                } else {
                    s
                }
            }
            Value::Text(s) => s.clone(),
            Value::Blob(b) => String::from_utf8_lossy(b).into_owned(),
        }
    }

    /// Convert to bytes with SQLite coercion rules
    ///
    /// - NULL -> empty vec
    /// - Integer -> decimal representation as bytes
    /// - Real -> decimal representation as bytes
    /// - Text -> UTF-8 bytes
    /// - Blob -> value
    pub fn to_blob(&self) -> Vec<u8> {
        match self {
            Value::Null => Vec::new(),
            Value::Integer(i) => i.to_string().into_bytes(),
            Value::Real(f) => {
                // Match to_text behavior for consistency
                let s = f.to_string();
                if !s.contains('.') && !s.contains('e') && !s.contains('E') {
                    format!("{}.0", s).into_bytes()
                } else {
                    s.into_bytes()
                }
            }
            Value::Text(s) => s.as_bytes().to_vec(),
            Value::Blob(b) => b.clone(),
        }
    }

    /// Check if value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Get byte length of the value
    pub fn bytes(&self) -> usize {
        match self {
            Value::Null => 0,
            Value::Integer(_) => 8,
            Value::Real(_) => 8,
            Value::Text(s) => s.len(),
            Value::Blob(b) => b.len(),
        }
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Integer(v as i64)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Real(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Blob(v)
    }
}

impl From<&[u8]> for Value {
    fn from(v: &[u8]) -> Self {
        Value::Blob(v.to_vec())
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(v: Option<T>) -> Self {
        match v {
            Some(val) => val.into(),
            None => Value::Null,
        }
    }
}

// ============================================================================
// VFS Flags and Types
// ============================================================================

bitflags! {
    /// File open flags (SQLITE_OPEN_*)
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
        const EXRESCODE      = 0x02000000;
    }

    /// Sync flags for VfsFile::sync()
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SyncFlags: u32 {
        const NORMAL   = 0x00002;
        const FULL     = 0x00003;
        const DATAONLY = 0x00010;
    }

    /// Access check flags
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AccessFlags: u32 {
        const EXISTS    = 0;
        const READWRITE = 1;
        const READ      = 2;
    }

    /// Device characteristics (SQLITE_IOCAP_*)
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct DeviceCharacteristics: u32 {
        const ATOMIC                 = 0x00000001;
        const ATOMIC512              = 0x00000002;
        const ATOMIC1K               = 0x00000004;
        const ATOMIC2K               = 0x00000008;
        const ATOMIC4K               = 0x00000010;
        const ATOMIC8K               = 0x00000020;
        const ATOMIC16K              = 0x00000040;
        const ATOMIC32K              = 0x00000080;
        const ATOMIC64K              = 0x00000100;
        const SAFE_APPEND            = 0x00000200;
        const SEQUENTIAL             = 0x00000400;
        const UNDELETABLE_WHEN_OPEN  = 0x00000800;
        const POWERSAFE_OVERWRITE    = 0x00001000;
        const IMMUTABLE              = 0x00002000;
        const BATCH_ATOMIC           = 0x00004000;
    }
}

/// Lock levels for file locking
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
pub enum LockLevel {
    /// No lock held
    None = 0,
    /// Shared (read) lock
    Shared = 1,
    /// Reserved lock (preparing to write)
    Reserved = 2,
    /// Pending lock (waiting for exclusive)
    Pending = 3,
    /// Exclusive (write) lock
    Exclusive = 4,
}

// ============================================================================
// Traits
// ============================================================================

/// Trait for database connection operations
pub trait Connection: Send {
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

/// Trait for prepared statement operations
pub trait Statement: Send {
    /// Execute one step, returns Row or Done
    fn step(&mut self) -> Result<StepResult>;

    /// Reset statement to re-execute
    fn reset(&mut self) -> Result<()>;

    /// Finalize and release resources
    fn finalize(self: Box<Self>) -> Result<()>;

    /// Clear all parameter bindings
    fn clear_bindings(&mut self) -> Result<()>;

    // Parameter binding (1-indexed, following SQLite convention)
    /// Bind NULL to parameter
    fn bind_null(&mut self, idx: i32) -> Result<()>;
    /// Bind i64 to parameter
    fn bind_i64(&mut self, idx: i32, value: i64) -> Result<()>;
    /// Bind f64 to parameter
    fn bind_f64(&mut self, idx: i32, value: f64) -> Result<()>;
    /// Bind text to parameter
    fn bind_text(&mut self, idx: i32, value: &str) -> Result<()>;
    /// Bind blob to parameter
    fn bind_blob(&mut self, idx: i32, value: &[u8]) -> Result<()>;
    /// Bind Value to parameter
    fn bind_value(&mut self, idx: i32, value: &Value) -> Result<()>;

    // Column access (0-indexed, following SQLite convention)
    /// Number of columns in result
    fn column_count(&self) -> i32;
    /// Column name at index
    fn column_name(&self, idx: i32) -> &str;
    /// Column type at index
    fn column_type(&self, idx: i32) -> ColumnType;
    /// Get column as i64
    fn column_i64(&self, idx: i32) -> i64;
    /// Get column as f64
    fn column_f64(&self, idx: i32) -> f64;
    /// Get column as text
    fn column_text(&self, idx: i32) -> &str;
    /// Get column as blob
    fn column_blob(&self, idx: i32) -> &[u8];
    /// Get column as Value
    fn column_value(&self, idx: i32) -> Value;
}

/// Virtual filesystem abstraction (sqlite3_vfs)
pub trait Vfs: Send + Sync {
    /// The file type this VFS produces
    type File: VfsFile;

    /// Open a file
    fn open(&self, path: &str, flags: OpenFlags) -> Result<Self::File>;

    /// Delete a file
    fn delete(&self, path: &str, sync_dir: bool) -> Result<()>;

    /// Check file accessibility
    fn access(&self, path: &str, flags: AccessFlags) -> Result<bool>;

    /// Convert to full pathname
    fn full_pathname(&self, path: &str) -> Result<String>;

    /// Fill buffer with random bytes, returns bytes written
    fn randomness(&self, buf: &mut [u8]) -> i32;

    /// Sleep for microseconds, returns actual sleep time
    fn sleep(&self, microseconds: i32) -> i32;

    /// Current time as Julian day number
    fn current_time(&self) -> f64;

    /// Current time with millisecond precision (ms since Unix epoch)
    fn current_time_i64(&self) -> i64;
}

/// File operations trait (sqlite3_file)
pub trait VfsFile: Send {
    /// Read data at offset, returns bytes read
    fn read(&mut self, buf: &mut [u8], offset: DbOffset) -> Result<usize>;

    /// Write data at offset
    fn write(&mut self, buf: &[u8], offset: DbOffset) -> Result<()>;

    /// Truncate file to size
    fn truncate(&mut self, size: DbOffset) -> Result<()>;

    /// Sync file to disk
    fn sync(&mut self, flags: SyncFlags) -> Result<()>;

    /// Get file size
    fn file_size(&self) -> Result<DbOffset>;

    /// Acquire lock at level
    fn lock(&mut self, level: LockLevel) -> Result<()>;

    /// Release lock to level
    fn unlock(&mut self, level: LockLevel) -> Result<()>;

    /// Check if reserved lock is held by another process
    fn check_reserved_lock(&self) -> Result<bool>;

    /// Sector size (minimum atomic write unit)
    fn sector_size(&self) -> i32;

    /// Device characteristics flags
    fn device_characteristics(&self) -> DeviceCharacteristics;
}

/// Context for user-defined functions (sqlite3_context)
pub trait FunctionContext: Send {
    /// Set result to NULL
    fn result_null(&mut self);

    /// Set result to i64
    fn result_i64(&mut self, value: i64);

    /// Set result to f64
    fn result_f64(&mut self, value: f64);

    /// Set result to text
    fn result_text(&mut self, value: &str);

    /// Set result to blob
    fn result_blob(&mut self, value: &[u8]);

    /// Set result to error with message
    fn result_error(&mut self, msg: &str);

    /// Set result to error with code
    fn result_error_code(&mut self, code: ErrorCode);

    /// Set result to Value
    fn result_value(&mut self, value: &Value);

    /// Get auxiliary data for argument n
    fn get_auxdata(&self, n: i32) -> Option<&dyn Any>;

    /// Set auxiliary data for argument n
    fn set_auxdata(&mut self, n: i32, data: Box<dyn Any + Send>);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_coercion_to_i64() {
        assert_eq!(Value::Null.to_i64(), 0);
        assert_eq!(Value::Integer(42).to_i64(), 42);
        assert_eq!(Value::Real(3.7).to_i64(), 3);
        assert_eq!(Value::Text("123".into()).to_i64(), 123);
        assert_eq!(Value::Text("abc".into()).to_i64(), 0);
        assert_eq!(Value::Blob(vec![1, 2, 3]).to_i64(), 0);
    }

    #[test]
    fn test_value_coercion_to_f64() {
        assert_eq!(Value::Null.to_f64(), 0.0);
        assert_eq!(Value::Integer(42).to_f64(), 42.0);
        assert_eq!(Value::Real(3.14).to_f64(), 3.14);
        assert_eq!(Value::Text("2.5".into()).to_f64(), 2.5);
        assert_eq!(Value::Text("abc".into()).to_f64(), 0.0);
        assert_eq!(Value::Blob(vec![1, 2, 3]).to_f64(), 0.0);
    }

    #[test]
    fn test_value_coercion_to_text() {
        assert_eq!(Value::Null.to_text(), "");
        assert_eq!(Value::Integer(42).to_text(), "42");
        assert_eq!(Value::Text("hello".into()).to_text(), "hello");
        assert_eq!(Value::Blob(b"hello".to_vec()).to_text(), "hello");
    }

    #[test]
    fn test_value_column_type() {
        assert_eq!(Value::Null.column_type(), ColumnType::Null);
        assert_eq!(Value::Integer(0).column_type(), ColumnType::Integer);
        assert_eq!(Value::Real(0.0).column_type(), ColumnType::Float);
        assert_eq!(Value::Text(String::new()).column_type(), ColumnType::Text);
        assert_eq!(Value::Blob(vec![]).column_type(), ColumnType::Blob);
    }

    #[test]
    fn test_value_from_conversions() {
        assert_eq!(Value::from(42i64), Value::Integer(42));
        assert_eq!(Value::from(42i32), Value::Integer(42));
        assert_eq!(Value::from(3.14f64), Value::Real(3.14));
        assert_eq!(Value::from("hello"), Value::Text("hello".into()));
        assert_eq!(Value::from(vec![1u8, 2, 3]), Value::Blob(vec![1, 2, 3]));
        assert_eq!(Value::from(None::<i64>), Value::Null);
        assert_eq!(Value::from(Some(42i64)), Value::Integer(42));
    }

    #[test]
    fn test_lock_level_ordering() {
        assert!(LockLevel::None < LockLevel::Shared);
        assert!(LockLevel::Shared < LockLevel::Reserved);
        assert!(LockLevel::Reserved < LockLevel::Pending);
        assert!(LockLevel::Pending < LockLevel::Exclusive);
    }

    #[test]
    fn test_open_flags() {
        let flags = OpenFlags::READWRITE | OpenFlags::CREATE;
        assert!(flags.contains(OpenFlags::READWRITE));
        assert!(flags.contains(OpenFlags::CREATE));
        assert!(!flags.contains(OpenFlags::READONLY));
    }
}
