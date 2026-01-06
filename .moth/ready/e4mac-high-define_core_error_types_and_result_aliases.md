# Define Core Error Types and Result Aliases

## Overview
Create SQLite-compatible error types and Result aliases that will be used throughout the codebase.

## Source Reference
- `sqlite3/src/sqliteInt.h` - Error code definitions (SQLITE_OK, SQLITE_ERROR, etc.)
- `sqlite3/src/main.c` - Error message handling

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Tasks

### 1. Define Error Codes
Translate SQLite's error codes to a Rust enum:

```rust
/// SQLite result codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ErrorCode {
    Ok = 0,              // SQLITE_OK
    Error = 1,           // SQLITE_ERROR - Generic error
    Internal = 2,        // SQLITE_INTERNAL
    Perm = 3,            // SQLITE_PERM - Access permission denied
    Abort = 4,           // SQLITE_ABORT - Callback requested abort
    Busy = 5,            // SQLITE_BUSY - Database is locked
    Locked = 6,          // SQLITE_LOCKED - Table is locked
    NoMem = 7,           // SQLITE_NOMEM - Out of memory
    ReadOnly = 8,        // SQLITE_READONLY - Database is read-only
    Interrupt = 9,       // SQLITE_INTERRUPT - Operation interrupted
    IoErr = 10,          // SQLITE_IOERR - I/O error
    Corrupt = 11,        // SQLITE_CORRUPT - Database is corrupted
    NotFound = 12,       // SQLITE_NOTFOUND - Unknown opcode
    Full = 13,           // SQLITE_FULL - Database is full
    CantOpen = 14,       // SQLITE_CANTOPEN - Cannot open database
    Protocol = 15,       // SQLITE_PROTOCOL - Lock protocol error
    Empty = 16,          // SQLITE_EMPTY - Internal use only
    Schema = 17,         // SQLITE_SCHEMA - Schema changed
    TooBig = 18,         // SQLITE_TOOBIG - String or blob too big
    Constraint = 19,     // SQLITE_CONSTRAINT - Constraint violation
    Mismatch = 20,       // SQLITE_MISMATCH - Data type mismatch
    Misuse = 21,         // SQLITE_MISUSE - API misuse
    NoLfs = 22,          // SQLITE_NOLFS - No large file support
    Auth = 23,           // SQLITE_AUTH - Authorization denied
    Format = 24,         // SQLITE_FORMAT - Not used
    Range = 25,          // SQLITE_RANGE - Parameter out of range
    NotADb = 26,         // SQLITE_NOTADB - Not a database file
    Notice = 27,         // SQLITE_NOTICE - Notification
    Warning = 28,        // SQLITE_WARNING - Warning
    Row = 100,           // SQLITE_ROW - Step has another row
    Done = 101,          // SQLITE_DONE - Step finished
}
```

### 2. Define Extended Error Codes
Include extended error codes (SQLITE_IOERR_READ, etc.):

```rust
/// Extended error codes for more specific error information
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtendedErrorCode {
    // IOERR extended codes
    IoErrRead,
    IoErrShortRead,
    IoErrWrite,
    IoErrFsync,
    IoErrDirFsync,
    IoErrTruncate,
    IoErrFstat,
    IoErrUnlock,
    IoErrRdLock,
    IoErrDelete,
    IoErrBlocked,
    IoErrNoMem,
    IoErrAccess,
    IoErrCheckReservedLock,
    IoErrLock,
    IoErrClose,
    IoErrDirClose,
    IoErrShmOpen,
    IoErrShmSize,
    IoErrShmLock,
    IoErrShmMap,
    IoErrSeek,
    IoErrDeleteNoEnt,
    IoErrMmap,
    IoErrGetTempPath,
    IoErrConvPath,

    // LOCKED extended codes
    LockedSharedCache,
    LockedVTab,

    // BUSY extended codes
    BusyRecovery,
    BusySnapshot,
    BusyTimeout,

    // CANTOPEN extended codes
    CantOpenNoTempDir,
    CantOpenIsDir,
    CantOpenFullPath,
    CantOpenConvPath,
    CantOpenDirtyWal,
    CantOpenSymlink,

    // CORRUPT extended codes
    CorruptVTab,
    CorruptSequence,
    CorruptIndex,

    // READONLY extended codes
    ReadOnlyRecovery,
    ReadOnlyCantLock,
    ReadOnlyRollback,
    ReadOnlyDbMoved,
    ReadOnlyCantInit,
    ReadOnlyDirectory,

    // ABORT extended codes
    AbortRollback,

    // CONSTRAINT extended codes
    ConstraintCheck,
    ConstraintCommitHook,
    ConstraintForeignKey,
    ConstraintFunction,
    ConstraintNotNull,
    ConstraintPrimaryKey,
    ConstraintTrigger,
    ConstraintUnique,
    ConstraintVTab,
    ConstraintRowId,
    ConstraintPinned,
    ConstraintDataType,

    // AUTH extended codes
    AuthUser,

    // OK extended codes (for notices)
    OkLoadPermanently,
    OkSymlink,
}
```

### 3. Create Error Struct
```rust
/// Main error type for RustQL
#[derive(Debug)]
pub struct Error {
    pub code: ErrorCode,
    pub extended: Option<ExtendedErrorCode>,
    pub message: Option<String>,
    pub offset: Option<i32>,  // For parse errors - byte offset in SQL
}

impl Error {
    pub fn new(code: ErrorCode) -> Self { ... }
    pub fn with_message(code: ErrorCode, msg: impl Into<String>) -> Self { ... }
    pub fn with_extended(code: ErrorCode, ext: ExtendedErrorCode) -> Self { ... }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.code)?;
        if let Some(msg) = &self.message {
            write!(f, ": {}", msg)?;
        }
        Ok(())
    }
}
```

### 4. Define Result Alias
```rust
/// Result type alias for RustQL operations
pub type Result<T> = std::result::Result<T, Error>;
```

### 5. Conversion Traits
```rust
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::with_message(ErrorCode::IoErr, e.to_string())
    }
}

impl From<ErrorCode> for Error {
    fn from(code: ErrorCode) -> Self {
        Error::new(code)
    }
}
```

## Acceptance Criteria
- [ ] All SQLite primary error codes (0-28, 100-101) translated
- [ ] Extended error codes for IOERR, BUSY, LOCKED, etc.
- [ ] Error struct with code, extended, message, offset fields
- [ ] Result type alias defined
- [ ] Implements std::error::Error and Display traits
- [ ] Conversion from std::io::Error implemented
