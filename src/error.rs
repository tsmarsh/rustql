//! Error types and Result aliases for RustQL
//!
//! This module provides SQLite-compatible error codes and a Rust-idiomatic
//! error type that preserves the original SQLite error semantics.

use std::fmt;

/// SQLite primary result codes.
///
/// These codes correspond to the primary result codes defined in sqlite3.h.
/// Extended error codes provide more detail and are represented separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ErrorCode {
    /// Successful result (SQLITE_OK = 0)
    Ok = 0,
    /// Generic error (SQLITE_ERROR = 1)
    Error = 1,
    /// Internal logic error in SQLite (SQLITE_INTERNAL = 2)
    Internal = 2,
    /// Access permission denied (SQLITE_PERM = 3)
    Perm = 3,
    /// Callback routine requested an abort (SQLITE_ABORT = 4)
    Abort = 4,
    /// The database file is locked (SQLITE_BUSY = 5)
    Busy = 5,
    /// A table in the database is locked (SQLITE_LOCKED = 6)
    Locked = 6,
    /// A malloc() failed (SQLITE_NOMEM = 7)
    NoMem = 7,
    /// Attempt to write a readonly database (SQLITE_READONLY = 8)
    ReadOnly = 8,
    /// Operation terminated by sqlite3_interrupt() (SQLITE_INTERRUPT = 9)
    Interrupt = 9,
    /// Some kind of disk I/O error occurred (SQLITE_IOERR = 10)
    IoErr = 10,
    /// The database disk image is malformed (SQLITE_CORRUPT = 11)
    Corrupt = 11,
    /// Unknown opcode in sqlite3_file_control() (SQLITE_NOTFOUND = 12)
    NotFound = 12,
    /// Insertion failed because database is full (SQLITE_FULL = 13)
    Full = 13,
    /// Unable to open the database file (SQLITE_CANTOPEN = 14)
    CantOpen = 14,
    /// Database lock protocol error (SQLITE_PROTOCOL = 15)
    Protocol = 15,
    /// Internal use only (SQLITE_EMPTY = 16)
    Empty = 16,
    /// The database schema changed (SQLITE_SCHEMA = 17)
    Schema = 17,
    /// String or BLOB exceeds size limit (SQLITE_TOOBIG = 18)
    TooBig = 18,
    /// Abort due to constraint violation (SQLITE_CONSTRAINT = 19)
    Constraint = 19,
    /// Data type mismatch (SQLITE_MISMATCH = 20)
    Mismatch = 20,
    /// Library used incorrectly (SQLITE_MISUSE = 21)
    Misuse = 21,
    /// Uses OS features not supported on host (SQLITE_NOLFS = 22)
    NoLfs = 22,
    /// Authorization denied (SQLITE_AUTH = 23)
    Auth = 23,
    /// Not used (SQLITE_FORMAT = 24)
    Format = 24,
    /// 2nd parameter to sqlite3_bind out of range (SQLITE_RANGE = 25)
    Range = 25,
    /// File opened that is not a database file (SQLITE_NOTADB = 26)
    NotADb = 26,
    /// Notifications from sqlite3_log() (SQLITE_NOTICE = 27)
    Notice = 27,
    /// Warnings from sqlite3_log() (SQLITE_WARNING = 28)
    Warning = 28,
    /// sqlite3_step() has another row ready (SQLITE_ROW = 100)
    Row = 100,
    /// sqlite3_step() has finished executing (SQLITE_DONE = 101)
    Done = 101,
}

impl ErrorCode {
    /// Returns the numeric value of the error code
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    /// Create an ErrorCode from a raw i32 value
    pub fn from_i32(code: i32) -> Option<Self> {
        match code {
            0 => Some(ErrorCode::Ok),
            1 => Some(ErrorCode::Error),
            2 => Some(ErrorCode::Internal),
            3 => Some(ErrorCode::Perm),
            4 => Some(ErrorCode::Abort),
            5 => Some(ErrorCode::Busy),
            6 => Some(ErrorCode::Locked),
            7 => Some(ErrorCode::NoMem),
            8 => Some(ErrorCode::ReadOnly),
            9 => Some(ErrorCode::Interrupt),
            10 => Some(ErrorCode::IoErr),
            11 => Some(ErrorCode::Corrupt),
            12 => Some(ErrorCode::NotFound),
            13 => Some(ErrorCode::Full),
            14 => Some(ErrorCode::CantOpen),
            15 => Some(ErrorCode::Protocol),
            16 => Some(ErrorCode::Empty),
            17 => Some(ErrorCode::Schema),
            18 => Some(ErrorCode::TooBig),
            19 => Some(ErrorCode::Constraint),
            20 => Some(ErrorCode::Mismatch),
            21 => Some(ErrorCode::Misuse),
            22 => Some(ErrorCode::NoLfs),
            23 => Some(ErrorCode::Auth),
            24 => Some(ErrorCode::Format),
            25 => Some(ErrorCode::Range),
            26 => Some(ErrorCode::NotADb),
            27 => Some(ErrorCode::Notice),
            28 => Some(ErrorCode::Warning),
            100 => Some(ErrorCode::Row),
            101 => Some(ErrorCode::Done),
            _ => None,
        }
    }

    /// Returns true if this is a success code (Ok, Row, or Done)
    pub fn is_success(self) -> bool {
        matches!(self, ErrorCode::Ok | ErrorCode::Row | ErrorCode::Done)
    }

    /// Returns true if this is an error code
    pub fn is_error(self) -> bool {
        !self.is_success()
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCode::Ok => write!(f, "not an error"),
            ErrorCode::Error => write!(f, "SQL logic error"),
            ErrorCode::Internal => write!(f, "internal error"),
            ErrorCode::Perm => write!(f, "access permission denied"),
            ErrorCode::Abort => write!(f, "query aborted"),
            ErrorCode::Busy => write!(f, "database is locked"),
            ErrorCode::Locked => write!(f, "database table is locked"),
            ErrorCode::NoMem => write!(f, "out of memory"),
            ErrorCode::ReadOnly => write!(f, "attempt to write a readonly database"),
            ErrorCode::Interrupt => write!(f, "interrupted"),
            ErrorCode::IoErr => write!(f, "disk I/O error"),
            ErrorCode::Corrupt => write!(f, "database disk image is malformed"),
            ErrorCode::NotFound => write!(f, "unknown operation"),
            ErrorCode::Full => write!(f, "database or disk is full"),
            ErrorCode::CantOpen => write!(f, "unable to open database file"),
            ErrorCode::Protocol => write!(f, "locking protocol"),
            ErrorCode::Empty => write!(f, "empty"),
            ErrorCode::Schema => write!(f, "database schema has changed"),
            ErrorCode::TooBig => write!(f, "string or blob too big"),
            ErrorCode::Constraint => write!(f, "constraint failed"),
            ErrorCode::Mismatch => write!(f, "datatype mismatch"),
            ErrorCode::Misuse => write!(f, "bad parameter or other API misuse"),
            ErrorCode::NoLfs => write!(f, "large file support is disabled"),
            ErrorCode::Auth => write!(f, "authorization denied"),
            ErrorCode::Format => write!(f, "auxiliary database format error"),
            ErrorCode::Range => write!(f, "column index out of range"),
            ErrorCode::NotADb => write!(f, "file is not a database"),
            ErrorCode::Notice => write!(f, "notification message"),
            ErrorCode::Warning => write!(f, "warning message"),
            ErrorCode::Row => write!(f, "another row available"),
            ErrorCode::Done => write!(f, "no more rows available"),
        }
    }
}

/// Extended error codes for more specific error information.
///
/// These provide additional detail beyond the primary error codes.
/// The extended code includes the primary code in its lower 8 bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExtendedErrorCode {
    // SQLITE_ERROR extended codes
    ErrorMissingCollseq,
    ErrorRetry,
    ErrorSnapshot,
    ErrorReserveSize,
    ErrorKey,
    ErrorUnable,

    // SQLITE_IOERR extended codes
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
    IoErrVnode,
    IoErrAuth,
    IoErrBeginAtomic,
    IoErrCommitAtomic,
    IoErrRollbackAtomic,
    IoErrData,
    IoErrCorruptFs,
    IoErrInPage,
    IoErrBadKey,
    IoErrCodec,

    // SQLITE_LOCKED extended codes
    LockedSharedCache,
    LockedVTab,

    // SQLITE_BUSY extended codes
    BusyRecovery,
    BusySnapshot,
    BusyTimeout,

    // SQLITE_CANTOPEN extended codes
    CantOpenNoTempDir,
    CantOpenIsDir,
    CantOpenFullPath,
    CantOpenConvPath,
    CantOpenDirtyWal,
    CantOpenSymlink,

    // SQLITE_CORRUPT extended codes
    CorruptVTab,
    CorruptSequence,
    CorruptIndex,

    // SQLITE_READONLY extended codes
    ReadOnlyRecovery,
    ReadOnlyCantLock,
    ReadOnlyRollback,
    ReadOnlyDbMoved,
    ReadOnlyCantInit,
    ReadOnlyDirectory,

    // SQLITE_ABORT extended codes
    AbortRollback,

    // SQLITE_CONSTRAINT extended codes
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

    // SQLITE_NOTICE extended codes
    NoticeRecoverWal,
    NoticeRecoverRollback,
    NoticeRbu,

    // SQLITE_WARNING extended codes
    WarningAutoIndex,

    // SQLITE_AUTH extended codes
    AuthUser,

    // SQLITE_OK extended codes
    OkLoadPermanently,
    OkSymlink,
}

impl ExtendedErrorCode {
    /// Returns the numeric SQLite extended error code value.
    /// Extended codes are: (primary_code | (subcode << 8))
    pub fn as_i32(&self) -> i32 {
        let primary = self.primary_code().as_i32();
        let subcode = match self {
            // Error subcodes (1-6)
            ExtendedErrorCode::ErrorMissingCollseq => 1,
            ExtendedErrorCode::ErrorRetry => 2,
            ExtendedErrorCode::ErrorSnapshot => 3,
            ExtendedErrorCode::ErrorReserveSize => 4,
            ExtendedErrorCode::ErrorKey => 5,
            ExtendedErrorCode::ErrorUnable => 6,

            // IoErr subcodes (1-35)
            ExtendedErrorCode::IoErrRead => 1,
            ExtendedErrorCode::IoErrShortRead => 2,
            ExtendedErrorCode::IoErrWrite => 3,
            ExtendedErrorCode::IoErrFsync => 4,
            ExtendedErrorCode::IoErrDirFsync => 5,
            ExtendedErrorCode::IoErrTruncate => 6,
            ExtendedErrorCode::IoErrFstat => 7,
            ExtendedErrorCode::IoErrUnlock => 8,
            ExtendedErrorCode::IoErrRdLock => 9,
            ExtendedErrorCode::IoErrDelete => 10,
            ExtendedErrorCode::IoErrBlocked => 11,
            ExtendedErrorCode::IoErrNoMem => 12,
            ExtendedErrorCode::IoErrAccess => 13,
            ExtendedErrorCode::IoErrCheckReservedLock => 14,
            ExtendedErrorCode::IoErrLock => 15,
            ExtendedErrorCode::IoErrClose => 16,
            ExtendedErrorCode::IoErrDirClose => 17,
            ExtendedErrorCode::IoErrShmOpen => 18,
            ExtendedErrorCode::IoErrShmSize => 19,
            ExtendedErrorCode::IoErrShmLock => 20,
            ExtendedErrorCode::IoErrShmMap => 21,
            ExtendedErrorCode::IoErrSeek => 22,
            ExtendedErrorCode::IoErrDeleteNoEnt => 23,
            ExtendedErrorCode::IoErrMmap => 24,
            ExtendedErrorCode::IoErrGetTempPath => 25,
            ExtendedErrorCode::IoErrConvPath => 26,
            ExtendedErrorCode::IoErrVnode => 27,
            ExtendedErrorCode::IoErrAuth => 28,
            ExtendedErrorCode::IoErrBeginAtomic => 29,
            ExtendedErrorCode::IoErrCommitAtomic => 30,
            ExtendedErrorCode::IoErrRollbackAtomic => 31,
            ExtendedErrorCode::IoErrData => 32,
            ExtendedErrorCode::IoErrCorruptFs => 33,
            ExtendedErrorCode::IoErrInPage => 34,
            ExtendedErrorCode::IoErrBadKey => 35,
            ExtendedErrorCode::IoErrCodec => 36,

            // Locked subcodes
            ExtendedErrorCode::LockedSharedCache => 1,
            ExtendedErrorCode::LockedVTab => 2,

            // Busy subcodes
            ExtendedErrorCode::BusyRecovery => 1,
            ExtendedErrorCode::BusySnapshot => 2,
            ExtendedErrorCode::BusyTimeout => 3,

            // CantOpen subcodes
            ExtendedErrorCode::CantOpenNoTempDir => 1,
            ExtendedErrorCode::CantOpenIsDir => 2,
            ExtendedErrorCode::CantOpenFullPath => 3,
            ExtendedErrorCode::CantOpenConvPath => 4,
            ExtendedErrorCode::CantOpenDirtyWal => 5,
            ExtendedErrorCode::CantOpenSymlink => 6,

            // Corrupt subcodes
            ExtendedErrorCode::CorruptVTab => 1,
            ExtendedErrorCode::CorruptSequence => 2,
            ExtendedErrorCode::CorruptIndex => 3,

            // ReadOnly subcodes
            ExtendedErrorCode::ReadOnlyRecovery => 1,
            ExtendedErrorCode::ReadOnlyCantLock => 2,
            ExtendedErrorCode::ReadOnlyRollback => 3,
            ExtendedErrorCode::ReadOnlyDbMoved => 4,
            ExtendedErrorCode::ReadOnlyCantInit => 5,
            ExtendedErrorCode::ReadOnlyDirectory => 6,

            // Abort subcodes
            ExtendedErrorCode::AbortRollback => 2,

            // Constraint subcodes - these are the important ones for trigger tests
            ExtendedErrorCode::ConstraintCheck => 1,
            ExtendedErrorCode::ConstraintCommitHook => 2,
            ExtendedErrorCode::ConstraintForeignKey => 3,
            ExtendedErrorCode::ConstraintFunction => 4,
            ExtendedErrorCode::ConstraintNotNull => 5,
            ExtendedErrorCode::ConstraintPrimaryKey => 6,
            ExtendedErrorCode::ConstraintTrigger => 7, // SQLITE_CONSTRAINT_TRIGGER
            ExtendedErrorCode::ConstraintUnique => 8,
            ExtendedErrorCode::ConstraintVTab => 9,
            ExtendedErrorCode::ConstraintRowId => 10,
            ExtendedErrorCode::ConstraintPinned => 11,
            ExtendedErrorCode::ConstraintDataType => 12,

            // Notice subcodes
            ExtendedErrorCode::NoticeRecoverWal => 1,
            ExtendedErrorCode::NoticeRecoverRollback => 2,
            ExtendedErrorCode::NoticeRbu => 3,

            // Warning subcodes
            ExtendedErrorCode::WarningAutoIndex => 1,

            // Auth subcodes
            ExtendedErrorCode::AuthUser => 1,

            // Ok subcodes
            ExtendedErrorCode::OkLoadPermanently => 1,
            ExtendedErrorCode::OkSymlink => 2,
        };
        primary | (subcode << 8)
    }

    /// Returns the primary error code for this extended code
    pub fn primary_code(&self) -> ErrorCode {
        match self {
            ExtendedErrorCode::ErrorMissingCollseq
            | ExtendedErrorCode::ErrorRetry
            | ExtendedErrorCode::ErrorSnapshot
            | ExtendedErrorCode::ErrorReserveSize
            | ExtendedErrorCode::ErrorKey
            | ExtendedErrorCode::ErrorUnable => ErrorCode::Error,

            ExtendedErrorCode::IoErrRead
            | ExtendedErrorCode::IoErrShortRead
            | ExtendedErrorCode::IoErrWrite
            | ExtendedErrorCode::IoErrFsync
            | ExtendedErrorCode::IoErrDirFsync
            | ExtendedErrorCode::IoErrTruncate
            | ExtendedErrorCode::IoErrFstat
            | ExtendedErrorCode::IoErrUnlock
            | ExtendedErrorCode::IoErrRdLock
            | ExtendedErrorCode::IoErrDelete
            | ExtendedErrorCode::IoErrBlocked
            | ExtendedErrorCode::IoErrNoMem
            | ExtendedErrorCode::IoErrAccess
            | ExtendedErrorCode::IoErrCheckReservedLock
            | ExtendedErrorCode::IoErrLock
            | ExtendedErrorCode::IoErrClose
            | ExtendedErrorCode::IoErrDirClose
            | ExtendedErrorCode::IoErrShmOpen
            | ExtendedErrorCode::IoErrShmSize
            | ExtendedErrorCode::IoErrShmLock
            | ExtendedErrorCode::IoErrShmMap
            | ExtendedErrorCode::IoErrSeek
            | ExtendedErrorCode::IoErrDeleteNoEnt
            | ExtendedErrorCode::IoErrMmap
            | ExtendedErrorCode::IoErrGetTempPath
            | ExtendedErrorCode::IoErrConvPath
            | ExtendedErrorCode::IoErrVnode
            | ExtendedErrorCode::IoErrAuth
            | ExtendedErrorCode::IoErrBeginAtomic
            | ExtendedErrorCode::IoErrCommitAtomic
            | ExtendedErrorCode::IoErrRollbackAtomic
            | ExtendedErrorCode::IoErrData
            | ExtendedErrorCode::IoErrCorruptFs
            | ExtendedErrorCode::IoErrInPage
            | ExtendedErrorCode::IoErrBadKey
            | ExtendedErrorCode::IoErrCodec => ErrorCode::IoErr,

            ExtendedErrorCode::LockedSharedCache | ExtendedErrorCode::LockedVTab => {
                ErrorCode::Locked
            }

            ExtendedErrorCode::BusyRecovery
            | ExtendedErrorCode::BusySnapshot
            | ExtendedErrorCode::BusyTimeout => ErrorCode::Busy,

            ExtendedErrorCode::CantOpenNoTempDir
            | ExtendedErrorCode::CantOpenIsDir
            | ExtendedErrorCode::CantOpenFullPath
            | ExtendedErrorCode::CantOpenConvPath
            | ExtendedErrorCode::CantOpenDirtyWal
            | ExtendedErrorCode::CantOpenSymlink => ErrorCode::CantOpen,

            ExtendedErrorCode::CorruptVTab
            | ExtendedErrorCode::CorruptSequence
            | ExtendedErrorCode::CorruptIndex => ErrorCode::Corrupt,

            ExtendedErrorCode::ReadOnlyRecovery
            | ExtendedErrorCode::ReadOnlyCantLock
            | ExtendedErrorCode::ReadOnlyRollback
            | ExtendedErrorCode::ReadOnlyDbMoved
            | ExtendedErrorCode::ReadOnlyCantInit
            | ExtendedErrorCode::ReadOnlyDirectory => ErrorCode::ReadOnly,

            ExtendedErrorCode::AbortRollback => ErrorCode::Abort,

            ExtendedErrorCode::ConstraintCheck
            | ExtendedErrorCode::ConstraintCommitHook
            | ExtendedErrorCode::ConstraintForeignKey
            | ExtendedErrorCode::ConstraintFunction
            | ExtendedErrorCode::ConstraintNotNull
            | ExtendedErrorCode::ConstraintPrimaryKey
            | ExtendedErrorCode::ConstraintTrigger
            | ExtendedErrorCode::ConstraintUnique
            | ExtendedErrorCode::ConstraintVTab
            | ExtendedErrorCode::ConstraintRowId
            | ExtendedErrorCode::ConstraintPinned
            | ExtendedErrorCode::ConstraintDataType => ErrorCode::Constraint,

            ExtendedErrorCode::NoticeRecoverWal
            | ExtendedErrorCode::NoticeRecoverRollback
            | ExtendedErrorCode::NoticeRbu => ErrorCode::Notice,

            ExtendedErrorCode::WarningAutoIndex => ErrorCode::Warning,

            ExtendedErrorCode::AuthUser => ErrorCode::Auth,

            ExtendedErrorCode::OkLoadPermanently | ExtendedErrorCode::OkSymlink => ErrorCode::Ok,
        }
    }
}

impl fmt::Display for ExtendedErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Error extended
            ExtendedErrorCode::ErrorMissingCollseq => write!(f, "missing collation sequence"),
            ExtendedErrorCode::ErrorRetry => write!(f, "retry"),
            ExtendedErrorCode::ErrorSnapshot => write!(f, "snapshot error"),
            ExtendedErrorCode::ErrorReserveSize => write!(f, "reserve size error"),
            ExtendedErrorCode::ErrorKey => write!(f, "key error"),
            ExtendedErrorCode::ErrorUnable => write!(f, "unable to complete operation"),

            // IoErr extended
            ExtendedErrorCode::IoErrRead => write!(f, "read error"),
            ExtendedErrorCode::IoErrShortRead => write!(f, "short read"),
            ExtendedErrorCode::IoErrWrite => write!(f, "write error"),
            ExtendedErrorCode::IoErrFsync => write!(f, "fsync error"),
            ExtendedErrorCode::IoErrDirFsync => write!(f, "directory fsync error"),
            ExtendedErrorCode::IoErrTruncate => write!(f, "truncate error"),
            ExtendedErrorCode::IoErrFstat => write!(f, "fstat error"),
            ExtendedErrorCode::IoErrUnlock => write!(f, "unlock error"),
            ExtendedErrorCode::IoErrRdLock => write!(f, "read lock error"),
            ExtendedErrorCode::IoErrDelete => write!(f, "delete error"),
            ExtendedErrorCode::IoErrBlocked => write!(f, "blocked"),
            ExtendedErrorCode::IoErrNoMem => write!(f, "out of memory during I/O"),
            ExtendedErrorCode::IoErrAccess => write!(f, "access error"),
            ExtendedErrorCode::IoErrCheckReservedLock => write!(f, "check reserved lock error"),
            ExtendedErrorCode::IoErrLock => write!(f, "lock error"),
            ExtendedErrorCode::IoErrClose => write!(f, "close error"),
            ExtendedErrorCode::IoErrDirClose => write!(f, "directory close error"),
            ExtendedErrorCode::IoErrShmOpen => write!(f, "shared memory open error"),
            ExtendedErrorCode::IoErrShmSize => write!(f, "shared memory size error"),
            ExtendedErrorCode::IoErrShmLock => write!(f, "shared memory lock error"),
            ExtendedErrorCode::IoErrShmMap => write!(f, "shared memory map error"),
            ExtendedErrorCode::IoErrSeek => write!(f, "seek error"),
            ExtendedErrorCode::IoErrDeleteNoEnt => write!(f, "delete file not found"),
            ExtendedErrorCode::IoErrMmap => write!(f, "mmap error"),
            ExtendedErrorCode::IoErrGetTempPath => write!(f, "get temp path error"),
            ExtendedErrorCode::IoErrConvPath => write!(f, "path conversion error"),
            ExtendedErrorCode::IoErrVnode => write!(f, "vnode error"),
            ExtendedErrorCode::IoErrAuth => write!(f, "authorization error during I/O"),
            ExtendedErrorCode::IoErrBeginAtomic => write!(f, "begin atomic error"),
            ExtendedErrorCode::IoErrCommitAtomic => write!(f, "commit atomic error"),
            ExtendedErrorCode::IoErrRollbackAtomic => write!(f, "rollback atomic error"),
            ExtendedErrorCode::IoErrData => write!(f, "data error"),
            ExtendedErrorCode::IoErrCorruptFs => write!(f, "corrupt filesystem"),
            ExtendedErrorCode::IoErrInPage => write!(f, "in-page I/O error"),
            ExtendedErrorCode::IoErrBadKey => write!(f, "bad encryption key"),
            ExtendedErrorCode::IoErrCodec => write!(f, "codec error"),

            // Locked extended
            ExtendedErrorCode::LockedSharedCache => write!(f, "locked in shared cache"),
            ExtendedErrorCode::LockedVTab => write!(f, "virtual table locked"),

            // Busy extended
            ExtendedErrorCode::BusyRecovery => write!(f, "busy during recovery"),
            ExtendedErrorCode::BusySnapshot => write!(f, "busy snapshot"),
            ExtendedErrorCode::BusyTimeout => write!(f, "busy timeout"),

            // CantOpen extended
            ExtendedErrorCode::CantOpenNoTempDir => write!(f, "no temp directory"),
            ExtendedErrorCode::CantOpenIsDir => write!(f, "path is a directory"),
            ExtendedErrorCode::CantOpenFullPath => write!(f, "cannot get full path"),
            ExtendedErrorCode::CantOpenConvPath => write!(f, "path conversion failed"),
            ExtendedErrorCode::CantOpenDirtyWal => write!(f, "dirty WAL"),
            ExtendedErrorCode::CantOpenSymlink => write!(f, "symlink not allowed"),

            // Corrupt extended
            ExtendedErrorCode::CorruptVTab => write!(f, "virtual table corrupt"),
            ExtendedErrorCode::CorruptSequence => write!(f, "sequence corrupt"),
            ExtendedErrorCode::CorruptIndex => write!(f, "index corrupt"),

            // ReadOnly extended
            ExtendedErrorCode::ReadOnlyRecovery => write!(f, "readonly during recovery"),
            ExtendedErrorCode::ReadOnlyCantLock => write!(f, "readonly cannot lock"),
            ExtendedErrorCode::ReadOnlyRollback => write!(f, "readonly rollback"),
            ExtendedErrorCode::ReadOnlyDbMoved => write!(f, "database moved"),
            ExtendedErrorCode::ReadOnlyCantInit => write!(f, "readonly cannot init"),
            ExtendedErrorCode::ReadOnlyDirectory => write!(f, "readonly directory"),

            // Abort extended
            ExtendedErrorCode::AbortRollback => write!(f, "abort due to rollback"),

            // Constraint extended
            ExtendedErrorCode::ConstraintCheck => write!(f, "CHECK constraint failed"),
            ExtendedErrorCode::ConstraintCommitHook => write!(f, "commit hook constraint"),
            ExtendedErrorCode::ConstraintForeignKey => write!(f, "FOREIGN KEY constraint failed"),
            ExtendedErrorCode::ConstraintFunction => write!(f, "function constraint"),
            ExtendedErrorCode::ConstraintNotNull => write!(f, "NOT NULL constraint failed"),
            ExtendedErrorCode::ConstraintPrimaryKey => write!(f, "PRIMARY KEY constraint failed"),
            ExtendedErrorCode::ConstraintTrigger => write!(f, "trigger constraint"),
            ExtendedErrorCode::ConstraintUnique => write!(f, "UNIQUE constraint failed"),
            ExtendedErrorCode::ConstraintVTab => write!(f, "virtual table constraint"),
            ExtendedErrorCode::ConstraintRowId => write!(f, "rowid constraint"),
            ExtendedErrorCode::ConstraintPinned => write!(f, "pinned constraint"),
            ExtendedErrorCode::ConstraintDataType => write!(f, "datatype constraint"),

            // Notice extended
            ExtendedErrorCode::NoticeRecoverWal => write!(f, "recovering WAL"),
            ExtendedErrorCode::NoticeRecoverRollback => write!(f, "recovering rollback journal"),
            ExtendedErrorCode::NoticeRbu => write!(f, "RBU notice"),

            // Warning extended
            ExtendedErrorCode::WarningAutoIndex => write!(f, "automatic index"),

            // Auth extended
            ExtendedErrorCode::AuthUser => write!(f, "user authorization denied"),

            // Ok extended
            ExtendedErrorCode::OkLoadPermanently => write!(f, "extension loaded permanently"),
            ExtendedErrorCode::OkSymlink => write!(f, "symlink resolved"),
        }
    }
}

/// Main error type for RustQL.
///
/// This error type preserves SQLite error semantics while providing
/// a Rust-idiomatic interface. It includes:
/// - A primary error code
/// - An optional extended error code for more detail
/// - An optional error message
/// - An optional byte offset for parse errors
#[derive(Debug)]
pub struct Error {
    /// Primary result code
    pub code: ErrorCode,
    /// Extended result code (provides more detail)
    pub extended: Option<ExtendedErrorCode>,
    /// Human-readable error message
    pub message: Option<String>,
    /// Byte offset in SQL for parse errors
    pub offset: Option<i32>,
    /// On-error action from trigger RAISE: 1=ROLLBACK, 2=ABORT, 3=FAIL
    /// Used by the statement executor to determine rollback behavior
    pub on_error_action: Option<u8>,
}

impl Error {
    /// Create a new error with just a primary code
    pub fn new(code: ErrorCode) -> Self {
        Error {
            code,
            extended: None,
            message: None,
            offset: None,
            on_error_action: None,
        }
    }

    /// Create an error with a message
    pub fn with_message(code: ErrorCode, msg: impl Into<String>) -> Self {
        Error {
            code,
            extended: None,
            message: Some(msg.into()),
            offset: None,
            on_error_action: None,
        }
    }

    /// Create an error with an extended code
    pub fn with_extended(code: ErrorCode, ext: ExtendedErrorCode) -> Self {
        Error {
            code,
            extended: Some(ext),
            message: None,
            offset: None,
            on_error_action: None,
        }
    }

    /// Create an error from just an extended code (derives primary code)
    pub fn from_extended(ext: ExtendedErrorCode) -> Self {
        Error {
            code: ext.primary_code(),
            extended: Some(ext),
            message: None,
            offset: None,
            on_error_action: None,
        }
    }

    /// Create a parse error with byte offset
    pub fn parse_error(msg: impl Into<String>, offset: i32) -> Self {
        Error {
            code: ErrorCode::Error,
            extended: None,
            message: Some(msg.into()),
            offset: Some(offset),
            on_error_action: None,
        }
    }

    /// Create an internal error (convenience for stubs)
    pub fn internal() -> Self {
        Error::new(ErrorCode::Internal)
    }

    /// Add a message to an existing error
    pub fn set_message(&mut self, msg: impl Into<String>) {
        self.message = Some(msg.into());
    }

    /// Add an extended code to an existing error
    pub fn set_extended(&mut self, ext: ExtendedErrorCode) {
        self.extended = Some(ext);
    }

    /// Get the SQLite-compatible error message (just the message, no code prefix)
    /// This mimics sqlite3_errmsg() behavior
    pub fn sqlite_errmsg(&self) -> String {
        if let Some(ref msg) = self.message {
            msg.clone()
        } else {
            // Fall back to error code description if no message
            format!("{}", self.code)
        }
    }
}

impl Default for Error {
    fn default() -> Self {
        Error::new(ErrorCode::Internal)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Start with primary code description
        write!(f, "{}", self.code)?;

        // Add extended code info if present
        if let Some(ext) = &self.extended {
            write!(f, " ({})", ext)?;
        }

        // Add custom message if present
        if let Some(msg) = &self.message {
            write!(f, ": {}", msg)?;
        }

        // Add offset for parse errors
        if let Some(offset) = self.offset {
            write!(f, " at byte {}", offset)?;
        }

        Ok(())
    }
}

impl std::error::Error for Error {}

impl From<ErrorCode> for Error {
    fn from(code: ErrorCode) -> Self {
        Error::new(code)
    }
}

impl From<ExtendedErrorCode> for Error {
    fn from(ext: ExtendedErrorCode) -> Self {
        Error::from_extended(ext)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        use std::io::ErrorKind;

        let (code, extended) = match e.kind() {
            ErrorKind::NotFound => (ErrorCode::CantOpen, None),
            ErrorKind::PermissionDenied => (ErrorCode::Perm, None),
            ErrorKind::AlreadyExists => (ErrorCode::Constraint, None),
            ErrorKind::WouldBlock => (ErrorCode::Busy, None),
            ErrorKind::InvalidInput => (ErrorCode::Misuse, None),
            ErrorKind::InvalidData => (ErrorCode::Corrupt, None),
            ErrorKind::TimedOut => (ErrorCode::Busy, Some(ExtendedErrorCode::BusyTimeout)),
            ErrorKind::Interrupted => (ErrorCode::Interrupt, None),
            ErrorKind::OutOfMemory => (ErrorCode::NoMem, None),
            _ => (ErrorCode::IoErr, None),
        };

        Error {
            code,
            extended,
            message: Some(e.to_string()),
            offset: None,
            on_error_action: None,
        }
    }
}

/// Result type alias for RustQL operations.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_values() {
        assert_eq!(ErrorCode::Ok.as_i32(), 0);
        assert_eq!(ErrorCode::Error.as_i32(), 1);
        assert_eq!(ErrorCode::Row.as_i32(), 100);
        assert_eq!(ErrorCode::Done.as_i32(), 101);
    }

    #[test]
    fn test_error_code_from_i32() {
        assert_eq!(ErrorCode::from_i32(0), Some(ErrorCode::Ok));
        assert_eq!(ErrorCode::from_i32(1), Some(ErrorCode::Error));
        assert_eq!(ErrorCode::from_i32(999), None);
    }

    #[test]
    fn test_is_success() {
        assert!(ErrorCode::Ok.is_success());
        assert!(ErrorCode::Row.is_success());
        assert!(ErrorCode::Done.is_success());
        assert!(!ErrorCode::Error.is_success());
        assert!(!ErrorCode::Busy.is_success());
    }

    #[test]
    fn test_extended_primary_code() {
        assert_eq!(
            ExtendedErrorCode::IoErrRead.primary_code(),
            ErrorCode::IoErr
        );
        assert_eq!(
            ExtendedErrorCode::ConstraintUnique.primary_code(),
            ErrorCode::Constraint
        );
        assert_eq!(
            ExtendedErrorCode::BusyTimeout.primary_code(),
            ErrorCode::Busy
        );
    }

    #[test]
    fn test_error_display() {
        let err = Error::new(ErrorCode::Busy);
        assert_eq!(format!("{}", err), "database is locked");

        let err = Error::with_message(ErrorCode::Error, "syntax error");
        assert_eq!(format!("{}", err), "SQL logic error: syntax error");

        let err = Error::from_extended(ExtendedErrorCode::ConstraintUnique);
        assert_eq!(
            format!("{}", err),
            "constraint failed (UNIQUE constraint failed)"
        );
    }

    #[test]
    fn test_parse_error() {
        let err = Error::parse_error("near \"SELEC\": syntax error", 0);
        assert_eq!(
            format!("{}", err),
            "SQL logic error: near \"SELEC\": syntax error at byte 0"
        );
    }
}
