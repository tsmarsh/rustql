//! RustQL - SQLite3 rewritten in Rust

// Allow dead code during development - many components are implemented
// but not yet integrated into the full execution pipeline
#![allow(dead_code)]
// Allow using 3.14 etc in tests without complaining about Pi approximation
#![allow(clippy::approx_constant)]

pub mod api;
pub mod error;
pub mod executor;
pub mod functions;
pub mod mem;
pub mod os;
pub mod parser;
pub mod random;
pub mod rtree;
pub mod schema;
pub mod storage;
pub mod types;
pub mod utf;
pub mod util;
pub mod vdbe;

// Re-export main public types
pub use error::{Error, Result};

// Re-export memory allocation functions
pub use mem::{
    sqlite3_free, sqlite3_malloc, sqlite3_memory_highwater, sqlite3_memory_used, sqlite3_msize,
    sqlite3_realloc, sqlite3_soft_heap_limit64, sqlite3_status, sqlite3_status64, StatusOp,
};

// Re-export random functions
pub use random::{
    sqlite3_prng_reset, sqlite3_prng_seed, sqlite3_random_blob, sqlite3_random_int64,
    sqlite3_randomness, sqlite3_temp_file_path, sqlite3_temp_filename,
};

// Re-export API types and functions
pub use api::{
    sqlite3_backup_finish, sqlite3_backup_init, sqlite3_backup_pagecount, sqlite3_backup_remaining,
    sqlite3_backup_step, sqlite3_changes, sqlite3_close, sqlite3_column_count,
    sqlite3_column_double, sqlite3_column_int, sqlite3_column_int64, sqlite3_column_name,
    sqlite3_column_text, sqlite3_column_type, sqlite3_column_value, sqlite3_db_status,
    sqlite3_db_status64, sqlite3_errcode, sqlite3_errmsg, sqlite3_errstr, sqlite3_finalize,
    sqlite3_initialize, sqlite3_last_insert_rowid, sqlite3_libversion, sqlite3_libversion_number,
    sqlite3_open, sqlite3_open_v2, sqlite3_prepare_v2, sqlite3_reset, sqlite3_shutdown,
    sqlite3_step, sqlite3_total_changes, Backup, BackupStepResult, DbStatusOp, PreparedStmt,
    SqliteConnection,
};

pub use rtree::{RtreeBbox, RtreeConstraint, RtreeResult, RtreeTable};
