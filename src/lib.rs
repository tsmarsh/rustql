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
pub mod os;
pub mod parser;
pub mod schema;
pub mod storage;
pub mod types;
pub mod utf;
pub mod util;
pub mod vdbe;

// Re-export main public types
pub use error::{Error, Result};

// Re-export API types and functions
pub use api::{
    sqlite3_changes, sqlite3_close, sqlite3_column_count, sqlite3_column_double,
    sqlite3_column_int, sqlite3_column_int64, sqlite3_column_name, sqlite3_column_text,
    sqlite3_column_type, sqlite3_column_value, sqlite3_errcode, sqlite3_errmsg, sqlite3_errstr,
    sqlite3_finalize, sqlite3_initialize, sqlite3_last_insert_rowid, sqlite3_libversion,
    sqlite3_libversion_number, sqlite3_open, sqlite3_open_v2, sqlite3_prepare_v2, sqlite3_reset,
    sqlite3_shutdown, sqlite3_step, sqlite3_total_changes, PreparedStmt, SqliteConnection,
};
