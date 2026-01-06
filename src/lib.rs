//! RustQL - SQLite3 rewritten in Rust

pub mod error;
pub mod types;
pub mod storage;
pub mod vdbe;
pub mod parser;
pub mod executor;
pub mod functions;
pub mod os;
pub mod util;
pub mod schema;
pub mod api;

// Re-export main public types
pub use error::{Error, Result};

// Re-export API types and functions
pub use api::{
    SqliteConnection, PreparedStmt,
    sqlite3_open, sqlite3_open_v2, sqlite3_close,
    sqlite3_prepare_v2, sqlite3_step, sqlite3_reset, sqlite3_finalize,
    sqlite3_errcode, sqlite3_errmsg, sqlite3_errstr,
    sqlite3_changes, sqlite3_total_changes, sqlite3_last_insert_rowid,
    sqlite3_libversion, sqlite3_libversion_number,
    sqlite3_initialize, sqlite3_shutdown,
};
