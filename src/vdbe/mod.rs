//! Virtual Database Engine (VDBE)

pub mod engine;
pub mod ops;
pub mod mem;
pub mod sort;
pub mod expr;
pub mod value;
pub mod aux;
pub mod types;

pub use value::{
    SqliteValue, FunctionContext,
    sqlite3_value_type, sqlite3_value_int, sqlite3_value_int64,
    sqlite3_value_double, sqlite3_value_text, sqlite3_value_blob,
    sqlite3_value_bytes, sqlite3_value_dup,
    sqlite3_result_null, sqlite3_result_int, sqlite3_result_int64,
    sqlite3_result_double, sqlite3_result_text, sqlite3_result_blob,
    sqlite3_result_error, sqlite3_result_value,
    sqlite3_aggregate_context,
};

pub use aux::{
    SerialType, VdbeBuilder, Label,
    get_varint, put_varint, varint_len,
    decode_record_header, make_record, deserialize_value,
    explain_program,
};

pub use types::{
    CursorType, CollSeq, Encoding, TypeClass,
    VDBE_MAGIC_INIT, VDBE_MAGIC_RUN, VDBE_MAGIC_HALT,
    VDBE_MAGIC_RESET, VDBE_MAGIC_DEAD,
    SQLITE_MAX_VARIABLE_NUMBER,
    compare_flags, seek_flags, insert_flags,
};
