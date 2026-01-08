//! Virtual Database Engine (VDBE)

pub mod auxdata;
pub mod bytecode;
pub mod engine;
pub mod expr;
pub mod mem;
pub mod ops;
pub mod sort;
pub mod trace;
pub mod types;
pub mod value;

pub use value::{
    sqlite3_aggregate_context, sqlite3_result_blob, sqlite3_result_double, sqlite3_result_error,
    sqlite3_result_int, sqlite3_result_int64, sqlite3_result_null, sqlite3_result_text,
    sqlite3_result_value, sqlite3_value_blob, sqlite3_value_bytes, sqlite3_value_double,
    sqlite3_value_dup, sqlite3_value_int, sqlite3_value_int64, sqlite3_value_text,
    sqlite3_value_type, FunctionContext, SqliteValue,
};

pub use auxdata::{
    decode_record_header, deserialize_value, explain_program, get_varint, make_record, put_varint,
    varint_len, Label, SerialType, VdbeBuilder,
};

pub use types::{
    compare_flags, insert_flags, seek_flags, CollSeq, CursorType, Encoding, TypeClass,
    SQLITE_MAX_VARIABLE_NUMBER, VDBE_MAGIC_DEAD, VDBE_MAGIC_HALT, VDBE_MAGIC_INIT,
    VDBE_MAGIC_RESET, VDBE_MAGIC_RUN,
};

pub use sort::{SorterRecord, SorterState, VdbeSorter};

pub use trace::{expand_sql, TraceCallback, TraceEvent, TraceFlags, TraceInfo, Tracer};

pub use bytecode::{
    bytecode_schema, explain_bytecode, explain_query_plan, BytecodeIterator, BytecodeRow,
};
