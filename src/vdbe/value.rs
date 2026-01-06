//! Value API for VDBE
//!
//! Implements sqlite3_value_* and sqlite3_result_* functions for
//! extracting values and setting results in user-defined functions.

use crate::error::{Error, ErrorCode, Result};
use crate::types::{ColumnType, Value};
use crate::vdbe::mem::Mem;

// ============================================================================
// Protected Value (sqlite3_value)
// ============================================================================

/// A protected value for use in SQL functions
///
/// This corresponds to sqlite3_value in the C API. Values are protected
/// meaning they cannot be modified by the function implementation.
#[derive(Debug, Clone)]
pub struct SqliteValue {
    /// The underlying memory cell
    mem: Mem,
}

impl SqliteValue {
    /// Create a new NULL value
    pub fn new() -> Self {
        Self { mem: Mem::new() }
    }

    /// Create from a Mem cell
    pub fn from_mem(mem: &Mem) -> Self {
        Self { mem: mem.clone() }
    }

    /// Create from a Value
    pub fn from_value(value: &Value) -> Self {
        Self {
            mem: Mem::from_value(value),
        }
    }
}

impl Default for SqliteValue {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Value Extraction (sqlite3_value_*)
// ============================================================================

/// sqlite3_value_type - Get the type of a value
pub fn sqlite3_value_type(value: &SqliteValue) -> ColumnType {
    value.mem.column_type()
}

/// sqlite3_value_numeric_type - Get numeric type after conversion
pub fn sqlite3_value_numeric_type(value: &SqliteValue) -> ColumnType {
    match value.mem.column_type() {
        ColumnType::Null => ColumnType::Null,
        ColumnType::Integer => ColumnType::Integer,
        ColumnType::Float => ColumnType::Float,
        ColumnType::Text => {
            // Try to determine if it's integer or float
            let s = value.mem.to_str();
            if s.parse::<i64>().is_ok() {
                ColumnType::Integer
            } else if s.parse::<f64>().is_ok() {
                ColumnType::Float
            } else {
                ColumnType::Float // Default to float for non-numeric strings
            }
        }
        ColumnType::Blob => ColumnType::Float,
    }
}

/// sqlite3_value_nochange - Check if value unchanged in UPDATE
pub fn sqlite3_value_nochange(value: &SqliteValue) -> bool {
    // This would be set by OP_Param for unchanged columns
    // For now, return false as we don't track this yet
    let _ = value;
    false
}

/// sqlite3_value_frombind - Check if value came from bind
pub fn sqlite3_value_frombind(value: &SqliteValue) -> bool {
    // Check if SUBTYPE flag is set (we use it to track bound values)
    use crate::vdbe::mem::MemFlags;
    value.mem.flags.contains(MemFlags::SUBTYPE)
}

/// sqlite3_value_int - Get value as 32-bit integer
pub fn sqlite3_value_int(value: &SqliteValue) -> i32 {
    sqlite3_value_int64(value) as i32
}

/// sqlite3_value_int64 - Get value as 64-bit integer
pub fn sqlite3_value_int64(value: &SqliteValue) -> i64 {
    value.mem.to_int()
}

/// sqlite3_value_double - Get value as double
pub fn sqlite3_value_double(value: &SqliteValue) -> f64 {
    value.mem.to_real()
}

/// sqlite3_value_text - Get value as UTF-8 text
pub fn sqlite3_value_text(value: &SqliteValue) -> String {
    value.mem.to_str()
}

/// sqlite3_value_text16 - Get value as UTF-16 text
pub fn sqlite3_value_text16(value: &SqliteValue) -> Vec<u16> {
    value.mem.to_str().encode_utf16().collect()
}

/// sqlite3_value_blob - Get value as blob
pub fn sqlite3_value_blob(value: &SqliteValue) -> Vec<u8> {
    value.mem.to_blob()
}

/// sqlite3_value_bytes - Get byte length (UTF-8)
pub fn sqlite3_value_bytes(value: &SqliteValue) -> i32 {
    value.mem.len() as i32
}

/// sqlite3_value_bytes16 - Get byte length (UTF-16)
pub fn sqlite3_value_bytes16(value: &SqliteValue) -> i32 {
    value.mem.to_str().encode_utf16().count() as i32 * 2
}

/// sqlite3_value_subtype - Get subtype of value
pub fn sqlite3_value_subtype(value: &SqliteValue) -> u32 {
    // Subtypes are used for things like JSON
    // For now, return 0 (no subtype)
    let _ = value;
    0
}

/// sqlite3_value_dup - Duplicate a value
pub fn sqlite3_value_dup(value: &SqliteValue) -> SqliteValue {
    value.clone()
}

/// sqlite3_value_free - Free a duplicated value
pub fn sqlite3_value_free(_value: SqliteValue) {
    // Rust handles cleanup automatically
}

// ============================================================================
// Function Context (sqlite3_context)
// ============================================================================

/// Context for a user-defined function call
///
/// This corresponds to sqlite3_context in the C API. It provides
/// methods to set the return value and error state.
pub struct FunctionContext {
    /// The result value
    result: Mem,
    /// Error message if any
    error: Option<String>,
    /// Error code
    error_code: ErrorCode,
    /// User data associated with function
    user_data: Option<Box<dyn std::any::Any + Send + Sync>>,
    /// Aggregate context (for aggregate functions)
    agg_context: Option<Box<dyn std::any::Any + Send + Sync>>,
    /// Subtype to set on result
    result_subtype: u32,
}

impl FunctionContext {
    /// Create a new function context
    pub fn new() -> Self {
        Self {
            result: Mem::new(),
            error: None,
            error_code: ErrorCode::Ok,
            user_data: None,
            agg_context: None,
            result_subtype: 0,
        }
    }

    /// Get the result
    pub fn get_result(&self) -> &Mem {
        &self.result
    }

    /// Check for error
    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }

    /// Get error
    pub fn get_error(&self) -> Option<(&str, ErrorCode)> {
        self.error
            .as_ref()
            .map(|msg| (msg.as_str(), self.error_code))
    }

    /// Get result subtype
    pub fn get_result_subtype(&self) -> u32 {
        self.result_subtype
    }
}

impl Default for FunctionContext {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Result Setting (sqlite3_result_*)
// ============================================================================

/// sqlite3_result_null - Set NULL result
pub fn sqlite3_result_null(ctx: &mut FunctionContext) {
    ctx.result.set_null();
}

/// sqlite3_result_int - Set 32-bit integer result
pub fn sqlite3_result_int(ctx: &mut FunctionContext, value: i32) {
    sqlite3_result_int64(ctx, value as i64);
}

/// sqlite3_result_int64 - Set 64-bit integer result
pub fn sqlite3_result_int64(ctx: &mut FunctionContext, value: i64) {
    ctx.result.set_int(value);
}

/// sqlite3_result_double - Set double result
pub fn sqlite3_result_double(ctx: &mut FunctionContext, value: f64) {
    ctx.result.set_real(value);
}

/// sqlite3_result_text - Set UTF-8 text result
pub fn sqlite3_result_text(ctx: &mut FunctionContext, value: &str) {
    ctx.result.set_str(value);
}

/// sqlite3_result_text16 - Set UTF-16 text result
pub fn sqlite3_result_text16(ctx: &mut FunctionContext, value: &[u16]) {
    let text = String::from_utf16_lossy(value);
    ctx.result.set_str(&text);
}

/// sqlite3_result_blob - Set blob result
pub fn sqlite3_result_blob(ctx: &mut FunctionContext, value: &[u8]) {
    ctx.result.set_blob(value);
}

/// sqlite3_result_blob64 - Set large blob result
pub fn sqlite3_result_blob64(ctx: &mut FunctionContext, value: &[u8]) {
    sqlite3_result_blob(ctx, value);
}

/// sqlite3_result_zeroblob - Set zero-filled blob result
pub fn sqlite3_result_zeroblob(ctx: &mut FunctionContext, size: i32) {
    ctx.result.set_blob(&vec![0u8; size as usize]);
}

/// sqlite3_result_zeroblob64 - Set large zero-filled blob result
pub fn sqlite3_result_zeroblob64(ctx: &mut FunctionContext, size: u64) -> Result<()> {
    if size > i32::MAX as u64 {
        return Err(Error::new(ErrorCode::TooBig));
    }
    sqlite3_result_zeroblob(ctx, size as i32);
    Ok(())
}

/// sqlite3_result_value - Set result from value
pub fn sqlite3_result_value(ctx: &mut FunctionContext, value: &SqliteValue) {
    ctx.result.copy_from(&value.mem);
}

/// sqlite3_result_error - Set error result
pub fn sqlite3_result_error(ctx: &mut FunctionContext, msg: &str) {
    ctx.error = Some(msg.to_string());
    ctx.error_code = ErrorCode::Error;
}

/// sqlite3_result_error16 - Set error result (UTF-16)
pub fn sqlite3_result_error16(ctx: &mut FunctionContext, msg: &[u16]) {
    let text = String::from_utf16_lossy(msg);
    sqlite3_result_error(ctx, &text);
}

/// sqlite3_result_error_toobig - Set TOOBIG error
pub fn sqlite3_result_error_toobig(ctx: &mut FunctionContext) {
    ctx.error = Some("string or blob too big".to_string());
    ctx.error_code = ErrorCode::TooBig;
}

/// sqlite3_result_error_nomem - Set NOMEM error
pub fn sqlite3_result_error_nomem(ctx: &mut FunctionContext) {
    ctx.error = Some("out of memory".to_string());
    ctx.error_code = ErrorCode::NoMem;
}

/// sqlite3_result_error_code - Set specific error code
pub fn sqlite3_result_error_code(ctx: &mut FunctionContext, code: ErrorCode) {
    ctx.error_code = code;
    if ctx.error.is_none() {
        ctx.error = Some(format!("error code {}", code as i32));
    }
}

/// sqlite3_result_subtype - Set result subtype
pub fn sqlite3_result_subtype(ctx: &mut FunctionContext, subtype: u32) {
    ctx.result_subtype = subtype;
}

// ============================================================================
// Context Functions
// ============================================================================

/// sqlite3_user_data - Get user data from context
pub fn sqlite3_user_data<T: 'static + Send + Sync>(ctx: &FunctionContext) -> Option<&T> {
    ctx.user_data
        .as_ref()
        .and_then(|data| data.downcast_ref::<T>())
}

/// sqlite3_context_db_handle - Get database connection from context
/// Note: Would need to store connection reference in context
pub fn sqlite3_context_db_handle(_ctx: &FunctionContext) -> Option<()> {
    // Would return connection reference if stored
    None
}

/// sqlite3_aggregate_context - Get/allocate aggregate context
///
/// For aggregate functions, this returns a persistent context that
/// survives across multiple step() calls.
pub fn sqlite3_aggregate_context<T: 'static + Default + Send + Sync>(
    ctx: &mut FunctionContext,
) -> &mut T {
    if ctx.agg_context.is_none() {
        ctx.agg_context = Some(Box::new(T::default()));
    }
    ctx.agg_context
        .as_mut()
        .unwrap()
        .downcast_mut::<T>()
        .expect("aggregate context type mismatch")
}

/// sqlite3_get_auxdata - Get auxiliary data
/// Note: Would need to implement auxiliary data storage
pub fn sqlite3_get_auxdata(_ctx: &FunctionContext, _n: i32) -> Option<()> {
    None
}

/// sqlite3_set_auxdata - Set auxiliary data
/// Note: Would need to implement auxiliary data storage
pub fn sqlite3_set_auxdata(_ctx: &mut FunctionContext, _n: i32, _data: ()) {
    // Not implemented yet
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_type() {
        let null_val = SqliteValue::new();
        assert_eq!(sqlite3_value_type(&null_val), ColumnType::Null);

        let int_val = SqliteValue::from_value(&Value::Integer(42));
        assert_eq!(sqlite3_value_type(&int_val), ColumnType::Integer);

        let real_val = SqliteValue::from_value(&Value::Real(3.14));
        assert_eq!(sqlite3_value_type(&real_val), ColumnType::Float);

        let text_val = SqliteValue::from_value(&Value::Text("hello".to_string()));
        assert_eq!(sqlite3_value_type(&text_val), ColumnType::Text);

        let blob_val = SqliteValue::from_value(&Value::Blob(vec![1, 2, 3]));
        assert_eq!(sqlite3_value_type(&blob_val), ColumnType::Blob);
    }

    #[test]
    fn test_value_int() {
        let val = SqliteValue::from_value(&Value::Integer(42));
        assert_eq!(sqlite3_value_int(&val), 42);
        assert_eq!(sqlite3_value_int64(&val), 42);
    }

    #[test]
    fn test_value_double() {
        let val = SqliteValue::from_value(&Value::Real(3.14));
        assert!((sqlite3_value_double(&val) - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn test_value_text() {
        let val = SqliteValue::from_value(&Value::Text("hello".to_string()));
        assert_eq!(sqlite3_value_text(&val), "hello");
        assert_eq!(sqlite3_value_bytes(&val), 5);
    }

    #[test]
    fn test_value_blob() {
        let val = SqliteValue::from_value(&Value::Blob(vec![1, 2, 3]));
        assert_eq!(sqlite3_value_blob(&val), vec![1, 2, 3]);
        assert_eq!(sqlite3_value_bytes(&val), 3);
    }

    #[test]
    fn test_result_null() {
        let mut ctx = FunctionContext::new();
        sqlite3_result_null(&mut ctx);
        assert!(ctx.result.is_null());
    }

    #[test]
    fn test_result_int() {
        let mut ctx = FunctionContext::new();
        sqlite3_result_int(&mut ctx, 42);
        assert_eq!(ctx.result.to_int(), 42);
    }

    #[test]
    fn test_result_double() {
        let mut ctx = FunctionContext::new();
        sqlite3_result_double(&mut ctx, 3.14);
        assert!((ctx.result.to_real() - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn test_result_text() {
        let mut ctx = FunctionContext::new();
        sqlite3_result_text(&mut ctx, "hello");
        assert_eq!(ctx.result.to_str(), "hello");
    }

    #[test]
    fn test_result_blob() {
        let mut ctx = FunctionContext::new();
        sqlite3_result_blob(&mut ctx, &[1, 2, 3]);
        assert_eq!(ctx.result.to_blob(), vec![1, 2, 3]);
    }

    #[test]
    fn test_result_error() {
        let mut ctx = FunctionContext::new();
        sqlite3_result_error(&mut ctx, "test error");
        assert!(ctx.has_error());
        let (msg, code) = ctx.get_error().unwrap();
        assert_eq!(msg, "test error");
        assert_eq!(code, ErrorCode::Error);
    }

    #[test]
    fn test_aggregate_context() {
        #[derive(Default)]
        struct SumState {
            total: i64,
        }

        let mut ctx = FunctionContext::new();

        // First call allocates
        {
            let state = sqlite3_aggregate_context::<SumState>(&mut ctx);
            state.total = 10;
        }

        // Second call returns same context
        {
            let state = sqlite3_aggregate_context::<SumState>(&mut ctx);
            assert_eq!(state.total, 10);
            state.total += 5;
        }

        // Verify persistence
        {
            let state = sqlite3_aggregate_context::<SumState>(&mut ctx);
            assert_eq!(state.total, 15);
        }
    }

    #[test]
    fn test_value_dup() {
        let val = SqliteValue::from_value(&Value::Integer(42));
        let dup = sqlite3_value_dup(&val);
        assert_eq!(sqlite3_value_int(&dup), 42);
    }

    #[test]
    fn test_numeric_type() {
        let int_str = SqliteValue::from_value(&Value::Text("123".to_string()));
        assert_eq!(sqlite3_value_numeric_type(&int_str), ColumnType::Integer);

        let float_str = SqliteValue::from_value(&Value::Text("3.14".to_string()));
        assert_eq!(sqlite3_value_numeric_type(&float_str), ColumnType::Float);
    }
}
