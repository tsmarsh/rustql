//! Scalar SQL functions
//!
//! This module implements SQLite's core scalar functions from func.c.
//! Each function takes zero or more Value arguments and returns a single Value.

use crate::error::{Error, Result};
use crate::types::Value;

use super::datetime::{
    func_current_date, func_current_time, func_current_timestamp, func_date, func_datetime,
    func_julianday, func_strftime, func_time, func_unixepoch,
};
use super::printf::printf_format;

// ============================================================================
// Function Registry
// ============================================================================

/// Function implementation type
pub type ScalarFunc = fn(&[Value]) -> Result<Value>;

/// Get a built-in scalar function by name
pub fn get_scalar_function(name: &str) -> Option<ScalarFunc> {
    match name.to_uppercase().as_str() {
        // Math functions
        "ABS" => Some(func_abs),
        "MAX" => Some(func_max),
        "MIN" => Some(func_min),
        "ROUND" => Some(func_round),
        "SIGN" => Some(func_sign),

        // String functions
        "LENGTH" => Some(func_length),
        "SUBSTR" | "SUBSTRING" => Some(func_substr),
        "INSTR" => Some(func_instr),
        "UPPER" => Some(func_upper),
        "LOWER" => Some(func_lower),
        "TRIM" => Some(func_trim),
        "LTRIM" => Some(func_ltrim),
        "RTRIM" => Some(func_rtrim),
        "REPLACE" => Some(func_replace),
        "REVERSE" => Some(func_reverse),

        // Type functions
        "TYPEOF" => Some(func_typeof),
        "COALESCE" => Some(func_coalesce),
        "NULLIF" => Some(func_nullif),
        "IFNULL" => Some(func_ifnull),
        "IIF" => Some(func_iif),

        // Blob functions
        "HEX" => Some(func_hex),
        "UNHEX" => Some(func_unhex),
        "ZEROBLOB" => Some(func_zeroblob),
        "QUOTE" => Some(func_quote),

        // Other functions
        "RANDOM" => Some(func_random),
        "RANDOMBLOB" => Some(func_randomblob),
        "UNICODE" => Some(func_unicode),
        "CHAR" => Some(func_char),
        "PRINTF" | "FORMAT" => Some(func_printf),
        "LIKE" => Some(func_like),
        "GLOB" => Some(func_glob),

        // Date/time functions
        "DATE" => Some(func_date),
        "TIME" => Some(func_time),
        "DATETIME" => Some(func_datetime),
        "JULIANDAY" => Some(func_julianday),
        "UNIXEPOCH" => Some(func_unixepoch),
        "STRFTIME" => Some(func_strftime),
        "CURRENT_DATE" => Some(func_current_date),
        "CURRENT_TIME" => Some(func_current_time),
        "CURRENT_TIMESTAMP" => Some(func_current_timestamp),

        _ => None,
    }
}

// ============================================================================
// Math Functions
// ============================================================================

/// abs(X) - Return the absolute value of X
pub fn func_abs(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "abs() requires exactly 1 argument",
        ));
    }

    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => Ok(Value::Integer(n.abs())),
        Value::Real(f) => Ok(Value::Real(f.abs())),
        Value::Text(s) => {
            // Try to parse as number
            if let Ok(n) = s.parse::<i64>() {
                Ok(Value::Integer(n.abs()))
            } else if let Ok(f) = s.parse::<f64>() {
                Ok(Value::Real(f.abs()))
            } else {
                Ok(Value::Integer(0))
            }
        }
        Value::Blob(_) => Ok(Value::Integer(0)),
    }
}

/// max(X, Y, ...) - Return the maximum value
pub fn func_max(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }

    let mut max_val = &args[0];
    for arg in &args[1..] {
        if compare_values(arg, max_val) > 0 {
            max_val = arg;
        }
    }
    Ok(max_val.clone())
}

/// min(X, Y, ...) - Return the minimum value
pub fn func_min(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Null);
    }

    let mut min_val = &args[0];
    for arg in &args[1..] {
        if compare_values(arg, min_val) < 0 {
            min_val = arg;
        }
    }
    Ok(min_val.clone())
}

/// round(X) or round(X, Y) - Round X to Y decimal places
pub fn func_round(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "round() requires 1 or 2 arguments",
        ));
    }

    let precision = if args.len() == 2 {
        value_to_i64(&args[1]) as i32
    } else {
        0
    };

    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => Ok(Value::Integer(*n)),
        Value::Real(f) => {
            let multiplier = 10f64.powi(precision);
            let rounded = (f * multiplier).round() / multiplier;
            Ok(Value::Real(rounded))
        }
        Value::Text(s) => {
            if let Ok(f) = s.parse::<f64>() {
                let multiplier = 10f64.powi(precision);
                let rounded = (f * multiplier).round() / multiplier;
                Ok(Value::Real(rounded))
            } else {
                Ok(Value::Real(0.0))
            }
        }
        Value::Blob(_) => Ok(Value::Real(0.0)),
    }
}

/// sign(X) - Return -1, 0, or 1 for negative, zero, or positive
pub fn func_sign(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "sign() requires exactly 1 argument",
        ));
    }

    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => Ok(Value::Integer(n.signum())),
        Value::Real(f) => {
            if f.is_nan() {
                Ok(Value::Null)
            } else if *f < 0.0 {
                Ok(Value::Integer(-1))
            } else if *f > 0.0 {
                Ok(Value::Integer(1))
            } else {
                Ok(Value::Integer(0))
            }
        }
        Value::Text(s) => {
            if let Ok(f) = s.parse::<f64>() {
                if f < 0.0 {
                    Ok(Value::Integer(-1))
                } else if f > 0.0 {
                    Ok(Value::Integer(1))
                } else {
                    Ok(Value::Integer(0))
                }
            } else {
                Ok(Value::Integer(0))
            }
        }
        Value::Blob(_) => Ok(Value::Integer(0)),
    }
}

// ============================================================================
// String Functions
// ============================================================================

/// length(X) - Return the length of X in characters (or bytes for blobs)
pub fn func_length(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "length() requires exactly 1 argument",
        ));
    }

    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Text(s) => Ok(Value::Integer(s.chars().count() as i64)),
        Value::Blob(b) => Ok(Value::Integer(b.len() as i64)),
        Value::Integer(n) => Ok(Value::Integer(n.to_string().len() as i64)),
        Value::Real(f) => Ok(Value::Integer(f.to_string().len() as i64)),
    }
}

/// substr(X, Y) or substr(X, Y, Z) - Extract substring
pub fn func_substr(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "substr() requires 2 or 3 arguments",
        ));
    }

    let s = value_to_string(&args[0]);
    let start = value_to_i64(&args[1]);
    let len = if args.len() == 3 {
        Some(value_to_i64(&args[2]))
    } else {
        None
    };

    // SQLite uses 1-based indexing, negative means from end
    let chars: Vec<char> = s.chars().collect();
    let str_len = chars.len() as i64;

    // Calculate actual start position (0-based)
    let actual_start = if start > 0 {
        (start - 1).min(str_len) as usize
    } else if start < 0 {
        (str_len + start).max(0) as usize
    } else {
        0
    };

    let actual_len = match len {
        Some(l) if l >= 0 => l as usize,
        Some(_) => {
            // Negative length in SQLite means characters before the position
            let end_pos = (start - 1).max(0) as usize;
            if end_pos > actual_start {
                end_pos - actual_start
            } else {
                0
            }
        }
        None => chars.len().saturating_sub(actual_start),
    };

    let result: String = chars.iter().skip(actual_start).take(actual_len).collect();

    Ok(Value::Text(result))
}

/// instr(X, Y) - Find first occurrence of Y in X
pub fn func_instr(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "instr() requires exactly 2 arguments",
        ));
    }

    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }

    let haystack = value_to_string(&args[0]);
    let needle = value_to_string(&args[1]);

    match haystack.find(&needle) {
        Some(pos) => {
            // Return 1-based character position
            let char_pos = haystack[..pos].chars().count() + 1;
            Ok(Value::Integer(char_pos as i64))
        }
        None => Ok(Value::Integer(0)),
    }
}

/// upper(X) - Convert to uppercase
pub fn func_upper(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "upper() requires exactly 1 argument",
        ));
    }

    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
        other => Ok(Value::Text(value_to_string(other).to_uppercase())),
    }
}

/// lower(X) - Convert to lowercase
pub fn func_lower(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "lower() requires exactly 1 argument",
        ));
    }

    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
        other => Ok(Value::Text(value_to_string(other).to_lowercase())),
    }
}

/// trim(X) or trim(X, Y) - Remove characters from both ends
pub fn func_trim(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "trim() requires 1 or 2 arguments",
        ));
    }

    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }

    let s = value_to_string(&args[0]);
    let chars_to_trim: Vec<char> = if args.len() == 2 {
        value_to_string(&args[1]).chars().collect()
    } else {
        vec![' ', '\t', '\n', '\r']
    };

    let result = s.trim_matches(|c| chars_to_trim.contains(&c));
    Ok(Value::Text(result.to_string()))
}

/// ltrim(X) or ltrim(X, Y) - Remove characters from left
pub fn func_ltrim(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "ltrim() requires 1 or 2 arguments",
        ));
    }

    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }

    let s = value_to_string(&args[0]);
    let chars_to_trim: Vec<char> = if args.len() == 2 {
        value_to_string(&args[1]).chars().collect()
    } else {
        vec![' ', '\t', '\n', '\r']
    };

    let result = s.trim_start_matches(|c| chars_to_trim.contains(&c));
    Ok(Value::Text(result.to_string()))
}

/// rtrim(X) or rtrim(X, Y) - Remove characters from right
pub fn func_rtrim(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "rtrim() requires 1 or 2 arguments",
        ));
    }

    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }

    let s = value_to_string(&args[0]);
    let chars_to_trim: Vec<char> = if args.len() == 2 {
        value_to_string(&args[1]).chars().collect()
    } else {
        vec![' ', '\t', '\n', '\r']
    };

    let result = s.trim_end_matches(|c| chars_to_trim.contains(&c));
    Ok(Value::Text(result.to_string()))
}

/// replace(X, Y, Z) - Replace all occurrences of Y with Z in X
pub fn func_replace(args: &[Value]) -> Result<Value> {
    if args.len() != 3 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "replace() requires exactly 3 arguments",
        ));
    }

    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }

    let s = value_to_string(&args[0]);
    let from = value_to_string(&args[1]);
    let to = value_to_string(&args[2]);

    if from.is_empty() {
        return Ok(Value::Text(s));
    }

    Ok(Value::Text(s.replace(&from, &to)))
}

/// reverse(X) - Reverse the characters in string X
pub fn func_reverse(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "reverse() requires exactly 1 argument",
        ));
    }

    match &args[0] {
        Value::Null => Ok(Value::Null),
        Value::Text(s) => Ok(Value::Text(s.chars().rev().collect())),
        other => Ok(Value::Text(value_to_string(other).chars().rev().collect())),
    }
}

// ============================================================================
// Type Functions
// ============================================================================

/// typeof(X) - Return the type of X as a string
pub fn func_typeof(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "typeof() requires exactly 1 argument",
        ));
    }

    let type_str = match &args[0] {
        Value::Null => "null",
        Value::Integer(_) => "integer",
        Value::Real(_) => "real",
        Value::Text(_) => "text",
        Value::Blob(_) => "blob",
    };

    Ok(Value::Text(type_str.to_string()))
}

/// coalesce(X, Y, ...) - Return first non-NULL argument
pub fn func_coalesce(args: &[Value]) -> Result<Value> {
    for arg in args {
        if !matches!(arg, Value::Null) {
            return Ok(arg.clone());
        }
    }
    Ok(Value::Null)
}

/// nullif(X, Y) - Return NULL if X equals Y, otherwise return X
pub fn func_nullif(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "nullif() requires exactly 2 arguments",
        ));
    }

    if compare_values(&args[0], &args[1]) == 0 {
        Ok(Value::Null)
    } else {
        Ok(args[0].clone())
    }
}

/// ifnull(X, Y) - Return X if not NULL, otherwise return Y
pub fn func_ifnull(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "ifnull() requires exactly 2 arguments",
        ));
    }

    if matches!(args[0], Value::Null) {
        Ok(args[1].clone())
    } else {
        Ok(args[0].clone())
    }
}

/// iif(X, Y, Z) - If X is true, return Y, else return Z
pub fn func_iif(args: &[Value]) -> Result<Value> {
    if args.len() != 3 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "iif() requires exactly 3 arguments",
        ));
    }

    let condition = value_is_true(&args[0]);
    if condition {
        Ok(args[1].clone())
    } else {
        Ok(args[2].clone())
    }
}

// ============================================================================
// Blob Functions
// ============================================================================

/// hex(X) - Convert X to hexadecimal string
pub fn func_hex(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "hex() requires exactly 1 argument",
        ));
    }

    let bytes: Vec<u8> = match &args[0] {
        Value::Null => return Ok(Value::Null),
        Value::Blob(b) => b.clone(),
        Value::Text(s) => s.as_bytes().to_vec(),
        Value::Integer(n) => n.to_string().as_bytes().to_vec(),
        Value::Real(f) => f.to_string().as_bytes().to_vec(),
    };

    let hex: String = bytes.iter().map(|b| format!("{:02X}", b)).collect();
    Ok(Value::Text(hex))
}

/// unhex(X) - Convert hexadecimal string to blob
pub fn func_unhex(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "unhex() requires exactly 1 argument",
        ));
    }

    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }

    let hex = value_to_string(&args[0]);
    let hex = hex.trim();

    if hex.len() % 2 != 0 {
        return Ok(Value::Null);
    }

    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for i in (0..hex.len()).step_by(2) {
        match u8::from_str_radix(&hex[i..i + 2], 16) {
            Ok(b) => bytes.push(b),
            Err(_) => return Ok(Value::Null),
        }
    }

    Ok(Value::Blob(bytes))
}

/// zeroblob(N) - Return a blob of N zero bytes
pub fn func_zeroblob(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "zeroblob() requires exactly 1 argument",
        ));
    }

    let n = value_to_i64(&args[0]);
    if n < 0 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "zeroblob() size must be non-negative",
        ));
    }

    // Limit size to prevent memory exhaustion
    let n = n.min(1_000_000_000) as usize;
    Ok(Value::Blob(vec![0u8; n]))
}

/// quote(X) - Return SQL literal representation of X
pub fn func_quote(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "quote() requires exactly 1 argument",
        ));
    }

    let quoted = match &args[0] {
        Value::Null => "NULL".to_string(),
        Value::Integer(n) => n.to_string(),
        Value::Real(f) => {
            if f.is_finite() {
                format!("{:?}", f)
            } else {
                "NULL".to_string()
            }
        }
        Value::Text(s) => {
            // Escape single quotes by doubling them
            let escaped = s.replace('\'', "''");
            format!("'{}'", escaped)
        }
        Value::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
            format!("X'{}'", hex)
        }
    };

    Ok(Value::Text(quoted))
}

// ============================================================================
// Other Functions
// ============================================================================

/// random() - Return a random 64-bit integer
pub fn func_random(args: &[Value]) -> Result<Value> {
    if !args.is_empty() {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "random() takes no arguments",
        ));
    }

    // Simple LCG random number generator
    // In production, this would use a proper RNG
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    // Mix bits
    let mut x = seed;
    x ^= x >> 17;
    x ^= x << 31;
    x ^= x >> 8;

    Ok(Value::Integer(x))
}

/// randomblob(N) - Return N bytes of random data
pub fn func_randomblob(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "randomblob() requires exactly 1 argument",
        ));
    }

    let n = value_to_i64(&args[0]);
    if n < 0 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "randomblob() size must be non-negative",
        ));
    }

    let n = n.min(1_000_000_000) as usize;

    // Generate pseudo-random bytes
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;

    let mut bytes = Vec::with_capacity(n);
    for _ in 0..n {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        bytes.push((seed >> 33) as u8);
    }

    Ok(Value::Blob(bytes))
}

/// unicode(X) - Return Unicode code point of first character
pub fn func_unicode(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "unicode() requires exactly 1 argument",
        ));
    }

    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }

    let s = value_to_string(&args[0]);
    match s.chars().next() {
        Some(c) => Ok(Value::Integer(c as i64)),
        None => Ok(Value::Null),
    }
}

/// char(X, Y, ...) - Return string from Unicode code points
pub fn func_char(args: &[Value]) -> Result<Value> {
    let mut result = String::new();

    for arg in args {
        let code = value_to_i64(arg);
        if code >= 0 && code <= 0x10FFFF {
            if let Some(c) = char::from_u32(code as u32) {
                result.push(c);
            }
        }
    }

    Ok(Value::Text(result))
}

/// printf(FORMAT, ...) - Format values according to format string
pub fn func_printf(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "printf() requires at least 1 argument",
        ));
    }
    let format = value_to_string(&args[0]);
    let format_args = &args[1..];
    Ok(Value::Text(printf_format(&format, format_args)?))
}

/// like(X, Y) or like(X, Y, Z) - Pattern matching
pub fn func_like(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "like() requires 2 or 3 arguments",
        ));
    }

    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }

    let pattern = value_to_string(&args[0]);
    let text = value_to_string(&args[1]);
    let escape = if args.len() == 3 {
        let e = value_to_string(&args[2]);
        e.chars().next()
    } else {
        None
    };

    let matched = like_match(&pattern, &text, escape, false);
    Ok(Value::Integer(if matched { 1 } else { 0 }))
}

/// glob(X, Y) - Unix-style glob pattern matching (case-sensitive)
pub fn func_glob(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(Error::with_message(
            crate::error::ErrorCode::Error,
            "glob() requires exactly 2 arguments",
        ));
    }

    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }

    let pattern = value_to_string(&args[0]);
    let text = value_to_string(&args[1]);

    let matched = glob_match(&pattern, &text);
    Ok(Value::Integer(if matched { 1 } else { 0 }))
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert Value to i64
fn value_to_i64(val: &Value) -> i64 {
    match val {
        Value::Null => 0,
        Value::Integer(n) => *n,
        Value::Real(f) => *f as i64,
        Value::Text(s) => s.parse().unwrap_or(0),
        Value::Blob(_) => 0,
    }
}

/// Convert Value to f64
fn value_to_f64(val: &Value) -> f64 {
    match val {
        Value::Null => 0.0,
        Value::Integer(n) => *n as f64,
        Value::Real(f) => *f,
        Value::Text(s) => s.parse().unwrap_or(0.0),
        Value::Blob(_) => 0.0,
    }
}

/// Convert Value to String
fn value_to_string(val: &Value) -> String {
    match val {
        Value::Null => String::new(),
        Value::Integer(n) => n.to_string(),
        Value::Real(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Blob(b) => String::from_utf8_lossy(b).to_string(),
    }
}

/// Check if Value is truthy (non-zero, non-empty)
fn value_is_true(val: &Value) -> bool {
    match val {
        Value::Null => false,
        Value::Integer(n) => *n != 0,
        Value::Real(f) => *f != 0.0,
        Value::Text(s) => {
            if let Ok(n) = s.parse::<i64>() {
                n != 0
            } else if let Ok(f) = s.parse::<f64>() {
                f != 0.0
            } else {
                false
            }
        }
        Value::Blob(_) => false,
    }
}

/// Compare two values (returns -1, 0, or 1)
fn compare_values(a: &Value, b: &Value) -> i32 {
    match (a, b) {
        (Value::Null, Value::Null) => 0,
        (Value::Null, _) => -1,
        (_, Value::Null) => 1,
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y) as i32,
        (Value::Real(x), Value::Real(y)) => {
            if x < y {
                -1
            } else if x > y {
                1
            } else {
                0
            }
        }
        (Value::Integer(x), Value::Real(y)) => {
            let fx = *x as f64;
            if fx < *y {
                -1
            } else if fx > *y {
                1
            } else {
                0
            }
        }
        (Value::Real(x), Value::Integer(y)) => {
            let fy = *y as f64;
            if *x < fy {
                -1
            } else if *x > fy {
                1
            } else {
                0
            }
        }
        (Value::Text(x), Value::Text(y)) => x.cmp(y) as i32,
        (Value::Blob(x), Value::Blob(y)) => x.cmp(y) as i32,
        // Mixed type comparisons: NULL < numbers < text < blob
        (Value::Integer(_), Value::Text(_)) | (Value::Real(_), Value::Text(_)) => -1,
        (Value::Text(_), Value::Integer(_)) | (Value::Text(_), Value::Real(_)) => 1,
        (Value::Blob(_), _) => 1,
        (_, Value::Blob(_)) => -1,
    }
}

/// LIKE pattern matching
fn like_match(pattern: &str, text: &str, escape: Option<char>, case_sensitive: bool) -> bool {
    let pattern: Vec<char> = if case_sensitive {
        pattern.chars().collect()
    } else {
        pattern.to_lowercase().chars().collect()
    };

    let text: Vec<char> = if case_sensitive {
        text.chars().collect()
    } else {
        text.to_lowercase().chars().collect()
    };

    like_match_impl(&pattern, &text, escape)
}

fn like_match_impl(pattern: &[char], text: &[char], escape: Option<char>) -> bool {
    let mut p_idx = 0;
    let mut t_idx = 0;
    let mut star_p_idx: Option<usize> = None;
    let mut star_t_idx: Option<usize> = None;

    while t_idx < text.len() {
        if p_idx < pattern.len() {
            let p_char = pattern[p_idx];

            // Check for escape character
            if Some(p_char) == escape && p_idx + 1 < pattern.len() {
                p_idx += 1;
                if pattern[p_idx] == text[t_idx] {
                    p_idx += 1;
                    t_idx += 1;
                    continue;
                }
            } else if p_char == '%' {
                star_p_idx = Some(p_idx);
                star_t_idx = Some(t_idx);
                p_idx += 1;
                continue;
            } else if p_char == '_' || p_char == text[t_idx] {
                p_idx += 1;
                t_idx += 1;
                continue;
            }
        }

        // Mismatch - backtrack if we had a %
        if let (Some(sp), Some(st)) = (star_p_idx, star_t_idx) {
            p_idx = sp + 1;
            star_t_idx = Some(st + 1);
            t_idx = st + 1;
        } else {
            return false;
        }
    }

    // Skip trailing % in pattern
    while p_idx < pattern.len() && pattern[p_idx] == '%' {
        p_idx += 1;
    }

    p_idx == pattern.len()
}

/// GLOB pattern matching (case-sensitive, uses * and ?)
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    glob_match_impl(&pattern, &text)
}

fn glob_match_impl(pattern: &[char], text: &[char]) -> bool {
    let mut p_idx = 0;
    let mut t_idx = 0;
    let mut star_p_idx: Option<usize> = None;
    let mut star_t_idx: Option<usize> = None;

    while t_idx < text.len() {
        if p_idx < pattern.len() {
            let p_char = pattern[p_idx];

            if p_char == '*' {
                star_p_idx = Some(p_idx);
                star_t_idx = Some(t_idx);
                p_idx += 1;
                continue;
            } else if p_char == '?' || p_char == text[t_idx] {
                p_idx += 1;
                t_idx += 1;
                continue;
            } else if p_char == '[' {
                // Character class
                if let Some((matched, end_idx)) = match_char_class(&pattern[p_idx..], text[t_idx]) {
                    if matched {
                        p_idx += end_idx;
                        t_idx += 1;
                        continue;
                    }
                }
            }
        }

        if let (Some(sp), Some(st)) = (star_p_idx, star_t_idx) {
            p_idx = sp + 1;
            star_t_idx = Some(st + 1);
            t_idx = st + 1;
        } else {
            return false;
        }
    }

    while p_idx < pattern.len() && pattern[p_idx] == '*' {
        p_idx += 1;
    }

    p_idx == pattern.len()
}

/// Match a character class [abc] or [a-z]
fn match_char_class(pattern: &[char], c: char) -> Option<(bool, usize)> {
    if pattern.is_empty() || pattern[0] != '[' {
        return None;
    }

    let mut idx = 1;
    let negate = if idx < pattern.len() && pattern[idx] == '^' {
        idx += 1;
        true
    } else {
        false
    };

    let mut matched = false;
    while idx < pattern.len() && pattern[idx] != ']' {
        if idx + 2 < pattern.len() && pattern[idx + 1] == '-' {
            // Range like a-z
            let start = pattern[idx];
            let end = pattern[idx + 2];
            if c >= start && c <= end {
                matched = true;
            }
            idx += 3;
        } else {
            if pattern[idx] == c {
                matched = true;
            }
            idx += 1;
        }
    }

    if idx < pattern.len() && pattern[idx] == ']' {
        Some((matched != negate, idx + 1))
    } else {
        None
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_abs() {
        assert_eq!(func_abs(&[Value::Integer(-5)]).unwrap(), Value::Integer(5));
        assert_eq!(func_abs(&[Value::Integer(5)]).unwrap(), Value::Integer(5));
        assert_eq!(func_abs(&[Value::Real(-3.14)]).unwrap(), Value::Real(3.14));
        assert_eq!(func_abs(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn test_max_min() {
        let args = vec![Value::Integer(1), Value::Integer(5), Value::Integer(3)];
        assert_eq!(func_max(&args).unwrap(), Value::Integer(5));
        assert_eq!(func_min(&args).unwrap(), Value::Integer(1));
    }

    #[test]
    fn test_length() {
        assert_eq!(
            func_length(&[Value::Text("hello".to_string())]).unwrap(),
            Value::Integer(5)
        );
        assert_eq!(
            func_length(&[Value::Text("héllo".to_string())]).unwrap(),
            Value::Integer(5)
        );
        assert_eq!(
            func_length(&[Value::Blob(vec![1, 2, 3])]).unwrap(),
            Value::Integer(3)
        );
        assert_eq!(func_length(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn test_substr() {
        let s = Value::Text("hello".to_string());
        assert_eq!(
            func_substr(&[s.clone(), Value::Integer(2)]).unwrap(),
            Value::Text("ello".to_string())
        );
        assert_eq!(
            func_substr(&[s.clone(), Value::Integer(2), Value::Integer(3)]).unwrap(),
            Value::Text("ell".to_string())
        );
    }

    #[test]
    fn test_upper_lower() {
        assert_eq!(
            func_upper(&[Value::Text("hello".to_string())]).unwrap(),
            Value::Text("HELLO".to_string())
        );
        assert_eq!(
            func_lower(&[Value::Text("HELLO".to_string())]).unwrap(),
            Value::Text("hello".to_string())
        );
    }

    #[test]
    fn test_trim() {
        assert_eq!(
            func_trim(&[Value::Text("  hello  ".to_string())]).unwrap(),
            Value::Text("hello".to_string())
        );
        assert_eq!(
            func_ltrim(&[Value::Text("  hello  ".to_string())]).unwrap(),
            Value::Text("hello  ".to_string())
        );
        assert_eq!(
            func_rtrim(&[Value::Text("  hello  ".to_string())]).unwrap(),
            Value::Text("  hello".to_string())
        );
    }

    #[test]
    fn test_typeof() {
        assert_eq!(
            func_typeof(&[Value::Null]).unwrap(),
            Value::Text("null".to_string())
        );
        assert_eq!(
            func_typeof(&[Value::Integer(42)]).unwrap(),
            Value::Text("integer".to_string())
        );
        assert_eq!(
            func_typeof(&[Value::Real(3.14)]).unwrap(),
            Value::Text("real".to_string())
        );
        assert_eq!(
            func_typeof(&[Value::Text("hi".to_string())]).unwrap(),
            Value::Text("text".to_string())
        );
    }

    #[test]
    fn test_coalesce() {
        assert_eq!(
            func_coalesce(&[Value::Null, Value::Integer(1)]).unwrap(),
            Value::Integer(1)
        );
        assert_eq!(
            func_coalesce(&[Value::Null, Value::Null]).unwrap(),
            Value::Null
        );
        assert_eq!(
            func_coalesce(&[Value::Integer(5), Value::Integer(1)]).unwrap(),
            Value::Integer(5)
        );
    }

    #[test]
    fn test_hex_unhex() {
        assert_eq!(
            func_hex(&[Value::Text("ABC".to_string())]).unwrap(),
            Value::Text("414243".to_string())
        );
        assert_eq!(
            func_unhex(&[Value::Text("414243".to_string())]).unwrap(),
            Value::Blob(vec![0x41, 0x42, 0x43])
        );
    }

    #[test]
    fn test_quote() {
        assert_eq!(
            func_quote(&[Value::Null]).unwrap(),
            Value::Text("NULL".to_string())
        );
        assert_eq!(
            func_quote(&[Value::Integer(42)]).unwrap(),
            Value::Text("42".to_string())
        );
        assert_eq!(
            func_quote(&[Value::Text("it's".to_string())]).unwrap(),
            Value::Text("'it''s'".to_string())
        );
    }

    #[test]
    fn test_like() {
        assert_eq!(
            func_like(&[
                Value::Text("%ello".to_string()),
                Value::Text("hello".to_string())
            ])
            .unwrap(),
            Value::Integer(1)
        );
        assert_eq!(
            func_like(&[
                Value::Text("h_llo".to_string()),
                Value::Text("hello".to_string())
            ])
            .unwrap(),
            Value::Integer(1)
        );
        assert_eq!(
            func_like(&[
                Value::Text("world".to_string()),
                Value::Text("hello".to_string())
            ])
            .unwrap(),
            Value::Integer(0)
        );
    }

    #[test]
    fn test_replace() {
        assert_eq!(
            func_replace(&[
                Value::Text("hello world".to_string()),
                Value::Text("world".to_string()),
                Value::Text("rust".to_string())
            ])
            .unwrap(),
            Value::Text("hello rust".to_string())
        );
    }

    #[test]
    fn test_instr() {
        assert_eq!(
            func_instr(&[
                Value::Text("hello".to_string()),
                Value::Text("l".to_string())
            ])
            .unwrap(),
            Value::Integer(3)
        );
        assert_eq!(
            func_instr(&[
                Value::Text("hello".to_string()),
                Value::Text("x".to_string())
            ])
            .unwrap(),
            Value::Integer(0)
        );
    }
}
