//! General utility functions translated from SQLite's util.c.

use std::cmp::Ordering;
use std::sync::Mutex;

use crate::error::{Error, ErrorCode, Result};
use crate::schema::{type_affinity, Affinity};
use crate::types::Value;

// ============================================================================
// String utilities
// ============================================================================

/// Check if string is a valid integer (ASCII, optional sign).
pub fn sqlite3_isint(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() {
        return false;
    }

    let bytes = s.as_bytes();
    let mut idx = 0usize;

    if matches!(bytes[0], b'+' | b'-') {
        idx += 1;
    }

    let mut has_digit = false;
    while idx < bytes.len() {
        if !bytes[idx].is_ascii_digit() {
            return false;
        }
        has_digit = true;
        idx += 1;
    }

    has_digit
}

/// Safe string comparison (handles NULL).
pub fn sqlite3_strcmp(a: Option<&str>, b: Option<&str>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(a), Some(b)) => a.cmp(b),
    }
}

/// Case-insensitive ASCII string comparison.
pub fn sqlite3_stricmp(a: &str, b: &str) -> Ordering {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    let mut i = 0usize;

    loop {
        match (a_bytes.get(i), b_bytes.get(i)) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(&ca), Some(&cb)) => {
                let la = ca.to_ascii_lowercase();
                let lb = cb.to_ascii_lowercase();
                if la != lb {
                    return la.cmp(&lb);
                }
            }
        }
        i += 1;
    }
}

/// Case-insensitive ASCII comparison with length limit (in bytes).
pub fn sqlite3_strnicmp(a: &str, b: &str, n: usize) -> Ordering {
    if n == 0 {
        return Ordering::Equal;
    }

    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    let mut i = 0usize;

    while i < n {
        match (a_bytes.get(i), b_bytes.get(i)) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(&ca), Some(&cb)) => {
                let la = ca.to_ascii_lowercase();
                let lb = cb.to_ascii_lowercase();
                if la != lb {
                    return la.cmp(&lb);
                }
            }
        }
        i += 1;
    }

    Ordering::Equal
}

/// Duplicate a string (for C compatibility).
pub fn sqlite3_strdup(s: &str) -> String {
    s.to_string()
}

// ============================================================================
// Numeric conversion and formatting
// ============================================================================

/// Convert string to integer with overflow detection (returns 0 on invalid).
pub fn sqlite3_atoi(s: &str) -> i32 {
    match sqlite3_atoi64(s) {
        Ok(value) if value >= i32::MIN as i64 && value <= i32::MAX as i64 => value as i32,
        _ => 0,
    }
}

/// Convert string to 64-bit integer.
pub fn sqlite3_atoi64(s: &str) -> Result<i64> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(Error::with_message(
            ErrorCode::Error,
            format!("invalid integer: {}", s),
        ));
    }

    let bytes = trimmed.as_bytes();
    let mut idx = 0usize;
    let mut neg = false;

    if matches!(bytes.get(idx), Some(b'+') | Some(b'-')) {
        neg = bytes[idx] == b'-';
        idx += 1;
    }

    let start_digits = idx;
    let mut value: u128 = 0;

    while idx < bytes.len() {
        let b = bytes[idx];
        if !b.is_ascii_digit() {
            break;
        }
        value = value * 10 + (b - b'0') as u128;
        idx += 1;
    }

    if idx == start_digits || idx != bytes.len() {
        return Err(Error::with_message(
            ErrorCode::Error,
            format!("invalid integer: {}", s),
        ));
    }

    let max = i64::MAX as u128;
    let min_abs = max + 1;
    if neg {
        if value > min_abs {
            return Err(Error::with_message(ErrorCode::Error, "integer overflow"));
        }
        if value == min_abs {
            return Ok(i64::MIN);
        }
        Ok(-(value as i64))
    } else {
        if value > max {
            return Err(Error::with_message(ErrorCode::Error, "integer overflow"));
        }
        Ok(value as i64)
    }
}

/// Safe integer addition with overflow check.
pub fn sqlite3_add_i64(a: i64, b: i64) -> Result<i64> {
    a.checked_add(b)
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "integer overflow"))
}

/// Safe integer subtraction with overflow check.
pub fn sqlite3_sub_i64(a: i64, b: i64) -> Result<i64> {
    a.checked_sub(b)
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "integer overflow"))
}

/// Safe integer multiplication with overflow check.
pub fn sqlite3_mul_i64(a: i64, b: i64) -> Result<i64> {
    a.checked_mul(b)
        .ok_or_else(|| Error::with_message(ErrorCode::Error, "integer overflow"))
}

/// Convert double to integer with range check.
pub fn sqlite3_real_to_i64(r: f64) -> Result<i64> {
    if r.is_nan() || r.is_infinite() || r < i64::MIN as f64 || r > i64::MAX as f64 {
        return Err(Error::with_message(ErrorCode::Error, "value out of range"));
    }
    Ok(r as i64)
}

/// Format integer as decimal string.
pub fn sqlite3_i64_to_str(n: i64) -> String {
    n.to_string()
}

/// Format double with appropriate precision.
pub fn sqlite3_real_to_str(r: f64, precision: Option<usize>) -> String {
    if r.is_nan() {
        return "NaN".to_string();
    }
    if r.is_infinite() {
        return if r.is_sign_negative() {
            "-Inf".to_string()
        } else {
            "Inf".to_string()
        };
    }

    match precision {
        Some(p) => format!("{:.*}", p, r),
        None => {
            let s = format!("{}", r);
            if s.contains('.') {
                s.trim_end_matches('0').trim_end_matches('.').to_string()
            } else {
                s
            }
        }
    }
}

// ============================================================================
// Varint encoding
// ============================================================================

/// Read a varint from a byte slice.
pub fn sqlite3_get_varint(buf: &[u8]) -> (u64, usize) {
    let mut result: u64 = 0;
    let mut bytes_read = 0;

    for (i, &byte) in buf.iter().enumerate().take(9) {
        bytes_read = i + 1;
        if i == 8 {
            result = (result << 8) | byte as u64;
            break;
        } else {
            result = (result << 7) | ((byte & 0x7f) as u64);
            if byte < 0x80 {
                break;
            }
        }
    }

    (result, bytes_read)
}

/// Write a varint to a byte buffer.
pub fn sqlite3_put_varint(buf: &mut [u8], value: u64) -> usize {
    if value <= 0x7f {
        if !buf.is_empty() {
            buf[0] = value as u8;
        }
        return 1;
    }

    if value <= 0x3fff {
        if buf.len() >= 2 {
            buf[0] = ((value >> 7) as u8) | 0x80;
            buf[1] = (value & 0x7f) as u8;
        } else if !buf.is_empty() {
            buf[0] = ((value >> 7) as u8) | 0x80;
        }
        return 2;
    }

    if value & 0xff00_0000_0000_0000 != 0 {
        let mut temp = [0u8; 9];
        temp[8] = value as u8;
        let mut v = value >> 8;
        for i in (0..8).rev() {
            temp[i] = ((v & 0x7f) as u8) | 0x80;
            v >>= 7;
        }
        let write_len = 9.min(buf.len());
        buf[..write_len].copy_from_slice(&temp[..write_len]);
        return 9;
    }

    let mut tmp = [0u8; 9];
    let mut v = value;
    let mut n = 0usize;
    loop {
        tmp[n] = ((v & 0x7f) as u8) | 0x80;
        n += 1;
        v >>= 7;
        if v == 0 {
            break;
        }
    }
    tmp[0] &= 0x7f;
    let write_len = n.min(buf.len());
    for i in 0..write_len {
        buf[i] = tmp[n - 1 - i];
    }
    n
}

/// Get the length of a varint without reading it.
pub fn sqlite3_varint_len(value: u64) -> usize {
    if value < (1 << 7) {
        1
    } else if value < (1 << 14) {
        2
    } else if value < (1 << 21) {
        3
    } else if value < (1 << 28) {
        4
    } else if value < (1 << 35) {
        5
    } else if value < (1 << 42) {
        6
    } else if value < (1 << 49) {
        7
    } else if value < (1 << 56) {
        8
    } else {
        9
    }
}

// ============================================================================
// Affinity and type conversion
// ============================================================================

/// Determine affinity from type name.
pub fn sqlite3_affinity_type(type_name: &str) -> Affinity {
    type_affinity(type_name)
}

/// Apply affinity to a value.
pub fn sqlite3_apply_affinity(value: &mut Value, affinity: Affinity) {
    match affinity {
        Affinity::Integer => {
            if let Value::Text(s) = value {
                if let Ok(i) = sqlite3_atoi64(s) {
                    *value = Value::Integer(i);
                }
            } else if let Value::Real(r) = value {
                if *r >= i64::MIN as f64 && *r <= i64::MAX as f64 {
                    let i = *r as i64;
                    if (i as f64) == *r {
                        *value = Value::Integer(i);
                    }
                }
            }
        }
        Affinity::Real => {
            if let Value::Text(s) = value {
                if let Ok(r) = s.trim().parse::<f64>() {
                    *value = Value::Real(r);
                }
            } else if let Value::Integer(i) = value {
                *value = Value::Real(*i as f64);
            }
        }
        Affinity::Numeric => {
            if let Value::Text(s) = value {
                if let Ok(i) = sqlite3_atoi64(s) {
                    *value = Value::Integer(i);
                } else if let Ok(r) = s.trim().parse::<f64>() {
                    *value = Value::Real(r);
                }
            }
        }
        Affinity::Text => match value {
            Value::Integer(i) => *value = Value::Text(i.to_string()),
            Value::Real(r) => *value = Value::Text(sqlite3_real_to_str(*r, None)),
            Value::Blob(b) => *value = Value::Text(String::from_utf8_lossy(b).into_owned()),
            _ => {}
        },
        Affinity::Blob => {}
    }
}

// ============================================================================
// Safe buffer operations
// ============================================================================

/// Safe memcpy with bounds checking.
pub fn sqlite3_memcpy(dst: &mut [u8], src: &[u8], n: usize) {
    let copy_len = n.min(dst.len()).min(src.len());
    dst[..copy_len].copy_from_slice(&src[..copy_len]);
}

/// Safe memset.
pub fn sqlite3_memset(dst: &mut [u8], val: u8, n: usize) {
    let fill_len = n.min(dst.len());
    dst[..fill_len].fill(val);
}

/// Safe memcmp.
pub fn sqlite3_memcmp(a: &[u8], b: &[u8], n: usize) -> Ordering {
    let cmp_len = n.min(a.len()).min(b.len());
    a[..cmp_len].cmp(&b[..cmp_len])
}

// ============================================================================
// Logging
// ============================================================================

/// Log levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Error = 1,
    Warning = 2,
    Notice = 3,
}

static LOG_CALLBACK: Mutex<Option<Box<dyn Fn(LogLevel, &str) + Send + Sync>>> = Mutex::new(None);

/// Log a message.
pub fn sqlite3_log(level: LogLevel, message: &str) {
    if let Ok(guard) = LOG_CALLBACK.lock() {
        if let Some(ref callback) = *guard {
            callback(level, message);
        } else {
            eprintln!("[SQLite {:?}] {}", level, message);
        }
    }
}

/// Configure the log callback.
pub fn sqlite3_config_log(callback: impl Fn(LogLevel, &str) + Send + Sync + 'static) {
    if let Ok(mut guard) = LOG_CALLBACK.lock() {
        *guard = Some(Box::new(callback));
    }
}

/// Clear the log callback.
pub fn sqlite3_clear_log_callback() {
    if let Ok(mut guard) = LOG_CALLBACK.lock() {
        *guard = None;
    }
}

// ============================================================================
// Error strings
// ============================================================================

/// Get error string for code.
pub fn sqlite3_errstr(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::Ok => "not an error",
        ErrorCode::Error => "SQL logic error",
        ErrorCode::Internal => "internal error",
        ErrorCode::Perm => "access permission denied",
        ErrorCode::Abort => "query aborted",
        ErrorCode::Busy => "database is locked",
        ErrorCode::Locked => "database table is locked",
        ErrorCode::NoMem => "out of memory",
        ErrorCode::ReadOnly => "attempt to write a readonly database",
        ErrorCode::Interrupt => "interrupted",
        ErrorCode::IoErr => "disk I/O error",
        ErrorCode::Corrupt => "database disk image is malformed",
        ErrorCode::NotFound => "unknown operation",
        ErrorCode::Full => "database or disk is full",
        ErrorCode::CantOpen => "unable to open database file",
        ErrorCode::Protocol => "locking protocol",
        ErrorCode::Empty => "empty",
        ErrorCode::Schema => "database schema has changed",
        ErrorCode::TooBig => "string or blob too big",
        ErrorCode::Constraint => "constraint failed",
        ErrorCode::Mismatch => "datatype mismatch",
        ErrorCode::Misuse => "bad parameter or other API misuse",
        ErrorCode::NoLfs => "large file support is disabled",
        ErrorCode::Auth => "authorization denied",
        ErrorCode::Format => "file format error",
        ErrorCode::Range => "column index out of range",
        ErrorCode::NotADb => "file is not a database",
        ErrorCode::Notice => "notification message",
        ErrorCode::Warning => "warning message",
        ErrorCode::Row => "another row available",
        ErrorCode::Done => "no more rows available",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isint() {
        assert!(sqlite3_isint("123"));
        assert!(sqlite3_isint("  -42 "));
        assert!(!sqlite3_isint("+"));
        assert!(!sqlite3_isint("12a"));
    }

    #[test]
    fn test_stricmp() {
        assert_eq!(sqlite3_stricmp("abc", "AbC"), Ordering::Equal);
        assert_eq!(sqlite3_stricmp("abc", "abd"), Ordering::Less);
    }

    #[test]
    fn test_strnicmp() {
        assert_eq!(sqlite3_strnicmp("abc", "ABd", 2), Ordering::Equal);
        assert_eq!(sqlite3_strnicmp("abc", "ABd", 3), Ordering::Less);
    }

    #[test]
    fn test_atoi64() {
        assert_eq!(sqlite3_atoi64("9223372036854775807").unwrap(), i64::MAX);
        assert_eq!(sqlite3_atoi64("-9223372036854775808").unwrap(), i64::MIN);
        assert!(sqlite3_atoi64("9223372036854775808").is_err());
        assert!(sqlite3_atoi64("abc").is_err());
    }

    #[test]
    fn test_varint_roundtrip() {
        let values = [0u64, 1, 127, 128, 16384, u32::MAX as u64, u64::MAX >> 1];
        for value in values {
            let mut buf = [0u8; 9];
            let written = sqlite3_put_varint(&mut buf, value);
            let (decoded, consumed) = sqlite3_get_varint(&buf);
            assert_eq!(decoded, value);
            assert_eq!(consumed, written);
        }
    }

    #[test]
    fn test_apply_affinity() {
        let mut value = Value::Text("123".to_string());
        sqlite3_apply_affinity(&mut value, Affinity::Integer);
        assert_eq!(value, Value::Integer(123));
    }

    #[test]
    fn test_log_callback() {
        let entries = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let entries_clone = entries.clone();
        sqlite3_config_log(move |level, message| {
            entries_clone
                .lock()
                .unwrap()
                .push(format!("{:?}:{}", level, message));
        });
        sqlite3_log(LogLevel::Notice, "hello");
        sqlite3_clear_log_callback();
        let guard = entries.lock().unwrap();
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0], "Notice:hello");
    }
}
