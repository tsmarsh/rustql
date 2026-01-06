# Translate util.c - General Utilities

## Overview
Translate general utility functions including string manipulation, numeric conversion, and memory helpers.

## Source Reference
- `sqlite3/src/util.c` - 1,863 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Functions

### String Utilities

```rust
/// Check if string is a valid integer
pub fn sqlite3_isint(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() {
        return false;
    }

    let mut chars = s.chars().peekable();

    // Optional sign
    if matches!(chars.peek(), Some('+') | Some('-')) {
        chars.next();
    }

    // Must have at least one digit
    if !chars.peek().map(|c| c.is_ascii_digit()).unwrap_or(false) {
        return false;
    }

    // All remaining must be digits
    chars.all(|c| c.is_ascii_digit())
}

/// Safe string comparison (handles NULL)
pub fn sqlite3_strcmp(a: Option<&str>, b: Option<&str>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => a.cmp(b),
    }
}

/// Case-insensitive string comparison
pub fn sqlite3_stricmp(a: &str, b: &str) -> std::cmp::Ordering {
    a.to_lowercase().cmp(&b.to_lowercase())
}

/// Case-insensitive comparison with length limit
pub fn sqlite3_strnicmp(a: &str, b: &str, n: usize) -> std::cmp::Ordering {
    let a_lower: String = a.chars().take(n).flat_map(|c| c.to_lowercase()).collect();
    let b_lower: String = b.chars().take(n).flat_map(|c| c.to_lowercase()).collect();
    a_lower.cmp(&b_lower)
}

/// Duplicate a string (for C compatibility)
pub fn sqlite3_strdup(s: &str) -> String {
    s.to_string()
}

/// Printf-style string formatting
pub fn sqlite3_mprintf(format: &str, args: &[&dyn std::fmt::Display]) -> String {
    // Simplified - in practice would use the printf module
    let mut result = format.to_string();
    for arg in args {
        if let Some(pos) = result.find('%') {
            // Find format specifier end
            let end = result[pos..].find(|c: char| c.is_alphabetic())
                .map(|i| pos + i + 1)
                .unwrap_or(result.len());
            result.replace_range(pos..end, &arg.to_string());
        }
    }
    result
}
```

### Numeric Conversion

```rust
/// Convert string to integer with overflow detection
pub fn sqlite3_atoi(s: &str) -> i32 {
    s.trim().parse().unwrap_or(0)
}

/// Convert string to 64-bit integer
pub fn sqlite3_atoi64(s: &str) -> Result<i64> {
    let s = s.trim();
    s.parse().map_err(|_| Error::with_message(
        ErrorCode::Error,
        format!("invalid integer: {}", s)
    ))
}

/// Safe integer addition with overflow check
pub fn sqlite3_add_i64(a: i64, b: i64) -> Result<i64> {
    a.checked_add(b).ok_or_else(|| Error::with_message(
        ErrorCode::Error,
        "integer overflow"
    ))
}

/// Safe integer subtraction with overflow check
pub fn sqlite3_sub_i64(a: i64, b: i64) -> Result<i64> {
    a.checked_sub(b).ok_or_else(|| Error::with_message(
        ErrorCode::Error,
        "integer overflow"
    ))
}

/// Safe integer multiplication with overflow check
pub fn sqlite3_mul_i64(a: i64, b: i64) -> Result<i64> {
    a.checked_mul(b).ok_or_else(|| Error::with_message(
        ErrorCode::Error,
        "integer overflow"
    ))
}

/// Convert double to integer with range check
pub fn sqlite3_real_to_i64(r: f64) -> Result<i64> {
    if r.is_nan() || r < i64::MIN as f64 || r > i64::MAX as f64 {
        return Err(Error::with_message(ErrorCode::Error, "value out of range"));
    }
    Ok(r as i64)
}

/// Format integer as decimal string
pub fn sqlite3_i64_to_str(n: i64) -> String {
    n.to_string()
}

/// Format double with appropriate precision
pub fn sqlite3_real_to_str(r: f64, precision: Option<usize>) -> String {
    match precision {
        Some(p) => format!("{:.prec$}", r, prec = p),
        None => {
            // Use minimal representation
            let s = format!("{}", r);
            if s.contains('.') {
                s.trim_end_matches('0').trim_end_matches('.').to_string()
            } else {
                s
            }
        }
    }
}
```

### Varint Encoding

```rust
/// Read a varint from a byte slice
pub fn sqlite3_get_varint(buf: &[u8]) -> (u64, usize) {
    let mut result: u64 = 0;
    let mut bytes_read = 0;

    for (i, &byte) in buf.iter().enumerate().take(9) {
        bytes_read = i + 1;

        if i == 8 {
            // 9th byte uses all 8 bits
            result = (result << 8) | (byte as u64);
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

/// Write a varint to a byte buffer
pub fn sqlite3_put_varint(buf: &mut [u8], value: u64) -> usize {
    if value <= 0x7f {
        buf[0] = value as u8;
        return 1;
    }

    let mut v = value;
    let mut temp = [0u8; 9];
    let mut i = 8;

    // Last byte stores full 8 bits
    temp[i] = (v & 0xff) as u8;
    v >>= 8;
    i -= 1;

    // Remaining bytes store 7 bits each
    while v > 0x7f {
        temp[i] = ((v & 0x7f) | 0x80) as u8;
        v >>= 7;
        i -= 1;
    }
    temp[i] = ((v & 0x7f) | 0x80) as u8;

    let len = 9 - i;
    buf[..len].copy_from_slice(&temp[i..]);

    len
}

/// Get the length of a varint without reading it
pub fn sqlite3_varint_len(value: u64) -> usize {
    if value < (1 << 7) { 1 }
    else if value < (1 << 14) { 2 }
    else if value < (1 << 21) { 3 }
    else if value < (1 << 28) { 4 }
    else if value < (1 << 35) { 5 }
    else if value < (1 << 42) { 6 }
    else if value < (1 << 49) { 7 }
    else if value < (1 << 56) { 8 }
    else { 9 }
}
```

### Affinity and Type Conversion

```rust
/// Column affinity types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Affinity {
    Blob,
    Text,
    Numeric,
    Integer,
    Real,
}

/// Determine affinity from type name
pub fn sqlite3_affinity_type(type_name: &str) -> Affinity {
    let upper = type_name.to_uppercase();

    // Rule 1: INT in name -> INTEGER
    if upper.contains("INT") {
        return Affinity::Integer;
    }

    // Rule 2: CHAR, CLOB, TEXT -> TEXT
    if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        return Affinity::Text;
    }

    // Rule 3: BLOB or empty -> BLOB
    if upper.contains("BLOB") || upper.is_empty() {
        return Affinity::Blob;
    }

    // Rule 4: REAL, FLOA, DOUB -> REAL
    if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        return Affinity::Real;
    }

    // Rule 5: Otherwise -> NUMERIC
    Affinity::Numeric
}

/// Apply affinity to a value
pub fn sqlite3_apply_affinity(value: &mut Value, affinity: Affinity) {
    match affinity {
        Affinity::Integer => {
            if let Value::Text(s) = value {
                if let Ok(i) = s.parse::<i64>() {
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
                if let Ok(r) = s.parse::<f64>() {
                    *value = Value::Real(r);
                }
            } else if let Value::Integer(i) = value {
                *value = Value::Real(*i as f64);
            }
        }
        Affinity::Numeric => {
            // Try integer first, then real
            if let Value::Text(s) = value {
                if let Ok(i) = s.parse::<i64>() {
                    *value = Value::Integer(i);
                } else if let Ok(r) = s.parse::<f64>() {
                    *value = Value::Real(r);
                }
            }
        }
        Affinity::Text => {
            match value {
                Value::Integer(i) => *value = Value::Text(i.to_string()),
                Value::Real(r) => *value = Value::Text(sqlite3_real_to_str(*r, None)),
                Value::Blob(b) => *value = Value::Text(String::from_utf8_lossy(b).to_string()),
                _ => {}
            }
        }
        Affinity::Blob => {
            // No conversion for BLOB affinity
        }
    }
}
```

### Safe Buffer Operations

```rust
/// Safe memcpy with bounds checking
pub fn sqlite3_memcpy(dst: &mut [u8], src: &[u8], n: usize) {
    let copy_len = n.min(dst.len()).min(src.len());
    dst[..copy_len].copy_from_slice(&src[..copy_len]);
}

/// Safe memset
pub fn sqlite3_memset(dst: &mut [u8], val: u8, n: usize) {
    let fill_len = n.min(dst.len());
    dst[..fill_len].fill(val);
}

/// Safe memcmp
pub fn sqlite3_memcmp(a: &[u8], b: &[u8], n: usize) -> std::cmp::Ordering {
    let cmp_len = n.min(a.len()).min(b.len());
    a[..cmp_len].cmp(&b[..cmp_len])
}
```

### Logging

```rust
/// Log levels
#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Error = 1,
    Warning = 2,
    Notice = 3,
}

/// Global log callback
static LOG_CALLBACK: Mutex<Option<Box<dyn Fn(LogLevel, &str) + Send + Sync>>> =
    Mutex::new(None);

pub fn sqlite3_log(level: LogLevel, message: &str) {
    if let Ok(guard) = LOG_CALLBACK.lock() {
        if let Some(ref callback) = *guard {
            callback(level, message);
        } else {
            eprintln!("[SQLite {:?}] {}", level, message);
        }
    }
}

pub fn sqlite3_config_log(callback: impl Fn(LogLevel, &str) + Send + Sync + 'static) {
    if let Ok(mut guard) = LOG_CALLBACK.lock() {
        *guard = Some(Box::new(callback));
    }
}
```

## Acceptance Criteria
- [ ] String comparison functions (case-sensitive/insensitive)
- [ ] String to integer conversion
- [ ] Integer to string conversion
- [ ] Double to string conversion
- [ ] Varint encoding/decoding
- [ ] Overflow-safe arithmetic
- [ ] Type affinity determination
- [ ] Affinity application to values
- [ ] Safe memory operations
- [ ] Logging infrastructure
- [ ] Error code to string mapping
