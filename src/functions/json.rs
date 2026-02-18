//! JSON functions
//!
//! This module implements SQLite's JSON1 extension functions.
//! It includes a from-scratch JSON parser and serializer (no external crates).

use crate::error::{Error, ErrorCode, Result};
use crate::types::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Integer(i64),
    Real(f64),
    String(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

/// Maximum JSON nesting depth (matches SQLite's JSON_MAX_DEPTH)
const JSON_MAX_DEPTH: usize = 1000;

struct JsonParser<'a> {
    input: &'a [u8],
    pos: usize,
    depth: usize,
    /// When true, allow relaxed JSON: trailing commas, unquoted keys, etc.
    /// SQLite's json() and other functions accept relaxed JSON, but json_valid() uses strict mode.
    relaxed: bool,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
            depth: 0,
            relaxed: true,
        }
    }
    fn new_strict(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
            depth: 0,
            relaxed: false,
        }
    }
    fn skip_ws(&mut self) {
        while self.pos < self.input.len() {
            match self.input[self.pos] {
                b' ' | b'\t' | b'\n' | b'\r' => self.pos += 1,
                _ => break,
            }
        }
    }
    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }
    fn advance(&mut self) -> Option<u8> {
        if self.pos < self.input.len() {
            let b = self.input[self.pos];
            self.pos += 1;
            Some(b)
        } else {
            None
        }
    }
    fn expect(&mut self, ch: u8) -> std::result::Result<(), ()> {
        if self.peek() == Some(ch) {
            self.pos += 1;
            Ok(())
        } else {
            Err(())
        }
    }
    fn parse(&mut self) -> std::result::Result<JsonValue, ()> {
        self.skip_ws();
        let val = self.parse_value()?;
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(());
        }
        Ok(val)
    }
    fn parse_value(&mut self) -> std::result::Result<JsonValue, ()> {
        self.skip_ws();
        match self.peek().ok_or(())? {
            b'"' => self.parse_string().map(JsonValue::String),
            b'\'' if self.relaxed => self.parse_single_quoted_string().map(JsonValue::String),
            b'{' => self.parse_object(),
            b'[' => self.parse_array(),
            b't' => self.parse_literal(b"true", JsonValue::Bool(true)),
            b'f' => self.parse_literal(b"false", JsonValue::Bool(false)),
            b'n' => self.parse_literal(b"null", JsonValue::Null),
            b'-' | b'0'..=b'9' => self.parse_number(),
            b'+' if self.relaxed => self.parse_number(),
            b'.' if self.relaxed => self.parse_number(),
            _ => Err(()),
        }
    }
    /// Parse a single-quoted string (relaxed JSON5 extension)
    fn parse_single_quoted_string(&mut self) -> std::result::Result<String, ()> {
        self.expect(b'\'')?;
        let mut s = String::new();
        loop {
            match self.advance().ok_or(())? {
                b'\'' => return Ok(s),
                b'\\' => match self.advance().ok_or(())? {
                    b'\'' => s.push('\''),
                    b'"' => s.push('"'),
                    b'\\' => s.push('\\'),
                    b'/' => s.push('/'),
                    b'b' => s.push('\u{0008}'),
                    b'f' => s.push('\u{000C}'),
                    b'n' => s.push('\n'),
                    b'r' => s.push('\r'),
                    b't' => s.push('\t'),
                    b'u' => {
                        let cp = self.parse_hex4()?;
                        if (0xD800..=0xDBFF).contains(&cp) {
                            if self.advance() != Some(b'\\') || self.advance() != Some(b'u') {
                                return Err(());
                            }
                            let cp2 = self.parse_hex4()?;
                            if !(0xDC00..=0xDFFF).contains(&cp2) {
                                return Err(());
                            }
                            let combined =
                                0x10000 + ((cp as u32 - 0xD800) << 10) + (cp2 as u32 - 0xDC00);
                            s.push(char::from_u32(combined).ok_or(())?);
                        } else {
                            s.push(char::from_u32(cp as u32).ok_or(())?);
                        }
                    }
                    _ => return Err(()),
                },
                b if b < 0x20 => return Err(()),
                b => {
                    if b < 0x80 {
                        s.push(b as char);
                    } else {
                        self.pos -= 1;
                        let start = self.pos;
                        let n = if b & 0xE0 == 0xC0 {
                            2
                        } else if b & 0xF0 == 0xE0 {
                            3
                        } else if b & 0xF8 == 0xF0 {
                            4
                        } else {
                            return Err(());
                        };
                        if self.pos + n > self.input.len() {
                            return Err(());
                        }
                        let utf8 =
                            std::str::from_utf8(&self.input[start..start + n]).map_err(|_| ())?;
                        s.push_str(utf8);
                        self.pos += n;
                    }
                }
            }
        }
    }
    fn parse_literal(
        &mut self,
        expected: &[u8],
        val: JsonValue,
    ) -> std::result::Result<JsonValue, ()> {
        for &b in expected {
            if self.advance() != Some(b) {
                return Err(());
            }
        }
        Ok(val)
    }
    fn parse_string(&mut self) -> std::result::Result<String, ()> {
        self.expect(b'"')?;
        let mut s = String::new();
        loop {
            match self.advance().ok_or(())? {
                b'"' => return Ok(s),
                b'\\' => match self.advance().ok_or(())? {
                    b'"' => s.push('"'),
                    b'\\' => s.push('\\'),
                    b'/' => s.push('/'),
                    b'b' => s.push('\u{0008}'),
                    b'f' => s.push('\u{000C}'),
                    b'n' => s.push('\n'),
                    b'r' => s.push('\r'),
                    b't' => s.push('\t'),
                    b'u' => {
                        let cp = self.parse_hex4()?;
                        if (0xD800..=0xDBFF).contains(&cp) {
                            if self.advance() != Some(b'\\') || self.advance() != Some(b'u') {
                                return Err(());
                            }
                            let cp2 = self.parse_hex4()?;
                            if !(0xDC00..=0xDFFF).contains(&cp2) {
                                return Err(());
                            }
                            let combined =
                                0x10000 + ((cp as u32 - 0xD800) << 10) + (cp2 as u32 - 0xDC00);
                            s.push(char::from_u32(combined).ok_or(())?);
                        } else {
                            s.push(char::from_u32(cp as u32).ok_or(())?);
                        }
                    }
                    _ => return Err(()),
                },
                b if b < 0x20 => return Err(()),
                b => {
                    if b < 0x80 {
                        s.push(b as char);
                    } else {
                        self.pos -= 1;
                        let start = self.pos;
                        let n = if b & 0xE0 == 0xC0 {
                            2
                        } else if b & 0xF0 == 0xE0 {
                            3
                        } else if b & 0xF8 == 0xF0 {
                            4
                        } else {
                            return Err(());
                        };
                        if self.pos + n > self.input.len() {
                            return Err(());
                        }
                        let utf8 =
                            std::str::from_utf8(&self.input[start..start + n]).map_err(|_| ())?;
                        s.push_str(utf8);
                        self.pos += n;
                    }
                }
            }
        }
    }
    fn parse_hex4(&mut self) -> std::result::Result<u16, ()> {
        let mut val: u16 = 0;
        for _ in 0..4 {
            let b = self.advance().ok_or(())?;
            let digit = match b {
                b'0'..=b'9' => b - b'0',
                b'a'..=b'f' => b - b'a' + 10,
                b'A'..=b'F' => b - b'A' + 10,
                _ => return Err(()),
            };
            val = val * 16 + digit as u16;
        }
        Ok(val)
    }
    fn parse_number(&mut self) -> std::result::Result<JsonValue, ()> {
        let start = self.pos;
        if self.peek() == Some(b'-') || (self.relaxed && self.peek() == Some(b'+')) {
            self.pos += 1;
        }
        let digit_start = self.pos;
        // In relaxed mode, allow leading dot (e.g., .5)
        if self.relaxed && self.peek() == Some(b'.') {
            // handled below by is_float
        } else {
            match self.peek() {
                Some(b'0') => {
                    self.pos += 1;
                }
                Some(b'1'..=b'9') => {
                    while let Some(b'0'..=b'9') = self.peek() {
                        self.pos += 1;
                    }
                }
                _ => return Err(()),
            }
            if self.pos == digit_start {
                return Err(());
            }
        }
        let mut is_float = false;
        if self.peek() == Some(b'.') {
            is_float = true;
            self.pos += 1;
            let fs = self.pos;
            while let Some(b'0'..=b'9') = self.peek() {
                self.pos += 1;
            }
            if self.pos == fs {
                return Err(());
            }
        }
        if matches!(self.peek(), Some(b'e') | Some(b'E')) {
            is_float = true;
            self.pos += 1;
            if matches!(self.peek(), Some(b'+') | Some(b'-')) {
                self.pos += 1;
            }
            let es = self.pos;
            while let Some(b'0'..=b'9') = self.peek() {
                self.pos += 1;
            }
            if self.pos == es {
                return Err(());
            }
        }
        let s = std::str::from_utf8(&self.input[start..self.pos]).map_err(|_| ())?;
        if is_float {
            Ok(JsonValue::Real(s.parse().map_err(|_| ())?))
        } else if let Ok(n) = s.parse::<i64>() {
            Ok(JsonValue::Integer(n))
        } else {
            Ok(JsonValue::Real(s.parse().map_err(|_| ())?))
        }
    }
    fn parse_array(&mut self) -> std::result::Result<JsonValue, ()> {
        self.expect(b'[')?;
        self.depth += 1;
        if self.depth > JSON_MAX_DEPTH {
            return Err(());
        }
        self.skip_ws();
        let mut arr = Vec::new();
        if self.peek() == Some(b']') {
            self.pos += 1;
            self.depth -= 1;
            return Ok(JsonValue::Array(arr));
        }
        loop {
            arr.push(self.parse_value()?);
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                    self.skip_ws();
                    // Allow trailing comma in relaxed mode (SQLite JSON1 extension behavior)
                    if self.relaxed && self.peek() == Some(b']') {
                        self.pos += 1;
                        self.depth -= 1;
                        return Ok(JsonValue::Array(arr));
                    }
                }
                Some(b']') => {
                    self.pos += 1;
                    self.depth -= 1;
                    return Ok(JsonValue::Array(arr));
                }
                _ => return Err(()),
            }
        }
    }
    fn parse_object(&mut self) -> std::result::Result<JsonValue, ()> {
        self.expect(b'{')?;
        self.depth += 1;
        if self.depth > JSON_MAX_DEPTH {
            return Err(());
        }
        self.skip_ws();
        let mut obj = Vec::new();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            self.depth -= 1;
            return Ok(JsonValue::Object(obj));
        }
        loop {
            self.skip_ws();
            let key = if self.relaxed && self.peek() != Some(b'"') && self.peek() != Some(b'\'') {
                // Relaxed: allow unquoted keys (like JSON5 / SQLite extension)
                self.parse_unquoted_key()?
            } else if self.relaxed && self.peek() == Some(b'\'') {
                self.parse_single_quoted_string()?
            } else {
                self.parse_string()?
            };
            self.skip_ws();
            self.expect(b':')?;
            let val = self.parse_value()?;
            obj.push((key, val));
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                    self.skip_ws();
                    // Allow trailing comma in relaxed mode (SQLite JSON1 extension behavior)
                    if self.relaxed && self.peek() == Some(b'}') {
                        self.pos += 1;
                        self.depth -= 1;
                        return Ok(JsonValue::Object(obj));
                    }
                }
                Some(b'}') => {
                    self.pos += 1;
                    self.depth -= 1;
                    return Ok(JsonValue::Object(obj));
                }
                _ => return Err(()),
            }
        }
    }
    /// Parse an unquoted key (relaxed JSON5 extension).
    /// Keys can contain alphanumeric characters, underscores, etc.
    fn parse_unquoted_key(&mut self) -> std::result::Result<String, ()> {
        let mut key = String::new();
        while let Some(b) = self.peek() {
            match b {
                b':' | b' ' | b'\t' | b'\n' | b'\r' | b'}' | b',' => break,
                _ => {
                    key.push(b as char);
                    self.pos += 1;
                }
            }
        }
        if key.is_empty() {
            return Err(());
        }
        Ok(key)
    }
}

fn parse_json(s: &str) -> std::result::Result<JsonValue, ()> {
    JsonParser::new(s).parse()
}

/// Parse JSON in strict mode (no trailing commas, no unquoted keys, no JSON5 extensions).
/// Used by json_valid() to validate strict RFC 7159 JSON.
fn parse_json_strict(s: &str) -> std::result::Result<JsonValue, ()> {
    JsonParser::new_strict(s).parse()
}

/// Parse JSON and return error position (1-based) on failure, 0 on success.
/// Uses relaxed parsing (SQLite's json_error_position accepts JSON5 extensions).
fn json_error_pos(s: &str) -> i64 {
    let mut parser = JsonParser::new(s);
    parser.skip_ws();
    match parser.parse_value() {
        Err(()) => (parser.pos + 1) as i64,
        Ok(_) => {
            parser.skip_ws();
            if parser.pos != parser.input.len() {
                (parser.pos + 1) as i64
            } else {
                0
            }
        }
    }
}
fn json_to_string(val: &JsonValue) -> String {
    let mut b = String::new();
    write_json(val, &mut b);
    b
}

fn write_json(val: &JsonValue, buf: &mut String) {
    match val {
        JsonValue::Null => buf.push_str("null"),
        JsonValue::Bool(b) => buf.push_str(if *b { "true" } else { "false" }),
        JsonValue::Integer(n) => buf.push_str(&n.to_string()),
        JsonValue::Real(f) => buf.push_str(&sqlite_float_fmt(*f)),
        JsonValue::String(s) => write_json_string(s, buf),
        JsonValue::Array(arr) => {
            buf.push('[');
            for (i, v) in arr.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                write_json(v, buf);
            }
            buf.push(']');
        }
        JsonValue::Object(obj) => {
            buf.push('{');
            for (i, (k, v)) in obj.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                write_json_string(k, buf);
                buf.push(':');
                write_json(v, buf);
            }
            buf.push('}');
        }
    }
}

fn write_json_string(s: &str, buf: &mut String) {
    buf.push('"');
    for ch in s.chars() {
        match ch {
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            '\u{0008}' => buf.push_str("\\b"),
            '\u{000C}' => buf.push_str("\\f"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                buf.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => buf.push(c),
        }
    }
    buf.push('"');
}

fn sqlite_float_fmt(f: f64) -> String {
    if f.is_infinite() {
        return if f > 0.0 {
            "9.0e+999".into()
        } else {
            "-9.0e+999".into()
        };
    }
    if f.is_nan() {
        return "null".into();
    }
    // Check if the float is a small-ish value that doesn't need scientific notation
    let abs = f.abs();
    if abs == 0.0 || (abs >= 1e-4 && abs < 1e15) {
        // Try the default Display format (shortest representation)
        let s = format!("{}", f);
        if !s.contains('.') && !s.contains('e') && !s.contains('E') {
            format!("{}.0", s)
        } else {
            s
        }
    } else {
        // Use scientific notation like SQLite: %.17e, then strip trailing zeros
        sqlite_json_float_scientific(f)
    }
}

/// Format a float using SQLite-compatible scientific notation.
/// Tries progressively more digits (1..17) to find the shortest representation
/// that round-trips, then formats in scientific notation with explicit exponent sign.
fn sqlite_json_float_scientific(f: f64) -> String {
    // Try increasing precision until the formatted value round-trips
    for prec in 1..=17 {
        let candidate = format!("{:.prec$e}", f, prec = prec);
        if let Ok(parsed) = candidate.parse::<f64>() {
            if parsed == f {
                return format_scientific_str(&candidate);
            }
        }
    }
    // Fallback to 17 digits
    format_scientific_str(&format!("{:.17e}", f))
}

/// Normalize a scientific notation string: strip trailing zeros, add explicit exponent sign.
fn format_scientific_str(raw: &str) -> String {
    let (sign, rest) = if raw.starts_with('-') {
        ("-", &raw[1..])
    } else {
        ("", &raw[..])
    };
    let parts: Vec<&str> = rest.splitn(2, 'e').collect();
    if parts.len() != 2 {
        return raw.to_string();
    }
    let mantissa = parts[0];
    let exp: i64 = parts[1].parse().unwrap_or(0);
    // Strip trailing zeros from mantissa (keep at least one decimal digit)
    let mantissa = if let Some(dot_pos) = mantissa.find('.') {
        let decimal_part = &mantissa[dot_pos + 1..];
        let trimmed = decimal_part.trim_end_matches('0');
        if trimmed.is_empty() {
            format!("{}.0", &mantissa[..dot_pos])
        } else {
            format!("{}.{}", &mantissa[..dot_pos], trimmed)
        }
    } else {
        format!("{}.0", mantissa)
    };
    let exp_str = if exp >= 0 {
        format!("e+{}", exp)
    } else {
        format!("e{}", exp)
    };
    format!("{}{}{}", sign, mantissa, exp_str)
}

/// Parse a key from a JSON path. Supports both quoted ("key") and unquoted keys.
/// Returns None on error.
fn parse_path_key(chars: &mut std::iter::Peekable<std::str::Chars>) -> Option<String> {
    if chars.peek() == Some(&'"') {
        // Quoted key: read until closing quote
        chars.next(); // consume opening quote
        let mut key = String::new();
        loop {
            match chars.next() {
                Some('"') => return Some(key),
                Some('\\') => {
                    if let Some(c) = chars.next() {
                        key.push(c);
                    } else {
                        return None;
                    }
                }
                Some(c) => key.push(c),
                None => return None, // unterminated quote
            }
        }
    } else {
        // Unquoted key: read until . or [ or end
        let mut key = String::new();
        while let Some(&c) = chars.peek() {
            if c == '.' || c == '[' {
                break;
            }
            key.push(c);
            chars.next();
        }
        if key.is_empty() {
            return None;
        }
        Some(key)
    }
}

/// Validate a JSON path. Returns Err with an error message if invalid.
fn validate_path(path: &str) -> std::result::Result<(), String> {
    if !path.starts_with('$') {
        return Err(format!("bad JSON path: '{}'", path));
    }
    if path == "$" {
        return Ok(());
    }
    let rest = &path[1..];
    if rest.is_empty() {
        return Ok(());
    }
    let mut chars = rest.chars().peekable();
    while chars.peek().is_some() {
        match chars.peek() {
            Some(&'.') => {
                chars.next();
                // After '.', must have a key (quoted or unquoted)
                if chars.peek().is_none() {
                    return Err(format!("bad JSON path: '{}'", path));
                }
                if chars.peek() == Some(&'"') {
                    // Quoted key
                    chars.next(); // opening quote
                    loop {
                        match chars.next() {
                            Some('"') => break,
                            Some('\\') => {
                                chars.next(); // skip escaped char
                            }
                            Some(_) => {}
                            None => return Err(format!("bad JSON path: '{}'", path)),
                        }
                    }
                } else {
                    // Unquoted key - must have at least one character
                    let mut found = false;
                    while let Some(&c) = chars.peek() {
                        if c == '.' || c == '[' {
                            break;
                        }
                        found = true;
                        chars.next();
                    }
                    if !found {
                        return Err(format!("bad JSON path: '{}'", path));
                    }
                }
            }
            Some(&'[') => {
                chars.next();
                // Read until ]
                let mut found_close = false;
                while let Some(&c) = chars.peek() {
                    if c == ']' {
                        chars.next();
                        found_close = true;
                        break;
                    }
                    chars.next();
                }
                if !found_close {
                    return Err(format!("bad JSON path: '{}'", path));
                }
            }
            _ => return Err(format!("bad JSON path: '{}'", path)),
        }
    }
    Ok(())
}

fn extract_path<'a>(
    root: &'a JsonValue,
    path: &str,
) -> std::result::Result<Option<&'a JsonValue>, String> {
    if path == "$" {
        return Ok(Some(root));
    }
    if !path.starts_with('$') {
        return Err(format!("bad JSON path: '{}'", path));
    }
    validate_path(path)?;
    let mut current = root;
    let mut chars = path[1..].chars().peekable();
    while chars.peek().is_some() {
        match chars.peek() {
            Some(&'.') => {
                chars.next();
                let key = match parse_path_key(&mut chars) {
                    Some(k) => k,
                    None => return Err(format!("bad JSON path: '{}'", path)),
                };
                match current {
                    JsonValue::Object(obj) => {
                        match obj.iter().find(|(k, _)| k == &key).map(|(_, v)| v) {
                            Some(v) => current = v,
                            None => return Ok(None),
                        }
                    }
                    _ => return Ok(None),
                }
            }
            Some(&'[') => {
                chars.next();
                let mut idx_s = String::new();
                while let Some(&c) = chars.peek() {
                    if c == ']' {
                        break;
                    }
                    idx_s.push(c);
                    chars.next();
                }
                if chars.next() != Some(']') {
                    return Err(format!("bad JSON path: '{}'", path));
                }
                if let Ok(idx) = idx_s.parse::<usize>() {
                    match current {
                        JsonValue::Array(arr) => match arr.get(idx) {
                            Some(v) => current = v,
                            None => return Ok(None),
                        },
                        _ => return Ok(None),
                    }
                } else if idx_s == "#" {
                    // $[#] means array length (for insert at end)
                    return Ok(None);
                } else if idx_s.starts_with("#-") {
                    if let JsonValue::Array(arr) = current {
                        if let Ok(n) = idx_s[2..].parse::<usize>() {
                            if n <= arr.len() {
                                match arr.get(arr.len() - n) {
                                    Some(v) => current = v,
                                    None => return Ok(None),
                                }
                            } else {
                                return Ok(None);
                            }
                        } else {
                            return Err(format!("bad JSON path: '{}'", path));
                        }
                    } else {
                        return Ok(None);
                    }
                } else {
                    return Err(format!("bad JSON path: '{}'", path));
                }
            }
            _ => return Err(format!("bad JSON path: '{}'", path)),
        }
    }
    Ok(Some(current))
}

fn json_to_sql_value(jv: &JsonValue) -> Value {
    match jv {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
        JsonValue::Integer(n) => Value::Integer(*n),
        JsonValue::Real(f) => {
            // JSON 9.0e+999 maps to SQL Infinity
            if *f == f64::INFINITY || *f == f64::NEG_INFINITY || f.is_nan() {
                Value::Real(*f)
            } else {
                Value::Real(*f)
            }
        }
        JsonValue::String(s) => Value::Text(s.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Text(json_to_string(jv)),
    }
}

fn sql_to_json(val: &Value) -> JsonValue {
    match val {
        Value::Null => JsonValue::Null,
        Value::Integer(n) => JsonValue::Integer(*n),
        Value::Real(f) => JsonValue::Real(*f),
        Value::Text(s) => {
            // Try to parse as JSON - if valid, return the parsed value for
            // structured types (arrays, objects).
            if let Ok(jv) = parse_json(s) {
                match &jv {
                    JsonValue::Array(_) | JsonValue::Object(_) => return jv,
                    _ => {}
                }
            }
            JsonValue::String(s.clone())
        }
        Value::Blob(b) => JsonValue::String(String::from_utf8_lossy(b).to_string()),
    }
}

fn set_at_path(root: &mut JsonValue, path: &str, value: JsonValue, create: bool) -> bool {
    if path == "$" {
        *root = value;
        return true;
    }
    if !path.starts_with('$') {
        return false;
    }
    set_impl(root, &path[1..], value, create)
}

fn set_impl(cur: &mut JsonValue, path: &str, value: JsonValue, create: bool) -> bool {
    if path.is_empty() {
        *cur = value;
        return true;
    }
    let mut chars = path.chars().peekable();
    match chars.peek() {
        Some('.') => {
            chars.next();
            let key = match parse_path_key(&mut chars) {
                Some(k) => k,
                None => return false,
            };
            let rest: String = chars.collect();
            match cur {
                JsonValue::Object(obj) => {
                    if let Some(pos) = obj.iter().position(|(k, _)| k == &key) {
                        if rest.is_empty() {
                            obj[pos].1 = value;
                            return true;
                        }
                        return set_impl(&mut obj[pos].1, &rest, value, create);
                    }
                    if create {
                        if rest.is_empty() {
                            obj.push((key, value));
                        } else {
                            // Determine intermediate structure based on rest path
                            let mut nv = if rest.starts_with('[') {
                                JsonValue::Array(Vec::new())
                            } else {
                                JsonValue::Object(Vec::new())
                            };
                            if set_impl(&mut nv, &rest, value, true) {
                                obj.push((key, nv));
                            } else {
                                return false;
                            }
                        }
                        return true;
                    }
                    false
                }
                _ => false,
            }
        }
        Some('[') => {
            chars.next();
            let mut is = String::new();
            while let Some(&c) = chars.peek() {
                if c == ']' {
                    break;
                }
                is.push(c);
                chars.next();
            }
            if chars.next() != Some(']') {
                return false;
            }
            let rest: String = chars.collect();
            if is == "#" {
                // $[#] means append to end of array
                match cur {
                    JsonValue::Array(arr) => {
                        if rest.is_empty() {
                            arr.push(value);
                            return true;
                        }
                        false
                    }
                    _ => false,
                }
            } else if let Ok(idx) = is.parse::<usize>() {
                match cur {
                    JsonValue::Array(arr) => {
                        if idx < arr.len() {
                            if rest.is_empty() {
                                arr[idx] = value;
                                return true;
                            }
                            return set_impl(&mut arr[idx], &rest, value, create);
                        }
                        if create {
                            if rest.is_empty() {
                                while arr.len() < idx {
                                    arr.push(JsonValue::Null);
                                }
                                arr.push(value);
                                return true;
                            } else {
                                // Create intermediate structure
                                while arr.len() <= idx {
                                    arr.push(JsonValue::Null);
                                }
                                let mut nv = if rest.starts_with('[') {
                                    JsonValue::Array(Vec::new())
                                } else if rest.starts_with('.') {
                                    JsonValue::Object(Vec::new())
                                } else {
                                    JsonValue::Null
                                };
                                if set_impl(&mut nv, &rest, value, true) {
                                    arr[idx] = nv;
                                    return true;
                                }
                                return false;
                            }
                        }
                        false
                    }
                    _ => false,
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

fn remove_at_path(root: &mut JsonValue, path: &str) -> bool {
    if path == "$" || !path.starts_with('$') {
        return false;
    }
    rm_impl(root, &path[1..])
}

fn rm_impl(cur: &mut JsonValue, path: &str) -> bool {
    if path.is_empty() {
        return false;
    }
    let mut chars = path.chars().peekable();
    match chars.peek() {
        Some('.') => {
            chars.next();
            let key = match parse_path_key(&mut chars) {
                Some(k) => k,
                None => return false,
            };
            let rest: String = chars.collect();
            match cur {
                JsonValue::Object(obj) => {
                    if rest.is_empty() {
                        if let Some(p) = obj.iter().position(|(k, _)| k == &key) {
                            obj.remove(p);
                            return true;
                        }
                        return false;
                    }
                    if let Some(p) = obj.iter().position(|(k, _)| k == &key) {
                        return rm_impl(&mut obj[p].1, &rest);
                    }
                    false
                }
                _ => false,
            }
        }
        Some('[') => {
            chars.next();
            let mut is = String::new();
            while let Some(&c) = chars.peek() {
                if c == ']' {
                    break;
                }
                is.push(c);
                chars.next();
            }
            if chars.next() != Some(']') {
                return false;
            }
            let rest: String = chars.collect();
            if let Ok(idx) = is.parse::<usize>() {
                match cur {
                    JsonValue::Array(arr) => {
                        if idx >= arr.len() {
                            return false;
                        }
                        if rest.is_empty() {
                            arr.remove(idx);
                            return true;
                        }
                        rm_impl(&mut arr[idx], &rest)
                    }
                    _ => false,
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

fn merge_patch(target: &JsonValue, patch: &JsonValue) -> JsonValue {
    match patch {
        JsonValue::Object(po) => {
            let mut r = match target {
                JsonValue::Object(o) => o.clone(),
                _ => Vec::new(),
            };
            for (key, pv) in po {
                if matches!(pv, JsonValue::Null) {
                    r.retain(|(k, _)| k != key);
                } else {
                    let ex = r
                        .iter()
                        .find(|(k, _)| k == key)
                        .map(|(_, v)| v.clone())
                        .unwrap_or(JsonValue::Object(Vec::new()));
                    let m = merge_patch(&ex, pv);
                    if let Some(p) = r.iter().position(|(k, _)| k == key) {
                        r[p].1 = m;
                    } else {
                        r.push((key.clone(), m));
                    }
                }
            }
            JsonValue::Object(r)
        }
        other => other.clone(),
    }
}

fn get_json_text(val: &Value) -> std::result::Result<String, Error> {
    match val {
        Value::Null => Ok("null".into()),
        Value::Integer(n) => Ok(n.to_string()),
        Value::Real(f) => Ok(sqlite_float_fmt(*f)),
        Value::Text(s) => Ok(s.clone()),
        Value::Blob(_) => Err(Error::with_message(
            ErrorCode::Error,
            "JSON cannot hold BLOB values",
        )),
    }
}

pub fn func_json(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "wrong number of arguments to function json()",
        ));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let t = get_json_text(&args[0])?;
    let jv = parse_json(&t).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    Ok(Value::Text(json_to_string(&jv)))
}

pub fn func_jsonb(args: &[Value]) -> Result<Value> {
    func_json(args)
}

pub fn func_json_valid(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "wrong number of arguments to function json_valid()",
        ));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    // Second argument is flags (optional, default 1 = check JSON text)
    // We accept it but only implement basic JSON text validation
    let t = match &args[0] {
        Value::Text(s) => s.clone(),
        Value::Blob(_) => return Ok(Value::Integer(0)),
        Value::Integer(n) => n.to_string(),
        Value::Real(f) => sqlite_float_fmt(*f),
        Value::Null => unreachable!(),
    };
    // json_valid uses strict JSON parsing (RFC 7159) - no trailing commas, no unquoted keys
    Ok(Value::Integer(if parse_json_strict(&t).is_ok() {
        1
    } else {
        0
    }))
}

pub fn func_json_error_position(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "wrong number of arguments to function json_error_position()",
        ));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let t = match &args[0] {
        Value::Text(s) => s.clone(),
        Value::Blob(_) => return Ok(Value::Integer(1)),
        Value::Integer(n) => n.to_string(),
        Value::Real(f) => sqlite_float_fmt(*f),
        Value::Null => unreachable!(),
    };
    Ok(Value::Integer(json_error_pos(&t)))
}

/// Normalize a JSON path argument: if it doesn't start with '$', prefix it.
/// Integer values become $[N], string values become $.key
fn normalize_json_path(val: &Value) -> String {
    match val {
        Value::Integer(n) => format!("$[{}]", n),
        Value::Text(s) => {
            if s.starts_with('$') {
                s.clone()
            } else {
                format!("$.{}", s)
            }
        }
        other => {
            let t = other.to_text();
            if t.starts_with('$') {
                t
            } else {
                format!("$.{}", t)
            }
        }
    }
}

pub fn func_json_extract(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::with_message(
            ErrorCode::Error,
            "wrong number of arguments to function json_extract()",
        ));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    if args.len() == 1 {
        // json_extract(JSON) with no path - just validate and return NULL
        let t = get_json_text(&args[0])?;
        let _root =
            parse_json(&t).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
        return Ok(Value::Null);
    }
    // Check for NULL path before parsing JSON (for -> and ->> operators)
    if args.len() == 2 && matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    let t = get_json_text(&args[0])?;
    let root =
        parse_json(&t).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    if args.len() == 2 {
        let p = normalize_json_path(&args[1]);
        match extract_path(&root, &p) {
            Ok(Some(jv)) => Ok(json_to_sql_value(jv)),
            Ok(None) => Ok(Value::Null),
            Err(msg) => Err(Error::with_message(ErrorCode::Error, msg)),
        }
    } else {
        let mut rs = Vec::new();
        for a in &args[1..] {
            if matches!(a, Value::Null) {
                rs.push(JsonValue::Null);
                continue;
            }
            let p = normalize_json_path(a);
            match extract_path(&root, &p) {
                Ok(Some(jv)) => rs.push(jv.clone()),
                Ok(None) => rs.push(JsonValue::Null),
                Err(msg) => return Err(Error::with_message(ErrorCode::Error, msg)),
            }
        }
        Ok(Value::Text(json_to_string(&JsonValue::Array(rs))))
    }
}

pub fn func_json_type(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "wrong number of arguments to function json_type()",
        ));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    // Check for NULL path before parsing JSON
    if args.len() == 2 && matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    let t = get_json_text(&args[0])?;
    let root =
        parse_json(&t).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    let tgt = if args.len() == 2 {
        let p = match &args[1] {
            Value::Text(s) => s.clone(),
            other => other.to_text(),
        };
        match extract_path(&root, &p) {
            Ok(Some(jv)) => jv,
            Ok(None) => return Ok(Value::Null),
            Err(msg) => return Err(Error::with_message(ErrorCode::Error, msg)),
        }
    } else {
        &root
    };
    let tn = match tgt {
        JsonValue::Null => "null",
        JsonValue::Bool(true) => "true",
        JsonValue::Bool(false) => "false",
        JsonValue::Integer(_) => "integer",
        JsonValue::Real(_) => "real",
        JsonValue::String(_) => "text",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    };
    Ok(Value::Text(tn.to_string()))
}

pub fn func_json_array(args: &[Value]) -> Result<Value> {
    for arg in args {
        if matches!(arg, Value::Blob(_)) {
            return Err(Error::with_message(
                ErrorCode::Error,
                "JSON cannot hold BLOB values",
            ));
        }
    }
    Ok(Value::Text(json_to_string(&JsonValue::Array(
        args.iter().map(sql_to_json).collect(),
    ))))
}

pub fn func_json_object(args: &[Value]) -> Result<Value> {
    if args.len() % 2 != 0 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "json_object() requires an even number of arguments",
        ));
    }
    let mut obj = Vec::new();
    for pair in args.chunks(2) {
        let key = match &pair[0] {
            Value::Null => {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "json_object() labels must be TEXT",
                ))
            }
            Value::Blob(_) | Value::Integer(_) | Value::Real(_) => {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "json_object() labels must be TEXT",
                ))
            }
            Value::Text(s) => s.clone(),
        };
        if matches!(&pair[1], Value::Blob(_)) {
            return Err(Error::with_message(
                ErrorCode::Error,
                "JSON cannot hold BLOB values",
            ));
        }
        obj.push((key, sql_to_json(&pair[1])));
    }
    Ok(Value::Text(json_to_string(&JsonValue::Object(obj))))
}

pub fn func_json_array_length(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "wrong number of arguments to function json_array_length()",
        ));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let t = get_json_text(&args[0])?;
    let root =
        parse_json(&t).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    let tgt = if args.len() == 2 {
        if matches!(args[1], Value::Null) {
            return Ok(Value::Null);
        }
        let p = match &args[1] {
            Value::Text(s) => s.clone(),
            other => other.to_text(),
        };
        match extract_path(&root, &p) {
            Ok(Some(jv)) => jv.clone(),
            Ok(None) => return Ok(Value::Null),
            Err(msg) => return Err(Error::with_message(ErrorCode::Error, msg)),
        }
    } else {
        root
    };
    match &tgt {
        JsonValue::Array(arr) => Ok(Value::Integer(arr.len() as i64)),
        _ => Ok(Value::Integer(0)),
    }
}

pub fn func_json_insert(args: &[Value]) -> Result<Value> {
    json_modify(args, false, true, "json_insert")
}
pub fn func_json_set(args: &[Value]) -> Result<Value> {
    json_modify(args, true, true, "json_set")
}
pub fn func_json_replace(args: &[Value]) -> Result<Value> {
    json_modify(args, true, false, "json_replace")
}

fn json_modify(args: &[Value], overwrite: bool, create: bool, name: &str) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::with_message(
            ErrorCode::Error,
            &format!("wrong number of arguments to function {}()", name),
        ));
    }
    // With just 1 arg, validate and return the original text unchanged
    if args.len() == 1 {
        if matches!(args[0], Value::Null) {
            return Ok(Value::Null);
        }
        let t = get_json_text(&args[0])?;
        let _root =
            parse_json(&t).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
        return Ok(Value::Text(t));
    }
    if (args.len() - 1) % 2 != 0 {
        return Err(Error::with_message(
            ErrorCode::Error,
            &format!("wrong number of arguments to function {}()", name),
        ));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let t = get_json_text(&args[0])?;
    let mut root =
        parse_json(&t).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    for pair in args[1..].chunks(2) {
        if matches!(pair[0], Value::Null) {
            // NULL path - skip this pair, return JSON unchanged
            continue;
        }
        let p = match &pair[0] {
            Value::Text(s) => s.clone(),
            other => other.to_text(),
        };
        let v = sql_to_json(&pair[1]);
        let exists = match extract_path(&root, &p) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(msg) => return Err(Error::with_message(ErrorCode::Error, msg)),
        };
        if exists && overwrite {
            set_at_path(&mut root, &p, v, false);
        } else if !exists && create {
            set_at_path(&mut root, &p, v, true);
        }
    }
    Ok(Value::Text(json_to_string(&root)))
}

pub fn func_json_remove(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::with_message(
            ErrorCode::Error,
            "wrong number of arguments to function json_remove()",
        ));
    }
    if matches!(args[0], Value::Null) {
        return Ok(Value::Null);
    }
    let t = get_json_text(&args[0])?;
    let mut root =
        parse_json(&t).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    if args.len() == 1 {
        // No paths - just validate and return the original text unchanged
        return Ok(Value::Text(t));
    }
    for a in &args[1..] {
        if matches!(a, Value::Null) {
            return Ok(Value::Null);
        }
        let p = match a {
            Value::Text(s) => s.clone(),
            other => other.to_text(),
        };
        remove_at_path(&mut root, &p);
    }
    Ok(Value::Text(json_to_string(&root)))
}

pub fn func_json_patch(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "wrong number of arguments to function json_patch()",
        ));
    }
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
        return Ok(Value::Null);
    }
    let t1 = get_json_text(&args[0])?;
    let t2 = get_json_text(&args[1])?;
    let tgt =
        parse_json(&t1).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    let ptch =
        parse_json(&t2).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    Ok(Value::Text(json_to_string(&merge_patch(&tgt, &ptch))))
}

pub fn func_json_quote(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "wrong number of arguments to function json_quote()",
        ));
    }
    match &args[0] {
        Value::Null => Ok(Value::Text("null".into())),
        Value::Integer(n) => Ok(Value::Text(n.to_string())),
        Value::Real(f) => Ok(Value::Text(sqlite_float_fmt(*f))),
        Value::Text(s) => {
            if let Ok(jv) = parse_json(s) {
                match &jv {
                    JsonValue::Array(_) | JsonValue::Object(_) => {
                        return Ok(Value::Text(json_to_string(&jv)))
                    }
                    _ => {}
                }
            }
            let mut b = String::new();
            write_json_string(s, &mut b);
            Ok(Value::Text(b))
        }
        Value::Blob(_) => Err(Error::with_message(
            ErrorCode::Error,
            "JSON cannot hold BLOB values",
        )),
    }
}

pub fn func_json_group_array(args: &[Value]) -> Result<Value> {
    Ok(Value::Text(json_to_string(&JsonValue::Array(
        args.iter().map(sql_to_json).collect(),
    ))))
}

pub fn func_json_group_object(args: &[Value]) -> Result<Value> {
    if args.len() % 2 != 0 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "json_group_object() requires an even number of arguments",
        ));
    }
    let mut obj = Vec::new();
    for pair in args.chunks(2) {
        let key = match &pair[0] {
            Value::Text(s) => s.clone(),
            other => other.to_text(),
        };
        obj.push((key, sql_to_json(&pair[1])));
    }
    Ok(Value::Text(json_to_string(&JsonValue::Object(obj))))
}

// Public accessors for use by aggregate functions
/// Convert a SQL value to a JSON value (for aggregate functions)
pub fn sql_to_json_for_agg(val: &Value) -> JsonValue {
    sql_to_json(val)
}

/// Convert a JsonValue to its JSON string representation (for aggregate functions)
pub fn json_to_string_pub(val: &JsonValue) -> String {
    json_to_string(val)
}

// jsonb_* aliases - In SQLite, these return binary JSON (JSONB format).
// For RustQL, they behave identically to their json_* counterparts
// since we don't have a separate binary JSON format.
pub fn func_jsonb_array(args: &[Value]) -> Result<Value> {
    func_json_array(args)
}
pub fn func_jsonb_object(args: &[Value]) -> Result<Value> {
    func_json_object(args)
}
pub fn func_jsonb_replace(args: &[Value]) -> Result<Value> {
    func_json_replace(args)
}
pub fn func_jsonb_set(args: &[Value]) -> Result<Value> {
    func_json_set(args)
}
pub fn func_jsonb_insert(args: &[Value]) -> Result<Value> {
    func_json_insert(args)
}
pub fn func_jsonb_remove(args: &[Value]) -> Result<Value> {
    func_json_remove(args)
}
pub fn func_jsonb_patch(args: &[Value]) -> Result<Value> {
    func_json_patch(args)
}
pub fn func_jsonb_extract(args: &[Value]) -> Result<Value> {
    func_json_extract(args)
}

// ============================================================================
// json_each / json_tree table-valued functions
// ============================================================================

/// A single row from json_each or json_tree.
/// Columns: key, value, type, atom, id, parent, fullkey, path
#[derive(Debug, Clone)]
pub struct JsonEachRow {
    pub key: Value,
    pub value: Value,
    pub type_name: Value,
    pub atom: Value,
    pub id: Value,
    pub parent: Value,
    pub fullkey: Value,
    pub path: Value,
}

/// Get the JSON type name for a JsonValue.
fn json_type_name(val: &JsonValue) -> &'static str {
    match val {
        JsonValue::Null => "null",
        JsonValue::Bool(true) => "true",
        JsonValue::Bool(false) => "false",
        JsonValue::Integer(_) => "integer",
        JsonValue::Real(_) => "real",
        JsonValue::String(_) => "text",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

/// Convert a JsonValue to its "atom" representation (scalars only, null for compounds).
fn json_atom(val: &JsonValue) -> Value {
    match val {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
        JsonValue::Integer(n) => Value::Integer(*n),
        JsonValue::Real(f) => Value::Real(*f),
        JsonValue::String(s) => Value::Text(s.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Null,
    }
}

/// Convert a JsonValue to its "value" representation (text for compounds, native for scalars).
fn json_value_to_sql(val: &JsonValue) -> Value {
    match val {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
        JsonValue::Integer(n) => Value::Integer(*n),
        JsonValue::Real(f) => Value::Real(*f),
        JsonValue::String(s) => Value::Text(s.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Text(json_to_string(val)),
    }
}

/// Compute the JSONB (binary JSON) size of a value, for computing node IDs.
/// The id in json_each/json_tree corresponds to byte offsets in JSONB encoding.
fn jsonb_size(val: &JsonValue) -> usize {
    match val {
        JsonValue::Null => 1,    // 1 byte header, no payload
        JsonValue::Bool(_) => 1, // 1 byte header, no payload
        JsonValue::Integer(n) => {
            // Header byte + varint payload
            let abs = if *n < 0 { (!n) as u64 + 1 } else { *n as u64 };
            let payload_size = if abs == 0 {
                0 // Zero is encoded in the type byte
            } else if abs <= 0xFF {
                1
            } else if abs <= 0xFFFF {
                2
            } else if abs <= 0xFFFFFFFF {
                4
            } else {
                8
            };
            jsonb_header_size(payload_size) + payload_size
        }
        JsonValue::Real(_) => {
            // Header + 8-byte IEEE 754
            jsonb_header_size(8) + 8
        }
        JsonValue::String(s) => {
            let payload_size = s.len();
            jsonb_header_size(payload_size) + payload_size
        }
        JsonValue::Array(items) => {
            let mut payload = 0;
            for item in items {
                payload += jsonb_size(item);
            }
            jsonb_header_size(payload) + payload
        }
        JsonValue::Object(pairs) => {
            let mut payload = 0;
            for (key, val) in pairs {
                // Key is stored as a JSONB text node
                let key_payload = key.len();
                payload += jsonb_header_size(key_payload) + key_payload;
                // Value is a JSONB node
                payload += jsonb_size(val);
            }
            jsonb_header_size(payload) + payload
        }
    }
}

/// Compute JSONB header size based on payload size.
/// JSONB format: 4-bit type + 4-bit size, or multi-byte headers for larger payloads.
fn jsonb_header_size(payload_size: usize) -> usize {
    if payload_size <= 11 {
        1 // type+size fits in a single byte
    } else if payload_size <= 0xFF {
        2 // 1 byte type + 1 byte size
    } else if payload_size <= 0xFFFF {
        3 // 1 byte type + 2 byte size
    } else if payload_size <= 0xFFFFFFFF {
        5 // 1 byte type + 4 byte size
    } else {
        9 // 1 byte type + 8 byte size
    }
}

/// Walk a JSON value and compute JSONB byte offsets for node IDs.
struct JsonWalker {
    canonical: String,
    rows: Vec<JsonEachRow>,
}

impl JsonWalker {
    fn new() -> Self {
        Self {
            canonical: String::new(),
            rows: Vec::new(),
        }
    }

    /// Generate rows for json_each (immediate children only).
    fn walk_each(&mut self, json_text: &str, root_path: &str) -> Result<Vec<JsonEachRow>> {
        let root = parse_json(json_text)
            .map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
        self.canonical = json_to_string(&root);
        self.rows.clear();

        match &root {
            JsonValue::Object(pairs) => {
                // Compute JSONB offsets: root object header + key/value pairs
                let mut payload_size = 0;
                for (key, val) in pairs {
                    payload_size += jsonb_header_size(key.len()) + key.len();
                    payload_size += jsonb_size(val);
                }
                let header = jsonb_header_size(payload_size);
                let mut offset = header; // skip root object header
                for (key, val) in pairs {
                    let key_offset = offset;
                    let key_node_size = jsonb_header_size(key.len()) + key.len();
                    offset += key_node_size;
                    // Value comes after key
                    let _val_offset = offset;
                    let val_node_size = jsonb_size(val);
                    offset += val_node_size;

                    let fullkey = format!("{}.{}", root_path, escape_path_key(key));
                    self.rows.push(JsonEachRow {
                        key: Value::Text(key.clone()),
                        value: json_value_to_sql(val),
                        type_name: Value::Text(json_type_name(val).to_string()),
                        atom: json_atom(val),
                        id: Value::Integer(key_offset as i64),
                        parent: Value::Null,
                        fullkey: Value::Text(fullkey),
                        path: Value::Text(root_path.to_string()),
                    });
                }
            }
            JsonValue::Array(items) => {
                // Compute JSONB offsets: root array header + elements
                let mut payload_size = 0;
                for item in items {
                    payload_size += jsonb_size(item);
                }
                let header = jsonb_header_size(payload_size);
                let mut offset = header;
                for (idx, val) in items.iter().enumerate() {
                    let val_offset = offset;
                    let val_node_size = jsonb_size(val);
                    offset += val_node_size;

                    let fullkey = format!("{}[{}]", root_path, idx);
                    self.rows.push(JsonEachRow {
                        key: Value::Integer(idx as i64),
                        value: json_value_to_sql(val),
                        type_name: Value::Text(json_type_name(val).to_string()),
                        atom: json_atom(val),
                        id: Value::Integer(val_offset as i64),
                        parent: Value::Null,
                        fullkey: Value::Text(fullkey),
                        path: Value::Text(root_path.to_string()),
                    });
                }
            }
            _ => {
                // Scalar value - return single row for the value itself
                self.rows.push(JsonEachRow {
                    key: Value::Null,
                    value: json_value_to_sql(&root),
                    type_name: Value::Text(json_type_name(&root).to_string()),
                    atom: json_atom(&root),
                    id: Value::Integer(0),
                    parent: Value::Null,
                    fullkey: Value::Text(root_path.to_string()),
                    path: Value::Text(root_path.to_string()),
                });
            }
        }
        Ok(std::mem::take(&mut self.rows))
    }

    /// Generate rows for json_tree (recursive walk).
    fn walk_tree(&mut self, json_text: &str, root_path: &str) -> Result<Vec<JsonEachRow>> {
        let root = parse_json(json_text)
            .map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
        self.canonical = json_to_string(&root);
        self.rows.clear();
        self.walk_tree_recursive(&root, root_path, Value::Null, 0);
        Ok(std::mem::take(&mut self.rows))
    }

    /// Recursively walk the JSON tree, computing JSONB byte offsets for node IDs.
    /// Returns the end offset (after this node's JSONB representation).
    fn walk_tree_recursive(
        &mut self,
        val: &JsonValue,
        path: &str,
        parent_id: Value,
        offset: usize,
    ) -> usize {
        let my_id = offset;
        let type_name = json_type_name(val).to_string();
        let atom = json_atom(val);
        let value = json_value_to_sql(val);

        // Extract key from path for non-root nodes
        let key = if path == "$" {
            Value::Null
        } else if let Some(bracket_pos) = path.rfind('[') {
            let after = &path[bracket_pos + 1..];
            if after.ends_with(']') {
                let idx_str = &after[..after.len() - 1];
                if let Ok(idx) = idx_str.parse::<i64>() {
                    Value::Integer(idx)
                } else {
                    Value::Null
                }
            } else {
                extract_key_from_path(path)
            }
        } else {
            extract_key_from_path(path)
        };

        // Determine the parent path - take the later of rfind('.') and rfind('[')
        let parent_path = if path == "$" {
            "$".to_string()
        } else {
            let dot_pos = path.rfind('.');
            let bracket_pos = path.rfind('[');
            match (dot_pos, bracket_pos) {
                (Some(d), Some(b)) => {
                    let pos = d.max(b);
                    path[..pos].to_string()
                }
                (Some(d), None) => path[..d].to_string(),
                (None, Some(b)) => path[..b].to_string(),
                (None, None) => "$".to_string(),
            }
        };

        self.rows.push(JsonEachRow {
            key,
            value,
            type_name: Value::Text(type_name),
            atom,
            id: Value::Integer(my_id as i64),
            parent: parent_id,
            fullkey: Value::Text(path.to_string()),
            path: Value::Text(parent_path),
        });

        match val {
            JsonValue::Object(pairs) => {
                // Compute JSONB layout
                let mut payload_size = 0;
                for (key, child) in pairs {
                    payload_size += jsonb_header_size(key.len()) + key.len();
                    payload_size += jsonb_size(child);
                }
                let header = jsonb_header_size(payload_size);
                let mut pos = offset + header;
                for (key, child) in pairs {
                    // Key node starts here - this is the id for the child row
                    let key_offset = pos;
                    let key_node_size = jsonb_header_size(key.len()) + key.len();
                    pos += key_node_size;
                    // Value node (recurse, but use key_offset as the id)
                    let child_path = format!("{}.{}", path, escape_path_key(key));
                    // We need to walk the value's subtree but report key_offset as this row's id
                    pos = self.walk_tree_recursive_with_id(
                        child,
                        &child_path,
                        Value::Integer(my_id as i64),
                        key_offset,
                        pos,
                    );
                }
                pos
            }
            JsonValue::Array(items) => {
                // Compute JSONB layout
                let mut payload_size = 0;
                for item in items {
                    payload_size += jsonb_size(item);
                }
                let header = jsonb_header_size(payload_size);
                let mut pos = offset + header;
                for (idx, child) in items.iter().enumerate() {
                    let child_path = format!("{}[{}]", path, idx);
                    pos = self.walk_tree_recursive(
                        child,
                        &child_path,
                        Value::Integer(my_id as i64),
                        pos,
                    );
                }
                pos
            }
            _ => offset + jsonb_size(val),
        }
    }

    /// Like walk_tree_recursive but uses `report_id` for the current row's id
    /// instead of the offset. Used for object children where the id should be
    /// the key's JSONB offset, not the value's offset.
    fn walk_tree_recursive_with_id(
        &mut self,
        val: &JsonValue,
        path: &str,
        parent_id: Value,
        report_id: usize,
        value_offset: usize,
    ) -> usize {
        let type_name = json_type_name(val).to_string();
        let atom = json_atom(val);
        let value = json_value_to_sql(val);

        let key = if path == "$" {
            Value::Null
        } else if let Some(bracket_pos) = path.rfind('[') {
            let after = &path[bracket_pos + 1..];
            if after.ends_with(']') {
                let idx_str = &after[..after.len() - 1];
                if let Ok(idx) = idx_str.parse::<i64>() {
                    Value::Integer(idx)
                } else {
                    Value::Null
                }
            } else {
                extract_key_from_path(path)
            }
        } else {
            extract_key_from_path(path)
        };

        let parent_path = if path == "$" {
            "$".to_string()
        } else {
            let dot_pos = path.rfind('.');
            let bracket_pos = path.rfind('[');
            match (dot_pos, bracket_pos) {
                (Some(d), Some(b)) => {
                    let pos = d.max(b);
                    path[..pos].to_string()
                }
                (Some(d), None) => path[..d].to_string(),
                (None, Some(b)) => path[..b].to_string(),
                (None, None) => "$".to_string(),
            }
        };

        self.rows.push(JsonEachRow {
            key,
            value,
            type_name: Value::Text(type_name),
            atom,
            id: Value::Integer(report_id as i64),
            parent: parent_id,
            fullkey: Value::Text(path.to_string()),
            path: Value::Text(parent_path),
        });

        // Now recurse into children using the value_offset for JSONB layout
        match val {
            JsonValue::Object(pairs) => {
                let mut payload_size = 0;
                for (key, child) in pairs {
                    payload_size += jsonb_header_size(key.len()) + key.len();
                    payload_size += jsonb_size(child);
                }
                let header = jsonb_header_size(payload_size);
                let mut pos = value_offset + header;
                for (key, child) in pairs {
                    let key_offset = pos;
                    let key_node_size = jsonb_header_size(key.len()) + key.len();
                    pos += key_node_size;
                    let child_path = format!("{}.{}", path, escape_path_key(key));
                    pos = self.walk_tree_recursive_with_id(
                        child,
                        &child_path,
                        Value::Integer(report_id as i64),
                        key_offset,
                        pos,
                    );
                }
                pos
            }
            JsonValue::Array(items) => {
                let mut payload_size = 0;
                for item in items {
                    payload_size += jsonb_size(item);
                }
                let header = jsonb_header_size(payload_size);
                let mut pos = value_offset + header;
                for (idx, child) in items.iter().enumerate() {
                    let child_path = format!("{}[{}]", path, idx);
                    pos = self.walk_tree_recursive(
                        child,
                        &child_path,
                        Value::Integer(report_id as i64),
                        pos,
                    );
                }
                pos
            }
            _ => value_offset + jsonb_size(val),
        }
    }
}

/// Extract the key portion from a JSON path like "$.a" or "$.a.b"
fn extract_key_from_path(path: &str) -> Value {
    if let Some(dot_pos) = path.rfind('.') {
        let key_part = &path[dot_pos + 1..];
        Value::Text(key_part.to_string())
    } else {
        Value::Null
    }
}

/// Escape a key for use in a JSON path. If the key needs quoting, quote it.
fn escape_path_key(key: &str) -> String {
    // Simple alphanumeric keys don't need quoting
    if key.chars().all(|c| c.is_alphanumeric() || c == '_')
        && !key.is_empty()
        && !key.chars().next().unwrap().is_ascii_digit()
    {
        key.to_string()
    } else {
        format!("\"{}\"", key)
    }
}

/// Compute json_each rows for the given JSON text and optional root path.
pub fn json_each_rows(json_text: &str, root_path: Option<&str>) -> Result<Vec<JsonEachRow>> {
    let path = root_path.unwrap_or("$");
    if path != "$" {
        // If a path is given, extract the value at that path first
        let root = parse_json(json_text)
            .map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
        let extracted =
            extract_path(&root, path).map_err(|msg| Error::with_message(ErrorCode::Error, msg))?;
        match extracted {
            Some(val) => {
                let sub_json = json_to_string(&val);
                let mut walker = JsonWalker::new();
                walker.walk_each(&sub_json, path)
            }
            None => Ok(Vec::new()),
        }
    } else {
        let mut walker = JsonWalker::new();
        walker.walk_each(json_text, path)
    }
}

/// Compute json_tree rows for the given JSON text and optional root path.
pub fn json_tree_rows(json_text: &str, root_path: Option<&str>) -> Result<Vec<JsonEachRow>> {
    let path = root_path.unwrap_or("$");
    if path != "$" {
        let root = parse_json(json_text)
            .map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
        let extracted =
            extract_path(&root, path).map_err(|msg| Error::with_message(ErrorCode::Error, msg))?;
        match extracted {
            Some(val) => {
                let sub_json = json_to_string(&val);
                let mut walker = JsonWalker::new();
                walker.walk_tree(&sub_json, path)
            }
            None => Ok(Vec::new()),
        }
    } else {
        let mut walker = JsonWalker::new();
        walker.walk_tree(json_text, path)
    }
}

/// Column names for json_each and json_tree table-valued functions.
pub const JSON_EACH_COLUMNS: &[&str] = &[
    "key", "value", "type", "atom", "id", "parent", "fullkey", "path",
];

#[cfg(test)]
mod tests {
    use super::*;
    fn text(s: &str) -> Value {
        Value::Text(s.to_string())
    }
    fn int(n: i64) -> Value {
        Value::Integer(n)
    }
    #[test]
    fn test_parse() {
        assert!(parse_json("null").is_ok());
        assert!(parse_json("{}").is_ok());
        assert!(parse_json("[1,2]").is_ok());
    }
    #[test]
    fn test_valid() {
        assert_eq!(func_json_valid(&[text(r#"{"a":1}"#)]).unwrap(), int(1));
        assert_eq!(func_json_valid(&[text("bad")]).unwrap(), int(0));
    }
    #[test]
    fn test_minify() {
        assert_eq!(
            func_json(&[text(r#" { "a" : 1 } "#)]).unwrap(),
            text(r#"{"a":1}"#)
        );
    }
    #[test]
    fn test_extract() {
        assert_eq!(
            func_json_extract(&[text(r#"{"a":1}"#), text("$.a")]).unwrap(),
            int(1)
        );
    }
    #[test]
    fn test_type() {
        assert_eq!(func_json_type(&[text("42")]).unwrap(), text("integer"));
    }
    #[test]
    fn test_array() {
        assert_eq!(func_json_array(&[int(1), int(2)]).unwrap(), text("[1,2]"));
    }
    #[test]
    fn test_object() {
        assert_eq!(
            func_json_object(&[text("a"), int(1)]).unwrap(),
            text(r#"{"a":1}"#)
        );
    }
    #[test]
    fn test_arr_len() {
        assert_eq!(func_json_array_length(&[text("[1,2,3]")]).unwrap(), int(3));
    }
    #[test]
    fn test_insert() {
        assert_eq!(
            func_json_insert(&[text(r#"{"a":1}"#), text("$.b"), int(2)]).unwrap(),
            text(r#"{"a":1,"b":2}"#)
        );
    }
    #[test]
    fn test_set() {
        assert_eq!(
            func_json_set(&[text(r#"{"a":1}"#), text("$.a"), int(9)]).unwrap(),
            text(r#"{"a":9}"#)
        );
    }
    #[test]
    fn test_replace() {
        assert_eq!(
            func_json_replace(&[text(r#"{"a":1}"#), text("$.b"), int(2)]).unwrap(),
            text(r#"{"a":1}"#)
        );
    }
    #[test]
    fn test_remove() {
        assert_eq!(
            func_json_remove(&[text(r#"{"a":1,"b":2}"#), text("$.b")]).unwrap(),
            text(r#"{"a":1}"#)
        );
    }
    #[test]
    fn test_patch() {
        assert_eq!(
            func_json_patch(&[text(r#"{"a":1}"#), text(r#"{"b":2}"#)]).unwrap(),
            text(r#"{"a":1,"b":2}"#)
        );
    }
    #[test]
    fn test_quote() {
        assert_eq!(func_json_quote(&[text("hi")]).unwrap(), text(r#""hi""#));
    }
    #[test]
    fn test_null() {
        assert_eq!(func_json(&[Value::Null]).unwrap(), Value::Null);
    }
    #[test]
    fn test_nested() {
        assert_eq!(
            func_json_extract(&[text(r#"{"a":{"b":42}}"#), text("$.a.b")]).unwrap(),
            int(42)
        );
    }
}
