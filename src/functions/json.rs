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

struct JsonParser<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input: input.as_bytes(), pos: 0 }
    }
    fn skip_ws(&mut self) {
        while self.pos < self.input.len() {
            match self.input[self.pos] { b' ' | b'\t' | b'\n' | b'\r' => self.pos += 1, _ => break }
        }
    }
    fn peek(&self) -> Option<u8> { self.input.get(self.pos).copied() }
    fn advance(&mut self) -> Option<u8> {
        if self.pos < self.input.len() { let b = self.input[self.pos]; self.pos += 1; Some(b) } else { None }
    }
    fn expect(&mut self, ch: u8) -> std::result::Result<(), ()> {
        if self.peek() == Some(ch) { self.pos += 1; Ok(()) } else { Err(()) }
    }
    fn parse(&mut self) -> std::result::Result<JsonValue, ()> {
        self.skip_ws(); let val = self.parse_value()?; self.skip_ws();
        if self.pos != self.input.len() { return Err(()); } Ok(val)
    }
    fn parse_value(&mut self) -> std::result::Result<JsonValue, ()> {
        self.skip_ws();
        match self.peek().ok_or(())? {
            b'"' => self.parse_string().map(JsonValue::String),
            b'{' => self.parse_object(), b'[' => self.parse_array(),
            b't' => self.parse_literal(b"true", JsonValue::Bool(true)),
            b'f' => self.parse_literal(b"false", JsonValue::Bool(false)),
            b'n' => self.parse_literal(b"null", JsonValue::Null),
            b'-' | b'0'..=b'9' => self.parse_number(),
            _ => Err(()),
        }
    }
    fn parse_literal(&mut self, expected: &[u8], val: JsonValue) -> std::result::Result<JsonValue, ()> {
        for &b in expected { if self.advance() != Some(b) { return Err(()); } } Ok(val)
    }
    fn parse_string(&mut self) -> std::result::Result<String, ()> {
        self.expect(b'"')?;
        let mut s = String::new();
        loop {
            match self.advance().ok_or(())? {
                b'"' => return Ok(s),
                b'\\' => match self.advance().ok_or(())? {
                    b'"' => s.push('"'), b'\\' => s.push('\\'), b'/' => s.push('/'),
                    b'b' => s.push('\u{0008}'), b'f' => s.push('\u{000C}'),
                    b'n' => s.push('\n'), b'r' => s.push('\r'), b't' => s.push('\t'),
                    b'u' => {
                        let cp = self.parse_hex4()?;
                        if (0xD800..=0xDBFF).contains(&cp) {
                            if self.advance() != Some(b'\\') || self.advance() != Some(b'u') { return Err(()); }
                            let cp2 = self.parse_hex4()?;
                            if !(0xDC00..=0xDFFF).contains(&cp2) { return Err(()); }
                            let combined = 0x10000 + ((cp as u32 - 0xD800) << 10) + (cp2 as u32 - 0xDC00);
                            s.push(char::from_u32(combined).ok_or(())?);
                        } else { s.push(char::from_u32(cp as u32).ok_or(())?); }
                    }
                    _ => return Err(()),
                },
                b if b < 0x20 => return Err(()),
                b => {
                    if b < 0x80 { s.push(b as char); } else {
                        self.pos -= 1; let start = self.pos;
                        let n = if b & 0xE0 == 0xC0 { 2 } else if b & 0xF0 == 0xE0 { 3 } else if b & 0xF8 == 0xF0 { 4 } else { return Err(()); };
                        if self.pos + n > self.input.len() { return Err(()); }
                        let utf8 = std::str::from_utf8(&self.input[start..start + n]).map_err(|_| ())?;
                        s.push_str(utf8); self.pos += n;
                    }
                }
            }
        }
    }
    fn parse_hex4(&mut self) -> std::result::Result<u16, ()> {
        let mut val: u16 = 0;
        for _ in 0..4 {
            let b = self.advance().ok_or(())?;
            let digit = match b { b'0'..=b'9' => b - b'0', b'a'..=b'f' => b - b'a' + 10, b'A'..=b'F' => b - b'A' + 10, _ => return Err(()) };
            val = val * 16 + digit as u16;
        }
        Ok(val)
    }
    fn parse_number(&mut self) -> std::result::Result<JsonValue, ()> {
        let start = self.pos;
        if self.peek() == Some(b'-') { self.pos += 1; }
        let digit_start = self.pos;
        match self.peek() {
            Some(b'0') => { self.pos += 1; }
            Some(b'1'..=b'9') => { while let Some(b'0'..=b'9') = self.peek() { self.pos += 1; } }
            _ => return Err(()),
        }
        if self.pos == digit_start { return Err(()); }
        let mut is_float = false;
        if self.peek() == Some(b'.') {
            is_float = true; self.pos += 1; let fs = self.pos;
            while let Some(b'0'..=b'9') = self.peek() { self.pos += 1; }
            if self.pos == fs { return Err(()); }
        }
        if matches!(self.peek(), Some(b'e') | Some(b'E')) {
            is_float = true; self.pos += 1;
            if matches!(self.peek(), Some(b'+') | Some(b'-')) { self.pos += 1; }
            let es = self.pos;
            while let Some(b'0'..=b'9') = self.peek() { self.pos += 1; }
            if self.pos == es { return Err(()); }
        }
        let s = std::str::from_utf8(&self.input[start..self.pos]).map_err(|_| ())?;
        if is_float { Ok(JsonValue::Real(s.parse().map_err(|_| ())?)) }
        else if let Ok(n) = s.parse::<i64>() { Ok(JsonValue::Integer(n)) }
        else { Ok(JsonValue::Real(s.parse().map_err(|_| ())?)) }
    }
    fn parse_array(&mut self) -> std::result::Result<JsonValue, ()> {
        self.expect(b'[')?; self.skip_ws();
        let mut arr = Vec::new();
        if self.peek() == Some(b']') { self.pos += 1; return Ok(JsonValue::Array(arr)); }
        loop {
            arr.push(self.parse_value()?); self.skip_ws();
            match self.peek() { Some(b',') => { self.pos += 1; } Some(b']') => { self.pos += 1; return Ok(JsonValue::Array(arr)); } _ => return Err(()) }
        }
    }
    fn parse_object(&mut self) -> std::result::Result<JsonValue, ()> {
        self.expect(b'{')?; self.skip_ws();
        let mut obj = Vec::new();
        if self.peek() == Some(b'}') { self.pos += 1; return Ok(JsonValue::Object(obj)); }
        loop {
            self.skip_ws(); let key = self.parse_string()?; self.skip_ws(); self.expect(b':')?;
            let val = self.parse_value()?; obj.push((key, val)); self.skip_ws();
            match self.peek() { Some(b',') => { self.pos += 1; } Some(b'}') => { self.pos += 1; return Ok(JsonValue::Object(obj)); } _ => return Err(()) }
        }
    }
}

fn parse_json(s: &str) -> std::result::Result<JsonValue, ()> { JsonParser::new(s).parse() }
fn json_to_string(val: &JsonValue) -> String { let mut b = String::new(); write_json(val, &mut b); b }

fn write_json(val: &JsonValue, buf: &mut String) {
    match val {
        JsonValue::Null => buf.push_str("null"),
        JsonValue::Bool(b) => buf.push_str(if *b { "true" } else { "false" }),
        JsonValue::Integer(n) => buf.push_str(&n.to_string()),
        JsonValue::Real(f) => buf.push_str(&sqlite_float_fmt(*f)),
        JsonValue::String(s) => write_json_string(s, buf),
        JsonValue::Array(arr) => { buf.push('['); for (i, v) in arr.iter().enumerate() { if i > 0 { buf.push(','); } write_json(v, buf); } buf.push(']'); }
        JsonValue::Object(obj) => { buf.push('{'); for (i, (k, v)) in obj.iter().enumerate() { if i > 0 { buf.push(','); } write_json_string(k, buf); buf.push(':'); write_json(v, buf); } buf.push('}'); }
    }
}

fn write_json_string(s: &str, buf: &mut String) {
    buf.push('"');
    for ch in s.chars() {
        match ch {
            '"' => buf.push_str("\\\""), '\\' => buf.push_str("\\\\"),
            '\u{0008}' => buf.push_str("\\b"), '\u{000C}' => buf.push_str("\\f"),
            '\n' => buf.push_str("\\n"), '\r' => buf.push_str("\\r"), '\t' => buf.push_str("\\t"),
            c if (c as u32) < 0x20 => { buf.push_str(&format!("\\u{:04x}", c as u32)); }
            c => buf.push(c),
        }
    }
    buf.push('"');
}

fn sqlite_float_fmt(f: f64) -> String {
    if f.is_infinite() { return if f > 0.0 { "9.0e+999".into() } else { "-9.0e+999".into() }; }
    if f.is_nan() { return "null".into(); }
    let s = format!("{}", f);
    if !s.contains('.') && !s.contains('e') && !s.contains('E') { format!("{}.0", s) } else { s }
}

fn extract_path<'a>(root: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    if path == "$" { return Some(root); }
    if !path.starts_with('$') { return None; }
    let mut current = root;
    let mut chars = path[1..].chars().peekable();
    while chars.peek().is_some() {
        match chars.peek() {
            Some('.') => {
                chars.next(); let mut key = String::new();
                while let Some(&c) = chars.peek() { if c == '.' || c == '[' { break; } key.push(c); chars.next(); }
                if key.is_empty() { return None; }
                match current { JsonValue::Object(obj) => { current = obj.iter().find(|(k, _)| k == &key).map(|(_, v)| v)?; } _ => return None }
            }
            Some('[') => {
                chars.next(); let mut idx_s = String::new();
                while let Some(&c) = chars.peek() { if c == ']' { break; } idx_s.push(c); chars.next(); }
                if chars.next() != Some(']') { return None; }
                if let Ok(idx) = idx_s.parse::<usize>() {
                    match current { JsonValue::Array(arr) => { current = arr.get(idx)?; } _ => return None }
                } else if idx_s.starts_with("#-") {
                    if let JsonValue::Array(arr) = current {
                        if let Ok(n) = idx_s[2..].parse::<usize>() { if n <= arr.len() { current = arr.get(arr.len() - n)?; } else { return None; } } else { return None; }
                    } else { return None; }
                } else { return None; }
            }
            _ => return None,
        }
    }
    Some(current)
}

fn json_to_sql_value(jv: &JsonValue) -> Value {
    match jv {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
        JsonValue::Integer(n) => Value::Integer(*n),
        JsonValue::Real(f) => Value::Real(*f),
        JsonValue::String(s) => Value::Text(s.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Text(json_to_string(jv)),
    }
}

fn sql_to_json(val: &Value) -> JsonValue {
    match val {
        Value::Null => JsonValue::Null, Value::Integer(n) => JsonValue::Integer(*n), Value::Real(f) => JsonValue::Real(*f),
        Value::Text(s) => { if let Ok(jv) = parse_json(s) { match &jv { JsonValue::Array(_) | JsonValue::Object(_) => return jv, _ => {} } } JsonValue::String(s.clone()) }
        Value::Blob(b) => JsonValue::String(String::from_utf8_lossy(b).to_string()),
    }
}

fn set_at_path(root: &mut JsonValue, path: &str, value: JsonValue, create: bool) -> bool {
    if path == "$" { *root = value; return true; }
    if !path.starts_with('$') { return false; }
    set_impl(root, &path[1..], value, create)
}

fn set_impl(cur: &mut JsonValue, path: &str, value: JsonValue, create: bool) -> bool {
    if path.is_empty() { *cur = value; return true; }
    let mut chars = path.chars().peekable();
    match chars.peek() {
        Some('.') => {
            chars.next(); let mut key = String::new(); let mut rest = String::new(); let mut fs = false;
            while let Some(&c) = chars.peek() { if c == '.' || c == '[' { fs = true; break; } key.push(c); chars.next(); }
            if fs { rest = chars.collect(); }
            if key.is_empty() { return false; }
            match cur {
                JsonValue::Object(obj) => {
                    if let Some(pos) = obj.iter().position(|(k, _)| k == &key) {
                        if rest.is_empty() { obj[pos].1 = value; return true; }
                        return set_impl(&mut obj[pos].1, &rest, value, create);
                    }
                    if create {
                        if rest.is_empty() { obj.push((key, value)); } else {
                            let mut nv = JsonValue::Object(Vec::new());
                            if set_impl(&mut nv, &rest, value, true) { obj.push((key, nv)); } else { return false; }
                        }
                        return true;
                    }
                    false
                }
                _ => false,
            }
        }
        Some('[') => {
            chars.next(); let mut is = String::new();
            while let Some(&c) = chars.peek() { if c == ']' { break; } is.push(c); chars.next(); }
            if chars.next() != Some(']') { return false; }
            let rest: String = chars.collect();
            if let Ok(idx) = is.parse::<usize>() {
                match cur {
                    JsonValue::Array(arr) => {
                        if idx < arr.len() { if rest.is_empty() { arr[idx] = value; return true; } return set_impl(&mut arr[idx], &rest, value, create); }
                        if create && rest.is_empty() { while arr.len() < idx { arr.push(JsonValue::Null); } arr.push(value); return true; }
                        false
                    }
                    _ => false,
                }
            } else { false }
        }
        _ => false,
    }
}

fn remove_at_path(root: &mut JsonValue, path: &str) -> bool {
    if path == "$" || !path.starts_with('$') { return false; }
    rm_impl(root, &path[1..])
}

fn rm_impl(cur: &mut JsonValue, path: &str) -> bool {
    if path.is_empty() { return false; }
    let mut chars = path.chars().peekable();
    match chars.peek() {
        Some('.') => {
            chars.next(); let mut key = String::new();
            while let Some(&c) = chars.peek() { if c == '.' || c == '[' { break; } key.push(c); chars.next(); }
            let rest: String = chars.collect();
            match cur {
                JsonValue::Object(obj) => {
                    if rest.is_empty() { if let Some(p) = obj.iter().position(|(k, _)| k == &key) { obj.remove(p); return true; } return false; }
                    if let Some(p) = obj.iter().position(|(k, _)| k == &key) { return rm_impl(&mut obj[p].1, &rest); }
                    false
                }
                _ => false,
            }
        }
        Some('[') => {
            chars.next(); let mut is = String::new();
            while let Some(&c) = chars.peek() { if c == ']' { break; } is.push(c); chars.next(); }
            if chars.next() != Some(']') { return false; }
            let rest: String = chars.collect();
            if let Ok(idx) = is.parse::<usize>() {
                match cur { JsonValue::Array(arr) => { if idx >= arr.len() { return false; } if rest.is_empty() { arr.remove(idx); return true; } rm_impl(&mut arr[idx], &rest) } _ => false }
            } else { false }
        }
        _ => false,
    }
}

fn merge_patch(target: &JsonValue, patch: &JsonValue) -> JsonValue {
    match patch {
        JsonValue::Object(po) => {
            let mut r = match target { JsonValue::Object(o) => o.clone(), _ => Vec::new() };
            for (key, pv) in po {
                if matches!(pv, JsonValue::Null) { r.retain(|(k, _)| k != key); } else {
                    let ex = r.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone()).unwrap_or(JsonValue::Object(Vec::new()));
                    let m = merge_patch(&ex, pv);
                    if let Some(p) = r.iter().position(|(k, _)| k == key) { r[p].1 = m; } else { r.push((key.clone(), m)); }
                }
            }
            JsonValue::Object(r)
        }
        other => other.clone(),
    }
}

fn get_json_text(val: &Value) -> std::result::Result<String, Error> {
    match val {
        Value::Null => Ok("null".into()), Value::Integer(n) => Ok(n.to_string()),
        Value::Real(f) => Ok(sqlite_float_fmt(*f)), Value::Text(s) => Ok(s.clone()),
        Value::Blob(_) => Err(Error::with_message(ErrorCode::Error, "JSON cannot hold BLOB values")),
    }
}

pub fn func_json(args: &[Value]) -> Result<Value> {
    if args.len() != 1 { return Err(Error::with_message(ErrorCode::Error, "wrong number of arguments to function json()")); }
    if matches!(args[0], Value::Null) { return Ok(Value::Null); }
    let t = get_json_text(&args[0])?;
    let jv = parse_json(&t).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    Ok(Value::Text(json_to_string(&jv)))
}

pub fn func_jsonb(args: &[Value]) -> Result<Value> { func_json(args) }

pub fn func_json_valid(args: &[Value]) -> Result<Value> {
    if args.len() != 1 { return Err(Error::with_message(ErrorCode::Error, "wrong number of arguments to function json_valid()")); }
    if matches!(args[0], Value::Null) { return Ok(Value::Null); }
    let t = match &args[0] { Value::Text(s) => s.clone(), Value::Blob(_) => return Ok(Value::Integer(0)), Value::Integer(n) => n.to_string(), Value::Real(f) => sqlite_float_fmt(*f), Value::Null => unreachable!() };
    Ok(Value::Integer(if parse_json(&t).is_ok() { 1 } else { 0 }))
}

pub fn func_json_extract(args: &[Value]) -> Result<Value> {
    if args.len() < 2 { return Err(Error::with_message(ErrorCode::Error, "wrong number of arguments to function json_extract()")); }
    if matches!(args[0], Value::Null) { return Ok(Value::Null); }
    let t = get_json_text(&args[0])?;
    let root = parse_json(&t).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    if args.len() == 2 {
        if matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let p = match &args[1] { Value::Text(s) => s.clone(), other => other.to_text() };
        match extract_path(&root, &p) { Some(jv) => Ok(json_to_sql_value(jv)), None => Ok(Value::Null) }
    } else {
        let mut rs = Vec::new();
        for a in &args[1..] {
            if matches!(a, Value::Null) { rs.push(JsonValue::Null); continue; }
            let p = match a { Value::Text(s) => s.clone(), other => other.to_text() };
            match extract_path(&root, &p) { Some(jv) => rs.push(jv.clone()), None => rs.push(JsonValue::Null) }
        }
        Ok(Value::Text(json_to_string(&JsonValue::Array(rs))))
    }
}

pub fn func_json_type(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 { return Err(Error::with_message(ErrorCode::Error, "wrong number of arguments to function json_type()")); }
    if matches!(args[0], Value::Null) { return Ok(Value::Null); }
    let t = get_json_text(&args[0])?;
    let root = parse_json(&t).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    let tgt = if args.len() == 2 {
        if matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let p = match &args[1] { Value::Text(s) => s.clone(), other => other.to_text() };
        match extract_path(&root, &p) { Some(jv) => jv, None => return Ok(Value::Null) }
    } else { &root };
    let tn = match tgt { JsonValue::Null => "null", JsonValue::Bool(true) => "true", JsonValue::Bool(false) => "false", JsonValue::Integer(_) => "integer", JsonValue::Real(_) => "real", JsonValue::String(_) => "text", JsonValue::Array(_) => "array", JsonValue::Object(_) => "object" };
    Ok(Value::Text(tn.to_string()))
}

pub fn func_json_array(args: &[Value]) -> Result<Value> {
    Ok(Value::Text(json_to_string(&JsonValue::Array(args.iter().map(sql_to_json).collect()))))
}

pub fn func_json_object(args: &[Value]) -> Result<Value> {
    if args.len() % 2 != 0 { return Err(Error::with_message(ErrorCode::Error, "json_object() requires an even number of arguments")); }
    let mut obj = Vec::new();
    for pair in args.chunks(2) {
        let key = match &pair[0] { Value::Null => return Err(Error::with_message(ErrorCode::Error, "json_object() labels must be TEXT")), Value::Text(s) => s.clone(), other => other.to_text() };
        obj.push((key, sql_to_json(&pair[1])));
    }
    Ok(Value::Text(json_to_string(&JsonValue::Object(obj))))
}

pub fn func_json_array_length(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 { return Err(Error::with_message(ErrorCode::Error, "wrong number of arguments to function json_array_length()")); }
    if matches!(args[0], Value::Null) { return Ok(Value::Null); }
    let t = get_json_text(&args[0])?;
    let root = parse_json(&t).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    let tgt = if args.len() == 2 {
        if matches!(args[1], Value::Null) { return Ok(Value::Null); }
        let p = match &args[1] { Value::Text(s) => s.clone(), other => other.to_text() };
        match extract_path(&root, &p) { Some(jv) => jv.clone(), None => return Ok(Value::Null) }
    } else { root };
    match &tgt { JsonValue::Array(arr) => Ok(Value::Integer(arr.len() as i64)), _ => Ok(Value::Integer(0)) }
}

pub fn func_json_insert(args: &[Value]) -> Result<Value> { json_modify(args, false, true, "json_insert") }
pub fn func_json_set(args: &[Value]) -> Result<Value> { json_modify(args, true, true, "json_set") }
pub fn func_json_replace(args: &[Value]) -> Result<Value> { json_modify(args, true, false, "json_replace") }

fn json_modify(args: &[Value], overwrite: bool, create: bool, name: &str) -> Result<Value> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 { return Err(Error::with_message(ErrorCode::Error, &format!("wrong number of arguments to function {}()", name))); }
    if matches!(args[0], Value::Null) { return Ok(Value::Null); }
    let t = get_json_text(&args[0])?;
    let mut root = parse_json(&t).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    for pair in args[1..].chunks(2) {
        let p = match &pair[0] { Value::Text(s) => s.clone(), other => other.to_text() };
        let v = sql_to_json(&pair[1]);
        let exists = extract_path(&root, &p).is_some();
        if exists && overwrite { set_at_path(&mut root, &p, v, false); }
        else if !exists && create { set_at_path(&mut root, &p, v, true); }
    }
    Ok(Value::Text(json_to_string(&root)))
}

pub fn func_json_remove(args: &[Value]) -> Result<Value> {
    if args.is_empty() { return Err(Error::with_message(ErrorCode::Error, "wrong number of arguments to function json_remove()")); }
    if matches!(args[0], Value::Null) { return Ok(Value::Null); }
    let t = get_json_text(&args[0])?;
    let mut root = parse_json(&t).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    if args.len() == 1 { return Ok(Value::Text(json_to_string(&root))); }
    for a in &args[1..] { let p = match a { Value::Text(s) => s.clone(), other => other.to_text() }; remove_at_path(&mut root, &p); }
    Ok(Value::Text(json_to_string(&root)))
}

pub fn func_json_patch(args: &[Value]) -> Result<Value> {
    if args.len() != 2 { return Err(Error::with_message(ErrorCode::Error, "wrong number of arguments to function json_patch()")); }
    if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) { return Ok(Value::Null); }
    let t1 = get_json_text(&args[0])?; let t2 = get_json_text(&args[1])?;
    let tgt = parse_json(&t1).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    let ptch = parse_json(&t2).map_err(|_| Error::with_message(ErrorCode::Error, "malformed JSON"))?;
    Ok(Value::Text(json_to_string(&merge_patch(&tgt, &ptch))))
}

pub fn func_json_quote(args: &[Value]) -> Result<Value> {
    if args.len() != 1 { return Err(Error::with_message(ErrorCode::Error, "wrong number of arguments to function json_quote()")); }
    match &args[0] {
        Value::Null => Ok(Value::Text("null".into())), Value::Integer(n) => Ok(Value::Text(n.to_string())),
        Value::Real(f) => Ok(Value::Text(sqlite_float_fmt(*f))),
        Value::Text(s) => {
            if let Ok(jv) = parse_json(s) { match &jv { JsonValue::Array(_) | JsonValue::Object(_) => return Ok(Value::Text(json_to_string(&jv))), _ => {} } }
            let mut b = String::new(); write_json_string(s, &mut b); Ok(Value::Text(b))
        }
        Value::Blob(_) => Err(Error::with_message(ErrorCode::Error, "JSON cannot hold BLOB values")),
    }
}

pub fn func_json_group_array(args: &[Value]) -> Result<Value> {
    Ok(Value::Text(json_to_string(&JsonValue::Array(args.iter().map(sql_to_json).collect()))))
}

pub fn func_json_group_object(args: &[Value]) -> Result<Value> {
    if args.len() % 2 != 0 { return Err(Error::with_message(ErrorCode::Error, "json_group_object() requires an even number of arguments")); }
    let mut obj = Vec::new();
    for pair in args.chunks(2) {
        let key = match &pair[0] { Value::Text(s) => s.clone(), other => other.to_text() };
        obj.push((key, sql_to_json(&pair[1])));
    }
    Ok(Value::Text(json_to_string(&JsonValue::Object(obj))))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn text(s: &str) -> Value { Value::Text(s.to_string()) }
    fn int(n: i64) -> Value { Value::Integer(n) }
    #[test] fn test_parse() { assert!(parse_json("null").is_ok()); assert!(parse_json("{}").is_ok()); assert!(parse_json("[1,2]").is_ok()); }
    #[test] fn test_valid() { assert_eq!(func_json_valid(&[text(r#"{"a":1}"#)]).unwrap(), int(1)); assert_eq!(func_json_valid(&[text("bad")]).unwrap(), int(0)); }
    #[test] fn test_minify() { assert_eq!(func_json(&[text(r#" { "a" : 1 } "#)]).unwrap(), text(r#"{"a":1}"#)); }
    #[test] fn test_extract() { assert_eq!(func_json_extract(&[text(r#"{"a":1}"#), text("$.a")]).unwrap(), int(1)); }
    #[test] fn test_type() { assert_eq!(func_json_type(&[text("42")]).unwrap(), text("integer")); }
    #[test] fn test_array() { assert_eq!(func_json_array(&[int(1), int(2)]).unwrap(), text("[1,2]")); }
    #[test] fn test_object() { assert_eq!(func_json_object(&[text("a"), int(1)]).unwrap(), text(r#"{"a":1}"#)); }
    #[test] fn test_arr_len() { assert_eq!(func_json_array_length(&[text("[1,2,3]")]).unwrap(), int(3)); }
    #[test] fn test_insert() { assert_eq!(func_json_insert(&[text(r#"{"a":1}"#), text("$.b"), int(2)]).unwrap(), text(r#"{"a":1,"b":2}"#)); }
    #[test] fn test_set() { assert_eq!(func_json_set(&[text(r#"{"a":1}"#), text("$.a"), int(9)]).unwrap(), text(r#"{"a":9}"#)); }
    #[test] fn test_replace() { assert_eq!(func_json_replace(&[text(r#"{"a":1}"#), text("$.b"), int(2)]).unwrap(), text(r#"{"a":1}"#)); }
    #[test] fn test_remove() { assert_eq!(func_json_remove(&[text(r#"{"a":1,"b":2}"#), text("$.b")]).unwrap(), text(r#"{"a":1}"#)); }
    #[test] fn test_patch() { assert_eq!(func_json_patch(&[text(r#"{"a":1}"#), text(r#"{"b":2}"#)]).unwrap(), text(r#"{"a":1,"b":2}"#)); }
    #[test] fn test_quote() { assert_eq!(func_json_quote(&[text("hi")]).unwrap(), text(r#""hi""#)); }
    #[test] fn test_null() { assert_eq!(func_json(&[Value::Null]).unwrap(), Value::Null); }
    #[test] fn test_nested() { assert_eq!(func_json_extract(&[text(r#"{"a":{"b":42}}"#), text("$.a.b")]).unwrap(), int(42)); }
}
