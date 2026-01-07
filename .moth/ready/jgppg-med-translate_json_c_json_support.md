# Translate json.c - JSON Support

## Overview
Translate JSON1 extension providing JSON functions for parsing, querying, and manipulating JSON data.

## Source Reference
- `sqlite3/src/json.c` - 5,599 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### JSON Node
```rust
/// Node types in JSON parse tree
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JsonNodeType {
    Null,
    True,
    False,
    Int,
    Real,
    String,
    Array,
    Object,
}

/// Node in JSON parse tree
pub struct JsonNode {
    /// Node type
    pub node_type: JsonNodeType,
    /// Number of child nodes (for arrays/objects)
    pub n_children: u32,
    /// Offset into original JSON string
    pub offset: u32,
    /// Length in original JSON string
    pub len: u32,
    /// String/number value (for leaf nodes)
    pub value: Option<JsonValue>,
}

#[derive(Debug, Clone)]
pub enum JsonValue {
    Int(i64),
    Real(f64),
    String(String),
}

/// Parsed JSON document
pub struct JsonParse {
    /// Original JSON text
    pub json: String,
    /// Array of nodes
    pub nodes: Vec<JsonNode>,
    /// Has errors
    pub has_error: bool,
    /// Error message
    pub error: Option<String>,
}
```

### JSON Path
```rust
/// Parsed JSON path
pub struct JsonPath {
    pub segments: Vec<PathSegment>,
}

pub enum PathSegment {
    /// Object key: .key or ["key"]
    Key(String),
    /// Array index: [0]
    Index(i64),
    /// Wildcard: [*]
    Wildcard,
    /// Recursive descent: **
    Recursive,
}
```

## JSON Parsing

### Parser
```rust
impl JsonParse {
    pub fn parse(json: &str) -> Result<Self> {
        let mut parser = JsonParser {
            json: json.as_bytes(),
            pos: 0,
            nodes: Vec::new(),
        };

        parser.skip_whitespace();
        parser.parse_value()?;
        parser.skip_whitespace();

        if parser.pos < parser.json.len() {
            return Err(Error::msg("trailing characters after JSON"));
        }

        Ok(JsonParse {
            json: json.to_string(),
            nodes: parser.nodes,
            has_error: false,
            error: None,
        })
    }
}

struct JsonParser<'a> {
    json: &'a [u8],
    pos: usize,
    nodes: Vec<JsonNode>,
}

impl<'a> JsonParser<'a> {
    fn parse_value(&mut self) -> Result<usize> {
        self.skip_whitespace();

        match self.peek() {
            Some(b'n') => self.parse_null(),
            Some(b't') => self.parse_true(),
            Some(b'f') => self.parse_false(),
            Some(b'"') => self.parse_string(),
            Some(b'[') => self.parse_array(),
            Some(b'{') => self.parse_object(),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.parse_number(),
            _ => Err(Error::msg("unexpected character")),
        }
    }

    fn parse_object(&mut self) -> Result<usize> {
        let node_idx = self.nodes.len();
        self.nodes.push(JsonNode {
            node_type: JsonNodeType::Object,
            n_children: 0,
            offset: self.pos as u32,
            len: 0,
            value: None,
        });

        self.expect(b'{')?;
        self.skip_whitespace();

        let mut n_children = 0;

        if self.peek() != Some(b'}') {
            loop {
                // Parse key
                self.parse_string()?;
                self.skip_whitespace();
                self.expect(b':')?;

                // Parse value
                self.parse_value()?;
                n_children += 1;

                self.skip_whitespace();
                if self.peek() == Some(b',') {
                    self.advance();
                    self.skip_whitespace();
                } else {
                    break;
                }
            }
        }

        self.expect(b'}')?;
        self.nodes[node_idx].n_children = n_children;
        self.nodes[node_idx].len = (self.pos - self.nodes[node_idx].offset as usize) as u32;

        Ok(node_idx)
    }

    fn parse_array(&mut self) -> Result<usize> {
        let node_idx = self.nodes.len();
        self.nodes.push(JsonNode {
            node_type: JsonNodeType::Array,
            n_children: 0,
            offset: self.pos as u32,
            len: 0,
            value: None,
        });

        self.expect(b'[')?;
        self.skip_whitespace();

        let mut n_children = 0;

        if self.peek() != Some(b']') {
            loop {
                self.parse_value()?;
                n_children += 1;

                self.skip_whitespace();
                if self.peek() == Some(b',') {
                    self.advance();
                    self.skip_whitespace();
                } else {
                    break;
                }
            }
        }

        self.expect(b']')?;
        self.nodes[node_idx].n_children = n_children;
        self.nodes[node_idx].len = (self.pos - self.nodes[node_idx].offset as usize) as u32;

        Ok(node_idx)
    }

    fn parse_string(&mut self) -> Result<usize> {
        let start = self.pos;
        self.expect(b'"')?;

        let mut s = String::new();

        loop {
            match self.peek() {
                Some(b'"') => {
                    self.advance();
                    break;
                }
                Some(b'\\') => {
                    self.advance();
                    match self.peek() {
                        Some(b'"') => { s.push('"'); self.advance(); }
                        Some(b'\\') => { s.push('\\'); self.advance(); }
                        Some(b'/') => { s.push('/'); self.advance(); }
                        Some(b'b') => { s.push('\x08'); self.advance(); }
                        Some(b'f') => { s.push('\x0c'); self.advance(); }
                        Some(b'n') => { s.push('\n'); self.advance(); }
                        Some(b'r') => { s.push('\r'); self.advance(); }
                        Some(b't') => { s.push('\t'); self.advance(); }
                        Some(b'u') => {
                            self.advance();
                            let hex = self.take_n(4)?;
                            let cp = u16::from_str_radix(std::str::from_utf8(hex)?, 16)?;
                            s.push(char::from_u32(cp as u32).unwrap_or('\u{FFFD}'));
                        }
                        _ => return Err(Error::msg("invalid escape")),
                    }
                }
                Some(c) if c >= 0x20 => {
                    s.push(c as char);
                    self.advance();
                }
                _ => return Err(Error::msg("unterminated string")),
            }
        }

        let node_idx = self.nodes.len();
        self.nodes.push(JsonNode {
            node_type: JsonNodeType::String,
            n_children: 0,
            offset: start as u32,
            len: (self.pos - start) as u32,
            value: Some(JsonValue::String(s)),
        });

        Ok(node_idx)
    }
}
```

## JSON Functions

### json() - Validate/minify
```rust
fn json_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let json_str = args[0].as_str();

    match JsonParse::parse(json_str) {
        Ok(parsed) => {
            // Return minified JSON
            ctx.result_text(&parsed.to_string());
        }
        Err(e) => {
            ctx.result_error(&format!("malformed JSON: {}", e));
        }
    }
    Ok(())
}
```

### json_extract()
```rust
fn json_extract_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let json_str = args[0].as_str();
    let parsed = JsonParse::parse(json_str)?;

    if args.len() == 2 {
        // Single path - return the value
        let path = JsonPath::parse(args[1].as_str())?;
        match parsed.extract(&path) {
            Some(node) => ctx.set_result(node.to_sql_value()),
            None => ctx.result_null(),
        }
    } else {
        // Multiple paths - return JSON array
        let mut results = Vec::new();
        for arg in &args[1..] {
            let path = JsonPath::parse(arg.as_str())?;
            match parsed.extract(&path) {
                Some(node) => results.push(node.to_json()),
                None => results.push("null".to_string()),
            }
        }
        ctx.result_text(&format!("[{}]", results.join(",")));
    }
    Ok(())
}
```

### json_set() / json_insert() / json_replace()
```rust
fn json_set_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    json_modify(ctx, args, ModifyMode::Set)
}

fn json_insert_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    json_modify(ctx, args, ModifyMode::Insert)
}

fn json_replace_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    json_modify(ctx, args, ModifyMode::Replace)
}

enum ModifyMode {
    Set,     // Insert or replace
    Insert,  // Insert only if not exists
    Replace, // Replace only if exists
}

fn json_modify(ctx: &mut Context, args: &[&Value], mode: ModifyMode) -> Result<()> {
    let json_str = args[0].as_str();
    let mut parsed = JsonParse::parse(json_str)?;

    // Process path/value pairs
    for chunk in args[1..].chunks(2) {
        if chunk.len() < 2 {
            return Err(Error::msg("missing value for path"));
        }

        let path = JsonPath::parse(chunk[0].as_str())?;
        let value = &chunk[1];

        parsed.modify(&path, value, mode)?;
    }

    ctx.result_text(&parsed.to_string());
    Ok(())
}
```

### json_array() / json_object()
```rust
fn json_array_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let elements: Vec<String> = args.iter()
        .map(|v| value_to_json(v))
        .collect();

    ctx.result_text(&format!("[{}]", elements.join(",")));
    Ok(())
}

fn json_object_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    if args.len() % 2 != 0 {
        return Err(Error::msg("json_object requires even number of arguments"));
    }

    let pairs: Vec<String> = args.chunks(2)
        .map(|chunk| {
            let key = chunk[0].as_str();
            let value = value_to_json(&chunk[1]);
            format!("{}:{}", json_quote_string(key), value)
        })
        .collect();

    ctx.result_text(&format!("{{{}}}", pairs.join(",")));
    Ok(())
}

fn value_to_json(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Real(r) => r.to_string(),
        Value::Text(s) => {
            // Check if it's already JSON
            if JsonParse::parse(s).is_ok() {
                s.clone()
            } else {
                json_quote_string(s)
            }
        }
        Value::Blob(b) => json_quote_string(&base64::encode(b)),
    }
}
```

### json_type()
```rust
fn json_type_func(ctx: &mut Context, args: &[&Value]) -> Result<()> {
    let json_str = args[0].as_str();
    let parsed = JsonParse::parse(json_str)?;

    let node = if args.len() > 1 {
        let path = JsonPath::parse(args[1].as_str())?;
        match parsed.extract(&path) {
            Some(n) => n,
            None => {
                ctx.result_null();
                return Ok(());
            }
        }
    } else {
        &parsed.nodes[0]
    };

    let type_name = match node.node_type {
        JsonNodeType::Null => "null",
        JsonNodeType::True | JsonNodeType::False => "boolean",
        JsonNodeType::Int | JsonNodeType::Real => "number",
        JsonNodeType::String => "text",
        JsonNodeType::Array => "array",
        JsonNodeType::Object => "object",
    };

    ctx.result_text(type_name);
    Ok(())
}
```

### json_each() / json_tree() Table-Valued Functions
```rust
pub struct JsonEachVtab {
    /// Base virtual table
    pub base: VirtualTable,
}

pub struct JsonEachCursor {
    /// Current position
    pub idx: usize,
    /// Flattened JSON elements
    pub elements: Vec<JsonElement>,
}

pub struct JsonElement {
    pub key: Option<String>,
    pub value: String,
    pub value_type: JsonNodeType,
    pub atom: Option<Value>,
    pub id: i64,
    pub parent: Option<i64>,
    pub fullkey: String,
    pub path: String,
}

impl VirtualTableModule for JsonEachVtab {
    fn connect(db: &Connection) -> Result<Self> {
        Ok(Self { base: VirtualTable::new() })
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // Require json argument
        info.estimated_cost = 1.0;
        Ok(())
    }

    fn open(&self) -> Result<Box<dyn Cursor>> {
        Ok(Box::new(JsonEachCursor {
            idx: 0,
            elements: Vec::new(),
        }))
    }
}

impl Cursor for JsonEachCursor {
    fn filter(&mut self, args: &[&Value]) -> Result<()> {
        let json_str = args[0].as_str();
        let path = args.get(1).map(|v| v.as_str()).unwrap_or("$");

        let parsed = JsonParse::parse(json_str)?;
        let json_path = JsonPath::parse(path)?;

        // Flatten JSON into elements
        self.elements = parsed.flatten(&json_path, false)?;
        self.idx = 0;

        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.idx += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.idx >= self.elements.len()
    }

    fn column(&self, i: usize) -> Result<Value> {
        let elem = &self.elements[self.idx];
        Ok(match i {
            0 => elem.key.clone().map(Value::Text).unwrap_or(Value::Null),
            1 => Value::Text(elem.value.clone()),
            2 => Value::Text(elem.value_type.to_string()),
            3 => elem.atom.clone().unwrap_or(Value::Null),
            4 => Value::Integer(elem.id),
            5 => elem.parent.map(Value::Integer).unwrap_or(Value::Null),
            6 => Value::Text(elem.fullkey.clone()),
            7 => Value::Text(elem.path.clone()),
            _ => Value::Null,
        })
    }
}
```

## Acceptance Criteria
- [ ] json() - validate and minify
- [ ] json_extract() / -> / ->> operators
- [ ] json_set() / json_insert() / json_replace()
- [ ] json_remove()
- [ ] json_array() / json_object()
- [ ] json_type()
- [ ] json_valid()
- [ ] json_quote()
- [ ] json_array_length()
- [ ] json_patch()
- [ ] json_each() table-valued function
- [ ] json_tree() table-valued function
- [ ] JSON path syntax ($.key, $[0], etc.)
- [ ] Proper Unicode handling
- [ ] Error handling for malformed JSON

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `json101.test` - Basic JSON functions
- `json102.test` - JSON extraction and path queries
- `json103.test` - JSON modification functions
- `json104.test` - JSON array/object construction
- `json105.test` - JSON type checking
- `json106.test` - JSON table-valued functions
- `json107.test` - JSON edge cases
- `json108.test` - JSON performance tests
- `json501.test` - JSON5 extended syntax
- `json502.test` - Additional JSON5 tests
- `jsonb01.test` - JSONB binary format
