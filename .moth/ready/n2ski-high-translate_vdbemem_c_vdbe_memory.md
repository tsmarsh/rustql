# Translate vdbemem.c - VDBE Memory

## Overview
Translate VDBE memory cell (Mem) operations including type conversions, comparisons, and serialization.

## Source Reference
- `sqlite3/src/vdbemem.c` - 2,057 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Functions

### Memory Cell Creation

```rust
impl Mem {
    /// Create a NULL value
    pub fn null() -> Self {
        Mem {
            value: MemValue::Null,
            flags: MemFlags::NULL,
            enc: Encoding::Utf8,
            n: 0,
            db: None,
        }
    }

    /// Create an integer value
    pub fn int(i: i64) -> Self {
        Mem {
            value: MemValue::Int(i),
            flags: MemFlags::INT,
            enc: Encoding::Utf8,
            n: 0,
            db: None,
        }
    }

    /// Create a real (double) value
    pub fn real(r: f64) -> Self {
        Mem {
            value: MemValue::Real(r),
            flags: MemFlags::REAL,
            enc: Encoding::Utf8,
            n: 0,
            db: None,
        }
    }

    /// Create a text value
    pub fn text(s: String) -> Self {
        let n = s.len() as i32;
        Mem {
            value: MemValue::Str { data: s, nul: true },
            flags: MemFlags::STR | MemFlags::TERM,
            enc: Encoding::Utf8,
            n,
            db: None,
        }
    }

    /// Create a blob value
    pub fn blob(b: Vec<u8>) -> Self {
        let n = b.len() as i32;
        Mem {
            value: MemValue::Blob(b),
            flags: MemFlags::BLOB,
            enc: Encoding::Utf8,
            n,
            db: None,
        }
    }

    /// Create a zeroblob
    pub fn zeroblob(n: i32) -> Self {
        Mem {
            value: MemValue::ZeroBlob(n),
            flags: MemFlags::BLOB | MemFlags::ZERO,
            enc: Encoding::Utf8,
            n,
            db: None,
        }
    }
}
```

### Type Checking

```rust
impl Mem {
    pub fn is_null(&self) -> bool {
        matches!(self.value, MemValue::Null)
    }

    pub fn is_int(&self) -> bool {
        matches!(self.value, MemValue::Int(_))
    }

    pub fn is_real(&self) -> bool {
        matches!(self.value, MemValue::Real(_))
    }

    pub fn is_text(&self) -> bool {
        matches!(self.value, MemValue::Str { .. })
    }

    pub fn is_blob(&self) -> bool {
        matches!(self.value, MemValue::Blob(_) | MemValue::ZeroBlob(_))
    }

    pub fn is_numeric(&self) -> bool {
        self.is_int() || self.is_real()
    }
}
```

### Type Conversions

```rust
impl Mem {
    /// Convert to integer (with SQLite coercion rules)
    pub fn to_i64(&self) -> i64 {
        match &self.value {
            MemValue::Null => 0,
            MemValue::Int(i) => *i,
            MemValue::Real(r) => *r as i64,
            MemValue::Str { data, .. } => {
                // Parse integer from string
                data.trim().parse().unwrap_or(0)
            }
            MemValue::Blob(b) => {
                // Parse from blob as string
                String::from_utf8_lossy(b)
                    .trim()
                    .parse()
                    .unwrap_or(0)
            }
            MemValue::ZeroBlob(_) => 0,
            MemValue::Ptr { .. } => 0,
        }
    }

    /// Convert to real (with SQLite coercion rules)
    pub fn to_f64(&self) -> f64 {
        match &self.value {
            MemValue::Null => 0.0,
            MemValue::Int(i) => *i as f64,
            MemValue::Real(r) => *r,
            MemValue::Str { data, .. } => {
                data.trim().parse().unwrap_or(0.0)
            }
            MemValue::Blob(b) => {
                String::from_utf8_lossy(b)
                    .trim()
                    .parse()
                    .unwrap_or(0.0)
            }
            MemValue::ZeroBlob(_) => 0.0,
            MemValue::Ptr { .. } => 0.0,
        }
    }

    /// Convert to text
    pub fn to_text(&self) -> &str {
        match &self.value {
            MemValue::Str { data, .. } => data,
            _ => "", // For other types, would need owned string
        }
    }

    /// Convert to text (owned)
    pub fn to_string(&self) -> String {
        match &self.value {
            MemValue::Null => String::new(),
            MemValue::Int(i) => i.to_string(),
            MemValue::Real(r) => format!("{}", r),
            MemValue::Str { data, .. } => data.clone(),
            MemValue::Blob(b) => String::from_utf8_lossy(b).to_string(),
            MemValue::ZeroBlob(n) => "\0".repeat(*n as usize),
            MemValue::Ptr { .. } => String::new(),
        }
    }

    /// Convert to blob
    pub fn to_blob(&self) -> &[u8] {
        match &self.value {
            MemValue::Blob(b) => b,
            MemValue::Str { data, .. } => data.as_bytes(),
            _ => &[],
        }
    }

    /// Convert to Value enum
    pub fn to_value(&self) -> Value {
        match &self.value {
            MemValue::Null => Value::Null,
            MemValue::Int(i) => Value::Integer(*i),
            MemValue::Real(r) => Value::Real(*r),
            MemValue::Str { data, .. } => Value::Text(data.clone()),
            MemValue::Blob(b) => Value::Blob(b.clone()),
            MemValue::ZeroBlob(n) => Value::Blob(vec![0u8; *n as usize]),
            MemValue::Ptr { .. } => Value::Null,
        }
    }
}
```

### Mutation Operations

```rust
impl Mem {
    /// Set to NULL
    pub fn set_null(&mut self) {
        self.value = MemValue::Null;
        self.flags = MemFlags::NULL;
        self.n = 0;
    }

    /// Set integer value
    pub fn set_int(&mut self, i: i64) {
        self.value = MemValue::Int(i);
        self.flags = MemFlags::INT;
        self.n = 0;
    }

    /// Set real value
    pub fn set_real(&mut self, r: f64) {
        self.value = MemValue::Real(r);
        self.flags = MemFlags::REAL;
        self.n = 0;
    }

    /// Set text value
    pub fn set_text(&mut self, s: String) {
        self.n = s.len() as i32;
        self.value = MemValue::Str { data: s, nul: true };
        self.flags = MemFlags::STR | MemFlags::TERM;
    }

    /// Set blob value
    pub fn set_blob(&mut self, b: Vec<u8>) {
        self.n = b.len() as i32;
        self.value = MemValue::Blob(b);
        self.flags = MemFlags::BLOB;
    }
}
```

### Serialization (for records)

```rust
impl Mem {
    /// Get serial type for this value
    pub fn serial_type(&self) -> SerialType {
        match &self.value {
            MemValue::Null => SerialType::Null,
            MemValue::Int(i) => {
                // Choose smallest encoding
                let i = *i;
                if i == 0 {
                    SerialType::Zero
                } else if i == 1 {
                    SerialType::One
                } else if i >= -128 && i <= 127 {
                    SerialType::Int8
                } else if i >= -32768 && i <= 32767 {
                    SerialType::Int16
                } else if i >= -8388608 && i <= 8388607 {
                    SerialType::Int24
                } else if i >= -2147483648 && i <= 2147483647 {
                    SerialType::Int32
                } else if i >= -140737488355328 && i <= 140737488355327 {
                    SerialType::Int48
                } else {
                    SerialType::Int64
                }
            }
            MemValue::Real(_) => SerialType::Float64,
            MemValue::Str { data, .. } => SerialType::Text(data.len() as u32),
            MemValue::Blob(b) => SerialType::Blob(b.len() as u32),
            MemValue::ZeroBlob(n) => SerialType::Blob(*n as u32),
            MemValue::Ptr { .. } => SerialType::Null,
        }
    }

    /// Serialize value to buffer
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match &self.value {
            MemValue::Null => {}
            MemValue::Int(i) => {
                let st = self.serial_type();
                match st {
                    SerialType::Zero | SerialType::One => {}
                    SerialType::Int8 => buf.push(*i as u8),
                    SerialType::Int16 => buf.extend(&(*i as i16).to_be_bytes()),
                    SerialType::Int24 => {
                        let bytes = (*i as i32).to_be_bytes();
                        buf.extend(&bytes[1..4]);
                    }
                    SerialType::Int32 => buf.extend(&(*i as i32).to_be_bytes()),
                    SerialType::Int48 => {
                        let bytes = i.to_be_bytes();
                        buf.extend(&bytes[2..8]);
                    }
                    SerialType::Int64 => buf.extend(&i.to_be_bytes()),
                    _ => {}
                }
            }
            MemValue::Real(r) => {
                buf.extend(&r.to_be_bytes());
            }
            MemValue::Str { data, .. } => {
                buf.extend(data.as_bytes());
            }
            MemValue::Blob(b) => {
                buf.extend(b);
            }
            MemValue::ZeroBlob(n) => {
                buf.extend(vec![0u8; *n as usize]);
            }
            MemValue::Ptr { .. } => {}
        }
    }

    /// Deserialize value from buffer
    pub fn deserialize(buf: &[u8], serial_type: SerialType) -> (Self, usize) {
        match serial_type {
            SerialType::Null => (Mem::null(), 0),
            SerialType::Zero => (Mem::int(0), 0),
            SerialType::One => (Mem::int(1), 0),
            SerialType::Int8 => {
                let i = buf[0] as i8 as i64;
                (Mem::int(i), 1)
            }
            SerialType::Int16 => {
                let i = i16::from_be_bytes([buf[0], buf[1]]) as i64;
                (Mem::int(i), 2)
            }
            // ... other integer sizes
            SerialType::Float64 => {
                let r = f64::from_be_bytes(buf[0..8].try_into().unwrap());
                (Mem::real(r), 8)
            }
            SerialType::Text(n) => {
                let s = String::from_utf8_lossy(&buf[..n as usize]).to_string();
                (Mem::text(s), n as usize)
            }
            SerialType::Blob(n) => {
                let b = buf[..n as usize].to_vec();
                (Mem::blob(b), n as usize)
            }
            _ => (Mem::null(), 0),
        }
    }
}
```

### Affinity Application

```rust
impl Mem {
    /// Apply type affinity to value
    pub fn apply_affinity(&mut self, affinity: Affinity) {
        match affinity {
            Affinity::Integer => {
                if self.is_text() || self.is_real() {
                    let i = self.to_i64();
                    self.set_int(i);
                }
            }
            Affinity::Real => {
                if self.is_text() {
                    let r = self.to_f64();
                    self.set_real(r);
                }
            }
            Affinity::Numeric => {
                if self.is_text() {
                    // Try integer first, then real
                    let s = self.to_string();
                    if let Ok(i) = s.trim().parse::<i64>() {
                        self.set_int(i);
                    } else if let Ok(r) = s.trim().parse::<f64>() {
                        self.set_real(r);
                    }
                }
            }
            Affinity::Text => {
                if self.is_int() || self.is_real() {
                    let s = self.to_string();
                    self.set_text(s);
                }
            }
            Affinity::Blob | Affinity::Flexnum => {
                // No conversion
            }
        }
    }
}
```

### SQL Literal Output

```rust
impl Mem {
    /// Format as SQL literal (for EXPLAIN)
    pub fn to_sql_literal(&self) -> String {
        match &self.value {
            MemValue::Null => "NULL".to_string(),
            MemValue::Int(i) => i.to_string(),
            MemValue::Real(r) => format!("{}", r),
            MemValue::Str { data, .. } => {
                format!("'{}'", data.replace('\'', "''"))
            }
            MemValue::Blob(b) => {
                format!("X'{}'", hex::encode(b))
            }
            MemValue::ZeroBlob(n) => {
                format!("zeroblob({})", n)
            }
            MemValue::Ptr { .. } => "NULL".to_string(),
        }
    }
}
```

## Acceptance Criteria
- [ ] Mem creation functions (null, int, real, text, blob, zeroblob)
- [ ] Type checking methods (is_null, is_int, is_real, is_text, is_blob)
- [ ] Type conversions (to_i64, to_f64, to_text, to_blob, to_value)
- [ ] Mutation methods (set_null, set_int, set_real, set_text, set_blob)
- [ ] Serial type calculation
- [ ] Serialization to record format
- [ ] Deserialization from record format
- [ ] Affinity application
- [ ] SQL literal formatting
