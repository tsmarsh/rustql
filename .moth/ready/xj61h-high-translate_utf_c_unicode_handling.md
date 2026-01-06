# Translate utf.c - Unicode Handling

## Overview
Translate UTF-8 and UTF-16 encoding/decoding routines for proper text handling throughout the database.

## Source Reference
- `sqlite3/src/utf.c` - ~500 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Text Encoding
```rust
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TextEncoding {
    Utf8,
    Utf16le,
    Utf16be,
    Utf16,  // Native byte order
}

impl TextEncoding {
    pub fn from_int(enc: i32) -> Option<Self> {
        match enc {
            1 => Some(Self::Utf8),
            2 => Some(Self::Utf16le),
            3 => Some(Self::Utf16be),
            4 => Some(Self::Utf16),
            _ => None,
        }
    }

    pub fn to_int(self) -> i32 {
        match self {
            Self::Utf8 => 1,
            Self::Utf16le => 2,
            Self::Utf16be => 3,
            Self::Utf16 => 4,
        }
    }
}
```

### UTF-8 Constants
```rust
/// UTF-8 byte sequence length lookup table
const UTF8_LEAD_BYTE_LEN: [u8; 256] = {
    let mut table = [1u8; 256];
    let mut i = 0xC0;
    while i < 0xE0 { table[i] = 2; i += 1; }
    while i < 0xF0 { table[i] = 3; i += 1; }
    while i < 0xF8 { table[i] = 4; i += 1; }
    table
};

/// Valid UTF-8 continuation byte check
fn is_utf8_continuation(byte: u8) -> bool {
    (byte & 0xC0) == 0x80
}
```

## UTF-8 Functions

### Reading UTF-8
```rust
/// Read a single UTF-8 character and advance pointer
pub fn utf8_read(bytes: &[u8], pos: &mut usize) -> Option<u32> {
    if *pos >= bytes.len() {
        return None;
    }

    let lead = bytes[*pos];
    let len = UTF8_LEAD_BYTE_LEN[lead as usize] as usize;

    if *pos + len > bytes.len() {
        // Incomplete sequence
        *pos += 1;
        return Some(0xFFFD); // Replacement character
    }

    let cp = match len {
        1 => lead as u32,
        2 => {
            let b1 = bytes[*pos + 1];
            if !is_utf8_continuation(b1) {
                *pos += 1;
                return Some(0xFFFD);
            }
            ((lead as u32 & 0x1F) << 6) | (b1 as u32 & 0x3F)
        }
        3 => {
            let b1 = bytes[*pos + 1];
            let b2 = bytes[*pos + 2];
            if !is_utf8_continuation(b1) || !is_utf8_continuation(b2) {
                *pos += 1;
                return Some(0xFFFD);
            }
            ((lead as u32 & 0x0F) << 12)
                | ((b1 as u32 & 0x3F) << 6)
                | (b2 as u32 & 0x3F)
        }
        4 => {
            let b1 = bytes[*pos + 1];
            let b2 = bytes[*pos + 2];
            let b3 = bytes[*pos + 3];
            if !is_utf8_continuation(b1) || !is_utf8_continuation(b2) || !is_utf8_continuation(b3) {
                *pos += 1;
                return Some(0xFFFD);
            }
            ((lead as u32 & 0x07) << 18)
                | ((b1 as u32 & 0x3F) << 12)
                | ((b2 as u32 & 0x3F) << 6)
                | (b3 as u32 & 0x3F)
        }
        _ => {
            *pos += 1;
            return Some(0xFFFD);
        }
    };

    *pos += len;

    // Validate: reject overlong encodings and surrogates
    if is_overlong(cp, len) || is_surrogate(cp) {
        Some(0xFFFD)
    } else {
        Some(cp)
    }
}

fn is_overlong(cp: u32, len: usize) -> bool {
    match len {
        2 => cp < 0x80,
        3 => cp < 0x800,
        4 => cp < 0x10000,
        _ => false,
    }
}

fn is_surrogate(cp: u32) -> bool {
    cp >= 0xD800 && cp <= 0xDFFF
}
```

### Writing UTF-8
```rust
/// Write a Unicode codepoint as UTF-8
pub fn utf8_write(buf: &mut Vec<u8>, cp: u32) {
    if cp < 0x80 {
        buf.push(cp as u8);
    } else if cp < 0x800 {
        buf.push(0xC0 | ((cp >> 6) as u8));
        buf.push(0x80 | ((cp & 0x3F) as u8));
    } else if cp < 0x10000 {
        buf.push(0xE0 | ((cp >> 12) as u8));
        buf.push(0x80 | (((cp >> 6) & 0x3F) as u8));
        buf.push(0x80 | ((cp & 0x3F) as u8));
    } else if cp < 0x110000 {
        buf.push(0xF0 | ((cp >> 18) as u8));
        buf.push(0x80 | (((cp >> 12) & 0x3F) as u8));
        buf.push(0x80 | (((cp >> 6) & 0x3F) as u8));
        buf.push(0x80 | ((cp & 0x3F) as u8));
    } else {
        // Invalid codepoint, write replacement
        buf.extend_from_slice(&[0xEF, 0xBF, 0xBD]); // U+FFFD
    }
}

/// Get UTF-8 encoded length for a codepoint
pub fn utf8_len(cp: u32) -> usize {
    if cp < 0x80 { 1 }
    else if cp < 0x800 { 2 }
    else if cp < 0x10000 { 3 }
    else { 4 }
}
```

## UTF-16 Functions

### Reading UTF-16
```rust
/// Read a UTF-16LE codepoint
pub fn utf16le_read(bytes: &[u8], pos: &mut usize) -> Option<u32> {
    if *pos + 2 > bytes.len() {
        return None;
    }

    let unit = u16::from_le_bytes([bytes[*pos], bytes[*pos + 1]]);
    *pos += 2;

    // Check for surrogate pair
    if unit >= 0xD800 && unit <= 0xDBFF {
        // High surrogate, need low surrogate
        if *pos + 2 > bytes.len() {
            return Some(0xFFFD);
        }
        let low = u16::from_le_bytes([bytes[*pos], bytes[*pos + 1]]);
        if low >= 0xDC00 && low <= 0xDFFF {
            *pos += 2;
            let cp = 0x10000
                + (((unit as u32 - 0xD800) << 10)
                | (low as u32 - 0xDC00));
            Some(cp)
        } else {
            Some(0xFFFD)
        }
    } else if unit >= 0xDC00 && unit <= 0xDFFF {
        // Lone low surrogate
        Some(0xFFFD)
    } else {
        Some(unit as u32)
    }
}

/// Read a UTF-16BE codepoint
pub fn utf16be_read(bytes: &[u8], pos: &mut usize) -> Option<u32> {
    if *pos + 2 > bytes.len() {
        return None;
    }

    let unit = u16::from_be_bytes([bytes[*pos], bytes[*pos + 1]]);
    *pos += 2;

    if unit >= 0xD800 && unit <= 0xDBFF {
        if *pos + 2 > bytes.len() {
            return Some(0xFFFD);
        }
        let low = u16::from_be_bytes([bytes[*pos], bytes[*pos + 1]]);
        if low >= 0xDC00 && low <= 0xDFFF {
            *pos += 2;
            let cp = 0x10000
                + (((unit as u32 - 0xD800) << 10)
                | (low as u32 - 0xDC00));
            Some(cp)
        } else {
            Some(0xFFFD)
        }
    } else if unit >= 0xDC00 && unit <= 0xDFFF {
        Some(0xFFFD)
    } else {
        Some(unit as u32)
    }
}
```

### Writing UTF-16
```rust
/// Write codepoint as UTF-16LE
pub fn utf16le_write(buf: &mut Vec<u8>, cp: u32) {
    if cp < 0x10000 {
        buf.extend_from_slice(&(cp as u16).to_le_bytes());
    } else {
        let cp = cp - 0x10000;
        let high = 0xD800 + ((cp >> 10) as u16);
        let low = 0xDC00 + ((cp & 0x3FF) as u16);
        buf.extend_from_slice(&high.to_le_bytes());
        buf.extend_from_slice(&low.to_le_bytes());
    }
}

/// Write codepoint as UTF-16BE
pub fn utf16be_write(buf: &mut Vec<u8>, cp: u32) {
    if cp < 0x10000 {
        buf.extend_from_slice(&(cp as u16).to_be_bytes());
    } else {
        let cp = cp - 0x10000;
        let high = 0xD800 + ((cp >> 10) as u16);
        let low = 0xDC00 + ((cp & 0x3FF) as u16);
        buf.extend_from_slice(&high.to_be_bytes());
        buf.extend_from_slice(&low.to_be_bytes());
    }
}
```

## Encoding Conversion

### UTF-8 to UTF-16
```rust
pub fn utf8_to_utf16(s: &str, encoding: TextEncoding) -> Vec<u8> {
    let mut result = Vec::with_capacity(s.len() * 2);

    for c in s.chars() {
        let cp = c as u32;
        match encoding {
            TextEncoding::Utf16le | TextEncoding::Utf16 => utf16le_write(&mut result, cp),
            TextEncoding::Utf16be => utf16be_write(&mut result, cp),
            _ => {}
        }
    }

    result
}
```

### UTF-16 to UTF-8
```rust
pub fn utf16_to_utf8(bytes: &[u8], encoding: TextEncoding) -> Result<String> {
    let mut result = Vec::with_capacity(bytes.len());
    let mut pos = 0;

    while pos < bytes.len() {
        let cp = match encoding {
            TextEncoding::Utf16le | TextEncoding::Utf16 => utf16le_read(bytes, &mut pos),
            TextEncoding::Utf16be => utf16be_read(bytes, &mut pos),
            _ => None,
        };

        match cp {
            Some(cp) => utf8_write(&mut result, cp),
            None => break,
        }
    }

    String::from_utf8(result).map_err(|_| Error::msg("invalid UTF conversion"))
}
```

## String Length Functions

### Character Count
```rust
/// Count characters in UTF-8 string
pub fn utf8_char_count(bytes: &[u8]) -> usize {
    bytes.iter().filter(|&&b| (b & 0xC0) != 0x80).count()
}

/// Count characters in UTF-16 string
pub fn utf16_char_count(bytes: &[u8], encoding: TextEncoding) -> usize {
    let mut count = 0;
    let mut pos = 0;

    while pos < bytes.len() {
        let cp = match encoding {
            TextEncoding::Utf16le | TextEncoding::Utf16 => utf16le_read(bytes, &mut pos),
            TextEncoding::Utf16be => utf16be_read(bytes, &mut pos),
            _ => None,
        };

        if cp.is_some() {
            count += 1;
        } else {
            break;
        }
    }

    count
}
```

## Case Conversion

### Unicode-aware Case Folding
```rust
/// Convert to uppercase (simple case folding)
pub fn utf8_uppercase(s: &str) -> String {
    s.to_uppercase()
}

/// Convert to lowercase (simple case folding)
pub fn utf8_lowercase(s: &str) -> String {
    s.to_lowercase()
}

/// Case-insensitive comparison
pub fn utf8_strcasecmp(a: &str, b: &str) -> std::cmp::Ordering {
    a.to_lowercase().cmp(&b.to_lowercase())
}
```

## Validation

### UTF-8 Validation
```rust
/// Check if bytes are valid UTF-8
pub fn utf8_valid(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).is_ok()
}

/// Sanitize potentially invalid UTF-8
pub fn utf8_sanitize(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}
```

## Integration with Value

```rust
impl Value {
    /// Get text in specified encoding
    pub fn text(&self, encoding: TextEncoding) -> Vec<u8> {
        match self {
            Value::Text(s) => match encoding {
                TextEncoding::Utf8 => s.as_bytes().to_vec(),
                _ => utf8_to_utf16(s, encoding),
            },
            Value::Blob(b) => b.clone(),
            _ => self.to_string().into_bytes(),
        }
    }

    /// Set text from specified encoding
    pub fn set_text(&mut self, bytes: &[u8], encoding: TextEncoding) -> Result<()> {
        *self = match encoding {
            TextEncoding::Utf8 => {
                Value::Text(String::from_utf8(bytes.to_vec())
                    .map_err(|_| Error::msg("invalid UTF-8"))?)
            }
            _ => {
                Value::Text(utf16_to_utf8(bytes, encoding)?)
            }
        };
        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] UTF-8 read/write operations
- [ ] UTF-16LE read/write operations
- [ ] UTF-16BE read/write operations
- [ ] UTF-8 to UTF-16 conversion
- [ ] UTF-16 to UTF-8 conversion
- [ ] Character counting (not byte counting)
- [ ] Proper surrogate pair handling
- [ ] Invalid sequence detection
- [ ] Replacement character (U+FFFD) for errors
- [ ] Case conversion (upper/lower)
- [ ] Case-insensitive comparison
- [ ] BOM handling (Byte Order Mark)
- [ ] Integration with Value type
