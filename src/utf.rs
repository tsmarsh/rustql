//! UTF-8/UTF-16 encoding and decoding helpers.
//!
//! This module mirrors the behavior of SQLite's utf.c for decoding,
//! encoding, and counting Unicode codepoints.

use crate::error::Result;
use crate::schema::Encoding;
use crate::types::Value;

// Lookup table used to decode UTF-8 lead bytes (sqlite3Utf8Trans1).
const UTF8_TRANS1: [u8; 64] = [
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x00, 0x00,
];

const REPLACEMENT_CHAR: u32 = 0xFFFD;

/// Append a single UTF-8 character to a buffer. Returns bytes written.
pub fn append_one_utf8_character(out: &mut [u8], v: u32) -> usize {
    if v < 0x80 {
        out[0] = (v & 0xff) as u8;
        1
    } else if v < 0x800 {
        out[0] = 0xc0 | ((v >> 6) as u8 & 0x1f);
        out[1] = 0x80 | ((v & 0x3f) as u8);
        2
    } else if v < 0x10000 {
        out[0] = 0xe0 | ((v >> 12) as u8 & 0x0f);
        out[1] = 0x80 | ((v >> 6) as u8 & 0x3f);
        out[2] = 0x80 | ((v & 0x3f) as u8);
        3
    } else {
        out[0] = 0xf0 | ((v >> 18) as u8 & 0x07);
        out[1] = 0x80 | ((v >> 12) as u8 & 0x3f);
        out[2] = 0x80 | ((v >> 6) as u8 & 0x3f);
        out[3] = 0x80 | ((v & 0x3f) as u8);
        4
    }
}

/// Read one UTF-8 character and advance `pos`.
pub fn utf8_read(bytes: &[u8], pos: &mut usize) -> u32 {
    if *pos >= bytes.len() {
        return 0;
    }

    let c = bytes[*pos] as u32;
    *pos += 1;

    if c >= 0xc0 {
        let idx = (c - 0xc0) as usize;
        let mut v = UTF8_TRANS1[idx] as u32;
        while *pos < bytes.len() && (bytes[*pos] & 0xc0) == 0x80 {
            v = (v << 6) + (bytes[*pos] as u32 & 0x3f);
            *pos += 1;
        }
        if v < 0x80 || (v & 0xFFFF_F800) == 0xD800 || (v & 0xFFFF_FFFE) == 0xFFFE {
            v = REPLACEMENT_CHAR;
        }
        v
    } else {
        c
    }
}

/// Read one UTF-8 character from up to `n` bytes.
pub fn utf8_read_limited(bytes: &[u8], n: usize) -> (u32, usize) {
    if bytes.is_empty() || n == 0 {
        return (0, 0);
    }

    let mut c = bytes[0] as u32;
    let mut i = 1usize;
    if c >= 0xc0 {
        let idx = (c - 0xc0) as usize;
        let mut v = UTF8_TRANS1[idx] as u32;
        let limit = n.min(bytes.len()).min(4);
        while i < limit && (bytes[i] & 0xc0) == 0x80 {
            v = (v << 6) + (bytes[i] as u32 & 0x3f);
            i += 1;
        }
        c = v;
    }

    (c, i)
}

/// Count Unicode characters in a UTF-8 byte buffer.
pub fn utf8_char_len(bytes: &[u8], n_byte: i32) -> usize {
    let mut pos = 0usize;
    let term = if n_byte >= 0 {
        bytes.len().min(n_byte as usize)
    } else {
        bytes.len()
    };
    let mut count = 0usize;

    while pos < term && bytes[pos] != 0 {
        let first = bytes[pos];
        pos += 1;
        if first >= 0xc0 {
            while pos < term && (bytes[pos] & 0xc0) == 0x80 {
                pos += 1;
            }
        }
        count += 1;
    }

    count
}

/// Append a UTF-8 encoded codepoint.
pub fn utf8_write(buf: &mut Vec<u8>, cp: u32) {
    let mut tmp = [0u8; 4];
    let n = append_one_utf8_character(&mut tmp, cp);
    buf.extend_from_slice(&tmp[..n]);
}

/// Append a UTF-16LE encoded codepoint.
pub fn utf16le_write(buf: &mut Vec<u8>, cp: u32) {
    if cp <= 0xFFFF {
        buf.extend_from_slice(&(cp as u16).to_le_bytes());
    } else {
        let cp = cp - 0x10000;
        let high = 0xD800 + ((cp >> 10) as u16);
        let low = 0xDC00 + ((cp & 0x3FF) as u16);
        buf.extend_from_slice(&high.to_le_bytes());
        buf.extend_from_slice(&low.to_le_bytes());
    }
}

/// Append a UTF-16BE encoded codepoint.
pub fn utf16be_write(buf: &mut Vec<u8>, cp: u32) {
    if cp <= 0xFFFF {
        buf.extend_from_slice(&(cp as u16).to_be_bytes());
    } else {
        let cp = cp - 0x10000;
        let high = 0xD800 + ((cp >> 10) as u16);
        let low = 0xDC00 + ((cp & 0x3FF) as u16);
        buf.extend_from_slice(&high.to_be_bytes());
        buf.extend_from_slice(&low.to_be_bytes());
    }
}

/// Read a UTF-16LE codepoint and advance `pos`.
pub fn utf16le_read(bytes: &[u8], pos: &mut usize) -> u32 {
    if *pos + 2 > bytes.len() {
        return 0;
    }

    let unit = u16::from_le_bytes([bytes[*pos], bytes[*pos + 1]]);
    *pos += 2;

    if (0xD800..=0xDBFF).contains(&unit) {
        if *pos + 2 > bytes.len() {
            return REPLACEMENT_CHAR;
        }
        let low = u16::from_le_bytes([bytes[*pos], bytes[*pos + 1]]);
        if (0xDC00..=0xDFFF).contains(&low) {
            *pos += 2;
            0x10000 + (((unit as u32 - 0xD800) << 10) | (low as u32 - 0xDC00))
        } else {
            REPLACEMENT_CHAR
        }
    } else if (0xDC00..=0xDFFF).contains(&unit) {
        REPLACEMENT_CHAR
    } else {
        unit as u32
    }
}

/// Read a UTF-16BE codepoint and advance `pos`.
pub fn utf16be_read(bytes: &[u8], pos: &mut usize) -> u32 {
    if *pos + 2 > bytes.len() {
        return 0;
    }

    let unit = u16::from_be_bytes([bytes[*pos], bytes[*pos + 1]]);
    *pos += 2;

    if (0xD800..=0xDBFF).contains(&unit) {
        if *pos + 2 > bytes.len() {
            return REPLACEMENT_CHAR;
        }
        let low = u16::from_be_bytes([bytes[*pos], bytes[*pos + 1]]);
        if (0xDC00..=0xDFFF).contains(&low) {
            *pos += 2;
            0x10000 + (((unit as u32 - 0xD800) << 10) | (low as u32 - 0xDC00))
        } else {
            REPLACEMENT_CHAR
        }
    } else if (0xDC00..=0xDFFF).contains(&unit) {
        REPLACEMENT_CHAR
    } else {
        unit as u32
    }
}

/// Convert UTF-8 bytes into UTF-16 bytes using the requested encoding.
pub fn utf8_to_utf16(bytes: &[u8], encoding: Encoding) -> Vec<u8> {
    if encoding == Encoding::Utf8 {
        return bytes.to_vec();
    }

    let mut pos = 0usize;
    let mut out = Vec::with_capacity(bytes.len() * 2);
    while pos < bytes.len() {
        let cp = utf8_read(bytes, &mut pos);
        match encoding {
            Encoding::Utf16le => utf16le_write(&mut out, cp),
            Encoding::Utf16be => utf16be_write(&mut out, cp),
            Encoding::Utf8 => {}
        }
    }
    out
}

/// Convert UTF-16 bytes into UTF-8 bytes using the requested encoding.
pub fn utf16_to_utf8(bytes: &[u8], encoding: Encoding) -> Vec<u8> {
    if encoding == Encoding::Utf8 {
        return bytes.to_vec();
    }

    let mut pos = 0usize;
    let mut out = Vec::with_capacity(bytes.len());
    while pos + 1 < bytes.len() {
        let cp = match encoding {
            Encoding::Utf16le => utf16le_read(bytes, &mut pos),
            Encoding::Utf16be => utf16be_read(bytes, &mut pos),
            Encoding::Utf8 => 0,
        };
        utf8_write(&mut out, cp);
    }
    out
}

/// Detect a UTF-16 BOM and return the encoding.
pub fn utf16_detect_bom(bytes: &[u8]) -> Option<Encoding> {
    if bytes.len() < 2 {
        return None;
    }
    let b1 = bytes[0];
    let b2 = bytes[1];
    if b1 == 0xFE && b2 == 0xFF {
        Some(Encoding::Utf16be)
    } else if b1 == 0xFF && b2 == 0xFE {
        Some(Encoding::Utf16le)
    } else {
        None
    }
}

/// Strip a UTF-16 BOM and return the detected encoding and slice.
pub fn utf16_strip_bom(bytes: &[u8], default_enc: Encoding) -> (Encoding, &[u8]) {
    if let Some(enc) = utf16_detect_bom(bytes) {
        (enc, &bytes[2..])
    } else {
        (default_enc, bytes)
    }
}

/// Convert to uppercase using Unicode case mapping.
pub fn utf8_uppercase(s: &str) -> String {
    s.to_uppercase()
}

/// Convert to lowercase using Unicode case mapping.
pub fn utf8_lowercase(s: &str) -> String {
    s.to_lowercase()
}

/// Case-insensitive comparison using Unicode case mapping.
pub fn utf8_strcasecmp(a: &str, b: &str) -> std::cmp::Ordering {
    a.to_lowercase().cmp(&b.to_lowercase())
}

impl Value {
    /// Get text value encoded in the requested encoding.
    pub fn text_with_encoding(&self, encoding: Encoding) -> Vec<u8> {
        match self {
            Value::Text(s) => match encoding {
                Encoding::Utf8 => s.as_bytes().to_vec(),
                Encoding::Utf16le | Encoding::Utf16be => utf8_to_utf16(s.as_bytes(), encoding),
            },
            Value::Blob(b) => b.clone(),
            _ => utf8_to_utf16(self.to_text().as_bytes(), encoding),
        }
    }

    /// Set text value from a byte buffer in the specified encoding.
    pub fn set_text_with_encoding(&mut self, bytes: &[u8], encoding: Encoding) -> Result<()> {
        let text = match encoding {
            Encoding::Utf8 => String::from_utf8_lossy(bytes).into_owned(),
            Encoding::Utf16le | Encoding::Utf16be => {
                let (enc, data) = utf16_strip_bom(bytes, encoding);
                let decoded = utf16_to_utf8(data, enc);
                String::from_utf8_lossy(&decoded).into_owned()
            }
        };

        *self = Value::Text(text);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utf8_read_basic() {
        let bytes = "a\u{07FF}\u{1F600}".as_bytes();
        let mut pos = 0;
        assert_eq!(utf8_read(bytes, &mut pos), b'a' as u32);
        assert_eq!(utf8_read(bytes, &mut pos), 0x07FF);
        assert_eq!(utf8_read(bytes, &mut pos), 0x1F600);
    }

    #[test]
    fn test_utf16_roundtrip() {
        let mut out = Vec::new();
        utf16le_write(&mut out, 0x1F600);
        let mut pos = 0;
        let cp = utf16le_read(&out, &mut pos);
        assert_eq!(cp, 0x1F600);
    }

    #[test]
    fn test_utf16_bom_strip() {
        let bytes = [0xFF, 0xFE, 0x61, 0x00];
        let (enc, rest) = utf16_strip_bom(&bytes, Encoding::Utf16be);
        assert_eq!(enc, Encoding::Utf16le);
        assert_eq!(rest, &[0x61, 0x00]);
    }

    #[test]
    fn test_utf8_char_len() {
        let bytes = "a\u{07FF}\u{1F600}".as_bytes();
        assert_eq!(utf8_char_len(bytes, -1), 3);
    }
}
