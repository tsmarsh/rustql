use std::collections::BTreeMap;

use crate::api::SqliteConnection;
use crate::error::{Error, ErrorCode, Result};

pub const FTS3_POS_END: u32 = 0;
pub const FTS3_POS_COLUMN: u32 = 1;

pub fn fts3_put_varint_u64(buf: &mut Vec<u8>, mut v: u64) -> usize {
    let mut written = 0;
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        written += 1;
        if v == 0 {
            break;
        }
    }
    written
}

pub fn fts3_get_varint_u64(data: &[u8]) -> Option<(u64, usize)> {
    let mut v = 0u64;
    let mut shift = 0u32;
    for (idx, &byte) in data.iter().enumerate() {
        v |= ((byte & 0x7f) as u64) << shift;
        if (byte & 0x80) == 0 {
            return Some((v, idx + 1));
        }
        shift += 7;
        if shift > 63 {
            break;
        }
    }
    None
}

pub fn fts3_varint_len(mut v: u64) -> usize {
    let mut len = 0;
    loop {
        len += 1;
        v >>= 7;
        if v == 0 {
            break;
        }
    }
    len
}

pub fn fts3_dequote(input: &str) -> String {
    let mut chars = input.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let quote = match first {
        '[' => ']',
        '\'' => '\'',
        '"' => '"',
        '`' => '`',
        _ => return input.to_string(),
    };
    let mut out = String::new();
    let mut iter = input[1..].chars().peekable();
    while let Some(ch) = iter.next() {
        if ch == quote {
            if iter.peek() == Some(&quote) {
                out.push(quote);
                iter.next();
            } else {
                break;
            }
        } else {
            out.push(ch);
        }
    }
    out
}

#[derive(Debug, Clone)]
pub struct Fts3Position {
    pub column: i32,
    pub offset: i32,
}

#[derive(Debug, Clone)]
pub struct Fts3DoclistEntry {
    pub rowid: i64,
    pub positions: Vec<Fts3Position>,
}

#[derive(Debug, Clone)]
pub struct Fts3Doclist {
    pub data: Vec<u8>,
}

impl Fts3Doclist {
    pub fn encode(entries: &[Fts3DoclistEntry]) -> Self {
        let mut data = Vec::new();
        let mut prev_rowid = 0i64;

        for entry in entries {
            let delta = entry.rowid - prev_rowid;
            fts3_put_varint_u64(&mut data, delta as u64);
            prev_rowid = entry.rowid;

            let mut by_column: BTreeMap<i32, Vec<i32>> = BTreeMap::new();
            for pos in &entry.positions {
                by_column
                    .entry(pos.column)
                    .or_default()
                    .push(pos.offset);
            }

            let mut first_column = true;
            for (column, mut offsets) in by_column {
                offsets.sort_unstable();
                if !first_column || column != 0 {
                    fts3_put_varint_u64(&mut data, FTS3_POS_COLUMN as u64);
                    fts3_put_varint_u64(&mut data, column as u64);
                }
                first_column = false;

                let mut prev_offset = 0i32;
                for offset in offsets {
                    let delta = offset - prev_offset + 2;
                    fts3_put_varint_u64(&mut data, delta as u64);
                    prev_offset = offset;
                }
            }

            fts3_put_varint_u64(&mut data, FTS3_POS_END as u64);
        }

        Self { data }
    }

    pub fn iter(&self) -> DoclistIter<'_> {
        DoclistIter::new(&self.data)
    }
}

pub struct DoclistIter<'a> {
    data: &'a [u8],
    pos: usize,
    rowid: i64,
}

impl<'a> DoclistIter<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            rowid: 0,
        }
    }
}

impl<'a> Iterator for DoclistIter<'a> {
    type Item = Fts3DoclistEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            return None;
        }

        let (delta, n) = fts3_get_varint_u64(&self.data[self.pos..])?;
        self.pos += n;
        self.rowid += delta as i64;

        let mut positions = Vec::new();
        let mut current_column = 0i32;
        let mut prev_offset = 0i32;

        while self.pos < self.data.len() {
            let (value, n) = fts3_get_varint_u64(&self.data[self.pos..])?;
            self.pos += n;

            if value == FTS3_POS_END as u64 {
                break;
            }

            if value == FTS3_POS_COLUMN as u64 {
                let (col, n) = fts3_get_varint_u64(&self.data[self.pos..])?;
                self.pos += n;
                current_column = col as i32;
                prev_offset = 0;
                continue;
            }

            let offset = prev_offset + (value as i32) - 2;
            positions.push(Fts3Position {
                column: current_column,
                offset,
            });
            prev_offset = offset;
        }

        Some(Fts3DoclistEntry {
            rowid: self.rowid,
            positions,
        })
    }
}

pub trait Fts3Tokenizer: Send + Sync {
    fn tokenize(&self, text: &str) -> Result<Vec<String>>;
}

#[derive(Default)]
pub struct SimpleTokenizer;

impl Fts3Tokenizer for SimpleTokenizer {
    fn tokenize(&self, text: &str) -> Result<Vec<String>> {
        Ok(text
            .split_whitespace()
            .filter(|token| !token.is_empty())
            .map(|token| token.to_string())
            .collect())
    }
}

#[derive(Debug, Clone)]
pub enum Fts3Expr {
    Term(String),
    Phrase(Vec<String>),
    And(Box<Fts3Expr>, Box<Fts3Expr>),
    Or(Box<Fts3Expr>, Box<Fts3Expr>),
    Not(Box<Fts3Expr>, Box<Fts3Expr>),
}

#[derive(Debug, Clone)]
pub struct Fts3Index {
    pub segments: String,
    pub segdir: String,
    pub stat: String,
}

#[derive(Debug, Clone)]
pub struct Fts3Segdir {
    pub level: i32,
    pub idx: i32,
    pub start_block: i64,
    pub leaves_end_block: i64,
    pub end_block: i64,
    pub root: Vec<u8>,
}

pub struct Fts3Table {
    pub db: *mut SqliteConnection,
    pub name: String,
    pub schema: String,
    pub columns: Vec<String>,
    pub tokenizer: Box<dyn Fts3Tokenizer>,
    pub has_langid: bool,
    pub has_content: bool,
    pub content_table: Option<String>,
    pub prefixes: Vec<i32>,
    pub index: Fts3Index,
}

pub struct Fts3Cursor {
    pub expr: Option<Fts3Expr>,
    pub doclist: Option<Fts3Doclist>,
    pub rowid: i64,
    pub eof: bool,
}

impl Fts3Table {
    pub fn new(
        name: impl Into<String>,
        schema: impl Into<String>,
        columns: Vec<String>,
        tokenizer: Box<dyn Fts3Tokenizer>,
    ) -> Self {
        let name = name.into();
        let schema = schema.into();
        Self {
            db: std::ptr::null_mut(),
            name: name.clone(),
            schema,
            columns,
            tokenizer,
            has_langid: false,
            has_content: true,
            content_table: None,
            prefixes: Vec::new(),
            index: Fts3Index {
                segments: format!("{}_segments", name),
                segdir: format!("{}_segdir", name),
                stat: format!("{}_stat", name),
            },
        }
    }

    pub fn tokenize(&self, text: &str) -> Result<Vec<String>> {
        self.tokenizer.tokenize(text)
    }

    pub fn insert(&mut self, _rowid: i64, _values: &[&str]) -> Result<()> {
        Err(Error::with_message(
            ErrorCode::Error,
            "fts3 insert path not implemented",
        ))
    }

    pub fn delete(&mut self, _rowid: i64) -> Result<()> {
        Err(Error::with_message(
            ErrorCode::Error,
            "fts3 delete path not implemented",
        ))
    }

    pub fn query(&self, _expr: &str) -> Result<Fts3Cursor> {
        Err(Error::with_message(
            ErrorCode::Error,
            "fts3 query path not implemented",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fts3_varint_roundtrip() {
        let values = [0u64, 1, 127, 128, 255, 16_383, 1_000_000, u32::MAX as u64];
        for value in values {
            let mut buf = Vec::new();
            fts3_put_varint_u64(&mut buf, value);
            let (decoded, n) = fts3_get_varint_u64(&buf).expect("decode varint");
            assert_eq!(decoded, value);
            assert_eq!(n, buf.len());
        }
    }

    #[test]
    fn test_doclist_encode_decode() {
        let entries = vec![
            Fts3DoclistEntry {
                rowid: 3,
                positions: vec![
                    Fts3Position {
                        column: 0,
                        offset: 1,
                    },
                    Fts3Position {
                        column: 0,
                        offset: 5,
                    },
                ],
            },
            Fts3DoclistEntry {
                rowid: 10,
                positions: vec![Fts3Position {
                    column: 1,
                    offset: 2,
                }],
            },
        ];
        let doclist = Fts3Doclist::encode(&entries);
        let decoded: Vec<Fts3DoclistEntry> = doclist.iter().collect();
        assert_eq!(decoded.len(), entries.len());
        assert_eq!(decoded[0].rowid, entries[0].rowid);
        assert_eq!(decoded[1].rowid, entries[1].rowid);
    }

    #[test]
    fn test_fts3_dequote() {
        assert_eq!(fts3_dequote("\"abc\""), "abc");
        assert_eq!(fts3_dequote("'x''y'"), "x'y");
        assert_eq!(fts3_dequote("[name]"), "name");
        assert_eq!(fts3_dequote("plain"), "plain");
    }
}
