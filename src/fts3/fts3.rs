use std::collections::BTreeMap;

use crate::api::SqliteConnection;
use crate::error::{Error, ErrorCode, Result};

use super::fts3_write::{LeafNode, PendingTerms};

pub const FTS3_POS_END: u32 = 0;
pub const FTS3_POS_COLUMN: u32 = 1;
pub const FTS3_LEAF_MAX: usize = 2048;

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
                by_column.entry(pos.column).or_default().push(pos.offset);
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
pub struct Fts3Segment {
    pub level: i32,
    pub idx: i32,
    pub leaves: Vec<Vec<u8>>,
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
    pub segments: Vec<Fts3Segment>,
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
            segments: Vec::new(),
        }
    }

    pub fn tokenize(&self, text: &str) -> Result<Vec<String>> {
        self.tokenizer.tokenize(text)
    }

    pub fn insert(&mut self, rowid: i64, values: &[&str]) -> Result<()> {
        let mut pending = PendingTerms::new();
        for (col_idx, value) in values.iter().enumerate() {
            let tokens = self.tokenize(value)?;
            for (pos, token) in tokens.into_iter().enumerate() {
                pending.add(&token, rowid, col_idx as i32, pos as i32);
            }
        }
        self.flush_pending(pending)?;
        Ok(())
    }

    pub fn delete(&mut self, rowid: i64, values: &[&str]) -> Result<()> {
        let mut pending = PendingTerms::new();
        for value in values {
            let tokens = self.tokenize(value)?;
            for token in tokens {
                pending.add_delete(&token, rowid);
            }
        }
        self.flush_pending(pending)?;
        Ok(())
    }

    pub fn query(&self, expr: &str) -> Result<Fts3Cursor> {
        let parsed = self.parse_query(expr)?;
        let doclist = self.evaluate_expr(&parsed)?;
        let eof = doclist.data.is_empty();
        Ok(Fts3Cursor {
            expr: Some(parsed),
            doclist: Some(doclist),
            rowid: 0,
            eof,
        })
    }

    fn flush_pending(&mut self, pending: PendingTerms) -> Result<()> {
        let items = pending.into_sorted_doclists();
        if items.is_empty() {
            return Ok(());
        }

        let mut leaves = Vec::new();
        let mut current = LeafNode::new();
        for (term, doclist) in items {
            if current.encoded_len_with(&term, &doclist) > FTS3_LEAF_MAX && !current.is_empty() {
                leaves.push(current.encode());
                current = LeafNode::new();
            }
            current.add_term(&term, &doclist);
        }
        if !current.is_empty() {
            leaves.push(current.encode());
        }

        let idx = self.next_segment_idx(0);
        self.segments.push(Fts3Segment {
            level: 0,
            idx,
            leaves,
        });
        Ok(())
    }

    fn next_segment_idx(&self, level: i32) -> i32 {
        let mut max_idx = -1;
        for seg in &self.segments {
            if seg.level == level && seg.idx > max_idx {
                max_idx = seg.idx;
            }
        }
        max_idx + 1
    }

    fn parse_query(&self, expr: &str) -> Result<Fts3Expr> {
        let expr = expr.trim();
        if expr.is_empty() {
            return Err(Error::with_message(ErrorCode::Error, "empty query"));
        }

        if let Some((left, right)) = split_keyword(expr, " OR ") {
            return Ok(Fts3Expr::Or(
                Box::new(self.parse_query(left)?),
                Box::new(self.parse_query(right)?),
            ));
        }
        if let Some((left, right)) = split_keyword(expr, " AND ") {
            return Ok(Fts3Expr::And(
                Box::new(self.parse_query(left)?),
                Box::new(self.parse_query(right)?),
            ));
        }
        if let Some((left, right)) = split_keyword(expr, " NOT ") {
            return Ok(Fts3Expr::Not(
                Box::new(self.parse_query(left)?),
                Box::new(self.parse_query(right)?),
            ));
        }

        let tokens: Vec<&str> = expr.split_whitespace().collect();
        if tokens.len() > 1 {
            return Ok(Fts3Expr::Phrase(
                tokens.iter().map(|t| t.to_string()).collect(),
            ));
        }

        Ok(Fts3Expr::Term(expr.to_string()))
    }

    fn evaluate_expr(&self, expr: &Fts3Expr) -> Result<Fts3Doclist> {
        match expr {
            Fts3Expr::Term(term) => self.lookup_term(term),
            Fts3Expr::Phrase(terms) => self.lookup_phrase(terms),
            Fts3Expr::And(left, right) => {
                let left = self.evaluate_expr(left)?;
                let right = self.evaluate_expr(right)?;
                Ok(intersect_doclists(&left, &right))
            }
            Fts3Expr::Or(left, right) => {
                let left = self.evaluate_expr(left)?;
                let right = self.evaluate_expr(right)?;
                Ok(union_doclists(&left, &right))
            }
            Fts3Expr::Not(left, right) => {
                let left = self.evaluate_expr(left)?;
                let right = self.evaluate_expr(right)?;
                Ok(except_doclists(&left, &right))
            }
        }
    }

    fn lookup_term(&self, term: &str) -> Result<Fts3Doclist> {
        let term_bytes = term.as_bytes();
        let mut segments: Vec<&Fts3Segment> = self.segments.iter().collect();
        segments.sort_by(|a, b| b.level.cmp(&a.level).then(a.idx.cmp(&b.idx)));

        let mut doclists = Vec::new();
        for segment in segments {
            for leaf in &segment.leaves {
                if let Some(doclist) = leaf_find_term(leaf, term_bytes) {
                    doclists.push(doclist);
                }
            }
        }

        Ok(merge_doclists(doclists))
    }

    fn lookup_phrase(&self, terms: &[String]) -> Result<Fts3Doclist> {
        let mut iter = terms.iter();
        let Some(first) = iter.next() else {
            return Ok(Fts3Doclist { data: Vec::new() });
        };
        let mut current = self.lookup_term(first)?;
        for term in iter {
            let next = self.lookup_term(term)?;
            current = intersect_doclists(&current, &next);
        }
        Ok(current)
    }
}

fn split_keyword<'a>(expr: &'a str, keyword: &str) -> Option<(&'a str, &'a str)> {
    let upper = expr.to_ascii_uppercase();
    let upper_keyword = keyword.to_ascii_uppercase();
    if let Some(pos) = upper.find(&upper_keyword) {
        let left = expr[..pos].trim();
        let right = expr[pos + keyword.len()..].trim();
        if !left.is_empty() && !right.is_empty() {
            return Some((left, right));
        }
    }
    None
}

fn leaf_find_term(leaf: &[u8], term: &[u8]) -> Option<Vec<u8>> {
    let mut pos = 0usize;
    let (_, n) = fts3_get_varint_u64(leaf)?;
    pos += n;

    let (term_len, n) = fts3_get_varint_u64(&leaf[pos..])?;
    pos += n;
    let term_len = term_len as usize;
    if pos + term_len > leaf.len() {
        return None;
    }
    let mut current_term = leaf[pos..pos + term_len].to_vec();
    pos += term_len;

    let (doclist_len, n) = fts3_get_varint_u64(&leaf[pos..])?;
    pos += n;
    let doclist_len = doclist_len as usize;
    if pos + doclist_len > leaf.len() {
        return None;
    }
    if current_term == term {
        return Some(leaf[pos..pos + doclist_len].to_vec());
    }
    pos += doclist_len;

    while pos < leaf.len() {
        let (prefix_len, n) = fts3_get_varint_u64(&leaf[pos..])?;
        pos += n;
        let (suffix_len, n) = fts3_get_varint_u64(&leaf[pos..])?;
        pos += n;
        let prefix_len = prefix_len as usize;
        let suffix_len = suffix_len as usize;
        if pos + suffix_len > leaf.len() {
            return None;
        }
        current_term.truncate(prefix_len);
        current_term.extend_from_slice(&leaf[pos..pos + suffix_len]);
        pos += suffix_len;

        let (doclist_len, n) = fts3_get_varint_u64(&leaf[pos..])?;
        pos += n;
        let doclist_len = doclist_len as usize;
        if pos + doclist_len > leaf.len() {
            return None;
        }
        if current_term == term {
            return Some(leaf[pos..pos + doclist_len].to_vec());
        }
        pos += doclist_len;
    }

    None
}

fn merge_doclists(doclists: Vec<Vec<u8>>) -> Fts3Doclist {
    let mut merged = BTreeMap::new();
    for doclist in doclists {
        let iter = DoclistIter::new(&doclist);
        for entry in iter {
            let positions = merged.entry(entry.rowid).or_insert_with(Vec::new);
            positions.extend(entry.positions);
        }
    }
    map_to_doclist(merged)
}

fn union_doclists(left: &Fts3Doclist, right: &Fts3Doclist) -> Fts3Doclist {
    let mut merged = doclist_to_map(left);
    for (rowid, positions) in doclist_to_map(right) {
        merged.entry(rowid).or_default().extend(positions);
    }
    map_to_doclist(merged)
}

fn intersect_doclists(left: &Fts3Doclist, right: &Fts3Doclist) -> Fts3Doclist {
    let left_map = doclist_to_map(left);
    let right_map = doclist_to_map(right);
    let mut merged = BTreeMap::new();
    for (rowid, positions) in left_map {
        if let Some(other_positions) = right_map.get(&rowid) {
            let mut combined = positions;
            combined.extend_from_slice(other_positions);
            merged.insert(rowid, combined);
        }
    }
    map_to_doclist(merged)
}

fn except_doclists(left: &Fts3Doclist, right: &Fts3Doclist) -> Fts3Doclist {
    let left_map = doclist_to_map(left);
    let right_map = doclist_to_map(right);
    let merged = left_map
        .into_iter()
        .filter(|(rowid, _)| !right_map.contains_key(rowid))
        .collect::<BTreeMap<_, _>>();
    map_to_doclist(merged)
}

fn doclist_to_map(doclist: &Fts3Doclist) -> BTreeMap<i64, Vec<Fts3Position>> {
    let mut map = BTreeMap::new();
    for entry in doclist.iter() {
        map.entry(entry.rowid)
            .or_insert_with(Vec::new)
            .extend(entry.positions);
    }
    map
}

fn map_to_doclist(mut map: BTreeMap<i64, Vec<Fts3Position>>) -> Fts3Doclist {
    let mut entries = Vec::new();
    for (rowid, mut positions) in map.iter_mut() {
        positions.sort_by(|a, b| a.column.cmp(&b.column).then(a.offset.cmp(&b.offset)));
        entries.push(Fts3DoclistEntry {
            rowid: *rowid,
            positions: positions.clone(),
        });
    }
    Fts3Doclist::encode(&entries)
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
