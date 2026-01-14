use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use crate::error::{Error, ErrorCode, Result};
use crate::schema::Schema;
use crate::storage::btree::{Btree, BtreeCursorFlags, BtreeInsertFlags, BtreePayload, CursorState};
use crate::vdbe::auxdata::{decode_record_header, deserialize_value, make_record, SerialType};
use crate::vdbe::mem::Mem;

use super::fts3_write::{LeafNode, PendingTerms};

pub const FTS3_POS_END: u32 = 0;
pub const FTS3_POS_COLUMN: u32 = 1;
pub const FTS3_LEAF_MAX: usize = 2048;
pub const FTS3_MERGE_THRESHOLD: usize = 4;

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
    fn tokenize(&self, text: &str) -> Result<Vec<Fts3Token>>;
}

#[derive(Default)]
pub struct SimpleTokenizer;

impl Fts3Tokenizer for SimpleTokenizer {
    fn tokenize(&self, text: &str) -> Result<Vec<Fts3Token>> {
        let mut tokens = Vec::new();
        let mut pos = 0i32;
        let mut idx = 0usize;
        let bytes = text.as_bytes();
        while idx < bytes.len() {
            while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
                idx += 1;
            }
            if idx >= bytes.len() {
                break;
            }
            let start = idx;
            while idx < bytes.len() && !bytes[idx].is_ascii_whitespace() {
                idx += 1;
            }
            let end = idx;
            let token = &text[start..end];
            tokens.push(Fts3Token {
                text: token.to_string(),
                position: pos,
                start,
                end,
            });
            pos += 1;
        }
        Ok(tokens)
    }
}

#[derive(Debug, Clone)]
pub struct Fts3Token {
    pub text: String,
    pub position: i32,
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone)]
pub enum Fts3Expr {
    Term(String),
    Prefix(String),
    Phrase(Vec<String>),
    And(Box<Fts3Expr>, Box<Fts3Expr>),
    Or(Box<Fts3Expr>, Box<Fts3Expr>),
    Not(Box<Fts3Expr>, Box<Fts3Expr>),
    Near(Box<Fts3Expr>, Box<Fts3Expr>, i32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Fts3QueryToken {
    Word(String),
    Phrase(String),
    LParen,
    RParen,
    And,
    Or,
    Not,
    Near(i32),
}

struct Fts3QueryParser<'a> {
    tokenizer: &'a dyn Fts3Tokenizer,
    tokens: Vec<Fts3QueryToken>,
    pos: usize,
}

impl<'a> Fts3QueryParser<'a> {
    fn new(expr: &'a str, tokenizer: &'a dyn Fts3Tokenizer) -> Result<Self> {
        let tokens = tokenize_query(expr)?;
        Ok(Self {
            tokenizer,
            tokens,
            pos: 0,
        })
    }

    fn parse(&mut self) -> Result<Fts3Expr> {
        let expr = self.parse_or()?;
        if self.peek().is_some() {
            return Err(Error::with_message(
                ErrorCode::Error,
                "unexpected token in query",
            ));
        }
        Ok(expr)
    }

    fn parse_or(&mut self) -> Result<Fts3Expr> {
        let mut expr = self.parse_and()?;
        while self.consume_if(&Fts3QueryToken::Or) {
            let right = self.parse_and()?;
            expr = Fts3Expr::Or(Box::new(expr), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<Fts3Expr> {
        let mut expr = self.parse_near()?;
        loop {
            if self.consume_if(&Fts3QueryToken::And) {
                let right = self.parse_near()?;
                expr = Fts3Expr::And(Box::new(expr), Box::new(right));
                continue;
            }
            if self.consume_if(&Fts3QueryToken::Not) {
                let right = self.parse_near()?;
                expr = Fts3Expr::Not(Box::new(expr), Box::new(right));
                continue;
            }
            if self.next_starts_expr() {
                let right = self.parse_near()?;
                expr = Fts3Expr::And(Box::new(expr), Box::new(right));
                continue;
            }
            break;
        }
        Ok(expr)
    }

    fn parse_near(&mut self) -> Result<Fts3Expr> {
        let mut expr = self.parse_primary()?;
        loop {
            let distance = match self.peek() {
                Some(Fts3QueryToken::Near(distance)) => *distance,
                _ => break,
            };
            self.advance();
            let right = self.parse_primary()?;
            expr = Fts3Expr::Near(Box::new(expr), Box::new(right), distance);
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<Fts3Expr> {
        match self.peek() {
            Some(Fts3QueryToken::Word(_)) => {
                let token = self.advance().cloned().unwrap();
                self.parse_word(token)
            }
            Some(Fts3QueryToken::Phrase(_)) => {
                let token = self.advance().cloned().unwrap();
                self.parse_phrase(token)
            }
            Some(Fts3QueryToken::LParen) => {
                self.advance();
                let expr = self.parse_or()?;
                if !self.consume_if(&Fts3QueryToken::RParen) {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "unterminated parenthesis in query",
                    ));
                }
                Ok(expr)
            }
            _ => Err(Error::with_message(
                ErrorCode::Error,
                "expected term in query",
            )),
        }
    }

    fn parse_word(&self, token: Fts3QueryToken) -> Result<Fts3Expr> {
        let Fts3QueryToken::Word(text) = token else {
            return Err(Error::with_message(ErrorCode::Error, "expected word token"));
        };
        if let Some(stripped) = text.strip_suffix('*') {
            if stripped.is_empty() {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "invalid prefix query",
                ));
            }
            let tokens = self.tokenizer.tokenize(stripped)?;
            if tokens.len() != 1 {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "invalid prefix query",
                ));
            }
            return Ok(Fts3Expr::Prefix(tokens[0].text.clone()));
        }

        let tokens = self.tokenizer.tokenize(&text)?;
        if tokens.is_empty() {
            return Err(Error::with_message(
                ErrorCode::Error,
                "invalid term in query",
            ));
        }
        if tokens.len() == 1 {
            Ok(Fts3Expr::Term(tokens[0].text.clone()))
        } else {
            Ok(Fts3Expr::Phrase(
                tokens.into_iter().map(|t| t.text).collect(),
            ))
        }
    }

    fn parse_phrase(&self, token: Fts3QueryToken) -> Result<Fts3Expr> {
        let Fts3QueryToken::Phrase(text) = token else {
            return Err(Error::with_message(
                ErrorCode::Error,
                "expected phrase token",
            ));
        };
        let tokens = self.tokenizer.tokenize(&text)?;
        if tokens.is_empty() {
            return Err(Error::with_message(
                ErrorCode::Error,
                "empty phrase in query",
            ));
        }
        Ok(Fts3Expr::Phrase(
            tokens.into_iter().map(|t| t.text).collect(),
        ))
    }

    fn next_starts_expr(&self) -> bool {
        matches!(
            self.peek(),
            Some(Fts3QueryToken::Word(_))
                | Some(Fts3QueryToken::Phrase(_))
                | Some(Fts3QueryToken::LParen)
        )
    }

    fn peek(&self) -> Option<&Fts3QueryToken> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&Fts3QueryToken> {
        let token = self.tokens.get(self.pos);
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    fn consume_if(&mut self, expected: &Fts3QueryToken) -> bool {
        if self.peek() == Some(expected) {
            self.advance();
            true
        } else {
            false
        }
    }
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
    pub content: HashMap<i64, Vec<String>>,
    pub loaded_from_storage: bool,
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
        let content_table = Some(format!("{}_content", name));
        Self {
            name: name.clone(),
            schema,
            columns,
            tokenizer,
            has_langid: false,
            has_content: true,
            content_table,
            prefixes: Vec::new(),
            index: Fts3Index {
                segments: format!("{}_segments", name),
                segdir: format!("{}_segdir", name),
                stat: format!("{}_stat", name),
            },
            segments: Vec::new(),
            content: HashMap::new(),
            loaded_from_storage: false,
        }
    }

    pub fn from_virtual_spec(
        name: impl Into<String>,
        schema: impl Into<String>,
        args: &[String],
    ) -> Self {
        let name = name.into();
        let schema = schema.into();
        let mut columns = Vec::new();
        let mut prefixes = Vec::new();
        let mut has_content = true;
        let mut content_table = None;

        let mut pending_prefix = false;
        for arg in args {
            let trimmed = arg.trim();
            if let Some(value) = trimmed.strip_prefix("prefix=") {
                prefixes.extend(parse_prefixes(value));
                pending_prefix = true;
            } else if let Some(value) = trimmed.strip_prefix("PREFIX=") {
                prefixes.extend(parse_prefixes(value));
                pending_prefix = true;
            } else if let Some(value) = trimmed.strip_prefix("content=") {
                let value = value.trim();
                if value.eq_ignore_ascii_case("none") {
                    has_content = false;
                    content_table = None;
                } else {
                    has_content = true;
                    content_table = Some(value.to_string());
                }
            } else if let Some(value) = trimmed.strip_prefix("CONTENT=") {
                let value = value.trim();
                if value.eq_ignore_ascii_case("none") {
                    has_content = false;
                    content_table = None;
                } else {
                    has_content = true;
                    content_table = Some(value.to_string());
                }
            } else if trimmed.starts_with("tokenize=") || trimmed.starts_with("TOKENIZE=") {
                // Tokenizer options are parsed elsewhere; default tokenizer for now.
                continue;
            } else if pending_prefix {
                if let Ok(value) = trimmed.parse::<i32>() {
                    prefixes.push(value);
                } else {
                    pending_prefix = false;
                    if !trimmed.contains('=') {
                        columns.push(trimmed.to_string());
                    }
                }
            } else if !trimmed.contains('=') {
                columns.push(trimmed.to_string());
            }
        }

        let mut table = Self::new(name, schema, columns, Box::new(SimpleTokenizer));
        table.prefixes = prefixes;
        table.has_content = has_content;
        if has_content {
            table.content_table = match content_table {
                Some(name) => Some(name),
                None => Some(format!("{}_content", table.name)),
            };
        } else {
            table.content_table = None;
        }
        table
    }

    pub fn tokenize(&self, text: &str) -> Result<Vec<Fts3Token>> {
        self.tokenizer.tokenize(text)
    }

    pub fn insert(&mut self, rowid: i64, values: &[&str]) -> Result<()> {
        if self.has_content && self.content_table.is_none() {
            self.content_table = Some(format!("{}_content", self.name));
        }
        let mut pending = PendingTerms::new();
        for (col_idx, value) in values.iter().enumerate() {
            let tokens = self.tokenize(value)?;
            for token in tokens {
                pending.add(&token.text, rowid, col_idx as i32, token.position);
                for prefix in &self.prefixes {
                    let prefix_len = *prefix as usize;
                    if token.text.len() >= prefix_len {
                        let prefix_term = format!("{}*", &token.text[..prefix_len]);
                        pending.add(&prefix_term, rowid, col_idx as i32, token.position);
                    }
                }
            }
        }
        if self.has_content {
            let values_owned = values.iter().map(|s| s.to_string()).collect();
            self.content.insert(rowid, values_owned);
        }
        self.flush_pending(pending)?;
        Ok(())
    }

    pub fn delete(&mut self, rowid: i64, values: &[&str]) -> Result<()> {
        if self.has_content && self.content_table.is_none() {
            self.content_table = Some(format!("{}_content", self.name));
        }
        let mut pending = PendingTerms::new();
        for value in values {
            let tokens = self.tokenize(value)?;
            for token in tokens {
                pending.add_delete(&token.text, rowid);
                for prefix in &self.prefixes {
                    let prefix_len = *prefix as usize;
                    if token.text.len() >= prefix_len {
                        let prefix_term = format!("{}*", &token.text[..prefix_len]);
                        pending.add_delete(&prefix_term, rowid);
                    }
                }
            }
        }
        if self.has_content {
            self.content.remove(&rowid);
        }
        self.flush_pending(pending)?;
        Ok(())
    }

    pub fn ensure_loaded(&mut self, btree: &Arc<Btree>, schema: &Schema) -> Result<()> {
        if self.loaded_from_storage {
            return Ok(());
        }

        let seg_root = find_table_root(schema, &self.index.segments);
        let dir_root = find_table_root(schema, &self.index.segdir);
        if let (Some(seg_root), Some(dir_root)) = (seg_root, dir_root) {
            self.load_segments(btree, seg_root, dir_root)?;
        }

        if self.has_content {
            if let Some(ref content_table) = self.content_table {
                if let Some(root) = find_table_root(schema, content_table) {
                    self.load_content(btree, root)?;
                }
            }
        }

        self.loaded_from_storage = true;
        Ok(())
    }

    pub fn insert_with_storage(
        &mut self,
        rowid: i64,
        values: &[&str],
        btree: &Arc<Btree>,
        schema: &Schema,
    ) -> Result<()> {
        self.ensure_loaded(btree, schema)?;
        self.insert(rowid, values)?;

        if self.has_content {
            if let Some(ref content_table) = self.content_table {
                if let Some(root) = find_table_root(schema, content_table) {
                    let values_owned: Vec<String> =
                        values.iter().map(|s| (*s).to_string()).collect();
                    self.persist_content_row(btree, root, rowid, &values_owned)?;
                }
            }
        }

        if let (Some(seg_root), Some(dir_root)) = (
            find_table_root(schema, &self.index.segments),
            find_table_root(schema, &self.index.segdir),
        ) {
            let stat_root = find_table_root(schema, &self.index.stat);
            self.persist_segments(btree, seg_root, dir_root, stat_root)?;
        }

        Ok(())
    }

    pub fn delete_with_storage(
        &mut self,
        rowid: i64,
        values: &[&str],
        btree: &Arc<Btree>,
        schema: &Schema,
    ) -> Result<()> {
        self.ensure_loaded(btree, schema)?;
        self.delete(rowid, values)?;

        if self.has_content {
            if let Some(ref content_table) = self.content_table {
                if let Some(root) = find_table_root(schema, content_table) {
                    self.delete_content_row(btree, root, rowid)?;
                }
            }
        }

        if let (Some(seg_root), Some(dir_root)) = (
            find_table_root(schema, &self.index.segments),
            find_table_root(schema, &self.index.segdir),
        ) {
            let stat_root = find_table_root(schema, &self.index.stat);
            self.persist_segments(btree, seg_root, dir_root, stat_root)?;
        }

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

    pub fn row_values(&self, rowid: i64) -> Option<&[String]> {
        if !self.has_content {
            return None;
        }
        self.content.get(&rowid).map(|values| values.as_slice())
    }

    pub fn load_row_values(
        &mut self,
        btree: &Arc<Btree>,
        schema: &Schema,
        rowid: i64,
    ) -> Result<Option<Vec<String>>> {
        if !self.has_content {
            return Ok(None);
        }
        if let Some(values) = self.content.get(&rowid) {
            return Ok(Some(values.clone()));
        }
        let Some(ref content_table) = self.content_table else {
            return Ok(None);
        };
        let Some(root) = find_table_root(schema, content_table) else {
            return Ok(None);
        };
        if let Some(values) = load_content_row(btree, root, rowid, self.columns.len())? {
            self.content.insert(rowid, values.clone());
            return Ok(Some(values));
        }
        Ok(None)
    }

    pub fn all_rowids(&self) -> Vec<i64> {
        if !self.has_content {
            return Vec::new();
        }
        let mut rowids: Vec<i64> = self.content.keys().copied().collect();
        rowids.sort_unstable();
        rowids
    }

    pub fn query_rowids(&self, expr: &str) -> Result<Vec<i64>> {
        let parsed = self.parse_query(expr)?;
        let doclist = self.evaluate_expr(&parsed)?;
        let mut rowids = Vec::new();
        for entry in doclist.iter() {
            rowids.push(entry.rowid);
        }
        Ok(rowids)
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
        self.maybe_merge()?;
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

    pub fn parse_query(&self, expr: &str) -> Result<Fts3Expr> {
        let expr = expr.trim();
        if expr.is_empty() {
            return Err(Error::with_message(ErrorCode::Error, "empty query"));
        }

        let mut parser = Fts3QueryParser::new(expr, self.tokenizer.as_ref())?;
        let parsed = parser.parse()?;
        Ok(parsed)
    }

    pub fn evaluate_expr(&self, expr: &Fts3Expr) -> Result<Fts3Doclist> {
        match expr {
            Fts3Expr::Term(term) => self.lookup_term(term),
            Fts3Expr::Prefix(prefix) => self.lookup_prefix(prefix),
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
            Fts3Expr::Near(left, right, distance) => {
                let left = self.evaluate_expr(left)?;
                let right = self.evaluate_expr(right)?;
                Ok(near_merge_doclists(&left, &right, *distance))
            }
        }
    }

    pub fn matchinfo(&self, query: &str, rowid: i64) -> Result<Vec<u8>> {
        let expr = self.parse_query(query)?;
        let mut phrases = Vec::new();
        collect_phrase_exprs(&expr, &mut phrases);

        let n_phrase = phrases.len() as u32;
        let n_col = self.columns.len() as u32;

        let mut buf = Vec::new();
        buf.extend_from_slice(&n_phrase.to_le_bytes());
        buf.extend_from_slice(&n_col.to_le_bytes());

        for phrase in phrases {
            let doclist = self.evaluate_expr(&phrase)?;
            for col in 0..n_col {
                let stats = matchinfo_stats(&doclist, col as i32, rowid);
                buf.extend_from_slice(&stats.hits_this_row.to_le_bytes());
                buf.extend_from_slice(&stats.hits_all.to_le_bytes());
                buf.extend_from_slice(&stats.docs_with_hits.to_le_bytes());
            }
        }

        Ok(buf)
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

    fn lookup_prefix(&self, prefix: &str) -> Result<Fts3Doclist> {
        let prefix_len = prefix.len() as i32;
        if self.prefixes.contains(&prefix_len) {
            let prefix_term = format!("{}*", prefix);
            return self.lookup_term(&prefix_term);
        }

        let mut doclists = Vec::new();
        for segment in &self.segments {
            for leaf in &segment.leaves {
                let Some(terms) = leaf_terms(leaf) else {
                    continue;
                };
                for (term, doclist) in terms {
                    if term.starts_with(prefix.as_bytes()) {
                        doclists.push(doclist);
                    }
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
            current = phrase_merge_doclists(&current, &next, 1);
        }
        Ok(current)
    }

    fn maybe_merge(&mut self) -> Result<()> {
        let mut level = 0;
        loop {
            let count = self
                .segments
                .iter()
                .filter(|seg| seg.level == level)
                .count();
            if count < FTS3_MERGE_THRESHOLD {
                break;
            }
            self.merge_level(level)?;
            level += 1;
        }
        Ok(())
    }

    fn merge_level(&mut self, level: i32) -> Result<()> {
        let mut segments: Vec<Fts3Segment> = self
            .segments
            .iter()
            .filter(|seg| seg.level == level)
            .cloned()
            .collect();
        if segments.is_empty() {
            return Ok(());
        }
        segments.sort_by(|a, b| a.idx.cmp(&b.idx));

        let mut term_doclists: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();
        for segment in &segments {
            for leaf in &segment.leaves {
                let Some(terms) = leaf_terms(leaf) else {
                    continue;
                };
                for (term, doclist) in terms {
                    term_doclists.entry(term).or_default().push(doclist);
                }
            }
        }

        let mut merged_terms: Vec<(Vec<u8>, Vec<u8>)> = term_doclists
            .into_iter()
            .map(|(term, doclists)| (term, merge_doclists(doclists).data))
            .collect();
        merged_terms.sort_by(|a, b| a.0.cmp(&b.0));

        let mut leaves = Vec::new();
        let mut current = LeafNode::new();
        for (term, doclist) in merged_terms {
            if current.encoded_len_with(&term, &doclist) > FTS3_LEAF_MAX && !current.is_empty() {
                leaves.push(current.encode());
                current = LeafNode::new();
            }
            current.add_term(&term, &doclist);
        }
        if !current.is_empty() {
            leaves.push(current.encode());
        }

        let new_idx = self.next_segment_idx(level + 1);
        let new_segment = Fts3Segment {
            level: level + 1,
            idx: new_idx,
            leaves,
        };

        self.segments
            .retain(|seg| seg.level != level || !segments.iter().any(|s| s.idx == seg.idx));
        self.segments.push(new_segment);
        Ok(())
    }

    fn load_content(&mut self, btree: &Arc<Btree>, root_page: u32) -> Result<()> {
        let rows = scan_table(btree, root_page, self.columns.len() + 1)?;
        self.content.clear();

        for (rowid, values) in rows {
            let docid = values.get(0).map(|value| value.to_int()).unwrap_or(rowid);
            let mut cols = Vec::with_capacity(self.columns.len());
            for idx in 0..self.columns.len() {
                let value = values.get(idx + 1).map_or(String::new(), Mem::to_str);
                cols.push(value);
            }
            self.content.insert(docid, cols);
        }

        Ok(())
    }

    fn load_segments(&mut self, btree: &Arc<Btree>, seg_root: u32, dir_root: u32) -> Result<()> {
        let mut block_map: BTreeMap<i64, Vec<u8>> = BTreeMap::new();
        let segment_rows = scan_table(btree, seg_root, 2)?;
        for (rowid, values) in segment_rows {
            let blockid = values.get(0).map(|value| value.to_int()).unwrap_or(rowid);
            let block = values
                .get(1)
                .map(|value| value.to_blob())
                .unwrap_or_default();
            block_map.insert(blockid, block);
        }

        let mut segments = Vec::new();
        let segdir_rows = scan_table(btree, dir_root, 6)?;
        for (_rowid, values) in segdir_rows {
            let level = values.get(0).map_or(0, Mem::to_int) as i32;
            let idx = values.get(1).map_or(0, Mem::to_int) as i32;
            let start_block = values.get(2).map_or(0, Mem::to_int);
            let leaves_end_block = values.get(3).map_or(start_block, Mem::to_int);
            let _end_block = values.get(4).map_or(leaves_end_block, Mem::to_int);

            let mut leaves = Vec::new();
            if start_block > 0 && leaves_end_block >= start_block {
                for blockid in start_block..=leaves_end_block {
                    if let Some(block) = block_map.get(&blockid) {
                        leaves.push(block.clone());
                    }
                }
            }

            if !leaves.is_empty() {
                segments.push(Fts3Segment { level, idx, leaves });
            }
        }

        self.segments = segments;
        Ok(())
    }

    fn persist_content_row(
        &self,
        btree: &Arc<Btree>,
        root_page: u32,
        rowid: i64,
        values: &[String],
    ) -> Result<()> {
        let mut cursor = btree.cursor(root_page, BtreeCursorFlags::WRCSR, None)?;
        if cursor.table_moveto(rowid, false)? == 0 {
            btree.delete(&mut cursor, BtreeInsertFlags::empty())?;
        }

        let mut mems = Vec::with_capacity(values.len() + 1);
        mems.push(Mem::from_int(rowid));
        for value in values {
            mems.push(Mem::from_str(value));
        }

        let record = make_record(&mems, 0, mems.len() as i32);
        let payload = BtreePayload {
            key: None,
            n_key: rowid,
            data: Some(record.clone()),
            mem: Vec::new(),
            n_data: record.len() as i32,
            n_zero: 0,
        };
        btree.insert(&mut cursor, &payload, BtreeInsertFlags::empty(), 0)?;
        Ok(())
    }

    fn delete_content_row(&self, btree: &Arc<Btree>, root_page: u32, rowid: i64) -> Result<()> {
        let mut cursor = btree.cursor(root_page, BtreeCursorFlags::WRCSR, None)?;
        if cursor.table_moveto(rowid, false)? == 0 {
            btree.delete(&mut cursor, BtreeInsertFlags::empty())?;
        }
        Ok(())
    }

    fn persist_segments(
        &self,
        btree: &Arc<Btree>,
        seg_root: u32,
        dir_root: u32,
        stat_root: Option<u32>,
    ) -> Result<()> {
        clear_table_by_scan(btree, seg_root)?;
        clear_table_by_scan(btree, dir_root)?;
        if let Some(stat_root) = stat_root {
            clear_table_by_scan(btree, stat_root)?;
        }

        let mut blockid = 1i64;
        let mut segdir_rowid = 1i64;
        let mut seg_cursor = btree.cursor(seg_root, BtreeCursorFlags::WRCSR, None)?;
        let mut dir_cursor = btree.cursor(dir_root, BtreeCursorFlags::WRCSR, None)?;

        for segment in &self.segments {
            if segment.leaves.is_empty() {
                continue;
            }
            let start_block = blockid;
            for leaf in &segment.leaves {
                let mems = vec![Mem::from_int(blockid), Mem::from_blob(leaf)];
                let record = make_record(&mems, 0, mems.len() as i32);
                let payload = BtreePayload {
                    key: None,
                    n_key: blockid,
                    data: Some(record.clone()),
                    mem: Vec::new(),
                    n_data: record.len() as i32,
                    n_zero: 0,
                };
                btree.insert(&mut seg_cursor, &payload, BtreeInsertFlags::empty(), 0)?;
                blockid += 1;
            }

            let end_block = blockid - 1;
            let leaves_end_block = end_block;
            let root = Vec::new();
            let mems = vec![
                Mem::from_int(segment.level as i64),
                Mem::from_int(segment.idx as i64),
                Mem::from_int(start_block),
                Mem::from_int(leaves_end_block),
                Mem::from_int(end_block),
                Mem::from_blob(&root),
            ];
            let record = make_record(&mems, 0, mems.len() as i32);
            let payload = BtreePayload {
                key: None,
                n_key: segdir_rowid,
                data: Some(record.clone()),
                mem: Vec::new(),
                n_data: record.len() as i32,
                n_zero: 0,
            };
            btree.insert(&mut dir_cursor, &payload, BtreeInsertFlags::empty(), 0)?;
            segdir_rowid += 1;
        }

        if let Some(stat_root) = stat_root {
            let mut stat_cursor = btree.cursor(stat_root, BtreeCursorFlags::WRCSR, None)?;
            let mut stat_value = Vec::new();
            fts3_put_varint_u64(&mut stat_value, self.content.len() as u64);
            fts3_put_varint_u64(&mut stat_value, self.columns.len() as u64);
            let mems = vec![Mem::from_int(0), Mem::from_blob(&stat_value)];
            let record = make_record(&mems, 0, mems.len() as i32);
            let payload = BtreePayload {
                key: None,
                n_key: 0,
                data: Some(record.clone()),
                mem: Vec::new(),
                n_data: record.len() as i32,
                n_zero: 0,
            };
            btree.insert(&mut stat_cursor, &payload, BtreeInsertFlags::empty(), 0)?;
        }

        Ok(())
    }
}

fn find_table_root(schema: &Schema, name: &str) -> Option<u32> {
    schema
        .tables
        .get(&name.to_ascii_lowercase())
        .map(|table| table.root_page)
}

fn scan_table(
    btree: &Arc<Btree>,
    root_page: u32,
    expected_cols: usize,
) -> Result<Vec<(i64, Vec<Mem>)>> {
    let mut cursor = btree.cursor(root_page, BtreeCursorFlags::empty(), None)?;
    let empty = cursor.first()?;
    if empty {
        return Ok(Vec::new());
    }

    let mut rows = Vec::new();
    loop {
        let rowid = cursor.n_key;
        let payload = cursor.info.payload.clone().unwrap_or_default();
        let values = decode_record_values(&payload, expected_cols)?;
        rows.push((rowid, values));

        cursor.next(0)?;
        if cursor.state != CursorState::Valid {
            break;
        }
    }

    Ok(rows)
}

fn load_content_row(
    btree: &Arc<Btree>,
    root_page: u32,
    rowid: i64,
    column_count: usize,
) -> Result<Option<Vec<String>>> {
    let mut cursor = btree.cursor(root_page, BtreeCursorFlags::empty(), None)?;
    if cursor.table_moveto(rowid, false)? != 0 {
        return Ok(None);
    }

    let payload = cursor.info.payload.clone().unwrap_or_default();
    let values = decode_record_values(&payload, column_count + 1)?;
    let mut cols = Vec::with_capacity(column_count);
    for idx in 0..column_count {
        let value = values.get(idx + 1).map_or(String::new(), Mem::to_str);
        cols.push(value);
    }
    Ok(Some(cols))
}

fn decode_record_values(payload: &[u8], expected_cols: usize) -> Result<Vec<Mem>> {
    if payload.is_empty() {
        return Ok(vec![Mem::new(); expected_cols]);
    }

    let (types, header_size) = decode_record_header(payload)?;
    let mut offset = header_size;
    let mut values = Vec::with_capacity(expected_cols.max(types.len()));

    for serial_type in types {
        let size = match serial_type {
            SerialType::Blob(n) | SerialType::Text(n) => n as usize,
            _ => serial_type.size(),
        };
        if offset + size > payload.len() {
            return Err(Error::with_message(
                ErrorCode::Corrupt,
                "record payload truncated",
            ));
        }
        let mem = deserialize_value(&payload[offset..offset + size], &serial_type)?;
        values.push(mem);
        offset += size;
    }

    if expected_cols > values.len() {
        values.resize_with(expected_cols, Mem::new);
    }

    Ok(values)
}

fn clear_table_by_scan(btree: &Arc<Btree>, root_page: u32) -> Result<()> {
    let mut cursor = btree.cursor(root_page, BtreeCursorFlags::WRCSR, None)?;
    loop {
        let empty = cursor.first()?;
        if empty {
            break;
        }
        btree.delete(&mut cursor, BtreeInsertFlags::empty())?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct MatchStats {
    hits_this_row: u32,
    hits_all: u32,
    docs_with_hits: u32,
}

fn collect_phrase_exprs(expr: &Fts3Expr, out: &mut Vec<Fts3Expr>) {
    match expr {
        Fts3Expr::Term(_) | Fts3Expr::Prefix(_) | Fts3Expr::Phrase(_) => out.push(expr.clone()),
        Fts3Expr::And(left, right)
        | Fts3Expr::Or(left, right)
        | Fts3Expr::Not(left, right)
        | Fts3Expr::Near(left, right, _) => {
            collect_phrase_exprs(left, out);
            collect_phrase_exprs(right, out);
        }
    }
}

fn matchinfo_stats(doclist: &Fts3Doclist, column: i32, rowid: i64) -> MatchStats {
    let mut hits_this_row = 0usize;
    let mut hits_all = 0usize;
    let mut docs_with_hits = 0usize;

    for entry in doclist.iter() {
        let mut hits_in_row = 0usize;
        for pos in entry.positions {
            if pos.column == column {
                hits_in_row += 1;
            }
        }

        if entry.rowid == rowid {
            hits_this_row = hits_in_row;
        }

        if hits_in_row > 0 {
            hits_all += hits_in_row;
            docs_with_hits += 1;
        }
    }

    MatchStats {
        hits_this_row: hits_this_row.min(u32::MAX as usize) as u32,
        hits_all: hits_all.min(u32::MAX as usize) as u32,
        docs_with_hits: docs_with_hits.min(u32::MAX as usize) as u32,
    }
}

fn tokenize_query(expr: &str) -> Result<Vec<Fts3QueryToken>> {
    let mut tokens = Vec::new();
    let mut iter = expr.char_indices().peekable();

    while let Some((_, ch)) = iter.next() {
        if ch.is_ascii_whitespace() {
            continue;
        }

        if ch == '(' {
            tokens.push(Fts3QueryToken::LParen);
            continue;
        }
        if ch == ')' {
            tokens.push(Fts3QueryToken::RParen);
            continue;
        }
        if ch == '"' {
            let mut phrase = String::new();
            let mut closed = false;
            while let Some((_, ch)) = iter.next() {
                if ch == '"' {
                    if let Some((_, next_ch)) = iter.peek() {
                        if *next_ch == '"' {
                            iter.next();
                            phrase.push('"');
                            continue;
                        }
                    }
                    closed = true;
                    break;
                }
                phrase.push(ch);
            }
            if !closed {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "unterminated phrase in query",
                ));
            }
            tokens.push(Fts3QueryToken::Phrase(phrase));
            continue;
        }

        let mut word = String::new();
        word.push(ch);
        while let Some((_, next_ch)) = iter.peek() {
            if next_ch.is_ascii_whitespace()
                || *next_ch == '('
                || *next_ch == ')'
                || *next_ch == '"'
            {
                break;
            }
            word.push(*next_ch);
            iter.next();
        }

        let upper = word.to_ascii_uppercase();
        if upper == "AND" {
            tokens.push(Fts3QueryToken::And);
        } else if upper == "OR" {
            tokens.push(Fts3QueryToken::Or);
        } else if upper == "NOT" {
            tokens.push(Fts3QueryToken::Not);
        } else if upper == "NEAR" {
            tokens.push(Fts3QueryToken::Near(10));
        } else if let Some(distance_str) = upper.strip_prefix("NEAR/") {
            if !distance_str.is_empty() && distance_str.chars().all(|c| c.is_ascii_digit()) {
                let distance = distance_str.parse::<i32>().unwrap_or(10);
                tokens.push(Fts3QueryToken::Near(distance));
            } else {
                tokens.push(Fts3QueryToken::Word(word));
            }
        } else {
            tokens.push(Fts3QueryToken::Word(word));
        }
    }

    Ok(tokens)
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

fn leaf_terms(leaf: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
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

    let mut terms = Vec::new();
    terms.push((current_term.clone(), leaf[pos..pos + doclist_len].to_vec()));
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
        terms.push((current_term.clone(), leaf[pos..pos + doclist_len].to_vec()));
        pos += doclist_len;
    }

    Some(terms)
}

fn merge_doclists(doclists: Vec<Vec<u8>>) -> Fts3Doclist {
    let mut merged = BTreeMap::new();
    for doclist in doclists {
        let iter = DoclistIter::new(&doclist);
        for entry in iter {
            if entry.positions.is_empty() {
                merged.remove(&entry.rowid);
            } else {
                let positions = merged.entry(entry.rowid).or_insert_with(Vec::new);
                positions.extend(entry.positions);
            }
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

fn phrase_merge_doclists(left: &Fts3Doclist, right: &Fts3Doclist, distance: i32) -> Fts3Doclist {
    let left_map = doclist_to_map(left);
    let right_map = doclist_to_map(right);
    let mut merged = BTreeMap::new();

    for (rowid, left_positions) in left_map {
        let Some(right_positions) = right_map.get(&rowid) else {
            continue;
        };

        let left_by_column = positions_by_column(&left_positions);
        let right_by_column = positions_by_column(right_positions);

        let mut matched = Vec::new();
        for (column, right_offsets) in right_by_column {
            let Some(left_offsets) = left_by_column.get(&column) else {
                continue;
            };
            let left_set: HashSet<i32> = left_offsets.iter().copied().collect();
            for offset in right_offsets {
                if left_set.contains(&(offset - distance)) {
                    matched.push(Fts3Position { column, offset });
                }
            }
        }

        if !matched.is_empty() {
            merged.insert(rowid, matched);
        }
    }

    map_to_doclist(merged)
}

fn near_merge_doclists(left: &Fts3Doclist, right: &Fts3Doclist, distance: i32) -> Fts3Doclist {
    let left_map = doclist_to_map(left);
    let right_map = doclist_to_map(right);
    let mut merged = BTreeMap::new();

    for (rowid, left_positions) in left_map {
        let Some(right_positions) = right_map.get(&rowid) else {
            continue;
        };

        let left_by_column = positions_by_column(&left_positions);
        let right_by_column = positions_by_column(right_positions);

        let mut matched = Vec::new();
        for (column, right_offsets) in right_by_column {
            let Some(left_offsets) = left_by_column.get(&column) else {
                continue;
            };
            for offset in right_offsets {
                if left_offsets
                    .iter()
                    .any(|left_offset| (offset - left_offset).abs() <= distance)
                {
                    matched.push(Fts3Position { column, offset });
                }
            }
        }

        if !matched.is_empty() {
            merged.insert(rowid, matched);
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

fn positions_by_column(positions: &[Fts3Position]) -> HashMap<i32, Vec<i32>> {
    let mut map: HashMap<i32, Vec<i32>> = HashMap::new();
    for pos in positions {
        map.entry(pos.column).or_default().push(pos.offset);
    }
    for offsets in map.values_mut() {
        offsets.sort_unstable();
    }
    map
}

fn map_to_doclist(mut map: BTreeMap<i64, Vec<Fts3Position>>) -> Fts3Doclist {
    let mut entries = Vec::new();
    for (rowid, positions) in map.iter_mut() {
        positions.sort_by(|a, b| a.column.cmp(&b.column).then(a.offset.cmp(&b.offset)));
        entries.push(Fts3DoclistEntry {
            rowid: *rowid,
            positions: positions.clone(),
        });
    }
    Fts3Doclist::encode(&entries)
}

fn parse_prefixes(value: &str) -> Vec<i32> {
    value
        .split(',')
        .filter_map(|part| part.trim().parse::<i32>().ok())
        .collect()
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
    fn test_phrase_merge() {
        let left = Fts3Doclist::encode(&[Fts3DoclistEntry {
            rowid: 1,
            positions: vec![
                Fts3Position {
                    column: 0,
                    offset: 2,
                },
                Fts3Position {
                    column: 0,
                    offset: 5,
                },
            ],
        }]);
        let right = Fts3Doclist::encode(&[Fts3DoclistEntry {
            rowid: 1,
            positions: vec![
                Fts3Position {
                    column: 0,
                    offset: 3,
                },
                Fts3Position {
                    column: 0,
                    offset: 6,
                },
            ],
        }]);

        let merged = phrase_merge_doclists(&left, &right, 1);
        let entries: Vec<Fts3DoclistEntry> = merged.iter().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].rowid, 1);
        assert_eq!(entries[0].positions.len(), 2);
    }

    #[test]
    fn test_prefix_query() {
        let mut table = Fts3Table::new(
            "docs",
            "main",
            vec!["body".to_string()],
            Box::new(SimpleTokenizer),
        );
        table.prefixes = vec![2];
        table.insert(1, &["hello world"]).expect("insert");
        let rows = table.query_rowids("he*").expect("query");
        assert_eq!(rows, vec![1]);
    }

    #[test]
    fn test_segment_merge() {
        let mut table = Fts3Table::new(
            "docs",
            "main",
            vec!["body".to_string()],
            Box::new(SimpleTokenizer),
        );
        for i in 0..FTS3_MERGE_THRESHOLD {
            let rowid = i as i64 + 1;
            let text = format!("term{}", i);
            table.insert(rowid, &[text.as_str()]).expect("insert");
        }
        assert!(table.segments.iter().any(|seg| seg.level == 1));
    }

    #[test]
    fn test_near_query() {
        let mut table = Fts3Table::new(
            "docs",
            "main",
            vec!["body".to_string()],
            Box::new(SimpleTokenizer),
        );
        table.insert(1, &["alpha beta gamma"]).expect("insert");
        let rows = table.query_rowids("alpha NEAR beta").expect("query");
        assert_eq!(rows, vec![1]);
    }

    #[test]
    fn test_fts3_dequote() {
        assert_eq!(fts3_dequote("\"abc\""), "abc");
        assert_eq!(fts3_dequote("'x''y'"), "x'y");
        assert_eq!(fts3_dequote("[name]"), "name");
        assert_eq!(fts3_dequote("plain"), "plain");
    }
}
