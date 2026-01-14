use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;

use crate::error::{Error, ErrorCode, Result};

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
            content: HashMap::new(),
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
        table.content_table = content_table;
        table
    }

    pub fn tokenize(&self, text: &str) -> Result<Vec<Fts3Token>> {
        self.tokenizer.tokenize(text)
    }

    pub fn insert(&mut self, rowid: i64, values: &[&str]) -> Result<()> {
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

    fn parse_query(&self, expr: &str) -> Result<Fts3Expr> {
        let expr = expr.trim();
        if expr.is_empty() {
            return Err(Error::with_message(ErrorCode::Error, "empty query"));
        }

        if let Some((left, right, distance)) = split_near(expr) {
            return Ok(Fts3Expr::Near(
                Box::new(self.parse_query(left)?),
                Box::new(self.parse_query(right)?),
                distance,
            ));
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

        if let Some(stripped) = expr.strip_suffix('*') {
            return Ok(Fts3Expr::Prefix(stripped.to_string()));
        }

        Ok(Fts3Expr::Term(expr.to_string()))
    }

    fn evaluate_expr(&self, expr: &Fts3Expr) -> Result<Fts3Doclist> {
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

fn split_near(expr: &str) -> Option<(&str, &str, i32)> {
    let upper = expr.to_ascii_uppercase();
    if let Some(pos) = upper.find(" NEAR/") {
        let left = expr[..pos].trim();
        let rest = &expr[pos + 6..];
        let mut distance_str = String::new();
        for ch in rest.chars() {
            if ch.is_ascii_digit() {
                distance_str.push(ch);
            } else {
                break;
            }
        }
        let distance = distance_str.parse::<i32>().unwrap_or(10);
        let right = rest[distance_str.len()..].trim();
        if !left.is_empty() && !right.is_empty() {
            return Some((left, right, distance));
        }
    }
    if let Some(pos) = upper.find(" NEAR ") {
        let left = expr[..pos].trim();
        let right = expr[pos + 6..].trim();
        if !left.is_empty() && !right.is_empty() {
            return Some((left, right, 10));
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
