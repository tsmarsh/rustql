use std::collections::{HashMap, HashSet};

use crate::error::{Error, ErrorCode, Result};

use super::expr::{parse_query, Fts5Expr};
use super::index::{Fts5DoclistEntry, Fts5Index, Fts5IndexConfig, Fts5Position};
use super::tokenizer::{create_tokenizer, parse_tokenize_arg, Fts5Tokenizer, SimpleTokenizer};

pub struct Fts5Table {
    pub name: String,
    pub schema: String,
    pub columns: Vec<String>,
    pub tokenizer: Box<dyn Fts5Tokenizer>,
    pub has_content: bool,
    pub content_table: Option<String>,
    pub prefixes: Vec<i32>,
    pub index: Fts5Index,
    pub content: HashMap<i64, Vec<String>>,
}

impl Fts5Table {
    pub fn new(
        name: impl Into<String>,
        schema: impl Into<String>,
        columns: Vec<String>,
        tokenizer: Box<dyn Fts5Tokenizer>,
    ) -> Self {
        let name = name.into();
        let schema = schema.into();
        let content_table = Some(format!("{}_content", name));
        Self {
            name: name.clone(),
            schema,
            columns,
            tokenizer,
            has_content: true,
            content_table,
            prefixes: Vec::new(),
            index: Fts5Index::new(Fts5IndexConfig::default()),
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
        let mut tokenizer: Box<dyn Fts5Tokenizer> = Box::new(SimpleTokenizer::default());

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
            } else if let Some((name, args)) = parse_tokenize_arg(trimmed) {
                let arg_refs: Vec<&str> = args.iter().map(|arg| arg.as_str()).collect();
                if let Ok(tok) = create_tokenizer(&name, &arg_refs) {
                    tokenizer = tok;
                }
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

        let mut table = Self::new(name, schema, columns, tokenizer);
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

    pub fn tokenize(&self, text: &str) -> Result<Vec<super::tokenizer::Fts5Token>> {
        self.tokenizer.tokenize(text)
    }

    pub fn insert(&mut self, rowid: i64, values: &[&str]) -> Result<()> {
        if rowid < 0 {
            return Err(Error::with_message(
                ErrorCode::Corrupt,
                "fts5 rowid must be non-negative",
            ));
        }
        for (col_idx, value) in values.iter().enumerate() {
            let tokens = self.tokenize(value)?;
            for token in tokens {
                let entry = Fts5DoclistEntry {
                    rowid,
                    positions: vec![Fts5Position {
                        column: col_idx as i32,
                        offset: token.position,
                    }],
                    deleted: false,
                };
                self.index.insert_term(token.text.as_bytes(), entry)?;
            }
        }
        if self.has_content {
            let values_owned = values.iter().map(|s| s.to_string()).collect();
            self.content.insert(rowid, values_owned);
        }
        self.index.flush()?;
        Ok(())
    }

    pub fn delete(&mut self, rowid: i64, values: &[&str]) -> Result<()> {
        for value in values {
            let tokens = self.tokenize(value)?;
            for token in tokens {
                let entry = Fts5DoclistEntry {
                    rowid,
                    positions: Vec::new(),
                    deleted: true,
                };
                self.index.insert_term(token.text.as_bytes(), entry)?;
            }
        }
        if self.has_content {
            self.content.remove(&rowid);
        }
        self.index.flush()?;
        Ok(())
    }

    pub fn row_values(&self, rowid: i64) -> Option<&[String]> {
        self.content.get(&rowid).map(|values| values.as_slice())
    }

    pub fn all_rowids(&self) -> Vec<i64> {
        let mut rowids: Vec<i64> = self.content.keys().copied().collect();
        rowids.sort_unstable();
        rowids
    }

    pub fn query_rowids(&self, query: &str) -> Result<Vec<i64>> {
        let expr = parse_query(query, self.tokenizer.as_ref())?;
        self.expr_rowids(&expr)
    }

    fn expr_rowids(&self, expr: &Fts5Expr) -> Result<Vec<i64>> {
        let mut rowids = match expr {
            Fts5Expr::Term(term) => entries_to_rowids(&self.index.lookup_term(term.as_bytes())?),
            Fts5Expr::Prefix(prefix) => {
                entries_to_rowids(&self.index.lookup_prefix(prefix.as_bytes())?)
            }
            Fts5Expr::Phrase(terms) => self.phrase_rowids(terms)?,
            Fts5Expr::Column(name, inner) => self.column_rowids(name, inner)?,
            Fts5Expr::And(left, right) => {
                intersect_rowids(&self.expr_rowids(left)?, &self.expr_rowids(right)?)
            }
            Fts5Expr::Or(left, right) => {
                union_rowids(&self.expr_rowids(left)?, &self.expr_rowids(right)?)
            }
            Fts5Expr::Not(left, right) => {
                subtract_rowids(&self.expr_rowids(left)?, &self.expr_rowids(right)?)
            }
            Fts5Expr::Near(exprs, distance) => self.near_group_rowids(exprs, *distance)?,
        };
        rowids.sort_unstable();
        rowids.dedup();
        Ok(rowids)
    }

    fn phrase_rowids(&self, terms: &[String]) -> Result<Vec<i64>> {
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        let mut term_maps = Vec::new();
        let mut rowid_sets = Vec::new();
        for term in terms {
            let entries = self.index.lookup_term(term.as_bytes())?;
            let map = doclist_to_map(&entries);
            let rowids: Vec<i64> = map.keys().copied().collect();
            term_maps.push(map);
            rowid_sets.push(rowids);
        }
        let mut rowids = rowid_sets
            .into_iter()
            .reduce(|a, b| intersect_rowids(&a, &b))
            .unwrap_or_default();
        rowids.retain(|rowid| {
            let positions = term_maps
                .iter()
                .map(|map| map.get(rowid).cloned().unwrap_or_default())
                .collect::<Vec<_>>();
            !phrase_positions(&positions).is_empty()
        });
        Ok(rowids)
    }

    fn near_group_rowids(&self, exprs: &[Fts5Expr], distance: i32) -> Result<Vec<i64>> {
        if exprs.len() < 2 {
            if let Some(expr) = exprs.first() {
                return self.expr_rowids(expr);
            }
            return Ok(Vec::new());
        }
        let mut rowids = self.expr_rowids(&exprs[0])?;
        for expr in &exprs[1..] {
            rowids = intersect_rowids(&rowids, &self.expr_rowids(expr)?);
        }
        if rowids.is_empty() {
            return Ok(rowids);
        }

        let mut filtered = Vec::new();
        for rowid in rowids.drain(..) {
            let mut positions = Vec::new();
            let mut unsupported = false;
            for expr in exprs {
                match self.expr_positions(expr, rowid)? {
                    Some(pos) => positions.push(pos),
                    None => {
                        unsupported = true;
                        break;
                    }
                }
            }
            if unsupported {
                filtered.push(rowid);
                continue;
            }

            let mut ok = true;
            for pair in positions.windows(2) {
                if !positions_within_distance(&pair[0], &pair[1], distance) {
                    ok = false;
                    break;
                }
            }
            if ok {
                filtered.push(rowid);
            }
        }
        Ok(filtered)
    }

    fn expr_positions(&self, expr: &Fts5Expr, rowid: i64) -> Result<Option<Vec<Fts5Position>>> {
        match expr {
            Fts5Expr::Term(term) => Ok(term_positions(
                &self.index.lookup_term(term.as_bytes())?,
                rowid,
            )),
            Fts5Expr::Prefix(prefix) => Ok(term_positions(
                &self.index.lookup_prefix(prefix.as_bytes())?,
                rowid,
            )),
            Fts5Expr::Phrase(terms) => {
                let mut term_maps = Vec::new();
                for term in terms {
                    let entries = self.index.lookup_term(term.as_bytes())?;
                    term_maps.push(doclist_to_map(&entries));
                }
                let positions = term_maps
                    .iter()
                    .map(|map| map.get(&rowid).cloned().unwrap_or_default())
                    .collect::<Vec<_>>();
                let mut phrase_pos = phrase_positions(&positions);
                if phrase_pos.is_empty() {
                    Ok(None)
                } else {
                    phrase_pos.sort_by(|a, b| (a.column, a.offset).cmp(&(b.column, b.offset)));
                    Ok(Some(phrase_pos))
                }
            }
            Fts5Expr::Column(name, inner) => {
                let Some(column) = self.column_index(name) else {
                    return Ok(None);
                };
                if let Some(mut positions) = self.expr_positions(inner, rowid)? {
                    positions.retain(|pos| pos.column == column);
                    if positions.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(positions))
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn column_rowids(&self, name: &str, inner: &Fts5Expr) -> Result<Vec<i64>> {
        let Some(column) = self.column_index(name) else {
            return Ok(Vec::new());
        };
        let mut rowids = self.expr_rowids(inner)?;
        rowids.retain(|rowid| {
            self.expr_positions(inner, *rowid)
                .ok()
                .flatten()
                .map(|positions| positions.iter().any(|pos| pos.column == column))
                .unwrap_or(false)
        });
        Ok(rowids)
    }

    fn column_index(&self, name: &str) -> Option<i32> {
        self.columns
            .iter()
            .position(|col| col.eq_ignore_ascii_case(name))
            .map(|idx| idx as i32)
    }
}

fn parse_prefixes(value: &str) -> Vec<i32> {
    value
        .split(',')
        .filter_map(|part| part.trim().parse::<i32>().ok())
        .collect()
}

fn entries_to_rowids(entries: &[Fts5DoclistEntry]) -> Vec<i64> {
    let mut rowids = Vec::new();
    for entry in entries {
        if entry.deleted {
            continue;
        }
        rowids.push(entry.rowid);
    }
    rowids
}

fn intersect_rowids(left: &[i64], right: &[i64]) -> Vec<i64> {
    let set: HashSet<i64> = right.iter().copied().collect();
    left.iter()
        .copied()
        .filter(|rowid| set.contains(rowid))
        .collect()
}

fn union_rowids(left: &[i64], right: &[i64]) -> Vec<i64> {
    let mut set: HashSet<i64> = left.iter().copied().collect();
    set.extend(right.iter().copied());
    let mut out: Vec<i64> = set.into_iter().collect();
    out.sort_unstable();
    out
}

fn subtract_rowids(left: &[i64], right: &[i64]) -> Vec<i64> {
    let set: HashSet<i64> = right.iter().copied().collect();
    left.iter()
        .copied()
        .filter(|rowid| !set.contains(rowid))
        .collect()
}

fn doclist_to_map(entries: &[Fts5DoclistEntry]) -> HashMap<i64, Vec<Fts5Position>> {
    let mut map = HashMap::new();
    for entry in entries {
        if entry.deleted {
            map.insert(entry.rowid, Vec::new());
        } else {
            map.insert(entry.rowid, entry.positions.clone());
        }
    }
    map
}

fn term_positions(entries: &[Fts5DoclistEntry], rowid: i64) -> Option<Vec<Fts5Position>> {
    for entry in entries {
        if entry.rowid == rowid {
            if entry.deleted {
                return None;
            }
            return Some(entry.positions.clone());
        }
    }
    None
}

fn phrase_positions(positions: &[Vec<Fts5Position>]) -> Vec<Fts5Position> {
    if positions.is_empty() {
        return Vec::new();
    }
    let mut per_term: Vec<HashMap<i32, HashSet<i32>>> = Vec::new();
    for term_positions in positions {
        let mut map: HashMap<i32, HashSet<i32>> = HashMap::new();
        for pos in term_positions {
            map.entry(pos.column).or_default().insert(pos.offset);
        }
        per_term.push(map);
    }

    let first = &per_term[0];
    let mut out = Vec::new();
    for (col, offsets) in first {
        for offset in offsets {
            let mut ok = true;
            for (idx, term_map) in per_term.iter().enumerate().skip(1) {
                let expected = offset + idx as i32;
                if !term_map
                    .get(col)
                    .map(|set| set.contains(&expected))
                    .unwrap_or(false)
                {
                    ok = false;
                    break;
                }
            }
            if ok {
                out.push(Fts5Position {
                    column: *col,
                    offset: *offset,
                });
            }
        }
    }

    out
}

fn positions_within_distance(left: &[Fts5Position], right: &[Fts5Position], distance: i32) -> bool {
    let mut left_by_col: HashMap<i32, Vec<i32>> = HashMap::new();
    let mut right_by_col: HashMap<i32, Vec<i32>> = HashMap::new();
    for pos in left {
        left_by_col.entry(pos.column).or_default().push(pos.offset);
    }
    for pos in right {
        right_by_col.entry(pos.column).or_default().push(pos.offset);
    }
    for (col, left_offsets) in left_by_col {
        let Some(right_offsets) = right_by_col.get(&col) else {
            continue;
        };
        for lo in &left_offsets {
            for ro in right_offsets {
                if (lo - ro).abs() <= distance {
                    return true;
                }
            }
        }
    }
    false
}
