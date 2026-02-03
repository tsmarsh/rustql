//! FTS3 helper functions (snippet/offsets).

use crate::error::{Error, ErrorCode, Result};
use std::sync::Mutex;

use lazy_static::lazy_static;

use crate::types::Value;

use crate::fts3;

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Integer(i) => i.to_string(),
        Value::Real(r) => r.to_string(),
        Value::Text(s) => s.clone(),
        Value::Blob(b) => String::from_utf8_lossy(b).into_owned(),
    }
}

fn value_to_i64(value: &Value) -> i64 {
    match value {
        Value::Integer(i) => *i,
        Value::Real(r) => *r as i64,
        Value::Text(s) => s.parse::<i64>().unwrap_or(0),
        Value::Blob(b) => String::from_utf8_lossy(b).parse::<i64>().unwrap_or(0),
        Value::Null => 0,
    }
}

#[derive(Debug, Clone)]
struct Fts3Context {
    table: String,
    rowid: i64,
    query: Option<String>,
}

lazy_static! {
    static ref FTS3_CONTEXT: Mutex<Option<Fts3Context>> = Mutex::new(None);
}

pub fn set_fts3_context(table: Option<String>, rowid: Option<i64>, query: Option<String>) {
    let mut guard = FTS3_CONTEXT.lock().expect("fts3 context lock");
    if let (Some(table), Some(rowid)) = (table, rowid) {
        *guard = Some(Fts3Context {
            table,
            rowid,
            query,
        });
    } else {
        *guard = None;
    }
}

fn get_fts3_context() -> Option<Fts3Context> {
    let guard = FTS3_CONTEXT.lock().expect("fts3 context lock");
    guard.clone()
}

#[derive(Debug, Clone)]
struct PhraseSpec {
    expr: fts3::Fts3Expr,
    terms: Vec<String>,
    term_start: usize,
}

fn collect_matchable_phrases(
    expr: &fts3::Fts3Expr,
    in_not_rhs: bool,
    out: &mut Vec<(fts3::Fts3Expr, Vec<String>)>,
) {
    if in_not_rhs {
        return;
    }
    match expr {
        fts3::Fts3Expr::Term(term) => {
            out.push((expr.clone(), vec![term.clone()]));
        }
        fts3::Fts3Expr::Prefix(prefix) => {
            out.push((expr.clone(), vec![prefix.clone()]));
        }
        fts3::Fts3Expr::Phrase(terms) => {
            out.push((expr.clone(), terms.clone()));
        }
        fts3::Fts3Expr::And(left, right)
        | fts3::Fts3Expr::Or(left, right)
        | fts3::Fts3Expr::Near(left, right, _) => {
            collect_matchable_phrases(left, false, out);
            collect_matchable_phrases(right, false, out);
        }
        fts3::Fts3Expr::Not(left, right) => {
            collect_matchable_phrases(left, false, out);
            collect_matchable_phrases(right, true, out);
        }
    }
}

fn phrase_positions(doclist: &fts3::Fts3Doclist, rowid: i64, column: i32) -> Vec<i64> {
    let mut positions = Vec::new();
    for entry in doclist.iter() {
        if entry.rowid != rowid {
            continue;
        }
        for pos in entry.positions {
            if pos.column == column {
                positions.push(pos.offset as i64);
            }
        }
        break;
    }
    positions.sort_unstable();
    positions
}

fn token_at(tokens: &[fts3::Fts3Token], pos: i64) -> Option<&fts3::Fts3Token> {
    if pos < 0 {
        return None;
    }
    let idx = pos as usize;
    tokens.get(idx)
}

/// snippet(text, query [, start, end, ellipsis])
pub fn func_snippet(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 6 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "snippet() expects 1 to 6 arguments",
        ));
    }

    let Some(ctx) = get_fts3_context() else {
        return Err(Error::with_message(
            ErrorCode::Error,
            "snippet() requires FTS3 context",
        ));
    };

    let query = if args.len() >= 2 {
        value_to_string(&args[1])
    } else {
        ctx.query.clone().unwrap_or_default()
    };
    let start = args
        .get(2)
        .map(value_to_string)
        .unwrap_or_else(|| "<b>".to_string());
    let end = args
        .get(3)
        .map(value_to_string)
        .unwrap_or_else(|| "</b>".to_string());
    let ellipsis = args
        .get(4)
        .map(value_to_string)
        .unwrap_or_else(|| "<b>...</b>".to_string());
    let n_token = args.get(5).map(value_to_i64).unwrap_or(15);

    let Some(table) = fts3::get_table(&ctx.table) else {
        return Err(Error::with_message(
            ErrorCode::Error,
            "snippet() missing FTS3 table",
        ));
    };

    if n_token == 0 {
        return Ok(Value::Text(String::new()));
    }

    let table = table.lock().expect("fts3 table lock");
    let parsed = table.parse_query(&query)?;
    let mut raw_phrases = Vec::new();
    collect_matchable_phrases(&parsed, false, &mut raw_phrases);
    if raw_phrases.is_empty() {
        return Ok(Value::Text(String::new()));
    }

    let mut phrases = Vec::new();
    let mut term_index = 0usize;
    let mut doclists = Vec::new();
    for (expr, terms) in raw_phrases {
        let doclist = table.evaluate_expr(&expr)?;
        doclists.push(doclist);
        phrases.push(PhraseSpec {
            expr,
            terms,
            term_start: term_index,
        });
        term_index += phrases.last().expect("phrase").terms.len();
    }

    let values = table
        .row_values(ctx.rowid)
        .map(|vals| vals.to_vec())
        .unwrap_or_default();
    if values.is_empty() {
        return Ok(Value::Text(String::new()));
    }

    let mut fragments = Vec::new();
    let mut covered = 0u64;
    let mut seen = 0u64;
    let mut fragment_len = 0i64;
    let capped = n_token.clamp(-64, 64);

    for count in 1..=4 {
        fragment_len = if capped >= 0 {
            (capped + count as i64 - 1) / count as i64
        } else {
            -capped
        };

        for _ in 0..count {
            let mut best_score = -1;
            let mut best_fragment = None;
            let mut seen_in_cols = 0u64;

            for (col_idx, _) in values.iter().enumerate() {
                let mut col_phrases = Vec::new();
                for (idx, spec) in phrases.iter().enumerate() {
                    let positions = phrase_positions(&doclists[idx], ctx.rowid, col_idx as i32);
                    col_phrases.push((spec.terms.len() as i64, positions));
                }
                let (fragment, score, col_seen) =
                    best_snippet_for_column(col_idx as i32, fragment_len, &col_phrases, covered)?;
                seen_in_cols |= col_seen;
                if score > best_score {
                    best_score = score;
                    best_fragment = Some(fragment);
                }
            }

            if let Some(fragment) = best_fragment {
                covered |= fragment.covered;
                seen |= seen_in_cols;
                fragments.push(fragment);
            }
        }

        if seen == covered || count == 4 {
            break;
        }
    }

    let mut output = String::new();
    for (idx, fragment) in fragments.iter().enumerate() {
        let is_last = idx == fragments.len() - 1;
        let col_idx = fragment.column as usize;
        if let Some(value) = values.get(col_idx) {
            let tokens = table.tokenize(value)?;
            snippet_text(
                value,
                &tokens,
                fragment,
                idx,
                is_last,
                fragment_len,
                &start,
                &end,
                &ellipsis,
                &mut output,
            );
        }
    }

    Ok(Value::Text(output))
}

/// offsets(text, query)
pub fn func_offsets(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "offsets() expects 1 or 2 arguments",
        ));
    }

    let Some(ctx) = get_fts3_context() else {
        return Err(Error::with_message(
            ErrorCode::Error,
            "offsets() requires FTS3 context",
        ));
    };

    let query = if args.len() >= 2 {
        value_to_string(&args[1])
    } else {
        ctx.query.clone().unwrap_or_default()
    };
    let Some(table) = fts3::get_table(&ctx.table) else {
        return Err(Error::with_message(
            ErrorCode::Error,
            "offsets() missing FTS3 table",
        ));
    };

    let table = table.lock().expect("fts3 table lock");
    let parsed = table.parse_query(&query)?;
    let mut raw_phrases = Vec::new();
    collect_matchable_phrases(&parsed, false, &mut raw_phrases);
    if raw_phrases.is_empty() {
        return Ok(Value::Text(String::new()));
    }

    let mut phrases = Vec::new();
    let mut doclists = Vec::new();
    let mut term_index = 0usize;
    for (expr, terms) in raw_phrases {
        let doclist = table.evaluate_expr(&expr)?;
        doclists.push(doclist);
        phrases.push(PhraseSpec {
            expr,
            terms,
            term_start: term_index,
        });
        term_index += phrases.last().expect("phrase").terms.len();
    }

    let values = table
        .row_values(ctx.rowid)
        .map(|vals| vals.to_vec())
        .unwrap_or_default();
    if values.is_empty() {
        return Ok(Value::Text(String::new()));
    }

    let mut entries: Vec<String> = Vec::new();
    for (col_idx, value) in values.iter().enumerate() {
        let tokens = table.tokenize(value)?;
        let mut term_positions: Vec<Vec<i64>> = vec![Vec::new(); term_index];
        for (phrase_idx, spec) in phrases.iter().enumerate() {
            let positions = phrase_positions(&doclists[phrase_idx], ctx.rowid, col_idx as i32);
            let n_term = spec.terms.len() as i64;
            for (i_term, dest) in term_positions
                .iter_mut()
                .skip(spec.term_start)
                .take(spec.terms.len())
                .enumerate()
            {
                let offset = n_term - i_term as i64 - 1;
                for &pos in &positions {
                    let adjusted = pos - offset;
                    if adjusted >= 0 {
                        dest.push(adjusted);
                    }
                }
            }
        }

        for positions in &mut term_positions {
            positions.sort_unstable();
        }

        let mut matches: Vec<(i64, usize, usize, usize)> = Vec::new();
        for (term_idx, positions) in term_positions.iter().enumerate() {
            for &pos in positions {
                if let Some(token) = token_at(&tokens, pos) {
                    matches.push((pos, term_idx, token.start, token.end - token.start));
                }
            }
        }

        matches.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        for (_, term_idx, start, len) in matches {
            entries.push(format!("{} {} {} {}", col_idx, term_idx, start, len));
        }
    }

    Ok(Value::Text(entries.join(" ")))
}

#[derive(Debug, Clone)]
struct SnippetFragment {
    column: i32,
    pos: i64,
    covered: u64,
    highlight: u64,
}

#[derive(Debug, Clone)]
struct SnippetPhrase {
    n_token: i64,
    positions: Vec<i64>,
    head_idx: usize,
    tail_idx: usize,
}

impl SnippetPhrase {
    fn advance_head(&mut self, target: i64) {
        while self.head_idx < self.positions.len() && self.positions[self.head_idx] < target {
            self.head_idx += 1;
        }
    }

    fn advance_tail(&mut self, target: i64) {
        while self.tail_idx < self.positions.len() && self.positions[self.tail_idx] < target {
            self.tail_idx += 1;
        }
    }
}

#[derive(Debug, Clone)]
struct SnippetIter {
    n_snippet: i64,
    i_current: i64,
    phrases: Vec<SnippetPhrase>,
}

fn snippet_next_candidate(iter: &mut SnippetIter) -> bool {
    if iter.i_current < 0 {
        iter.i_current = 0;
        for phrase in &mut iter.phrases {
            phrase.advance_head(iter.n_snippet);
        }
        return false;
    }

    let mut min_head = i64::MAX;
    for phrase in &iter.phrases {
        if phrase.head_idx < phrase.positions.len() {
            let value = phrase.positions[phrase.head_idx];
            if value < min_head {
                min_head = value;
            }
        }
    }

    if min_head == i64::MAX {
        return true;
    }

    let i_start = min_head - iter.n_snippet + 1;
    iter.i_current = i_start;
    for phrase in &mut iter.phrases {
        phrase.advance_head(min_head + 1);
        phrase.advance_tail(i_start);
    }

    false
}

fn snippet_details(iter: &SnippetIter, covered: u64) -> (i64, i32, u64, u64) {
    let i_start = iter.i_current;
    let mut score = 0i32;
    let mut cover = 0u64;
    let mut highlight = 0u64;

    for (idx, phrase) in iter.phrases.iter().enumerate() {
        if phrase.tail_idx >= phrase.positions.len() {
            continue;
        }
        let mut pos_idx = phrase.tail_idx;
        while pos_idx < phrase.positions.len() {
            let pos = phrase.positions[pos_idx];
            if pos < i_start {
                pos_idx += 1;
                continue;
            }
            if pos >= i_start + iter.n_snippet {
                break;
            }

            let phrase_mask = 1u64 << (idx % 64);
            if (cover | covered) & phrase_mask != 0 {
                score += 1;
            } else {
                score += 1000;
            }
            cover |= phrase_mask;

            let rel = pos - i_start;
            if rel >= 0 && rel < 64 {
                let mut token_mask = 1u64 << rel;
                let limit = phrase.n_token.min(iter.n_snippet);
                for _ in 0..limit {
                    highlight |= token_mask;
                    token_mask >>= 1;
                }
            }

            pos_idx += 1;
        }
    }

    (i_start, score, cover, highlight)
}

fn best_snippet_for_column(
    column: i32,
    n_snippet: i64,
    phrases: &[(i64, Vec<i64>)],
    covered: u64,
) -> Result<(SnippetFragment, i32, u64)> {
    let limit = phrases.len().min(64);
    let mut snippet_phrases = Vec::with_capacity(limit);
    let mut seen = 0u64;
    for (idx, (n_token, positions)) in phrases.iter().take(limit).enumerate() {
        let mut sorted = positions.clone();
        sorted.sort_unstable();
        if !sorted.is_empty() {
            seen |= 1u64 << idx;
        }
        snippet_phrases.push(SnippetPhrase {
            n_token: *n_token,
            positions: sorted,
            head_idx: 0,
            tail_idx: 0,
        });
    }

    let mut iter = SnippetIter {
        n_snippet,
        i_current: -1,
        phrases: snippet_phrases,
    };

    let mut best_score = -1;
    let mut best = SnippetFragment {
        column,
        pos: 0,
        covered: 0,
        highlight: 0,
    };

    while !snippet_next_candidate(&mut iter) {
        let (pos, score, cover, highlight) = snippet_details(&iter, covered);
        if score > best_score {
            best_score = score;
            best = SnippetFragment {
                column,
                pos,
                covered: cover,
                highlight,
            };
        }
    }

    Ok((best, best_score, seen))
}

fn shift_snippet(total_tokens: usize, n_snippet: i64, pos: &mut i64, highlight: &mut u64) {
    if *highlight == 0 || n_snippet <= 0 {
        return;
    }
    let n_snippet = n_snippet.min(64) as u64;
    let mut left = 0u64;
    while left < n_snippet && (*highlight & (1u64 << left)) == 0 {
        left += 1;
    }
    let mut right = 0u64;
    while right < n_snippet && (*highlight & (1u64 << (n_snippet - 1 - right))) == 0 {
        right += 1;
    }
    if left >= n_snippet || right >= n_snippet {
        return;
    }
    let desired = (left as i64 - right as i64) / 2;
    if desired <= 0 {
        return;
    }
    let available = total_tokens as i64 - (*pos + n_snippet as i64);
    if available <= 0 {
        return;
    }
    let shift = desired.min(available);
    if shift > 0 {
        *pos += shift;
        *highlight >>= shift;
    }
}

fn snippet_text(
    text: &str,
    tokens: &[fts3::Fts3Token],
    fragment: &SnippetFragment,
    fragment_index: usize,
    is_last: bool,
    n_snippet: i64,
    start: &str,
    end: &str,
    ellipsis: &str,
    out: &mut String,
) {
    if tokens.is_empty() || n_snippet <= 0 {
        return;
    }

    let mut pos = fragment.pos.max(0);
    let mut highlight = fragment.highlight;
    shift_snippet(tokens.len(), n_snippet, &mut pos, &mut highlight);

    let start_idx = pos as usize;
    if start_idx >= tokens.len() {
        return;
    }

    let end_idx = (start_idx + n_snippet as usize).min(tokens.len());
    let first_start = tokens[start_idx].start;

    if pos > 0 || fragment_index > 0 {
        out.push_str(ellipsis);
    } else if first_start > 0 && first_start <= text.len() {
        out.push_str(&text[..first_start]);
    }

    let mut prev_end = first_start;
    for idx in start_idx..end_idx {
        let token = &tokens[idx];
        if token.start >= text.len() || token.end > text.len() {
            continue;
        }
        if idx > start_idx && token.start >= prev_end {
            out.push_str(&text[prev_end..token.start]);
        }
        let rel = idx as i64 - pos;
        let is_highlight = rel >= 0 && rel < 64 && (highlight & (1u64 << rel)) != 0;
        if is_highlight {
            out.push_str(start);
        }
        out.push_str(&text[token.start..token.end]);
        if is_highlight {
            out.push_str(end);
        }
        prev_end = token.end;
    }

    if is_last {
        if end_idx < tokens.len() {
            out.push_str(ellipsis);
        } else if prev_end < text.len() {
            out.push_str(&text[prev_end..]);
        }
    }
}

fn matchinfo_format_valid(format: &str) -> bool {
    format
        .chars()
        .all(|ch| matches!(ch, 'p' | 'c' | 'n' | 'a' | 'l' | 's' | 'x' | 'y' | 'b'))
}

fn current_row_lengths(table: &fts3::Fts3Table, rowid: i64, n_col: usize) -> Result<Vec<u32>> {
    let mut lengths = vec![0u32; n_col];
    if let Some(values) = table.row_values(rowid) {
        for (idx, value) in values.iter().enumerate().take(n_col) {
            let tokens = table.tokenize(value)?;
            lengths[idx] = tokens.len().min(u32::MAX as usize) as u32;
        }
    }
    Ok(lengths)
}

fn average_lengths(table: &fts3::Fts3Table, n_col: usize, n_doc: u32) -> Result<Vec<u32>> {
    let mut totals = vec![0u64; n_col];
    if n_doc == 0 {
        return Ok(vec![0u32; n_col]);
    }
    for rowid in table.all_rowids() {
        if let Some(values) = table.row_values(rowid) {
            for (idx, value) in values.iter().enumerate().take(n_col) {
                let tokens = table.tokenize(value)?;
                totals[idx] += tokens.len() as u64;
            }
        }
    }
    let mut averages = vec![0u32; n_col];
    for (idx, total) in totals.iter().enumerate() {
        let avg = (*total + (n_doc as u64 / 2)) / n_doc as u64;
        averages[idx] = avg.min(u32::MAX as u64) as u32;
    }
    Ok(averages)
}

fn lcs_for_column(
    phrases: &[PhraseSpec],
    doclists: &[fts3::Fts3Doclist],
    rowid: i64,
    column: i32,
) -> u32 {
    let mut sequences: Vec<(i64, u32)> = Vec::new();
    for (idx, phrase) in phrases.iter().enumerate() {
        let positions = phrase_positions(&doclists[idx], rowid, column);
        let shift = phrase.terms.len().saturating_sub(1) as i64;
        let mut next = Vec::new();
        for pos in positions {
            let start_pos = pos - shift;
            if start_pos < 0 {
                continue;
            }
            let mut best = 1u32;
            for (prev_pos, prev_len) in &sequences {
                if *prev_pos < start_pos {
                    best = best.max(prev_len + 1);
                }
            }
            next.push((start_pos, best));
        }
        sequences.extend(next);
    }
    sequences.iter().map(|(_, len)| *len).max().unwrap_or(0)
}

/// matchinfo(text, query)
pub fn func_matchinfo(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "matchinfo() expects 2 to 3 arguments",
        ));
    }

    let query = value_to_string(&args[1]);
    let format = args
        .get(2)
        .map(value_to_string)
        .unwrap_or_else(|| "pcx".to_string());
    if !matchinfo_format_valid(&format) {
        return Err(Error::with_message(
            ErrorCode::Error,
            "matchinfo() unrecognized request",
        ));
    }
    let Some(ctx) = get_fts3_context() else {
        return Err(Error::with_message(
            ErrorCode::Error,
            "matchinfo() requires FTS3 context",
        ));
    };
    let Some(table) = fts3::get_table(&ctx.table) else {
        return Err(Error::with_message(
            ErrorCode::Error,
            "matchinfo() missing FTS3 table",
        ));
    };

    let table = table.lock().expect("fts3 table lock");
    let parsed = table.parse_query(&query)?;
    let mut raw_phrases = Vec::new();
    collect_matchable_phrases(&parsed, false, &mut raw_phrases);

    let n_phrase = raw_phrases.len() as u32;
    let n_col = table.columns.len() as u32;
    let n_doc = table.all_rowids().len() as u32;

    let mut phrases = Vec::new();
    let mut doclists = Vec::new();
    for (expr, terms) in raw_phrases {
        let doclist = table.evaluate_expr(&expr)?;
        doclists.push(doclist);
        phrases.push(PhraseSpec {
            expr,
            terms,
            term_start: 0,
        });
    }

    let lengths = current_row_lengths(&table, ctx.rowid, n_col as usize)?;
    let avg_lengths = average_lengths(&table, n_col as usize, n_doc)?;

    let mut hits_this_row = vec![vec![0u32; n_col as usize]; n_phrase as usize];
    let mut hits_all = vec![vec![0u32; n_col as usize]; n_phrase as usize];
    let mut docs_with_hits = vec![vec![0u32; n_col as usize]; n_phrase as usize];

    for (phrase_idx, doclist) in doclists.iter().enumerate() {
        for entry in doclist.iter() {
            let mut per_col = vec![0u32; n_col as usize];
            for pos in entry.positions {
                if pos.column >= 0 && (pos.column as usize) < per_col.len() {
                    per_col[pos.column as usize] += 1;
                }
            }
            for col in 0..per_col.len() {
                let count = per_col[col];
                if count > 0 {
                    hits_all[phrase_idx][col] += count;
                    docs_with_hits[phrase_idx][col] += 1;
                }
                if entry.rowid == ctx.rowid {
                    hits_this_row[phrase_idx][col] = count;
                }
            }
        }
    }

    let mut out = Vec::new();
    for flag in format.chars() {
        match flag {
            'p' => out.push(n_phrase),
            'c' => out.push(n_col),
            'n' => out.push(n_doc),
            'a' => out.extend(avg_lengths.iter().copied()),
            'l' => out.extend(lengths.iter().copied()),
            's' => {
                for col in 0..n_col as usize {
                    let lcs = lcs_for_column(&phrases, &doclists, ctx.rowid, col as i32);
                    out.push(lcs);
                }
            }
            'x' => {
                for phrase_idx in 0..phrases.len() {
                    for col in 0..n_col as usize {
                        out.push(hits_this_row[phrase_idx][col]);
                        out.push(hits_all[phrase_idx][col]);
                        out.push(docs_with_hits[phrase_idx][col]);
                    }
                }
            }
            'y' => {
                for phrase_idx in 0..phrases.len() {
                    for col in 0..n_col as usize {
                        out.push(hits_this_row[phrase_idx][col]);
                    }
                }
            }
            'b' => {
                let groups = (n_col as usize + 31) / 32;
                for phrase_idx in 0..phrases.len() {
                    for group in 0..groups {
                        let mut mask = 0u32;
                        for bit in 0..32 {
                            let col = group * 32 + bit;
                            if col >= n_col as usize {
                                break;
                            }
                            if hits_this_row[phrase_idx][col] > 0 {
                                mask |= 1u32 << bit;
                            }
                        }
                        out.push(mask);
                    }
                }
            }
            _ => {}
        }
    }

    let mut buf = Vec::with_capacity(out.len() * 4);
    for value in out {
        buf.extend_from_slice(&value.to_le_bytes());
    }

    Ok(Value::Blob(buf))
}

// ============================================================================
// FTS3 Expression Test Functions
// ============================================================================

#[derive(Debug, Clone)]
enum ExprTestNode {
    Phrase(i32, Vec<(String, bool)>),
    And(Box<ExprTestNode>, Box<ExprTestNode>),
    Or(Box<ExprTestNode>, Box<ExprTestNode>),
    Not(Box<ExprTestNode>, Box<ExprTestNode>),
    Near(i32, Box<ExprTestNode>, Box<ExprTestNode>),
}

impl ExprTestNode {
    fn to_string_repr(&self) -> String {
        match self {
            ExprTestNode::Phrase(col, terms) => {
                let mut p = vec!["PHRASE".to_string(), col.to_string(), "0".to_string()];
                for (t, pfx) in terms {
                    p.push(if *pfx { format!("{}+", t) } else { t.clone() });
                }
                p.join(" ")
            }
            ExprTestNode::And(l, r) => format!("AND {{{}}} {{{}}}", l.to_string_repr(), r.to_string_repr()),
            ExprTestNode::Or(l, r) => format!("OR {{{}}} {{{}}}", l.to_string_repr(), r.to_string_repr()),
            ExprTestNode::Not(l, r) => format!("NOT {{{}}} {{{}}}", l.to_string_repr(), r.to_string_repr()),
            ExprTestNode::Near(d, l, r) => format!("NEAR/{} {{{}}} {{{}}}", d, l.to_string_repr(), r.to_string_repr()),
        }
    }
}

#[derive(Debug, Clone)]
enum ExprTestToken {
    Word(Option<String>, String, bool),
    QuotedPhrase(Vec<(String, bool)>),
    LParen, RParen, And, Or, Not, Near(i32),
}

fn tokenize_exprtest(expr: &str) -> Vec<ExprTestToken> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = expr.chars().collect();
    let len = chars.len();
    let mut i = 0;
    while i < len {
        let ch = chars[i];
        if ch.is_ascii_whitespace() { i += 1; continue; }
        if ch == '(' { tokens.push(ExprTestToken::LParen); i += 1; continue; }
        if ch == ')' { tokens.push(ExprTestToken::RParen); i += 1; continue; }
        if ch == '"' {
            i += 1;
            let mut phrase_terms: Vec<(String, bool)> = Vec::new();
            let mut cw = String::new();
            while i < len && chars[i] != '"' {
                let c = chars[i];
                if c.is_ascii_alphanumeric() || c == '_' || (!c.is_ascii() && c != '"') {
                    cw.push(c.to_ascii_lowercase()); i += 1;
                } else if c == '*' && !cw.is_empty() {
                    phrase_terms.push((cw.clone(), true)); cw.clear(); i += 1;
                } else {
                    if !cw.is_empty() { phrase_terms.push((cw.clone(), false)); cw.clear(); }
                    i += 1;
                }
            }
            if !cw.is_empty() { phrase_terms.push((cw, false)); }
            if i < len && chars[i] == '"' { i += 1; }
            if !phrase_terms.is_empty() { tokens.push(ExprTestToken::QuotedPhrase(phrase_terms)); }
            continue;
        }
        if ch.is_ascii_alphanumeric() || ch == '_' || !ch.is_ascii() {
            let mut word = String::new();
            while i < len && (chars[i].is_ascii_alphanumeric() || chars[i] == '_' || !chars[i].is_ascii()) {
                word.push(chars[i]); i += 1;
            }
            let upper = word.to_ascii_uppercase();
            if upper == "AND" { tokens.push(ExprTestToken::And); continue; }
            if upper == "OR" { tokens.push(ExprTestToken::Or); continue; }
            if upper == "NOT" { tokens.push(ExprTestToken::Not); continue; }
            if upper == "NEAR" {
                let mut dist = 10;
                if i < len && chars[i] == '/' {
                    i += 1;
                    let mut ns = String::new();
                    while i < len && chars[i].is_ascii_digit() { ns.push(chars[i]); i += 1; }
                    if !ns.is_empty() { dist = ns.parse::<i32>().unwrap_or(10); }
                }
                tokens.push(ExprTestToken::Near(dist)); continue;
            }
            let mut col_prefix = None;
            if i < len && chars[i] == ':' {
                col_prefix = Some(word.to_ascii_lowercase());
                i += 1;
                word = String::new();
                while i < len && (chars[i].is_ascii_alphanumeric() || chars[i] == '_' || !chars[i].is_ascii()) {
                    word.push(chars[i]); i += 1;
                }
                if word.is_empty() { word = col_prefix.take().unwrap(); }
            }
            let is_prefix = i < len && chars[i] == '*';
            if is_prefix { i += 1; }
            tokens.push(ExprTestToken::Word(col_prefix, word.to_ascii_lowercase(), is_prefix));
            continue;
        }
        i += 1;
    }
    tokens
}

struct ExprTestParser { tokens: Vec<ExprTestToken>, pos: usize, columns: Vec<String>, n_col: i32 }

impl ExprTestParser {
    fn new(tokens: Vec<ExprTestToken>, columns: Vec<String>) -> Self {
        let n_col = columns.len() as i32;
        ExprTestParser { tokens, pos: 0, columns, n_col }
    }
    fn peek(&self) -> Option<&ExprTestToken> { self.tokens.get(self.pos) }
    fn advance(&mut self) { self.pos += 1; }
    fn resolve_column(&self, name: &str) -> i32 {
        let lower = name.to_ascii_lowercase();
        for (idx, col) in self.columns.iter().enumerate() {
            if col.to_ascii_lowercase() == lower { return idx as i32; }
        }
        self.n_col
    }
    fn parse(&mut self) -> Result<Option<ExprTestNode>> { self.parse_or() }
    fn parse_or(&mut self) -> Result<Option<ExprTestNode>> {
        let mut left = match self.parse_and()? { Some(n) => n, None => return Ok(None) };
        while let Some(ExprTestToken::Or) = self.peek() {
            self.advance();
            let right = self.parse_and()?.ok_or_else(|| Error::with_message(ErrorCode::Error, "expected expression after OR"))?;
            left = ExprTestNode::Or(Box::new(left), Box::new(right));
        }
        Ok(Some(left))
    }
    fn parse_and(&mut self) -> Result<Option<ExprTestNode>> {
        let mut left = match self.parse_not()? { Some(n) => n, None => return Ok(None) };
        loop {
            if let Some(ExprTestToken::And) = self.peek() {
                self.advance();
                let right = self.parse_not()?.ok_or_else(|| Error::with_message(ErrorCode::Error, "expected expression after AND"))?;
                left = ExprTestNode::And(Box::new(left), Box::new(right));
                continue;
            }
            match self.peek() {
                Some(ExprTestToken::Word(..)) | Some(ExprTestToken::QuotedPhrase(_)) | Some(ExprTestToken::LParen) => {
                    let right = self.parse_not()?.ok_or_else(|| Error::with_message(ErrorCode::Error, "expected expression"))?;
                    left = ExprTestNode::And(Box::new(left), Box::new(right));
                    continue;
                }
                _ => break,
            }
        }
        Ok(Some(left))
    }
    fn parse_not(&mut self) -> Result<Option<ExprTestNode>> {
        let mut left = match self.parse_near()? { Some(n) => n, None => return Ok(None) };
        while let Some(ExprTestToken::Not) = self.peek() {
            self.advance();
            let right = self.parse_near()?.ok_or_else(|| Error::with_message(ErrorCode::Error, "expected expression after NOT"))?;
            left = ExprTestNode::Not(Box::new(left), Box::new(right));
        }
        Ok(Some(left))
    }
    fn parse_near(&mut self) -> Result<Option<ExprTestNode>> {
        let mut left = match self.parse_primary()? { Some(n) => n, None => return Ok(None) };
        while let Some(ExprTestToken::Near(dist)) = self.peek() {
            let dist = *dist; self.advance();
            let right = self.parse_primary()?.ok_or_else(|| Error::with_message(ErrorCode::Error, "expected expression after NEAR"))?;
            left = ExprTestNode::Near(dist, Box::new(left), Box::new(right));
        }
        Ok(Some(left))
    }
    fn parse_primary(&mut self) -> Result<Option<ExprTestNode>> {
        match self.peek().cloned() {
            Some(ExprTestToken::Word(col_prefix, term, is_prefix)) => {
                self.advance();
                let col = if let Some(ref p) = col_prefix { self.resolve_column(p) } else { self.n_col };
                Ok(Some(ExprTestNode::Phrase(col, vec![(term, is_prefix)])))
            }
            Some(ExprTestToken::QuotedPhrase(terms)) => { self.advance(); Ok(Some(ExprTestNode::Phrase(self.n_col, terms))) }
            Some(ExprTestToken::LParen) => {
                self.advance();
                let node = self.parse_or()?;
                if let Some(ExprTestToken::RParen) = self.peek() { self.advance(); }
                Ok(node)
            }
            _ => Ok(None),
        }
    }
}

fn flatten_and(node: &ExprTestNode) -> Vec<ExprTestNode> {
    match node {
        ExprTestNode::And(l, r) => { let mut res = flatten_and(l); res.extend(flatten_and(r)); res }
        _ => vec![node.clone()],
    }
}

fn build_balanced_and(nodes: &[ExprTestNode]) -> ExprTestNode {
    assert!(!nodes.is_empty());
    if nodes.len() == 1 { return nodes[0].clone(); }
    if nodes.len() == 2 { return ExprTestNode::And(Box::new(nodes[0].clone()), Box::new(nodes[1].clone())); }
    let mid = (nodes.len() + 1) / 2;
    ExprTestNode::And(Box::new(build_balanced_and(&nodes[..mid])), Box::new(build_balanced_and(&nodes[mid..])))
}

fn rebalance_tree(node: &ExprTestNode) -> ExprTestNode {
    match node {
        ExprTestNode::And(_, _) => {
            let flat: Vec<ExprTestNode> = flatten_and(node).into_iter().map(|n| rebalance_tree(&n)).collect();
            build_balanced_and(&flat)
        }
        ExprTestNode::Or(l, r) => ExprTestNode::Or(Box::new(rebalance_tree(l)), Box::new(rebalance_tree(r))),
        ExprTestNode::Not(l, r) => ExprTestNode::Not(Box::new(rebalance_tree(l)), Box::new(rebalance_tree(r))),
        ExprTestNode::Near(d, l, r) => ExprTestNode::Near(*d, Box::new(rebalance_tree(l)), Box::new(rebalance_tree(r))),
        ExprTestNode::Phrase(c, t) => ExprTestNode::Phrase(*c, t.clone()),
    }
}

pub fn func_fts3_exprtest(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(Error::with_message(ErrorCode::Error, "fts3_exprtest requires at least 3 arguments"));
    }
    let _tokenizer_name = value_to_string(&args[0]);
    let expression = value_to_string(&args[1]);
    let columns: Vec<String> = args[2..].iter().map(|v| value_to_string(v).to_ascii_lowercase()).collect();
    let tokens = tokenize_exprtest(&expression);
    let mut parser = ExprTestParser::new(tokens, columns);
    match parser.parse()? {
        Some(node) => Ok(Value::Text(node.to_string_repr())),
        None => Err(Error::with_message(ErrorCode::Error, "fts3_exprtest: parse error")),
    }
}

pub fn func_fts3_exprtest_rebalance(args: &[Value]) -> Result<Value> {
    if args.len() < 3 {
        return Err(Error::with_message(ErrorCode::Error, "fts3_exprtest_rebalance requires at least 3 arguments"));
    }
    let _tokenizer_name = value_to_string(&args[0]);
    let expression = value_to_string(&args[1]);
    let columns: Vec<String> = args[2..].iter().map(|v| value_to_string(v).to_ascii_lowercase()).collect();
    let tokens = tokenize_exprtest(&expression);
    let mut parser = ExprTestParser::new(tokens, columns);
    match parser.parse()? {
        Some(node) => { let r = rebalance_tree(&node); Ok(Value::Text(r.to_string_repr())) }
        None => Err(Error::with_message(ErrorCode::Error, "fts3_exprtest_rebalance: parse error")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snippet_basic() {
        let mut table = fts3::Fts3Table::new(
            "docs_snippet",
            "main",
            vec!["body".to_string()],
            Box::new(fts3::SimpleTokenizer::default()),
        );
        table.insert(1, &["hello world"]).expect("insert");
        fts3::register_table(table);
        set_fts3_context(Some("docs_snippet".to_string()), Some(1), None);

        let args = [
            Value::Text("ignored".to_string()),
            Value::Text("world".to_string()),
        ];
        let result = func_snippet(&args).expect("snippet");
        match result {
            Value::Text(text) => assert!(text.contains("<b>world</b>")),
            _ => panic!("expected text"),
        }
    }

    #[test]
    fn test_offsets_basic() {
        let mut table = fts3::Fts3Table::new(
            "docs_offsets",
            "main",
            vec!["body".to_string()],
            Box::new(fts3::SimpleTokenizer::default()),
        );
        table.insert(1, &["alpha beta alpha"]).expect("insert");
        fts3::register_table(table);
        set_fts3_context(Some("docs_offsets".to_string()), Some(1), None);

        let args = [
            Value::Text("ignored".to_string()),
            Value::Text("alpha".to_string()),
        ];
        let result = func_offsets(&args).expect("offsets");
        match result {
            Value::Text(text) => assert!(!text.is_empty()),
            _ => panic!("expected text"),
        }
    }

    #[test]
    fn test_matchinfo_basic() {
        let mut table = fts3::Fts3Table::new(
            "docs",
            "main",
            vec!["body".to_string()],
            Box::new(fts3::SimpleTokenizer::default()),
        );
        table.insert(1, &["alpha beta alpha"]).expect("insert");
        fts3::register_table(table);
        set_fts3_context(Some("docs".to_string()), Some(1), None);

        let args = [
            Value::Text("alpha beta alpha".to_string()),
            Value::Text("alpha beta".to_string()),
        ];
        let result = func_matchinfo(&args).expect("matchinfo");
        match result {
            Value::Blob(blob) => {
                assert!(blob.len() >= 8);
            }
            _ => panic!("expected blob"),
        }
    }
}
