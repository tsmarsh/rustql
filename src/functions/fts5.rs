//! FTS5 helper functions (bm25/highlight/snippet).

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use lazy_static::lazy_static;

use crate::error::{Error, ErrorCode, Result};
use crate::types::Value;

use crate::fts5;

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

fn value_to_f64(value: &Value) -> f64 {
    match value {
        Value::Integer(i) => *i as f64,
        Value::Real(r) => *r,
        Value::Text(s) => s.parse::<f64>().unwrap_or(0.0),
        Value::Blob(b) => String::from_utf8_lossy(b).parse::<f64>().unwrap_or(0.0),
        Value::Null => 0.0,
    }
}

#[derive(Debug, Clone)]
struct Fts5Context {
    table: String,
    rowid: i64,
    query: Option<String>,
}

lazy_static! {
    static ref FTS5_CONTEXT: Mutex<Option<Fts5Context>> = Mutex::new(None);
}

pub fn set_fts5_context(table: Option<String>, rowid: Option<i64>, query: Option<String>) {
    let mut guard = FTS5_CONTEXT.lock().expect("fts5 context lock");
    if let (Some(table), Some(rowid)) = (table, rowid) {
        *guard = Some(Fts5Context {
            table,
            rowid,
            query,
        });
    } else {
        *guard = None;
    }
}

fn get_fts5_context() -> Option<Fts5Context> {
    let guard = FTS5_CONTEXT.lock().expect("fts5 context lock");
    guard.clone()
}

pub fn func_bm25(args: &[Value]) -> Result<Value> {
    let Some(ctx) = get_fts5_context() else {
        return Err(Error::with_message(
            ErrorCode::Error,
            "bm25() requires FTS5 context",
        ));
    };

    let Some(table) = fts5::get_table(&ctx.table) else {
        return Err(Error::with_message(
            ErrorCode::Error,
            "bm25() missing FTS5 table",
        ));
    };
    let table = table.lock().expect("fts5 table lock");
    let Some(values) = table.row_values(ctx.rowid) else {
        return Ok(Value::Real(0.0));
    };

    let query = ctx.query.unwrap_or_default();
    if query.is_empty() {
        return Ok(Value::Real(0.0));
    }

    let expr = fts5::expr::parse_query(&query, table.tokenizer.as_ref())?;
    let phrases = collect_phrases(&expr);
    if phrases.is_empty() {
        return Ok(Value::Real(0.0));
    }

    let rowids = table.all_rowids();
    if rowids.is_empty() {
        return Ok(Value::Real(0.0));
    }

    let avgdl = {
        let mut total_tokens = 0usize;
        for rowid in &rowids {
            if let Some(row_values) = table.row_values(*rowid) {
                total_tokens += count_tokens(&table, row_values)?;
            }
        }
        if total_tokens == 0 {
            return Ok(Value::Real(0.0));
        }
        total_tokens as f64 / rowids.len() as f64
    };

    let doc_len = count_tokens(&table, values)? as f64;
    if doc_len == 0.0 {
        return Ok(Value::Real(0.0));
    }

    let weights: Vec<f64> = args.iter().map(value_to_f64).collect();
    let mut score = 0.0;
    for phrase in &phrases {
        let docfreq = table.expr_rowids(phrase)?.len();
        if docfreq == 0 {
            continue;
        }

        let mut freq = 0.0;
        for pos in table.expr_positions_for_row(phrase, ctx.rowid)? {
            let weight = weights.get(pos.column as usize).copied().unwrap_or(1.0);
            freq += weight;
        }
        if freq == 0.0 {
            continue;
        }

        let mut idf = ((rowids.len() as f64 - docfreq as f64 + 0.5) / (docfreq as f64 + 0.5)).ln();
        if idf <= 0.0 {
            idf = 1e-6;
        }

        let k1 = 1.2;
        let b = 0.75;
        score += idf * ((freq * (k1 + 1.0)) / (freq + k1 * (1.0 - b + b * doc_len / avgdl)));
    }

    Ok(Value::Real(-score))
}

/// highlight(text, column, open, close)
pub fn func_highlight(args: &[Value]) -> Result<Value> {
    if args.len() > 4 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "highlight() expects 0 to 4 arguments",
        ));
    }

    let Some(ctx) = get_fts5_context() else {
        return Err(Error::with_message(
            ErrorCode::Error,
            "highlight() requires FTS5 context",
        ));
    };
    let Some(table) = fts5::get_table(&ctx.table) else {
        return Err(Error::with_message(
            ErrorCode::Error,
            "highlight() missing FTS5 table",
        ));
    };

    let table = table.lock().expect("fts5 table lock");
    let Some(values) = table.row_values(ctx.rowid) else {
        return Ok(Value::Text(String::new()));
    };

    let col = args.get(0).map(value_to_i64).unwrap_or(0);
    let open = args
        .get(1)
        .map(value_to_string)
        .unwrap_or_else(|| "<b>".to_string());
    let close = args
        .get(2)
        .map(value_to_string)
        .unwrap_or_else(|| "</b>".to_string());

    let col_idx = col.max(0) as usize;
    let Some(text) = values.get(col_idx) else {
        return Ok(Value::Text(String::new()));
    };

    let query = ctx.query.unwrap_or_default();
    if query.is_empty() {
        return Ok(Value::Text(text.clone()));
    }
    let expr = fts5::expr::parse_query(&query, table.tokenizer.as_ref())?;
    let tokens = table.tokenize(text)?;
    let matches = match_indices_for_expr(&expr, &table, ctx.rowid, col_idx as i32, &tokens)?;
    let highlighted = apply_highlight(text, &tokens, &matches, &open, &close);
    Ok(Value::Text(highlighted))
}

/// snippet(text, column, open, close, ellipsis, max_tokens)
pub fn func_snippet(args: &[Value]) -> Result<Value> {
    if args.len() > 6 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "snippet() expects 0 to 6 arguments",
        ));
    }

    let Some(ctx) = get_fts5_context() else {
        return Err(Error::with_message(
            ErrorCode::Error,
            "snippet() requires FTS5 context",
        ));
    };
    let Some(table) = fts5::get_table(&ctx.table) else {
        return Err(Error::with_message(
            ErrorCode::Error,
            "snippet() missing FTS5 table",
        ));
    };

    let table = table.lock().expect("fts5 table lock");
    let Some(values) = table.row_values(ctx.rowid) else {
        return Ok(Value::Text(String::new()));
    };

    let col = args.get(0).map(value_to_i64).unwrap_or(-1);
    let open = args
        .get(1)
        .map(value_to_string)
        .unwrap_or_else(|| "<b>".to_string());
    let close = args
        .get(2)
        .map(value_to_string)
        .unwrap_or_else(|| "</b>".to_string());
    let ellipsis = args
        .get(3)
        .map(value_to_string)
        .unwrap_or_else(|| "...".to_string());
    let max_tokens = args.get(4).map(value_to_i64).unwrap_or(15).max(1) as usize;

    let query = ctx.query.unwrap_or_default();
    if query.is_empty() {
        return Ok(Value::Text(String::new()));
    }
    let expr = fts5::expr::parse_query(&query, table.tokenizer.as_ref())?;

    let mut chosen = None;
    let mut best_hits = 0usize;
    for (idx, text) in values.iter().enumerate() {
        if col >= 0 && idx != col as usize {
            continue;
        }
        let tokens = table.tokenize(text)?;
        let matches = match_indices_for_expr(&expr, &table, ctx.rowid, idx as i32, &tokens)?;
        if matches.is_empty() {
            continue;
        }
        let hits = matches.len();
        if hits > best_hits {
            best_hits = hits;
            chosen = Some((text.clone(), tokens, matches));
            if col >= 0 {
                break;
            }
        }
    }

    let Some((text, tokens, matches)) = chosen else {
        return Ok(Value::Text(String::new()));
    };

    let first_hit = matches[0];
    let start_token = first_hit.saturating_sub(max_tokens / 2);
    let end_token = (start_token + max_tokens).min(tokens.len());
    let start_byte = tokens[start_token].start;
    let end_byte = tokens[end_token - 1].end;

    let mut snippet = String::new();
    if start_byte > 0 {
        snippet.push_str(&ellipsis);
    }
    let slice = &text[start_byte..end_byte];
    let slice_tokens: Vec<fts5::tokenizer::Fts5Token> = tokens[start_token..end_token]
        .iter()
        .map(|token| fts5::tokenizer::Fts5Token {
            text: token.text.clone(),
            position: token.position - tokens[start_token].position,
            start: token.start - start_byte,
            end: token.end - start_byte,
        })
        .collect();
    let slice_matches: Vec<usize> = matches
        .iter()
        .filter_map(|idx| {
            if *idx >= start_token && *idx < end_token {
                Some(*idx - start_token)
            } else {
                None
            }
        })
        .collect();
    snippet.push_str(&apply_highlight(
        slice,
        &slice_tokens,
        &slice_matches,
        &open,
        &close,
    ));
    if end_byte < text.len() {
        snippet.push_str(&ellipsis);
    }

    Ok(Value::Text(snippet))
}

fn collect_phrases(expr: &fts5::expr::Fts5Expr) -> Vec<fts5::expr::Fts5Expr> {
    let mut phrases = Vec::new();
    collect_phrases_inner(expr, &mut phrases);
    phrases
}

fn collect_phrases_inner(expr: &fts5::expr::Fts5Expr, phrases: &mut Vec<fts5::expr::Fts5Expr>) {
    match expr {
        fts5::expr::Fts5Expr::Term(_)
        | fts5::expr::Fts5Expr::Prefix(_)
        | fts5::expr::Fts5Expr::Phrase(_)
        | fts5::expr::Fts5Expr::Column(_, _) => phrases.push(expr.clone()),
        fts5::expr::Fts5Expr::And(left, right)
        | fts5::expr::Fts5Expr::Or(left, right)
        | fts5::expr::Fts5Expr::Not(left, right) => {
            collect_phrases_inner(left, phrases);
            collect_phrases_inner(right, phrases);
        }
        fts5::expr::Fts5Expr::Near(items, _) => {
            for item in items {
                collect_phrases_inner(item, phrases);
            }
        }
    }
}

fn match_indices_for_expr(
    expr: &fts5::expr::Fts5Expr,
    table: &fts5::Fts5Table,
    rowid: i64,
    column: i32,
    tokens: &[fts5::tokenizer::Fts5Token],
) -> Result<Vec<usize>> {
    let mut matches = Vec::new();
    let pos_map: HashMap<i32, usize> = tokens
        .iter()
        .enumerate()
        .map(|(idx, token)| (token.position, idx))
        .collect();
    collect_matches(expr, table, rowid, column, tokens, &pos_map, &mut matches)?;
    matches.sort_unstable();
    matches.dedup();
    Ok(matches)
}

fn collect_matches(
    expr: &fts5::expr::Fts5Expr,
    table: &fts5::Fts5Table,
    rowid: i64,
    column: i32,
    tokens: &[fts5::tokenizer::Fts5Token],
    pos_map: &HashMap<i32, usize>,
    matches: &mut Vec<usize>,
) -> Result<()> {
    match expr {
        fts5::expr::Fts5Expr::Term(_)
        | fts5::expr::Fts5Expr::Prefix(_)
        | fts5::expr::Fts5Expr::Phrase(_)
        | fts5::expr::Fts5Expr::Column(_, _) => {
            let positions = table.expr_positions_for_row(expr, rowid)?;
            let phrase_len = phrase_length(expr);
            for pos in positions {
                if pos.column != column {
                    continue;
                }
                if let Some(start_idx) = pos_map.get(&pos.offset) {
                    for idx in *start_idx..(*start_idx + phrase_len).min(tokens.len()) {
                        matches.push(idx);
                    }
                }
            }
        }
        fts5::expr::Fts5Expr::And(left, right) | fts5::expr::Fts5Expr::Or(left, right) => {
            collect_matches(left, table, rowid, column, tokens, pos_map, matches)?;
            collect_matches(right, table, rowid, column, tokens, pos_map, matches)?;
        }
        fts5::expr::Fts5Expr::Not(left, _) => {
            collect_matches(left, table, rowid, column, tokens, pos_map, matches)?;
        }
        fts5::expr::Fts5Expr::Near(items, _) => {
            for item in items {
                collect_matches(item, table, rowid, column, tokens, pos_map, matches)?;
            }
        }
    }
    Ok(())
}

fn phrase_length(expr: &fts5::expr::Fts5Expr) -> usize {
    match expr {
        fts5::expr::Fts5Expr::Phrase(terms) => terms.len().max(1),
        fts5::expr::Fts5Expr::Column(_, inner) => phrase_length(inner),
        _ => 1,
    }
}

fn apply_highlight(
    text: &str,
    tokens: &[fts5::tokenizer::Fts5Token],
    matches: &[usize],
    open: &str,
    close: &str,
) -> String {
    let mut out = String::new();
    let mut last = 0usize;
    let match_set: HashSet<usize> = matches.iter().copied().collect();
    for (idx, token) in tokens.iter().enumerate() {
        if token.start > last {
            out.push_str(&text[last..token.start]);
        }
        if match_set.contains(&idx) {
            out.push_str(open);
            out.push_str(&text[token.start..token.end]);
            out.push_str(close);
        } else {
            out.push_str(&text[token.start..token.end]);
        }
        last = token.end;
    }
    if last < text.len() {
        out.push_str(&text[last..]);
    }
    out
}

fn count_tokens(table: &fts5::Fts5Table, values: &[String]) -> Result<usize> {
    let mut count = 0usize;
    for value in values {
        count += table.tokenize(value)?.len();
    }
    Ok(count)
}
