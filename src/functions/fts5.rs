//! FTS5 helper functions (bm25/highlight/snippet).

use std::collections::HashSet;
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

pub fn func_bm25(_args: &[Value]) -> Result<Value> {
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
    let (terms, prefixes) = collect_terms(&expr);
    if terms.is_empty() && prefixes.is_empty() {
        return Ok(Value::Real(0.0));
    }

    let mut score = 0.0;
    for value in values {
        let tokens = table.tokenize(value)?;
        for token in tokens {
            if terms.iter().any(|term| term == &token.text) {
                score += 1.0;
            } else if prefixes.iter().any(|prefix| token.text.starts_with(prefix)) {
                score += 1.0;
            }
        }
    }

    Ok(Value::Real(score))
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
    let (terms, prefixes) = collect_terms(&expr);
    let tokens = table.tokenize(text)?;
    let matches = match_indices(&tokens, &terms, &prefixes);
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
    let (terms, prefixes) = collect_terms(&expr);

    let mut chosen = None;
    for (idx, text) in values.iter().enumerate() {
        if col >= 0 && idx != col as usize {
            continue;
        }
        let tokens = table.tokenize(text)?;
        let matches = match_indices(&tokens, &terms, &prefixes);
        if matches.is_empty() {
            continue;
        }
        chosen = Some((text.clone(), tokens, matches));
        break;
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

fn collect_terms(expr: &fts5::expr::Fts5Expr) -> (Vec<String>, Vec<String>) {
    let mut terms = Vec::new();
    let mut prefixes = Vec::new();
    collect_terms_inner(expr, &mut terms, &mut prefixes);
    (terms, prefixes)
}

fn collect_terms_inner(
    expr: &fts5::expr::Fts5Expr,
    terms: &mut Vec<String>,
    prefixes: &mut Vec<String>,
) {
    match expr {
        fts5::expr::Fts5Expr::Term(term) => terms.push(term.clone()),
        fts5::expr::Fts5Expr::Prefix(prefix) => prefixes.push(prefix.clone()),
        fts5::expr::Fts5Expr::Phrase(items) => terms.extend(items.iter().cloned()),
        fts5::expr::Fts5Expr::Column(_, inner) => collect_terms_inner(inner, terms, prefixes),
        fts5::expr::Fts5Expr::And(left, right)
        | fts5::expr::Fts5Expr::Or(left, right)
        | fts5::expr::Fts5Expr::Not(left, right) => {
            collect_terms_inner(left, terms, prefixes);
            collect_terms_inner(right, terms, prefixes);
        }
        fts5::expr::Fts5Expr::Near(exprs, _) => {
            for expr in exprs {
                collect_terms_inner(expr, terms, prefixes);
            }
        }
    }
}

fn match_indices(
    tokens: &[fts5::tokenizer::Fts5Token],
    terms: &[String],
    prefixes: &[String],
) -> Vec<usize> {
    let mut matches = Vec::new();
    for (idx, token) in tokens.iter().enumerate() {
        if terms.iter().any(|term| term == &token.text)
            || prefixes.iter().any(|prefix| token.text.starts_with(prefix))
        {
            matches.push(idx);
        }
    }
    matches
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
