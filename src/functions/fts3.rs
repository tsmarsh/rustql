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

fn tokenize_query(query: &str) -> Vec<String> {
    query
        .split_whitespace()
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_lowercase())
        .collect()
}

#[derive(Debug, Clone)]
struct Fts3Context {
    table: String,
    rowid: i64,
}

lazy_static! {
    static ref FTS3_CONTEXT: Mutex<Option<Fts3Context>> = Mutex::new(None);
}

pub fn set_fts3_context(table: Option<String>, rowid: Option<i64>) {
    let mut guard = FTS3_CONTEXT.lock().expect("fts3 context lock");
    if let (Some(table), Some(rowid)) = (table, rowid) {
        *guard = Some(Fts3Context { table, rowid });
    } else {
        *guard = None;
    }
}

fn get_fts3_context() -> Option<Fts3Context> {
    let guard = FTS3_CONTEXT.lock().expect("fts3 context lock");
    guard.clone()
}

/// snippet(text, query [, start, end, ellipsis])
pub fn func_snippet(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 5 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "snippet() expects 2 to 5 arguments",
        ));
    }

    let text = value_to_string(&args[0]);
    let query = value_to_string(&args[1]);
    let start = args.get(2).map(value_to_string).unwrap_or_else(|| "<b>".to_string());
    let end = args.get(3).map(value_to_string).unwrap_or_else(|| "</b>".to_string());
    let ellipsis = args
        .get(4)
        .map(value_to_string)
        .unwrap_or_else(|| "...".to_string());

    let lower_text = text.to_ascii_lowercase();
    let terms = tokenize_query(&query);
    let mut match_pos = None;
    let mut match_len = 0usize;

    for term in &terms {
        if term.is_empty() {
            continue;
        }
        if let Some(pos) = lower_text.find(term) {
            match_pos = Some(pos);
            match_len = term.len();
            break;
        }
    }

    let Some(pos) = match_pos else {
        return Ok(Value::Text(text));
    };

    let window = 20usize;
    let start_idx = pos.saturating_sub(window);
    let end_idx = (pos + match_len + window).min(text.len());
    let prefix = if start_idx > 0 { &ellipsis } else { "" };
    let suffix = if end_idx < text.len() { &ellipsis } else { "" };

    let mut snippet = String::new();
    snippet.push_str(prefix);
    snippet.push_str(&text[start_idx..pos]);
    snippet.push_str(&start);
    snippet.push_str(&text[pos..pos + match_len]);
    snippet.push_str(&end);
    snippet.push_str(&text[pos + match_len..end_idx]);
    snippet.push_str(suffix);

    Ok(Value::Text(snippet))
}

/// offsets(text, query)
pub fn func_offsets(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "offsets() expects 2 arguments",
        ));
    }

    let text = value_to_string(&args[0]);
    let query = value_to_string(&args[1]);
    let lower_text = text.to_ascii_lowercase();
    let terms = tokenize_query(&query);

    let mut parts = Vec::new();
    for term in &terms {
        if term.is_empty() {
            continue;
        }
        let mut search_start = 0usize;
        while let Some(pos) = lower_text[search_start..].find(term) {
            let absolute = search_start + pos;
            parts.push(format!("0 0 {} {}", absolute, term.len()));
            search_start = absolute + term.len();
        }
    }

    Ok(Value::Text(parts.join(" ")))
}

/// matchinfo(text, query)
pub fn func_matchinfo(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "matchinfo() expects 2 arguments",
        ));
    }

    let query = value_to_string(&args[1]);
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
    let buf = table.matchinfo(&query, ctx.rowid)?;
    Ok(Value::Blob(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snippet_basic() {
        let args = [
            Value::Text("hello world".to_string()),
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
        let args = [
            Value::Text("alpha beta alpha".to_string()),
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
            Box::new(fts3::SimpleTokenizer),
        );
        table.insert(1, &["alpha beta alpha"]).expect("insert");
        fts3::register_table(table);
        set_fts3_context(Some("docs".to_string()), Some(1));

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
