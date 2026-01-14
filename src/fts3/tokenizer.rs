use std::collections::HashMap;
use std::sync::RwLock;

use lazy_static::lazy_static;

use super::fts3::{Fts3Tokenizer, SimpleTokenizer};
use super::porter::porter_stem;
use super::unicode::{unicode_fold, unicode_isalnum, unicode_isdiacritic};
use crate::error::{Error, ErrorCode, Result};

pub type TokenizerFactory = fn(&[&str]) -> Result<Box<dyn Fts3Tokenizer>>;

lazy_static! {
    static ref TOKENIZERS: RwLock<HashMap<String, TokenizerFactory>> = {
        let mut map: HashMap<String, TokenizerFactory> = HashMap::new();
        map.insert("simple".to_string(), |args| {
            Ok(Box::new(SimpleTokenizer::new(args)?) as Box<dyn Fts3Tokenizer>)
        });
        map.insert("unicode61".to_string(), |args| {
            Ok(Box::new(Unicode61Tokenizer::new(args)?) as Box<dyn Fts3Tokenizer>)
        });
        map.insert("porter".to_string(), |_args| {
            Ok(Box::new(PorterTokenizer::new()) as Box<dyn Fts3Tokenizer>)
        });
        RwLock::new(map)
    };
}

pub fn register_tokenizer(name: &str, factory: TokenizerFactory) {
    let mut registry = TOKENIZERS.write().expect("tokenizer registry lock");
    registry.insert(name.to_ascii_lowercase(), factory);
}

pub fn create_tokenizer(name: &str, args: &[&str]) -> Result<Box<dyn Fts3Tokenizer>> {
    let registry = TOKENIZERS.read().expect("tokenizer registry lock");
    let key = name.to_ascii_lowercase();
    let factory = registry.get(&key).ok_or_else(|| {
        Error::with_message(ErrorCode::Error, format!("unknown tokenizer: {}", name))
    })?;
    factory(args)
}

pub fn parse_tokenize_arg(arg: &str) -> Option<(String, Vec<String>)> {
    let trimmed = arg.trim();
    let spec = trimmed
        .strip_prefix("tokenize=")
        .or_else(|| trimmed.strip_prefix("TOKENIZE="))?;
    let tokens = split_tokenize_args(spec);
    if tokens.is_empty() {
        return None;
    }
    let name = tokens[0].clone();
    let args = tokens[1..].to_vec();
    Some((name, args))
}

fn split_tokenize_args(spec: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut chars = spec.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch.is_ascii_whitespace() {
            continue;
        }
        if ch == '"' || ch == '\'' {
            let quote = ch;
            let mut value = String::new();
            while let Some(next) = chars.next() {
                if next == quote {
                    if let Some(peek) = chars.peek() {
                        if *peek == quote {
                            chars.next();
                            value.push(quote);
                            continue;
                        }
                    }
                    break;
                }
                value.push(next);
            }
            args.push(value);
            continue;
        }

        let mut value = String::new();
        value.push(ch);
        while let Some(peek) = chars.peek() {
            if peek.is_ascii_whitespace() {
                break;
            }
            value.push(*peek);
            chars.next();
        }
        args.push(value);
    }

    args
}

#[derive(Debug, Clone)]
pub struct Unicode61Tokenizer {
    remove_diacritics: i32,
    token_chars: Vec<i32>,
    separators: Vec<i32>,
}

impl Unicode61Tokenizer {
    pub fn new(args: &[&str]) -> Result<Self> {
        let mut tokenizer = Self {
            remove_diacritics: 1,
            token_chars: Vec::new(),
            separators: Vec::new(),
        };

        let mut idx = 0usize;
        while idx < args.len() {
            match args[idx] {
                "remove_diacritics" => {
                    idx += 1;
                    if let Some(value) = args.get(idx) {
                        tokenizer.remove_diacritics = match *value {
                            "0" => 0,
                            "2" => 2,
                            _ => 1,
                        };
                    }
                }
                "tokenchars" => {
                    idx += 1;
                    if let Some(value) = args.get(idx) {
                        tokenizer.add_exceptions(true, value);
                    }
                }
                "separators" => {
                    idx += 1;
                    if let Some(value) = args.get(idx) {
                        tokenizer.add_exceptions(false, value);
                    }
                }
                _ => {}
            }
            idx += 1;
        }

        Ok(tokenizer)
    }

    fn add_exceptions(&mut self, token_chars: bool, value: &str) {
        let mut list = if token_chars {
            std::mem::take(&mut self.token_chars)
        } else {
            std::mem::take(&mut self.separators)
        };

        for ch in value.chars() {
            let code = ch as i32;
            if unicode_isdiacritic(code) {
                continue;
            }
            if !list.contains(&code) {
                list.push(code);
            }
        }
        list.sort_unstable();
        list.dedup();

        if token_chars {
            self.token_chars = list;
        } else {
            self.separators = list;
        }
    }

    fn is_exception(list: &[i32], code: i32) -> bool {
        list.binary_search(&code).is_ok()
    }

    fn is_token_char(&self, code: i32) -> bool {
        if unicode_isdiacritic(code) {
            return false;
        }
        if Self::is_exception(&self.separators, code) {
            return false;
        }
        if Self::is_exception(&self.token_chars, code) {
            return true;
        }
        unicode_isalnum(code)
    }
}

impl Fts3Tokenizer for Unicode61Tokenizer {
    fn tokenize(&self, text: &str) -> Result<Vec<super::fts3::Fts3Token>> {
        let mut tokens = Vec::new();
        let mut position = 0i32;
        let mut iter = text.char_indices().peekable();

        while let Some((start_idx, ch)) = iter.peek().cloned() {
            let code = ch as i32;
            if !self.is_token_char(code) {
                iter.next();
                continue;
            }

            let start = start_idx;
            let mut token = String::new();
            while let Some((_idx, ch)) = iter.peek().cloned() {
                let code = ch as i32;
                if !self.is_token_char(code) {
                    break;
                }
                iter.next();
                let folded = unicode_fold(code, self.remove_diacritics);
                if let Some(ch) = char::from_u32(folded as u32) {
                    token.push(ch);
                }
            }
            let end = iter.peek().map(|(idx, _)| *idx).unwrap_or(text.len());

            tokens.push(super::fts3::Fts3Token {
                text: token,
                position,
                start,
                end,
            });
            position += 1;
        }

        Ok(tokens)
    }
}

const PORTER_ID_CHAR: [u8; 80] = [
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0,
];

fn porter_is_delim(ch: u8) -> bool {
    (ch & 0x80) == 0 && (ch < 0x30 || PORTER_ID_CHAR[(ch - 0x30) as usize] == 0)
}

pub struct PorterTokenizer;

impl PorterTokenizer {
    pub fn new() -> Self {
        Self
    }
}

impl Fts3Tokenizer for PorterTokenizer {
    fn tokenize(&self, text: &str) -> Result<Vec<super::fts3::Fts3Token>> {
        let bytes = text.as_bytes();
        let mut tokens = Vec::new();
        let mut position = 0i32;
        let mut idx = 0usize;

        while idx < bytes.len() {
            while idx < bytes.len() && porter_is_delim(bytes[idx]) {
                idx += 1;
            }
            let start = idx;
            while idx < bytes.len() && !porter_is_delim(bytes[idx]) {
                idx += 1;
            }
            if idx > start {
                let token_text = String::from_utf8_lossy(&bytes[start..idx]).to_string();
                let stemmed = porter_stem(&token_text);
                tokens.push(super::fts3::Fts3Token {
                    text: stemmed,
                    position,
                    start,
                    end: idx,
                });
                position += 1;
            }
        }

        Ok(tokens)
    }
}
