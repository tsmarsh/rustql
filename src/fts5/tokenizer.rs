use std::collections::HashMap;
use std::sync::RwLock;

use lazy_static::lazy_static;

use super::porter::porter_stem;
use super::unicode::{unicode_fold, unicode_isalnum, unicode_isdiacritic};
use crate::error::{Error, ErrorCode, Result};

pub trait Fts5Tokenizer: Send + Sync {
    fn tokenize(&self, text: &str) -> Result<Vec<Fts5Token>>;
}

#[derive(Debug, Clone)]
pub struct Fts5Token {
    pub text: String,
    pub position: i32,
    pub start: usize,
    pub end: usize,
}

pub type TokenizerFactory = fn(&[&str]) -> Result<Box<dyn Fts5Tokenizer>>;

lazy_static! {
    static ref TOKENIZERS: RwLock<HashMap<String, TokenizerFactory>> = {
        let mut map: HashMap<String, TokenizerFactory> = HashMap::new();
        map.insert("simple".to_string(), |args| {
            Ok(Box::new(SimpleTokenizer::new(args)?) as Box<dyn Fts5Tokenizer>)
        });
        map.insert("unicode61".to_string(), |args| {
            Ok(Box::new(Unicode61Tokenizer::new(args)?) as Box<dyn Fts5Tokenizer>)
        });
        map.insert("porter".to_string(), |_args| {
            Ok(Box::new(PorterTokenizer::new()) as Box<dyn Fts5Tokenizer>)
        });
        RwLock::new(map)
    };
}

pub fn register_tokenizer(name: &str, factory: TokenizerFactory) {
    let mut registry = TOKENIZERS.write().expect("fts5 tokenizer registry lock");
    registry.insert(name.to_ascii_lowercase(), factory);
}

pub fn create_tokenizer(name: &str, args: &[&str]) -> Result<Box<dyn Fts5Tokenizer>> {
    let registry = TOKENIZERS.read().expect("fts5 tokenizer registry lock");
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

pub struct SimpleTokenizer {
    delim: [bool; 128],
}

impl Default for SimpleTokenizer {
    fn default() -> Self {
        Self::new(&[]).unwrap_or(Self {
            delim: [false; 128],
        })
    }
}

impl SimpleTokenizer {
    pub fn new(args: &[&str]) -> Result<Self> {
        let mut tokenizer = Self {
            delim: [false; 128],
        };
        if let Some(delims) = args.first() {
            for ch in delims.bytes() {
                if ch >= 0x80 {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "simple tokenizer does not accept UTF-8 delimiters",
                    ));
                }
                tokenizer.delim[ch as usize] = true;
            }
        } else {
            for i in 1..0x80 {
                let ch = i as u8;
                tokenizer.delim[i] = !ch.is_ascii_alphanumeric();
            }
        }
        Ok(tokenizer)
    }

    fn is_delim(&self, ch: u8) -> bool {
        ch < 0x80 && self.delim[ch as usize]
    }
}

impl Fts5Tokenizer for SimpleTokenizer {
    fn tokenize(&self, text: &str) -> Result<Vec<Fts5Token>> {
        let mut tokens = Vec::new();
        let mut pos = 0i32;
        let mut idx = 0usize;
        let bytes = text.as_bytes();

        while idx < bytes.len() {
            while idx < bytes.len() && self.is_delim(bytes[idx]) {
                idx += 1;
            }
            let start = idx;
            while idx < bytes.len() && !self.is_delim(bytes[idx]) {
                idx += 1;
            }
            if idx > start {
                let mut token_bytes = bytes[start..idx].to_vec();
                for byte in &mut token_bytes {
                    if (b'A'..=b'Z').contains(byte) {
                        *byte = *byte - b'A' + b'a';
                    }
                }
                let token = String::from_utf8_lossy(&token_bytes).to_string();
                tokens.push(Fts5Token {
                    text: token,
                    position: pos,
                    start,
                    end: idx,
                });
                pos += 1;
            }
        }
        Ok(tokens)
    }
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

impl Fts5Tokenizer for Unicode61Tokenizer {
    fn tokenize(&self, text: &str) -> Result<Vec<Fts5Token>> {
        let mut tokens = Vec::new();
        let mut position = 0i32;
        let mut iter = text.char_indices().peekable();

        while let Some((start_idx, ch)) = iter.peek().cloned() {
            let code = ch as i32;
            if !self.is_token_char(code) {
                iter.next();
                continue;
            }

            let mut token = String::new();
            let mut end_idx = start_idx;
            while let Some((idx, ch)) = iter.peek().cloned() {
                let code = ch as i32;
                if !self.is_token_char(code) {
                    break;
                }
                let folded = unicode_fold(code, self.remove_diacritics);
                if folded >= 0 {
                    if let Some(folded_ch) = std::char::from_u32(folded as u32) {
                        token.push(folded_ch);
                    }
                }
                end_idx = idx + ch.len_utf8();
                iter.next();
            }

            tokens.push(Fts5Token {
                text: token,
                position,
                start: start_idx,
                end: end_idx,
            });
            position += 1;
        }

        Ok(tokens)
    }
}

#[derive(Debug, Clone)]
pub struct PorterTokenizer {
    base: Unicode61Tokenizer,
}

impl PorterTokenizer {
    pub fn new() -> Self {
        Self {
            base: Unicode61Tokenizer {
                remove_diacritics: 1,
                token_chars: Vec::new(),
                separators: Vec::new(),
            },
        }
    }
}

impl Fts5Tokenizer for PorterTokenizer {
    fn tokenize(&self, text: &str) -> Result<Vec<Fts5Token>> {
        let mut tokens = Vec::new();
        for token in self.base.tokenize(text)? {
            let stemmed = porter_stem(&token.text);
            tokens.push(Fts5Token {
                text: stemmed,
                ..token
            });
        }
        Ok(tokens)
    }
}
