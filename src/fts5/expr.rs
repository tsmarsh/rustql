use crate::error::{Error, ErrorCode, Result};

use super::tokenizer::Fts5Tokenizer;

#[derive(Debug, Clone)]
pub enum Fts5Expr {
    Term(String),
    Prefix(String),
    Phrase(Vec<String>),
    Column(String, Box<Fts5Expr>),
    And(Box<Fts5Expr>, Box<Fts5Expr>),
    Or(Box<Fts5Expr>, Box<Fts5Expr>),
    Not(Box<Fts5Expr>, Box<Fts5Expr>),
    Near(Vec<Fts5Expr>, i32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Fts5QueryToken {
    Word(String),
    Phrase(String),
    LParen,
    RParen,
    Comma,
    And,
    Or,
    Not,
    Near(i32),
}

pub fn parse_query(expr: &str, tokenizer: &dyn Fts5Tokenizer) -> Result<Fts5Expr> {
    let tokens = tokenize_query(expr)?;
    let mut parser = Fts5QueryParser {
        tokenizer,
        tokens,
        pos: 0,
    };
    parser.parse()
}

struct Fts5QueryParser<'a> {
    tokenizer: &'a dyn Fts5Tokenizer,
    tokens: Vec<Fts5QueryToken>,
    pos: usize,
}

impl<'a> Fts5QueryParser<'a> {
    fn parse(&mut self) -> Result<Fts5Expr> {
        let expr = self.parse_or()?;
        if self.peek().is_some() {
            return Err(Error::with_message(
                ErrorCode::Error,
                "unexpected token in query",
            ));
        }
        Ok(expr)
    }

    fn parse_or(&mut self) -> Result<Fts5Expr> {
        let mut expr = self.parse_and()?;
        while self.consume_if(&Fts5QueryToken::Or) {
            let right = self.parse_and()?;
            expr = Fts5Expr::Or(Box::new(expr), Box::new(right));
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<Fts5Expr> {
        let mut expr = self.parse_near()?;
        loop {
            if self.consume_if(&Fts5QueryToken::And) {
                let right = self.parse_near()?;
                expr = Fts5Expr::And(Box::new(expr), Box::new(right));
                continue;
            }
            if self.consume_if(&Fts5QueryToken::Not) {
                let right = self.parse_near()?;
                expr = Fts5Expr::Not(Box::new(expr), Box::new(right));
                continue;
            }
            if self.next_starts_expr() {
                let right = self.parse_near()?;
                expr = Fts5Expr::And(Box::new(expr), Box::new(right));
                continue;
            }
            break;
        }
        Ok(expr)
    }

    fn parse_near(&mut self) -> Result<Fts5Expr> {
        let mut expr = self.parse_primary()?;
        loop {
            let distance = match self.peek() {
                Some(Fts5QueryToken::Near(distance)) => *distance,
                _ => break,
            };
            self.advance();
            let right = self.parse_primary()?;
            expr = Fts5Expr::Near(vec![expr, right], distance);
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<Fts5Expr> {
        if let Some((col, rest)) = self.peek_column_token() {
            self.advance();
            let expr = if rest.is_empty() {
                self.parse_primary()?
            } else {
                self.parse_word(Fts5QueryToken::Word(rest))?
            };
            return Ok(Fts5Expr::Column(col, Box::new(expr)));
        }
        match self.peek() {
            Some(Fts5QueryToken::Word(_)) => {
                let token = self.advance().cloned().unwrap();
                self.parse_word(token)
            }
            Some(Fts5QueryToken::Phrase(_)) => {
                let token = self.advance().cloned().unwrap();
                self.parse_phrase(token)
            }
            Some(Fts5QueryToken::Near(_)) => self.parse_near_group(),
            Some(Fts5QueryToken::LParen) => {
                self.advance();
                let expr = self.parse_or()?;
                if !self.consume_if(&Fts5QueryToken::RParen) {
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

    fn parse_word(&self, token: Fts5QueryToken) -> Result<Fts5Expr> {
        let Fts5QueryToken::Word(text) = token else {
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
            return Ok(Fts5Expr::Prefix(tokens[0].text.clone()));
        }

        let tokens = self.tokenizer.tokenize(&text)?;
        if tokens.is_empty() {
            return Err(Error::with_message(
                ErrorCode::Error,
                "invalid term in query",
            ));
        }
        if tokens.len() == 1 {
            Ok(Fts5Expr::Term(tokens[0].text.clone()))
        } else {
            Ok(Fts5Expr::Phrase(
                tokens.into_iter().map(|t| t.text).collect(),
            ))
        }
    }

    fn parse_phrase(&self, token: Fts5QueryToken) -> Result<Fts5Expr> {
        let Fts5QueryToken::Phrase(text) = token else {
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
        Ok(Fts5Expr::Phrase(
            tokens.into_iter().map(|t| t.text).collect(),
        ))
    }

    fn parse_near_group(&mut self) -> Result<Fts5Expr> {
        let Some(Fts5QueryToken::Near(distance)) = self.advance().cloned() else {
            return Err(Error::with_message(ErrorCode::Error, "expected NEAR"));
        };
        if !self.consume_if(&Fts5QueryToken::LParen) {
            return Err(Error::with_message(
                ErrorCode::Error,
                "expected '(' after NEAR",
            ));
        }

        let mut exprs = Vec::new();
        let mut near_distance = distance;
        loop {
            match self.peek() {
                Some(Fts5QueryToken::RParen) => {
                    self.advance();
                    break;
                }
                Some(Fts5QueryToken::Comma) => {
                    self.advance();
                    let token = self.advance().cloned();
                    let Some(Fts5QueryToken::Word(value)) = token else {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            "expected distance value after comma",
                        ));
                    };
                    if let Ok(parsed) = value.parse::<i32>() {
                        near_distance = parsed;
                    }
                }
                _ => {
                    exprs.push(self.parse_primary()?);
                }
            }
        }

        if exprs.len() < 2 {
            return Err(Error::with_message(
                ErrorCode::Error,
                "NEAR requires at least two terms",
            ));
        }
        Ok(Fts5Expr::Near(exprs, near_distance))
    }

    fn peek(&self) -> Option<&Fts5QueryToken> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&Fts5QueryToken> {
        let tok = self.tokens.get(self.pos);
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn consume_if(&mut self, tok: &Fts5QueryToken) -> bool {
        if let Some(next) = self.peek() {
            if next == tok {
                self.pos += 1;
                return true;
            }
        }
        false
    }

    fn next_starts_expr(&self) -> bool {
        matches!(
            self.peek(),
            Some(Fts5QueryToken::Word(_))
                | Some(Fts5QueryToken::Phrase(_))
                | Some(Fts5QueryToken::Near(_))
                | Some(Fts5QueryToken::LParen)
        )
    }

    fn peek_column_token(&self) -> Option<(String, String)> {
        let Some(Fts5QueryToken::Word(word)) = self.peek() else {
            return None;
        };
        if let Some((col, rest)) = word.split_once(':') {
            if !col.is_empty() {
                return Some((col.to_string(), rest.to_string()));
            }
        } else if word.ends_with(':') {
            let col = word.trim_end_matches(':');
            if !col.is_empty() {
                return Some((col.to_string(), String::new()));
            }
        }
        None
    }
}

fn tokenize_query(expr: &str) -> Result<Vec<Fts5QueryToken>> {
    let mut tokens = Vec::new();
    let mut iter = expr.char_indices().peekable();

    while let Some((_, ch)) = iter.next() {
        if ch.is_ascii_whitespace() {
            continue;
        }

        if ch == '(' {
            tokens.push(Fts5QueryToken::LParen);
            continue;
        }
        if ch == ')' {
            tokens.push(Fts5QueryToken::RParen);
            continue;
        }
        if ch == ',' {
            tokens.push(Fts5QueryToken::Comma);
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
            tokens.push(Fts5QueryToken::Phrase(phrase));
            continue;
        }

        let mut word = String::new();
        word.push(ch);
        while let Some((_, next_ch)) = iter.peek() {
            if next_ch.is_ascii_whitespace()
                || *next_ch == '('
                || *next_ch == ')'
                || *next_ch == ','
                || *next_ch == '"'
            {
                break;
            }
            word.push(*next_ch);
            iter.next();
        }

        let upper = word.to_ascii_uppercase();
        if upper == "AND" {
            tokens.push(Fts5QueryToken::And);
        } else if upper == "OR" {
            tokens.push(Fts5QueryToken::Or);
        } else if upper == "NOT" {
            tokens.push(Fts5QueryToken::Not);
        } else if upper == "NEAR" {
            tokens.push(Fts5QueryToken::Near(10));
        } else if let Some(distance_str) = upper.strip_prefix("NEAR/") {
            if !distance_str.is_empty() && distance_str.chars().all(|c| c.is_ascii_digit()) {
                let distance = distance_str.parse::<i32>().unwrap_or(10);
                tokens.push(Fts5QueryToken::Near(distance));
            } else {
                tokens.push(Fts5QueryToken::Word(word));
            }
        } else {
            tokens.push(Fts5QueryToken::Word(word));
        }
    }

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::{parse_query, Fts5Expr};
    use crate::fts5::tokenizer::SimpleTokenizer;

    #[test]
    fn parse_basic_query() {
        let tokenizer = SimpleTokenizer::default();
        let expr = parse_query("alpha AND beta", &tokenizer).expect("parse query");
        match expr {
            Fts5Expr::And(left, right) => {
                assert!(matches!(*left, Fts5Expr::Term(_)));
                assert!(matches!(*right, Fts5Expr::Term(_)));
            }
            _ => panic!("expected AND expression"),
        }
    }
}
