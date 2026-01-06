# Translate fts3_expr.c and fts3_snippet.c - FTS3 Expressions and Snippets

## Overview
Translate FTS3 query expression parser and snippet/highlight generation.

## Source Reference
- `sqlite3/ext/fts3/fts3_expr.c` - Expression parsing
- `sqlite3/ext/fts3/fts3_snippet.c` - Snippet generation

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### FTS3 Expression
```rust
/// FTS3 query expression
#[derive(Debug, Clone)]
pub enum Fts3Expr {
    /// Single term
    Term(String),
    /// Phrase "word1 word2"
    Phrase(Vec<String>),
    /// Prefix term*
    Prefix(String),
    /// NEAR(a b, n)
    Near(Vec<Fts3Expr>, i32),
    /// AND
    And(Box<Fts3Expr>, Box<Fts3Expr>),
    /// OR
    Or(Box<Fts3Expr>, Box<Fts3Expr>),
    /// NOT
    Not(Box<Fts3Expr>, Box<Fts3Expr>),
}
```

### Snippet Context
```rust
/// Context for snippet generation
pub struct SnippetContext {
    /// Original text
    text: String,
    /// Token positions
    tokens: Vec<TokenInfo>,
    /// Matching token indices
    matches: Vec<usize>,
    /// Open tag
    open_tag: String,
    /// Close tag
    close_tag: String,
    /// Ellipsis
    ellipsis: String,
    /// Max tokens
    max_tokens: i32,
}

struct TokenInfo {
    start: usize,
    end: usize,
    is_match: bool,
}
```

## Expression Parser

```rust
/// Parse FTS3 query string
pub fn fts3_parse_expr(query: &str) -> Result<Fts3Expr> {
    let mut parser = Fts3ExprParser::new(query);
    parser.parse()
}

struct Fts3ExprParser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Fts3ExprParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse(&mut self) -> Result<Fts3Expr> {
        self.skip_whitespace();
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Fts3Expr> {
        let mut left = self.parse_and()?;

        while self.match_keyword("OR") {
            self.skip_whitespace();
            let right = self.parse_and()?;
            left = Fts3Expr::Or(Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Fts3Expr> {
        let mut left = self.parse_not()?;

        // Implicit AND between terms
        while self.match_keyword("AND") || self.is_start_of_term() {
            self.skip_whitespace();
            let right = self.parse_not()?;
            left = Fts3Expr::And(Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Fts3Expr> {
        let mut neg = false;

        if self.match_keyword("NOT") || self.match_char('-') {
            neg = true;
            self.skip_whitespace();
        }

        let expr = self.parse_primary()?;

        if neg {
            // NOT expr is equivalent to (all docs) NOT expr
            // For now, just return as-is (will be handled at query time)
            Ok(Fts3Expr::Not(
                Box::new(Fts3Expr::Term("*".to_string())),
                Box::new(expr)
            ))
        } else {
            Ok(expr)
        }
    }

    fn parse_primary(&mut self) -> Result<Fts3Expr> {
        self.skip_whitespace();

        if self.match_char('(') {
            let expr = self.parse()?;
            self.expect_char(')')?;
            Ok(expr)
        } else if self.match_char('"') {
            self.parse_phrase()
        } else if self.match_keyword("NEAR") {
            self.parse_near()
        } else {
            self.parse_term()
        }
    }

    fn parse_phrase(&mut self) -> Result<Fts3Expr> {
        let mut words = Vec::new();

        while !self.match_char('"') && !self.is_eof() {
            self.skip_whitespace();
            if let Some(word) = self.read_word() {
                words.push(word);
            }
        }

        Ok(Fts3Expr::Phrase(words))
    }

    fn parse_near(&mut self) -> Result<Fts3Expr> {
        // NEAR/N(term1 term2)
        let distance = if self.match_char('/') {
            self.read_number()? as i32
        } else {
            10 // default
        };

        self.expect_char('(')?;

        let mut terms = Vec::new();
        while !self.match_char(')') && !self.is_eof() {
            self.skip_whitespace();
            terms.push(self.parse_term()?);
        }

        Ok(Fts3Expr::Near(terms, distance))
    }

    fn parse_term(&mut self) -> Result<Fts3Expr> {
        let word = self.read_word()
            .ok_or_else(|| Error::with_message(ErrorCode::Error, "expected term"))?;

        if self.match_char('*') {
            Ok(Fts3Expr::Prefix(word))
        } else {
            Ok(Fts3Expr::Term(word))
        }
    }

    fn read_word(&mut self) -> Option<String> {
        let start = self.pos;
        while self.pos < self.input.len() {
            let c = self.input[self.pos..].chars().next()?;
            if c.is_alphanumeric() || c == '_' {
                self.pos += c.len_utf8();
            } else {
                break;
            }
        }

        if self.pos > start {
            Some(self.input[start..self.pos].to_string())
        } else {
            None
        }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() {
            if self.input[self.pos..].starts_with(char::is_whitespace) {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn match_keyword(&mut self, kw: &str) -> bool {
        if self.input[self.pos..].to_uppercase().starts_with(kw) {
            let next = self.pos + kw.len();
            if next >= self.input.len() ||
               !self.input[next..].starts_with(char::is_alphanumeric) {
                self.pos = next;
                return true;
            }
        }
        false
    }

    fn match_char(&mut self, c: char) -> bool {
        if self.input[self.pos..].starts_with(c) {
            self.pos += c.len_utf8();
            true
        } else {
            false
        }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.input.len()
    }
}
```

## Snippet Generation

```rust
impl SnippetContext {
    pub fn new(text: &str, expr: &Fts3Expr, tokenizer: &dyn Fts3Tokenizer) -> Result<Self> {
        let mut ctx = Self {
            text: text.to_string(),
            tokens: Vec::new(),
            matches: Vec::new(),
            open_tag: "<b>".to_string(),
            close_tag: "</b>".to_string(),
            ellipsis: "...".to_string(),
            max_tokens: 64,
        };

        // Tokenize and mark matches
        tokenizer.tokenize(text, |token, start, end| {
            let is_match = ctx.is_matching_token(token, expr);
            let idx = ctx.tokens.len();

            ctx.tokens.push(TokenInfo {
                start: start as usize,
                end: end as usize,
                is_match,
            });

            if is_match {
                ctx.matches.push(idx);
            }

            Ok(())
        })?;

        Ok(ctx)
    }

    fn is_matching_token(&self, token: &str, expr: &Fts3Expr) -> bool {
        match expr {
            Fts3Expr::Term(t) => token.eq_ignore_ascii_case(t),
            Fts3Expr::Prefix(p) => token.to_lowercase().starts_with(&p.to_lowercase()),
            Fts3Expr::Phrase(terms) => terms.iter().any(|t| token.eq_ignore_ascii_case(t)),
            Fts3Expr::And(l, r) => self.is_matching_token(token, l) || self.is_matching_token(token, r),
            Fts3Expr::Or(l, r) => self.is_matching_token(token, l) || self.is_matching_token(token, r),
            Fts3Expr::Not(l, _) => self.is_matching_token(token, l),
            Fts3Expr::Near(terms, _) => terms.iter().any(|t| self.is_matching_token(token, t)),
        }
    }

    /// Generate snippet with highlighted matches
    pub fn snippet(&self) -> String {
        if self.matches.is_empty() {
            // No matches, return start of text
            return self.truncate_text(0, self.max_tokens as usize);
        }

        // Find best window around matches
        let (start, end) = self.find_best_window();

        self.format_snippet(start, end)
    }

    fn find_best_window(&self) -> (usize, usize) {
        // Find window with most matches
        let mut best_start = 0;
        let mut best_end = self.max_tokens as usize;
        let mut best_score = 0;

        for &match_idx in &self.matches {
            let start = match_idx.saturating_sub(self.max_tokens as usize / 2);
            let end = (start + self.max_tokens as usize).min(self.tokens.len());

            let score = self.matches.iter()
                .filter(|&&m| m >= start && m < end)
                .count();

            if score > best_score {
                best_score = score;
                best_start = start;
                best_end = end;
            }
        }

        (best_start, best_end)
    }

    fn format_snippet(&self, start: usize, end: usize) -> String {
        let mut result = String::new();

        // Add leading ellipsis if not at start
        if start > 0 {
            result.push_str(&self.ellipsis);
        }

        // Format tokens with highlighting
        for i in start..end {
            let token = &self.tokens[i];
            let text = &self.text[token.start..token.end];

            if token.is_match {
                result.push_str(&self.open_tag);
                result.push_str(text);
                result.push_str(&self.close_tag);
            } else {
                result.push_str(text);
            }

            // Add space between tokens
            if i + 1 < end && i + 1 < self.tokens.len() {
                let gap_start = token.end;
                let gap_end = self.tokens[i + 1].start;
                result.push_str(&self.text[gap_start..gap_end]);
            }
        }

        // Add trailing ellipsis if not at end
        if end < self.tokens.len() {
            result.push_str(&self.ellipsis);
        }

        result
    }

    fn truncate_text(&self, start: usize, max_tokens: usize) -> String {
        let end = (start + max_tokens).min(self.tokens.len());

        if end == 0 {
            return String::new();
        }

        let text_start = self.tokens[start].start;
        let text_end = self.tokens[end - 1].end;

        let mut result = self.text[text_start..text_end].to_string();
        if end < self.tokens.len() {
            result.push_str(&self.ellipsis);
        }

        result
    }
}
```

## Offsets Function

```rust
/// Generate offsets for match positions
pub fn fts3_offsets(text: &str, expr: &Fts3Expr, tokenizer: &dyn Fts3Tokenizer) -> Result<String> {
    let mut offsets = Vec::new();

    tokenizer.tokenize(text, |token, start, end| {
        if matches_expr(token, expr) {
            // Format: col term_idx byte_start byte_len
            offsets.push(format!("0 0 {} {}", start, end - start));
        }
        Ok(())
    })?;

    Ok(offsets.join(" "))
}

fn matches_expr(token: &str, expr: &Fts3Expr) -> bool {
    match expr {
        Fts3Expr::Term(t) => token.eq_ignore_ascii_case(t),
        Fts3Expr::Prefix(p) => token.to_lowercase().starts_with(&p.to_lowercase()),
        Fts3Expr::Phrase(terms) => terms.iter().any(|t| token.eq_ignore_ascii_case(t)),
        Fts3Expr::And(l, r) | Fts3Expr::Or(l, r) => matches_expr(token, l) || matches_expr(token, r),
        Fts3Expr::Not(l, _) => matches_expr(token, l),
        Fts3Expr::Near(terms, _) => terms.iter().any(|t| matches_expr(token, t)),
    }
}
```

## Matchinfo Function

```rust
/// Generate match info blob
pub fn fts3_matchinfo(cursor: &Fts3Cursor, format: &str) -> Result<Vec<u8>> {
    let mut result = Vec::new();

    for c in format.chars() {
        match c {
            'p' => {
                // Number of phrases
                let n = cursor.phrase_count();
                result.extend_from_slice(&(n as u32).to_le_bytes());
            }
            'c' => {
                // Number of columns
                let n = cursor.column_count();
                result.extend_from_slice(&(n as u32).to_le_bytes());
            }
            'x' => {
                // Phrase/column hit counts
                for phrase in 0..cursor.phrase_count() {
                    for col in 0..cursor.column_count() {
                        let hits_this = cursor.phrase_hits(phrase, col);
                        let hits_total = cursor.phrase_hits_total(phrase, col);
                        let docs_with = cursor.docs_with_phrase(phrase, col);

                        result.extend_from_slice(&(hits_this as u32).to_le_bytes());
                        result.extend_from_slice(&(hits_total as u32).to_le_bytes());
                        result.extend_from_slice(&(docs_with as u32).to_le_bytes());
                    }
                }
            }
            'n' => {
                // Total rows
                let n = cursor.total_rows();
                result.extend_from_slice(&(n as u32).to_le_bytes());
            }
            's' => {
                // Column sizes
                for col in 0..cursor.column_count() {
                    let size = cursor.column_size(col);
                    result.extend_from_slice(&(size as u32).to_le_bytes());
                }
            }
            _ => {}
        }
    }

    Ok(result)
}
```

## Acceptance Criteria
- [ ] FTS3 expression parsing
- [ ] Term queries
- [ ] Phrase queries
- [ ] Prefix queries
- [ ] NEAR queries
- [ ] Boolean operators (AND, OR, NOT)
- [ ] snippet() function
- [ ] offsets() function
- [ ] matchinfo() function
- [ ] Highlight tags customization
- [ ] Ellipsis handling
- [ ] Window optimization for snippets
