# Translate tokenize.c - SQL Tokenizer

## Overview
Translate the SQL tokenizer (lexer) which breaks SQL text into tokens for parsing.

## Source Reference
- `sqlite3/src/tokenize.c` - 899 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Token
```rust
#[derive(Debug, Clone)]
pub struct Token {
    /// Token type
    pub kind: TokenKind,

    /// Token text (slice into source)
    pub text: String,

    /// Byte offset in source
    pub offset: usize,

    /// Length in bytes
    pub len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    // Literals
    Integer,
    Float,
    String,
    Blob,

    // Identifiers
    Id,
    Variable,  // ?NNN, :name, @name, $name

    // Keywords (partial list)
    Select, From, Where, And, Or, Not,
    Insert, Into, Values, Update, Set, Delete,
    Create, Table, Index, Drop, Alter,
    Join, Left, Right, Inner, Outer, Cross, On,
    Order, By, Asc, Desc, Limit, Offset,
    Group, Having, Distinct, All, As,
    Null, True, False,
    Primary, Key, Foreign, References,
    Unique, Check, Default, Autoincrement,
    Begin, Commit, Rollback, Transaction,
    // ... many more

    // Operators
    Plus, Minus, Star, Slash, Percent,
    Eq, Ne, Lt, Le, Gt, Ge,
    BitAnd, BitOr, BitNot, LShift, RShift,
    Concat,  // ||
    Is, In, Like, Glob, Match, Regexp,
    Between, Case, When, Then, Else, End,

    // Punctuation
    LParen, RParen,
    Comma, Semi, Dot,

    // Special
    Eof,
    Illegal,
    Space,
    Comment,
}
```

### Tokenizer State
```rust
pub struct Tokenizer<'a> {
    /// Source SQL text
    source: &'a str,

    /// Current byte position
    pos: usize,

    /// Current line number (for errors)
    line: usize,

    /// Column number
    col: usize,
}
```

## Key Functions

### Tokenizer Implementation

```rust
impl<'a> Tokenizer<'a> {
    pub fn new(source: &'a str) -> Self {
        Tokenizer {
            source,
            pos: 0,
            line: 1,
            col: 1,
        }
    }

    /// Get next token
    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace_and_comments();

        if self.pos >= self.source.len() {
            return Token {
                kind: TokenKind::Eof,
                text: String::new(),
                offset: self.pos,
                len: 0,
            };
        }

        let start = self.pos;
        let c = self.current_char();

        let kind = match c {
            // Single-character tokens
            '(' => { self.advance(); TokenKind::LParen }
            ')' => { self.advance(); TokenKind::RParen }
            ',' => { self.advance(); TokenKind::Comma }
            ';' => { self.advance(); TokenKind::Semi }
            '+' => { self.advance(); TokenKind::Plus }
            '*' => { self.advance(); TokenKind::Star }
            '/' => { self.advance(); TokenKind::Slash }
            '%' => { self.advance(); TokenKind::Percent }
            '&' => { self.advance(); TokenKind::BitAnd }
            '~' => { self.advance(); TokenKind::BitNot }

            // Multi-character operators
            '-' => self.scan_minus_or_comment(),
            '|' => self.scan_pipe(),
            '<' => self.scan_less_than(),
            '>' => self.scan_greater_than(),
            '=' => self.scan_equals(),
            '!' => self.scan_bang(),
            '.' => self.scan_dot_or_float(),

            // Strings
            '\'' => self.scan_string(),
            '"' => self.scan_quoted_id(),
            '`' => self.scan_backtick_id(),
            '[' => self.scan_bracket_id(),

            // Blob literal
            'x' | 'X' if self.peek() == Some('\'') => self.scan_blob(),

            // Numbers
            '0'..='9' => self.scan_number(),

            // Variables
            '?' => self.scan_variable_question(),
            ':' => self.scan_variable_colon(),
            '@' => self.scan_variable_at(),
            '$' => self.scan_variable_dollar(),

            // Identifiers and keywords
            'a'..='z' | 'A'..='Z' | '_' => self.scan_identifier(),

            _ => {
                self.advance();
                TokenKind::Illegal
            }
        };

        Token {
            kind,
            text: self.source[start..self.pos].to_string(),
            offset: start,
            len: self.pos - start,
        }
    }
}
```

### String Scanning

```rust
impl<'a> Tokenizer<'a> {
    fn scan_string(&mut self) -> TokenKind {
        self.advance(); // consume opening quote

        while self.pos < self.source.len() {
            match self.current_char() {
                '\'' => {
                    self.advance();
                    // Check for escaped quote ''
                    if self.current_char() == '\'' {
                        self.advance();
                    } else {
                        return TokenKind::String;
                    }
                }
                _ => self.advance(),
            }
        }

        TokenKind::Illegal // Unterminated string
    }
}
```

### Number Scanning

```rust
impl<'a> Tokenizer<'a> {
    fn scan_number(&mut self) -> TokenKind {
        // Integer part
        while self.current_char().is_ascii_digit() {
            self.advance();
        }

        // Check for hex: 0x...
        if self.source[self.pos - 1..].starts_with("0x") ||
           self.source[self.pos - 1..].starts_with("0X") {
            while self.current_char().is_ascii_hexdigit() {
                self.advance();
            }
            return TokenKind::Integer;
        }

        // Decimal point
        if self.current_char() == '.' && self.peek().map_or(false, |c| c.is_ascii_digit()) {
            self.advance(); // consume '.'
            while self.current_char().is_ascii_digit() {
                self.advance();
            }

            // Exponent
            if self.current_char() == 'e' || self.current_char() == 'E' {
                self.advance();
                if self.current_char() == '+' || self.current_char() == '-' {
                    self.advance();
                }
                while self.current_char().is_ascii_digit() {
                    self.advance();
                }
            }

            return TokenKind::Float;
        }

        // Exponent without decimal
        if self.current_char() == 'e' || self.current_char() == 'E' {
            self.advance();
            if self.current_char() == '+' || self.current_char() == '-' {
                self.advance();
            }
            while self.current_char().is_ascii_digit() {
                self.advance();
            }
            return TokenKind::Float;
        }

        TokenKind::Integer
    }
}
```

### Keyword Recognition

```rust
impl<'a> Tokenizer<'a> {
    fn scan_identifier(&mut self) -> TokenKind {
        let start = self.pos;

        while self.current_char().is_ascii_alphanumeric() ||
              self.current_char() == '_' {
            self.advance();
        }

        let text = &self.source[start..self.pos];

        // Check if keyword (case-insensitive)
        match text.to_uppercase().as_str() {
            "SELECT" => TokenKind::Select,
            "FROM" => TokenKind::From,
            "WHERE" => TokenKind::Where,
            "AND" => TokenKind::And,
            "OR" => TokenKind::Or,
            "NOT" => TokenKind::Not,
            "INSERT" => TokenKind::Insert,
            "INTO" => TokenKind::Into,
            "VALUES" => TokenKind::Values,
            "UPDATE" => TokenKind::Update,
            "SET" => TokenKind::Set,
            "DELETE" => TokenKind::Delete,
            "CREATE" => TokenKind::Create,
            "TABLE" => TokenKind::Table,
            "INDEX" => TokenKind::Index,
            "DROP" => TokenKind::Drop,
            "ALTER" => TokenKind::Alter,
            "NULL" => TokenKind::Null,
            "TRUE" => TokenKind::True,
            "FALSE" => TokenKind::False,
            "IS" => TokenKind::Is,
            "IN" => TokenKind::In,
            "LIKE" => TokenKind::Like,
            "GLOB" => TokenKind::Glob,
            "BETWEEN" => TokenKind::Between,
            "CASE" => TokenKind::Case,
            "WHEN" => TokenKind::When,
            "THEN" => TokenKind::Then,
            "ELSE" => TokenKind::Else,
            "END" => TokenKind::End,
            // ... all 140+ SQLite keywords
            _ => TokenKind::Id,
        }
    }
}
```

## Keyword Table

SQLite has ~140 keywords. Key groups:

```rust
/// All SQLite keywords
pub static KEYWORDS: &[(&str, TokenKind)] = &[
    ("ABORT", TokenKind::Abort),
    ("ACTION", TokenKind::Action),
    ("ADD", TokenKind::Add),
    ("AFTER", TokenKind::After),
    ("ALL", TokenKind::All),
    // ... complete list
];

/// Check if identifier is a keyword
pub fn keyword_code(name: &str) -> Option<TokenKind> {
    let upper = name.to_uppercase();
    KEYWORDS.iter()
        .find(|(kw, _)| *kw == upper)
        .map(|(_, tk)| *tk)
}
```

## Public API

```rust
/// Tokenize SQL into vector of tokens
pub fn tokenize(sql: &str) -> Vec<Token> {
    let mut tokenizer = Tokenizer::new(sql);
    let mut tokens = Vec::new();

    loop {
        let token = tokenizer.next_token();
        let is_eof = token.kind == TokenKind::Eof;
        tokens.push(token);
        if is_eof {
            break;
        }
    }

    tokens
}

/// Get token at specific offset (for error messages)
pub fn token_at_offset(sql: &str, offset: usize) -> Option<Token> {
    let mut tokenizer = Tokenizer::new(sql);

    loop {
        let token = tokenizer.next_token();
        if token.offset <= offset && offset < token.offset + token.len {
            return Some(token);
        }
        if token.kind == TokenKind::Eof {
            break;
        }
    }

    None
}
```

## Acceptance Criteria
- [ ] Token and TokenKind types defined
- [ ] Tokenizer struct with position tracking
- [ ] next_token() returns all token types
- [ ] String literals with escape handling
- [ ] Numeric literals (int, float, hex)
- [ ] Quoted identifiers ("id", `id`, [id])
- [ ] Blob literals (X'...')
- [ ] Variables (?NNN, :name, @name, $name)
- [ ] All SQLite keywords recognized
- [ ] Comments (-- and /* */) handled
- [ ] Line/column tracking for errors
