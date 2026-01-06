//! SQL tokenization
//!
//! Implements a tokenizer for SQL statements that matches SQLite's
//! tokenization rules including keywords, identifiers, literals,
//! and operators.

use crate::error::{Error, ErrorCode, Result};

// ============================================================================
// Token Types
// ============================================================================

/// Token kind enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    // Literals
    Integer,
    Float,
    String,
    Blob,

    // Identifiers and Keywords
    Identifier,

    // Keywords (alphabetical)
    Abort,
    Action,
    Add,
    After,
    All,
    Alter,
    Always,
    Analyze,
    And,
    As,
    Asc,
    Attach,
    Autoincrement,
    Before,
    Begin,
    Between,
    By,
    Cascade,
    Case,
    Cast,
    Check,
    Collate,
    Column,
    Commit,
    Conflict,
    Constraint,
    Create,
    Cross,
    Current,
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,
    Database,
    Default,
    Deferrable,
    Deferred,
    Delete,
    Desc,
    Detach,
    Distinct,
    Do,
    Drop,
    Each,
    Else,
    End,
    Escape,
    Except,
    Exclude,
    Exclusive,
    Exists,
    Explain,
    Fail,
    Filter,
    First,
    Following,
    For,
    Foreign,
    From,
    Full,
    Generated,
    Glob,
    Group,
    Groups,
    Having,
    If,
    Ignore,
    Immediate,
    In,
    Index,
    Indexed,
    Initially,
    Inner,
    Insert,
    Instead,
    Intersect,
    Into,
    Is,
    Isnull,
    Join,
    Key,
    Last,
    Left,
    Like,
    Limit,
    Match,
    Materialized,
    Natural,
    No,
    Not,
    Nothing,
    Notnull,
    Null,
    Nulls,
    Of,
    Offset,
    On,
    Or,
    Order,
    Others,
    Outer,
    Over,
    Partition,
    Plan,
    Pragma,
    Preceding,
    Primary,
    Query,
    Raise,
    Range,
    Recursive,
    References,
    Regexp,
    Reindex,
    Release,
    Rename,
    Replace,
    Restrict,
    Returning,
    Right,
    Rollback,
    Row,
    Rows,
    Savepoint,
    Select,
    Set,
    Stored,
    Table,
    Temp,
    Temporary,
    Then,
    Ties,
    To,
    Transaction,
    Trigger,
    Unbounded,
    Union,
    Unique,
    Update,
    Using,
    Vacuum,
    Values,
    View,
    Virtual,
    When,
    Where,
    Window,
    With,
    Without,

    // Operators
    Plus,         // +
    Minus,        // -
    Star,         // *
    Slash,        // /
    Percent,      // %
    Eq,           // =
    EqEq,         // ==
    Ne,           // <>
    BangEq,       // !=
    Lt,           // <
    Le,           // <=
    Gt,           // >
    Ge,           // >=
    Ampersand,    // &
    Pipe,         // |
    DoublePipe,   // ||
    LtLt,         // <<
    GtGt,         // >>
    Tilde,        // ~
    Bang,         // !

    // Punctuation
    LParen,       // (
    RParen,       // )
    Comma,        // ,
    Semicolon,    // ;
    Dot,          // .
    Colon,        // :
    Question,     // ?
    At,           // @
    Dollar,       // $

    // Special
    Eof,
}

impl TokenKind {
    /// Check if this token is a keyword
    pub fn is_keyword(&self) -> bool {
        matches!(
            self,
            TokenKind::Abort
                | TokenKind::Action
                | TokenKind::Add
                | TokenKind::After
                | TokenKind::All
                | TokenKind::Alter
                | TokenKind::Always
                | TokenKind::Analyze
                | TokenKind::And
                | TokenKind::As
                | TokenKind::Asc
                | TokenKind::Attach
                | TokenKind::Autoincrement
                | TokenKind::Before
                | TokenKind::Begin
                | TokenKind::Between
                | TokenKind::By
                | TokenKind::Cascade
                | TokenKind::Case
                | TokenKind::Cast
                | TokenKind::Check
                | TokenKind::Collate
                | TokenKind::Column
                | TokenKind::Commit
                | TokenKind::Conflict
                | TokenKind::Constraint
                | TokenKind::Create
                | TokenKind::Cross
                | TokenKind::Current
                | TokenKind::CurrentDate
                | TokenKind::CurrentTime
                | TokenKind::CurrentTimestamp
                | TokenKind::Database
                | TokenKind::Default
                | TokenKind::Deferrable
                | TokenKind::Deferred
                | TokenKind::Delete
                | TokenKind::Desc
                | TokenKind::Detach
                | TokenKind::Distinct
                | TokenKind::Do
                | TokenKind::Drop
                | TokenKind::Each
                | TokenKind::Else
                | TokenKind::End
                | TokenKind::Escape
                | TokenKind::Except
                | TokenKind::Exclude
                | TokenKind::Exclusive
                | TokenKind::Exists
                | TokenKind::Explain
                | TokenKind::Fail
                | TokenKind::Filter
                | TokenKind::First
                | TokenKind::Following
                | TokenKind::For
                | TokenKind::Foreign
                | TokenKind::From
                | TokenKind::Full
                | TokenKind::Generated
                | TokenKind::Glob
                | TokenKind::Group
                | TokenKind::Groups
                | TokenKind::Having
                | TokenKind::If
                | TokenKind::Ignore
                | TokenKind::Immediate
                | TokenKind::In
                | TokenKind::Index
                | TokenKind::Indexed
                | TokenKind::Initially
                | TokenKind::Inner
                | TokenKind::Insert
                | TokenKind::Instead
                | TokenKind::Intersect
                | TokenKind::Into
                | TokenKind::Is
                | TokenKind::Isnull
                | TokenKind::Join
                | TokenKind::Key
                | TokenKind::Last
                | TokenKind::Left
                | TokenKind::Like
                | TokenKind::Limit
                | TokenKind::Match
                | TokenKind::Materialized
                | TokenKind::Natural
                | TokenKind::No
                | TokenKind::Not
                | TokenKind::Nothing
                | TokenKind::Notnull
                | TokenKind::Null
                | TokenKind::Nulls
                | TokenKind::Of
                | TokenKind::Offset
                | TokenKind::On
                | TokenKind::Or
                | TokenKind::Order
                | TokenKind::Others
                | TokenKind::Outer
                | TokenKind::Over
                | TokenKind::Partition
                | TokenKind::Plan
                | TokenKind::Pragma
                | TokenKind::Preceding
                | TokenKind::Primary
                | TokenKind::Query
                | TokenKind::Raise
                | TokenKind::Range
                | TokenKind::Recursive
                | TokenKind::References
                | TokenKind::Regexp
                | TokenKind::Reindex
                | TokenKind::Release
                | TokenKind::Rename
                | TokenKind::Replace
                | TokenKind::Restrict
                | TokenKind::Returning
                | TokenKind::Right
                | TokenKind::Rollback
                | TokenKind::Row
                | TokenKind::Rows
                | TokenKind::Savepoint
                | TokenKind::Select
                | TokenKind::Set
                | TokenKind::Stored
                | TokenKind::Table
                | TokenKind::Temp
                | TokenKind::Temporary
                | TokenKind::Then
                | TokenKind::Ties
                | TokenKind::To
                | TokenKind::Transaction
                | TokenKind::Trigger
                | TokenKind::Unbounded
                | TokenKind::Union
                | TokenKind::Unique
                | TokenKind::Update
                | TokenKind::Using
                | TokenKind::Vacuum
                | TokenKind::Values
                | TokenKind::View
                | TokenKind::Virtual
                | TokenKind::When
                | TokenKind::Where
                | TokenKind::Window
                | TokenKind::With
                | TokenKind::Without
        )
    }
}

// ============================================================================
// Token
// ============================================================================

/// A token from the SQL source
#[derive(Debug, Clone)]
pub struct Token {
    /// Token type
    pub kind: TokenKind,
    /// Start position in source
    pub start: usize,
    /// End position in source (exclusive)
    pub end: usize,
    /// Line number (1-based)
    pub line: u32,
    /// Column number (1-based)
    pub column: u32,
}

impl Token {
    /// Create a new token
    pub fn new(kind: TokenKind, start: usize, end: usize, line: u32, column: u32) -> Self {
        Token {
            kind,
            start,
            end,
            line,
            column,
        }
    }

    /// Get the text of this token from the source
    pub fn text<'a>(&self, source: &'a str) -> &'a str {
        &source[self.start..self.end]
    }

    /// Get the length of this token
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Check if the token is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ============================================================================
// Tokenizer
// ============================================================================

/// SQL tokenizer
pub struct Tokenizer<'a> {
    source: &'a str,
    bytes: &'a [u8],
    pos: usize,
    line: u32,
    column: u32,
}

impl<'a> Tokenizer<'a> {
    /// Create a new tokenizer
    pub fn new(source: &'a str) -> Self {
        Tokenizer {
            source,
            bytes: source.as_bytes(),
            pos: 0,
            line: 1,
            column: 1,
        }
    }

    /// Tokenize the entire source
    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();

        loop {
            let token = self.next_token()?;
            let is_eof = token.kind == TokenKind::Eof;
            tokens.push(token);
            if is_eof {
                break;
            }
        }

        Ok(tokens)
    }

    /// Get the next token
    pub fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace_and_comments();

        if self.is_eof() {
            return Ok(Token::new(TokenKind::Eof, self.pos, self.pos, self.line, self.column));
        }

        let start = self.pos;
        let start_line = self.line;
        let start_column = self.column;

        let kind = self.scan_token()?;

        Ok(Token::new(kind, start, self.pos, start_line, start_column))
    }

    /// Scan a single token
    fn scan_token(&mut self) -> Result<TokenKind> {
        let c = self.current();

        // Numbers
        if c.is_ascii_digit() {
            return self.scan_number();
        }

        // Identifiers and keywords
        if c.is_ascii_alphabetic() || c == b'_' {
            return self.scan_identifier();
        }

        // Quoted identifiers
        if c == b'"' || c == b'`' || c == b'[' {
            return self.scan_quoted_identifier();
        }

        // Strings
        if c == b'\'' {
            return self.scan_string();
        }

        // Blob literals
        if (c == b'x' || c == b'X') && self.peek() == Some(b'\'') {
            return self.scan_blob();
        }

        // Operators and punctuation
        self.scan_operator()
    }

    /// Scan a number (integer or float)
    fn scan_number(&mut self) -> Result<TokenKind> {
        // Handle hex numbers
        if self.current() == b'0' && matches!(self.peek(), Some(b'x') | Some(b'X')) {
            self.advance();
            self.advance();
            while !self.is_eof() && self.current().is_ascii_hexdigit() {
                self.advance();
            }
            return Ok(TokenKind::Integer);
        }

        // Integer part
        while !self.is_eof() && self.current().is_ascii_digit() {
            self.advance();
        }

        // Check for float
        if !self.is_eof() && self.current() == b'.' {
            if let Some(next) = self.peek() {
                if next.is_ascii_digit() {
                    self.advance(); // consume '.'
                    while !self.is_eof() && self.current().is_ascii_digit() {
                        self.advance();
                    }
                    // Check for exponent
                    if !self.is_eof() && matches!(self.current(), b'e' | b'E') {
                        self.advance();
                        if !self.is_eof() && matches!(self.current(), b'+' | b'-') {
                            self.advance();
                        }
                        while !self.is_eof() && self.current().is_ascii_digit() {
                            self.advance();
                        }
                    }
                    return Ok(TokenKind::Float);
                }
            }
        }

        // Check for exponent (without decimal point)
        if !self.is_eof() && matches!(self.current(), b'e' | b'E') {
            let saved_pos = self.pos;
            self.advance();
            if !self.is_eof() && matches!(self.current(), b'+' | b'-') {
                self.advance();
            }
            if !self.is_eof() && self.current().is_ascii_digit() {
                while !self.is_eof() && self.current().is_ascii_digit() {
                    self.advance();
                }
                return Ok(TokenKind::Float);
            }
            // Not a valid exponent, restore position
            self.pos = saved_pos;
        }

        Ok(TokenKind::Integer)
    }

    /// Scan an identifier or keyword
    fn scan_identifier(&mut self) -> Result<TokenKind> {
        let start = self.pos;

        while !self.is_eof() {
            let c = self.current();
            if c.is_ascii_alphanumeric() || c == b'_' {
                self.advance();
            } else {
                break;
            }
        }

        let text = &self.source[start..self.pos];
        Ok(keyword_or_identifier(text))
    }

    /// Scan a quoted identifier ("foo", `foo`, or [foo])
    fn scan_quoted_identifier(&mut self) -> Result<TokenKind> {
        let quote = self.current();
        let close = if quote == b'[' { b']' } else { quote };
        self.advance();

        while !self.is_eof() {
            if self.current() == close {
                // Check for escaped quote (doubled)
                if quote != b'[' && self.peek() == Some(close) {
                    self.advance();
                    self.advance();
                } else {
                    self.advance();
                    break;
                }
            } else {
                self.advance();
            }
        }

        Ok(TokenKind::Identifier)
    }

    /// Scan a string literal
    fn scan_string(&mut self) -> Result<TokenKind> {
        self.advance(); // consume opening quote

        while !self.is_eof() {
            if self.current() == b'\'' {
                // Check for escaped quote
                if self.peek() == Some(b'\'') {
                    self.advance();
                    self.advance();
                } else {
                    self.advance();
                    break;
                }
            } else {
                if self.current() == b'\n' {
                    self.line += 1;
                    self.column = 0;
                }
                self.advance();
            }
        }

        Ok(TokenKind::String)
    }

    /// Scan a blob literal (X'...')
    fn scan_blob(&mut self) -> Result<TokenKind> {
        self.advance(); // consume 'x' or 'X'
        self.advance(); // consume opening quote

        while !self.is_eof() && self.current() != b'\'' {
            if !self.current().is_ascii_hexdigit() {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("invalid hex digit in blob literal at line {}", self.line),
                ));
            }
            self.advance();
        }

        if !self.is_eof() {
            self.advance(); // consume closing quote
        }

        Ok(TokenKind::Blob)
    }

    /// Scan an operator or punctuation
    fn scan_operator(&mut self) -> Result<TokenKind> {
        let c = self.current();
        self.advance();

        match c {
            b'+' => Ok(TokenKind::Plus),
            b'-' => {
                // Check for -- comment
                if !self.is_eof() && self.current() == b'-' {
                    // This shouldn't happen as comments are skipped
                    self.advance();
                    while !self.is_eof() && self.current() != b'\n' {
                        self.advance();
                    }
                    self.next_token().map(|t| t.kind)
                } else {
                    Ok(TokenKind::Minus)
                }
            }
            b'*' => Ok(TokenKind::Star),
            b'/' => Ok(TokenKind::Slash),
            b'%' => Ok(TokenKind::Percent),
            b'=' => {
                if !self.is_eof() && self.current() == b'=' {
                    self.advance();
                    Ok(TokenKind::EqEq)
                } else {
                    Ok(TokenKind::Eq)
                }
            }
            b'<' => {
                if !self.is_eof() {
                    match self.current() {
                        b'=' => {
                            self.advance();
                            Ok(TokenKind::Le)
                        }
                        b'>' => {
                            self.advance();
                            Ok(TokenKind::Ne)
                        }
                        b'<' => {
                            self.advance();
                            Ok(TokenKind::LtLt)
                        }
                        _ => Ok(TokenKind::Lt),
                    }
                } else {
                    Ok(TokenKind::Lt)
                }
            }
            b'>' => {
                if !self.is_eof() {
                    match self.current() {
                        b'=' => {
                            self.advance();
                            Ok(TokenKind::Ge)
                        }
                        b'>' => {
                            self.advance();
                            Ok(TokenKind::GtGt)
                        }
                        _ => Ok(TokenKind::Gt),
                    }
                } else {
                    Ok(TokenKind::Gt)
                }
            }
            b'!' => {
                if !self.is_eof() && self.current() == b'=' {
                    self.advance();
                    Ok(TokenKind::BangEq)
                } else {
                    Ok(TokenKind::Bang)
                }
            }
            b'&' => Ok(TokenKind::Ampersand),
            b'|' => {
                if !self.is_eof() && self.current() == b'|' {
                    self.advance();
                    Ok(TokenKind::DoublePipe)
                } else {
                    Ok(TokenKind::Pipe)
                }
            }
            b'~' => Ok(TokenKind::Tilde),
            b'(' => Ok(TokenKind::LParen),
            b')' => Ok(TokenKind::RParen),
            b',' => Ok(TokenKind::Comma),
            b';' => Ok(TokenKind::Semicolon),
            b'.' => Ok(TokenKind::Dot),
            b':' => Ok(TokenKind::Colon),
            b'?' => Ok(TokenKind::Question),
            b'@' => Ok(TokenKind::At),
            b'$' => Ok(TokenKind::Dollar),
            _ => Err(Error::with_message(
                ErrorCode::Error,
                format!("unexpected character '{}' at line {}", c as char, self.line),
            )),
        }
    }

    /// Skip whitespace and comments
    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Skip whitespace
            while !self.is_eof() && self.current().is_ascii_whitespace() {
                if self.current() == b'\n' {
                    self.line += 1;
                    self.column = 0;
                }
                self.advance();
            }

            if self.is_eof() {
                break;
            }

            // Skip -- comments
            if self.current() == b'-' && self.peek() == Some(b'-') {
                while !self.is_eof() && self.current() != b'\n' {
                    self.advance();
                }
                continue;
            }

            // Skip /* */ comments
            if self.current() == b'/' && self.peek() == Some(b'*') {
                self.advance();
                self.advance();
                while !self.is_eof() {
                    if self.current() == b'*' && self.peek() == Some(b'/') {
                        self.advance();
                        self.advance();
                        break;
                    }
                    if self.current() == b'\n' {
                        self.line += 1;
                        self.column = 0;
                    }
                    self.advance();
                }
                continue;
            }

            break;
        }
    }

    /// Check if at end of input
    fn is_eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    /// Get current byte
    fn current(&self) -> u8 {
        self.bytes[self.pos]
    }

    /// Peek at next byte
    fn peek(&self) -> Option<u8> {
        if self.pos + 1 < self.bytes.len() {
            Some(self.bytes[self.pos + 1])
        } else {
            None
        }
    }

    /// Advance to next byte
    fn advance(&mut self) {
        self.pos += 1;
        self.column += 1;
    }
}

// ============================================================================
// Keyword Recognition
// ============================================================================

/// Map a text to a keyword or identifier token
fn keyword_or_identifier(text: &str) -> TokenKind {
    // Case-insensitive keyword matching
    match text.to_uppercase().as_str() {
        "ABORT" => TokenKind::Abort,
        "ACTION" => TokenKind::Action,
        "ADD" => TokenKind::Add,
        "AFTER" => TokenKind::After,
        "ALL" => TokenKind::All,
        "ALTER" => TokenKind::Alter,
        "ALWAYS" => TokenKind::Always,
        "ANALYZE" => TokenKind::Analyze,
        "AND" => TokenKind::And,
        "AS" => TokenKind::As,
        "ASC" => TokenKind::Asc,
        "ATTACH" => TokenKind::Attach,
        "AUTOINCREMENT" => TokenKind::Autoincrement,
        "BEFORE" => TokenKind::Before,
        "BEGIN" => TokenKind::Begin,
        "BETWEEN" => TokenKind::Between,
        "BY" => TokenKind::By,
        "CASCADE" => TokenKind::Cascade,
        "CASE" => TokenKind::Case,
        "CAST" => TokenKind::Cast,
        "CHECK" => TokenKind::Check,
        "COLLATE" => TokenKind::Collate,
        "COLUMN" => TokenKind::Column,
        "COMMIT" => TokenKind::Commit,
        "CONFLICT" => TokenKind::Conflict,
        "CONSTRAINT" => TokenKind::Constraint,
        "CREATE" => TokenKind::Create,
        "CROSS" => TokenKind::Cross,
        "CURRENT" => TokenKind::Current,
        "CURRENT_DATE" => TokenKind::CurrentDate,
        "CURRENT_TIME" => TokenKind::CurrentTime,
        "CURRENT_TIMESTAMP" => TokenKind::CurrentTimestamp,
        "DATABASE" => TokenKind::Database,
        "DEFAULT" => TokenKind::Default,
        "DEFERRABLE" => TokenKind::Deferrable,
        "DEFERRED" => TokenKind::Deferred,
        "DELETE" => TokenKind::Delete,
        "DESC" => TokenKind::Desc,
        "DETACH" => TokenKind::Detach,
        "DISTINCT" => TokenKind::Distinct,
        "DO" => TokenKind::Do,
        "DROP" => TokenKind::Drop,
        "EACH" => TokenKind::Each,
        "ELSE" => TokenKind::Else,
        "END" => TokenKind::End,
        "ESCAPE" => TokenKind::Escape,
        "EXCEPT" => TokenKind::Except,
        "EXCLUDE" => TokenKind::Exclude,
        "EXCLUSIVE" => TokenKind::Exclusive,
        "EXISTS" => TokenKind::Exists,
        "EXPLAIN" => TokenKind::Explain,
        "FAIL" => TokenKind::Fail,
        "FILTER" => TokenKind::Filter,
        "FIRST" => TokenKind::First,
        "FOLLOWING" => TokenKind::Following,
        "FOR" => TokenKind::For,
        "FOREIGN" => TokenKind::Foreign,
        "FROM" => TokenKind::From,
        "FULL" => TokenKind::Full,
        "GENERATED" => TokenKind::Generated,
        "GLOB" => TokenKind::Glob,
        "GROUP" => TokenKind::Group,
        "GROUPS" => TokenKind::Groups,
        "HAVING" => TokenKind::Having,
        "IF" => TokenKind::If,
        "IGNORE" => TokenKind::Ignore,
        "IMMEDIATE" => TokenKind::Immediate,
        "IN" => TokenKind::In,
        "INDEX" => TokenKind::Index,
        "INDEXED" => TokenKind::Indexed,
        "INITIALLY" => TokenKind::Initially,
        "INNER" => TokenKind::Inner,
        "INSERT" => TokenKind::Insert,
        "INSTEAD" => TokenKind::Instead,
        "INTERSECT" => TokenKind::Intersect,
        "INTO" => TokenKind::Into,
        "IS" => TokenKind::Is,
        "ISNULL" => TokenKind::Isnull,
        "JOIN" => TokenKind::Join,
        "KEY" => TokenKind::Key,
        "LAST" => TokenKind::Last,
        "LEFT" => TokenKind::Left,
        "LIKE" => TokenKind::Like,
        "LIMIT" => TokenKind::Limit,
        "MATCH" => TokenKind::Match,
        "MATERIALIZED" => TokenKind::Materialized,
        "NATURAL" => TokenKind::Natural,
        "NO" => TokenKind::No,
        "NOT" => TokenKind::Not,
        "NOTHING" => TokenKind::Nothing,
        "NOTNULL" => TokenKind::Notnull,
        "NULL" => TokenKind::Null,
        "NULLS" => TokenKind::Nulls,
        "OF" => TokenKind::Of,
        "OFFSET" => TokenKind::Offset,
        "ON" => TokenKind::On,
        "OR" => TokenKind::Or,
        "ORDER" => TokenKind::Order,
        "OTHERS" => TokenKind::Others,
        "OUTER" => TokenKind::Outer,
        "OVER" => TokenKind::Over,
        "PARTITION" => TokenKind::Partition,
        "PLAN" => TokenKind::Plan,
        "PRAGMA" => TokenKind::Pragma,
        "PRECEDING" => TokenKind::Preceding,
        "PRIMARY" => TokenKind::Primary,
        "QUERY" => TokenKind::Query,
        "RAISE" => TokenKind::Raise,
        "RANGE" => TokenKind::Range,
        "RECURSIVE" => TokenKind::Recursive,
        "REFERENCES" => TokenKind::References,
        "REGEXP" => TokenKind::Regexp,
        "REINDEX" => TokenKind::Reindex,
        "RELEASE" => TokenKind::Release,
        "RENAME" => TokenKind::Rename,
        "REPLACE" => TokenKind::Replace,
        "RESTRICT" => TokenKind::Restrict,
        "RETURNING" => TokenKind::Returning,
        "RIGHT" => TokenKind::Right,
        "ROLLBACK" => TokenKind::Rollback,
        "ROW" => TokenKind::Row,
        "ROWS" => TokenKind::Rows,
        "SAVEPOINT" => TokenKind::Savepoint,
        "SELECT" => TokenKind::Select,
        "SET" => TokenKind::Set,
        "STORED" => TokenKind::Stored,
        "TABLE" => TokenKind::Table,
        "TEMP" => TokenKind::Temp,
        "TEMPORARY" => TokenKind::Temporary,
        "THEN" => TokenKind::Then,
        "TIES" => TokenKind::Ties,
        "TO" => TokenKind::To,
        "TRANSACTION" => TokenKind::Transaction,
        "TRIGGER" => TokenKind::Trigger,
        "UNBOUNDED" => TokenKind::Unbounded,
        "UNION" => TokenKind::Union,
        "UNIQUE" => TokenKind::Unique,
        "UPDATE" => TokenKind::Update,
        "USING" => TokenKind::Using,
        "VACUUM" => TokenKind::Vacuum,
        "VALUES" => TokenKind::Values,
        "VIEW" => TokenKind::View,
        "VIRTUAL" => TokenKind::Virtual,
        "WHEN" => TokenKind::When,
        "WHERE" => TokenKind::Where,
        "WINDOW" => TokenKind::Window,
        "WITH" => TokenKind::With,
        "WITHOUT" => TokenKind::Without,
        _ => TokenKind::Identifier,
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Tokenize a SQL string
pub fn tokenize(source: &str) -> Result<Vec<Token>> {
    let mut tokenizer = Tokenizer::new(source);
    tokenizer.tokenize()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_keywords() {
        let tokens = tokenize("SELECT FROM WHERE").unwrap();
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0].kind, TokenKind::Select);
        assert_eq!(tokens[1].kind, TokenKind::From);
        assert_eq!(tokens[2].kind, TokenKind::Where);
        assert_eq!(tokens[3].kind, TokenKind::Eof);
    }

    #[test]
    fn test_tokenize_case_insensitive() {
        let tokens = tokenize("select FROM Where").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Select);
        assert_eq!(tokens[1].kind, TokenKind::From);
        assert_eq!(tokens[2].kind, TokenKind::Where);
    }

    #[test]
    fn test_tokenize_identifiers() {
        let tokens = tokenize("foo bar123 _baz").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Identifier);
        assert_eq!(tokens[1].kind, TokenKind::Identifier);
        assert_eq!(tokens[2].kind, TokenKind::Identifier);
    }

    #[test]
    fn test_tokenize_numbers() {
        let tokens = tokenize("42 3.14 1e10 0x1F").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Integer);
        assert_eq!(tokens[1].kind, TokenKind::Float);
        assert_eq!(tokens[2].kind, TokenKind::Float);
        assert_eq!(tokens[3].kind, TokenKind::Integer);
    }

    #[test]
    fn test_tokenize_strings() {
        let tokens = tokenize("'hello' 'it''s'").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::String);
        assert_eq!(tokens[1].kind, TokenKind::String);
    }

    #[test]
    fn test_tokenize_blob() {
        let tokens = tokenize("X'48656C6C6F'").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Blob);
    }

    #[test]
    fn test_tokenize_operators() {
        let tokens = tokenize("+ - * / = <> != <= >= << >> || &amp; | ~").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Plus);
        assert_eq!(tokens[1].kind, TokenKind::Minus);
        assert_eq!(tokens[2].kind, TokenKind::Star);
        assert_eq!(tokens[3].kind, TokenKind::Slash);
        assert_eq!(tokens[4].kind, TokenKind::Eq);
        assert_eq!(tokens[5].kind, TokenKind::Ne);
        assert_eq!(tokens[6].kind, TokenKind::BangEq);
        assert_eq!(tokens[7].kind, TokenKind::Le);
        assert_eq!(tokens[8].kind, TokenKind::Ge);
        assert_eq!(tokens[9].kind, TokenKind::LtLt);
        assert_eq!(tokens[10].kind, TokenKind::GtGt);
        assert_eq!(tokens[11].kind, TokenKind::DoublePipe);
    }

    #[test]
    fn test_tokenize_comments() {
        let tokens = tokenize("SELECT -- comment\nFROM").unwrap();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].kind, TokenKind::Select);
        assert_eq!(tokens[1].kind, TokenKind::From);
    }

    #[test]
    fn test_tokenize_block_comment() {
        let tokens = tokenize("SELECT /* multi\nline */ FROM").unwrap();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].kind, TokenKind::Select);
        assert_eq!(tokens[1].kind, TokenKind::From);
    }

    #[test]
    fn test_tokenize_quoted_identifier() {
        let tokens = tokenize("\"my table\" `another` [bracketed]").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Identifier);
        assert_eq!(tokens[1].kind, TokenKind::Identifier);
        assert_eq!(tokens[2].kind, TokenKind::Identifier);
    }

    #[test]
    fn test_tokenize_parameters() {
        let tokens = tokenize("? ?1 :name @var $dollar").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Question);
        assert_eq!(tokens[1].kind, TokenKind::Question);
        assert_eq!(tokens[2].kind, TokenKind::Integer);
        assert_eq!(tokens[3].kind, TokenKind::Colon);
        assert_eq!(tokens[4].kind, TokenKind::Identifier);
        assert_eq!(tokens[5].kind, TokenKind::At);
        assert_eq!(tokens[6].kind, TokenKind::Identifier);
        assert_eq!(tokens[7].kind, TokenKind::Dollar);
        assert_eq!(tokens[8].kind, TokenKind::Identifier);
    }

    #[test]
    fn test_tokenize_select_statement() {
        let tokens = tokenize("SELECT id, name FROM users WHERE id = 1").unwrap();
        assert_eq!(tokens[0].kind, TokenKind::Select);
        assert_eq!(tokens[1].kind, TokenKind::Identifier);
        assert_eq!(tokens[2].kind, TokenKind::Comma);
        assert_eq!(tokens[3].kind, TokenKind::Identifier);
        assert_eq!(tokens[4].kind, TokenKind::From);
        assert_eq!(tokens[5].kind, TokenKind::Identifier);
        assert_eq!(tokens[6].kind, TokenKind::Where);
        assert_eq!(tokens[7].kind, TokenKind::Identifier);
        assert_eq!(tokens[8].kind, TokenKind::Eq);
        assert_eq!(tokens[9].kind, TokenKind::Integer);
    }

    #[test]
    fn test_token_position() {
        let tokens = tokenize("SELECT\nFROM").unwrap();
        assert_eq!(tokens[0].line, 1);
        assert_eq!(tokens[1].line, 2);
    }
}
