# Translate fts5_main.c and fts5_expr.c - FTS5 Main and Expressions

## Overview
Translate FTS5 virtual table module and query expression parser/evaluator.

## Source Reference
- `sqlite3/ext/fts5/fts5_main.c` - Main virtual table implementation
- `sqlite3/ext/fts5/fts5_expr.c` - Query expression handling

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### FTS5 Virtual Table
```rust
/// FTS5 virtual table module
pub struct Fts5Module;

impl VirtualTableModule for Fts5Module {
    fn create(&self, db: &Connection, args: &[&str]) -> Result<Box<dyn VirtualTable>> {
        // Parse CREATE VIRTUAL TABLE arguments
        let config = Fts5Config::parse(args)?;

        // Create shadow tables
        create_shadow_tables(db, &config)?;

        Ok(Box::new(Fts5Table::new(db, config)?))
    }

    fn connect(&self, db: &Connection, args: &[&str]) -> Result<Box<dyn VirtualTable>> {
        let config = Fts5Config::parse(args)?;
        Ok(Box::new(Fts5Table::new(db, config)?))
    }
}

/// FTS5 virtual table instance
pub struct Fts5Table {
    /// Configuration
    config: Fts5Config,
    /// Index handle
    index: Fts5Index,
    /// Auxiliary functions
    aux_functions: HashMap<String, Fts5AuxFunc>,
}

/// FTS5 cursor
pub struct Fts5Cursor {
    /// Current rowid
    rowid: i64,
    /// Query plan
    plan: QueryPlan,
    /// Expression iterator
    expr_iter: Option<Fts5ExprIter>,
    /// Is at EOF
    eof: bool,
    /// Phrase count for ranking
    phrase_count: i32,
}
```

### Query Expression
```rust
/// FTS5 query expression
#[derive(Debug, Clone)]
pub enum Fts5Expr {
    /// Simple term: word
    Term(Fts5Term),
    /// Phrase: "word1 word2"
    Phrase(Vec<Fts5Term>),
    /// Prefix: word*
    Prefix(String),
    /// NEAR group: NEAR(a b c, distance)
    Near(Vec<Fts5Expr>, i32),
    /// AND: expr1 AND expr2
    And(Box<Fts5Expr>, Box<Fts5Expr>),
    /// OR: expr1 OR expr2
    Or(Box<Fts5Expr>, Box<Fts5Expr>),
    /// NOT: expr1 NOT expr2
    Not(Box<Fts5Expr>, Box<Fts5Expr>),
    /// Column filter: col:expr
    Column(String, Box<Fts5Expr>),
}

#[derive(Debug, Clone)]
pub struct Fts5Term {
    pub term: String,
    pub col: Option<i32>,
    pub is_prefix: bool,
}
```

## Expression Parser

```rust
/// Parse FTS5 query string
pub fn fts5_parse_expr(query: &str, config: &Fts5Config) -> Result<Fts5Expr> {
    let tokens = fts5_tokenize_query(query)?;
    let mut parser = Fts5ExprParser::new(&tokens, config);
    parser.parse_expr()
}

struct Fts5ExprParser<'a> {
    tokens: &'a [QueryToken],
    pos: usize,
    config: &'a Fts5Config,
}

impl<'a> Fts5ExprParser<'a> {
    fn parse_expr(&mut self) -> Result<Fts5Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Fts5Expr> {
        let mut left = self.parse_and()?;

        while self.match_token(TokenType::Or) {
            let right = self.parse_and()?;
            left = Fts5Expr::Or(Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Fts5Expr> {
        let mut left = self.parse_not()?;

        while self.match_token(TokenType::And) || self.is_implicit_and() {
            let right = self.parse_not()?;
            left = Fts5Expr::And(Box::new(left), Box::new(right));
        }

        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Fts5Expr> {
        let left = self.parse_primary()?;

        if self.match_token(TokenType::Not) {
            let right = self.parse_primary()?;
            Ok(Fts5Expr::Not(Box::new(left), Box::new(right)))
        } else {
            Ok(left)
        }
    }

    fn parse_primary(&mut self) -> Result<Fts5Expr> {
        // Check for column filter: col:
        let col = if self.is_column_prefix() {
            let name = self.consume_column_name()?;
            Some(name)
        } else {
            None
        };

        let expr = if self.match_token(TokenType::LParen) {
            // Parenthesized expression
            let inner = self.parse_expr()?;
            self.expect_token(TokenType::RParen)?;
            inner
        } else if self.match_token(TokenType::Quote) {
            // Phrase
            self.parse_phrase()?
        } else if self.check_token(TokenType::Near) {
            // NEAR group
            self.parse_near()?
        } else {
            // Single term
            self.parse_term()?
        };

        if let Some(col_name) = col {
            Ok(Fts5Expr::Column(col_name, Box::new(expr)))
        } else {
            Ok(expr)
        }
    }

    fn parse_phrase(&mut self) -> Result<Fts5Expr> {
        let mut terms = Vec::new();

        while !self.match_token(TokenType::Quote) && !self.is_eof() {
            if let Some(term) = self.consume_word()? {
                terms.push(Fts5Term {
                    term,
                    col: None,
                    is_prefix: false,
                });
            }
        }

        Ok(Fts5Expr::Phrase(terms))
    }

    fn parse_near(&mut self) -> Result<Fts5Expr> {
        self.expect_token(TokenType::Near)?;
        self.expect_token(TokenType::LParen)?;

        let mut exprs = Vec::new();
        let mut distance = 10; // default

        loop {
            if self.match_token(TokenType::RParen) {
                break;
            }

            if self.match_token(TokenType::Comma) {
                // Distance parameter
                distance = self.consume_number()? as i32;
            } else {
                exprs.push(self.parse_primary()?);
            }
        }

        Ok(Fts5Expr::Near(exprs, distance))
    }

    fn parse_term(&mut self) -> Result<Fts5Expr> {
        let term = self.consume_word()?
            .ok_or_else(|| Error::with_message(ErrorCode::Error, "expected term"))?;

        let is_prefix = self.match_token(TokenType::Star);

        if is_prefix {
            Ok(Fts5Expr::Prefix(term))
        } else {
            Ok(Fts5Expr::Term(Fts5Term {
                term,
                col: None,
                is_prefix: false,
            }))
        }
    }
}
```

## Expression Evaluation

```rust
pub struct Fts5ExprIter {
    expr: Fts5Expr,
    index: Arc<Fts5Index>,
    state: ExprIterState,
}

enum ExprIterState {
    Term(Fts5Iter),
    And(Box<Fts5ExprIter>, Box<Fts5ExprIter>),
    Or(Box<Fts5ExprIter>, Box<Fts5ExprIter>),
    Not(Box<Fts5ExprIter>, Box<Fts5ExprIter>),
}

impl Fts5ExprIter {
    pub fn new(expr: &Fts5Expr, index: Arc<Fts5Index>) -> Result<Self> {
        let state = Self::create_state(expr, &index)?;
        Ok(Self {
            expr: expr.clone(),
            index,
            state,
        })
    }

    fn create_state(expr: &Fts5Expr, index: &Fts5Index) -> Result<ExprIterState> {
        match expr {
            Fts5Expr::Term(t) => {
                Ok(ExprIterState::Term(index.lookup_term(&t.term)?))
            }
            Fts5Expr::Prefix(p) => {
                Ok(ExprIterState::Term(index.lookup_prefix(p)?))
            }
            Fts5Expr::And(l, r) => {
                let left = Box::new(Self::new(l, index.clone())?);
                let right = Box::new(Self::new(r, index.clone())?);
                Ok(ExprIterState::And(left, right))
            }
            Fts5Expr::Or(l, r) => {
                let left = Box::new(Self::new(l, index.clone())?);
                let right = Box::new(Self::new(r, index.clone())?);
                Ok(ExprIterState::Or(left, right))
            }
            Fts5Expr::Not(l, r) => {
                let left = Box::new(Self::new(l, index.clone())?);
                let right = Box::new(Self::new(r, index.clone())?);
                Ok(ExprIterState::Not(left, right))
            }
            _ => Err(Error::with_message(ErrorCode::Error, "unsupported expression")),
        }
    }

    pub fn rowid(&self) -> i64 {
        match &self.state {
            ExprIterState::Term(iter) => iter.rowid(),
            ExprIterState::And(l, _) => l.rowid(),
            ExprIterState::Or(l, r) => l.rowid().min(r.rowid()),
            ExprIterState::Not(l, _) => l.rowid(),
        }
    }

    pub fn next(&mut self) -> Result<()> {
        match &mut self.state {
            ExprIterState::Term(iter) => iter.next(),
            ExprIterState::And(l, r) => {
                // Both must match
                let current = l.rowid();
                l.next()?;

                // Advance both to same position
                while !l.eof() && !r.eof() {
                    if l.rowid() == r.rowid() {
                        break;
                    } else if l.rowid() < r.rowid() {
                        l.next()?;
                    } else {
                        r.next()?;
                    }
                }
                Ok(())
            }
            ExprIterState::Or(l, r) => {
                let current = self.rowid();
                if l.rowid() == current {
                    l.next()?;
                }
                if r.rowid() == current {
                    r.next()?;
                }
                Ok(())
            }
            ExprIterState::Not(l, r) => {
                l.next()?;
                // Skip any that match right side
                while !l.eof() && !r.eof() && l.rowid() == r.rowid() {
                    l.next()?;
                    r.next()?;
                }
                Ok(())
            }
        }
    }

    pub fn eof(&self) -> bool {
        match &self.state {
            ExprIterState::Term(iter) => iter.eof(),
            ExprIterState::And(l, r) => l.eof() || r.eof(),
            ExprIterState::Or(l, r) => l.eof() && r.eof(),
            ExprIterState::Not(l, _) => l.eof(),
        }
    }
}
```

## Auxiliary Functions

```rust
/// FTS5 auxiliary function context
pub struct Fts5AuxContext<'a> {
    cursor: &'a Fts5Cursor,
    column: i32,
    phrase_idx: i32,
}

pub type Fts5AuxFunc = fn(&mut Context, &Fts5AuxContext, &[&Value]) -> Result<()>;

/// Built-in auxiliary functions
pub fn fts5_builtin_aux_functions() -> HashMap<String, Fts5AuxFunc> {
    let mut funcs = HashMap::new();

    // bm25() - ranking function
    funcs.insert("bm25".to_string(), bm25_func as Fts5AuxFunc);

    // highlight() - return highlighted text
    funcs.insert("highlight".to_string(), highlight_func as Fts5AuxFunc);

    // snippet() - return snippet with highlights
    funcs.insert("snippet".to_string(), snippet_func as Fts5AuxFunc);

    funcs
}

fn bm25_func(ctx: &mut Context, aux: &Fts5AuxContext, args: &[&Value]) -> Result<()> {
    // BM25 ranking algorithm
    let k1 = 1.2;
    let b = 0.75;

    // Calculate BM25 score
    let score = 0.0; // TODO: implement

    ctx.result_double(score);
    Ok(())
}

fn highlight_func(ctx: &mut Context, aux: &Fts5AuxContext, args: &[&Value]) -> Result<()> {
    let col = args.get(0).map(|v| v.as_int() as i32).unwrap_or(0);
    let open_tag = args.get(1).map(|v| v.as_str()).unwrap_or("<b>");
    let close_tag = args.get(2).map(|v| v.as_str()).unwrap_or("</b>");

    // Get column text and highlight matches
    let text = ""; // TODO: get from cursor

    ctx.result_text(text);
    Ok(())
}

fn snippet_func(ctx: &mut Context, aux: &Fts5AuxContext, args: &[&Value]) -> Result<()> {
    let col = args.get(0).map(|v| v.as_int() as i32).unwrap_or(-1);
    let open_tag = args.get(1).map(|v| v.as_str()).unwrap_or("<b>");
    let close_tag = args.get(2).map(|v| v.as_str()).unwrap_or("</b>");
    let ellipsis = args.get(3).map(|v| v.as_str()).unwrap_or("...");
    let max_tokens = args.get(4).map(|v| v.as_int()).unwrap_or(64) as i32;

    // Generate snippet
    let snippet = ""; // TODO: implement

    ctx.result_text(snippet);
    Ok(())
}
```

## Acceptance Criteria
- [ ] Virtual table CREATE/CONNECT
- [ ] Query expression parsing (AND, OR, NOT)
- [ ] Phrase queries ("word1 word2")
- [ ] Prefix queries (word*)
- [ ] NEAR queries
- [ ] Column filter (col:expr)
- [ ] Expression evaluation with iterators
- [ ] bm25() ranking function
- [ ] highlight() function
- [ ] snippet() function
- [ ] Custom auxiliary function registration
- [ ] xFilter/xNext/xColumn implementation

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `fts5.test` - FTS5 core virtual table
- `fts5aa.test` - FTS5 basic queries
- `fts5expr.test` - FTS5 expression parsing
- `fts5near.test` - FTS5 NEAR queries
- `fts5phrase.test` - FTS5 phrase queries
- `fts5prefix.test` - FTS5 prefix queries
- `fts5rank.test` - FTS5 ranking (bm25)
- `fts5snippet.test` - FTS5 snippet/highlight functions
