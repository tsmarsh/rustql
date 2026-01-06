# Translate parse.y - SQL Parser

## Overview
Translate the SQL parser. SQLite uses a Lemon-generated LALR(1) parser. For Rust, we can either use a parser generator (lalrpop, pest) or hand-write a recursive descent parser.

## Source Reference
- `sqlite3/src/parse.y` - ~2,000 lines (Lemon grammar)
- `sqlite3/src/parse.c` - Generated parser (~8,000 lines, produced from `parse.y` by Lemon; not present in this tree)

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Approach Options

### Option A: Parser Generator (Recommended)
Use `lalrpop` or `pest` crate:
- Less code to maintain
- Grammar changes are easier
- Well-tested parsing infrastructure

### Option B: Hand-written Recursive Descent
- More control over error messages
- Easier debugging
- No external dependency

## AST Structures

### Statements
```rust
#[derive(Debug, Clone)]
pub enum Stmt {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    CreateIndex(CreateIndexStmt),
    DropTable(DropTableStmt),
    DropIndex(DropIndexStmt),
    AlterTable(AlterTableStmt),
    Begin(BeginStmt),
    Commit,
    Rollback(RollbackStmt),
    Savepoint(String),
    Release(String),
    Pragma(PragmaStmt),
    Vacuum(Option<String>),
    Analyze(Option<QualifiedName>),
    Reindex(Option<QualifiedName>),
    Attach(AttachStmt),
    Detach(String),
    Explain(Box<Stmt>),
    ExplainQueryPlan(Box<Stmt>),
}
```

### SELECT Statement
```rust
#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub distinct: Distinct,
    pub columns: Vec<ResultColumn>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Box<Expr>>,
    pub group_by: Option<Vec<Expr>>,
    pub having: Option<Box<Expr>>,
    pub order_by: Option<Vec<OrderingTerm>>,
    pub limit: Option<LimitClause>,
    pub compound: Option<CompoundSelect>,
}

#[derive(Debug, Clone)]
pub enum Distinct {
    All,
    Distinct,
    // No distinct keyword
    None,
}

#[derive(Debug, Clone)]
pub enum ResultColumn {
    Star,                           // *
    TableStar(String),              // table.*
    Expr(Expr, Option<String>),     // expr AS alias
}

#[derive(Debug, Clone)]
pub struct FromClause {
    pub tables: Vec<TableRef>,
}

#[derive(Debug, Clone)]
pub enum TableRef {
    Table {
        name: QualifiedName,
        alias: Option<String>,
        indexed_by: Option<IndexedBy>,
    },
    Subquery {
        query: Box<SelectStmt>,
        alias: Option<String>,
    },
    Join {
        left: Box<TableRef>,
        join_type: JoinType,
        right: Box<TableRef>,
        constraint: Option<JoinConstraint>,
    },
    TableFunction {
        name: String,
        args: Vec<Expr>,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct CompoundSelect {
    pub op: CompoundOp,
    pub select: Box<SelectStmt>,
}

#[derive(Debug, Clone, Copy)]
pub enum CompoundOp {
    Union,
    UnionAll,
    Intersect,
    Except,
}
```

### INSERT Statement
```rust
#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub or_action: Option<ConflictAction>,
    pub table: QualifiedName,
    pub columns: Option<Vec<String>>,
    pub source: InsertSource,
    pub returning: Option<Vec<ResultColumn>>,
}

#[derive(Debug, Clone)]
pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Select(Box<SelectStmt>),
    DefaultValues,
}

#[derive(Debug, Clone, Copy)]
pub enum ConflictAction {
    Rollback,
    Abort,
    Replace,
    Fail,
    Ignore,
}
```

### Expressions
```rust
#[derive(Debug, Clone)]
pub enum Expr {
    // Literals
    Null,
    Integer(i64),
    Float(f64),
    String(String),
    Blob(Vec<u8>),
    Bool(bool),

    // Identifiers
    Column(ColumnRef),
    Variable(Variable),

    // Operators
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },

    // Special forms
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    In {
        expr: Box<Expr>,
        list: InList,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
        negated: bool,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        type_name: TypeName,
    },
    Collate {
        expr: Box<Expr>,
        collation: String,
    },
    Function {
        name: String,
        args: FunctionArgs,
        filter: Option<Box<Expr>>,
        over: Option<WindowSpec>,
    },
    Subquery(Box<SelectStmt>),
    Exists {
        subquery: Box<SelectStmt>,
        negated: bool,
    },
}

#[derive(Debug, Clone)]
pub struct ColumnRef {
    pub database: Option<String>,
    pub table: Option<String>,
    pub column: String,
}

#[derive(Debug, Clone)]
pub enum Variable {
    Numbered(i32),      // ?NNN
    Named(String),      // :name, @name, $name
    Anonymous,          // ?
}
```

### CREATE TABLE
```rust
#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub if_not_exists: bool,
    pub name: QualifiedName,
    pub definition: TableDefinition,
}

#[derive(Debug, Clone)]
pub enum TableDefinition {
    Columns {
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    },
    AsSelect(Box<SelectStmt>),
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub type_name: Option<TypeName>,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone)]
pub enum ColumnConstraint {
    PrimaryKey {
        order: Option<SortOrder>,
        conflict: Option<ConflictAction>,
        autoincrement: bool,
    },
    NotNull {
        conflict: Option<ConflictAction>,
    },
    Unique {
        conflict: Option<ConflictAction>,
    },
    Check(Expr),
    Default(DefaultValue),
    Collate(String),
    ForeignKey(ForeignKeyClause),
    Generated {
        expr: Expr,
        storage: GeneratedStorage,
    },
}
```

## Parser Implementation

### Recursive Descent Parser
```rust
pub struct Parser<'a> {
    tokens: Vec<Token>,
    pos: usize,
    source: &'a str,
}

impl<'a> Parser<'a> {
    pub fn new(source: &'a str) -> Self {
        let tokens = tokenize(source);
        Parser {
            tokens,
            pos: 0,
            source,
        }
    }

    /// Parse a complete SQL statement
    pub fn parse_stmt(&mut self) -> Result<Stmt> {
        match self.current().kind {
            TokenKind::Select => self.parse_select(),
            TokenKind::Insert => self.parse_insert(),
            TokenKind::Update => self.parse_update(),
            TokenKind::Delete => self.parse_delete(),
            TokenKind::Create => self.parse_create(),
            TokenKind::Drop => self.parse_drop(),
            TokenKind::Alter => self.parse_alter(),
            TokenKind::Begin => self.parse_begin(),
            TokenKind::Commit => { self.advance(); Ok(Stmt::Commit) }
            TokenKind::Rollback => self.parse_rollback(),
            TokenKind::Pragma => self.parse_pragma(),
            TokenKind::Explain => self.parse_explain(),
            _ => Err(self.error("expected statement")),
        }
    }

    /// Parse SELECT statement
    fn parse_select(&mut self) -> Result<Stmt> {
        self.expect(TokenKind::Select)?;

        let distinct = self.parse_distinct()?;
        let columns = self.parse_result_columns()?;

        let from = if self.match_token(TokenKind::From) {
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        let where_clause = if self.match_token(TokenKind::Where) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        // ... continue parsing GROUP BY, HAVING, ORDER BY, LIMIT

        Ok(Stmt::Select(SelectStmt {
            distinct,
            columns,
            from,
            where_clause,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            compound: None,
        }))
    }

    /// Parse expression with precedence climbing
    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_expr_precedence(0)
    }

    fn parse_expr_precedence(&mut self, min_prec: u8) -> Result<Expr> {
        let mut left = self.parse_unary()?;

        while let Some(op) = self.current_binary_op() {
            let prec = op.precedence();
            if prec < min_prec {
                break;
            }

            self.advance();
            let right = self.parse_expr_precedence(prec + 1)?;

            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }
}
```

## Public API

```rust
/// Parse a single SQL statement
pub fn parse(sql: &str) -> Result<Stmt> {
    let mut parser = Parser::new(sql);
    parser.parse_stmt()
}

/// Parse multiple SQL statements
pub fn parse_all(sql: &str) -> Result<Vec<Stmt>> {
    let mut parser = Parser::new(sql);
    let mut stmts = Vec::new();

    while !parser.is_eof() {
        stmts.push(parser.parse_stmt()?);
        parser.skip_semicolons();
    }

    Ok(stmts)
}
```

## Acceptance Criteria
- [ ] All AST node types defined
- [ ] Tokenizer integration
- [ ] SELECT with all clauses (FROM, WHERE, GROUP BY, etc.)
- [ ] INSERT (VALUES, SELECT, DEFAULT VALUES)
- [ ] UPDATE with SET and WHERE
- [ ] DELETE with WHERE
- [ ] CREATE TABLE with all constraint types
- [ ] CREATE INDEX
- [ ] DROP TABLE/INDEX
- [ ] ALTER TABLE
- [ ] Transaction statements (BEGIN, COMMIT, ROLLBACK)
- [ ] Expression parsing with correct precedence
- [ ] Subquery support
- [ ] JOIN syntax (all types)
- [ ] UNION/INTERSECT/EXCEPT
- [ ] Clear error messages with location
