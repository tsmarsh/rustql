# Translate prepare.c - Statement Preparation

## Overview
Translate the statement preparation code which compiles SQL text into executable VDBE bytecode.

## Source Reference
- `sqlite3/src/prepare.c` - 1,092 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Parse Context
```rust
pub struct Parse<'a> {
    /// Database connection
    pub db: &'a Connection,

    /// Error message (if any)
    pub error_msg: Option<String>,

    /// Error code
    pub rc: ErrorCode,

    /// VDBE being constructed
    pub vdbe: Option<Vdbe>,

    /// SQL text being parsed
    pub sql: &'a str,

    /// Current token
    pub token: Token,

    /// Nested parse depth (for subqueries)
    pub depth: i32,

    /// Trigger being parsed (if any)
    pub trigger: Option<Arc<Trigger>>,

    /// Table being modified (for triggers)
    pub trigger_table: Option<Arc<Table>>,

    /// Name resolution context
    pub nc_list: Vec<NameContext>,

    /// Variables
    pub n_var: i32,
    pub var_names: Vec<Option<String>>,

    /// Memory cell allocations
    pub n_mem: i32,

    /// Cursor allocations
    pub n_cursor: i32,

    /// Label allocations
    pub n_label: i32,

    /// EXPLAIN mode
    pub explain: ExplainMode,

    /// Flags
    pub declare_vtab: bool,
    pub has_compound: bool,
}
```

### Name Context
```rust
pub struct NameContext {
    /// Parser
    pub parse: *mut Parse<'static>,

    /// List of sources (FROM clause tables)
    pub src_list: Option<Vec<SrcItem>>,

    /// Expression being resolved
    pub expr: Option<*mut Expr>,

    /// Mask of tables used
    pub src_mask: u64,

    /// Number of errors
    pub n_err: i32,

    /// Allowed reference types
    pub nc_flags: NcFlags,
}

bitflags! {
    pub struct NcFlags: u32 {
        const ALLOW_AGG = 0x0001;    // Allow aggregate functions
        const HAS_AGG = 0x0002;      // Has aggregate function
        const ALLOW_WIN = 0x0010;    // Allow window functions
        const HAS_WIN = 0x0020;      // Has window function
        const IN_HAVING = 0x0100;    // Processing HAVING clause
        const IN_ORDER_BY = 0x0200;  // Processing ORDER BY clause
    }
}
```

## Key Functions

### Statement Preparation

```rust
impl Connection {
    /// Prepare a SQL statement
    /// sqlite3_prepare_v2()
    pub fn prepare(&self, sql: &str) -> Result<Statement> {
        // Create parse context
        let mut parse = Parse::new(self, sql);

        // Parse SQL
        parse.run_parser()?;

        // Get VDBE
        let vdbe = parse.vdbe.take()
            .ok_or_else(|| Error::new(ErrorCode::Error))?;

        // Finalize VDBE
        vdbe.make_ready()?;

        Ok(Statement {
            db: self.clone(),
            vdbe,
            sql_text: sql.to_string(),
        })
    }

    /// Prepare and return pointer to tail (unused SQL)
    pub fn prepare_v3(
        &self,
        sql: &str,
        flags: PrepareFlags,
    ) -> Result<(Statement, usize)> {
        let mut parse = Parse::new(self, sql);
        parse.run_parser()?;

        let vdbe = parse.vdbe.take()
            .ok_or_else(|| Error::new(ErrorCode::Error))?;
        vdbe.make_ready()?;

        let tail = parse.token.offset + parse.token.len;

        Ok((Statement {
            db: self.clone(),
            vdbe,
            sql_text: sql[..tail].to_string(),
        }, tail))
    }
}

bitflags! {
    pub struct PrepareFlags: u32 {
        const PERSISTENT = 0x01;  // Statement may be long-lived
        const NORMALIZE = 0x02;   // Normalize SQL text
        const NO_VTAB = 0x04;     // Disallow virtual tables
    }
}
```

### Parse Context

```rust
impl<'a> Parse<'a> {
    pub fn new(db: &'a Connection, sql: &'a str) -> Self {
        Parse {
            db,
            error_msg: None,
            rc: ErrorCode::Ok,
            vdbe: Some(Vdbe::new(db.clone())),
            sql,
            token: Token::default(),
            depth: 0,
            trigger: None,
            trigger_table: None,
            nc_list: Vec::new(),
            n_var: 0,
            var_names: Vec::new(),
            n_mem: 0,
            n_cursor: 0,
            n_label: 0,
            explain: ExplainMode::None,
            declare_vtab: false,
            has_compound: false,
        }
    }

    /// Run the parser
    pub fn run_parser(&mut self) -> Result<()> {
        // Create tokenizer
        let mut tokenizer = Tokenizer::new(self.sql);

        // Create parser
        let mut parser = Parser::new();

        // Parse tokens
        loop {
            let token = tokenizer.next_token();

            if token.kind == TokenKind::Space ||
               token.kind == TokenKind::Comment {
                continue;
            }

            self.token = token.clone();

            if token.kind == TokenKind::Eof {
                parser.parse(self, TokenKind::Eof, Token::default())?;
                break;
            }

            if token.kind == TokenKind::Illegal {
                self.error_msg = Some("unrecognized token".to_string());
                return Err(Error::new(ErrorCode::Error));
            }

            parser.parse(self, token.kind, token)?;

            if self.rc != ErrorCode::Ok {
                return Err(Error::with_message(
                    self.rc,
                    self.error_msg.clone().unwrap_or_default()
                ));
            }
        }

        Ok(())
    }
}
```

### Code Generation Helpers

```rust
impl<'a> Parse<'a> {
    /// Allocate memory cell
    pub fn alloc_mem(&mut self) -> i32 {
        self.n_mem += 1;
        self.n_mem
    }

    /// Allocate multiple memory cells
    pub fn alloc_mem_n(&mut self, n: i32) -> i32 {
        let start = self.n_mem + 1;
        self.n_mem += n;
        start
    }

    /// Allocate cursor
    pub fn alloc_cursor(&mut self) -> i32 {
        let cursor = self.n_cursor;
        self.n_cursor += 1;
        cursor
    }

    /// Allocate label for forward jumps
    pub fn make_label(&mut self) -> i32 {
        self.n_label += 1;
        -self.n_label
    }

    /// Resolve label to current address
    pub fn resolve_label(&mut self, label: i32) {
        if let Some(vdbe) = &mut self.vdbe {
            vdbe.resolve_label(label);
        }
    }

    /// Add opcode to VDBE
    pub fn add_op(&mut self, op: Opcode, p1: i32, p2: i32, p3: i32) -> i32 {
        if let Some(vdbe) = &mut self.vdbe {
            vdbe.add_op(op, p1, p2, p3)
        } else {
            -1
        }
    }

    /// Record error
    pub fn error(&mut self, msg: &str) {
        if self.error_msg.is_none() {
            self.error_msg = Some(msg.to_string());
            self.rc = ErrorCode::Error;
        }
    }
}
```

### Schema Access

```rust
impl<'a> Parse<'a> {
    /// Find table by name
    pub fn find_table(&self, name: &str, db_name: Option<&str>) -> Option<Arc<Table>> {
        let schema = if let Some(db) = db_name {
            self.db.schema_for(db)?
        } else {
            self.db.main_schema()
        };

        schema.tables.get(name).cloned()
    }

    /// Find index by name
    pub fn find_index(&self, name: &str) -> Option<Arc<Index>> {
        self.db.main_schema().indexes.get(name).cloned()
    }

    /// Begin table scan
    pub fn open_table(
        &mut self,
        table: &Table,
        cursor: i32,
        writable: bool,
    ) -> Result<()> {
        let op = if writable {
            Opcode::OpenWrite
        } else {
            Opcode::OpenRead
        };

        self.add_op(op, cursor, table.root_page as i32, table.columns.len() as i32);
        Ok(())
    }
}
```

## Re-preparation

```rust
impl Statement {
    /// Re-prepare statement if schema changed
    pub fn reprepare(&mut self) -> Result<()> {
        // Check if schema changed
        if !self.db.schema_changed_since(&self.schema_version) {
            return Ok(());
        }

        // Re-parse original SQL
        let new_stmt = self.db.prepare(&self.sql_text)?;

        // Replace VDBE
        self.vdbe = new_stmt.vdbe;
        self.schema_version = self.db.schema_version();

        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] Parse struct with all context fields
- [ ] prepare() compiles SQL to VDBE
- [ ] prepare_v3() with flags and tail
- [ ] Memory cell allocation
- [ ] Cursor allocation
- [ ] Label allocation and resolution
- [ ] Error handling with location
- [ ] Schema lookup (tables, indexes)
- [ ] Re-preparation on schema change
- [ ] PrepareFlags support
- [ ] NameContext for resolution
