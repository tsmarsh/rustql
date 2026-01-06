# Translate build.c - Schema Building

## Overview
Translate the schema building code which processes DDL statements (CREATE, DROP, ALTER) and maintains the database schema.

## Source Reference
- `sqlite3/src/build.c` - 5,815 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Schema
```rust
pub struct Schema {
    /// Tables in this schema
    pub tables: HashMap<String, Arc<Table>>,

    /// Indexes
    pub indexes: HashMap<String, Arc<Index>>,

    /// Triggers
    pub triggers: HashMap<String, Arc<Trigger>>,

    /// Schema cookie (version)
    pub schema_cookie: u32,

    /// File format
    pub file_format: u8,

    /// Encoding
    pub encoding: Encoding,
}
```

### Table
```rust
pub struct Table {
    /// Table name
    pub name: String,

    /// Database index (0=main, 1=temp, 2+=attached)
    pub db_idx: i32,

    /// Root page number
    pub root_page: Pgno,

    /// Columns
    pub columns: Vec<Column>,

    /// Primary key columns (indices into columns)
    pub primary_key: Option<Vec<usize>>,

    /// Indexes on this table
    pub indexes: Vec<Arc<Index>>,

    /// Foreign key constraints
    pub foreign_keys: Vec<ForeignKey>,

    /// CHECK constraints
    pub checks: Vec<Expr>,

    /// Is WITHOUT ROWID table
    pub without_rowid: bool,

    /// Is STRICT table
    pub strict: bool,

    /// Is virtual table
    pub is_virtual: bool,

    /// Autoincrement tracking
    pub autoincrement: bool,

    /// CREATE TABLE statement (for schema table)
    pub sql: Option<String>,
}
```

### Column
```rust
pub struct Column {
    /// Column name
    pub name: String,

    /// Declared type (if any)
    pub type_name: Option<String>,

    /// Type affinity
    pub affinity: Affinity,

    /// NOT NULL constraint
    pub not_null: bool,

    /// Conflict action for NOT NULL
    pub not_null_conflict: Option<ConflictAction>,

    /// Default value
    pub default_value: Option<DefaultValue>,

    /// Collation sequence name
    pub collation: String,

    /// Is primary key
    pub is_primary_key: bool,

    /// Is hidden (generated, rowid, etc.)
    pub is_hidden: bool,

    /// Generated column expression
    pub generated: Option<GeneratedColumn>,
}

#[derive(Debug, Clone)]
pub enum DefaultValue {
    Null,
    Integer(i64),
    Float(f64),
    String(String),
    Blob(Vec<u8>),
    Expr(Expr),
    CurrentTime,
    CurrentDate,
    CurrentTimestamp,
}

#[derive(Debug, Clone)]
pub struct GeneratedColumn {
    pub expr: Expr,
    pub storage: GeneratedStorage,
}

#[derive(Debug, Clone, Copy)]
pub enum GeneratedStorage {
    Virtual,  // Computed on read
    Stored,   // Stored in database
}
```

### Index
```rust
pub struct Index {
    /// Index name
    pub name: String,

    /// Table this indexes
    pub table: String,

    /// Index columns
    pub columns: Vec<IndexColumn>,

    /// Root page
    pub root_page: Pgno,

    /// Is UNIQUE index
    pub unique: bool,

    /// Is partial index (has WHERE)
    pub partial: Option<Expr>,

    /// Is primary key index
    pub is_primary_key: bool,

    /// Collation sequences
    pub collations: Vec<String>,

    /// Sort orders (true = DESC)
    pub sort_orders: Vec<bool>,

    /// CREATE INDEX statement
    pub sql: Option<String>,
}

pub struct IndexColumn {
    pub column_idx: i32,   // Index into table columns, or -1 for expression
    pub expr: Option<Expr>, // For expression indexes
    pub sort_order: SortOrder,
    pub collation: String,
}
```

## Key Functions

### Table Creation

```rust
impl Schema {
    /// Process CREATE TABLE statement
    pub fn create_table(
        &mut self,
        db: &mut Connection,
        stmt: &CreateTableStmt,
    ) -> Result<()> {
        // Check if table exists
        if self.tables.contains_key(&stmt.name.name) {
            if stmt.if_not_exists {
                return Ok(());
            }
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("table {} already exists", stmt.name.name)
            ));
        }

        // Build table structure
        let table = match &stmt.definition {
            TableDefinition::Columns { columns, constraints } => {
                self.build_table_from_columns(&stmt.name, columns, constraints)?
            }
            TableDefinition::AsSelect(select) => {
                self.build_table_from_select(&stmt.name, select)?
            }
        };

        // Allocate root page
        let root_page = db.btree().create_table()?;

        let mut table = table;
        table.root_page = root_page;

        // Create implicit indexes for PRIMARY KEY, UNIQUE
        self.create_implicit_indexes(db, &table)?;

        // Store in schema
        let sql = stmt.to_sql();
        table.sql = Some(sql.clone());

        // Write to sqlite_master
        self.insert_into_master(db, "table", &table.name, &table.name, root_page, &sql)?;

        self.tables.insert(table.name.clone(), Arc::new(table));

        Ok(())
    }

    fn build_table_from_columns(
        &self,
        name: &QualifiedName,
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
    ) -> Result<Table> {
        let mut table = Table {
            name: name.name.clone(),
            db_idx: name.database_idx(),
            root_page: 0,
            columns: Vec::new(),
            primary_key: None,
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            checks: Vec::new(),
            without_rowid: false,
            strict: false,
            is_virtual: false,
            autoincrement: false,
            sql: None,
        };

        // Process columns
        for (i, col_def) in columns.iter().enumerate() {
            let column = self.build_column(col_def, i)?;
            table.columns.push(column);
        }

        // Process table constraints
        for constraint in constraints {
            self.apply_table_constraint(&mut table, constraint)?;
        }

        // Validate
        self.validate_table(&table)?;

        Ok(table)
    }

    fn build_column(&self, def: &ColumnDef, idx: usize) -> Result<Column> {
        let affinity = def.type_name.as_ref()
            .map(|t| type_affinity(t))
            .unwrap_or(Affinity::Blob);

        let mut column = Column {
            name: def.name.clone(),
            type_name: def.type_name.clone(),
            affinity,
            not_null: false,
            not_null_conflict: None,
            default_value: None,
            collation: "BINARY".to_string(),
            is_primary_key: false,
            is_hidden: false,
            generated: None,
        };

        // Apply column constraints
        for constraint in &def.constraints {
            self.apply_column_constraint(&mut column, constraint)?;
        }

        Ok(column)
    }
}
```

### Index Creation

```rust
impl Schema {
    /// Process CREATE INDEX statement
    pub fn create_index(
        &mut self,
        db: &mut Connection,
        stmt: &CreateIndexStmt,
    ) -> Result<()> {
        // Find table
        let table = self.tables.get(&stmt.table)
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", stmt.table)
            ))?;

        // Build index structure
        let mut index = Index {
            name: stmt.name.clone(),
            table: stmt.table.clone(),
            columns: Vec::new(),
            root_page: 0,
            unique: stmt.unique,
            partial: stmt.where_clause.clone(),
            is_primary_key: false,
            collations: Vec::new(),
            sort_orders: Vec::new(),
            sql: None,
        };

        // Process indexed columns
        for indexed_col in &stmt.columns {
            let col_idx = if let Some(expr) = &indexed_col.expr {
                -1 // Expression index
            } else {
                table.find_column(&indexed_col.name)?
            };

            index.columns.push(IndexColumn {
                column_idx: col_idx,
                expr: indexed_col.expr.clone(),
                sort_order: indexed_col.order.unwrap_or(SortOrder::Asc),
                collation: indexed_col.collation.clone()
                    .unwrap_or_else(|| "BINARY".to_string()),
            });
        }

        // Allocate root page
        index.root_page = db.btree().create_table()?;

        // Store SQL
        let sql = stmt.to_sql();
        index.sql = Some(sql.clone());

        // Write to sqlite_master
        self.insert_into_master(
            db, "index", &index.name, &index.table,
            index.root_page, &sql
        )?;

        self.indexes.insert(index.name.clone(), Arc::new(index));

        Ok(())
    }
}
```

### Type Affinity

```rust
/// Determine column affinity from type name
pub fn type_affinity(type_name: &str) -> Affinity {
    let upper = type_name.to_uppercase();

    // Rule 1: INT -> INTEGER
    if upper.contains("INT") {
        return Affinity::Integer;
    }

    // Rule 2: CHAR, CLOB, TEXT -> TEXT
    if upper.contains("CHAR") ||
       upper.contains("CLOB") ||
       upper.contains("TEXT") {
        return Affinity::Text;
    }

    // Rule 3: BLOB or empty -> BLOB
    if upper.contains("BLOB") || type_name.is_empty() {
        return Affinity::Blob;
    }

    // Rule 4: REAL, FLOA, DOUB -> REAL
    if upper.contains("REAL") ||
       upper.contains("FLOA") ||
       upper.contains("DOUB") {
        return Affinity::Real;
    }

    // Rule 5: Otherwise -> NUMERIC
    Affinity::Numeric
}
```

### Schema Loading

```rust
impl Schema {
    /// Load schema from sqlite_master table
    pub fn load(db: &Connection) -> Result<Self> {
        let mut schema = Schema::new();

        // Read sqlite_master
        let mut stmt = db.prepare(
            "SELECT type, name, tbl_name, rootpage, sql FROM sqlite_master"
        )?;

        while stmt.step()? == StepResult::Row {
            let obj_type = stmt.column_text(0);
            let name = stmt.column_text(1);
            let tbl_name = stmt.column_text(2);
            let root_page = stmt.column_i64(3) as Pgno;
            let sql = stmt.column_text(4);

            match obj_type {
                "table" => {
                    if !name.starts_with("sqlite_") {
                        let table = schema.parse_create_table(sql, root_page)?;
                        schema.tables.insert(name.to_string(), Arc::new(table));
                    }
                }
                "index" => {
                    let index = schema.parse_create_index(sql, root_page)?;
                    schema.indexes.insert(name.to_string(), Arc::new(index));
                }
                "trigger" => {
                    let trigger = schema.parse_create_trigger(sql)?;
                    schema.triggers.insert(name.to_string(), Arc::new(trigger));
                }
                _ => {}
            }
        }

        Ok(schema)
    }
}
```

## Acceptance Criteria
- [ ] Schema struct with tables, indexes, triggers
- [ ] Table struct with columns and constraints
- [ ] Column struct with affinity and constraints
- [ ] Index struct with columns and options
- [ ] CREATE TABLE processing
- [ ] CREATE INDEX processing
- [ ] DROP TABLE/INDEX processing
- [ ] Type affinity calculation
- [ ] Schema loading from sqlite_master
- [ ] NOT NULL, UNIQUE, CHECK constraints
- [ ] Foreign key constraints
- [ ] Generated columns
- [ ] WITHOUT ROWID tables
- [ ] STRICT tables
