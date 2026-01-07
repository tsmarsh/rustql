//! Schema building and management
//!
//! This module handles DDL statement processing (CREATE, DROP, ALTER) and
//! maintains the database schema. It corresponds to SQLite's build.c.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Error, ErrorCode, Result};
use crate::types::Pgno;

// ============================================================================
// Constants
// ============================================================================

/// Default collation sequence
pub const DEFAULT_COLLATION: &str = "BINARY";

/// Maximum number of attached databases
pub const MAX_ATTACHED: usize = 10;

// ============================================================================
// Basic Types
// ============================================================================

/// Text encoding (SQLITE_UTF8, etc.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Encoding {
    #[default]
    Utf8 = 1,
    Utf16le = 2,
    Utf16be = 3,
}

/// Column type affinity (Section 3.1 of SQLite docs)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Affinity {
    /// BLOB affinity (no type coercion)
    #[default]
    Blob,
    /// TEXT affinity
    Text,
    /// NUMERIC affinity (prefers integer, then real)
    Numeric,
    /// INTEGER affinity
    Integer,
    /// REAL affinity
    Real,
}

/// Sort order for columns/indexes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

/// ON CONFLICT clause action
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConflictAction {
    #[default]
    Abort,
    Rollback,
    Fail,
    Ignore,
    Replace,
}

/// Foreign key actions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FkAction {
    #[default]
    NoAction,
    Restrict,
    SetNull,
    SetDefault,
    Cascade,
}

/// Deferrable constraint types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Deferrable {
    #[default]
    NotDeferrable,
    DeferrableInitiallyImmediate,
    DeferrableInitiallyDeferred,
}

// ============================================================================
// Expression Placeholder
// ============================================================================

/// Placeholder for expression AST node
/// Will be properly defined in the parser module
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Null literal
    Null,
    /// Integer literal
    Integer(i64),
    /// Real literal
    Real(f64),
    /// String literal
    String(String),
    /// Blob literal
    Blob(Vec<u8>),
    /// Column reference
    Column {
        table: Option<String>,
        column: String,
    },
    /// Binary operation
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    /// Unary operation
    UnaryOp { op: UnaryOp, operand: Box<Expr> },
    /// Function call
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    /// CASE expression
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    /// CAST expression
    Cast { expr: Box<Expr>, type_name: String },
    /// IN expression
    In {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// BETWEEN expression
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    /// LIKE expression
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
        negated: bool,
    },
    /// IS NULL / IS NOT NULL
    IsNull { expr: Box<Expr>, negated: bool },
    /// Subquery
    Subquery(Box<Select>),
    /// EXISTS subquery
    Exists {
        subquery: Box<Select>,
        negated: bool,
    },
    /// Collate
    Collate { expr: Box<Expr>, collation: String },
    /// Parameter placeholder (?N or :name)
    Parameter {
        index: Option<i32>,
        name: Option<String>,
    },
    /// CURRENT_TIME
    CurrentTime,
    /// CURRENT_DATE
    CurrentDate,
    /// CURRENT_TIMESTAMP
    CurrentTimestamp,
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Concat,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    BitAnd,
    BitOr,
    LeftShift,
    RightShift,
    Is,
    IsNot,
    Glob,
    Match,
    Regexp,
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
    BitNot,
    Plus,
}

/// Select statement placeholder (will be expanded in parser)
#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub columns: Vec<ResultColumn>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Option<Vec<Expr>>,
    pub having: Option<Expr>,
    pub order_by: Option<Vec<OrderTerm>>,
    pub limit: Option<LimitClause>,
    pub compound: Option<Box<CompoundSelect>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResultColumn {
    All,
    TableAll(String),
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub tables: Vec<TableRef>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    Table {
        name: QualifiedName,
        alias: Option<String>,
        indexed_by: Option<String>,
    },
    Subquery {
        select: Box<Select>,
        alias: String,
    },
    Join {
        left: Box<TableRef>,
        join_type: JoinType,
        right: Box<TableRef>,
        constraint: Option<JoinConstraint>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Natural,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderTerm {
    pub expr: Expr,
    pub order: SortOrder,
    pub nulls: Option<NullsOrder>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LimitClause {
    pub limit: Expr,
    pub offset: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompoundSelect {
    pub op: CompoundOp,
    pub select: Select,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompoundOp {
    Union,
    UnionAll,
    Intersect,
    Except,
}

// ============================================================================
// Qualified Name
// ============================================================================

/// Qualified name for database objects (schema.table)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QualifiedName {
    /// Schema/database name (main, temp, attached)
    pub schema: Option<String>,
    /// Object name
    pub name: String,
}

impl QualifiedName {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            schema: None,
            name: name.into(),
        }
    }

    pub fn with_schema(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: Some(schema.into()),
            name: name.into(),
        }
    }

    /// Get database index (0=main, 1=temp, 2+=attached)
    pub fn database_idx(&self) -> i32 {
        match self.schema.as_deref() {
            None | Some("main") => 0,
            Some("temp") => 1,
            _ => 2, // Will need proper lookup for attached DBs
        }
    }
}

// ============================================================================
// Default Value
// ============================================================================

/// Default value for a column
#[derive(Debug, Clone, PartialEq)]
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

// ============================================================================
// Generated Column
// ============================================================================

/// Generated (computed) column information
#[derive(Debug, Clone, PartialEq)]
pub struct GeneratedColumn {
    /// Expression that generates the value
    pub expr: Expr,
    /// Storage type
    pub storage: GeneratedStorage,
}

/// Storage type for generated columns
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GeneratedStorage {
    /// Computed on read (VIRTUAL)
    #[default]
    Virtual,
    /// Stored in database (STORED)
    Stored,
}

// ============================================================================
// Column Definition
// ============================================================================

/// Column in a table
#[derive(Debug, Clone, PartialEq)]
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
    /// Is part of primary key
    pub is_primary_key: bool,
    /// Is hidden (generated, rowid, etc.)
    pub is_hidden: bool,
    /// Generated column expression
    pub generated: Option<GeneratedColumn>,
}

impl Default for Column {
    fn default() -> Self {
        Self {
            name: String::new(),
            type_name: None,
            affinity: Affinity::Blob,
            not_null: false,
            not_null_conflict: None,
            default_value: None,
            collation: DEFAULT_COLLATION.to_string(),
            is_primary_key: false,
            is_hidden: false,
            generated: None,
        }
    }
}

impl Column {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }
}

// ============================================================================
// Foreign Key
// ============================================================================

/// Foreign key constraint
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKey {
    /// Columns in this table
    pub columns: Vec<usize>,
    /// Referenced table name
    pub ref_table: String,
    /// Referenced columns (None = primary key)
    pub ref_columns: Option<Vec<String>>,
    /// ON DELETE action
    pub on_delete: FkAction,
    /// ON UPDATE action
    pub on_update: FkAction,
    /// Deferrable type
    pub deferrable: Deferrable,
}

impl Default for ForeignKey {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            ref_table: String::new(),
            ref_columns: None,
            on_delete: FkAction::NoAction,
            on_update: FkAction::NoAction,
            deferrable: Deferrable::NotDeferrable,
        }
    }
}

// ============================================================================
// Index
// ============================================================================

/// Index column specification
#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumn {
    /// Index into table columns, or -1 for expression
    pub column_idx: i32,
    /// Expression (for expression indexes)
    pub expr: Option<Expr>,
    /// Sort order
    pub sort_order: SortOrder,
    /// Collation sequence
    pub collation: String,
}

impl Default for IndexColumn {
    fn default() -> Self {
        Self {
            column_idx: -1,
            expr: None,
            sort_order: SortOrder::Asc,
            collation: DEFAULT_COLLATION.to_string(),
        }
    }
}

/// Database index
#[derive(Debug, Clone, PartialEq)]
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
    /// CREATE INDEX statement (for schema table)
    pub sql: Option<String>,
    /// Statistics for the index (sqlite_stat1)
    pub stats: Option<IndexStats>,
}

impl Default for Index {
    fn default() -> Self {
        Self {
            name: String::new(),
            table: String::new(),
            columns: Vec::new(),
            root_page: 0,
            unique: false,
            partial: None,
            is_primary_key: false,
            sql: None,
            stats: None,
        }
    }
}

impl Index {
    pub fn new(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            ..Default::default()
        }
    }
}

// ============================================================================
// Table
// ============================================================================

/// Database table
#[derive(Debug, Clone, PartialEq)]
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
    /// Has AUTOINCREMENT column
    pub autoincrement: bool,
    /// CREATE TABLE statement (for schema table)
    pub sql: Option<String>,
    /// Estimated row count from ANALYZE
    pub row_estimate: i64,
}

impl Default for Table {
    fn default() -> Self {
        Self {
            name: String::new(),
            db_idx: 0,
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
            row_estimate: 0,
        }
    }
}

impl Table {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Find column index by name
    pub fn find_column(&self, name: &str) -> Result<i32> {
        for (i, col) in self.columns.iter().enumerate() {
            if col.name.eq_ignore_ascii_case(name) {
                return Ok(i as i32);
            }
        }
        Err(Error::with_message(
            ErrorCode::Error,
            format!("no such column: {}", name),
        ))
    }

    /// Get column by name
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Check if table has an INTEGER PRIMARY KEY (rowid alias)
    pub fn has_rowid_alias(&self) -> bool {
        if self.without_rowid {
            return false;
        }
        if let Some(ref pk) = self.primary_key {
            if pk.len() == 1 {
                let col = &self.columns[pk[0]];
                return col.affinity == Affinity::Integer;
            }
        }
        false
    }
}

// ============================================================================
// Trigger (placeholder)
// ============================================================================

/// Trigger timing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

/// Trigger event type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

/// Database trigger
#[derive(Debug, Clone, PartialEq)]
pub struct Trigger {
    /// Trigger name
    pub name: String,
    /// Table this trigger is on
    pub table: String,
    /// Timing (BEFORE, AFTER, INSTEAD OF)
    pub timing: TriggerTiming,
    /// Event type (INSERT, UPDATE, DELETE)
    pub event: TriggerEvent,
    /// FOR EACH ROW (always true in SQLite)
    pub for_each_row: bool,
    /// UPDATE OF columns (for UPDATE triggers)
    pub update_columns: Option<Vec<String>>,
    /// WHEN clause
    pub when_clause: Option<Expr>,
    /// Trigger body statements (placeholder - will be Statement list)
    pub body: Vec<TriggerStep>,
    /// CREATE TRIGGER statement
    pub sql: Option<String>,
}

impl Default for Trigger {
    fn default() -> Self {
        Self {
            name: String::new(),
            table: String::new(),
            timing: TriggerTiming::Before,
            event: TriggerEvent::Insert,
            for_each_row: true,
            update_columns: None,
            when_clause: None,
            body: Vec::new(),
            sql: None,
        }
    }
}

/// Trigger step (placeholder for trigger body statement)
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerStep {
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
    },
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        where_clause: Option<Expr>,
    },
    Delete {
        table: String,
        where_clause: Option<Expr>,
    },
    Select(Select),
}

// ============================================================================
// Schema
// ============================================================================

/// Database schema containing all objects
#[derive(Debug, Clone, Default)]
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
    /// Text encoding
    pub encoding: Encoding,
    /// sqlite_stat1 rows keyed by (table, index)
    pub stat1: HashMap<(String, Option<String>), Stat1Row>,
}

impl Schema {
    /// Create a new empty schema
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if table exists
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(&name.to_lowercase())
    }

    /// Get table by name (case-insensitive)
    pub fn table(&self, name: &str) -> Option<Arc<Table>> {
        self.tables.get(&name.to_lowercase()).cloned()
    }

    /// Check if index exists
    pub fn index_exists(&self, name: &str) -> bool {
        self.indexes.contains_key(&name.to_lowercase())
    }

    /// Get index by name (case-insensitive)
    pub fn index(&self, name: &str) -> Option<Arc<Index>> {
        self.indexes.get(&name.to_lowercase()).cloned()
    }

    /// Get trigger by name (case-insensitive)
    pub fn trigger(&self, name: &str) -> Option<Arc<Trigger>> {
        self.triggers.get(&name.to_lowercase()).cloned()
    }

    /// Remove all sqlite_stat1 rows for a table
    pub fn clear_stat1_for_table(&mut self, table: &str) {
        let table_key = table.to_lowercase();
        self.stat1
            .retain(|(tbl, _), _| !tbl.eq_ignore_ascii_case(&table_key));
    }

    /// Insert or replace a sqlite_stat1 row and apply it to schema objects
    pub fn set_stat1(&mut self, row: Stat1Row) -> Result<()> {
        let key = (
            row.tbl.to_lowercase(),
            row.idx.as_ref().map(|s| s.to_lowercase()),
        );
        self.stat1.insert(key, row.clone());
        self.apply_stat1_row(&row)
    }

    /// Apply all sqlite_stat1 rows to tables and indexes
    pub fn load_statistics(&mut self) -> Result<()> {
        let rows: Vec<Stat1Row> = self.stat1.values().cloned().collect();
        for row in rows {
            self.apply_stat1_row(&row)?;
        }
        Ok(())
    }

    fn apply_stat1_row(&mut self, row: &Stat1Row) -> Result<()> {
        let stats = parse_stat1(row.stat.as_str())?;
        if let Some(idx_name) = row.idx.as_ref() {
            let key = idx_name.to_lowercase();
            if let Some(index_arc) = self.indexes.get(&key).cloned() {
                let mut index = (*index_arc).clone();
                index.stats = Some(stats);
                self.indexes.insert(key, Arc::new(index));
            }
        } else {
            let key = row.tbl.to_lowercase();
            if let Some(table_arc) = self.tables.get_mut(&key) {
                let table = Arc::make_mut(table_arc);
                table.row_estimate = stats.row_count;
            }
        }
        Ok(())
    }
}

/// sqlite_stat1 row
#[derive(Debug, Clone, PartialEq)]
pub struct Stat1Row {
    pub tbl: String,
    pub idx: Option<String>,
    pub stat: String,
}

/// Statistics for an index from sqlite_stat1
#[derive(Debug, Clone, PartialEq)]
pub struct IndexStats {
    pub row_count: i64,
    pub avg_eq: Vec<f64>,
    pub n_distinct: Vec<i64>,
}

/// sqlite_stat4 row (sampled statistics)
#[derive(Debug, Clone, PartialEq)]
pub struct Stat4Row {
    pub tbl: String,
    pub idx: String,
    pub nlt: Vec<i64>,
    pub ndlt: Vec<i64>,
    pub neq: Vec<i64>,
    pub sample: Vec<u8>,
}

fn parse_stat1(stat: &str) -> Result<IndexStats> {
    let mut parts = stat.split_whitespace();
    let row_part = parts
        .next()
        .ok_or_else(|| Error::with_message(ErrorCode::Corrupt, "empty stat string"))?;
    let row_count = row_part
        .parse::<i64>()
        .map_err(|_| Error::with_message(ErrorCode::Corrupt, "invalid row count in stat string"))?;

    let mut avg_eq = Vec::new();
    let mut n_distinct = Vec::new();
    for part in parts {
        let avg = part
            .parse::<f64>()
            .map_err(|_| Error::with_message(ErrorCode::Corrupt, "invalid index stat value"))?;
        avg_eq.push(avg);
        let distinct = if avg > 0.0 {
            ((row_count as f64) / avg).round() as i64
        } else {
            0
        };
        n_distinct.push(distinct);
    }

    Ok(IndexStats {
        row_count,
        avg_eq,
        n_distinct,
    })
}

// ============================================================================
// Type Affinity
// ============================================================================

/// Determine column affinity from type name (Section 3.1.1 of SQLite docs)
///
/// The affinity is determined by the declared type using these rules:
/// 1. If type contains "INT" -> INTEGER
/// 2. If type contains "CHAR", "CLOB", or "TEXT" -> TEXT
/// 3. If type contains "BLOB" or is empty -> BLOB
/// 4. If type contains "REAL", "FLOA", or "DOUB" -> REAL
/// 5. Otherwise -> NUMERIC
pub fn type_affinity(type_name: &str) -> Affinity {
    let upper = type_name.to_uppercase();

    // Rule 1: INT -> INTEGER
    if upper.contains("INT") {
        return Affinity::Integer;
    }

    // Rule 2: CHAR, CLOB, TEXT -> TEXT
    if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        return Affinity::Text;
    }

    // Rule 3: BLOB or empty -> BLOB
    if upper.contains("BLOB") || type_name.is_empty() {
        return Affinity::Blob;
    }

    // Rule 4: REAL, FLOA, DOUB -> REAL
    if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        return Affinity::Real;
    }

    // Rule 5: Otherwise -> NUMERIC
    Affinity::Numeric
}

// ============================================================================
// DDL Statement Types
// ============================================================================

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    /// IF NOT EXISTS
    pub if_not_exists: bool,
    /// Table name
    pub name: QualifiedName,
    /// Table definition
    pub definition: TableDefinition,
    /// WITHOUT ROWID
    pub without_rowid: bool,
    /// STRICT
    pub strict: bool,
}

/// Table definition (columns or AS SELECT)
#[derive(Debug, Clone, PartialEq)]
pub enum TableDefinition {
    /// Column definitions and constraints
    Columns {
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    },
    /// AS SELECT (CREATE TABLE ... AS SELECT)
    AsSelect(Select),
}

/// Column definition in CREATE TABLE
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Type name
    pub type_name: Option<String>,
    /// Column constraints
    pub constraints: Vec<ColumnConstraint>,
}

/// Column constraint
#[derive(Debug, Clone, PartialEq)]
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
    ForeignKey {
        ref_table: String,
        ref_columns: Option<Vec<String>>,
        on_delete: Option<FkAction>,
        on_update: Option<FkAction>,
        deferrable: Option<Deferrable>,
    },
    Generated {
        expr: Expr,
        storage: GeneratedStorage,
    },
}

/// Table constraint
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey {
        columns: Vec<IndexedColumn>,
        conflict: Option<ConflictAction>,
    },
    Unique {
        columns: Vec<IndexedColumn>,
        conflict: Option<ConflictAction>,
    },
    Check(Expr),
    ForeignKey {
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Option<Vec<String>>,
        on_delete: Option<FkAction>,
        on_update: Option<FkAction>,
        deferrable: Option<Deferrable>,
    },
}

/// Indexed column specification
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedColumn {
    /// Column name or expression
    pub name: Option<String>,
    /// Expression (for expression indexes)
    pub expr: Option<Expr>,
    /// Collation
    pub collation: Option<String>,
    /// Sort order
    pub order: Option<SortOrder>,
}

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStmt {
    /// UNIQUE index
    pub unique: bool,
    /// IF NOT EXISTS
    pub if_not_exists: bool,
    /// Index name
    pub name: QualifiedName,
    /// Table to index
    pub table: String,
    /// Indexed columns
    pub columns: Vec<IndexedColumn>,
    /// WHERE clause (partial index)
    pub where_clause: Option<Expr>,
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStmt {
    /// IF EXISTS
    pub if_exists: bool,
    /// Table name
    pub name: QualifiedName,
}

/// DROP INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStmt {
    /// IF EXISTS
    pub if_exists: bool,
    /// Index name
    pub name: QualifiedName,
}

// ============================================================================
// ALTER TABLE Statement
// ============================================================================

/// ALTER TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStmt {
    pub table: QualifiedName,
    pub action: AlterTableAction,
}

/// ALTER TABLE action
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction {
    RenameTable(String),
    RenameColumn { old: String, new: String },
    AddColumn(ColumnDef),
    DropColumn(String),
}

// ============================================================================
// Schema Builder Implementation
// ============================================================================

impl Schema {
    /// Process CREATE TABLE statement (sqlite3StartTable, sqlite3EndTable)
    pub fn create_table(&mut self, stmt: &CreateTableStmt) -> Result<()> {
        let name_lower = stmt.name.name.to_lowercase();

        // Check if table exists
        if self.tables.contains_key(&name_lower) {
            if stmt.if_not_exists {
                return Ok(());
            }
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("table {} already exists", stmt.name.name),
            ));
        }

        // Build table structure
        let table = match &stmt.definition {
            TableDefinition::Columns {
                columns,
                constraints,
            } => self.build_table_from_columns(&stmt.name, columns, constraints, stmt)?,
            TableDefinition::AsSelect(_select) => {
                // For CREATE TABLE ... AS SELECT, we would need to analyze the select
                // to determine column types. For now, return an error.
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "CREATE TABLE ... AS SELECT not yet implemented",
                ));
            }
        };

        self.tables.insert(name_lower, Arc::new(table));
        Ok(())
    }

    fn build_table_from_columns(
        &self,
        name: &QualifiedName,
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
        stmt: &CreateTableStmt,
    ) -> Result<Table> {
        let mut table = Table {
            name: name.name.clone(),
            db_idx: name.database_idx(),
            root_page: 0, // Will be set when allocating btree page
            columns: Vec::new(),
            primary_key: None,
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            checks: Vec::new(),
            without_rowid: stmt.without_rowid,
            strict: stmt.strict,
            is_virtual: false,
            autoincrement: false,
            sql: None,
            row_estimate: 0,
        };

        // Process columns
        for col_def in columns {
            let column = self.build_column(col_def, &mut table)?;
            table.columns.push(column);
        }

        // Process table constraints
        for constraint in constraints {
            self.apply_table_constraint(&mut table, constraint)?;
        }

        // Validate the table
        self.validate_table(&table)?;

        Ok(table)
    }

    fn build_column(&self, def: &ColumnDef, table: &mut Table) -> Result<Column> {
        let affinity = def
            .type_name
            .as_ref()
            .map(|t| type_affinity(t))
            .unwrap_or(Affinity::Blob);

        let mut column = Column {
            name: def.name.clone(),
            type_name: def.type_name.clone(),
            affinity,
            not_null: false,
            not_null_conflict: None,
            default_value: None,
            collation: DEFAULT_COLLATION.to_string(),
            is_primary_key: false,
            is_hidden: false,
            generated: None,
        };

        // Apply column constraints
        for constraint in &def.constraints {
            self.apply_column_constraint(&mut column, constraint, table)?;
        }

        Ok(column)
    }

    fn apply_column_constraint(
        &self,
        column: &mut Column,
        constraint: &ColumnConstraint,
        table: &mut Table,
    ) -> Result<()> {
        match constraint {
            ColumnConstraint::PrimaryKey {
                order: _,
                conflict,
                autoincrement,
            } => {
                column.is_primary_key = true;
                column.not_null = true;
                column.not_null_conflict = *conflict;

                // Set table primary key
                let col_idx = table.columns.len();
                if table.primary_key.is_none() {
                    table.primary_key = Some(vec![col_idx]);
                } else {
                    // Multiple PRIMARY KEY constraints
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "table has multiple primary keys",
                    ));
                }

                if *autoincrement {
                    if column.affinity != Affinity::Integer {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            "AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY",
                        ));
                    }
                    table.autoincrement = true;
                }
            }
            ColumnConstraint::NotNull { conflict } => {
                column.not_null = true;
                column.not_null_conflict = *conflict;
            }
            ColumnConstraint::Unique { conflict: _ } => {
                // Unique constraint creates an implicit index
                // Will be handled during index creation
            }
            ColumnConstraint::Check(expr) => {
                table.checks.push(expr.clone());
            }
            ColumnConstraint::Default(value) => {
                column.default_value = Some(value.clone());
            }
            ColumnConstraint::Collate(name) => {
                column.collation = name.clone();
            }
            ColumnConstraint::ForeignKey {
                ref_table,
                ref_columns,
                on_delete,
                on_update,
                deferrable,
            } => {
                let col_idx = table.columns.len();
                table.foreign_keys.push(ForeignKey {
                    columns: vec![col_idx],
                    ref_table: ref_table.clone(),
                    ref_columns: ref_columns.clone(),
                    on_delete: on_delete.unwrap_or(FkAction::NoAction),
                    on_update: on_update.unwrap_or(FkAction::NoAction),
                    deferrable: deferrable.unwrap_or(Deferrable::NotDeferrable),
                });
            }
            ColumnConstraint::Generated { expr, storage } => {
                column.generated = Some(GeneratedColumn {
                    expr: expr.clone(),
                    storage: *storage,
                });
                column.is_hidden = *storage == GeneratedStorage::Virtual;
            }
        }
        Ok(())
    }

    fn apply_table_constraint(
        &self,
        table: &mut Table,
        constraint: &TableConstraint,
    ) -> Result<()> {
        match constraint {
            TableConstraint::PrimaryKey { columns, conflict } => {
                if table.primary_key.is_some() {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "table has multiple primary keys",
                    ));
                }

                let mut pk_indices = Vec::new();
                for col in columns {
                    if let Some(name) = &col.name {
                        let idx = table.find_column(name)?;
                        pk_indices.push(idx as usize);
                        table.columns[idx as usize].is_primary_key = true;
                        table.columns[idx as usize].not_null = true;
                        if let Some(c) = conflict {
                            table.columns[idx as usize].not_null_conflict = Some(*c);
                        }
                    }
                }
                table.primary_key = Some(pk_indices);
            }
            TableConstraint::Unique {
                columns: _,
                conflict: _,
            } => {
                // Unique constraint creates an implicit index
                // Will be handled during index creation
            }
            TableConstraint::Check(expr) => {
                table.checks.push(expr.clone());
            }
            TableConstraint::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                on_delete,
                on_update,
                deferrable,
            } => {
                let mut col_indices = Vec::new();
                for col_name in columns {
                    let idx = table.find_column(col_name)?;
                    col_indices.push(idx as usize);
                }
                table.foreign_keys.push(ForeignKey {
                    columns: col_indices,
                    ref_table: ref_table.clone(),
                    ref_columns: ref_columns.clone(),
                    on_delete: on_delete.unwrap_or(FkAction::NoAction),
                    on_update: on_update.unwrap_or(FkAction::NoAction),
                    deferrable: deferrable.unwrap_or(Deferrable::NotDeferrable),
                });
            }
        }
        Ok(())
    }

    fn validate_table(&self, table: &Table) -> Result<()> {
        // WITHOUT ROWID requires a PRIMARY KEY
        if table.without_rowid && table.primary_key.is_none() {
            return Err(Error::with_message(
                ErrorCode::Error,
                "PRIMARY KEY missing on table declared WITHOUT ROWID",
            ));
        }

        // AUTOINCREMENT is only for rowid tables
        if table.autoincrement && table.without_rowid {
            return Err(Error::with_message(
                ErrorCode::Error,
                "AUTOINCREMENT not allowed on WITHOUT ROWID tables",
            ));
        }

        // Must have at least one column
        if table.columns.is_empty() {
            return Err(Error::with_message(
                ErrorCode::Error,
                "table has no columns",
            ));
        }

        // Check for duplicate column names
        let mut seen = std::collections::HashSet::new();
        for col in &table.columns {
            let lower = col.name.to_lowercase();
            if !seen.insert(lower) {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("duplicate column name: {}", col.name),
                ));
            }
        }

        Ok(())
    }

    /// Process CREATE INDEX statement
    pub fn create_index(&mut self, stmt: &CreateIndexStmt) -> Result<()> {
        let name_lower = stmt.name.name.to_lowercase();

        // Check if index exists
        if self.indexes.contains_key(&name_lower) {
            if stmt.if_not_exists {
                return Ok(());
            }
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("index {} already exists", stmt.name.name),
            ));
        }

        // Find table
        let table = self.tables.get(&stmt.table.to_lowercase()).ok_or_else(|| {
            Error::with_message(ErrorCode::Error, format!("no such table: {}", stmt.table))
        })?;

        // Build index structure
        let mut index = Index {
            name: stmt.name.name.clone(),
            table: stmt.table.clone(),
            columns: Vec::new(),
            root_page: 0, // Will be set when allocating btree page
            unique: stmt.unique,
            partial: stmt.where_clause.clone(),
            is_primary_key: false,
            sql: None,
            stats: None,
        };

        // Process indexed columns
        for indexed_col in &stmt.columns {
            let col_idx = if indexed_col.expr.is_some() {
                -1 // Expression index
            } else if let Some(name) = &indexed_col.name {
                table.find_column(name)?
            } else {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "index column has no name or expression",
                ));
            };

            index.columns.push(IndexColumn {
                column_idx: col_idx,
                expr: indexed_col.expr.clone(),
                sort_order: indexed_col.order.unwrap_or(SortOrder::Asc),
                collation: indexed_col
                    .collation
                    .clone()
                    .unwrap_or_else(|| DEFAULT_COLLATION.to_string()),
            });
        }

        self.indexes.insert(name_lower, Arc::new(index));
        Ok(())
    }

    /// Process DROP TABLE statement
    pub fn drop_table(&mut self, stmt: &DropTableStmt) -> Result<()> {
        let name_lower = stmt.name.name.to_lowercase();

        if self.tables.remove(&name_lower).is_none() {
            if stmt.if_exists {
                return Ok(());
            }
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", stmt.name.name),
            ));
        }

        // Remove associated indexes
        let indexes_to_remove: Vec<_> = self
            .indexes
            .iter()
            .filter(|(_, idx)| idx.table.eq_ignore_ascii_case(&stmt.name.name))
            .map(|(k, _)| k.clone())
            .collect();

        for idx_name in indexes_to_remove {
            self.indexes.remove(&idx_name);
        }

        // Remove associated triggers
        let triggers_to_remove: Vec<_> = self
            .triggers
            .iter()
            .filter(|(_, trig)| trig.table.eq_ignore_ascii_case(&stmt.name.name))
            .map(|(k, _)| k.clone())
            .collect();

        for trig_name in triggers_to_remove {
            self.triggers.remove(&trig_name);
        }

        Ok(())
    }

    /// Process DROP INDEX statement
    pub fn drop_index(&mut self, stmt: &DropIndexStmt) -> Result<()> {
        let name_lower = stmt.name.name.to_lowercase();

        if self.indexes.remove(&name_lower).is_none() {
            if stmt.if_exists {
                return Ok(());
            }
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("no such index: {}", stmt.name.name),
            ));
        }

        Ok(())
    }

    /// Process ALTER TABLE statement
    pub fn alter_table(&mut self, stmt: &AlterTableStmt) -> Result<()> {
        let table_name = stmt.table.name.clone();
        let table_key = table_name.to_lowercase();
        let table_arc = self.tables.get(&table_key).cloned().ok_or_else(|| {
            Error::with_message(ErrorCode::Error, format!("no such table: {}", table_name))
        })?;

        if is_system_table_name(&table_name) {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("table {} may not be altered", table_name),
            ));
        }

        match &stmt.action {
            AlterTableAction::RenameTable(new_name) => {
                self.rename_table(&table_arc, &table_name, new_name)
            }
            AlterTableAction::RenameColumn { old, new } => {
                self.rename_column(&table_arc, &table_name, old, new)
            }
            AlterTableAction::AddColumn(def) => self.add_column(&table_arc, def),
            AlterTableAction::DropColumn(name) => self.drop_column(&table_arc, name),
        }
    }

    fn rename_table(
        &mut self,
        table_arc: &Arc<Table>,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let new_key = new_name.to_lowercase();
        if new_key == old_name.to_lowercase() {
            return Ok(());
        }
        if self.table_exists(new_name) || self.index_exists(new_name) {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!(
                    "there is already another table or index with this name: {}",
                    new_name
                ),
            ));
        }
        if is_system_table_name(new_name) {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("table {} may not be altered", new_name),
            ));
        }

        self.update_foreign_keys_for_table(old_name, new_name)?;

        let mut table = (**table_arc).clone();
        table.name = new_name.to_string();

        let mut index_updates = Vec::new();
        for (name, idx) in &self.indexes {
            if idx.table.eq_ignore_ascii_case(old_name) {
                index_updates.push(name.clone());
            }
        }
        for name in index_updates {
            if let Some(idx_arc) = self.indexes.get(&name).cloned() {
                let mut idx = (*idx_arc).clone();
                idx.table = new_name.to_string();
                let arc = Arc::new(idx);
                self.indexes.insert(name.clone(), arc.clone());
            }
        }

        table.indexes = table
            .indexes
            .iter()
            .filter_map(|idx| self.indexes.get(&idx.name.to_lowercase()).cloned())
            .collect();

        let mut trigger_updates = Vec::new();
        for (name, trigger) in &self.triggers {
            if trigger.table.eq_ignore_ascii_case(old_name) {
                trigger_updates.push(name.clone());
            }
        }
        for name in trigger_updates {
            if let Some(trig) = self.triggers.get(&name).cloned() {
                let mut updated = (*trig).clone();
                updated.table = new_name.to_string();
                self.triggers.insert(name, Arc::new(updated));
            }
        }

        self.tables.remove(&old_name.to_lowercase());
        self.tables.insert(new_key, Arc::new(table));
        Ok(())
    }

    fn rename_column(
        &mut self,
        table_arc: &Arc<Table>,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let mut table = (**table_arc).clone();
        let col_idx = table.find_column(old_name)? as usize;
        if table.column(new_name).is_some() {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("duplicate column name: {}", new_name),
            ));
        }

        table.columns[col_idx].name = new_name.to_string();

        let updates = update_ref_columns_for_table(self, table_name, old_name, new_name)?;
        for (key, updated) in updates {
            self.tables.insert(key, updated);
        }

        self.tables
            .insert(table_name.to_lowercase(), Arc::new(table));
        Ok(())
    }

    fn add_column(&mut self, table_arc: &Arc<Table>, def: &ColumnDef) -> Result<()> {
        let mut table = (**table_arc).clone();

        if table.column(&def.name).is_some() {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("duplicate column name: {}", def.name),
            ));
        }

        let mut has_not_null = false;
        let mut has_default = false;
        let mut has_primary_key = false;
        let mut has_unique = false;
        let mut has_stored_generated = false;
        let mut has_expr_default = false;

        for constraint in &def.constraints {
            match constraint {
                ColumnConstraint::PrimaryKey { .. } => has_primary_key = true,
                ColumnConstraint::Unique { .. } => has_unique = true,
                ColumnConstraint::NotNull { .. } => has_not_null = true,
                ColumnConstraint::Default(value) => {
                    has_default = !matches!(value, DefaultValue::Null);
                    if matches!(value, DefaultValue::Expr(_)) {
                        has_expr_default = true;
                    }
                }
                ColumnConstraint::Generated { storage, .. } => {
                    if *storage == GeneratedStorage::Stored {
                        has_stored_generated = true;
                    }
                }
                _ => {}
            }
        }

        if has_primary_key {
            return Err(Error::with_message(
                ErrorCode::Error,
                "Cannot add a PRIMARY KEY column",
            ));
        }
        if has_unique {
            return Err(Error::with_message(
                ErrorCode::Error,
                "Cannot add a UNIQUE column",
            ));
        }
        if has_not_null && !has_default {
            return Err(Error::with_message(
                ErrorCode::Error,
                "Cannot add a NOT NULL column with default value NULL",
            ));
        }
        if has_expr_default {
            return Err(Error::with_message(
                ErrorCode::Error,
                "Cannot add a column with non-constant default",
            ));
        }
        if has_stored_generated {
            return Err(Error::with_message(
                ErrorCode::Error,
                "cannot add a STORED column",
            ));
        }

        let column = self.build_column(def, &mut table)?;
        table.columns.push(column);
        self.tables
            .insert(table.name.to_lowercase(), Arc::new(table));
        Ok(())
    }

    fn drop_column(&mut self, table_arc: &Arc<Table>, name: &str) -> Result<()> {
        let mut table = (**table_arc).clone();
        let col_idx = table.find_column(name)? as usize;

        if table.columns.len() <= 1 {
            return Err(Error::with_message(
                ErrorCode::Error,
                "cannot drop column: only one column remaining",
            ));
        }

        if table
            .primary_key
            .as_ref()
            .map_or(false, |pk| pk.contains(&col_idx))
        {
            return Err(Error::with_message(
                ErrorCode::Error,
                "cannot drop PRIMARY KEY column",
            ));
        }

        for idx in &table.indexes {
            if idx
                .columns
                .iter()
                .any(|col| col.column_idx == col_idx as i32)
            {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "cannot drop column used by an index",
                ));
            }
        }

        for fk in &table.foreign_keys {
            if fk.columns.iter().any(|c| *c == col_idx) {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "cannot drop column used by a foreign key",
                ));
            }
        }

        if references_column(self, &table.name, name) {
            return Err(Error::with_message(
                ErrorCode::Error,
                "cannot drop column referenced by a foreign key",
            ));
        }

        table.columns.remove(col_idx);

        if let Some(pk) = &mut table.primary_key {
            for entry in pk.iter_mut() {
                if *entry > col_idx {
                    *entry -= 1;
                }
            }
        }

        for fk in &mut table.foreign_keys {
            for col in fk.columns.iter_mut() {
                if *col > col_idx {
                    *col -= 1;
                }
            }
        }

        let mut index_updates = Vec::new();
        for (name, idx) in &self.indexes {
            if idx.table.eq_ignore_ascii_case(&table.name) {
                index_updates.push(name.clone());
            }
        }
        for name in index_updates {
            if let Some(idx_arc) = self.indexes.get(&name).cloned() {
                let mut idx = (*idx_arc).clone();
                for col in idx.columns.iter_mut() {
                    if col.column_idx > col_idx as i32 {
                        col.column_idx -= 1;
                    }
                }
                let arc = Arc::new(idx);
                self.indexes.insert(name.clone(), arc.clone());
            }
        }

        table.indexes = table
            .indexes
            .iter()
            .filter_map(|idx| self.indexes.get(&idx.name.to_lowercase()).cloned())
            .collect();

        self.tables
            .insert(table.name.to_lowercase(), Arc::new(table));
        Ok(())
    }

    fn update_foreign_keys_for_table(&mut self, old: &str, new: &str) -> Result<()> {
        let mut updates = Vec::new();
        for (key, table_arc) in &self.tables {
            let mut table = (**table_arc).clone();
            let mut changed = false;
            for fk in &mut table.foreign_keys {
                if fk.ref_table.eq_ignore_ascii_case(old) {
                    fk.ref_table = new.to_string();
                    changed = true;
                }
            }
            if changed {
                updates.push((key.clone(), Arc::new(table)));
            }
        }
        for (key, arc) in updates {
            self.tables.insert(key, arc);
        }
        Ok(())
    }
}

fn is_system_table_name(name: &str) -> bool {
    name.to_lowercase().starts_with("sqlite_")
}

fn update_ref_columns_for_table(
    schema: &Schema,
    table_name: &str,
    old_col: &str,
    new_col: &str,
) -> Result<Vec<(String, Arc<Table>)>> {
    let mut updates = Vec::new();
    for (key, table_arc) in &schema.tables {
        let mut table = (**table_arc).clone();
        let mut changed = false;
        for fk in &mut table.foreign_keys {
            if fk.ref_table.eq_ignore_ascii_case(table_name) {
                if let Some(cols) = fk.ref_columns.as_mut() {
                    for col in cols.iter_mut() {
                        if col.eq_ignore_ascii_case(old_col) {
                            *col = new_col.to_string();
                            changed = true;
                        }
                    }
                }
            }
        }
        if changed {
            updates.push((key.clone(), Arc::new(table)));
        }
    }
    Ok(updates)
}

fn references_column(schema: &Schema, table_name: &str, col_name: &str) -> bool {
    for table_arc in schema.tables.values() {
        for fk in &table_arc.foreign_keys {
            if fk.ref_table.eq_ignore_ascii_case(table_name) {
                if let Some(cols) = &fk.ref_columns {
                    if cols.iter().any(|c| c.eq_ignore_ascii_case(col_name)) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_affinity() {
        // Rule 1: INT -> INTEGER
        assert_eq!(type_affinity("INT"), Affinity::Integer);
        assert_eq!(type_affinity("INTEGER"), Affinity::Integer);
        assert_eq!(type_affinity("TINYINT"), Affinity::Integer);
        assert_eq!(type_affinity("SMALLINT"), Affinity::Integer);
        assert_eq!(type_affinity("MEDIUMINT"), Affinity::Integer);
        assert_eq!(type_affinity("BIGINT"), Affinity::Integer);
        assert_eq!(type_affinity("UNSIGNED BIG INT"), Affinity::Integer);
        assert_eq!(type_affinity("INT2"), Affinity::Integer);
        assert_eq!(type_affinity("INT8"), Affinity::Integer);

        // Rule 2: CHAR, CLOB, TEXT -> TEXT
        assert_eq!(type_affinity("CHARACTER(20)"), Affinity::Text);
        assert_eq!(type_affinity("VARCHAR(255)"), Affinity::Text);
        assert_eq!(type_affinity("VARYING CHARACTER(255)"), Affinity::Text);
        assert_eq!(type_affinity("NCHAR(55)"), Affinity::Text);
        assert_eq!(type_affinity("NATIVE CHARACTER(70)"), Affinity::Text);
        assert_eq!(type_affinity("NVARCHAR(100)"), Affinity::Text);
        assert_eq!(type_affinity("TEXT"), Affinity::Text);
        assert_eq!(type_affinity("CLOB"), Affinity::Text);

        // Rule 3: BLOB -> BLOB
        assert_eq!(type_affinity("BLOB"), Affinity::Blob);
        assert_eq!(type_affinity(""), Affinity::Blob);

        // Rule 4: REAL, FLOA, DOUB -> REAL
        assert_eq!(type_affinity("REAL"), Affinity::Real);
        assert_eq!(type_affinity("DOUBLE"), Affinity::Real);
        assert_eq!(type_affinity("DOUBLE PRECISION"), Affinity::Real);
        assert_eq!(type_affinity("FLOAT"), Affinity::Real);

        // Rule 5: Otherwise -> NUMERIC
        assert_eq!(type_affinity("NUMERIC"), Affinity::Numeric);
        assert_eq!(type_affinity("DECIMAL(10,5)"), Affinity::Numeric);
        assert_eq!(type_affinity("BOOLEAN"), Affinity::Numeric);
        assert_eq!(type_affinity("DATE"), Affinity::Numeric);
        assert_eq!(type_affinity("DATETIME"), Affinity::Numeric);
    }

    #[test]
    fn test_qualified_name() {
        let name = QualifiedName::new("users");
        assert_eq!(name.database_idx(), 0);
        assert!(name.schema.is_none());
        assert_eq!(name.name, "users");

        let name = QualifiedName::with_schema("temp", "data");
        assert_eq!(name.database_idx(), 1);
        assert_eq!(name.schema, Some("temp".to_string()));
        assert_eq!(name.name, "data");

        let name = QualifiedName::with_schema("main", "users");
        assert_eq!(name.database_idx(), 0);
    }

    #[test]
    fn test_column_builder() {
        let mut col = Column::new("id");
        assert_eq!(col.name, "id");
        assert_eq!(col.affinity, Affinity::Blob);
        assert!(!col.not_null);

        col.affinity = Affinity::Integer;
        col.not_null = true;
        col.is_primary_key = true;

        assert_eq!(col.affinity, Affinity::Integer);
        assert!(col.not_null);
        assert!(col.is_primary_key);
    }

    #[test]
    fn test_table_find_column() {
        let table = Table {
            name: "users".to_string(),
            columns: vec![Column::new("id"), Column::new("name"), Column::new("email")],
            ..Default::default()
        };

        assert_eq!(table.find_column("id").unwrap(), 0);
        assert_eq!(table.find_column("NAME").unwrap(), 1);
        assert_eq!(table.find_column("Email").unwrap(), 2);
        assert!(table.find_column("unknown").is_err());
    }

    #[test]
    fn test_schema_create_table() {
        let mut schema = Schema::new();

        let stmt = CreateTableStmt {
            if_not_exists: false,
            name: QualifiedName::new("users"),
            definition: TableDefinition::Columns {
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                        type_name: Some("INTEGER".to_string()),
                        constraints: vec![ColumnConstraint::PrimaryKey {
                            order: None,
                            conflict: None,
                            autoincrement: true,
                        }],
                    },
                    ColumnDef {
                        name: "name".to_string(),
                        type_name: Some("TEXT".to_string()),
                        constraints: vec![ColumnConstraint::NotNull { conflict: None }],
                    },
                    ColumnDef {
                        name: "email".to_string(),
                        type_name: Some("VARCHAR(255)".to_string()),
                        constraints: vec![],
                    },
                ],
                constraints: vec![],
            },
            without_rowid: false,
            strict: false,
        };

        schema.create_table(&stmt).unwrap();

        assert!(schema.table_exists("users"));
        assert!(schema.table_exists("USERS"));

        let table = schema.table("users").unwrap();
        assert_eq!(table.columns.len(), 3);
        assert_eq!(table.columns[0].affinity, Affinity::Integer);
        assert!(table.columns[0].is_primary_key);
        assert!(table.autoincrement);
        assert_eq!(table.columns[1].affinity, Affinity::Text);
        assert!(table.columns[1].not_null);
        assert_eq!(table.columns[2].affinity, Affinity::Text);
    }

    #[test]
    fn test_schema_table_already_exists() {
        let mut schema = Schema::new();

        let stmt = CreateTableStmt {
            if_not_exists: false,
            name: QualifiedName::new("users"),
            definition: TableDefinition::Columns {
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    type_name: Some("INTEGER".to_string()),
                    constraints: vec![],
                }],
                constraints: vec![],
            },
            without_rowid: false,
            strict: false,
        };

        schema.create_table(&stmt).unwrap();
        assert!(schema.create_table(&stmt).is_err());

        // IF NOT EXISTS should succeed
        let stmt_if_not_exists = CreateTableStmt {
            if_not_exists: true,
            ..stmt
        };
        assert!(schema.create_table(&stmt_if_not_exists).is_ok());
    }

    #[test]
    fn test_schema_create_index() {
        let mut schema = Schema::new();

        // First create a table
        let table_stmt = CreateTableStmt {
            if_not_exists: false,
            name: QualifiedName::new("users"),
            definition: TableDefinition::Columns {
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                        type_name: Some("INTEGER".to_string()),
                        constraints: vec![],
                    },
                    ColumnDef {
                        name: "email".to_string(),
                        type_name: Some("TEXT".to_string()),
                        constraints: vec![],
                    },
                ],
                constraints: vec![],
            },
            without_rowid: false,
            strict: false,
        };
        schema.create_table(&table_stmt).unwrap();

        // Create index
        let idx_stmt = CreateIndexStmt {
            unique: true,
            if_not_exists: false,
            name: QualifiedName::new("idx_users_email"),
            table: "users".to_string(),
            columns: vec![IndexedColumn {
                name: Some("email".to_string()),
                expr: None,
                collation: None,
                order: None,
            }],
            where_clause: None,
        };

        schema.create_index(&idx_stmt).unwrap();

        assert!(schema.index_exists("idx_users_email"));
        let idx = schema.index("idx_users_email").unwrap();
        assert!(idx.unique);
        assert_eq!(idx.table, "users");
        assert_eq!(idx.columns.len(), 1);
    }

    #[test]
    fn test_schema_drop_table() {
        let mut schema = Schema::new();

        let stmt = CreateTableStmt {
            if_not_exists: false,
            name: QualifiedName::new("users"),
            definition: TableDefinition::Columns {
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    type_name: Some("INTEGER".to_string()),
                    constraints: vec![],
                }],
                constraints: vec![],
            },
            without_rowid: false,
            strict: false,
        };

        schema.create_table(&stmt).unwrap();
        assert!(schema.table_exists("users"));

        let drop_stmt = DropTableStmt {
            if_exists: false,
            name: QualifiedName::new("users"),
        };

        schema.drop_table(&drop_stmt).unwrap();
        assert!(!schema.table_exists("users"));

        // Drop non-existent should fail
        assert!(schema.drop_table(&drop_stmt).is_err());

        // IF EXISTS should succeed
        let drop_if_exists = DropTableStmt {
            if_exists: true,
            name: QualifiedName::new("users"),
        };
        assert!(schema.drop_table(&drop_if_exists).is_ok());
    }

    #[test]
    fn test_schema_alter_table_rename_and_fk_updates() {
        let mut schema = Schema::new();

        let parent = CreateTableStmt {
            if_not_exists: false,
            name: QualifiedName::new("parent"),
            definition: TableDefinition::Columns {
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    type_name: Some("INTEGER".to_string()),
                    constraints: vec![ColumnConstraint::PrimaryKey {
                        order: None,
                        conflict: None,
                        autoincrement: false,
                    }],
                }],
                constraints: vec![],
            },
            without_rowid: false,
            strict: false,
        };

        let child = CreateTableStmt {
            if_not_exists: false,
            name: QualifiedName::new("child"),
            definition: TableDefinition::Columns {
                columns: vec![ColumnDef {
                    name: "parent_id".to_string(),
                    type_name: Some("INTEGER".to_string()),
                    constraints: vec![ColumnConstraint::ForeignKey {
                        ref_table: "parent".to_string(),
                        ref_columns: Some(vec!["id".to_string()]),
                        on_delete: None,
                        on_update: None,
                        deferrable: None,
                    }],
                }],
                constraints: vec![],
            },
            without_rowid: false,
            strict: false,
        };

        schema.create_table(&parent).unwrap();
        schema.create_table(&child).unwrap();

        let rename = AlterTableStmt {
            table: QualifiedName::new("parent"),
            action: AlterTableAction::RenameTable("parent2".to_string()),
        };
        schema.alter_table(&rename).unwrap();

        assert!(schema.table_exists("parent2"));
        let child_table = schema.table("child").unwrap();
        assert_eq!(child_table.foreign_keys[0].ref_table, "parent2");

        let rename_col = AlterTableStmt {
            table: QualifiedName::new("parent2"),
            action: AlterTableAction::RenameColumn {
                old: "id".to_string(),
                new: "pid".to_string(),
            },
        };
        schema.alter_table(&rename_col).unwrap();

        let parent_table = schema.table("parent2").unwrap();
        assert_eq!(parent_table.columns[0].name, "pid");
        let child_table = schema.table("child").unwrap();
        assert_eq!(
            child_table.foreign_keys[0].ref_columns.as_ref().unwrap()[0],
            "pid"
        );
    }

    #[test]
    fn test_schema_alter_table_add_drop_column() {
        let mut schema = Schema::new();

        let stmt = CreateTableStmt {
            if_not_exists: false,
            name: QualifiedName::new("items"),
            definition: TableDefinition::Columns {
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                        type_name: Some("INTEGER".to_string()),
                        constraints: vec![],
                    },
                    ColumnDef {
                        name: "name".to_string(),
                        type_name: Some("TEXT".to_string()),
                        constraints: vec![],
                    },
                ],
                constraints: vec![],
            },
            without_rowid: false,
            strict: false,
        };

        schema.create_table(&stmt).unwrap();

        let add_col = AlterTableStmt {
            table: QualifiedName::new("items"),
            action: AlterTableAction::AddColumn(ColumnDef {
                name: "category".to_string(),
                type_name: Some("TEXT".to_string()),
                constraints: vec![
                    ColumnConstraint::NotNull { conflict: None },
                    ColumnConstraint::Default(DefaultValue::String("misc".to_string())),
                ],
            }),
        };
        schema.alter_table(&add_col).unwrap();

        let table = schema.table("items").unwrap();
        assert_eq!(table.columns.len(), 3);

        let drop_col = AlterTableStmt {
            table: QualifiedName::new("items"),
            action: AlterTableAction::DropColumn("name".to_string()),
        };
        schema.alter_table(&drop_col).unwrap();

        let table = schema.table("items").unwrap();
        assert_eq!(table.columns.len(), 2);
        assert!(table.column("name").is_none());
    }

    #[test]
    fn test_without_rowid_requires_pk() {
        let mut schema = Schema::new();

        let stmt = CreateTableStmt {
            if_not_exists: false,
            name: QualifiedName::new("test"),
            definition: TableDefinition::Columns {
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    type_name: Some("INTEGER".to_string()),
                    constraints: vec![],
                }],
                constraints: vec![],
            },
            without_rowid: true,
            strict: false,
        };

        let result = schema.create_table(&stmt);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message
            .as_ref()
            .unwrap()
            .contains("PRIMARY KEY missing"));
    }

    #[test]
    fn test_duplicate_column_names() {
        let mut schema = Schema::new();

        let stmt = CreateTableStmt {
            if_not_exists: false,
            name: QualifiedName::new("test"),
            definition: TableDefinition::Columns {
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                        type_name: Some("INTEGER".to_string()),
                        constraints: vec![],
                    },
                    ColumnDef {
                        name: "ID".to_string(),
                        type_name: Some("TEXT".to_string()),
                        constraints: vec![],
                    },
                ],
                constraints: vec![],
            },
            without_rowid: false,
            strict: false,
        };

        let result = schema.create_table(&stmt);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .message
            .as_ref()
            .unwrap()
            .contains("duplicate column"));
    }

    #[test]
    fn test_table_has_rowid_alias() {
        // Table with INTEGER PRIMARY KEY has rowid alias
        let table = Table {
            name: "test".to_string(),
            columns: vec![Column {
                name: "id".to_string(),
                affinity: Affinity::Integer,
                is_primary_key: true,
                ..Default::default()
            }],
            primary_key: Some(vec![0]),
            ..Default::default()
        };
        assert!(table.has_rowid_alias());

        // WITHOUT ROWID table has no alias
        let table = Table {
            without_rowid: true,
            ..table.clone()
        };
        assert!(!table.has_rowid_alias());

        // TEXT PRIMARY KEY has no alias
        let table = Table {
            name: "test".to_string(),
            columns: vec![Column {
                name: "id".to_string(),
                affinity: Affinity::Text,
                is_primary_key: true,
                ..Default::default()
            }],
            primary_key: Some(vec![0]),
            ..Default::default()
        };
        assert!(!table.has_rowid_alias());

        // Composite key has no alias
        let table = Table {
            name: "test".to_string(),
            columns: vec![
                Column {
                    name: "a".to_string(),
                    affinity: Affinity::Integer,
                    is_primary_key: true,
                    ..Default::default()
                },
                Column {
                    name: "b".to_string(),
                    affinity: Affinity::Integer,
                    is_primary_key: true,
                    ..Default::default()
                },
            ],
            primary_key: Some(vec![0, 1]),
            ..Default::default()
        };
        assert!(!table.has_rowid_alias());
    }

    #[test]
    fn test_encoding() {
        assert_eq!(Encoding::default(), Encoding::Utf8);
        assert_eq!(Encoding::Utf8 as u8, 1);
        assert_eq!(Encoding::Utf16le as u8, 2);
        assert_eq!(Encoding::Utf16be as u8, 3);
    }

    #[test]
    fn test_conflict_action() {
        assert_eq!(ConflictAction::default(), ConflictAction::Abort);
    }

    #[test]
    fn test_foreign_key_action() {
        assert_eq!(FkAction::default(), FkAction::NoAction);
    }
}
