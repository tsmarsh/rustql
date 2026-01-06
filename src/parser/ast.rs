//! Abstract Syntax Tree
//!
//! Defines the AST nodes for SQL statements and expressions.
//! These structures represent the parsed form of SQL before
//! analysis and code generation.

use std::fmt;

// ============================================================================
// Core Types
// ============================================================================

/// A qualified name (optional schema.name)
#[derive(Debug, Clone, PartialEq)]
pub struct QualifiedName {
    pub schema: Option<String>,
    pub name: String,
}

impl QualifiedName {
    pub fn new(name: impl Into<String>) -> Self {
        QualifiedName {
            schema: None,
            name: name.into(),
        }
    }

    pub fn with_schema(schema: impl Into<String>, name: impl Into<String>) -> Self {
        QualifiedName {
            schema: Some(schema.into()),
            name: name.into(),
        }
    }
}

impl fmt::Display for QualifiedName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref schema) = self.schema {
            write!(f, "{}.{}", schema, self.name)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

/// Sort order
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

/// Null ordering
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NullsOrder {
    #[default]
    Default,
    First,
    Last,
}

/// Conflict resolution action
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConflictAction {
    #[default]
    Abort,
    Rollback,
    Fail,
    Ignore,
    Replace,
}

// ============================================================================
// Statements
// ============================================================================

/// Top-level SQL statement
#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    CreateIndex(CreateIndexStmt),
    CreateView(CreateViewStmt),
    CreateTrigger(CreateTriggerStmt),
    DropTable(DropStmt),
    DropIndex(DropStmt),
    DropView(DropStmt),
    DropTrigger(DropStmt),
    AlterTable(AlterTableStmt),
    Begin(BeginStmt),
    Commit,
    Rollback(RollbackStmt),
    Savepoint(String),
    Release(String),
    Pragma(PragmaStmt),
    Vacuum(VacuumStmt),
    Analyze(Option<QualifiedName>),
    Reindex(Option<QualifiedName>),
    Attach(AttachStmt),
    Detach(String),
    Explain(Box<Stmt>),
    ExplainQueryPlan(Box<Stmt>),
}

// ============================================================================
// SELECT Statement
// ============================================================================

/// SELECT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub with: Option<WithClause>,
    pub body: SelectBody,
    pub order_by: Option<Vec<OrderingTerm>>,
    pub limit: Option<LimitClause>,
}

impl SelectStmt {
    pub fn simple(columns: Vec<ResultColumn>) -> Self {
        SelectStmt {
            with: None,
            body: SelectBody::Select(SelectCore {
                distinct: Distinct::All,
                columns,
                from: None,
                where_clause: None,
                group_by: None,
                having: None,
                window: None,
            }),
            order_by: None,
            limit: None,
        }
    }
}

/// WITH clause for CTEs
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub recursive: bool,
    pub ctes: Vec<CommonTableExpr>,
}

/// Common Table Expression
#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpr {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub materialized: Option<bool>,
    pub query: Box<SelectStmt>,
}

/// SELECT body (simple select or compound)
#[derive(Debug, Clone, PartialEq)]
pub enum SelectBody {
    Select(SelectCore),
    Compound {
        op: CompoundOp,
        left: Box<SelectBody>,
        right: Box<SelectBody>,
    },
}

/// Core SELECT without ORDER BY and LIMIT
#[derive(Debug, Clone, PartialEq)]
pub struct SelectCore {
    pub distinct: Distinct,
    pub columns: Vec<ResultColumn>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Box<Expr>>,
    pub group_by: Option<Vec<Expr>>,
    pub having: Option<Box<Expr>>,
    pub window: Option<Vec<WindowDef>>,
}

/// DISTINCT mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Distinct {
    #[default]
    All,
    Distinct,
}

/// Compound SELECT operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompoundOp {
    Union,
    UnionAll,
    Intersect,
    Except,
}

/// Result column in SELECT
#[derive(Debug, Clone, PartialEq)]
pub enum ResultColumn {
    /// All columns (*)
    Star,
    /// Table.* (table.*)
    TableStar(String),
    /// Expression with optional alias
    Expr { expr: Expr, alias: Option<String> },
}

/// FROM clause
#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub tables: Vec<TableRef>,
}

/// Table reference in FROM clause
#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    /// Simple table reference
    Table {
        name: QualifiedName,
        alias: Option<String>,
        indexed_by: Option<IndexedBy>,
    },
    /// Subquery
    Subquery {
        query: Box<SelectStmt>,
        alias: Option<String>,
    },
    /// JOIN
    Join {
        left: Box<TableRef>,
        join_type: JoinType,
        right: Box<TableRef>,
        constraint: Option<JoinConstraint>,
    },
    /// Table-valued function
    TableFunction {
        name: String,
        args: Vec<Expr>,
        alias: Option<String>,
    },
    /// Parenthesized table reference
    Parens(Box<TableRef>),
}

/// INDEXED BY clause
#[derive(Debug, Clone, PartialEq)]
pub enum IndexedBy {
    /// INDEXED BY index_name
    Index(String),
    /// NOT INDEXED
    NotIndexed,
}

/// JOIN type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JoinType {
    #[default]
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Natural,
    NaturalLeft,
    NaturalRight,
    NaturalFull,
}

/// JOIN constraint
#[derive(Debug, Clone, PartialEq)]
pub enum JoinConstraint {
    On(Box<Expr>),
    Using(Vec<String>),
}

/// ORDER BY term
#[derive(Debug, Clone, PartialEq)]
pub struct OrderingTerm {
    pub expr: Expr,
    pub order: SortOrder,
    pub nulls: NullsOrder,
}

/// LIMIT clause
#[derive(Debug, Clone, PartialEq)]
pub struct LimitClause {
    pub limit: Box<Expr>,
    pub offset: Option<Box<Expr>>,
}

/// Window definition
#[derive(Debug, Clone, PartialEq)]
pub struct WindowDef {
    pub name: String,
    pub spec: WindowSpec,
}

/// Window specification
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    pub base: Option<String>,
    pub partition_by: Option<Vec<Expr>>,
    pub order_by: Option<Vec<OrderingTerm>>,
    pub frame: Option<WindowFrame>,
}

/// Window frame
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub mode: WindowFrameMode,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
    pub exclude: WindowFrameExclude,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WindowFrameMode {
    #[default]
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    CurrentRow,
    UnboundedPreceding,
    UnboundedFollowing,
    Preceding(Box<Expr>),
    Following(Box<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WindowFrameExclude {
    #[default]
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

// ============================================================================
// INSERT Statement
// ============================================================================

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub with: Option<WithClause>,
    pub or_action: Option<ConflictAction>,
    pub table: QualifiedName,
    pub alias: Option<String>,
    pub columns: Option<Vec<String>>,
    pub source: InsertSource,
    pub on_conflict: Option<OnConflict>,
    pub returning: Option<Vec<ResultColumn>>,
}

/// INSERT source
#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Select(Box<SelectStmt>),
    DefaultValues,
}

/// ON CONFLICT clause
#[derive(Debug, Clone, PartialEq)]
pub struct OnConflict {
    pub target: Option<ConflictTarget>,
    pub action: ConflictResolution,
}

/// Conflict target
#[derive(Debug, Clone, PartialEq)]
pub struct ConflictTarget {
    pub columns: Vec<IndexedColumn>,
    pub where_clause: Option<Box<Expr>>,
}

/// Conflict resolution
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictResolution {
    Nothing,
    Update {
        assignments: Vec<Assignment>,
        where_clause: Option<Box<Expr>>,
    },
}

/// Assignment (column = expr)
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub columns: Vec<String>,
    pub expr: Expr,
}

// ============================================================================
// UPDATE Statement
// ============================================================================

/// UPDATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
    pub with: Option<WithClause>,
    pub or_action: Option<ConflictAction>,
    pub table: QualifiedName,
    pub alias: Option<String>,
    pub indexed_by: Option<IndexedBy>,
    pub assignments: Vec<Assignment>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Box<Expr>>,
    pub returning: Option<Vec<ResultColumn>>,
    pub order_by: Option<Vec<OrderingTerm>>,
    pub limit: Option<LimitClause>,
}

// ============================================================================
// DELETE Statement
// ============================================================================

/// DELETE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    pub with: Option<WithClause>,
    pub table: QualifiedName,
    pub alias: Option<String>,
    pub indexed_by: Option<IndexedBy>,
    pub where_clause: Option<Box<Expr>>,
    pub returning: Option<Vec<ResultColumn>>,
    pub order_by: Option<Vec<OrderingTerm>>,
    pub limit: Option<LimitClause>,
}

// ============================================================================
// CREATE TABLE Statement
// ============================================================================

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    pub temporary: bool,
    pub if_not_exists: bool,
    pub name: QualifiedName,
    pub definition: TableDefinition,
    pub without_rowid: bool,
    pub strict: bool,
}

/// Table definition
#[derive(Debug, Clone, PartialEq)]
pub enum TableDefinition {
    Columns {
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    },
    AsSelect(Box<SelectStmt>),
}

/// Column definition
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub type_name: Option<TypeName>,
    pub constraints: Vec<ColumnConstraint>,
}

/// Type name
#[derive(Debug, Clone, PartialEq)]
pub struct TypeName {
    pub name: String,
    pub args: Vec<i64>,
}

impl TypeName {
    pub fn new(name: impl Into<String>) -> Self {
        TypeName {
            name: name.into(),
            args: Vec::new(),
        }
    }

    pub fn with_args(name: impl Into<String>, args: Vec<i64>) -> Self {
        TypeName {
            name: name.into(),
            args,
        }
    }
}

/// Column constraint
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnConstraint {
    pub name: Option<String>,
    pub kind: ColumnConstraintKind,
}

/// Column constraint kind
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraintKind {
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
    Check(Box<Expr>),
    Default(DefaultValue),
    Collate(String),
    ForeignKey(ForeignKeyClause),
    Generated {
        expr: Box<Expr>,
        storage: GeneratedStorage,
    },
}

/// Default value
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultValue {
    Expr(Box<Expr>),
    Literal(Literal),
    CurrentTime,
    CurrentDate,
    CurrentTimestamp,
}

/// Generated column storage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GeneratedStorage {
    #[default]
    Virtual,
    Stored,
}

/// Foreign key clause
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKeyClause {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub on_delete: Option<ForeignKeyAction>,
    pub on_update: Option<ForeignKeyAction>,
    pub match_type: Option<String>,
    pub deferrable: Option<Deferrable>,
}

/// Foreign key action
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignKeyAction {
    SetNull,
    SetDefault,
    Cascade,
    Restrict,
    NoAction,
}

/// Deferrable constraint
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deferrable {
    pub not: bool,
    pub initially: Option<DeferrableInitially>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferrableInitially {
    Deferred,
    Immediate,
}

/// Table constraint
#[derive(Debug, Clone, PartialEq)]
pub struct TableConstraint {
    pub name: Option<String>,
    pub kind: TableConstraintKind,
}

/// Table constraint kind
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraintKind {
    PrimaryKey {
        columns: Vec<IndexedColumn>,
        conflict: Option<ConflictAction>,
    },
    Unique {
        columns: Vec<IndexedColumn>,
        conflict: Option<ConflictAction>,
    },
    Check(Box<Expr>),
    ForeignKey {
        columns: Vec<String>,
        clause: ForeignKeyClause,
    },
}

/// Indexed column
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedColumn {
    pub column: IndexedColumnKind,
    pub collation: Option<String>,
    pub order: Option<SortOrder>,
}

/// Indexed column kind
#[derive(Debug, Clone, PartialEq)]
pub enum IndexedColumnKind {
    Name(String),
    Expr(Box<Expr>),
}

// ============================================================================
// CREATE INDEX Statement
// ============================================================================

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStmt {
    pub unique: bool,
    pub if_not_exists: bool,
    pub name: QualifiedName,
    pub table: String,
    pub columns: Vec<IndexedColumn>,
    pub where_clause: Option<Box<Expr>>,
}

// ============================================================================
// CREATE VIEW Statement
// ============================================================================

/// CREATE VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStmt {
    pub temporary: bool,
    pub if_not_exists: bool,
    pub name: QualifiedName,
    pub columns: Option<Vec<String>>,
    pub query: Box<SelectStmt>,
}

// ============================================================================
// CREATE TRIGGER Statement
// ============================================================================

/// CREATE TRIGGER statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTriggerStmt {
    pub temporary: bool,
    pub if_not_exists: bool,
    pub name: QualifiedName,
    pub time: TriggerTime,
    pub event: TriggerEvent,
    pub table: String,
    pub for_each_row: bool,
    pub when: Option<Box<Expr>>,
    pub body: Vec<Stmt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerTime {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TriggerEvent {
    Delete,
    Insert,
    Update(Option<Vec<String>>),
}

// ============================================================================
// DROP Statement
// ============================================================================

/// DROP statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropStmt {
    pub if_exists: bool,
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
// Transaction Statements
// ============================================================================

/// BEGIN statement
#[derive(Debug, Clone, PartialEq)]
pub struct BeginStmt {
    pub mode: Option<TransactionMode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    Deferred,
    Immediate,
    Exclusive,
}

/// ROLLBACK statement
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackStmt {
    pub savepoint: Option<String>,
}

// ============================================================================
// PRAGMA Statement
// ============================================================================

/// PRAGMA statement
#[derive(Debug, Clone, PartialEq)]
pub struct PragmaStmt {
    pub schema: Option<String>,
    pub name: String,
    pub value: Option<PragmaValue>,
}

/// PRAGMA value
#[derive(Debug, Clone, PartialEq)]
pub enum PragmaValue {
    Set(Expr),
    Call(Expr),
}

// ============================================================================
// VACUUM Statement
// ============================================================================

/// VACUUM statement
#[derive(Debug, Clone, PartialEq)]
pub struct VacuumStmt {
    pub schema: Option<String>,
    pub into: Option<String>,
}

// ============================================================================
// ATTACH/DETACH Statements
// ============================================================================

/// ATTACH statement
#[derive(Debug, Clone, PartialEq)]
pub struct AttachStmt {
    pub expr: Expr,
    pub schema: String,
}

// ============================================================================
// Expressions
// ============================================================================

/// Expression
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    // Literals
    Literal(Literal),

    // Column reference
    Column(ColumnRef),

    // Parameter/variable
    Variable(Variable),

    // Unary operators
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },

    // Binary operators
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },

    // BETWEEN
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },

    // IN
    In {
        expr: Box<Expr>,
        list: InList,
        negated: bool,
    },

    // LIKE/GLOB/REGEXP
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape: Option<Box<Expr>>,
        op: LikeOp,
        negated: bool,
    },

    // IS NULL / IS NOT NULL
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },

    // IS DISTINCT FROM
    IsDistinct {
        left: Box<Expr>,
        right: Box<Expr>,
        negated: bool,
    },

    // CASE expression
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<WhenClause>,
        else_clause: Option<Box<Expr>>,
    },

    // CAST expression
    Cast {
        expr: Box<Expr>,
        type_name: TypeName,
    },

    // COLLATE
    Collate {
        expr: Box<Expr>,
        collation: String,
    },

    // Function call
    Function(FunctionCall),

    // Subquery
    Subquery(Box<SelectStmt>),

    // EXISTS
    Exists {
        subquery: Box<SelectStmt>,
        negated: bool,
    },

    // Parenthesized expression
    Parens(Box<Expr>),

    // RAISE function
    Raise {
        action: RaiseAction,
        message: Option<String>,
    },
}

impl Expr {
    pub fn int(value: i64) -> Self {
        Expr::Literal(Literal::Integer(value))
    }

    pub fn float(value: f64) -> Self {
        Expr::Literal(Literal::Float(value))
    }

    pub fn string(value: impl Into<String>) -> Self {
        Expr::Literal(Literal::String(value.into()))
    }

    pub fn null() -> Self {
        Expr::Literal(Literal::Null)
    }

    pub fn column(name: impl Into<String>) -> Self {
        Expr::Column(ColumnRef {
            database: None,
            table: None,
            column: name.into(),
        })
    }
}

/// Literal value
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Integer(i64),
    Float(f64),
    String(String),
    Blob(Vec<u8>),
    Bool(bool),
    CurrentTime,
    CurrentDate,
    CurrentTimestamp,
}

/// Column reference
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    pub database: Option<String>,
    pub table: Option<String>,
    pub column: String,
}

impl ColumnRef {
    pub fn new(column: impl Into<String>) -> Self {
        ColumnRef {
            database: None,
            table: None,
            column: column.into(),
        }
    }

    pub fn with_table(table: impl Into<String>, column: impl Into<String>) -> Self {
        ColumnRef {
            database: None,
            table: Some(table.into()),
            column: column.into(),
        }
    }
}

/// Parameter variable
#[derive(Debug, Clone, PartialEq)]
pub enum Variable {
    /// Numbered parameter (?NNN or ?)
    Numbered(Option<i32>),
    /// Named parameter (:name, @name, $name)
    Named { prefix: char, name: String },
}

/// Unary operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,    // -
    Pos,    // +
    Not,    // NOT
    BitNot, // ~
}

/// Binary operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,

    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Is,
    IsNot,

    // Logical
    And,
    Or,

    // Bitwise
    BitAnd,
    BitOr,
    ShiftLeft,
    ShiftRight,

    // String
    Concat,
}

impl BinaryOp {
    /// Get the precedence of this operator (higher = tighter binding)
    pub fn precedence(&self) -> u8 {
        match self {
            BinaryOp::Or => 1,
            BinaryOp::And => 2,
            BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::Gt
            | BinaryOp::Ge
            | BinaryOp::Is
            | BinaryOp::IsNot => 3,
            BinaryOp::BitOr => 4,
            BinaryOp::BitAnd => 5,
            BinaryOp::ShiftLeft | BinaryOp::ShiftRight => 6,
            BinaryOp::Add | BinaryOp::Sub => 7,
            BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => 8,
            BinaryOp::Concat => 9,
        }
    }
}

/// IN list
#[derive(Debug, Clone, PartialEq)]
pub enum InList {
    Values(Vec<Expr>),
    Subquery(Box<SelectStmt>),
    Table(QualifiedName),
}

/// LIKE operator type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LikeOp {
    Like,
    Glob,
    Regexp,
    Match,
}

/// WHEN clause in CASE
#[derive(Debug, Clone, PartialEq)]
pub struct WhenClause {
    pub when: Box<Expr>,
    pub then: Box<Expr>,
}

/// Function call
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    pub args: FunctionArgs,
    pub distinct: bool,
    pub filter: Option<Box<Expr>>,
    pub over: Option<Over>,
}

/// Function arguments
#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArgs {
    Star,
    Exprs(Vec<Expr>),
}

/// OVER clause
#[derive(Debug, Clone, PartialEq)]
pub enum Over {
    Window(String),
    Spec(WindowSpec),
}

/// RAISE action
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaiseAction {
    Ignore,
    Rollback,
    Abort,
    Fail,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qualified_name() {
        let name = QualifiedName::new("users");
        assert_eq!(name.to_string(), "users");

        let name = QualifiedName::with_schema("main", "users");
        assert_eq!(name.to_string(), "main.users");
    }

    #[test]
    fn test_expr_helpers() {
        let e = Expr::int(42);
        assert!(matches!(e, Expr::Literal(Literal::Integer(42))));

        let e = Expr::string("hello");
        assert!(matches!(e, Expr::Literal(Literal::String(_))));

        let e = Expr::column("id");
        assert!(matches!(e, Expr::Column(ColumnRef { column, .. }) if column == "id"));
    }

    #[test]
    fn test_binary_op_precedence() {
        assert!(BinaryOp::Mul.precedence() > BinaryOp::Add.precedence());
        assert!(BinaryOp::And.precedence() > BinaryOp::Or.precedence());
        assert!(BinaryOp::Eq.precedence() > BinaryOp::And.precedence());
    }

    #[test]
    fn test_select_stmt_simple() {
        let stmt = SelectStmt::simple(vec![ResultColumn::Star]);
        assert!(matches!(stmt.body, SelectBody::Select(_)));
    }
}
