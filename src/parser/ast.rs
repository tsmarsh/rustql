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
    /// Original name with quotes preserved (for error messages)
    pub original_name: Option<String>,
}

impl QualifiedName {
    pub fn new(name: impl Into<String>) -> Self {
        QualifiedName {
            schema: None,
            name: name.into(),
            original_name: None,
        }
    }

    pub fn with_schema(schema: impl Into<String>, name: impl Into<String>) -> Self {
        QualifiedName {
            schema: Some(schema.into()),
            name: name.into(),
            original_name: None,
        }
    }

    /// Create with original name preserved for error messages
    pub fn with_original(name: impl Into<String>, original: impl Into<String>) -> Self {
        QualifiedName {
            schema: None,
            name: name.into(),
            original_name: Some(original.into()),
        }
    }

    /// Get the display name (original if available, otherwise name)
    pub fn display_name(&self) -> &str {
        self.original_name.as_deref().unwrap_or(&self.name)
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
    CreateVirtualTable(CreateVirtualTableStmt),
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

impl SelectBody {
    /// Get the leftmost SelectCore (used for column names in compound queries)
    /// In SQL, column names/types come from the first SELECT in a UNION
    pub fn leftmost_core(&self) -> &SelectCore {
        match self {
            SelectBody::Select(core) => core,
            SelectBody::Compound { left, .. } => left.leftmost_core(),
        }
    }

    /// Get a mutable reference to the leftmost SelectCore
    pub fn leftmost_core_mut(&mut self) -> &mut SelectCore {
        match self {
            SelectBody::Select(core) => core,
            SelectBody::Compound { left, .. } => left.leftmost_core_mut(),
        }
    }

    /// Collect all SelectCores in this body (for validation, analysis)
    pub fn all_cores(&self) -> Vec<&SelectCore> {
        let mut cores = Vec::new();
        self.collect_cores(&mut cores);
        cores
    }

    fn collect_cores<'a>(&'a self, cores: &mut Vec<&'a SelectCore>) {
        match self {
            SelectBody::Select(core) => cores.push(core),
            SelectBody::Compound { left, right, .. } => {
                left.collect_cores(cores);
                right.collect_cores(cores);
            }
        }
    }

    /// Apply a function to each SelectCore
    pub fn for_each_core<F>(&self, mut f: F)
    where
        F: FnMut(&SelectCore),
    {
        match self {
            SelectBody::Select(core) => f(core),
            SelectBody::Compound { left, right, .. } => {
                left.for_each_core(&mut f);
                right.for_each_core(f);
            }
        }
    }

    /// Apply a fallible function to each SelectCore
    pub fn try_for_each_core<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&SelectCore) -> Result<(), E>,
    {
        match self {
            SelectBody::Select(core) => f(core),
            SelectBody::Compound { left, right, .. } => {
                left.try_for_each_core(&mut f)?;
                right.try_for_each_core(f)
            }
        }
    }

    /// Check if this is a compound query
    pub fn is_compound(&self) -> bool {
        matches!(self, SelectBody::Compound { .. })
    }

    /// Get the number of result columns (from leftmost core)
    pub fn column_count(&self) -> usize {
        self.leftmost_core().columns.len()
    }
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

// ============================================================================
// FROM clause - matches SQLite's SrcList/SrcItem structure
// ============================================================================

/// FROM clause as a flat list of source items (like SQLite's SrcList)
///
/// SQLite represents `FROM t1 JOIN t2 ON x JOIN t3 USING(a)` as a flat array:
/// ```text
/// items[0] = { source: t1, join_type: 0 }
/// items[1] = { source: t2, join_type: INNER, on_clause: x }
/// items[2] = { source: t3, join_type: INNER, using_columns: [a] }
/// ```
///
/// The join_type on each item describes the join with the *previous* item.
/// The first item's join_type is always empty (no previous item to join with).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SrcList {
    pub items: Vec<SrcItem>,
}

/// A single source item in FROM clause (like SQLite's SrcItem)
#[derive(Debug, Clone, PartialEq)]
pub struct SrcItem {
    /// The source (table, subquery, or table function)
    pub source: TableSource,
    /// Table alias (the "B" in "A AS B")
    pub alias: Option<String>,
    /// Join type flags for joining with the *previous* item in the list.
    /// For the first item, this should be JoinFlags::empty().
    pub join_type: JoinFlags,
    /// ON clause - mutually exclusive with using_columns
    pub on_clause: Option<Box<Expr>>,
    /// USING columns - mutually exclusive with on_clause
    pub using_columns: Option<Vec<String>>,
    /// INDEXED BY / NOT INDEXED clause
    pub indexed_by: Option<IndexedBy>,
}

impl SrcItem {
    /// Create a simple table source item (first item in FROM, no join)
    pub fn table(name: QualifiedName) -> Self {
        SrcItem {
            source: TableSource::Table(name),
            alias: None,
            join_type: JoinFlags::empty(),
            on_clause: None,
            using_columns: None,
            indexed_by: None,
        }
    }

    /// Create a table source with alias
    pub fn table_with_alias(name: QualifiedName, alias: String) -> Self {
        SrcItem {
            source: TableSource::Table(name),
            alias: Some(alias),
            join_type: JoinFlags::empty(),
            on_clause: None,
            using_columns: None,
            indexed_by: None,
        }
    }
}

/// The actual source of data in a FROM item
#[derive(Debug, Clone, PartialEq)]
pub enum TableSource {
    /// Simple table reference
    Table(QualifiedName),
    /// Subquery (SELECT ...)
    Subquery(Box<SelectStmt>),
    /// Table-valued function
    TableFunction { name: String, args: Vec<Expr> },
}

bitflags::bitflags! {
    /// Join type flags - matches SQLite's JT_* flags from sqliteInt.h
    ///
    /// These flags can be combined. For example:
    /// - `LEFT | OUTER` = LEFT OUTER JOIN
    /// - `NATURAL | INNER` = NATURAL JOIN
    /// - `NATURAL | LEFT | OUTER` = NATURAL LEFT OUTER JOIN
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct JoinFlags: u8 {
        /// Any kind of inner or cross join (JT_INNER)
        const INNER   = 0x01;
        /// Explicit use of the CROSS keyword (JT_CROSS)
        const CROSS   = 0x02;
        /// True for a "natural" join (JT_NATURAL)
        const NATURAL = 0x04;
        /// Left outer join (JT_LEFT)
        const LEFT    = 0x08;
        /// Right outer join (JT_RIGHT)
        const RIGHT   = 0x10;
        /// The "OUTER" keyword is present (JT_OUTER)
        const OUTER   = 0x20;
    }
}

impl JoinFlags {
    /// Check if this is an outer join (LEFT, RIGHT, or FULL)
    pub fn is_outer(&self) -> bool {
        self.intersects(JoinFlags::LEFT | JoinFlags::RIGHT)
    }

    /// Check if this is a NATURAL join
    pub fn is_natural(&self) -> bool {
        self.contains(JoinFlags::NATURAL)
    }
}

/// INDEXED BY clause
#[derive(Debug, Clone, PartialEq)]
pub enum IndexedBy {
    /// INDEXED BY index_name
    Index(String),
    /// NOT INDEXED
    NotIndexed,
}

// ============================================================================
// Legacy FROM clause types - to be removed after migration
// ============================================================================

/// Legacy FROM clause - wrapper around TableRef tree
/// Will be converted to SrcList before code generation
#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub tables: Vec<TableRef>,
}

impl FromClause {
    /// Convert legacy FROM clause to flat SrcList (SQLite model)
    pub fn to_src_list(&self) -> SrcList {
        let mut items = Vec::new();
        for table_ref in &self.tables {
            flatten_table_ref(table_ref, &mut items, JoinFlags::empty());
        }
        SrcList { items }
    }
}

/// Flatten a TableRef tree into a flat list of SrcItems
fn flatten_table_ref(table_ref: &TableRef, items: &mut Vec<SrcItem>, join_type: JoinFlags) {
    match table_ref {
        TableRef::Table {
            name,
            alias,
            indexed_by,
        } => {
            items.push(SrcItem {
                source: TableSource::Table(name.clone()),
                alias: alias.clone(),
                join_type,
                on_clause: None,
                using_columns: None,
                indexed_by: indexed_by.clone(),
            });
        }
        TableRef::Subquery { query, alias } => {
            items.push(SrcItem {
                source: TableSource::Subquery(query.clone()),
                alias: alias.clone(),
                join_type,
                on_clause: None,
                using_columns: None,
                indexed_by: None,
            });
        }
        TableRef::Join {
            left,
            join_type: jt,
            right,
            constraint,
        } => {
            // Transform RIGHT JOIN into LEFT JOIN by swapping left/right tables.
            // A RIGHT JOIN B ON cond  ===  B LEFT JOIN A ON cond
            // This avoids needing separate right-join null-fill logic in the executor.
            // FULL JOIN (LEFT|RIGHT) is left as-is for now.
            let is_right_only = jt.contains(JoinFlags::RIGHT) && !jt.contains(JoinFlags::LEFT);
            let (actual_left, actual_right, actual_jt) = if is_right_only {
                // Swap: remove RIGHT, add LEFT, keep NATURAL/OUTER/CROSS/INNER as appropriate
                let mut new_flags = *jt;
                new_flags.remove(JoinFlags::RIGHT);
                new_flags.insert(JoinFlags::LEFT);
                (right.as_ref(), left.as_ref(), new_flags)
            } else {
                (left.as_ref(), right.as_ref(), *jt)
            };

            // Flatten left side first (with inherited join_type for first item)
            flatten_table_ref(actual_left, items, join_type);

            // Right side gets the actual join type and constraint
            let (on_clause, using_columns) = match constraint {
                Some(JoinConstraint::On(expr)) => (Some(expr.clone()), None),
                Some(JoinConstraint::Using(cols)) => (None, Some(cols.clone())),
                None => (None, None),
            };

            // Flatten right side with the join info
            match actual_right {
                TableRef::Table {
                    name,
                    alias,
                    indexed_by,
                } => {
                    items.push(SrcItem {
                        source: TableSource::Table(name.clone()),
                        alias: alias.clone(),
                        join_type: actual_jt,
                        on_clause,
                        using_columns,
                        indexed_by: indexed_by.clone(),
                            });
                }
                TableRef::Subquery { query, alias } => {
                    items.push(SrcItem {
                        source: TableSource::Subquery(query.clone()),
                        alias: alias.clone(),
                        join_type: actual_jt,
                        on_clause,
                        using_columns,
                        indexed_by: None,
                            });
                }
                TableRef::TableFunction { name, args, alias } => {
                    items.push(SrcItem {
                        source: TableSource::TableFunction {
                            name: name.clone(),
                            args: args.clone(),
                        },
                        alias: alias.clone(),
                        join_type: actual_jt,
                        on_clause,
                        using_columns,
                        indexed_by: None,
                            });
                }
                // For nested joins on the right side of an OUTER JOIN, we must
                // convert the entire parenthesized group into a subquery so the
                // inner join is evaluated first and the whole result is null-filled
                // as a unit.  E.g. `t1 LEFT JOIN (t2 JOIN t3)` must null-fill
                // both t2 and t3 together when there's no match, not independently.
                //
                // For INNER joins the flat model works fine, so just flatten.
                TableRef::Join { .. } | TableRef::Parens(_) => {
                    if actual_jt.contains(JoinFlags::LEFT) {
                        // Convert the parenthesized join to a subquery:
                        //   SELECT * FROM <inner_join>
                        let subquery = Box::new(SelectStmt {
                            with: None,
                            body: SelectBody::Select(SelectCore {
                                distinct: Distinct::All,
                                columns: vec![ResultColumn::Star],
                                from: Some(FromClause {
                                    tables: vec![actual_right.clone()],
                                }),
                                where_clause: None,
                                group_by: None,
                                having: None,
                                window: None,
                            }),
                            order_by: None,
                            limit: None,
                        });
                        items.push(SrcItem {
                            source: TableSource::Subquery(subquery),
                            alias: None,
                            join_type: actual_jt,
                            on_clause,
                            using_columns,
                            indexed_by: None,
                                    });
                    } else {
                        let start_idx = items.len();
                        flatten_table_ref(actual_right, items, actual_jt);
                        // Apply constraint to first item added from right side
                        if start_idx < items.len() {
                            items[start_idx].on_clause = on_clause;
                            items[start_idx].using_columns = using_columns;
                        }
                    }
                }
            }
        }
        TableRef::TableFunction { name, args, alias } => {
            items.push(SrcItem {
                source: TableSource::TableFunction {
                    name: name.clone(),
                    args: args.clone(),
                },
                alias: alias.clone(),
                join_type,
                on_clause: None,
                using_columns: None,
                indexed_by: None,
            });
        }
        TableRef::Parens(inner) => {
            flatten_table_ref(inner, items, join_type);
        }
    }
}

/// Legacy JOIN constraint enum
#[derive(Debug, Clone, PartialEq)]
pub enum JoinConstraint {
    On(Box<Expr>),
    Using(Vec<String>),
}

/// Legacy table reference enum (tree structure)
/// Parser produces this, then it's flattened to SrcList
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
    /// JOIN - will be flattened into SrcList
    Join {
        left: Box<TableRef>,
        join_type: JoinFlags,
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

// Type alias for gradual migration - parser still uses JoinType
pub type JoinType = JoinFlags;

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

/// CREATE VIRTUAL TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateVirtualTableStmt {
    pub if_not_exists: bool,
    pub name: QualifiedName,
    pub module: String,
    pub args: Vec<String>,
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
    /// Explicit NULL constraint (allows NULL values - this is the default)
    Null,
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
    /// Table name (without schema prefix - schema is in table_schema)
    pub table: String,
    /// Optional schema prefix for the table (e.g., "orig" in "ON orig.t1")
    pub table_schema: Option<String>,
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
    /// Optional transaction name/label (SQLite allows: BEGIN TRANSACTION 'name')
    pub name: Option<String>,
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

    // Row value (vector) - e.g., (a, b, c)
    Vector(Vec<Expr>),

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
            column_index: None,
            source_text: None,
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
    pub column_index: Option<i32>,
    /// Original source text for this column reference (used when both
    /// short_column_names and full_column_names are OFF to preserve spacing)
    pub source_text: Option<String>,
}

impl ColumnRef {
    pub fn new(column: impl Into<String>) -> Self {
        ColumnRef {
            database: None,
            table: None,
            column: column.into(),
            column_index: None,
            source_text: None,
        }
    }

    pub fn with_table(table: impl Into<String>, column: impl Into<String>) -> Self {
        ColumnRef {
            database: None,
            table: Some(table.into()),
            column: column.into(),
            column_index: None,
            source_text: None,
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

    // JSON operators
    JsonExtract,     // ->
    JsonExtractText, // ->>
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
            BinaryOp::JsonExtract | BinaryOp::JsonExtractText => 10,
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
