//! WHERE clause analysis and optimization
//!
//! This module implements SQLite's query planner which analyzes WHERE clauses
//! and chooses optimal join orders and index usage. Corresponds to where.c.

use bitflags::bitflags;

use crate::error::{Error, ErrorCode, Result};
use crate::parser::ast::{BinaryOp, Expr, LikeOp, Literal, UnaryOp};
use crate::schema::IndexStats;

use super::where_expr;

// ============================================================================
// Constants
// ============================================================================

/// Maximum number of tables in a join
const MAX_TABLES: usize = 64;

/// Cost of reading a single row
const ROW_READ_COST: f64 = 1.0;

/// Cost of seeking in an index
const INDEX_SEEK_COST: f64 = 10.0;

/// Cost of a full table scan
const FULL_SCAN_COST_MULT: f64 = 3.0;

// ============================================================================
// WhereTermFlags
// ============================================================================

bitflags! {
    /// Flags describing properties of a WHERE term
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct WhereTermFlags: u32 {
        /// Term is dynamically allocated
        const DYNAMIC = 0x0001;
        /// Virtual term, don't code
        const VIRTUAL = 0x0002;
        /// Part of a compound OR clause
        const OR_INFO = 0x0004;
        /// Use as index scan constraint
        const INDEX_CONSTRAINT = 0x0008;
        /// Column is on left side of comparison
        const LEFT_COLUMN = 0x0010;
        /// Term is a reference to another
        const EQUIV = 0x0020;
        /// Term has been coded
        const CODED = 0x0040;
        /// Term from a BETWEEN expr
        const BETWEEN = 0x0080;
        /// Term originally had LIKE
        const LIKE = 0x0100;
        /// IS NOT NULL term
        const IS_NOT_NULL = 0x0200;
        /// AND-connected terms
        const AND = 0x0400;
        /// Term references outer query
        const OUTER_REF = 0x0800;
        /// LIKE term has a usable prefix
        const LIKE_PREFIX = 0x1000;
    }
}

// ============================================================================
// WhereLevelFlags
// ============================================================================

bitflags! {
    /// Flags for a WhereLevel
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct WhereLevelFlags: u32 {
        /// Use an index
        const INDEXED = 0x0001;
        /// IN constraint present
        const IN_LOOP = 0x0002;
        /// Virtual table
        const VIRTUAL = 0x0004;
        /// Use temp B-Tree
        const TEMP_BTREE = 0x0008;
        /// Or-connected terms
        const OR_SUBSET = 0x0010;
        /// Use index only (covering)
        const IDX_ONLY = 0x0020;
        /// Multi-value IN
        const MULTI_OR = 0x0040;
        /// One-pass algorithm
        const ONE_PASS = 0x0080;
        /// Unique index lookup
        const UNIQUE = 0x0100;
        /// Order by satisfied
        const ORDER_BY = 0x0200;
    }
}

// ============================================================================
// WherePlan
// ============================================================================

/// Access plan for a single table in a query
#[derive(Debug, Clone, Default)]
pub enum WherePlan {
    /// Full table scan
    #[default]
    FullScan,

    /// Use an index for scanning
    IndexScan {
        /// The index to use
        index_name: String,
        /// Number of equality constraints
        eq_cols: i32,
        /// Is it a covering index?
        covering: bool,
        /// Has range constraint after equality columns (for BETWEEN, <, >, etc.)
        has_range: bool,
        /// Range termination info: (column_idx, operator, term_idx) for early scan termination
        /// Only set for upper-bound constraints (Lt, Le) that can terminate the scan
        range_end: Option<(i32, TermOp, i32)>,
    },

    /// Use primary key/rowid lookup
    PrimaryKey {
        /// Number of equality columns
        eq_cols: i32,
    },

    /// Rowid equality (WHERE rowid = ?)
    RowidEq,

    /// Rowid range scan
    RowidRange {
        /// Start constraint (>=, >)
        has_start: bool,
        /// End constraint (<=, <)
        has_end: bool,
    },
}

// ============================================================================
// WhereTerm
// ============================================================================

/// A single term in a WHERE clause
#[derive(Debug, Clone)]
pub struct WhereTerm {
    /// The expression for this term
    pub expr: Box<Expr>,

    /// Bitmask of tables used by this term's prerequisites
    pub prereq: u64,

    /// Bitmask of tables used by this term itself
    pub mask: u64,

    /// Index in the WHERE clause
    pub idx: i32,

    /// Flags describing the term
    pub flags: WhereTermFlags,

    /// Left column index (for equality/range)
    pub left_col: Option<(i32, i32)>, // (table_idx, column_idx)

    /// Selectivity estimate (0.0-1.0)
    pub selectivity: f64,

    /// Operator type
    pub op: Option<TermOp>,

    /// OR clause components (if this term is an OR expression)
    pub or_terms: Vec<Expr>,
}

/// Operator type for a WHERE term
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermOp {
    Eq,        // =
    Lt,        // <
    Le,        // <=
    Gt,        // >
    Ge,        // >=
    Ne,        // != or <>
    Is,        // IS
    Like,      // LIKE
    Glob,      // GLOB
    In,        // IN
    IsNull,    // IS NULL
    IsNotNull, // IS NOT NULL
    Between,   // BETWEEN
}

impl WhereTerm {
    /// Create a new WHERE term from an expression
    pub fn new(expr: Expr, idx: i32) -> Self {
        WhereTerm {
            expr: Box::new(expr),
            prereq: 0,
            mask: 0,
            idx,
            flags: WhereTermFlags::empty(),
            left_col: None,
            selectivity: 0.25, // Default 25% selectivity
            op: None,
            or_terms: Vec::new(),
        }
    }

    /// Check if this term can be used as an index constraint
    pub fn is_index_usable(&self) -> bool {
        matches!(
            self.op,
            Some(TermOp::Eq)
                | Some(TermOp::Is)
                | Some(TermOp::Lt)
                | Some(TermOp::Le)
                | Some(TermOp::Gt)
                | Some(TermOp::Ge)
                | Some(TermOp::In)
        )
    }

    /// Check if this is an equality term
    pub fn is_equality(&self) -> bool {
        matches!(self.op, Some(TermOp::Eq) | Some(TermOp::Is))
    }

    /// Check if this is a range term
    pub fn is_range(&self) -> bool {
        matches!(
            self.op,
            Some(TermOp::Lt) | Some(TermOp::Le) | Some(TermOp::Gt) | Some(TermOp::Ge)
        )
    }
}

// ============================================================================
// WhereLevel
// ============================================================================

/// Information about one level of a nested loop join
#[derive(Debug, Clone)]
pub struct WhereLevel {
    /// Index into the FROM clause
    pub from_idx: i32,

    /// Table name
    pub table_name: String,

    /// Flags
    pub flags: WhereLevelFlags,

    /// The access plan for this level
    pub plan: WherePlan,

    /// Terms used at this level
    pub used_terms: Vec<i32>,

    /// Address of loop start in VDBE
    pub addr_first: i32,

    /// Address of loop continuation
    pub addr_cont: i32,

    /// Address of loop end
    pub addr_brk: i32,

    /// Estimated rows output
    pub rows_out: f64,

    /// Cost of this level
    pub cost: f64,
}

impl WhereLevel {
    /// Create a new WhereLevel for a table
    pub fn new(from_idx: i32, table_name: String) -> Self {
        WhereLevel {
            from_idx,
            table_name,
            flags: WhereLevelFlags::empty(),
            plan: WherePlan::FullScan,
            used_terms: Vec::new(),
            addr_first: 0,
            addr_cont: 0,
            addr_brk: 0,
            rows_out: 0.0,
            cost: 0.0,
        }
    }
}

// ============================================================================
// WhereInfo
// ============================================================================

/// Complete WHERE clause analysis result
#[derive(Debug)]
pub struct WhereInfo {
    /// Analyzed WHERE terms
    pub terms: Vec<WhereTerm>,

    /// Nested loop levels (one per table in FROM)
    pub levels: Vec<WhereLevel>,

    /// Total estimated rows output
    pub n_row_out: f64,

    /// Total estimated cost
    pub total_cost: f64,

    /// Order by is satisfied by index
    pub order_by_satisfied: bool,

    /// Bitmask of all tables
    pub all_tables_mask: u64,
}

impl WhereInfo {
    /// Create new WhereInfo
    pub fn new() -> Self {
        WhereInfo {
            terms: Vec::new(),
            levels: Vec::new(),
            n_row_out: 1.0,
            total_cost: 0.0,
            order_by_satisfied: false,
            all_tables_mask: 0,
        }
    }

    /// Get the term at a given index
    pub fn get_term(&self, idx: i32) -> Option<&WhereTerm> {
        self.terms.get(idx as usize)
    }

    /// Get the level at a given index
    pub fn get_level(&self, idx: i32) -> Option<&WhereLevel> {
        self.levels.get(idx as usize)
    }
}

impl Default for WhereInfo {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// WhereClause
// ============================================================================

/// Represents a WHERE clause being analyzed
#[derive(Debug)]
pub struct WhereClause {
    /// All terms in the WHERE clause
    terms: Vec<WhereTerm>,

    /// Number of base terms (not derived)
    n_base: usize,
}

impl WhereClause {
    /// Create a new WHERE clause
    pub fn new() -> Self {
        WhereClause {
            terms: Vec::new(),
            n_base: 0,
        }
    }

    /// Add a term to the WHERE clause
    pub fn add_term(&mut self, term: WhereTerm) {
        let is_virtual = term.flags.contains(WhereTermFlags::VIRTUAL);
        self.terms.push(term);
        if !is_virtual {
            self.n_base = self.terms.len();
        }
    }

    /// Get number of terms
    pub fn len(&self) -> usize {
        self.terms.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    /// Iterate over terms
    pub fn iter(&self) -> impl Iterator<Item = &WhereTerm> {
        self.terms.iter()
    }

    /// Get mutable iterator
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut WhereTerm> {
        self.terms.iter_mut()
    }
}

impl Default for WhereClause {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Query Planner
// ============================================================================

/// Table information for query planning
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table name
    pub name: String,

    /// Alias (if any)
    pub alias: Option<String>,

    /// Index in FROM clause
    pub from_idx: i32,

    /// Bitmask for this table
    pub mask: u64,

    /// Estimated row count
    pub estimated_rows: i64,

    /// Available indexes
    pub indexes: Vec<IndexInfo>,

    /// Has rowid?
    pub has_rowid: bool,

    /// Table column names (for column index resolution)
    pub columns: Vec<String>,

    /// Column index that is INTEGER PRIMARY KEY (rowid alias), if any
    /// This is set when a column is declared as `INTEGER PRIMARY KEY` and
    /// acts as an alias for the rowid. Value is -1 if no IPK column.
    pub ipk_column: i32,
}

/// Index information for planning
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name
    pub name: String,

    /// Column indices in the index
    pub columns: Vec<i32>,

    /// Is this the primary key?
    pub is_primary: bool,

    /// Is this a unique index?
    pub is_unique: bool,

    /// Is covering (includes all needed columns)?
    pub is_covering: bool,
    /// Index statistics (sqlite_stat1)
    pub stats: Option<IndexStats>,
}

/// Query planner for WHERE clause optimization
pub struct QueryPlanner {
    /// Tables in the query
    tables: Vec<TableInfo>,

    /// WHERE clause being analyzed
    where_clause: WhereClause,

    /// Current best plan
    best_cost: f64,
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new() -> Self {
        QueryPlanner {
            tables: Vec::new(),
            where_clause: WhereClause::new(),
            best_cost: f64::MAX,
        }
    }

    /// Add a table to the planner
    pub fn add_table(&mut self, name: String, alias: Option<String>, estimated_rows: i64) {
        let from_idx = self.tables.len() as i32;
        let mask = 1u64 << from_idx;

        self.tables.push(TableInfo {
            name,
            alias,
            from_idx,
            mask,
            estimated_rows,
            indexes: Vec::new(),
            has_rowid: true,
            columns: Vec::new(),
            ipk_column: -1, // No INTEGER PRIMARY KEY column by default
        });
    }

    pub fn set_table_columns(&mut self, table_idx: usize, columns: Vec<String>) {
        if let Some(table) = self.tables.get_mut(table_idx) {
            table.columns = columns;
        }
    }

    pub fn set_table_rowid(&mut self, table_idx: usize, has_rowid: bool) {
        if let Some(table) = self.tables.get_mut(table_idx) {
            table.has_rowid = has_rowid;
        }
    }

    /// Set the INTEGER PRIMARY KEY column index for a table
    /// This column is an alias for rowid
    pub fn set_table_ipk(&mut self, table_idx: usize, ipk_column: i32) {
        if let Some(table) = self.tables.get_mut(table_idx) {
            table.ipk_column = ipk_column;
        }
    }

    /// Add an index to a table
    pub fn add_index(&mut self, table_idx: usize, index: IndexInfo) {
        if let Some(table) = self.tables.get_mut(table_idx) {
            table.indexes.push(index);
        }
    }

    /// Analyze a WHERE expression and extract terms
    pub fn analyze_where(&mut self, where_expr: Option<&Expr>) -> Result<()> {
        if let Some(expr) = where_expr {
            self.split_where_expr(expr, 0)?;
        }
        self.analyze_terms()?;
        Ok(())
    }

    /// Split a WHERE expression into individual terms
    fn split_where_expr(&mut self, expr: &Expr, depth: i32) -> Result<()> {
        match expr {
            // AND splits into multiple terms
            Expr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => {
                self.split_where_expr(left, depth + 1)?;
                self.split_where_expr(right, depth + 1)?;
            }

            // OR creates a single term (more complex handling needed)
            Expr::Binary {
                op: BinaryOp::Or, ..
            } => {
                let idx = self.where_clause.terms.len() as i32;
                let mut term = WhereTerm::new(expr.clone(), idx);
                term.flags |= WhereTermFlags::OR_INFO;
                term.or_terms = where_expr::split_or_clause(expr);
                self.where_clause.add_term(term);
            }

            Expr::Between {
                expr: inner,
                low,
                high,
                negated: false,
            } => {
                let idx = self.where_clause.terms.len() as i32;
                let mut lower = WhereTerm::new(
                    Expr::Binary {
                        op: BinaryOp::Ge,
                        left: inner.clone(),
                        right: low.clone(),
                    },
                    idx,
                );
                lower.flags |= WhereTermFlags::BETWEEN | WhereTermFlags::VIRTUAL;
                self.where_clause.add_term(lower);

                let idx = self.where_clause.terms.len() as i32;
                let mut upper = WhereTerm::new(
                    Expr::Binary {
                        op: BinaryOp::Le,
                        left: inner.clone(),
                        right: high.clone(),
                    },
                    idx,
                );
                upper.flags |= WhereTermFlags::BETWEEN | WhereTermFlags::VIRTUAL;
                self.where_clause.add_term(upper);
            }

            // All other expressions become individual terms
            _ => {
                let idx = self.where_clause.terms.len() as i32;
                let term = WhereTerm::new(expr.clone(), idx);
                self.where_clause.add_term(term);
            }
        }
        Ok(())
    }

    /// Analyze all terms to extract table references and operator types
    fn analyze_terms(&mut self) -> Result<()> {
        // Collect table info needed for analysis
        let table_usage_info: Vec<_> = self
            .tables
            .iter()
            .map(|t| (t.name.clone(), t.alias.clone(), t.mask))
            .collect();
        // Include ipk_column in the tuple for recognizing INTEGER PRIMARY KEY columns
        let table_info: Vec<_> = self
            .tables
            .iter()
            .map(|t| {
                (
                    t.name.clone(),
                    t.alias.clone(),
                    t.mask,
                    t.columns.clone(),
                    t.ipk_column,
                )
            })
            .collect();

        for term in self.where_clause.iter_mut() {
            term.mask = where_expr::expr_usage(term.expr.as_ref(), &table_usage_info);
            term.prereq = term.mask;
            // Determine operator type and selectivity
            Self::analyze_term_expr_static(&table_info, term)?;
        }
        Ok(())
    }

    /// Analyze a single term's expression (static version for borrow checker)
    fn analyze_term_expr_static(
        table_info: &[(String, Option<String>, u64, Vec<String>, i32)],
        term: &mut WhereTerm,
    ) -> Result<()> {
        let needs_commute = match term.expr.as_ref() {
            Expr::Binary { op, left, right } => {
                let is_comparison = matches!(
                    op,
                    BinaryOp::Eq
                        | BinaryOp::Ne
                        | BinaryOp::Lt
                        | BinaryOp::Le
                        | BinaryOp::Gt
                        | BinaryOp::Ge
                        | BinaryOp::Is
                        | BinaryOp::IsNot
                );
                if !is_comparison {
                    false
                } else {
                    let left_is_column = matches!(left.as_ref(), Expr::Column(_));
                    let right_is_column = matches!(right.as_ref(), Expr::Column(_));
                    !left_is_column && right_is_column
                }
            }
            _ => false,
        };

        if needs_commute {
            where_expr::commute_comparison(term.expr.as_mut());
        }

        let expr = term.expr.clone();
        match expr.as_ref() {
            Expr::Binary { op, .. } => {
                term.op = Some(match op {
                    BinaryOp::Eq => TermOp::Eq,
                    BinaryOp::Ne => TermOp::Ne,
                    BinaryOp::Lt => TermOp::Lt,
                    BinaryOp::Le => TermOp::Le,
                    BinaryOp::Gt => TermOp::Gt,
                    BinaryOp::Ge => TermOp::Ge,
                    BinaryOp::Is => TermOp::Is,
                    _ => return Ok(()),
                });

                term.selectivity = match term.op {
                    Some(TermOp::Eq) | Some(TermOp::Is) => 0.1, // 10% for equality
                    Some(TermOp::Ne) => 0.9,                    // 90% for not-equal
                    Some(TermOp::Lt | TermOp::Le | TermOp::Gt | TermOp::Ge) => 0.33,
                    Some(TermOp::Like | TermOp::Glob) => 0.25,
                    _ => 0.25,
                };

                let left = match expr.as_ref() {
                    Expr::Binary { left, .. } => left,
                    _ => return Ok(()),
                };
                Self::analyze_column_ref_static(table_info, term, left)?;
                if term.left_col.is_some() && term.is_index_usable() {
                    term.flags |= WhereTermFlags::INDEX_CONSTRAINT;
                }
            }

            Expr::IsNull { negated, expr } => {
                term.op = Some(if *negated {
                    TermOp::IsNotNull
                } else {
                    TermOp::IsNull
                });
                term.selectivity = if *negated { 0.95 } else { 0.05 };
                Self::analyze_column_ref_static(table_info, term, expr)?;
                if term.left_col.is_some() {
                    term.flags |= WhereTermFlags::INDEX_CONSTRAINT;
                }
            }

            Expr::In { expr: inner, .. } => {
                term.op = Some(TermOp::In);
                term.selectivity = 0.25;
                Self::analyze_column_ref_static(table_info, term, inner)?;
                if term.left_col.is_some() {
                    term.flags |= WhereTermFlags::INDEX_CONSTRAINT;
                }
            }

            Expr::Between { expr: inner, .. } => {
                term.op = Some(TermOp::Between);
                term.selectivity = 0.25;
                term.flags |= WhereTermFlags::BETWEEN;
                Self::analyze_column_ref_static(table_info, term, inner)?;
            }

            Expr::Like {
                expr: inner, op, ..
            } => {
                term.op = Some(match op {
                    LikeOp::Like | LikeOp::Regexp | LikeOp::Match => TermOp::Like,
                    LikeOp::Glob => TermOp::Glob,
                });
                term.selectivity = 0.25;
                term.flags |= WhereTermFlags::LIKE;
                if let Expr::Like { pattern, .. } = expr.as_ref() {
                    if Self::like_prefix(pattern, *op) {
                        term.flags |= WhereTermFlags::LIKE_PREFIX;
                    }
                }
                Self::analyze_column_ref_static(table_info, term, inner)?;
            }

            Expr::Unary {
                op: UnaryOp::Not, ..
            } => {
                // NOT expression - invert selectivity
                let inner_selectivity = 0.25;
                term.selectivity = 1.0 - inner_selectivity;
            }

            _ => {
                // Default selectivity for unknown expressions
                term.selectivity = 0.25;
            }
        }
        Ok(())
    }

    /// Analyze a potential column reference in an expression (static version)
    fn analyze_column_ref_static(
        table_info: &[(String, Option<String>, u64, Vec<String>, i32)],
        term: &mut WhereTerm,
        expr: &Expr,
    ) -> Result<()> {
        if let Expr::Column(col_ref) = expr {
            // Try to find which table this column belongs to
            for (i, (name, alias, mask, columns, ipk_column)) in table_info.iter().enumerate() {
                let table_matches = match (&col_ref.table, alias) {
                    (Some(t), Some(a)) => t == a || t == name,
                    (Some(t), None) => t == name,
                    (None, _) => true, // Could match any table
                };

                if table_matches {
                    // Determine column index:
                    // - If it's a rowid alias name (rowid, _rowid_, oid), use -1
                    // - If the column matches the INTEGER PRIMARY KEY column, use -1 (rowid alias)
                    // - Otherwise, use the regular column index
                    let column_idx = if Self::is_rowid_alias(&col_ref.column) {
                        Some(-1)
                    } else if let Some(idx) = col_ref.column_index {
                        // Check if this column is the INTEGER PRIMARY KEY (rowid alias)
                        if *ipk_column >= 0 && idx == *ipk_column {
                            Some(-1) // Treat as rowid
                        } else {
                            Some(idx)
                        }
                    } else {
                        // Look up column by name
                        let found_idx = columns
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(&col_ref.column))
                            .map(|idx| idx as i32);

                        // Check if this column is the INTEGER PRIMARY KEY (rowid alias)
                        if let Some(idx) = found_idx {
                            if *ipk_column >= 0 && idx == *ipk_column {
                                Some(-1) // Treat as rowid
                            } else {
                                Some(idx)
                            }
                        } else {
                            None
                        }
                    };

                    term.mask |= mask;
                    if let Some(idx) = column_idx {
                        term.left_col = Some((i as i32, idx));
                        term.flags |= WhereTermFlags::LEFT_COLUMN;
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    fn like_prefix(pattern: &Expr, op: LikeOp) -> bool {
        let text = match pattern {
            Expr::Literal(Literal::String(text)) => text,
            _ => return false,
        };

        let mut chars = text.chars();
        let first = match chars.next() {
            Some(ch) => ch,
            None => return false,
        };

        match op {
            LikeOp::Like | LikeOp::Regexp | LikeOp::Match => first != '%' && first != '_',
            LikeOp::Glob => first != '*' && first != '?',
        }
    }

    fn is_rowid_alias(name: &str) -> bool {
        name.eq_ignore_ascii_case("rowid")
            || name.eq_ignore_ascii_case("_rowid_")
            || name.eq_ignore_ascii_case("oid")
    }

    /// Find the optimal query plan
    pub fn find_best_plan(&mut self) -> Result<WhereInfo> {
        let n_tables = self.tables.len();
        if n_tables == 0 {
            return Ok(WhereInfo::new());
        }

        if n_tables > MAX_TABLES {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("Too many tables in join (max {})", MAX_TABLES),
            ));
        }

        // For small joins, try all permutations
        // For larger joins, use greedy algorithm
        if n_tables <= 6 {
            self.find_best_plan_exhaustive()
        } else {
            self.find_best_plan_greedy()
        }
    }

    /// Try all table orderings (for small joins)
    fn find_best_plan_exhaustive(&mut self) -> Result<WhereInfo> {
        let n = self.tables.len();
        let mut best_info = WhereInfo::new();
        let mut best_cost = f64::MAX;

        // Generate all permutations
        let mut indices: Vec<usize> = (0..n).collect();

        loop {
            // Evaluate this ordering
            let (cost, info) = self.evaluate_ordering(&indices)?;
            if cost < best_cost {
                best_cost = cost;
                best_info = info;
            }

            // Next permutation
            if !next_permutation(&mut indices) {
                break;
            }
        }

        best_info.total_cost = best_cost;
        Ok(best_info)
    }

    /// Greedy algorithm for larger joins
    fn find_best_plan_greedy(&mut self) -> Result<WhereInfo> {
        let n = self.tables.len();
        let mut info = WhereInfo::new();
        let mut used_tables = 0u64;
        let mut total_cost = 0.0;
        let mut rows_so_far = 1.0;

        for _ in 0..n {
            let mut best_table = 0;
            let mut best_add_cost = f64::MAX;
            let mut best_level = None;

            // Find the best next table to add
            for (i, table) in self.tables.iter().enumerate() {
                if used_tables & table.mask != 0 {
                    continue; // Already used
                }

                let (add_cost, level) = self.evaluate_table_access(i, used_tables, rows_so_far)?;

                if add_cost < best_add_cost {
                    best_add_cost = add_cost;
                    best_table = i;
                    best_level = Some(level);
                }
            }

            if let Some(level) = best_level {
                used_tables |= self.tables[best_table].mask;
                total_cost += best_add_cost;
                rows_so_far *= level.rows_out;
                info.levels.push(level);
            }
        }

        info.total_cost = total_cost;
        info.n_row_out = rows_so_far;
        info.all_tables_mask = used_tables;

        // Copy terms
        info.terms = self.where_clause.terms.clone();

        Ok(info)
    }

    /// Evaluate a specific table ordering
    fn evaluate_ordering(&self, order: &[usize]) -> Result<(f64, WhereInfo)> {
        let mut info = WhereInfo::new();
        let mut used_tables = 0u64;
        let mut total_cost = 0.0;
        let mut rows_so_far = 1.0;

        for &table_idx in order {
            let (add_cost, level) =
                self.evaluate_table_access(table_idx, used_tables, rows_so_far)?;

            used_tables |= self.tables[table_idx].mask;
            total_cost += add_cost;
            rows_so_far *= level.rows_out;
            info.levels.push(level);
        }

        info.total_cost = total_cost;
        info.n_row_out = rows_so_far;
        info.all_tables_mask = used_tables;
        info.terms = self.where_clause.terms.clone();

        Ok((total_cost, info))
    }

    /// Evaluate accessing a single table given what's already been joined
    fn evaluate_table_access(
        &self,
        table_idx: usize,
        prereq_mask: u64,
        rows_in: f64,
    ) -> Result<(f64, WhereLevel)> {
        let table = &self.tables[table_idx];
        let mut level = WhereLevel::new(table.from_idx, table.name.clone());

        // Find usable terms for this table
        let mut usable_eq_terms: Vec<&WhereTerm> = Vec::new();
        let mut usable_range_terms: Vec<&WhereTerm> = Vec::new();
        let mut total_selectivity = 1.0;

        for term in self.where_clause.iter() {
            // Check if term references this table and prereqs are satisfied
            if term.mask & table.mask != 0 {
                let prereqs_satisfied =
                    (term.prereq & !prereq_mask) == 0 || term.prereq & table.mask != 0;

                if prereqs_satisfied && term.is_index_usable() {
                    if term.is_equality() {
                        usable_eq_terms.push(term);
                    } else if term.is_range() {
                        usable_range_terms.push(term);
                    }
                    total_selectivity *= term.selectivity;
                    level.used_terms.push(term.idx);
                }
            }
        }

        // Try to find best index
        let mut best_plan = WherePlan::FullScan;
        let mut best_cost = table.estimated_rows as f64 * FULL_SCAN_COST_MULT;

        // Check rowid equality
        if usable_eq_terms
            .iter()
            .any(|t| t.left_col == Some((table_idx as i32, -1)))
        {
            let cost = INDEX_SEEK_COST + ROW_READ_COST;
            if cost < best_cost {
                best_cost = cost;
                best_plan = WherePlan::RowidEq;
            }
        }

        // Check indexes
        for index in &table.indexes {
            let eq_match_count = self.count_index_eq_matches(index, &usable_eq_terms, table_idx);
            let has_range =
                self.index_has_range_match(index, &usable_range_terms, table_idx, eq_match_count);

            // Find range termination term (Lt or Le on the column after eq columns)
            let range_end = if has_range && eq_match_count > 0 {
                let next_idx = eq_match_count as usize;
                if next_idx < index.columns.len() {
                    let col_idx = index.columns[next_idx];
                    usable_range_terms.iter().find_map(|t| {
                        if t.left_col
                            .is_some_and(|(ti, ci)| ti == table_idx as i32 && ci == col_idx)
                        {
                            // Only use Lt or Le for termination (upper bound)
                            match t.op {
                                Some(TermOp::Lt) | Some(TermOp::Le) => {
                                    Some((col_idx, t.op.unwrap(), t.idx))
                                }
                                _ => None,
                            }
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            } else {
                None
            };

            if eq_match_count > 0 || has_range {
                let rows = if eq_match_count > 0 {
                    self.estimate_index_rows(table, index, eq_match_count)
                } else {
                    (table.estimated_rows as f64 * 0.33).max(1.0)
                };
                let cost = INDEX_SEEK_COST + rows * ROW_READ_COST;

                if cost < best_cost {
                    best_cost = cost;
                    best_plan = WherePlan::IndexScan {
                        index_name: index.name.clone(),
                        eq_cols: eq_match_count,
                        covering: index.is_covering,
                        has_range,
                        range_end,
                    };
                    level.flags |= WhereLevelFlags::INDEXED;
                    if index.is_unique && eq_match_count == index.columns.len() as i32 {
                        level.flags |= WhereLevelFlags::UNIQUE;
                    }
                }
            }
        }

        // Check primary key
        if table.has_rowid {
            let pk_eq_count = usable_eq_terms
                .iter()
                .filter(|t| {
                    t.left_col
                        .is_some_and(|(ti, ci)| ti == table_idx as i32 && ci == -1)
                })
                .count();

            if pk_eq_count > 0 {
                let cost = INDEX_SEEK_COST + ROW_READ_COST;
                if cost < best_cost {
                    best_cost = cost;
                    best_plan = WherePlan::PrimaryKey {
                        eq_cols: pk_eq_count as i32,
                    };
                    level.flags |= WhereLevelFlags::UNIQUE;
                }
            }
        }

        // Calculate output rows
        let output_rows = match &best_plan {
            WherePlan::RowidEq | WherePlan::PrimaryKey { .. } => 1.0,
            WherePlan::IndexScan {
                index_name,
                eq_cols,
                ..
            } => {
                if let Some(index) = table.indexes.iter().find(|idx| idx.name == *index_name) {
                    self.estimate_index_rows(table, index, *eq_cols)
                } else {
                    (table.estimated_rows as f64 * 0.1f64.powi(*eq_cols)).max(1.0)
                }
            }
            _ => table.estimated_rows as f64 * total_selectivity,
        };

        level.plan = best_plan;
        level.rows_out = output_rows.max(1.0);
        level.cost = best_cost * rows_in;

        Ok((level.cost, level))
    }

    fn estimate_index_rows(
        &self,
        table: &TableInfo,
        index: &IndexInfo,
        eq_match_count: i32,
    ) -> f64 {
        if eq_match_count <= 0 {
            return (table.estimated_rows as f64).max(1.0);
        }
        if let Some(stats) = &index.stats {
            let idx = (eq_match_count as usize).saturating_sub(1);
            if idx < stats.avg_eq.len() {
                let estimate = stats.avg_eq[idx];
                if estimate > 0.0 {
                    return estimate.max(1.0);
                }
            }
            let base = if stats.row_count > 0 {
                stats.row_count as f64
            } else {
                table.estimated_rows as f64
            };
            return (base * 0.1f64.powi(eq_match_count)).max(1.0);
        }
        (table.estimated_rows as f64 * 0.1f64.powi(eq_match_count)).max(1.0)
    }

    /// Count how many index columns match equality terms
    fn count_index_eq_matches(
        &self,
        index: &IndexInfo,
        eq_terms: &[&WhereTerm],
        table_idx: usize,
    ) -> i32 {
        let mut count = 0;
        for (i, &col_idx) in index.columns.iter().enumerate() {
            let has_eq = eq_terms.iter().any(|t| {
                t.left_col
                    .is_some_and(|(ti, ci)| ti == table_idx as i32 && ci == col_idx)
            });

            if has_eq {
                count = (i + 1) as i32;
            } else {
                break; // Can only use prefix of index
            }
        }
        count
    }

    fn index_has_range_match(
        &self,
        index: &IndexInfo,
        range_terms: &[&WhereTerm],
        table_idx: usize,
        eq_match_count: i32,
    ) -> bool {
        let next_idx = eq_match_count as usize;
        if next_idx >= index.columns.len() {
            return false;
        }
        let col_idx = index.columns[next_idx];
        range_terms.iter().any(|t| {
            t.left_col
                .is_some_and(|(ti, ci)| ti == table_idx as i32 && ci == col_idx)
        })
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Generate next permutation (lexicographic order)
fn next_permutation<T: Ord>(arr: &mut [T]) -> bool {
    let n = arr.len();
    if n < 2 {
        return false;
    }

    // Find largest index i such that arr[i] < arr[i + 1]
    let mut i = n - 2;
    while arr[i] >= arr[i + 1] {
        if i == 0 {
            return false;
        }
        i -= 1;
    }

    // Find largest index j such that arr[i] < arr[j]
    let mut j = n - 1;
    while arr[i] >= arr[j] {
        j -= 1;
    }

    // Swap arr[i] and arr[j]
    arr.swap(i, j);

    // Reverse arr[i+1..]
    arr[i + 1..].reverse();

    true
}

// ============================================================================
// Public API
// ============================================================================

/// Analyze a WHERE clause and produce an optimized query plan
pub fn analyze_where(
    tables: &[(String, Option<String>, i64)], // (name, alias, estimated_rows)
    indexes: &[(usize, IndexInfo)],           // (table_idx, index_info)
    where_expr: Option<&Expr>,
) -> Result<WhereInfo> {
    let mut planner = QueryPlanner::new();

    // Add tables
    for (name, alias, rows) in tables {
        planner.add_table(name.clone(), alias.clone(), *rows);
    }

    // Add indexes
    for (table_idx, index) in indexes {
        planner.add_index(*table_idx, index.clone());
    }

    // Analyze WHERE clause
    planner.analyze_where(where_expr)?;

    // Find best plan
    planner.find_best_plan()
}

/// Estimate the cost of a simple single-table query
pub fn estimate_simple_cost(estimated_rows: i64, has_index: bool, eq_terms: i32) -> f64 {
    if has_index && eq_terms > 0 {
        INDEX_SEEK_COST + (estimated_rows as f64 * 0.1f64.powi(eq_terms)).max(1.0)
    } else {
        estimated_rows as f64 * FULL_SCAN_COST_MULT
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_where_term_creation() {
        let expr = Expr::Literal(crate::parser::ast::Literal::Integer(1));
        let term = WhereTerm::new(expr, 0);

        assert_eq!(term.idx, 0);
        assert!(term.flags.is_empty());
        assert_eq!(term.selectivity, 0.25);
    }

    #[test]
    fn test_query_planner_single_table() {
        let mut planner = QueryPlanner::new();
        planner.add_table("users".to_string(), None, 1000);

        let result = planner.find_best_plan().unwrap();
        assert_eq!(result.levels.len(), 1);
        assert!(matches!(result.levels[0].plan, WherePlan::FullScan));
    }

    #[test]
    fn test_query_planner_with_index() {
        let mut planner = QueryPlanner::new();
        planner.add_table("users".to_string(), None, 1000);
        planner.add_index(
            0,
            IndexInfo {
                name: "idx_users_email".to_string(),
                columns: vec![1],
                is_primary: false,
                is_unique: true,
                is_covering: false,
                stats: None,
            },
        );

        let result = planner.find_best_plan().unwrap();
        assert_eq!(result.levels.len(), 1);
    }

    #[test]
    fn test_where_clause_split() {
        let mut planner = QueryPlanner::new();
        planner.add_table("users".to_string(), None, 1000);

        // a = 1 AND b = 2
        let expr = Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Column(crate::parser::ast::ColumnRef {
                    database: None,
                    table: None,
                    column: "a".to_string(),
                    column_index: None,
                })),
                right: Box::new(Expr::Literal(crate::parser::ast::Literal::Integer(1))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Column(crate::parser::ast::ColumnRef {
                    database: None,
                    table: None,
                    column: "b".to_string(),
                    column_index: None,
                })),
                right: Box::new(Expr::Literal(crate::parser::ast::Literal::Integer(2))),
            }),
        };

        planner.analyze_where(Some(&expr)).unwrap();

        // Should be split into 2 terms
        assert_eq!(planner.where_clause.len(), 2);
    }

    #[test]
    fn test_where_clause_between_split() {
        let mut planner = QueryPlanner::new();
        planner.add_table("users".to_string(), None, 1000);

        let expr = Expr::Between {
            expr: Box::new(Expr::column("a")),
            low: Box::new(Expr::Literal(crate::parser::ast::Literal::Integer(1))),
            high: Box::new(Expr::Literal(crate::parser::ast::Literal::Integer(10))),
            negated: false,
        };

        planner.analyze_where(Some(&expr)).unwrap();
        assert_eq!(planner.where_clause.len(), 2);
        assert!(planner
            .where_clause
            .terms
            .iter()
            .any(|term| term.op == Some(TermOp::Ge)));
        assert!(planner
            .where_clause
            .terms
            .iter()
            .any(|term| term.op == Some(TermOp::Le)));
    }

    #[test]
    fn test_where_clause_or_split() {
        let mut planner = QueryPlanner::new();
        planner.add_table("users".to_string(), None, 1000);

        let expr = Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::column("a")),
                right: Box::new(Expr::Literal(crate::parser::ast::Literal::Integer(1))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::column("b")),
                right: Box::new(Expr::Literal(crate::parser::ast::Literal::Integer(2))),
            }),
        };

        planner.analyze_where(Some(&expr)).unwrap();
        assert_eq!(planner.where_clause.len(), 1);
        let term = &planner.where_clause.terms[0];
        assert!(term.flags.contains(WhereTermFlags::OR_INFO));
        assert_eq!(term.or_terms.len(), 2);
    }

    #[test]
    fn test_like_prefix_flag() {
        let mut planner = QueryPlanner::new();
        planner.add_table("users".to_string(), None, 1000);

        let expr = Expr::Like {
            expr: Box::new(Expr::column("a")),
            pattern: Box::new(Expr::Literal(crate::parser::ast::Literal::String(
                "abc%".to_string(),
            ))),
            escape: None,
            op: LikeOp::Like,
            negated: false,
        };

        planner.analyze_where(Some(&expr)).unwrap();
        let term = &planner.where_clause.terms[0];
        assert!(term.flags.contains(WhereTermFlags::LIKE_PREFIX));
    }

    #[test]
    fn test_permutation() {
        let mut arr = [1, 2, 3];
        let mut count = 1;
        while next_permutation(&mut arr) {
            count += 1;
        }
        assert_eq!(count, 6); // 3! = 6
    }

    #[test]
    fn test_selectivity_estimates() {
        let mut term = WhereTerm::new(Expr::Literal(crate::parser::ast::Literal::Integer(1)), 0);
        term.op = Some(TermOp::Eq);
        term.selectivity = 0.1;
        assert!(term.is_equality());
        assert!(!term.is_range());

        term.op = Some(TermOp::Lt);
        assert!(!term.is_equality());
        assert!(term.is_range());
    }

    #[test]
    fn test_estimate_simple_cost() {
        // Full scan of 1000 rows
        let cost1 = estimate_simple_cost(1000, false, 0);
        assert!(cost1 > 1000.0);

        // Index lookup with equality
        let cost2 = estimate_simple_cost(1000, true, 1);
        assert!(cost2 < cost1);

        // Two equality constraints should be cheaper
        let cost3 = estimate_simple_cost(1000, true, 2);
        assert!(cost3 < cost2);
    }
}
