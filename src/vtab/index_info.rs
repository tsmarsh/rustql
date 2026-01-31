//! Index information for virtual table query planning
//!
//! This module provides structures for the xBestIndex callback, which allows
//! virtual tables to communicate query planning decisions to the SQLite query
//! optimizer.

use crate::vdbe::mem::Mem;

/// SQLite index constraint operators
pub const SQLITE_INDEX_CONSTRAINT_EQ: u8 = 2;
pub const SQLITE_INDEX_CONSTRAINT_GT: u8 = 4;
pub const SQLITE_INDEX_CONSTRAINT_LE: u8 = 8;
pub const SQLITE_INDEX_CONSTRAINT_LT: u8 = 16;
pub const SQLITE_INDEX_CONSTRAINT_GE: u8 = 32;
pub const SQLITE_INDEX_CONSTRAINT_MATCH: u8 = 64;
pub const SQLITE_INDEX_CONSTRAINT_LIKE: u8 = 65;
pub const SQLITE_INDEX_CONSTRAINT_GLOB: u8 = 66;
pub const SQLITE_INDEX_CONSTRAINT_REGEXP: u8 = 67;
pub const SQLITE_INDEX_CONSTRAINT_NE: u8 = 68;
pub const SQLITE_INDEX_CONSTRAINT_ISNOT: u8 = 69;
pub const SQLITE_INDEX_CONSTRAINT_ISNOTNULL: u8 = 70;
pub const SQLITE_INDEX_CONSTRAINT_ISNULL: u8 = 71;
pub const SQLITE_INDEX_CONSTRAINT_IS: u8 = 72;
pub const SQLITE_INDEX_CONSTRAINT_LIMIT: u8 = 73;
pub const SQLITE_INDEX_CONSTRAINT_OFFSET: u8 = 74;

/// Information passed to xBestIndex
///
/// This structure mirrors `sqlite3_index_info` and is used by virtual tables
/// to communicate query planning decisions to the optimizer.
///
/// The structure has two parts:
/// - Input fields (constraints, order_by) describe the query
/// - Output fields (idx_num, idx_str, constraint_usage, etc.) describe the
///   chosen strategy
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Input: array of constraint specifications
    pub constraints: Vec<IndexConstraint>,

    /// Input: ORDER BY specification
    pub order_by: Vec<OrderBy>,

    /// Output: recommended index number (passed to xFilter)
    pub idx_num: i32,

    /// Output: index string (passed to xFilter)
    pub idx_str: Option<String>,

    /// Output: which constraints can be handled by this index
    pub constraint_usage: Vec<ConstraintUsage>,

    /// Output: true if output is already ordered according to order_by
    pub order_by_consumed: bool,

    /// Output: estimated cost of this index strategy
    pub estimated_cost: f64,

    /// Output: estimated number of rows
    pub estimated_rows: i64,

    /// Output: mask of columns used by this index
    pub columns_used: u64,
}

impl IndexInfo {
    /// Create a new IndexInfo for query planning
    pub fn new(constraints: Vec<IndexConstraint>, order_by: Vec<OrderBy>) -> Self {
        let n_constraints = constraints.len();
        Self {
            constraints,
            order_by,
            idx_num: 0,
            idx_str: None,
            constraint_usage: vec![ConstraintUsage::default(); n_constraints],
            order_by_consumed: false,
            estimated_cost: f64::MAX,
            estimated_rows: i64::MAX,
            columns_used: 0,
        }
    }

    /// Find constraints for a specific column
    pub fn constraints_for_column(
        &self,
        col_idx: i32,
    ) -> impl Iterator<Item = (usize, &IndexConstraint)> {
        self.constraints
            .iter()
            .enumerate()
            .filter(move |(_, c)| c.col_idx == col_idx && c.usable)
    }

    /// Find a specific constraint type for a column
    pub fn find_constraint(&self, col_idx: i32, op: u8) -> Option<(usize, &IndexConstraint)> {
        self.constraints_for_column(col_idx)
            .find(|(_, c)| c.op == op)
    }

    /// Mark a constraint as used by the index
    pub fn use_constraint(&mut self, idx: usize, arg_index: i32, omit: bool) {
        if idx < self.constraint_usage.len() {
            self.constraint_usage[idx] = ConstraintUsage { arg_index, omit };
        }
    }
}

impl Default for IndexInfo {
    fn default() -> Self {
        Self::new(vec![], vec![])
    }
}

/// Single constraint specification from the WHERE clause
#[derive(Debug, Clone)]
pub struct IndexConstraint {
    /// Column index, or -1 for rowid
    pub col_idx: i32,

    /// Comparison operator (see SQLITE_INDEX_CONSTRAINT_* constants)
    pub op: u8,

    /// Is this constraint usable for the virtual table?
    /// A constraint is not usable if it references a table to the right
    /// in a join.
    pub usable: bool,
}

impl IndexConstraint {
    /// Create a new constraint
    pub fn new(col_idx: i32, op: u8, usable: bool) -> Self {
        Self {
            col_idx,
            op,
            usable,
        }
    }

    /// Check if this is an equality constraint
    pub fn is_eq(&self) -> bool {
        self.op == SQLITE_INDEX_CONSTRAINT_EQ
    }

    /// Check if this is a MATCH constraint (used by FTS)
    pub fn is_match(&self) -> bool {
        self.op == SQLITE_INDEX_CONSTRAINT_MATCH
    }

    /// Get a human-readable description of the operator
    pub fn op_name(&self) -> &'static str {
        match self.op {
            SQLITE_INDEX_CONSTRAINT_EQ => "=",
            SQLITE_INDEX_CONSTRAINT_GT => ">",
            SQLITE_INDEX_CONSTRAINT_LE => "<=",
            SQLITE_INDEX_CONSTRAINT_LT => "<",
            SQLITE_INDEX_CONSTRAINT_GE => ">=",
            SQLITE_INDEX_CONSTRAINT_MATCH => "MATCH",
            SQLITE_INDEX_CONSTRAINT_LIKE => "LIKE",
            SQLITE_INDEX_CONSTRAINT_GLOB => "GLOB",
            SQLITE_INDEX_CONSTRAINT_REGEXP => "REGEXP",
            SQLITE_INDEX_CONSTRAINT_NE => "!=",
            SQLITE_INDEX_CONSTRAINT_ISNOT => "IS NOT",
            SQLITE_INDEX_CONSTRAINT_ISNOTNULL => "IS NOT NULL",
            SQLITE_INDEX_CONSTRAINT_ISNULL => "IS NULL",
            SQLITE_INDEX_CONSTRAINT_IS => "IS",
            SQLITE_INDEX_CONSTRAINT_LIMIT => "LIMIT",
            SQLITE_INDEX_CONSTRAINT_OFFSET => "OFFSET",
            _ => "?",
        }
    }
}

/// Usage details for a constraint (output from xBestIndex)
#[derive(Debug, Clone, Default)]
pub struct ConstraintUsage {
    /// Argument index for this constraint when passed to xFilter (1-based)
    /// 0 means the constraint is not used
    pub arg_index: i32,

    /// If true, the virtual table guarantees the constraint is satisfied
    /// and SQLite should not double-check it
    pub omit: bool,
}

/// ORDER BY clause specification
#[derive(Debug, Clone)]
pub struct OrderBy {
    /// Column index
    pub col_idx: i32,

    /// Is descending order?
    pub desc: bool,
}

impl OrderBy {
    /// Create a new ORDER BY specification
    pub fn new(col_idx: i32, desc: bool) -> Self {
        Self { col_idx, desc }
    }
}

/// Constraint value passed to xFilter
///
/// This represents a bound parameter from the WHERE clause that was
/// marked as usable by xBestIndex.
#[derive(Debug, Clone)]
pub struct ConstraintValue {
    /// The actual value
    pub value: Mem,

    /// The constraint operator that produced this value
    pub op: u8,

    /// The column this constraint applies to
    pub col_idx: i32,
}

impl ConstraintValue {
    /// Create a new constraint value
    pub fn new(value: Mem, op: u8, col_idx: i32) -> Self {
        Self { value, op, col_idx }
    }

    /// Get the value as a string (for MATCH/LIKE/GLOB queries)
    pub fn as_str(&self) -> String {
        self.value.to_str()
    }

    /// Get the value as an integer
    pub fn as_int(&self) -> i64 {
        self.value.to_int()
    }

    /// Get the value as a float
    pub fn as_float(&self) -> f64 {
        self.value.to_real()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_info_creation() {
        let constraints = vec![
            IndexConstraint::new(0, SQLITE_INDEX_CONSTRAINT_EQ, true),
            IndexConstraint::new(1, SQLITE_INDEX_CONSTRAINT_GT, true),
        ];
        let order_by = vec![OrderBy::new(0, false)];

        let info = IndexInfo::new(constraints, order_by);
        assert_eq!(info.constraints.len(), 2);
        assert_eq!(info.order_by.len(), 1);
        assert_eq!(info.constraint_usage.len(), 2);
    }

    #[test]
    fn test_find_constraint() {
        let constraints = vec![
            IndexConstraint::new(0, SQLITE_INDEX_CONSTRAINT_EQ, true),
            IndexConstraint::new(0, SQLITE_INDEX_CONSTRAINT_GT, false),
            IndexConstraint::new(1, SQLITE_INDEX_CONSTRAINT_MATCH, true),
        ];
        let info = IndexInfo::new(constraints, vec![]);

        // Should find usable EQ constraint on column 0
        let found = info.find_constraint(0, SQLITE_INDEX_CONSTRAINT_EQ);
        assert!(found.is_some());
        assert_eq!(found.unwrap().0, 0);

        // Should not find GT constraint (not usable)
        let found = info.find_constraint(0, SQLITE_INDEX_CONSTRAINT_GT);
        assert!(found.is_none());

        // Should find MATCH constraint on column 1
        let found = info.find_constraint(1, SQLITE_INDEX_CONSTRAINT_MATCH);
        assert!(found.is_some());
    }

    #[test]
    fn test_constraint_op_names() {
        assert_eq!(
            IndexConstraint::new(0, SQLITE_INDEX_CONSTRAINT_EQ, true).op_name(),
            "="
        );
        assert_eq!(
            IndexConstraint::new(0, SQLITE_INDEX_CONSTRAINT_MATCH, true).op_name(),
            "MATCH"
        );
        assert_eq!(
            IndexConstraint::new(0, SQLITE_INDEX_CONSTRAINT_LIKE, true).op_name(),
            "LIKE"
        );
    }
}
