//! Types and helper functions for SELECT statement compilation

use crate::parser::ast::{Expr, JoinType, Literal};
use crate::schema::{Affinity, Table};
use std::sync::Arc;

// ============================================================================
// Select Destination
// ============================================================================

/// Where to send SELECT results
#[derive(Debug, Clone, Default)]
pub enum SelectDest {
    /// Return results to caller (normal query)
    #[default]
    Output,
    /// Store in memory registers starting at reg
    Mem { base_reg: i32 },
    /// Store in table with given cursor
    Table { cursor: i32 },
    /// Store in ephemeral table for UNION, etc.
    EphemTable { cursor: i32 },
    /// Store in ephemeral table with DISTINCT (skip duplicates) for UNION
    EphemTableDistinct { cursor: i32 },
    /// Coroutine yield
    Coroutine { reg: i32 },
    /// EXISTS subquery - set reg to 1 if any rows
    Exists { reg: i32 },
    /// Store in sorter for ORDER BY
    Sorter { cursor: i32 },
    /// Set result to column 0 of first row
    Set { reg: i32 },
    /// Discard results (e.g., INSERT ... SELECT with side effects)
    Discard,
}

// ============================================================================
// Column Info
// ============================================================================

/// Resolved column information
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name (or alias)
    pub name: String,
    /// Source table (if known)
    pub table: Option<String>,
    /// Column affinity
    pub affinity: Affinity,
    /// Register holding the value
    pub reg: i32,
    /// Explicit alias (for ORDER BY resolution)
    pub alias: Option<String>,
}

// ============================================================================
// Table Reference Info
// ============================================================================

/// Information about a table in FROM clause
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table name or alias
    pub name: String,
    /// Original table name (if alias used)
    pub table_name: String,
    /// VDBE cursor number
    pub cursor: i32,
    /// Schema table (if real table)
    pub schema_table: Option<Arc<Table>>,
    /// Is this from a subquery?
    pub is_subquery: bool,
    /// Join type (for joined tables)
    pub join_type: JoinType,
    /// Subquery result column names (for * expansion)
    pub subquery_columns: Option<Vec<String>>,
}

// ============================================================================
// FTS Match Filter (internal)
// ============================================================================

#[derive(Debug, Clone)]
pub(crate) struct Fts3MatchFilter {
    pub cursor: i32,
    pub pattern: Expr,
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Extract literal text from an expression
pub(crate) fn filter_literal_text(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(Literal::String(text)) => Some(text.clone()),
        _ => None,
    }
}

/// Check if a column name is a rowid alias
pub(crate) fn is_rowid_alias(name: &str) -> bool {
    name.eq_ignore_ascii_case("rowid")
        || name.eq_ignore_ascii_case("_rowid_")
        || name.eq_ignore_ascii_case("oid")
}
