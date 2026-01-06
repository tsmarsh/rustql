//! Query execution: SELECT, INSERT, UPDATE, DELETE

pub mod delete;
pub mod insert;
pub mod planner;
pub mod prepare;
pub mod select;
pub mod update;
pub mod where_clause;
pub mod where_expr;
pub mod wherecode;

pub use where_clause::{
    analyze_where, estimate_simple_cost, IndexInfo, QueryPlanner, TableInfo, TermOp, WhereClause,
    WhereInfo, WhereLevel, WherePlan, WhereTerm,
};
pub use where_expr::{
    allowed_expr_op, commute_comparison, expr_list_usage, expr_usage, operator_mask, select_usage,
    split_or_clause, OperatorMask,
};

pub use wherecode::{apply_affinity, generate_where_code, Affinity, WhereCodeGen};

pub use delete::{compile_delete, DeleteCompiler};
pub use insert::{compile_insert, InsertCompiler};
pub use prepare::{compile_sql, parse_sql, CompiledStmt, StatementCompiler, StmtType};
pub use update::{compile_update, UpdateCompiler};
