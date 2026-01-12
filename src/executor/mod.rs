//! Query execution: SELECT, INSERT, UPDATE, DELETE

pub mod analyze;
pub mod delete;
pub mod fkey;
pub mod insert;
pub mod planner;
pub mod pragma;
pub mod prepare;
pub mod select;
pub mod trigger;
pub mod update;
pub mod where_clause;
pub mod where_expr;
pub mod wherecode;
pub mod window;

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
pub use fkey::{
    fk_check_delete, fk_check_insert, fk_check_update, foreign_key_check, DeferredFkState,
    FkContext, FkViolation,
};
pub use insert::{compile_insert, InsertCompiler};
pub use prepare::{
    compile_sql, compile_sql_with_schema, parse_sql, CompiledStmt, StatementCompiler, StmtType,
};
pub use trigger::{
    compile_create_trigger, compile_drop_trigger, find_matching_triggers, generate_trigger_code,
    TriggerContext, TriggerMask,
};
pub use update::{compile_update, UpdateCompiler};
pub use window::{
    default_frame_for_function, has_window_function, select_has_window_functions, WindowCompiler,
    WindowFunc, WindowFuncType, WindowInfo,
};
