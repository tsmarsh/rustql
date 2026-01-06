//! Query execution: SELECT, INSERT, UPDATE, DELETE

pub mod select;
pub mod insert;
pub mod update;
pub mod delete;
pub mod planner;
pub mod where_clause;
pub mod wherecode;

pub use where_clause::{
    WhereInfo, WhereTerm, WhereLevel, WherePlan, WhereClause,
    QueryPlanner, TableInfo, IndexInfo, TermOp,
    analyze_where, estimate_simple_cost,
};

pub use wherecode::{
    WhereCodeGen, Affinity,
    generate_where_code, apply_affinity,
};

pub use insert::{InsertCompiler, compile_insert};
pub use update::{UpdateCompiler, compile_update};
pub use delete::{DeleteCompiler, compile_delete};
