//! SELECT statement code generation
//!
//! This module generates VDBE opcodes for SELECT statements.
//! Corresponds to SQLite's select.c.

mod types;

use types::{filter_literal_text, is_rowid_alias, Fts3MatchFilter};
pub use types::{ColumnInfo, SelectDest, TableInfo};

use std::collections::{HashMap, HashSet};

use crate::error::{Error, ErrorCode, Result};
use crate::executor::where_clause::{
    IndexInfo, QueryPlanner, TermOp, WhereInfo, WhereLevel, WherePlan, WhereTerm, WhereTermFlags,
};
use crate::executor::window::{select_has_window_functions, WindowCompiler};
use crate::parser::ast::{
    BinaryOp, ColumnRef, CommonTableExpr, CompoundOp, Distinct, Expr, FromClause, JoinFlags,
    JoinType, LikeOp, LimitClause, Literal, OrderingTerm, ResultColumn, SelectBody, SelectCore,
    SelectStmt, SortOrder, TableRef, WithClause,
};
use crate::schema::{Affinity, GeneratedStorage, Table};
use crate::vdbe::ops::{
    affinity as vdbe_affinity, KeyInfo, Opcode, VFilterConstraint, VFilterPlan, VdbeOp, P4,
};
use crate::vdbe::types::SQLITE_MAX_COMPOUND_SELECT;
use crate::vtab::{
    IndexConstraint as VtabIndexConstraint, IndexInfo as VtabIndexInfo, VtabRegistry,
    SQLITE_INDEX_CONSTRAINT_EQ, SQLITE_INDEX_CONSTRAINT_GE, SQLITE_INDEX_CONSTRAINT_GT,
    SQLITE_INDEX_CONSTRAINT_LE, SQLITE_INDEX_CONSTRAINT_LIKE, SQLITE_INDEX_CONSTRAINT_LT,
    SQLITE_INDEX_CONSTRAINT_MATCH,
};

/// Maximum number of tables allowed in a single join (matches SQLite BMS)
const MAX_TABLES_IN_JOIN: usize = 64;

// ============================================================================
// Select Compiler State
// ============================================================================

/// State for SELECT compilation
pub struct SelectCompiler<'s> {
    /// Generated opcodes
    ops: Vec<VdbeOp>,
    /// Next available register
    next_reg: i32,
    /// Next available cursor
    next_cursor: i32,
    /// Tables in FROM clause
    tables: Vec<TableInfo>,
    /// Resolved columns
    columns: Vec<ColumnInfo>,
    /// Label counter for jumps
    next_label: i32,
    /// Pending labels (label -> address)
    labels: HashMap<i32, Option<i32>>,
    /// CTE definitions
    ctes: HashMap<String, CommonTableExpr>,
    /// Names of CTEs declared in WITH RECURSIVE
    recursive_ctes: HashSet<String>,
    /// CTEs mapped to existing cursors (used for recursive evaluation)
    cte_cursors: HashMap<String, (i32, Vec<String>)>,
    /// Is this a compound select?
    is_compound: bool,
    /// Has aggregates?
    has_aggregates: bool,
    /// Has window functions?
    has_window_functions: bool,
    /// GROUP BY expressions
    group_by_regs: Vec<i32>,
    /// Expanded column names (populated during compile)
    result_column_names: Vec<String>,
    /// Result column aliases mapped to their register (for ORDER BY alias resolution)
    result_aliases: HashMap<String, i32>,
    /// Result column alias expressions (for WHERE clause alias resolution)
    alias_expressions: HashMap<String, Expr>,
    /// Schema for name resolution (optional)
    schema: Option<&'s crate::schema::Schema>,
    /// Temp schema for name resolution (optional, for TEMP tables/views)
    temp_schema: Option<&'s crate::schema::Schema>,
    /// Attached database schemas (name, schema) in attach order
    attached_schemas: Vec<(String, &'s crate::schema::Schema)>,
    /// Register holding the remaining LIMIT counter (None if no limit)
    limit_counter_reg: Option<i32>,
    /// Register holding the remaining OFFSET counter (None if no offset)
    offset_counter_reg: Option<i32>,
    /// Label to jump to when LIMIT is exhausted
    limit_done_label: Option<i32>,
    /// ORDER BY terms (when outputting to sorter)
    order_by_terms: Option<Vec<OrderingTerm>>,
    /// Finalized aggregate result registers (for nested aggregate expressions)
    agg_final_regs: Vec<i32>,
    /// Current index into agg_final_regs when compiling expressions
    agg_final_idx: usize,
    /// GROUP BY column registers for substitution during finalization
    /// Maps column name (lowercase) to register containing the group key value
    group_column_regs: HashMap<String, i32>,
    /// Non-aggregate result column saved registers for GROUP BY finalization
    /// (base_reg, indices) where indices[i] is Some(offset) if result column i is non-agg
    /// base_reg + offset gives the register holding the saved non-agg value
    non_agg_saved_regs: Option<(i32, Vec<Option<usize>>)>,
    /// Saved column registers for aggregate queries (simple aggregate mode)
    /// Maps (cursor, col_idx) to register containing saved value
    /// Used to substitute column reads after cursor has moved past end
    saved_column_regs: Option<HashMap<(i32, i32), i32>>,
    /// Number of columns in compound select (for UNION, INTERSECT, EXCEPT output)
    compound_column_count: usize,
    /// Aliases from compound SELECT parts (for ORDER BY resolution)
    /// Maps alias name to column position (0-based)
    compound_aliases: HashMap<String, usize>,
    /// PRAGMA short_column_names (default ON) - use just column name
    short_column_names: bool,
    /// PRAGMA full_column_names (default OFF) - use table.column format
    full_column_names: bool,
    /// Counter for anonymous subquery naming (subquery-0, subquery-1, etc.)
    next_subquery: usize,
    /// Join conditions collected from ON/USING/NATURAL in FROM clause
    /// These are merged with WHERE clause during compilation
    join_conditions: Vec<Expr>,
    /// Columns to exclude from * expansion for each table (for NATURAL/USING coalescing)
    /// Key: table index, Value: set of column names to exclude
    coalesced_columns: HashMap<usize, std::collections::HashSet<String>>,
    /// Index where outer (correlation context) tables end and local tables begin.
    /// Tables at index < outer_tables_boundary are from outer queries and should not be looped over.
    /// Tables at index >= outer_tables_boundary are local to this query and should be looped.
    outer_tables_boundary: usize,
    /// Map from table cursor to index cursor (for index scans)
    index_cursors: HashMap<i32, i32>,
    /// Cached query plan from WHERE clause analysis
    where_info: Option<WhereInfo>,
    /// Parameter names for Variable compilation (from prepare.rs extract_parameters)
    param_names: Vec<Option<String>>,
    /// Counter for unnamed parameters (?) during compilation
    next_unnamed_param: i32,
    /// LIKE case sensitivity (for LIKE index optimization)
    case_sensitive_like: bool,
    /// Aliases currently being resolved (to detect infinite recursion)
    resolving_aliases: HashSet<String>,
    /// (table_name, index_name) that can satisfy ORDER BY (set by check_order_by_satisfied)
    order_by_index: Option<(String, String)>,
    /// Views currently being expanded (to detect circular view definitions)
    expanding_views: HashSet<String>,
    /// Depth of main database view expansions (when > 0, don't look in temp schema for views)
    /// This implements SQLite's behavior where views bind to their own database's objects
    main_view_depth: usize,
    /// Enable view access (from db config enable_view)
    enable_view: bool,
    /// Virtual table registry for xBestIndex calls
    vtab_registry: Option<std::sync::Arc<VtabRegistry>>,
}

impl<'s> SelectCompiler<'s> {
    /// Create a new SELECT compiler
    pub fn new() -> Self {
        Self {
            ops: Vec::new(),
            next_reg: 1,
            next_cursor: 0,
            tables: Vec::new(),
            columns: Vec::new(),
            next_label: 0,
            labels: HashMap::new(),
            ctes: HashMap::new(),
            recursive_ctes: HashSet::new(),
            cte_cursors: HashMap::new(),
            is_compound: false,
            has_aggregates: false,
            has_window_functions: false,
            group_by_regs: Vec::new(),
            result_column_names: Vec::new(),
            result_aliases: HashMap::new(),
            alias_expressions: HashMap::new(),
            schema: None,
            temp_schema: None,
            attached_schemas: Vec::new(),
            limit_counter_reg: None,
            offset_counter_reg: None,
            limit_done_label: None,
            order_by_terms: None,
            agg_final_regs: Vec::new(),
            agg_final_idx: 0,
            group_column_regs: HashMap::new(),
            non_agg_saved_regs: None,
            saved_column_regs: None,
            compound_column_count: 0,
            short_column_names: true, // Default ON
            full_column_names: false, // Default OFF
            next_subquery: 0,
            compound_aliases: HashMap::new(),
            join_conditions: Vec::new(),
            coalesced_columns: HashMap::new(),
            outer_tables_boundary: 0,
            index_cursors: HashMap::new(),
            where_info: None,
            param_names: Vec::new(),
            next_unnamed_param: 1,
            case_sensitive_like: false,
            resolving_aliases: HashSet::new(),
            order_by_index: None,
            expanding_views: HashSet::new(),
            main_view_depth: 0,
            enable_view: true,
            vtab_registry: None,
        }
    }

    /// Set enable_view flag (from db config)
    pub fn set_enable_view(&mut self, enable: bool) {
        self.enable_view = enable;
    }

    /// Set starting cursor number (to avoid conflicts with pre-allocated cursors)
    pub fn set_next_cursor(&mut self, cursor: i32) {
        self.next_cursor = cursor;
    }

    /// Set parameter names for Variable compilation
    pub fn set_param_names(&mut self, param_names: Vec<Option<String>>) {
        self.param_names = param_names;
    }

    /// Create a new SELECT compiler with schema access
    pub fn with_schema(schema: &'s crate::schema::Schema) -> Self {
        Self {
            ops: Vec::new(),
            next_reg: 1,
            next_cursor: 0,
            tables: Vec::new(),
            columns: Vec::new(),
            next_label: 0,
            labels: HashMap::new(),
            ctes: HashMap::new(),
            recursive_ctes: HashSet::new(),
            cte_cursors: HashMap::new(),
            is_compound: false,
            has_aggregates: false,
            has_window_functions: false,
            group_by_regs: Vec::new(),
            result_column_names: Vec::new(),
            result_aliases: HashMap::new(),
            alias_expressions: HashMap::new(),
            schema: Some(schema),
            temp_schema: None,
            attached_schemas: Vec::new(),
            limit_counter_reg: None,
            offset_counter_reg: None,
            limit_done_label: None,
            order_by_terms: None,
            agg_final_regs: Vec::new(),
            agg_final_idx: 0,
            group_column_regs: HashMap::new(),
            non_agg_saved_regs: None,
            saved_column_regs: None,
            compound_column_count: 0,
            short_column_names: true, // Default ON
            full_column_names: false, // Default OFF
            next_subquery: 0,
            compound_aliases: HashMap::new(),
            join_conditions: Vec::new(),
            coalesced_columns: HashMap::new(),
            outer_tables_boundary: 0,
            index_cursors: HashMap::new(),
            where_info: None,
            param_names: Vec::new(),
            next_unnamed_param: 1,
            case_sensitive_like: false,
            resolving_aliases: HashSet::new(),
            order_by_index: None,
            expanding_views: HashSet::new(),
            main_view_depth: 0,
            enable_view: true,
            vtab_registry: None,
        }
    }

    /// Set LIKE case sensitivity for index optimization
    pub fn set_case_sensitive_like(&mut self, value: bool) {
        self.case_sensitive_like = value;
    }

    /// Set the temp schema for TEMP tables/views lookup
    pub fn set_temp_schema(&mut self, temp_schema: &'s crate::schema::Schema) {
        self.temp_schema = Some(temp_schema);
    }

    /// Set the virtual table registry for xBestIndex calls
    pub fn set_vtab_registry(&mut self, registry: std::sync::Arc<VtabRegistry>) {
        self.vtab_registry = Some(registry);
    }

    pub fn set_attached_schemas(&mut self, schemas: Vec<(String, &'s crate::schema::Schema)>) {
        self.attached_schemas = schemas;
    }

    /// Set column naming flags from PRAGMA settings
    pub fn set_column_name_flags(&mut self, short_column_names: bool, full_column_names: bool) {
        self.short_column_names = short_column_names;
        self.full_column_names = full_column_names;
    }

    /// Set the starting register and cursor numbers
    /// Used when inlining subqueries to avoid register/cursor conflicts
    pub fn set_register_base(&mut self, next_reg: i32, next_cursor: i32) {
        self.next_reg = next_reg;
        self.next_cursor = next_cursor;
    }

    /// Add an outer table for correlated subquery support in DELETE/UPDATE
    /// The cursor must already be open and positioned on a row.
    /// After calling this, the table will be available for column resolution
    /// in subsequent compile() calls, but won't be looped over.
    pub fn add_outer_table(
        &mut self,
        name: String,
        table_name: String,
        cursor: i32,
        schema_table: Option<std::sync::Arc<crate::schema::Table>>,
    ) {
        self.tables.push(TableInfo {
            name,
            table_name,
            cursor,
            schema_table,
            is_subquery: false,
            join_type: crate::parser::ast::JoinType::empty(),
            subquery_columns: None,
        });
        // All tables added via this method are outer tables (for correlation)
        // Update boundary so these tables are used for column resolution but not looped
        self.outer_tables_boundary = self.tables.len();
    }

    /// Get the expanded column names after compilation
    pub fn column_names(&self) -> &[String] {
        &self.result_column_names
    }

    /// Convert a schema expression to a parser AST expression
    /// This is used for evaluating generated column expressions at compile time
    fn convert_schema_expr_to_ast(schema_expr: &crate::schema::Expr) -> Expr {
        use crate::schema::Expr as SchemaExpr;
        match schema_expr {
            SchemaExpr::Null => Expr::Literal(Literal::Null),
            SchemaExpr::Integer(i) => Expr::Literal(Literal::Integer(*i)),
            SchemaExpr::Real(f) => Expr::Literal(Literal::Float(*f)),
            SchemaExpr::String(s) => Expr::Literal(Literal::String(s.clone())),
            SchemaExpr::Blob(b) => Expr::Literal(Literal::Blob(b.clone())),
            SchemaExpr::Column { table, column } => Expr::Column(ColumnRef {
                database: None,
                table: table.clone(),
                column: column.clone(),
                column_index: None,
                source_text: None,
            }),
            SchemaExpr::BinaryOp { left, op, right } => {
                // Handle GLOB/MATCH/REGEXP as Like expressions, not binary ops
                match op {
                    crate::schema::BinaryOp::Glob => {
                        return Expr::Like {
                            expr: Box::new(Self::convert_schema_expr_to_ast(left)),
                            pattern: Box::new(Self::convert_schema_expr_to_ast(right)),
                            escape: None,
                            op: LikeOp::Glob,
                            negated: false,
                        };
                    }
                    crate::schema::BinaryOp::Match => {
                        return Expr::Like {
                            expr: Box::new(Self::convert_schema_expr_to_ast(left)),
                            pattern: Box::new(Self::convert_schema_expr_to_ast(right)),
                            escape: None,
                            op: LikeOp::Match,
                            negated: false,
                        };
                    }
                    crate::schema::BinaryOp::Regexp => {
                        return Expr::Like {
                            expr: Box::new(Self::convert_schema_expr_to_ast(left)),
                            pattern: Box::new(Self::convert_schema_expr_to_ast(right)),
                            escape: None,
                            op: LikeOp::Regexp,
                            negated: false,
                        };
                    }
                    _ => {}
                }
                let ast_op = match op {
                    crate::schema::BinaryOp::Add => BinaryOp::Add,
                    crate::schema::BinaryOp::Sub => BinaryOp::Sub,
                    crate::schema::BinaryOp::Mul => BinaryOp::Mul,
                    crate::schema::BinaryOp::Div => BinaryOp::Div,
                    crate::schema::BinaryOp::Mod => BinaryOp::Mod,
                    crate::schema::BinaryOp::Concat => BinaryOp::Concat,
                    crate::schema::BinaryOp::Eq => BinaryOp::Eq,
                    crate::schema::BinaryOp::Ne => BinaryOp::Ne,
                    crate::schema::BinaryOp::Lt => BinaryOp::Lt,
                    crate::schema::BinaryOp::Le => BinaryOp::Le,
                    crate::schema::BinaryOp::Gt => BinaryOp::Gt,
                    crate::schema::BinaryOp::Ge => BinaryOp::Ge,
                    crate::schema::BinaryOp::And => BinaryOp::And,
                    crate::schema::BinaryOp::Or => BinaryOp::Or,
                    crate::schema::BinaryOp::BitAnd => BinaryOp::BitAnd,
                    crate::schema::BinaryOp::BitOr => BinaryOp::BitOr,
                    crate::schema::BinaryOp::LeftShift => BinaryOp::ShiftLeft,
                    crate::schema::BinaryOp::RightShift => BinaryOp::ShiftRight,
                    crate::schema::BinaryOp::Is => BinaryOp::Is,
                    crate::schema::BinaryOp::IsNot => BinaryOp::IsNot,
                    // These were handled above
                    crate::schema::BinaryOp::Glob
                    | crate::schema::BinaryOp::Match
                    | crate::schema::BinaryOp::Regexp => unreachable!(),
                };
                Expr::Binary {
                    op: ast_op,
                    left: Box::new(Self::convert_schema_expr_to_ast(left)),
                    right: Box::new(Self::convert_schema_expr_to_ast(right)),
                }
            }
            SchemaExpr::UnaryOp { op, operand } => {
                let ast_op = match op {
                    crate::schema::UnaryOp::Neg => crate::parser::ast::UnaryOp::Neg,
                    crate::schema::UnaryOp::Not => crate::parser::ast::UnaryOp::Not,
                    crate::schema::UnaryOp::BitNot => crate::parser::ast::UnaryOp::BitNot,
                    crate::schema::UnaryOp::Plus => crate::parser::ast::UnaryOp::Pos,
                };
                Expr::Unary {
                    op: ast_op,
                    expr: Box::new(Self::convert_schema_expr_to_ast(operand)),
                }
            }
            SchemaExpr::Function {
                name,
                args,
                distinct,
            } => Expr::Function(crate::parser::ast::FunctionCall {
                name: name.clone(),
                args: crate::parser::ast::FunctionArgs::Exprs(
                    args.iter().map(Self::convert_schema_expr_to_ast).collect(),
                ),
                distinct: *distinct,
                filter: None,
                over: None,
            }),
            SchemaExpr::Case {
                operand,
                when_clauses,
                else_clause,
            } => Expr::Case {
                operand: operand
                    .as_ref()
                    .map(|e| Box::new(Self::convert_schema_expr_to_ast(e))),
                when_clauses: when_clauses
                    .iter()
                    .map(|(w, t)| crate::parser::ast::WhenClause {
                        when: Box::new(Self::convert_schema_expr_to_ast(w)),
                        then: Box::new(Self::convert_schema_expr_to_ast(t)),
                    })
                    .collect(),
                else_clause: else_clause
                    .as_ref()
                    .map(|e| Box::new(Self::convert_schema_expr_to_ast(e))),
            },
            SchemaExpr::Cast { expr, type_name } => Expr::Cast {
                expr: Box::new(Self::convert_schema_expr_to_ast(expr)),
                type_name: crate::parser::ast::TypeName {
                    name: type_name.clone(),
                    args: Vec::new(),
                },
            },
            SchemaExpr::In {
                expr,
                list,
                negated,
            } => Expr::In {
                expr: Box::new(Self::convert_schema_expr_to_ast(expr)),
                list: crate::parser::ast::InList::Values(
                    list.iter().map(Self::convert_schema_expr_to_ast).collect(),
                ),
                negated: *negated,
            },
            SchemaExpr::Between {
                expr,
                low,
                high,
                negated,
            } => Expr::Between {
                expr: Box::new(Self::convert_schema_expr_to_ast(expr)),
                low: Box::new(Self::convert_schema_expr_to_ast(low)),
                high: Box::new(Self::convert_schema_expr_to_ast(high)),
                negated: *negated,
            },
            SchemaExpr::Like {
                expr,
                pattern,
                escape,
                op,
                negated,
            } => Expr::Like {
                expr: Box::new(Self::convert_schema_expr_to_ast(expr)),
                pattern: Box::new(Self::convert_schema_expr_to_ast(pattern)),
                escape: escape
                    .as_ref()
                    .map(|e| Box::new(Self::convert_schema_expr_to_ast(e))),
                op: *op,
                negated: *negated,
            },
            SchemaExpr::IsNull { expr, negated } => Expr::IsNull {
                expr: Box::new(Self::convert_schema_expr_to_ast(expr)),
                negated: *negated,
            },
            SchemaExpr::Collate { expr, collation } => Expr::Collate {
                expr: Box::new(Self::convert_schema_expr_to_ast(expr)),
                collation: collation.clone(),
            },
            SchemaExpr::Parameter { index, name } => {
                if let Some(n) = name {
                    // Named parameter - use ':' as default prefix
                    Expr::Variable(crate::parser::ast::Variable::Named {
                        prefix: ':',
                        name: n.clone(),
                    })
                } else {
                    // Numbered parameter
                    Expr::Variable(crate::parser::ast::Variable::Numbered(*index))
                }
            }
            SchemaExpr::CurrentTime => Expr::Literal(Literal::CurrentTime),
            SchemaExpr::CurrentDate => Expr::Literal(Literal::CurrentDate),
            SchemaExpr::CurrentTimestamp => Expr::Literal(Literal::CurrentTimestamp),
            // Subquery and Exists not typically used in generated columns
            SchemaExpr::Subquery(_) | SchemaExpr::Exists { .. } => Expr::Literal(Literal::Null),
        }
    }

    /// Compile a SELECT statement
    pub fn compile(&mut self, select: &SelectStmt, dest: &SelectDest) -> Result<Vec<VdbeOp>> {
        // Emit Init opcode at address 0 - will jump to Goto at the end
        // SQLite pattern: Init jumps past main code to setup section
        let init_label = self.alloc_label();
        self.emit(Opcode::Init, 0, init_label, 0, P4::Unused);

        // Handle WITH clause (CTEs)
        if let Some(with) = &select.with {
            self.process_with_clause(with)?;
        }

        // Check compound SELECT term limit
        let compound_terms = Self::count_compound_terms(&select.body);
        if SQLITE_MAX_COMPOUND_SELECT > 0 && compound_terms > SQLITE_MAX_COMPOUND_SELECT as usize {
            return Err(Error::with_message(
                ErrorCode::Error,
                "too many terms in compound SELECT".to_string(),
            ));
        }

        // Check for aggregates in ORDER BY without GROUP BY
        // Note: For compound SELECT, we skip this check because:
        // 1. The ORDER BY references result columns from the compound result
        // 2. Aggregates in ORDER BY match expressions from the left SELECT's result columns
        if let Some(order_by) = &select.order_by {
            let should_check = match &select.body {
                SelectBody::Select(core) => !core.group_by.is_some(),
                // For compound SELECT, don't check - ORDER BY aggregates reference result columns
                SelectBody::Compound { .. } => false,
            };
            if should_check {
                for term in order_by {
                    if let Some(agg_name) = self.find_aggregate_in_expr(&term.expr) {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("misuse of aggregate: {}()", agg_name),
                        ));
                    }
                }
            }
        }

        // Check if this is a simple aggregate query (aggregates without GROUP BY)
        // For such queries, ORDER BY is meaningless since there's only one result row
        let is_simple_aggregate = match &select.body {
            SelectBody::Select(core) => {
                let has_agg = self.check_for_aggregates(core);
                has_agg && core.group_by.is_none()
            }
            SelectBody::Compound { .. } => false,
        };

        // If ORDER BY is present, redirect output to a sorter
        // Skip sorter for simple aggregate queries (only one row, ORDER BY is meaningless)
        // Also skip sorter if ORDER BY is satisfied by an index scan
        let (actual_dest, sorter_cursor, order_by_cols) = if let Some(order_by) = &select.order_by {
            if is_simple_aggregate {
                // Simple aggregate query - ignore ORDER BY
                (dest.clone(), None, None)
            } else {
                // Check if ORDER BY is satisfied by an index scan
                let order_satisfied = match &select.body {
                    SelectBody::Select(core) => self.check_order_by_satisfied(core, order_by),
                    SelectBody::Compound { .. } => false,
                };
                if order_satisfied {
                    // Index scan provides correct order, no sorter needed
                    (dest.clone(), None, None)
                } else {
                    let sorter_cursor = self.alloc_cursor();
                    let num_cols = order_by.len();
                    // Sort directions (true=DESC, false=ASC)
                    let sort_orders: Vec<bool> = order_by
                        .iter()
                        .map(|t| t.order == SortOrder::Desc)
                        .collect();
                    // Extract collation from each ORDER BY term
                    // If expr has explicit `COLLATE name`, use `name`; otherwise look up column schema collation
                    let collations: Vec<String> = order_by
                        .iter()
                        .map(|t| self.extract_collation_from_expr(&t.expr, Some(&select.body)))
                        .collect();

                    // Check if any custom collations are used
                    let has_custom_collation = collations.iter().any(|c| c != "BINARY");

                    // Open ephemeral table for sorting
                    if has_custom_collation {
                        // Use KeyInfo when custom collations are present
                        use crate::vdbe::ops::KeyInfo;
                        use std::sync::Arc;
                        let key_info = Arc::new(KeyInfo {
                            collations,
                            sort_orders,
                            n_key_field: num_cols as u16,
                        });
                        self.emit(
                            Opcode::OpenEphemeral,
                            sorter_cursor,
                            num_cols as i32,
                            0,
                            P4::KeyInfo(key_info),
                        );
                    } else {
                        // Use simple blob for sort directions (backwards compatible)
                        let sort_dirs: Vec<u8> =
                            sort_orders.iter().map(|&d| if d { 1 } else { 0 }).collect();
                        self.emit(
                            Opcode::OpenEphemeral,
                            sorter_cursor,
                            num_cols as i32,
                            0,
                            P4::Blob(sort_dirs),
                        );
                    }
                    // Store ORDER BY terms so output_row_inner can include them in records
                    self.order_by_terms = Some(order_by.clone());
                    (
                        SelectDest::Sorter {
                            cursor: sorter_cursor,
                        },
                        Some(sorter_cursor),
                        Some(order_by.clone()),
                    )
                }
            }
        } else {
            (dest.clone(), None, None)
        };

        // Handle LIMIT/OFFSET - only compile for body if there's no ORDER BY.
        // When ORDER BY is present, LIMIT must be applied AFTER sorting.
        if sorter_cursor.is_none() {
            if let Some(limit) = &select.limit {
                self.compile_limit(limit)?;
            }
        }

        // For compound SELECTs with ORDER BY, validate ORDER BY terms BEFORE compilation
        // This must happen before compile_body because the sorter will try to compile
        // ORDER BY expressions and fail with "no such column" instead of the proper error
        if let SelectBody::Compound { .. } = &select.body {
            if let Some(order_by) = &select.order_by {
                // Get column names from ALL parts of the compound SELECT
                // because ORDER BY can reference columns from any part
                let all_column_names = self.get_all_compound_column_names(&select.body);
                // Also get the result column count from the leftmost SELECT (for numeric indices)
                let result_column_count = self.count_select_body_columns(&select.body);
                for (idx, term) in order_by.iter().enumerate() {
                    if let Some(err_msg) = self.validate_order_by_for_compound(
                        &term.expr,
                        &all_column_names,
                        result_column_count,
                        idx,
                    ) {
                        return Err(Error::with_message(ErrorCode::Error, err_msg));
                    }
                }
            }
        }

        // Compile the body with appropriate destination
        self.compile_body(&select.body, &actual_dest)?;

        // For compound SELECTs with ORDER BY, validate that ORDER BY terms match result columns
        // (This is a secondary check after compilation, in case the early check missed something)
        if self.is_compound {
            if let Some(order_by) = &select.order_by {
                for (idx, term) in order_by.iter().enumerate() {
                    if !self.is_valid_compound_order_by_term(&term.expr) {
                        let ordinal = match idx {
                            0 => "1st".to_string(),
                            1 => "2nd".to_string(),
                            2 => "3rd".to_string(),
                            n => format!("{}th", n + 1),
                        };
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!(
                                "{} ORDER BY term does not match any column in the result set",
                                ordinal
                            ),
                        ));
                    }
                }
            }
        }

        // Handle ORDER BY output (after body has populated sorter)
        if let (Some(sorter_cursor), Some(order_by)) = (sorter_cursor, order_by_cols) {
            // When ORDER BY is present, compile LIMIT for the output phase
            if let Some(limit) = &select.limit {
                self.compile_limit(limit)?;
            }
            self.compile_order_by_output(&order_by, sorter_cursor, dest)?;
        }

        // Add Halt opcode
        self.emit(Opcode::Halt, 0, 0, 0, P4::Unused);

        // Add initialization section (SQLite pattern: after Halt, before Goto)
        // Init's label points here
        self.resolve_label(init_label, self.current_addr());

        // Emit Transaction opcode only if the query accesses tables
        // (next_cursor > 0 means cursors were allocated for tables)
        if self.next_cursor > 0 {
            self.emit(Opcode::Transaction, 0, 0, 0, P4::Unused);
        }

        // Goto jumps back to address 1 (first instruction after Init)
        // Mark with p5=0xFFFF to skip label resolution (1 is a literal address, not a label)
        self.emit_with_p5(Opcode::Goto, 0, 1, 0, P4::Unused, 0xFFFF);

        // Resolve all labels
        self.resolve_labels()?;

        Ok(std::mem::take(&mut self.ops))
    }

    /// Process WITH clause
    pub fn process_with_clause(&mut self, with: &WithClause) -> Result<()> {
        for cte in &with.ctes {
            let name_lower = cte.name.to_lowercase();
            if with.recursive {
                self.recursive_ctes.insert(name_lower.clone());
            }
            self.ctes.insert(name_lower, cte.clone());
        }
        Ok(())
    }

    /// Compile SELECT body
    fn compile_body(&mut self, body: &SelectBody, dest: &SelectDest) -> Result<()> {
        match body {
            SelectBody::Select(core) => self.compile_select_core(core, dest),
            SelectBody::Compound { op, left, right } => {
                self.compile_compound(*op, left, right, dest)
            }
        }
    }

    /// Compile a full SELECT statement for use in subqueries (handles ORDER BY/LIMIT)
    /// Unlike compile(), this does not emit Halt or resolve labels
    fn compile_subselect(&mut self, select: &SelectStmt, dest: &SelectDest) -> Result<()> {
        // Handle WITH clause (CTEs)
        if let Some(with) = &select.with {
            self.process_with_clause(with)?;
        }

        // Check if ORDER BY is present
        let (actual_dest, sorter_cursor, order_by_cols) = if let Some(order_by) = &select.order_by {
            let sorter_cursor = self.alloc_cursor();
            let num_cols = order_by.len();
            // Build KeyInfo with sort directions and collations
            let key_info = KeyInfo {
                sort_orders: order_by
                    .iter()
                    .map(|t| t.order == SortOrder::Desc)
                    .collect(),
                collations: order_by
                    .iter()
                    .map(|t| Self::extract_collation(&t.expr))
                    .collect(),
                n_key_field: num_cols as u16,
            };
            // Open ephemeral table for sorting with key info
            self.emit(
                Opcode::OpenEphemeral,
                sorter_cursor,
                num_cols as i32,
                0,
                P4::KeyInfo(std::sync::Arc::new(key_info)),
            );
            // Store ORDER BY terms so output_row_inner can include them in records
            self.order_by_terms = Some(order_by.clone());
            (
                SelectDest::Sorter {
                    cursor: sorter_cursor,
                },
                Some(sorter_cursor),
                Some(order_by.clone()),
            )
        } else {
            (dest.clone(), None, None)
        };

        // Handle LIMIT/OFFSET - only compile for body if there's no ORDER BY
        if sorter_cursor.is_none() {
            if let Some(limit) = &select.limit {
                self.compile_limit(limit)?;
            }
        }

        // Compile the body with appropriate destination
        self.compile_body(&select.body, &actual_dest)?;

        // Handle ORDER BY output (after body has populated sorter)
        if let (Some(sorter_cursor), Some(order_by)) = (sorter_cursor, order_by_cols) {
            // When ORDER BY is present, compile LIMIT for the output phase
            if let Some(limit) = &select.limit {
                self.compile_limit(limit)?;
            }
            self.compile_order_by_output(&order_by, sorter_cursor, dest)?;
        }

        Ok(())
    }

    fn compile_subquery_to_ephemeral(
        &mut self,
        query: &SelectStmt,
        cursor: i32,
        exclude_cte: Option<&str>,
    ) -> Result<Vec<String>> {
        // Create ephemeral table for subquery results
        self.emit(Opcode::OpenEphemeral, cursor, 0, 0, P4::Unused);

        // Compile subquery into ephemeral table
        let subquery_dest = SelectDest::EphemTable { cursor };
        let mut subcompiler = if let Some(schema) = self.schema {
            SelectCompiler::with_schema(schema)
        } else {
            SelectCompiler::new()
        };
        // Propagate temp schema for TEMP tables/views
        if let Some(temp_schema) = self.temp_schema {
            subcompiler.set_temp_schema(temp_schema);
        }
        subcompiler.next_reg = self.next_reg;
        subcompiler.next_cursor = self.next_cursor;
        subcompiler.ctes = self.ctes.clone();
        subcompiler.recursive_ctes = self.recursive_ctes.clone();
        subcompiler.cte_cursors = self.cte_cursors.clone();
        // Propagate expanding_views for circular view detection
        subcompiler.expanding_views = self.expanding_views.clone();
        // Propagate main_view_depth for proper temp schema handling
        subcompiler.main_view_depth = self.main_view_depth;
        // Propagate enable_view flag
        subcompiler.enable_view = self.enable_view;

        // Pass outer tables for correlated subquery support
        // FROM subqueries inside scalar subquery contexts (IN, EXISTS, scalar subqueries)
        // may need to reference columns from the outer query's tables
        // This enables patterns like: SELECT x FROM t1 WHERE x IN (SELECT * FROM (SELECT x+1))
        // IMPORTANT: Only pass tables up to outer_tables_boundary (the actual outer tables)
        // Do not pass local tables, as those would cause ambiguity with the subquery's own tables
        for table in self.tables.iter().take(self.outer_tables_boundary) {
            subcompiler.add_outer_table(
                table.name.clone(),
                table.table_name.clone(),
                table.cursor,
                table.schema_table.clone(),
            );
        }

        if let Some(name) = exclude_cte {
            subcompiler.ctes.remove(name);
            subcompiler.recursive_ctes.remove(name);
            subcompiler.cte_cursors.remove(name);
        }
        // Pass column naming settings to subquery compiler
        subcompiler.set_column_name_flags(self.short_column_names, self.full_column_names);
        let subquery_ops = subcompiler.compile(query, &subquery_dest)?;

        // Capture subquery result column names for * expansion
        let subquery_col_names = subcompiler.result_column_names.clone();

        // Inline the subquery ops (skip Init, Halt, Transaction, Goto wrapper)
        // Adjust jump addresses by the current offset
        // Mark inlined jump ops so resolve_labels doesn't reprocess them
        let offset = self.ops.len() as i32;

        // Identify which instructions to skip:
        // - Init (always at address 0)
        // - Halt (usually right before Transaction/Goto footer)
        // - Transaction (part of footer)
        // - The final Goto that jumps back to start (wrapper, not control-flow Goto)
        // Control-flow Gotos within the query should be kept.
        let len = subquery_ops.len();
        let mut skip_indices = std::collections::HashSet::new();

        // Skip Init at 0
        if !subquery_ops.is_empty() && subquery_ops[0].opcode == Opcode::Init {
            skip_indices.insert(0);
        }

        // Skip footer: typically Halt, Transaction, Goto at the end
        // Work backwards from the end to find these
        for i in (0..len).rev() {
            let op = &subquery_ops[i];
            if op.opcode == Opcode::Halt
                || op.opcode == Opcode::Transaction
                || (op.opcode == Opcode::Goto && i >= len.saturating_sub(3))
            {
                skip_indices.insert(i);
            } else {
                // Stop when we hit a non-wrapper instruction
                break;
            }
        }

        // Build address mapping: old_addr -> new_addr
        // Skipped instructions get mapped to the end of the inlined section
        let mut addr_map: Vec<i32> = Vec::with_capacity(len);
        let mut new_addr = offset;
        for i in 0..len {
            if skip_indices.contains(&i) {
                // Skipped instruction - will map to end later
                addr_map.push(-1);
            } else {
                addr_map.push(new_addr);
                new_addr += 1;
            }
        }

        // Calculate end address for the inlined section
        let inlined_end = new_addr;

        // Fix up -1 entries (skipped instructions) to point to end
        for addr in &mut addr_map {
            if *addr == -1 {
                *addr = inlined_end;
            }
        }

        for (old_addr, mut op) in subquery_ops.into_iter().enumerate() {
            // Skip wrapper instructions only
            if !skip_indices.contains(&old_addr) {
                // Adjust P2 for jump instructions using the address map
                if op.opcode.is_jump() {
                    let target = op.p2 as usize;
                    if target < addr_map.len() {
                        op.p2 = addr_map[target];
                    } else {
                        // Jump beyond subquery - point to end
                        op.p2 = inlined_end;
                    }
                    // Use P5 = 0xFFFF to mark as already resolved so resolve_labels skips it
                    op.p5 = 0xFFFF;
                }
                self.ops.push(op);
            }
        }

        self.next_reg = subcompiler.next_reg;
        self.next_cursor = subcompiler.next_cursor;

        Ok(subquery_col_names)
    }

    fn compile_recursive_cte(
        &mut self,
        cte: &CommonTableExpr,
        cte_cursor: i32,
        name_lower: &str,
    ) -> Result<Vec<String>> {
        let SelectBody::Compound { op, left, right } = &cte.query.body else {
            return Err(Error::with_message(
                ErrorCode::Error,
                "recursive CTE requires a compound SELECT",
            ));
        };

        let distinct = match op {
            CompoundOp::Union => true,
            CompoundOp::UnionAll => false,
            _ => {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "recursive CTE requires UNION or UNION ALL",
                ));
            }
        };

        let seed_select = SelectStmt {
            with: None,
            body: (*left.clone()).clone(),
            order_by: None,
            limit: None,
        };

        let recursive_select = SelectStmt {
            with: None,
            body: (*right.clone()).clone(),
            order_by: cte.query.order_by.clone(),
            limit: cte.query.limit.clone(),
        };

        let work_cursor = self.alloc_cursor();
        let queue_cursor = self.alloc_cursor();
        let next_cursor = self.alloc_cursor();

        self.emit(Opcode::OpenEphemeral, cte_cursor, 0, 0, P4::Unused);
        self.emit(Opcode::OpenEphemeral, work_cursor, 0, 0, P4::Unused);
        self.emit(Opcode::OpenEphemeral, queue_cursor, 0, 0, P4::Unused);

        let limit_reg = if let Some(limit) = &cte.query.limit {
            let reg = self.alloc_reg();
            self.compile_expr(&limit.limit, reg)?;
            Some(reg)
        } else {
            None
        };

        let offset_reg = if let Some(limit) = &cte.query.limit {
            if let Some(offset) = &limit.offset {
                let reg = self.alloc_reg();
                self.compile_expr(offset, reg)?;
                Some(reg)
            } else {
                None
            }
        } else {
            None
        };

        let done_label = self.alloc_label();

        let seed_columns =
            self.compile_subquery_to_ephemeral(&seed_select, next_cursor, Some(name_lower))?;
        let column_names = if let Some(explicit) = &cte.columns {
            if explicit.len() != seed_columns.len() {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!(
                        "table {} has {} values for {} columns",
                        cte.name,
                        seed_columns.len(),
                        explicit.len()
                    ),
                ));
            }
            explicit.clone()
        } else {
            seed_columns
        };

        let column_count = column_names.len();

        self.emit_recursive_cte_process_cursor(
            next_cursor,
            work_cursor,
            queue_cursor,
            cte_cursor,
            column_count,
            distinct,
            limit_reg,
            offset_reg,
            done_label,
        )?;

        let loop_start_label = self.alloc_label();
        self.resolve_label(loop_start_label, self.current_addr());
        self.emit(Opcode::Rewind, queue_cursor, done_label, 0, P4::Unused);

        // Clear next_cursor at the start of each iteration to avoid accumulating
        // rows from all previous iterations. Each iteration should only see
        // the rows produced by THIS iteration's recursive SELECT.
        self.emit(Opcode::OpenEphemeral, next_cursor, 0, 0, P4::Unused);

        let mut subcompiler = if let Some(schema) = self.schema {
            SelectCompiler::with_schema(schema)
        } else {
            SelectCompiler::new()
        };
        subcompiler.next_reg = self.next_reg;
        subcompiler.next_cursor = self.next_cursor;
        subcompiler.ctes = self.ctes.clone();
        subcompiler.recursive_ctes = self.recursive_ctes.clone();
        subcompiler.cte_cursors = HashMap::new();
        subcompiler
            .cte_cursors
            .insert(name_lower.to_string(), (queue_cursor, column_names.clone()));
        subcompiler.set_column_name_flags(self.short_column_names, self.full_column_names);
        let recursive_ops = subcompiler.compile(
            &recursive_select,
            &SelectDest::EphemTable {
                cursor: next_cursor,
            },
        )?;

        let recursive_cols = subcompiler.result_column_names.clone();
        if recursive_cols.len() != column_count {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!(
                    "table {} has {} values for {} columns",
                    cte.name,
                    recursive_cols.len(),
                    column_count
                ),
            ));
        }

        // Use the same sophisticated approach as compile_subquery_to_ephemeral
        // Identify wrapper instructions to skip (Init at start, Halt/Transaction/Goto at end)
        let len = recursive_ops.len();
        let mut skip_indices = std::collections::HashSet::new();

        // Skip Init at 0
        if !recursive_ops.is_empty() && recursive_ops[0].opcode == Opcode::Init {
            skip_indices.insert(0);
        }

        // Skip footer: Halt, Transaction, Goto at the end (working backwards)
        for i in (0..len).rev() {
            let op = &recursive_ops[i];
            if op.opcode == Opcode::Halt
                || op.opcode == Opcode::Transaction
                || (op.opcode == Opcode::Goto && i >= len.saturating_sub(3))
            {
                skip_indices.insert(i);
            } else {
                break;
            }
        }

        // Build address mapping: old_addr -> new_addr
        let offset = self.ops.len() as i32;
        let mut addr_map: Vec<i32> = Vec::with_capacity(len);
        let mut new_addr = offset;
        for i in 0..len {
            if skip_indices.contains(&i) {
                addr_map.push(-1);
            } else {
                addr_map.push(new_addr);
                new_addr += 1;
            }
        }

        // Calculate end address for the inlined section
        let inlined_end = new_addr;

        // Fix up -1 entries (skipped instructions) to point to end
        for addr in &mut addr_map {
            if *addr == -1 {
                *addr = inlined_end;
            }
        }

        for (old_addr, mut op) in recursive_ops.into_iter().enumerate() {
            if !skip_indices.contains(&old_addr) {
                // Adjust P2 for jump instructions using the address map
                if op.opcode.is_jump() {
                    let target = op.p2 as usize;
                    if target < addr_map.len() {
                        op.p2 = addr_map[target];
                    } else {
                        op.p2 = inlined_end;
                    }
                    op.p5 = 0xFFFF;
                }
                self.ops.push(op);
            }
        }
        self.next_reg = subcompiler.next_reg;
        self.next_cursor = subcompiler.next_cursor;

        self.emit(Opcode::OpenEphemeral, queue_cursor, 0, 0, P4::Unused);
        self.emit_recursive_cte_process_cursor(
            next_cursor,
            work_cursor,
            queue_cursor,
            cte_cursor,
            column_count,
            distinct,
            limit_reg,
            offset_reg,
            done_label,
        )?;
        self.emit(Opcode::Goto, 0, loop_start_label, 0, P4::Unused);

        self.resolve_label(done_label, self.current_addr());

        Ok(column_names)
    }

    fn emit_recursive_cte_process_cursor(
        &mut self,
        src_cursor: i32,
        work_cursor: i32,
        queue_cursor: i32,
        output_cursor: i32,
        column_count: usize,
        distinct: bool,
        limit_reg: Option<i32>,
        offset_reg: Option<i32>,
        done_label: i32,
    ) -> Result<()> {
        let done = self.alloc_label();
        self.emit(Opcode::Rewind, src_cursor, done, 0, P4::Unused);

        let loop_label = self.alloc_label();
        self.resolve_label(loop_label, self.current_addr());

        let base_reg = self.next_reg;
        for _ in 0..column_count {
            let reg = self.alloc_reg();
            self.emit(Opcode::Column, src_cursor, reg - base_reg, reg, P4::Unused);
        }

        let record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            base_reg,
            column_count as i32,
            record_reg,
            P4::Unused,
        );

        let skip_label = self.alloc_label();
        if distinct {
            self.emit(
                Opcode::Found,
                work_cursor,
                skip_label,
                record_reg,
                P4::Unused,
            );
        }

        let work_rowid = self.alloc_reg();
        self.emit(Opcode::NewRowid, work_cursor, work_rowid, 0, P4::Unused);
        self.emit(
            Opcode::Insert,
            work_cursor,
            record_reg,
            work_rowid,
            P4::Unused,
        );

        let queue_rowid = self.alloc_reg();
        self.emit(Opcode::NewRowid, queue_cursor, queue_rowid, 0, P4::Unused);
        self.emit(
            Opcode::Insert,
            queue_cursor,
            record_reg,
            queue_rowid,
            P4::Unused,
        );

        let after_output = self.alloc_label();
        if let Some(offset_reg) = offset_reg {
            let after_offset = self.alloc_label();
            self.emit(Opcode::IfNot, offset_reg, after_offset, 0, P4::Unused);
            self.emit(Opcode::AddImm, offset_reg, -1, 0, P4::Unused);
            self.emit(Opcode::Goto, 0, after_output, 0, P4::Unused);
            self.resolve_label(after_offset, self.current_addr());
        }

        if let Some(limit_reg) = limit_reg {
            self.emit(Opcode::IfNot, limit_reg, done_label, 0, P4::Unused);
        }

        let out_rowid = self.alloc_reg();
        self.emit(Opcode::NewRowid, output_cursor, out_rowid, 0, P4::Unused);
        self.emit(
            Opcode::Insert,
            output_cursor,
            record_reg,
            out_rowid,
            P4::Unused,
        );

        if let Some(limit_reg) = limit_reg {
            self.emit(Opcode::AddImm, limit_reg, -1, 0, P4::Unused);
        }

        self.resolve_label(after_output, self.current_addr());
        self.resolve_label(skip_label, self.current_addr());
        self.emit(Opcode::Next, src_cursor, loop_label, 0, P4::Unused);
        self.resolve_label(done, self.current_addr());

        Ok(())
    }

    /// Compile a simple SELECT (not compound)
    fn compile_select_core(&mut self, core: &SelectCore, dest: &SelectDest) -> Result<()> {
        // Check for aggregates and window functions
        self.has_aggregates = self.check_for_aggregates(core);
        self.has_window_functions = select_has_window_functions(core);

        // Validate no nested aggregates (e.g., SUM(min(f1)))
        self.validate_no_nested_aggregates(&core.columns)?;

        // Validate no aggregate aliases used in WHERE clause
        self.validate_no_aggregate_aliases_in_where(core.where_clause.as_deref(), &core.columns)?;

        // Validate no aggregate aliases used inside aggregates in HAVING clause
        self.validate_no_aggregate_alias_in_having_aggregate(
            core.having.as_deref(),
            &core.columns,
        )?;

        // Pre-scan result columns to extract alias expressions (needed for ON clause validation)
        // This allows checking if ON clause references aliases that derive from tables to the right
        self.prescan_result_aliases(&core.columns);

        // Process FROM clause - open cursors
        if let Some(from) = &core.from {
            self.compile_from_clause(from)?;
        }

        // Generate the main query loop
        if self.has_window_functions {
            self.compile_with_window_functions(core, dest)
        } else if core.group_by.is_some() {
            // GROUP BY always uses grouped aggregate, even without aggregate functions
            // (returns one row per group)
            self.compile_grouped_aggregate(core, dest)
        } else if self.has_aggregates {
            self.compile_simple_aggregate(core, dest)
        } else {
            self.compile_simple_select(core, dest)
        }
    }

    /// Compile a simple SELECT without aggregates
    fn compile_simple_select(&mut self, core: &SelectCore, dest: &SelectDest) -> Result<()> {
        // Pre-scan result columns to extract alias expressions (for WHERE clause alias resolution)
        self.prescan_result_aliases(&core.columns);

        let (fts3_filter, original_where) = match core.where_clause.as_deref() {
            Some(expr) => self.split_virtual_filter(expr),
            None => (None, None),
        };

        // Keep a copy of the original WHERE clause (before join conditions are merged)
        // This is needed for LEFT JOIN null-fill rows: the null-fill row should only be
        // filtered by the original WHERE clause, not by the join conditions.
        // Join conditions determine which right table rows match; WHERE filters the final result.
        let original_where_for_null_fill = original_where.clone();

        // Merge join conditions (from NATURAL/USING/ON) with WHERE clause
        // This follows SQLite's approach of adding join conditions to pWhere
        let remaining_where = self.merge_join_conditions(original_where);

        // Check for constant-false WHERE clause (e.g., WHERE 0)
        // In this case, skip all loop generation and return immediately - no rows match
        if self.is_constant_false_where(remaining_where.as_ref()) {
            // Still need to populate result_column_names for subquery column resolution
            // even though we won't output any rows
            self.populate_result_column_names(&core.columns);
            return Ok(());
        }

        // Analyze WHERE clause for index optimization
        // This produces a query plan that may use indexes instead of full scans
        let where_info = self.analyze_query_plan(remaining_where.as_ref())?;

        // Determine if we need DISTINCT processing
        let distinct_cursor = if core.distinct == Distinct::Distinct {
            let cursor = self.alloc_cursor();
            // Open ephemeral table for distinct
            self.emit(Opcode::OpenEphemeral, cursor, 0, 0, P4::Unused);
            Some(cursor)
        } else {
            None
        };

        // Collect table cursors and join types to avoid borrow checker issues
        // Only include local tables (index >= outer_tables_boundary) for loop generation.
        // Outer tables are from enclosing queries and should not be looped over.
        let table_cursors: Vec<i32> = self
            .tables
            .iter()
            .skip(self.outer_tables_boundary)
            .map(|t| t.cursor)
            .collect();
        let table_join_types: Vec<JoinType> = self
            .tables
            .iter()
            .skip(self.outer_tables_boundary)
            .map(|t| t.join_type)
            .collect();

        // Check if any table has an outer join (LEFT/RIGHT/FULL)
        // If so, we cannot freely reorder tables - the outer join constraints must be respected
        // For now, fall back to FROM clause order when outer joins are present
        let has_outer_join = table_join_types.iter().any(|jt| jt.is_outer());

        // Build iteration order - use optimizer's order if available, else FROM clause order
        // The optimizer reorders tables by cost (cheapest first), so info.levels[0] is the
        // table to scan first (outer loop), and the last level is the innermost loop.
        // IMPORTANT: For outer joins, we must preserve FROM clause order to maintain
        // correct LEFT/RIGHT JOIN semantics (left table must be in outer loop).
        let iteration_order: Vec<usize> = if has_outer_join {
            // Outer join present - preserve FROM clause order for correctness
            (0..table_cursors.len()).collect()
        } else if let Some(info) = &where_info {
            if info.levels.len() == table_cursors.len() {
                // Use optimizer's order - level.from_idx maps to table_cursors position
                info.levels
                    .iter()
                    .map(|level| level.from_idx as usize)
                    .collect()
            } else {
                // Incomplete plan - fall back to FROM clause order
                (0..table_cursors.len()).collect()
            }
        } else {
            // No query plan - use FROM clause order
            (0..table_cursors.len()).collect()
        };

        // Generate proper nested loop structure for cross joins
        // For N tables, we need nested Rewind/Next pairs where inner tables
        // get rewound for each row of outer tables.
        //
        // Structure for 2 tables (A, B):
        //   Rewind A → done_all
        // outer_loop:
        //   Rewind B → next_outer
        // inner_loop:
        //   ... body ...
        //   Next B → inner_loop
        // next_outer:
        //   Next A → outer_loop
        // done_all:
        //
        // For LEFT JOIN, we need to emit the outer row even when inner is empty/unmatched:
        //   Rewind A → done_all
        // outer_loop:
        //   Integer 0, found_match_reg    ; initialize "found match" flag
        //   Rewind B → check_match        ; if empty, check if need to emit null row
        // inner_loop:
        //   ... body ...
        //   Integer 1, found_match_reg    ; set "found match"
        //   Next B → inner_loop
        // check_match:
        //   If found_match_reg > 0 → next_outer  ; if matched, skip null output
        //   NullRow B                            ; set B columns to NULL
        //   ... output row ...                   ; output with NULL right columns
        // next_outer:
        //   Next A → outer_loop
        // done_all:
        //
        let mut loop_labels: Vec<i32> = Vec::with_capacity(table_cursors.len());
        let mut next_labels: Vec<i32> = Vec::with_capacity(table_cursors.len());
        let mut found_match_regs: Vec<Option<i32>> = Vec::with_capacity(table_cursors.len());

        // Emit Rewind/loop structure for each table level
        // First, allocate found_match registers for outer joins (but don't emit initialization yet)
        for i in 0..table_cursors.len() {
            // Outer join if LEFT or RIGHT flags are set
            let is_outer_join = table_join_types[i].is_outer();
            let found_match_reg = if is_outer_join && i > 0 {
                Some(self.alloc_reg())
            } else {
                None
            };
            found_match_regs.push(found_match_reg);
        }

        // Track scan metadata for each table (for loop end code)
        // (is_index_scan, index_cursor, key_base_reg, key_count, is_rowid_eq)
        let mut scan_info: Vec<(bool, Option<i32>, i32, i32, bool)> =
            Vec::with_capacity(table_cursors.len());

        // Track range end keys for early termination on upper bound constraints
        // Option<(end_key_reg, key_count, op)> - op is Lt or Le to determine IdxGE vs IdxGT
        let mut range_end_keys: Vec<Option<(i32, i32, TermOp)>> =
            Vec::with_capacity(table_cursors.len());

        // Plan virtual table access using xBestIndex
        // This needs to happen before the loop since plan_virtual_table_access may allocate registers
        // Track both planned access AND which cursors are virtual tables (for full scans)
        let mut vtab_plans: std::collections::HashMap<i32, VFilterPlan> =
            std::collections::HashMap::new();
        let mut vtab_cursors: std::collections::HashSet<i32> = std::collections::HashSet::new();
        for i in self.outer_tables_boundary..self.tables.len() {
            let table = self.tables[i].clone();
            if table.schema_table.as_ref().is_some_and(|t| t.is_virtual) {
                vtab_cursors.insert(table.cursor);
                if let Ok(Some(plan)) =
                    self.plan_virtual_table_access(&table, remaining_where.as_ref())
                {
                    vtab_plans.insert(table.cursor, plan);
                }
            }
        }

        // Now emit the Rewind/loop structure (or index seek structure based on plan)
        // Iterate in optimizer order: iteration_order[loop_pos] gives the FROM clause index
        for (loop_pos, &from_idx) in iteration_order.iter().enumerate() {
            let cursor = table_cursors[from_idx];

            // Handle virtual table filter
            // Virtual tables ALWAYS need VFilter emitted (even for full table scans)
            if let Some(plan) = vtab_plans.remove(&cursor) {
                // Use xBestIndex-planned VFilter with VFilterPlan
                self.emit(Opcode::VFilter, cursor, 0, 0, P4::VFilterPlan(plan));
            } else if let Some(filter) = &fts3_filter {
                // FTS3-specific filter handling (legacy path)
                if filter.cursor == cursor {
                    match &filter.pattern {
                        Expr::Literal(Literal::String(text)) => {
                            self.emit(Opcode::VFilter, cursor, 0, 0, P4::Text(text.clone()));
                        }
                        expr => {
                            let reg = self.alloc_reg();
                            self.compile_expr(expr, reg)?;
                            self.emit(Opcode::VFilter, cursor, reg, 0, P4::Unused);
                        }
                    }
                }
            } else if vtab_cursors.contains(&cursor) {
                // Virtual table full scan (no constraints) - emit VFilter with idx_num=0
                let full_scan_plan = VFilterPlan {
                    idx_num: 0,
                    idx_str: None,
                    constraints: vec![],
                };
                self.emit(
                    Opcode::VFilter,
                    cursor,
                    0,
                    0,
                    P4::VFilterPlan(full_scan_plan),
                );
            }

            // For the outermost table, jump to done_all on empty
            // For inner tables, jump to next_outer (advance outer cursor)
            let skip_label = self.alloc_label();

            // Check if we have a query plan for this table
            // For outer joins, we don't use index scans from the query plan because:
            // 1. The iteration order is forced to FROM clause order (not optimizer order)
            // 2. Index scans on WHERE conditions would pre-filter the inner table,
            //    which breaks LEFT JOIN semantics (WHERE should filter after join)
            // 3. The levels[] index wouldn't match the from_idx anyway
            let plan = if has_outer_join {
                None // Fall back to full scan with runtime filtering
            } else {
                where_info
                    .as_ref()
                    .and_then(|info| info.levels.get(loop_pos))
                    .map(|level| &level.plan)
            };

            match plan {
                Some(WherePlan::IndexScan {
                    index_name,
                    eq_cols,
                    range_end,
                    range_start,
                    ..
                }) if *eq_cols > 0 => {
                    // Index scan with equality constraints
                    let index_cursor = self.alloc_cursor();
                    self.index_cursors.insert(cursor, index_cursor);

                    // Open the index
                    self.emit(
                        Opcode::OpenRead,
                        index_cursor,
                        0,
                        0,
                        P4::Text(index_name.clone()),
                    );

                    // Allocate registers for the index key
                    let key_base_reg = self.next_reg;
                    for _ in 0..*eq_cols {
                        self.alloc_reg();
                    }

                    // Build index key from equality terms in WHERE clause
                    // Find equality expressions and sort by column index to match index order
                    let mut eq_exprs: Vec<(i32, Expr)> = if let Some(info) = &where_info {
                        if let Some(level) = info.levels.get(loop_pos) {
                            self.find_index_equality_terms(info, level, index_name)
                                .into_iter()
                                .map(|(col_idx, expr)| (col_idx, expr.clone()))
                                .collect()
                        } else {
                            Vec::new()
                        }
                    } else {
                        Vec::new()
                    };
                    // Sort by column index to ensure index key is built in correct column order
                    eq_exprs.sort_by_key(|(col_idx, _)| *col_idx);

                    // Compile equality expressions into key registers (in sorted order)
                    for (pos, (_, expr)) in eq_exprs.iter().enumerate() {
                        if pos < *eq_cols as usize {
                            self.compile_expr(expr, key_base_reg + pos as i32)?;
                        }
                    }

                    // Apply column affinities to key registers before MakeRecord
                    // This ensures type coercion (e.g., '111' matches integer 111 in index)
                    if *eq_cols > 0 {
                        if let Some(table_info) = self.tables.get(from_idx) {
                            if let Some(table) = &table_info.schema_table {
                                if let Some(index) =
                                    table.indexes.iter().find(|i| i.name == *index_name)
                                {
                                    let mut affinity_str = String::with_capacity(*eq_cols as usize);
                                    for col in index.columns.iter().take(*eq_cols as usize) {
                                        if col.column_idx >= 0 {
                                            if let Some(col_info) =
                                                table.columns.get(col.column_idx as usize)
                                            {
                                                let ch = match col_info.affinity {
                                                    crate::schema::Affinity::None => '@',
                                                    crate::schema::Affinity::Blob => 'A',
                                                    crate::schema::Affinity::Text => 'B',
                                                    crate::schema::Affinity::Numeric => 'C',
                                                    crate::schema::Affinity::Integer => 'D',
                                                    crate::schema::Affinity::Real => 'E',
                                                    crate::schema::Affinity::Flexnum => 'F',
                                                };
                                                affinity_str.push(ch);
                                            }
                                        }
                                    }
                                    if !affinity_str.is_empty() {
                                        self.emit(
                                            Opcode::Affinity,
                                            key_base_reg,
                                            *eq_cols,
                                            0,
                                            P4::Text(affinity_str),
                                        );
                                    }
                                }
                            }
                        }
                    }

                    // Build seek key - includes range start value if present
                    let (seek_key_reg, seek_key_cols, seek_opcode) =
                        if let Some((_col_idx, op, term_idx)) = range_start {
                            // Build extended key with equality + range start value
                            if let Some(info) = &where_info {
                                if let Some(term) = info.terms.get(*term_idx as usize) {
                                    if let Expr::Binary { right, .. } = term.expr.as_ref() {
                                        // Allocate register for range start value
                                        let range_val_reg = self.alloc_reg();
                                        self.compile_expr(right, range_val_reg)?;

                                        // Create extended key record (eq cols + range start)
                                        let ext_key_reg = self.alloc_reg();
                                        self.emit(
                                            Opcode::MakeRecord,
                                            key_base_reg,
                                            *eq_cols + 1,
                                            ext_key_reg,
                                            P4::Unused,
                                        );

                                        // Use SeekGT for > and SeekGE for >=
                                        let opcode = match op {
                                            TermOp::Gt => Opcode::SeekGT,
                                            _ => Opcode::SeekGE,
                                        };
                                        (ext_key_reg, *eq_cols + 1, opcode)
                                    } else {
                                        // Fallback to equality-only key
                                        let key_reg = self.alloc_reg();
                                        self.emit(
                                            Opcode::MakeRecord,
                                            key_base_reg,
                                            *eq_cols,
                                            key_reg,
                                            P4::Unused,
                                        );
                                        (key_reg, *eq_cols, Opcode::SeekGE)
                                    }
                                } else {
                                    // Fallback to equality-only key
                                    let key_reg = self.alloc_reg();
                                    self.emit(
                                        Opcode::MakeRecord,
                                        key_base_reg,
                                        *eq_cols,
                                        key_reg,
                                        P4::Unused,
                                    );
                                    (key_reg, *eq_cols, Opcode::SeekGE)
                                }
                            } else {
                                // Fallback to equality-only key
                                let key_reg = self.alloc_reg();
                                self.emit(
                                    Opcode::MakeRecord,
                                    key_base_reg,
                                    *eq_cols,
                                    key_reg,
                                    P4::Unused,
                                );
                                (key_reg, *eq_cols, Opcode::SeekGE)
                            }
                        } else {
                            // No range start - use equality-only key
                            let key_reg = self.alloc_reg();
                            self.emit(
                                Opcode::MakeRecord,
                                key_base_reg,
                                *eq_cols,
                                key_reg,
                                P4::Unused,
                            );
                            (key_reg, *eq_cols, Opcode::SeekGE)
                        };

                    // Seek to first matching entry
                    self.emit(
                        seek_opcode,
                        index_cursor,
                        skip_label,
                        seek_key_reg,
                        P4::Int64(seek_key_cols as i64),
                    );
                    next_labels.push(skip_label);

                    // Build range end key BEFORE the loop so the check can be at loop start
                    let range_end_info = if let Some((_col_idx, op, term_idx)) = range_end {
                        // Find the range term's RHS expression
                        if let Some(info) = &where_info {
                            if let Some(term) = info.terms.get(*term_idx as usize) {
                                // The range term expression is col op value, extract the value
                                if let Expr::Binary { right, .. } = term.expr.as_ref() {
                                    // Allocate consecutive registers for end key values
                                    let end_key_base = self.next_reg;
                                    // Copy eq values first - pre-allocate all registers
                                    let copy_regs: Vec<i32> =
                                        (0..*eq_cols).map(|_| self.alloc_reg()).collect();
                                    for (j, dest_reg) in copy_regs.iter().enumerate() {
                                        self.emit(
                                            Opcode::Copy,
                                            key_base_reg + j as i32,
                                            *dest_reg,
                                            0,
                                            P4::Unused,
                                        );
                                    }
                                    // Compile range bound value into next consecutive register
                                    let range_val_reg = self.alloc_reg();
                                    self.compile_expr(right, range_val_reg)?;
                                    // Return base register of consecutive key values (not a record)
                                    Some((end_key_base, *eq_cols + 1, *op))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    // Mark the loop start
                    let loop_label = self.alloc_label();
                    self.resolve_label(loop_label, self.current_addr());
                    loop_labels.push(loop_label);

                    // Check if we've gone past the equality key range BEFORE any column reads
                    // This prevents unnecessary deferred seeks for out-of-range rows
                    // The check is done at the top of the loop so that after Next advances,
                    // we immediately exit if past range without triggering deferred seeks
                    if *eq_cols > 0 {
                        self.emit(
                            Opcode::IdxGT,
                            index_cursor,
                            skip_label,
                            key_base_reg,
                            P4::Int64(*eq_cols as i64),
                        );
                    }

                    // Also check range end bound at loop start (for upper bound constraints)
                    if let Some((end_key_reg, end_key_count, op)) = &range_end_info {
                        // For Lt (y < 100): terminate when y >= 100 -> use IdxGE
                        // For Le (y <= 100): terminate when y > 100 -> use IdxGT
                        let opcode = match op {
                            TermOp::Lt => Opcode::IdxGE,
                            TermOp::Le => Opcode::IdxGT,
                            _ => Opcode::IdxGE,
                        };
                        self.emit(
                            opcode,
                            index_cursor,
                            skip_label,
                            *end_key_reg,
                            P4::Int64(*end_key_count as i64),
                        );
                    }

                    // DeferredSeek sets up table cursor to read from index
                    // Build alt-map for covering index optimization
                    let alt_map_p4 =
                        if let Some(alt_map) = self.build_index_alt_map(cursor, index_name) {
                            P4::IntArray(alt_map)
                        } else {
                            P4::Unused
                        };
                    self.emit(Opcode::DeferredSeek, cursor, 0, index_cursor, alt_map_p4);

                    scan_info.push((true, Some(index_cursor), key_base_reg, *eq_cols, false));
                    // Range end check is now at loop START, so mark as handled
                    range_end_keys.push(None);
                }
                Some(WherePlan::IndexScan {
                    index_name,
                    has_range: true,
                    ..
                }) => {
                    // Index range scan (for BETWEEN, <, >, etc.) without equality prefix
                    let index_cursor = self.alloc_cursor();
                    self.index_cursors.insert(cursor, index_cursor);

                    // Open the index
                    self.emit(
                        Opcode::OpenRead,
                        index_cursor,
                        0,
                        0,
                        P4::Text(index_name.clone()),
                    );

                    // Find range terms from WHERE clause
                    // Returns (start_bound, end_bound) where each is (expr, is_strict)
                    let (start_bound, end_bound) = if let Some(info) = &where_info {
                        if let Some(level) = info.levels.get(loop_pos) {
                            self.find_range_bounds(info, level)
                        } else {
                            (None, None)
                        }
                    } else {
                        (None, None)
                    };

                    let key_base_reg = self.next_reg;
                    let start_key_reg = self.alloc_reg();
                    let end_key_reg = if end_bound.is_some() {
                        Some(self.alloc_reg())
                    } else {
                        None
                    };

                    // Compile start bound and seek to it
                    if let Some((start_expr, is_strict)) = &start_bound {
                        self.compile_expr(start_expr, start_key_reg)?;
                        // Create a single-column key for the range bound
                        let key_record_reg = self.alloc_reg();
                        self.emit(
                            Opcode::MakeRecord,
                            start_key_reg,
                            1,
                            key_record_reg,
                            P4::Unused,
                        );
                        // Use SeekGT for > (strict), SeekGE for >= (inclusive)
                        let seek_op = if *is_strict {
                            Opcode::SeekGT
                        } else {
                            Opcode::SeekGE
                        };
                        self.emit(
                            seek_op,
                            index_cursor,
                            skip_label,
                            key_record_reg,
                            P4::Int64(1),
                        );
                    } else if end_bound.is_some() {
                        // No start bound but we have an end bound (e.g., y < 33)
                        // Use SeekGT(NULL) to skip past NULL values at the start of the index
                        // This is necessary because NULL comparisons are always UNKNOWN, not TRUE,
                        // so NULL values should never be included in range results
                        let null_key_reg = self.alloc_reg();
                        self.emit(Opcode::Null, 0, null_key_reg, 0, P4::Unused);
                        let null_record_reg = self.alloc_reg();
                        self.emit(
                            Opcode::MakeRecord,
                            null_key_reg,
                            1,
                            null_record_reg,
                            P4::Unused,
                        );
                        // SeekGT(NULL) positions past all NULL values
                        self.emit(
                            Opcode::SeekGT,
                            index_cursor,
                            skip_label,
                            null_record_reg,
                            P4::Int64(1),
                        );
                    } else {
                        // No constraints - start from beginning
                        self.emit(Opcode::Rewind, index_cursor, skip_label, 0, P4::Unused);
                    }

                    // Compile end bound for checking in loop
                    if let (Some((end_expr, _is_strict)), Some(end_reg)) = (&end_bound, end_key_reg)
                    {
                        self.compile_expr(end_expr, end_reg)?;
                    }

                    next_labels.push(skip_label);

                    // Mark the loop start
                    let loop_label = self.alloc_label();
                    self.resolve_label(loop_label, self.current_addr());
                    loop_labels.push(loop_label);

                    // Check range end bound at loop START to avoid unnecessary deferred seeks
                    // for out-of-range rows. This check must happen before DeferredSeek.
                    // For Lt (y < 100): terminate when y >= 100 -> use IdxGE
                    // For Le (y <= 100): terminate when y > 100 -> use IdxGT
                    if let Some(end_reg) = end_key_reg {
                        let opcode = match &end_bound {
                            Some((_, true)) => Opcode::IdxGE,  // strict < uses IdxGE
                            Some((_, false)) => Opcode::IdxGT, // inclusive <= uses IdxGT
                            _ => Opcode::IdxGE,
                        };
                        self.emit(opcode, index_cursor, skip_label, end_reg, P4::Int64(1));
                    }

                    // DeferredSeek sets up table cursor to read from index
                    // Build alt-map for covering index optimization
                    let alt_map_p4 =
                        if let Some(alt_map) = self.build_index_alt_map(cursor, index_name) {
                            P4::IntArray(alt_map)
                        } else {
                            P4::Unused
                        };
                    self.emit(Opcode::DeferredSeek, cursor, 0, index_cursor, alt_map_p4);

                    // Store scan info (no equality keys for pure range scan)
                    scan_info.push((true, Some(index_cursor), 0, 0, false));

                    // Range end check is now at loop START, so mark it as handled
                    range_end_keys.push(None);
                }
                Some(WherePlan::RowidEq) => {
                    // Direct rowid lookup - find the rowid term and compile it
                    let rowid_reg = self.alloc_reg();

                    // Find and compile the rowid equality expression
                    if let Some(info) = &where_info {
                        if let Some(level) = info.levels.get(loop_pos) {
                            for &term_idx in &level.used_terms {
                                if let Some(term) = info.terms.get(term_idx as usize) {
                                    if term.is_equality() {
                                        if let Some((_, col_idx)) = term.left_col {
                                            if col_idx == -1 {
                                                // This is the rowid term
                                                if let Expr::Binary { right, .. } =
                                                    term.expr.as_ref()
                                                {
                                                    self.compile_expr(right, rowid_reg)?;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // SeekRowid positions cursor at exact rowid
                    self.emit(Opcode::SeekRowid, cursor, skip_label, rowid_reg, P4::Unused);
                    next_labels.push(skip_label);

                    // Mark the loop start (even though there's only one row)
                    let loop_label = self.alloc_label();
                    self.resolve_label(loop_label, self.current_addr());
                    loop_labels.push(loop_label);

                    scan_info.push((false, None, 0, 0, true));
                    range_end_keys.push(None);
                }
                Some(WherePlan::RowidIn { term_idx }) => {
                    // Rowid IN list - iterate through IN values and seek each rowid
                    // This is much more efficient than a full scan for small IN lists

                    // Find the IN term and get the values
                    let in_term = where_info
                        .as_ref()
                        .and_then(|info| info.terms.get(*term_idx as usize));

                    if let Some(term) = in_term {
                        if let Expr::In { list, .. } = term.expr.as_ref() {
                            if let crate::parser::ast::InList::Values(values) = list {
                                // Create ephemeral table for the IN values
                                // Using P3=1 for BTREE_UNORDERED to allow duplicates
                                let eph_cursor = self.alloc_cursor();
                                self.emit(Opcode::OpenEphemeral, eph_cursor, 1, 0, P4::Unused);

                                // Populate ephemeral table with IN values as actual rows
                                // Use Insert (not IdxInsert) so we can read back with Column
                                let rowid_counter = self.alloc_reg();
                                self.emit(Opcode::Integer, 0, rowid_counter, 0, P4::Unused);

                                for value in values {
                                    // Increment rowid counter
                                    self.emit(Opcode::AddImm, rowid_counter, 1, 0, P4::Unused);

                                    // Compile the value
                                    let val_reg = self.alloc_reg();
                                    self.compile_expr(value, val_reg)?;

                                    // Create record with the value
                                    let rec_reg = self.alloc_reg();
                                    self.emit(Opcode::MakeRecord, val_reg, 1, rec_reg, P4::Unused);

                                    // Insert as row with explicit rowid (use Insert, same as SQLite)
                                    self.emit(
                                        Opcode::Insert,
                                        eph_cursor,
                                        rec_reg,
                                        rowid_counter,
                                        P4::Unused,
                                    );
                                }

                                // Rewind on ephemeral table
                                self.emit(Opcode::Rewind, eph_cursor, skip_label, 0, P4::Unused);
                                next_labels.push(skip_label);

                                // Mark the loop start
                                let loop_label = self.alloc_label();
                                self.resolve_label(loop_label, self.current_addr());
                                loop_labels.push(loop_label);

                                // Read the rowid value from ephemeral table column 0
                                let rowid_reg = self.alloc_reg();
                                self.emit(Opcode::Column, eph_cursor, 0, rowid_reg, P4::Unused);

                                // Seek to the rowid in the main table
                                // If not found, continue to next value
                                let not_found_label = self.alloc_label();
                                self.emit(
                                    Opcode::SeekRowid,
                                    cursor,
                                    not_found_label,
                                    rowid_reg,
                                    P4::Unused,
                                );

                                // Store info for loop end generation
                                // is_index_scan=false, index_cursor=Some(eph_cursor) for Next
                                // Use a dummy tuple - only the first element (not_found_label) is used
                                scan_info.push((false, Some(eph_cursor), 0, 0, false));
                                range_end_keys.push(Some((not_found_label, 0, TermOp::Eq)));
                            } else {
                                // Subquery IN - fall back to full scan with filter
                                self.emit(Opcode::Rewind, cursor, skip_label, 0, P4::Unused);
                                next_labels.push(skip_label);
                                let loop_label = self.alloc_label();
                                self.resolve_label(loop_label, self.current_addr());
                                loop_labels.push(loop_label);
                                scan_info.push((false, None, 0, 0, false));
                                range_end_keys.push(None);
                            }
                        } else {
                            // Not an IN expression - fall back
                            self.emit(Opcode::Rewind, cursor, skip_label, 0, P4::Unused);
                            next_labels.push(skip_label);
                            let loop_label = self.alloc_label();
                            self.resolve_label(loop_label, self.current_addr());
                            loop_labels.push(loop_label);
                            scan_info.push((false, None, 0, 0, false));
                            range_end_keys.push(None);
                        }
                    } else {
                        // Term not found - fall back
                        self.emit(Opcode::Rewind, cursor, skip_label, 0, P4::Unused);
                        next_labels.push(skip_label);
                        let loop_label = self.alloc_label();
                        self.resolve_label(loop_label, self.current_addr());
                        loop_labels.push(loop_label);
                        scan_info.push((false, None, 0, 0, false));
                        range_end_keys.push(None);
                    }
                }
                Some(WherePlan::RowidRange {
                    has_start,
                    has_end: _,
                }) => {
                    // Rowid range scan
                    if *has_start {
                        // Find and compile the start value
                        let start_reg = self.alloc_reg();
                        let mut found_start = false;

                        if let Some(info) = &where_info {
                            if let Some(level) = info.levels.get(loop_pos) {
                                for &term_idx in &level.used_terms {
                                    if let Some(term) = info.terms.get(term_idx as usize) {
                                        if let Some((_, col_idx)) = term.left_col {
                                            if col_idx == -1 && term.is_range() {
                                                // Check if it's a >= or > constraint
                                                if let Expr::Binary { op, right, .. } =
                                                    term.expr.as_ref()
                                                {
                                                    if matches!(op, BinaryOp::Ge | BinaryOp::Gt) {
                                                        self.compile_expr(right, start_reg)?;
                                                        found_start = true;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if found_start {
                            self.emit(Opcode::SeekGE, cursor, skip_label, start_reg, P4::Unused);
                        } else {
                            self.emit(Opcode::Rewind, cursor, skip_label, 0, P4::Unused);
                        }
                    } else {
                        self.emit(Opcode::Rewind, cursor, skip_label, 0, P4::Unused);
                    }
                    next_labels.push(skip_label);

                    let loop_label = self.alloc_label();
                    self.resolve_label(loop_label, self.current_addr());
                    loop_labels.push(loop_label);

                    scan_info.push((false, None, 0, 0, false));
                    range_end_keys.push(None);
                }
                _ => {
                    // Check if we can use ORDER BY index scan for this table
                    // ORDER BY index scan only works for the outermost loop (loop_pos == 0)
                    let table_info = self
                        .tables
                        .get(self.outer_tables_boundary + from_idx)
                        .cloned();
                    let use_order_by_index = loop_pos == 0
                        && self
                            .order_by_index
                            .as_ref()
                            .map(|(tbl, _idx)| {
                                table_info
                                    .as_ref()
                                    .map(|t| t.table_name.to_lowercase() == tbl.to_lowercase())
                                    .unwrap_or(false)
                            })
                            .unwrap_or(false);

                    if use_order_by_index {
                        // Use index scan for ORDER BY
                        let index_name = self.order_by_index.as_ref().unwrap().1.clone();
                        let index_cursor = self.alloc_cursor();
                        self.index_cursors.insert(cursor, index_cursor);

                        // Open the index
                        self.emit(
                            Opcode::OpenRead,
                            index_cursor,
                            0,
                            0,
                            P4::Text(index_name.clone()),
                        );

                        // Rewind on index cursor
                        self.emit(Opcode::Rewind, index_cursor, skip_label, 0, P4::Unused);
                        next_labels.push(skip_label);

                        // Mark the loop start
                        let loop_label = self.alloc_label();
                        self.resolve_label(loop_label, self.current_addr());
                        loop_labels.push(loop_label);

                        // DeferredSeek sets up table cursor to read from index
                        // Build alt-map for covering index optimization
                        let alt_map_p4 =
                            if let Some(alt_map) = self.build_index_alt_map(cursor, &index_name) {
                                P4::IntArray(alt_map)
                            } else {
                                P4::Unused
                            };
                        self.emit(Opcode::DeferredSeek, cursor, 0, index_cursor, alt_map_p4);

                        // Mark this as index scan so Next uses index_cursor
                        scan_info.push((true, Some(index_cursor), 0, 0, false));
                        range_end_keys.push(None);
                    } else {
                        // Full scan (default)
                        self.emit(Opcode::Rewind, cursor, skip_label, 0, P4::Unused);
                        next_labels.push(skip_label);

                        // Mark the loop start for this level
                        let loop_label = self.alloc_label();
                        self.resolve_label(loop_label, self.current_addr());
                        loop_labels.push(loop_label);

                        scan_info.push((false, None, 0, 0, false));
                        range_end_keys.push(None);
                    }
                }
            }

            // Initialize found_match for the NEXT table (if it's an outer join)
            // This must be INSIDE the current loop (after loop_label) so it resets on each iteration
            if loop_pos + 1 < iteration_order.len() {
                let next_from_idx = iteration_order[loop_pos + 1];
                if let Some(reg) = found_match_regs[next_from_idx] {
                    self.emit(Opcode::Integer, 0, reg, 0, P4::Unused);
                }
            }
        }

        // Inner loop start is the innermost loop label
        let loop_start_label = *loop_labels.last().unwrap_or(&self.alloc_label());

        // Evaluate WHERE clause, filtering out terms consumed by index seeks
        // For outer joins, we disabled index seeks so we must compile the full WHERE clause
        let where_skip_label = if has_outer_join {
            // Outer join: compile full WHERE clause (index seeks were disabled)
            if let Some(where_expr) = remaining_where.as_ref() {
                let label = self.alloc_label();
                self.compile_where_condition(where_expr, label)?;
                Some(label)
            } else {
                None
            }
        } else if let Some(info) = &where_info {
            // Use optimized path: only compile terms not consumed by index seeks
            let label = self.alloc_label();
            let any_terms = self.compile_runtime_filter_terms(info, label)?;
            if any_terms {
                Some(label)
            } else {
                // All terms were consumed by index seeks, no runtime filter needed
                None
            }
        } else if let Some(where_expr) = remaining_where.as_ref() {
            // No query plan available, compile full WHERE clause
            let label = self.alloc_label();
            self.compile_where_condition(where_expr, label)?;
            Some(label)
        } else {
            None
        };

        // Evaluate result columns
        let result_regs = self.compile_result_columns(&core.columns)?;

        // Handle DISTINCT
        let distinct_skip_label = if let Some(distinct_cursor) = distinct_cursor {
            let skip_label = self.alloc_label();
            // Make record for lookup
            let record_reg = self.alloc_reg();
            self.emit(
                Opcode::MakeRecord,
                result_regs.0,
                result_regs.1 as i32,
                record_reg,
                P4::Unused,
            );
            // Check if row exists in distinct table (skip output if found)
            self.emit(
                Opcode::IdxGE,
                distinct_cursor,
                skip_label,
                record_reg,
                P4::Int64(result_regs.1 as i64),
            );
            // Insert into distinct table
            self.emit(
                Opcode::IdxInsert,
                distinct_cursor,
                record_reg,
                0,
                P4::Unused,
            );
            Some(skip_label)
        } else {
            None
        };

        // Create a loop continuation label for OFFSET skip
        let loop_continue_label = self.alloc_label();

        // Output the row (with LIMIT/OFFSET if applicable)
        if self.limit_counter_reg.is_some() || self.offset_counter_reg.is_some() {
            self.output_row_with_limit(dest, result_regs.0, result_regs.1, loop_continue_label)?;
        } else {
            self.output_row(dest, result_regs.0, result_regs.1)?;
        }

        // For outer joins, mark that we found a matching row
        for found_match_reg in &found_match_regs {
            if let Some(reg) = found_match_reg {
                self.emit(Opcode::Integer, 1, *reg, 0, P4::Unused);
            }
        }

        // Loop continuation target (for WHERE skip, DISTINCT skip, OFFSET skip)
        self.resolve_label(loop_continue_label, self.current_addr());
        if let Some(label) = where_skip_label {
            self.resolve_label(label, self.current_addr());
        }

        // DISTINCT skip target
        if let Some(label) = distinct_skip_label {
            self.resolve_label(label, self.current_addr());
        }

        // Generate Next for each table in reverse order (innermost first)
        // Each table's Next jumps back to its own loop start
        // When a table's Next fails, fall through to resolve the skip label
        // which then tries Next on the outer table
        for loop_pos in (0..iteration_order.len()).rev() {
            let from_idx = iteration_order[loop_pos];
            let cursor = table_cursors[from_idx];
            let loop_label = loop_labels[loop_pos];

            // Get scan info for this table (indexed by loop position)
            let (is_index_scan, index_cursor, key_base_reg, key_count, is_rowid_eq) = scan_info
                .get(loop_pos)
                .copied()
                .unwrap_or((false, None, 0, 0, false));

            if is_rowid_eq {
                // Rowid equality - no Next needed, just resolve skip label
                // (single row lookup, no iteration)
            } else if is_index_scan {
                if let Some(idx_cursor) = index_cursor {
                    // Check range end key first (for early termination on upper bound)
                    if let Some(Some((end_key_reg, end_key_count, op))) =
                        range_end_keys.get(loop_pos)
                    {
                        // For Lt (y < 100): terminate when y >= 100 -> use IdxGE
                        // For Le (y <= 100): terminate when y > 100 -> use IdxGT
                        let opcode = match op {
                            TermOp::Lt => Opcode::IdxGE,
                            TermOp::Le => Opcode::IdxGT,
                            _ => Opcode::IdxGE, // Default to IdxGE for safety
                        };
                        self.emit(
                            opcode,
                            idx_cursor,
                            next_labels[loop_pos],
                            *end_key_reg,
                            P4::Int64(*end_key_count as i64),
                        );
                    }
                    // Note: IdxGT for equality key range check is now emitted at loop START
                    // (before DeferredSeek) to avoid unnecessary deferred seeks for out-of-range rows
                    // Next on the index cursor, not the table cursor
                    self.emit(Opcode::Next, idx_cursor, loop_label, 0, P4::Unused);
                }
            } else if let Some(eph_cursor) = index_cursor {
                // RowidIn case - index_cursor is the ephemeral table cursor
                // First resolve the not_found_label so SeekRowid failures come here
                if let Some(Some((not_found_label, _, _))) = range_end_keys.get(loop_pos) {
                    self.resolve_label(*not_found_label, self.current_addr());
                }
                // Next on the ephemeral cursor to get next value from IN list
                self.emit(Opcode::Next, eph_cursor, loop_label, 0, P4::Unused);
            } else {
                // Full scan or rowid range - Next on table cursor
                self.emit(Opcode::Next, cursor, loop_label, 0, P4::Unused);
            }

            // For outer joins: if no match was found, emit null row
            // Both empty Rewind and exhausted Next come here
            // found_match_regs is indexed by FROM clause position
            if let Some(found_match_reg) = found_match_regs[from_idx] {
                // Resolve the skip label HERE so Rewind jumps to check_match, not past it
                self.resolve_label(next_labels[loop_pos], self.current_addr());

                // Label to skip null row output if we found a match
                let skip_null_output = self.alloc_label();

                // If found_match > 0, skip null row output
                self.emit(
                    Opcode::IfPos,
                    found_match_reg,
                    skip_null_output,
                    0,
                    P4::Unused,
                );

                // Set cursor to null row mode (columns will return NULL)
                self.emit(Opcode::NullRow, cursor, 0, 0, P4::Unused);

                // Re-evaluate the ORIGINAL WHERE clause (not including join conditions) with the null row
                // This is critical for LEFT JOIN with WHERE on the left table
                // e.g., SELECT * FROM t1 LEFT JOIN t2 ON true WHERE t1.a IS NULL
                // The null-fill row should only be output if the original WHERE passes.
                // Join conditions are NOT re-evaluated because they already determined that no
                // right table rows matched - the null-fill is the correct behavior for outer joins.
                if let Some(where_expr) = original_where_for_null_fill.as_ref() {
                    self.compile_where_condition(where_expr, skip_null_output)?;
                }

                // Re-evaluate result columns with null row
                // Save column metadata since compile_result_columns adds to these vectors
                let saved_result_column_names = self.result_column_names.len();
                let saved_columns = self.columns.len();
                let null_result_regs = self.compile_result_columns(&core.columns)?;
                // Restore column metadata (don't double-count columns for null row output)
                self.result_column_names.truncate(saved_result_column_names);
                self.columns.truncate(saved_columns);

                // Output the null row
                self.output_row(dest, null_result_regs.0, null_result_regs.1)?;

                // Skip null output target
                self.resolve_label(skip_null_output, self.current_addr());
            } else {
                // Non-outer join: resolve skip label after Next
                self.resolve_label(next_labels[loop_pos], self.current_addr());
            }
        }

        // Note: SQLite does NOT emit Close for table/index cursors in SELECT queries
        // Cursors are implicitly closed when the statement ends (after Halt)
        // Only emit Close for ephemeral cursors used in compound queries

        // Close distinct cursor if used (ephemeral table)
        if let Some(cursor) = distinct_cursor {
            self.emit(Opcode::Close, cursor, 0, 0, P4::Unused);
        }

        // Resolve LIMIT done label (jump here when limit exhausted)
        if let Some(done_label) = self.limit_done_label {
            self.resolve_label(done_label, self.current_addr());
        }

        Ok(())
    }

    /// Compile SELECT with window functions
    ///
    /// Window functions require special handling:
    /// 1. First compile the base query into an ephemeral table
    /// 2. Sort by PARTITION BY + ORDER BY
    /// 3. Process each partition, computing window function values
    /// 4. Output rows with window function results
    fn compile_with_window_functions(
        &mut self,
        core: &SelectCore,
        dest: &SelectDest,
    ) -> Result<()> {
        use crate::executor::window::{WindowFunc, WindowFuncType};

        // Create a WindowCompiler to analyze and compile window functions
        let mut window_compiler = WindowCompiler::new(self.next_reg, self.next_cursor);

        // Collect window function information
        let window_funcs = window_compiler.collect_window_functions(core)?;

        if window_funcs.is_empty() {
            // No window functions after all, fall back to regular compilation
            return self.compile_simple_select(core, dest);
        }

        // Group by window specification
        let windows = window_compiler.group_by_window(window_funcs.clone())?;

        // Update our register/cursor counters
        self.next_reg = window_compiler.next_reg();
        self.next_cursor = window_compiler.next_cursor();

        // For aggregate window functions with empty OVER(), we need to:
        // 1. Allocate accumulator registers
        // 2. Emit AggStep during row collection
        // 3. Emit AggFinal after collection
        // 4. Use finalized value during output
        //
        // Identify simple aggregate window functions (AGG() OVER() with no partition/order)
        let mut simple_agg_window_funcs: Vec<(usize, String, Vec<Expr>, i32, i32)> = Vec::new(); // (col_index, agg_name, args, accum_reg, final_reg)
        for func in &window_funcs {
            let is_empty_spec = func.spec.partition_by.is_none()
                && func.spec.order_by.is_none()
                && func.spec.frame.is_none();

            if is_empty_spec {
                if let WindowFuncType::Aggregate(agg_name) = &func.func_type {
                    let accum_reg = self.alloc_reg();
                    let final_reg = self.alloc_reg();
                    // Initialize accumulator to NULL
                    self.emit(Opcode::Null, 0, accum_reg, 0, P4::Unused);
                    simple_agg_window_funcs.push((
                        func.col_index,
                        agg_name.clone(),
                        func.args.clone(),
                        accum_reg,
                        final_reg,
                    ));
                }
            }
        }

        // Step 1: Open ephemeral table to store intermediate results
        let eph_cursor = self.alloc_cursor();
        self.emit(Opcode::OpenEphemeral, eph_cursor, 0, 0, P4::Unused);

        // Step 2: Collect table cursors
        let table_cursors: Vec<i32> = self
            .tables
            .iter()
            .skip(self.outer_tables_boundary)
            .map(|t| t.cursor)
            .collect();

        // Generate proper nested loop structure for cross joins
        let mut loop_labels: Vec<i32> = Vec::with_capacity(table_cursors.len());
        let mut done_labels: Vec<i32> = Vec::with_capacity(table_cursors.len());
        for cursor in &table_cursors {
            let done_label = self.alloc_label();
            self.emit(Opcode::Rewind, *cursor, done_label, 0, P4::Unused);
            done_labels.push(done_label);

            // Mark loop start for this level
            let loop_label = self.alloc_label();
            self.resolve_label(loop_label, self.current_addr());
            loop_labels.push(loop_label);
        }

        // The innermost loop label is used for WHERE skip target
        let _loop_start_label = *loop_labels.last().unwrap_or(&self.alloc_label());

        // Evaluate WHERE clause
        let where_skip_label = if let Some(where_expr) = &core.where_clause {
            let label = self.alloc_label();
            self.compile_where_condition(where_expr, label)?;
            Some(label)
        } else {
            None
        };

        // Evaluate all result columns (except window functions get placeholders)
        let (result_base, result_count) = self.compile_result_columns_for_window(core)?;

        // For simple aggregate window functions, emit AggStep to accumulate values
        for (_, agg_name, args, accum_reg, _) in &simple_agg_window_funcs {
            // Compile arguments
            let argc = args.len();
            let arg_base = self.alloc_regs(argc.max(1));

            if argc == 0 {
                // For COUNT(*) etc., use NULL argument
                self.emit(Opcode::Null, 0, arg_base, 0, P4::Unused);
            } else {
                for (i, arg) in args.iter().enumerate() {
                    self.compile_expr(arg, arg_base + i as i32)?;
                }
            }

            // Emit AggStep: P1=argc, P2=arg_base, P3=accum, P4=func_name
            self.emit(
                Opcode::AggStep,
                argc.max(1) as i32,
                arg_base,
                *accum_reg,
                P4::Text(agg_name.clone()),
            );
        }

        // Store into ephemeral table
        let record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            result_base,
            result_count as i32,
            record_reg,
            P4::Unused,
        );
        self.emit(Opcode::NewRowid, eph_cursor, result_base, 0, P4::Unused);
        self.emit(
            Opcode::Insert,
            eph_cursor,
            record_reg,
            result_base,
            P4::Unused,
        );

        // WHERE skip target
        if let Some(label) = where_skip_label {
            self.resolve_label(label, self.current_addr());
        }

        // Generate Next for each table in reverse order (innermost first)
        for i in (0..table_cursors.len()).rev() {
            let cursor = table_cursors[i];
            let loop_label = loop_labels[i];

            // Next on this cursor, jump back to its loop start
            self.emit(Opcode::Next, cursor, loop_label, 0, P4::Unused);

            // Resolve done label for this level
            self.resolve_label(done_labels[i], self.current_addr());
        }

        // Finalize simple aggregate window functions
        for (_, agg_name, _, accum_reg, final_reg) in &simple_agg_window_funcs {
            // Emit AggFinal: P1=accum, P2=dest, P4=func_name
            self.emit(
                Opcode::AggFinal,
                *accum_reg,
                *final_reg,
                0,
                P4::Text(agg_name.clone()),
            );
        }

        // Step 3: Now process window functions
        // (Skip the old window compiler ops as we handle simple aggregates directly)
        let _window_ops = window_compiler.take_ops();
        // Only call compile_window_functions for non-simple window functions
        // For now we skip this as we handle simple aggregates directly
        // window_compiler.compile_window_functions(&windows, result_base, result_count)?;
        // let window_ops = window_compiler.take_ops();
        // for op in window_ops {
        //     self.ops.push(op);
        // }

        // Step 4: Read from ephemeral table and output with window results
        let done_label = self.alloc_label();
        self.emit(Opcode::Rewind, eph_cursor, done_label, 0, P4::Unused);

        let read_loop = self.current_addr();

        // Build a map of col_index -> final_reg for simple aggregate window functions
        let simple_agg_map: HashMap<usize, i32> = simple_agg_window_funcs
            .iter()
            .map(|(col_idx, _, _, _, final_reg)| (*col_idx, *final_reg))
            .collect();

        // Read column values, but for simple aggregate window functions,
        // use the finalized value instead of reading from ephemeral table
        for i in 0..result_count {
            if let Some(final_reg) = simple_agg_map.get(&i) {
                // Use the finalized aggregate value
                self.emit(
                    Opcode::Copy,
                    *final_reg,
                    result_base + i as i32,
                    0,
                    P4::Unused,
                );
            } else {
                // Read from ephemeral table
                self.emit(
                    Opcode::Column,
                    eph_cursor,
                    i as i32,
                    result_base + i as i32,
                    P4::Unused,
                );
            }
        }

        // Output the row
        self.output_row(dest, result_base, result_count)?;

        // Next row
        self.emit(Opcode::Next, eph_cursor, read_loop as i32, 0, P4::Unused);

        self.resolve_label(done_label, self.current_addr());

        // Close cursors
        self.emit(Opcode::Close, eph_cursor, 0, 0, P4::Unused);
        for cursor in &table_cursors {
            self.emit(Opcode::Close, *cursor, 0, 0, P4::Unused);
        }

        Ok(())
    }

    /// Compile result columns for window function processing
    ///
    /// For window function columns, just allocate a register (value computed later)
    /// For non-window columns, compile normally
    fn compile_result_columns_for_window(&mut self, core: &SelectCore) -> Result<(i32, usize)> {
        use crate::executor::window::has_window_function;

        let base_reg = self.next_reg;
        let mut count = 0;

        for col in &core.columns {
            match col {
                ResultColumn::Star => {
                    // Expand * to all columns from all tables
                    // Skip coalesced columns from NATURAL/USING joins
                    let tables_snapshot: Vec<_> = self.tables.clone();
                    let coalesced_snapshot = self.coalesced_columns.clone();

                    // Check if there are any tables to expand - SELECT * requires at least one table
                    if tables_snapshot.is_empty() {
                        return Err(Error::with_message(ErrorCode::Error, "no tables specified"));
                    }

                    for (table_idx, table) in tables_snapshot.iter().enumerate() {
                        let excluded_cols = coalesced_snapshot.get(&table_idx);

                        if let Some(schema_table) = &table.schema_table {
                            for (col_idx, col_def) in schema_table.columns.iter().enumerate() {
                                // Skip coalesced columns
                                if let Some(excluded) = excluded_cols {
                                    if excluded.contains(&col_def.name.to_lowercase()) {
                                        continue;
                                    }
                                }

                                let reg = self.alloc_reg();

                                // Use VColumn for virtual tables
                                let col_opcode = if schema_table.is_virtual {
                                    Opcode::VColumn
                                } else {
                                    Opcode::Column
                                };

                                // Check if this is a VIRTUAL generated column
                                if let Some(ref gen) = col_def.generated {
                                    if gen.storage == GeneratedStorage::Virtual {
                                        // Compile the generated expression
                                        let gen_expr = Self::convert_schema_expr_to_ast(&gen.expr);
                                        self.compile_expr(&gen_expr, reg)?;
                                    } else {
                                        // STORED generated columns are read normally
                                        self.emit(
                                            col_opcode,
                                            table.cursor,
                                            col_idx as i32,
                                            reg,
                                            P4::Unused,
                                        );
                                    }
                                } else {
                                    self.emit(
                                        col_opcode,
                                        table.cursor,
                                        col_idx as i32,
                                        reg,
                                        P4::Unused,
                                    );
                                }
                                count += 1;
                            }
                        }
                    }
                }
                ResultColumn::TableStar(table_name) => {
                    // Expand table.* to columns from specific table
                    // Note: Only match on table.name (alias if provided, or original name if no alias)
                    // SQLite doesn't allow using the original table name when an alias is provided
                    let tables_snapshot: Vec<_> = self.tables.clone();
                    let mut found = false;
                    for table in &tables_snapshot {
                        if table.name.eq_ignore_ascii_case(table_name) {
                            found = true;
                            if let Some(schema_table) = &table.schema_table {
                                // Use VColumn for virtual tables
                                let col_opcode = if schema_table.is_virtual {
                                    Opcode::VColumn
                                } else {
                                    Opcode::Column
                                };

                                for (col_idx, col_def) in schema_table.columns.iter().enumerate() {
                                    let reg = self.alloc_reg();

                                    // Check if this is a VIRTUAL generated column
                                    if let Some(ref gen) = col_def.generated {
                                        if gen.storage == GeneratedStorage::Virtual {
                                            // Compile the generated expression
                                            let gen_expr =
                                                Self::convert_schema_expr_to_ast(&gen.expr);
                                            self.compile_expr(&gen_expr, reg)?;
                                        } else {
                                            // STORED generated columns are read normally
                                            self.emit(
                                                col_opcode,
                                                table.cursor,
                                                col_idx as i32,
                                                reg,
                                                P4::Unused,
                                            );
                                        }
                                    } else {
                                        self.emit(
                                            col_opcode,
                                            table.cursor,
                                            col_idx as i32,
                                            reg,
                                            P4::Unused,
                                        );
                                    }
                                    count += 1;
                                }
                            }
                            break;
                        }
                    }
                    if !found {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("no such table: {}", table_name),
                        ));
                    }
                }
                ResultColumn::Expr { expr, .. } => {
                    let reg = self.alloc_reg();
                    if has_window_function(expr) {
                        // Window function - will be filled in later
                        self.emit(Opcode::Null, 0, reg, 0, P4::Unused);
                    } else {
                        // Regular expression
                        self.compile_expr(expr, reg)?;
                    }
                    count += 1;
                }
            }
        }

        Ok((base_reg, count))
    }

    /// Compile SELECT with aggregates but no GROUP BY
    fn compile_simple_aggregate(&mut self, core: &SelectCore, dest: &SelectDest) -> Result<()> {
        // Optimization: Use Count opcode for simple COUNT(*) queries
        // This matches SQLite's behavior of using the Count opcode instead of
        // iterating through all rows when counting without a WHERE clause
        if self.try_compile_count_star(core, dest)? {
            return Ok(());
        }

        // Initialize aggregate accumulators
        let agg_regs = self.init_aggregates(&core.columns)?;

        // Check if we have MIN/MAX aggregates - affects bare column affinity
        let has_min_max = self.has_min_max_aggregate(&core.columns);

        // Collect column references from result columns and HAVING clause
        // For simple aggregates, SQLite uses the FIRST row's values for non-aggregate columns
        // UNLESS there's a MIN/MAX, in which case use the min/max row's values
        let mut col_refs_to_save: Vec<(i32, i32, String)> = Vec::new(); // (cursor, col_idx, col_name)
        for col in &core.columns {
            if let ResultColumn::Expr { expr, .. } = col {
                self.collect_column_refs(expr, &mut col_refs_to_save);
            }
        }
        if let Some(having) = &core.having {
            self.collect_column_refs(having, &mut col_refs_to_save);
        }

        // Deduplicate column references
        col_refs_to_save.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        col_refs_to_save.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);

        // Allocate registers to save column values
        let saved_col_base = self.next_reg;
        let mut saved_col_map: HashMap<(i32, i32), i32> = HashMap::new();

        for (cursor, col_idx, _) in &col_refs_to_save {
            let reg = self.alloc_reg();
            saved_col_map.insert((*cursor, *col_idx), reg);
        }

        // Collect table cursors and join types to avoid borrow checker issues
        let table_cursors: Vec<i32> = self
            .tables
            .iter()
            .skip(self.outer_tables_boundary)
            .map(|t| t.cursor)
            .collect();
        let table_join_types: Vec<JoinType> = self
            .tables
            .iter()
            .skip(self.outer_tables_boundary)
            .map(|t| t.join_type)
            .collect();

        // For queries without MIN/MAX, use first-row saving semantics
        // For queries with MIN/MAX, we'll save when min/max changes (inside aggregate accumulation)
        let first_row_flag_reg = if !has_min_max {
            let reg = self.alloc_reg();
            self.emit(Opcode::Integer, 0, reg, 0, P4::Unused);
            Some(reg)
        } else {
            None
        };

        // Allocate a register for tracking when MIN/MAX changes (for bare column affinity)
        let min_max_changed_reg = if has_min_max {
            Some(self.alloc_reg())
        } else {
            None
        };

        // Allocate found_match registers for LEFT JOINs
        let mut found_match_regs: Vec<Option<i32>> = Vec::with_capacity(table_cursors.len());
        for (i, join_type) in table_join_types.iter().enumerate() {
            let is_outer_join = join_type.is_outer();
            let found_match_reg = if is_outer_join && i > 0 {
                let reg = self.alloc_reg();
                Some(reg)
            } else {
                None
            };
            found_match_regs.push(found_match_reg);
        }

        // Generate proper nested loop structure for cross joins and LEFT JOINs
        // For N tables with LEFT JOIN on inner tables:
        //   Rewind t0, done0
        // loop0:
        //   init found_match = 0
        //   Rewind t1, null_row1  ; if empty, jump to null row handling
        // loop1:
        //   ... body (marks found_match=1) ...
        //   Next t1, loop1
        //   IfPos found_match, after_null1  ; skip null row if found match
        // null_row1:
        //   NullRow t1
        //   ... body again with NULL values (skips found_match marking) ...
        // after_null1:
        //   Next t0, loop0
        // done0:
        let mut loop_labels: Vec<i32> = Vec::with_capacity(table_cursors.len());
        let mut done_labels: Vec<i32> = Vec::with_capacity(table_cursors.len());
        let mut null_row_labels: Vec<Option<i32>> = Vec::with_capacity(table_cursors.len());
        let mut after_null_labels: Vec<Option<i32>> = Vec::with_capacity(table_cursors.len());

        for (i, cursor) in table_cursors.iter().enumerate() {
            let is_left_join_inner = i > 0 && table_join_types[i].is_outer();

            // Initialize found_match for THIS table if it's an outer join
            if let Some(reg) = found_match_regs[i] {
                self.emit(Opcode::Integer, 0, reg, 0, P4::Unused);
            }

            let done_label = self.alloc_label();

            if is_left_join_inner {
                // For LEFT JOIN inner table: Rewind jumps to null_row handling if empty
                let null_row_label = self.alloc_label();
                self.emit(Opcode::Rewind, *cursor, null_row_label, 0, P4::Unused);
                null_row_labels.push(Some(null_row_label));
                after_null_labels.push(Some(self.alloc_label()));
            } else {
                self.emit(Opcode::Rewind, *cursor, done_label, 0, P4::Unused);
                null_row_labels.push(None);
                after_null_labels.push(None);
            }

            done_labels.push(done_label);

            // Mark loop start for this level
            let loop_label = self.alloc_label();
            self.resolve_label(loop_label, self.current_addr());
            loop_labels.push(loop_label);
        }

        // The innermost loop label is used for WHERE skip target
        let loop_start_label = *loop_labels.last().unwrap_or(&self.alloc_label());

        // Evaluate WHERE clause
        let where_skip_label = if let Some(where_expr) = &core.where_clause {
            let label = self.alloc_label();
            self.compile_where_condition(where_expr, label)?;
            Some(label)
        } else {
            None
        };

        // For non-MIN/MAX queries: Save column refs only from the FIRST row
        if let Some(flag_reg) = first_row_flag_reg {
            let skip_save_label = self.alloc_label();
            self.emit(Opcode::If, flag_reg, skip_save_label, 0, P4::Unused);

            for (cursor, col_idx, _col_name) in &col_refs_to_save {
                if let Some(&dest_reg) = saved_col_map.get(&(*cursor, *col_idx)) {
                    if *col_idx == -1 {
                        self.emit(Opcode::Rowid, *cursor, dest_reg, 0, P4::Unused);
                    } else {
                        self.emit(Opcode::Column, *cursor, *col_idx, dest_reg, P4::Unused);
                    }
                }
            }

            // Set flag to indicate first row values have been saved
            self.emit(Opcode::Integer, 1, flag_reg, 0, P4::Unused);
            self.resolve_label(skip_save_label, self.current_addr());
        }

        // For LEFT JOIN tables, mark that we found a match
        for (i, _) in table_cursors.iter().enumerate() {
            if let Some(reg) = found_match_regs[i] {
                self.emit(Opcode::Integer, 1, reg, 0, P4::Unused);
            }
        }

        // Accumulate aggregates
        // For MIN/MAX queries, pass the column refs so they get saved when min/max changes
        if let Some(changed_reg) = min_max_changed_reg {
            self.accumulate_aggregates_with_bare_cols(
                &core.columns,
                &agg_regs,
                changed_reg,
                &col_refs_to_save,
                &saved_col_map,
            )?;
        } else {
            self.accumulate_aggregates(&core.columns, &agg_regs)?;
        }

        // WHERE skip target
        if let Some(label) = where_skip_label {
            self.resolve_label(label, self.current_addr());
        }

        // Generate Next for each table in reverse order (innermost first)
        // Each table's Next jumps back to its own loop start
        // When a table's Next fails, fall through to resolve done label, then try Next on outer
        for i in (0..table_cursors.len()).rev() {
            let cursor = table_cursors[i];
            let loop_label = loop_labels[i];

            // Next on this cursor, jump back to its loop start
            self.emit(Opcode::Next, cursor, loop_label, 0, P4::Unused);

            // For LEFT JOIN: handle empty/exhausted inner table
            if let Some(found_match_reg) = found_match_regs[i] {
                // If found_match > 0, skip null row handling
                if let Some(after_null_label) = after_null_labels[i] {
                    self.emit(
                        Opcode::IfPos,
                        found_match_reg,
                        after_null_label,
                        0,
                        P4::Unused,
                    );
                }

                // Resolve null_row_label here - both Rewind-when-empty AND loop-no-match land here
                if let Some(null_row_label) = null_row_labels[i] {
                    self.resolve_label(null_row_label, self.current_addr());
                }

                // Set cursor to null row mode
                self.emit(Opcode::NullRow, cursor, 0, 0, P4::Unused);

                // For non-MIN/MAX queries: Save outer table column refs if this is the first row
                // This handles the case where inner table is empty from the start
                if let Some(flag_reg) = first_row_flag_reg {
                    let skip_save_label = self.alloc_label();
                    self.emit(Opcode::If, flag_reg, skip_save_label, 0, P4::Unused);

                    // Save column values from outer tables (not the LEFT JOIN inner table)
                    for (cur, col_idx, _col_name) in &col_refs_to_save {
                        // Only save from outer tables (cursor != current LEFT JOIN cursor)
                        if *cur != cursor {
                            if let Some(&dest_reg) = saved_col_map.get(&(*cur, *col_idx)) {
                                if *col_idx == -1 {
                                    self.emit(Opcode::Rowid, *cur, dest_reg, 0, P4::Unused);
                                } else {
                                    self.emit(Opcode::Column, *cur, *col_idx, dest_reg, P4::Unused);
                                }
                            }
                        }
                    }

                    // Set flag to indicate first row values have been saved
                    self.emit(Opcode::Integer, 1, flag_reg, 0, P4::Unused);
                    self.resolve_label(skip_save_label, self.current_addr());
                }

                // Accumulate with NULL values from the LEFT JOIN table
                if let Some(changed_reg) = min_max_changed_reg {
                    self.accumulate_aggregates_with_bare_cols(
                        &core.columns,
                        &agg_regs,
                        changed_reg,
                        &col_refs_to_save,
                        &saved_col_map,
                    )?;
                } else {
                    self.accumulate_aggregates(&core.columns, &agg_regs)?;
                }

                // Resolve after_null_label
                if let Some(after_null_label) = after_null_labels[i] {
                    self.resolve_label(after_null_label, self.current_addr());
                }
            }

            // Resolve done label for this level (for non-LEFT-JOIN Rewind-empty)
            self.resolve_label(done_labels[i], self.current_addr());
        }

        // Set up saved_column_regs for expression compilation to use
        self.saved_column_regs = Some(saved_col_map);

        // Finalize aggregates
        let result_regs = self.finalize_aggregates(&core.columns, &agg_regs)?;

        // HAVING clause - evaluated after aggregates are finalized
        // For simple aggregate (no GROUP BY), HAVING filters the single result row
        // Create skip label for LIMIT/OFFSET
        let after_output_label = self.alloc_label();
        if let Some(having) = &core.having {
            let skip_output_label = self.alloc_label();
            self.compile_where_condition(having, skip_output_label)?;
            // Clear the saved column context
            self.saved_column_regs = None;
            // Output single row only if HAVING condition is true
            self.output_row_with_limit(dest, result_regs.0, result_regs.1, after_output_label)?;
            self.resolve_label(skip_output_label, self.current_addr());
        } else {
            // Clear the saved column context
            self.saved_column_regs = None;
            // Output single row
            self.output_row_with_limit(dest, result_regs.0, result_regs.1, after_output_label)?;
        }
        self.resolve_label(after_output_label, self.current_addr());

        // Resolve limit_done_label if set
        if let Some(done_label) = self.limit_done_label {
            self.resolve_label(done_label, self.current_addr());
        }

        // Close cursors
        for cursor in &table_cursors {
            self.emit(Opcode::Close, *cursor, 0, 0, P4::Unused);
        }

        // Suppress unused variable warning
        let _ = saved_col_base;

        Ok(())
    }

    /// Collect all column references from an expression
    fn collect_column_refs(&self, expr: &Expr, refs: &mut Vec<(i32, i32, String)>) {
        match expr {
            Expr::Column(col_ref) => {
                // Find the table and column index for this reference
                if let Some((cursor, col_idx)) = self.resolve_column_ref_to_cursor(col_ref) {
                    refs.push((cursor, col_idx, col_ref.column.clone()));
                }
            }
            Expr::Binary { left, right, .. } => {
                self.collect_column_refs(left, refs);
                self.collect_column_refs(right, refs);
            }
            Expr::Unary { expr, .. } => {
                self.collect_column_refs(expr, refs);
            }
            Expr::Function(func) => {
                if let crate::parser::ast::FunctionArgs::Exprs(args) = &func.args {
                    for arg in args {
                        self.collect_column_refs(arg, refs);
                    }
                }
                if let Some(filter) = &func.filter {
                    self.collect_column_refs(filter, refs);
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    self.collect_column_refs(op, refs);
                }
                for wc in when_clauses {
                    self.collect_column_refs(&wc.when, refs);
                    self.collect_column_refs(&wc.then, refs);
                }
                if let Some(ec) = else_clause {
                    self.collect_column_refs(ec, refs);
                }
            }
            Expr::Cast { expr, .. } => {
                self.collect_column_refs(expr, refs);
            }
            Expr::Collate { expr, .. } => {
                self.collect_column_refs(expr, refs);
            }
            Expr::In { expr, list, .. } => {
                self.collect_column_refs(expr, refs);
                if let crate::parser::ast::InList::Values(vals) = list {
                    for v in vals {
                        self.collect_column_refs(v, refs);
                    }
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.collect_column_refs(expr, refs);
                self.collect_column_refs(low, refs);
                self.collect_column_refs(high, refs);
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.collect_column_refs(expr, refs);
                self.collect_column_refs(pattern, refs);
                if let Some(esc) = escape {
                    self.collect_column_refs(esc, refs);
                }
            }
            Expr::IsNull { expr, .. } => {
                self.collect_column_refs(expr, refs);
            }
            Expr::Parens(inner) => {
                self.collect_column_refs(inner, refs);
            }
            // Subqueries, literals, etc. don't contain direct column references
            _ => {}
        }
    }

    /// Resolve a column reference to (cursor, column_index)
    fn resolve_column_ref_to_cursor(&self, col_ref: &ColumnRef) -> Option<(i32, i32)> {
        // Find matching table
        for table in &self.tables {
            let matches = if let Some(ref tbl_name) = col_ref.table {
                Self::table_name_matches(table, tbl_name)
            } else {
                // No table qualifier - check if column exists in this table
                self.column_index_in_table(table, &col_ref.column).is_some()
            };

            if matches {
                if let Some(col_idx) = self.column_index_in_table(table, &col_ref.column) {
                    return Some((table.cursor, col_idx));
                }
            }
        }
        None
    }

    /// Try to compile using the Count opcode optimization
    /// Returns true if the optimization was applied, false otherwise
    fn try_compile_count_star(&mut self, core: &SelectCore, dest: &SelectDest) -> Result<bool> {
        // Check conditions for Count opcode optimization:
        // 1. No WHERE clause
        // 2. Single table (no joins)
        // 3. Result is just COUNT(*) (no other columns)
        // 4. No DISTINCT
        // 5. No HAVING clause (HAVING may need to reference non-aggregate columns)

        // Must have no WHERE clause
        if core.where_clause.is_some() {
            return Ok(false);
        }

        // Must have no HAVING clause (HAVING needs full row loop to save column values)
        if core.having.is_some() {
            return Ok(false);
        }

        // Must not be DISTINCT
        if core.distinct == Distinct::Distinct {
            return Ok(false);
        }

        // Get the local tables (excluding outer scope)
        let local_tables: Vec<_> = self
            .tables
            .iter()
            .skip(self.outer_tables_boundary)
            .collect();

        // Must have exactly one table
        if local_tables.len() != 1 {
            return Ok(false);
        }

        // Check if all result columns are COUNT(*)
        // Allow COUNT(*) or COUNT() with no arguments
        let mut is_pure_count_star = true;
        for col in &core.columns {
            match col {
                ResultColumn::Expr { expr, .. } => {
                    if !self.is_count_star(expr) {
                        is_pure_count_star = false;
                        break;
                    }
                }
                _ => {
                    is_pure_count_star = false;
                    break;
                }
            }
        }

        if !is_pure_count_star {
            return Ok(false);
        }

        // Optimization applies! Use Count opcode
        let cursor = local_tables[0].cursor;
        let count_reg = self.alloc_reg();
        let result_reg = self.alloc_reg();

        // Count opcode: P1 = cursor, P2 = destination register
        self.emit(Opcode::Count, cursor, count_reg, 0, P4::Unused);

        // Close the cursor
        self.emit(Opcode::Close, cursor, 0, 0, P4::Unused);

        // Copy to result register (matches SQLite's pattern)
        self.emit(Opcode::Copy, count_reg, result_reg, 0, P4::Unused);

        // Populate result_column_names for subquery column resolution
        for col in &core.columns {
            if let ResultColumn::Expr { expr, alias } = col {
                let name = alias
                    .clone()
                    .unwrap_or_else(|| self.expr_to_name(expr, self.result_column_names.len() + 1));
                self.result_column_names.push(name);
            }
        }

        // Output the result with LIMIT/OFFSET support
        let after_output_label = self.alloc_label();
        self.output_row_with_limit(dest, result_reg, 1, after_output_label)?;
        self.resolve_label(after_output_label, self.current_addr());

        // Resolve limit_done_label if set
        if let Some(done_label) = self.limit_done_label {
            self.resolve_label(done_label, self.current_addr());
        }

        Ok(true)
    }

    /// Check if an expression is COUNT(*) or COUNT()
    fn is_count_star(&self, expr: &Expr) -> bool {
        if let Expr::Function(func_call) = expr {
            let name_upper = func_call.name.to_uppercase();
            if name_upper == "COUNT" {
                match &func_call.args {
                    crate::parser::ast::FunctionArgs::Star => return true,
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => return exprs.is_empty(),
                }
            }
        }
        false
    }

    /// Compile SELECT with GROUP BY
    fn compile_grouped_aggregate(&mut self, core: &SelectCore, dest: &SelectDest) -> Result<()> {
        let group_by = core.group_by.as_ref().unwrap();

        // Pre-scan result columns to extract alias expressions (for GROUP BY alias resolution)
        // SQLite allows GROUP BY to reference result column aliases
        self.prescan_result_aliases(&core.columns);

        // Count result columns for GROUP BY column number validation
        let num_result_cols = core.columns.len();

        // Validate and resolve GROUP BY column numbers
        // GROUP BY integer literals refer to result columns (1-based index)
        for (i, expr) in group_by.iter().enumerate() {
            if let Expr::Literal(Literal::Integer(col_idx)) = expr {
                let col_idx = *col_idx as i32;
                if col_idx < 1 || col_idx > num_result_cols as i32 {
                    let ordinal = match i + 1 {
                        1 => "1st".to_string(),
                        2 => "2nd".to_string(),
                        3 => "3rd".to_string(),
                        n => format!("{}th", n),
                    };
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        format!(
                            "{} GROUP BY term out of range - should be between 1 and {}",
                            ordinal, num_result_cols
                        ),
                    ));
                }
            }
        }

        // Resolve GROUP BY expressions:
        // 1. Column numbers (GROUP BY 1) -> the corresponding result expression
        // 2. Aliases (GROUP BY x) -> the aliased expression
        let resolved_group_by: Vec<Expr> = group_by
            .iter()
            .map(|expr| {
                // First, check if this is a column number literal
                if let Expr::Literal(Literal::Integer(col_idx)) = expr {
                    let col_idx = *col_idx as usize;
                    // Column numbers are 1-based, convert to 0-based index
                    if col_idx >= 1 && col_idx <= core.columns.len() {
                        // Get the expression from the result column
                        if let ResultColumn::Expr {
                            expr: result_expr, ..
                        } = &core.columns[col_idx - 1]
                        {
                            return result_expr.clone();
                        }
                    }
                }
                // Otherwise, resolve aliases
                self.resolve_where_aliases(expr)
            })
            .collect();

        // Count total columns needed in sorter: group columns + aggregate arguments + non-agg result cols
        // Also include HAVING aggregate arguments since they need to be stored in the sorter
        let num_group_cols = resolved_group_by.len();
        let num_col_agg_args = self.count_aggregate_args(&core.columns);
        let num_having_agg_args = core
            .having
            .as_ref()
            .map(|h| self.count_aggregate_args_in_expr(h))
            .unwrap_or(0);
        let num_agg_args = num_col_agg_args + num_having_agg_args;
        let num_non_agg_cols =
            self.count_non_agg_result_cols(&core.columns, Some(&resolved_group_by));
        let total_sorter_cols = num_group_cols + num_agg_args + num_non_agg_cols;

        // Open sorter for grouping
        let sorter_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenEphemeral,
            sorter_cursor,
            total_sorter_cols as i32,
            0,
            P4::Unused,
        );

        // Collect table cursors and join types to avoid borrow checker issues
        // Skip outer tables (for correlated subqueries) - only close this query's tables
        let table_cursors: Vec<i32> = self
            .tables
            .iter()
            .skip(self.outer_tables_boundary)
            .map(|t| t.cursor)
            .collect();
        let table_join_types: Vec<JoinType> = self
            .tables
            .iter()
            .skip(self.outer_tables_boundary)
            .map(|t| t.join_type)
            .collect();

        // Allocate found_match registers for LEFT JOINs
        let mut found_match_regs: Vec<Option<i32>> = Vec::with_capacity(table_cursors.len());
        for (i, join_type) in table_join_types.iter().enumerate() {
            let is_outer_join = join_type.is_outer();
            let found_match_reg = if is_outer_join && i > 0 {
                let reg = self.alloc_reg();
                Some(reg)
            } else {
                None
            };
            found_match_regs.push(found_match_reg);
        }

        // Generate proper nested loop structure for cross joins and LEFT JOINs
        let mut loop_labels: Vec<i32> = Vec::with_capacity(table_cursors.len());
        let mut done_labels: Vec<i32> = Vec::with_capacity(table_cursors.len());
        let mut null_row_labels: Vec<Option<i32>> = Vec::with_capacity(table_cursors.len());
        let mut after_null_labels: Vec<Option<i32>> = Vec::with_capacity(table_cursors.len());

        for (i, cursor) in table_cursors.iter().enumerate() {
            let is_left_join_inner = i > 0 && table_join_types[i].is_outer();

            // Initialize found_match for THIS table if it's an outer join
            if let Some(reg) = found_match_regs[i] {
                self.emit(Opcode::Integer, 0, reg, 0, P4::Unused);
            }

            let done_label = self.alloc_label();

            if is_left_join_inner {
                // For LEFT JOIN inner table: Rewind jumps to null_row handling if empty
                let null_row_label = self.alloc_label();
                self.emit(Opcode::Rewind, *cursor, null_row_label, 0, P4::Unused);
                null_row_labels.push(Some(null_row_label));
                after_null_labels.push(Some(self.alloc_label()));
            } else {
                self.emit(Opcode::Rewind, *cursor, done_label, 0, P4::Unused);
                null_row_labels.push(None);
                after_null_labels.push(None);
            }

            done_labels.push(done_label);

            // Mark loop start for this level
            let loop_label = self.alloc_label();
            self.resolve_label(loop_label, self.current_addr());
            loop_labels.push(loop_label);
        }

        // The innermost loop label is used for WHERE skip target
        let _loop_start_label = *loop_labels.last().unwrap_or(&self.alloc_label());

        // Evaluate WHERE clause
        let where_skip_label = if let Some(where_expr) = &core.where_clause {
            let label = self.alloc_label();
            self.compile_where_condition(where_expr, label)?;
            Some(label)
        } else {
            None
        };

        // Evaluate GROUP BY expressions (with aliases resolved) and store in sorter
        let group_regs = self.compile_expressions(&resolved_group_by)?;

        // Evaluate aggregate arguments from result columns
        let agg_arg_regs = self.compile_aggregate_args(&core.columns)?;

        // Also evaluate aggregate arguments from HAVING clause
        let having_agg_args_count = if let Some(having) = &core.having {
            self.compile_aggregate_args_in_expr(having)?
        } else {
            0
        };

        // Evaluate non-aggregate result columns (these can't be read after table cursors close)
        let (non_agg_base_reg, non_agg_count, non_agg_indices) =
            self.compile_non_agg_result_cols(&core.columns, Some(&resolved_group_by))?;

        // Store non-agg sorter offset for use during accumulation
        // Offset in sorter is num_group_cols + num_agg_args
        let non_agg_sorter_offset = num_group_cols + num_agg_args;

        // Copy all values into contiguous registers for MakeRecord.
        // compile_expr allocates temporary registers, so the result registers may not be contiguous.
        let total_cols = group_regs.1 + agg_arg_regs.1 + having_agg_args_count + non_agg_count;
        let contiguous_base = self.alloc_regs(total_cols);
        let mut dest_offset = 0;

        // Copy group columns
        for i in 0..group_regs.1 {
            self.emit(
                Opcode::Copy,
                group_regs.0 + i as i32,
                contiguous_base + dest_offset,
                0,
                P4::Unused,
            );
            dest_offset += 1;
        }

        // Copy aggregate arguments
        for i in 0..agg_arg_regs.1 {
            self.emit(
                Opcode::Copy,
                agg_arg_regs.0 + i as i32,
                contiguous_base + dest_offset,
                0,
                P4::Unused,
            );
            dest_offset += 1;
        }

        // Copy HAVING aggregate arguments (they follow immediately after regular agg args)
        for i in 0..having_agg_args_count {
            self.emit(
                Opcode::Copy,
                agg_arg_regs.0 + agg_arg_regs.1 as i32 + i as i32,
                contiguous_base + dest_offset,
                0,
                P4::Unused,
            );
            dest_offset += 1;
        }

        // Copy non-aggregate result columns
        for i in 0..non_agg_count {
            self.emit(
                Opcode::Copy,
                non_agg_base_reg + i as i32,
                contiguous_base + dest_offset,
                0,
                P4::Unused,
            );
            dest_offset += 1;
        }
        // MakeRecord from the contiguous block
        let record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            contiguous_base,
            total_cols as i32,
            record_reg,
            P4::Unused,
        );
        // Mark found_match=1 for any LEFT JOIN inner tables
        for reg in found_match_regs.iter().flatten() {
            self.emit(Opcode::Integer, 1, *reg, 0, P4::Unused);
        }

        self.emit(
            Opcode::SorterInsert,
            sorter_cursor,
            record_reg,
            0,
            P4::Unused,
        );

        // WHERE skip target
        if let Some(label) = where_skip_label {
            self.resolve_label(label, self.current_addr());
        }

        // Generate Next for each table in reverse order (innermost first)
        for i in (0..table_cursors.len()).rev() {
            let cursor = table_cursors[i];
            let loop_label = loop_labels[i];

            // Next on this cursor, jump back to its loop start
            self.emit(Opcode::Next, cursor, loop_label, 0, P4::Unused);

            // For LEFT JOIN: handle empty/exhausted inner table
            if let Some(found_match_reg) = found_match_regs[i] {
                // If found_match > 0, skip null row handling
                if let Some(after_null_label) = after_null_labels[i] {
                    self.emit(
                        Opcode::IfPos,
                        found_match_reg,
                        after_null_label,
                        0,
                        P4::Unused,
                    );
                }

                // Resolve null_row_label here - both Rewind-when-empty AND loop-no-match land here
                if let Some(null_row_label) = null_row_labels[i] {
                    self.resolve_label(null_row_label, self.current_addr());
                }

                // Set cursor to null row mode
                self.emit(Opcode::NullRow, cursor, 0, 0, P4::Unused);

                // Re-evaluate expressions with NULL values and insert into sorter
                // For GROUP BY queries, we need to re-compute the group columns, agg args, and non-agg cols
                // with the cursor in NullRow mode, then insert into sorter

                // Re-evaluate GROUP BY expressions
                let null_group_regs = self.compile_expressions(&resolved_group_by)?;

                // Re-evaluate aggregate arguments
                let null_agg_arg_regs = self.compile_aggregate_args(&core.columns)?;

                // Re-evaluate HAVING aggregate args
                let null_having_args = if let Some(having) = &core.having {
                    self.compile_aggregate_args_in_expr(having)?
                } else {
                    0
                };

                // Re-evaluate non-aggregate result columns
                let (null_non_agg_base, null_non_agg_count, _) =
                    self.compile_non_agg_result_cols(&core.columns, Some(&resolved_group_by))?;

                // Copy to contiguous registers for MakeRecord
                let null_total_cols =
                    null_group_regs.1 + null_agg_arg_regs.1 + null_having_args + null_non_agg_count;
                let null_contiguous_base = self.alloc_regs(null_total_cols);
                let mut null_dest_offset = 0;

                // Copy group columns
                for j in 0..null_group_regs.1 {
                    self.emit(
                        Opcode::Copy,
                        null_group_regs.0 + j as i32,
                        null_contiguous_base + null_dest_offset,
                        0,
                        P4::Unused,
                    );
                    null_dest_offset += 1;
                }

                // Copy aggregate arguments
                for j in 0..null_agg_arg_regs.1 {
                    self.emit(
                        Opcode::Copy,
                        null_agg_arg_regs.0 + j as i32,
                        null_contiguous_base + null_dest_offset,
                        0,
                        P4::Unused,
                    );
                    null_dest_offset += 1;
                }

                // Copy HAVING aggregate arguments
                for j in 0..null_having_args {
                    self.emit(
                        Opcode::Copy,
                        null_agg_arg_regs.0 + null_agg_arg_regs.1 as i32 + j as i32,
                        null_contiguous_base + null_dest_offset,
                        0,
                        P4::Unused,
                    );
                    null_dest_offset += 1;
                }

                // Copy non-aggregate columns
                for j in 0..null_non_agg_count {
                    self.emit(
                        Opcode::Copy,
                        null_non_agg_base + j as i32,
                        null_contiguous_base + null_dest_offset,
                        0,
                        P4::Unused,
                    );
                    null_dest_offset += 1;
                }

                // MakeRecord and SorterInsert
                let null_record_reg = self.alloc_reg();
                self.emit(
                    Opcode::MakeRecord,
                    null_contiguous_base,
                    null_total_cols as i32,
                    null_record_reg,
                    P4::Unused,
                );
                self.emit(
                    Opcode::SorterInsert,
                    sorter_cursor,
                    null_record_reg,
                    0,
                    P4::Unused,
                );

                // Resolve after_null_label
                if let Some(after_null_label) = after_null_labels[i] {
                    self.resolve_label(after_null_label, self.current_addr());
                }
            }

            // Resolve done label for this level
            self.resolve_label(done_labels[i], self.current_addr());
        }

        // Close table cursors
        for cursor in &table_cursors {
            self.emit(Opcode::Close, *cursor, 0, 0, P4::Unused);
        }

        // Sort the results
        let sort_done_label = self.alloc_label();
        self.emit(
            Opcode::SorterSort,
            sorter_cursor,
            sort_done_label,
            0,
            P4::Unused,
        );

        // Initialize aggregates from result columns
        let mut agg_regs = self.init_aggregates(&core.columns)?;

        // Also initialize HAVING aggregates
        let num_having_aggs = core
            .having
            .as_ref()
            .map(|h| self.count_aggregates_in_expr(h))
            .unwrap_or(0);
        for _ in 0..num_having_aggs {
            let reg = self.alloc_reg();
            self.emit(Opcode::Null, 0, reg, 0, P4::Unused);
            agg_regs.push(reg);
        }

        // Previous group key registers
        let prev_group_regs = self.alloc_regs(num_group_cols);
        self.emit(
            Opcode::Null,
            0,
            prev_group_regs,
            prev_group_regs + num_group_cols as i32 - 1,
            P4::Unused,
        );

        // Flag to track if this is the first group (0 = first, 1 = not first)
        // We can't use IsNull on prev_group_regs because the first group's key might be NULL
        let first_group_flag = self.alloc_reg();
        self.emit(Opcode::Integer, 0, first_group_flag, 0, P4::Unused);

        // Allocate registers to store non-aggregate values for the current group
        // These are updated during accumulation and used during finalization
        let prev_non_agg_regs = if non_agg_count > 0 {
            let regs = self.alloc_regs(non_agg_count);
            self.emit(
                Opcode::Null,
                0,
                regs,
                regs + non_agg_count as i32 - 1,
                P4::Unused,
            );
            // Store the base register and indices for finalization
            self.non_agg_saved_regs = Some((regs, non_agg_indices.clone()));
            Some(regs)
        } else {
            None
        };

        // Use label to avoid collision with resolve_labels
        let sorter_loop_start_label = self.alloc_label();
        self.resolve_label(sorter_loop_start_label, self.current_addr());

        // Get current row from sorter
        let sorter_data_reg = self.alloc_reg();
        self.emit(
            Opcode::SorterData,
            sorter_cursor,
            sorter_data_reg,
            0,
            P4::Unused,
        );

        // Extract group columns
        let curr_group_regs = self.alloc_regs(num_group_cols);
        for i in 0..num_group_cols {
            self.emit(
                Opcode::Column,
                sorter_cursor,
                i as i32,
                curr_group_regs + i as i32,
                P4::Unused,
            );
        }

        // Check if this is the first row - must be done BEFORE Compare because
        // if the first group key is NULL, it would compare equal to the initialized NULL prev_group_regs
        let first_group_label = self.alloc_label();
        self.emit(
            Opcode::IfNot,
            first_group_flag,
            first_group_label,
            0,
            P4::Unused,
        );

        // Compare with previous group (only after first row has been processed)
        let same_group_label = self.alloc_label();
        self.emit(
            Opcode::Compare,
            prev_group_regs,
            curr_group_regs,
            num_group_cols as i32,
            P4::Unused,
        );
        // Jump to same_group_label when compare result = 0 (same group)
        self.emit(Opcode::Jump, 0, same_group_label, 0, P4::Unused);

        // New group - output previous group (we know it's not the first because we checked above)

        // Finalize and output previous group
        // Save column names length - finalize_aggregates_with_group adds to result_column_names
        // but we only want the names added once (first iteration only)
        let saved_result_column_names_prev = self.result_column_names.len();
        let result_regs = self.finalize_aggregates_with_group(
            &core.columns,
            &agg_regs,
            Some(&resolved_group_by),
            prev_group_regs,
        )?;
        // Only keep names from first group output (truncate to saved length unless this is first)
        if saved_result_column_names_prev > 0 {
            self.result_column_names
                .truncate(saved_result_column_names_prev);
        }

        // Finalize HAVING aggregates (aggregates that appear in HAVING but not in result columns)
        // and add them to agg_final_regs so they can be found during HAVING compilation
        if num_having_aggs > 0 {
            let having_agg_start_idx = agg_regs.len() - num_having_aggs;
            for i in 0..num_having_aggs {
                let agg_reg = agg_regs[having_agg_start_idx + i];
                let result_reg = self.alloc_reg();
                // Get the aggregate function name from the HAVING expression
                let agg_name = self.get_aggregate_name_at_index(core.having.as_ref().unwrap(), i);
                self.emit(Opcode::AggFinal, agg_reg, result_reg, 0, P4::Text(agg_name));
                self.agg_final_regs.push(result_reg);
            }
        }

        // HAVING clause
        // Create skip label for LIMIT/OFFSET - when offset skips, continue to next group
        let after_group_output_label = self.alloc_label();
        if let Some(having) = &core.having {
            // Reset agg_final_idx so HAVING expression can find finalized aggregates
            self.agg_final_idx = 0;
            let skip_output_label = self.alloc_label();
            self.compile_where_condition(having, skip_output_label)?;
            self.output_row_with_limit(
                dest,
                result_regs.0,
                result_regs.1,
                after_group_output_label,
            )?;
            self.resolve_label(skip_output_label, self.current_addr());
        } else {
            self.output_row_with_limit(
                dest,
                result_regs.0,
                result_regs.1,
                after_group_output_label,
            )?;
        }
        self.resolve_label(after_group_output_label, self.current_addr());

        // Clear group column substitution context after HAVING is compiled
        // Note: Don't clear non_agg_saved_regs here - it's needed for subsequent group outputs
        self.group_column_regs.clear();
        // Clear agg_final_regs after HAVING is done (will be repopulated for next group)
        self.agg_final_regs.clear();

        self.resolve_label(first_group_label, self.current_addr());

        // Mark that we've processed at least one group
        self.emit(Opcode::Integer, 1, first_group_flag, 0, P4::Unused);

        // Reset aggregates for new group
        self.reset_aggregates(&agg_regs)?;

        // Copy current group to previous
        for i in 0..num_group_cols {
            self.emit(
                Opcode::Copy,
                curr_group_regs + i as i32,
                prev_group_regs + i as i32,
                0,
                P4::Unused,
            );
        }

        // Copy non-aggregate values from sorter to saved registers
        // This is done BEFORE same_group_label so it only happens on the FIRST row
        // of each group (not on subsequent rows where we jump directly to same_group_label)
        if let Some(prev_regs) = prev_non_agg_regs {
            for (idx, maybe_offset) in non_agg_indices.iter().enumerate() {
                if let Some(offset) = maybe_offset {
                    let sorter_col = (non_agg_sorter_offset + offset) as i32;
                    let dest_reg = prev_regs + *offset as i32;
                    self.emit(
                        Opcode::Column,
                        sorter_cursor,
                        sorter_col,
                        dest_reg,
                        P4::Unused,
                    );
                }
            }
        }

        self.resolve_label(same_group_label, self.current_addr());

        // Accumulate current row into aggregates from result columns
        let agg_col_start = num_group_cols;
        self.accumulate_from_sorter(sorter_cursor, &core.columns, &agg_regs, agg_col_start)?;

        // Also accumulate HAVING aggregates
        if num_having_aggs > 0 {
            if let Some(having) = &core.having {
                let having_agg_start_idx = agg_regs.len() - num_having_aggs;
                let mut having_agg_idx = having_agg_start_idx;
                // HAVING aggregate args start after column aggregate args in the sorter
                let mut having_col_idx = num_group_cols + num_col_agg_args;
                self.accumulate_from_sorter_in_expr(
                    sorter_cursor,
                    having,
                    &agg_regs,
                    &mut having_agg_idx,
                    &mut having_col_idx,
                )?;
            }
        }

        // Next sorter row
        self.emit(
            Opcode::SorterNext,
            sorter_cursor,
            sorter_loop_start_label,
            0,
            P4::Unused,
        );

        // Output final group
        // Save column names length - finalize_aggregates_with_group adds to result_column_names
        // but we only want the names added once (they were added during the first group output)
        let saved_result_column_names = self.result_column_names.len();
        let result_regs = self.finalize_aggregates_with_group(
            &core.columns,
            &agg_regs,
            Some(&resolved_group_by),
            prev_group_regs,
        )?;
        // Restore column names (don't double-count for final group output)
        self.result_column_names.truncate(saved_result_column_names);

        // Finalize HAVING aggregates for final group
        if num_having_aggs > 0 {
            let having_agg_start_idx = agg_regs.len() - num_having_aggs;
            for i in 0..num_having_aggs {
                let agg_reg = agg_regs[having_agg_start_idx + i];
                let result_reg = self.alloc_reg();
                let agg_name = self.get_aggregate_name_at_index(core.having.as_ref().unwrap(), i);
                self.emit(Opcode::AggFinal, agg_reg, result_reg, 0, P4::Text(agg_name));
                self.agg_final_regs.push(result_reg);
            }
        }

        // Create skip label for LIMIT/OFFSET - when offset skips final group, just continue to end
        let after_final_output_label = self.alloc_label();
        if let Some(having) = &core.having {
            // Reset agg_final_idx so HAVING expression can find finalized aggregates
            self.agg_final_idx = 0;
            let skip_output_label = self.alloc_label();
            self.compile_where_condition(having, skip_output_label)?;
            self.output_row_with_limit(
                dest,
                result_regs.0,
                result_regs.1,
                after_final_output_label,
            )?;
            self.resolve_label(skip_output_label, self.current_addr());
        } else {
            self.output_row_with_limit(
                dest,
                result_regs.0,
                result_regs.1,
                after_final_output_label,
            )?;
        }
        self.resolve_label(after_final_output_label, self.current_addr());

        // Clear group column substitution context after HAVING is compiled
        self.group_column_regs.clear();
        self.non_agg_saved_regs = None;
        self.agg_final_regs.clear();

        self.resolve_label(sort_done_label, self.current_addr());

        // Resolve limit_done_label to exit point (same as sort_done)
        if let Some(done_label) = self.limit_done_label {
            self.resolve_label(done_label, self.current_addr());
        }

        // Close sorter
        self.emit(Opcode::Close, sorter_cursor, 0, 0, P4::Unused);

        Ok(())
    }

    /// Compile UNION/INTERSECT/EXCEPT
    fn compile_compound(
        &mut self,
        op: CompoundOp,
        left: &SelectBody,
        right: &SelectBody,
        dest: &SelectDest,
    ) -> Result<()> {
        // Validate column count consistency for the entire compound tree
        // This finds the first (innermost) mismatch in depth-first order
        self.validate_compound_column_counts(op, left, right)?;

        self.is_compound = true;

        // Save LIMIT/OFFSET counters - they should be applied to the final output, not individual bodies
        let saved_limit_reg = self.limit_counter_reg.take();
        let saved_offset_reg = self.offset_counter_reg.take();
        let saved_limit_done = self.limit_done_label.take();

        // Create ephemeral table for results
        let result_cursor = self.alloc_cursor();
        self.emit(Opcode::OpenEphemeral, result_cursor, 0, 0, P4::Unused);

        // Compile left side into ephemeral table
        // Clear tables and result column names to avoid accumulating from parent context
        // Also reset outer_tables_boundary since we're starting fresh - compound query bodies
        // don't inherit the outer scope's tables (they're not correlated subqueries)
        self.tables.clear();
        self.outer_tables_boundary = 0;
        self.result_column_names.clear();
        // For UNION, INTERSECT, EXCEPT we need to deduplicate the left side
        // For UNION ALL, we don't need deduplication
        let left_dest = if matches!(op, CompoundOp::UnionAll) {
            SelectDest::EphemTable {
                cursor: result_cursor,
            }
        } else {
            SelectDest::EphemTableDistinct {
                cursor: result_cursor,
            }
        };
        self.compile_body(left, &left_dest)?;

        // Track column count from left side for output
        self.compound_column_count = self.result_column_names.len();
        // Save the left side's column names (right side will add more but we only want left's names)
        let saved_column_names = self.result_column_names.clone();

        // Track if we need sorted output (UNION, INTERSECT, EXCEPT all return sorted results)
        let needs_sorted_output = !matches!(op, CompoundOp::UnionAll);

        // Track right cursor for INTERSECT/EXCEPT filtering
        let mut right_cursor_for_filtering: Option<i32> = None;

        match op {
            CompoundOp::UnionAll => {
                // Clear tables before compiling right side (but keep column names from left)
                self.tables.clear();
                self.outer_tables_boundary = 0;
                // Just add right side to same table
                self.compile_body(right, &left_dest)?;
            }
            CompoundOp::Union => {
                // Clear tables before compiling right side
                self.tables.clear();
                self.outer_tables_boundary = 0;
                // Right side goes to separate table, then merge with distinct
                let right_cursor = self.alloc_cursor();
                self.emit(Opcode::OpenEphemeral, right_cursor, 0, 0, P4::Unused);
                let right_dest = SelectDest::EphemTable {
                    cursor: right_cursor,
                };
                self.compile_body(right, &right_dest)?;

                // Merge with distinct
                self.merge_distinct(result_cursor, right_cursor)?;
                self.emit(Opcode::Close, right_cursor, 0, 0, P4::Unused);
            }
            CompoundOp::Intersect => {
                // Clear tables before compiling right side
                self.tables.clear();
                self.outer_tables_boundary = 0;
                // Keep only rows that appear in both
                let right_cursor = self.alloc_cursor();
                self.emit(Opcode::OpenEphemeral, right_cursor, 0, 0, P4::Unused);
                let right_dest = SelectDest::EphemTable {
                    cursor: right_cursor,
                };
                self.compile_body(right, &right_dest)?;
                right_cursor_for_filtering = Some(right_cursor);
            }
            CompoundOp::Except => {
                // Clear tables before compiling right side
                self.tables.clear();
                self.outer_tables_boundary = 0;
                // Remove rows that appear in right
                let right_cursor = self.alloc_cursor();
                self.emit(Opcode::OpenEphemeral, right_cursor, 0, 0, P4::Unused);
                let right_dest = SelectDest::EphemTable {
                    cursor: right_cursor,
                };
                self.compile_body(right, &right_dest)?;
                right_cursor_for_filtering = Some(right_cursor);
            }
        }

        // Capture aliases from the right side's columns for ORDER BY resolution
        // Right side columns are at positions [left_col_count..], map them to [0..]
        let left_col_count = saved_column_names.len();
        for (i, name) in self
            .result_column_names
            .iter()
            .enumerate()
            .skip(left_col_count)
        {
            let result_pos = i - left_col_count;
            self.compound_aliases
                .insert(name.to_lowercase(), result_pos);
        }

        // Restore left side's column names (right side added its own but we want only left's names)
        self.result_column_names = saved_column_names;

        // Restore LIMIT/OFFSET counters for the final output phase
        self.limit_counter_reg = saved_limit_reg;
        self.offset_counter_reg = saved_offset_reg;
        self.limit_done_label = saved_limit_done;

        // Output results from ephemeral table with conditional filtering for INTERSECT/EXCEPT
        match op {
            CompoundOp::Intersect => {
                // INTERSECT: Output only rows from left that also appear in right
                if let Some(right_cursor) = right_cursor_for_filtering {
                    self.output_ephemeral_table_intersect(
                        result_cursor,
                        right_cursor,
                        dest,
                        needs_sorted_output,
                    )?;
                    self.emit(Opcode::Close, right_cursor, 0, 0, P4::Unused);
                }
            }
            CompoundOp::Except => {
                // EXCEPT: Output only rows from left that do NOT appear in right
                if let Some(right_cursor) = right_cursor_for_filtering {
                    self.output_ephemeral_table_except(
                        result_cursor,
                        right_cursor,
                        dest,
                        needs_sorted_output,
                    )?;
                    self.emit(Opcode::Close, right_cursor, 0, 0, P4::Unused);
                }
            }
            _ => {
                // UNION, UNION ALL: Regular output
                if needs_sorted_output {
                    self.output_ephemeral_table_sorted(result_cursor, dest)?;
                } else {
                    self.output_ephemeral_table(result_cursor, dest)?;
                }
            }
        }

        self.emit(Opcode::Close, result_cursor, 0, 0, P4::Unused);

        Ok(())
    }

    /// Count the number of result columns in a SelectBody (for compound query validation)
    fn count_select_body_columns(&self, body: &SelectBody) -> usize {
        match body {
            SelectBody::Select(core) => self.count_select_core_columns(core),
            SelectBody::Compound { left, .. } => {
                // For compound queries, column count comes from the leftmost SELECT
                self.count_select_body_columns(left)
            }
        }
    }

    /// Validate column count consistency for compound queries
    /// Recursively validates the entire compound tree and returns error for the first (innermost) mismatch
    fn validate_compound_column_counts(
        &self,
        op: CompoundOp,
        left: &SelectBody,
        right: &SelectBody,
    ) -> Result<()> {
        // First, recursively validate any nested compound queries in the left subtree
        if let SelectBody::Compound {
            op: left_op,
            left: left_left,
            right: left_right,
        } = left
        {
            self.validate_compound_column_counts(*left_op, left_left, left_right)?;
        }

        // Then validate any nested compound queries in the right subtree
        if let SelectBody::Compound {
            op: right_op,
            left: right_left,
            right: right_right,
        } = right
        {
            self.validate_compound_column_counts(*right_op, right_left, right_right)?;
        }

        // Now validate the current level
        let left_cols = self.count_select_body_columns(left);
        let right_cols = self.count_select_body_columns(right);
        if left_cols != right_cols {
            let op_name = match op {
                CompoundOp::Union => "UNION",
                CompoundOp::UnionAll => "UNION ALL",
                CompoundOp::Intersect => "INTERSECT",
                CompoundOp::Except => "EXCEPT",
            };
            return Err(Error::with_message(
                ErrorCode::Error,
                format!(
                    "SELECTs to the left and right of {} do not have the same number of result columns",
                    op_name
                ),
            ));
        }

        Ok(())
    }

    /// Get result column names from a SelectBody without compiling
    /// This is used for early ORDER BY validation in compound SELECTs
    fn get_select_body_column_names(&self, body: &SelectBody) -> Vec<String> {
        match body {
            SelectBody::Select(core) => self.get_select_core_column_names(core),
            SelectBody::Compound { left, .. } => {
                // For compound queries, column names come from the leftmost SELECT
                self.get_select_body_column_names(left)
            }
        }
    }

    /// Get ALL column names from a compound SelectBody (from all parts)
    /// This is used for ORDER BY validation where columns from any part are valid
    fn get_all_compound_column_names(&self, body: &SelectBody) -> Vec<String> {
        let mut names = Vec::new();
        self.collect_compound_column_names(body, &mut names);
        names
    }

    /// Helper to collect column names from all parts of a compound SELECT
    fn collect_compound_column_names(&self, body: &SelectBody, names: &mut Vec<String>) {
        match body {
            SelectBody::Select(core) => {
                for name in self.get_select_core_column_names(core) {
                    if !names.iter().any(|n| n.eq_ignore_ascii_case(&name)) {
                        names.push(name);
                    }
                }
            }
            SelectBody::Compound { left, right, .. } => {
                self.collect_compound_column_names(left, names);
                self.collect_compound_column_names(right, names);
            }
        }
    }

    /// Get result column names from a SelectCore without compiling
    fn get_select_core_column_names(&self, core: &SelectCore) -> Vec<String> {
        let mut names = Vec::new();
        for col in &core.columns {
            match col {
                ResultColumn::Star => {
                    // For *, get column names from all tables in FROM
                    if let Some(from) = &core.from {
                        if let Some(schema) = self.schema {
                            for table_ref in &from.tables {
                                if let TableRef::Table { name, .. } = table_ref {
                                    if let Some(table) = schema.table(&name.name) {
                                        for col_def in &table.columns {
                                            names.push(col_def.name.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // If we couldn't get column names, add a placeholder
                    if names.is_empty() {
                        names.push("*".to_string());
                    }
                }
                ResultColumn::TableStar(table_name) => {
                    // For table.*, get columns from that table
                    if let Some(schema) = self.schema {
                        if let Some(table) = schema.table(table_name) {
                            for col_def in &table.columns {
                                names.push(col_def.name.clone());
                            }
                        } else {
                            names.push(format!("{}.*", table_name));
                        }
                    } else {
                        names.push(format!("{}.*", table_name));
                    }
                }
                ResultColumn::Expr { expr, alias } => {
                    if let Some(alias_name) = alias {
                        names.push(alias_name.clone());
                    } else {
                        // Try to extract a name from the expression
                        let name = match expr {
                            Expr::Column(col_ref) => col_ref.column.clone(),
                            Expr::Literal(Literal::Integer(n)) => n.to_string(),
                            Expr::Literal(Literal::String(s)) => s.clone(),
                            _ => format!("column{}", names.len() + 1),
                        };
                        names.push(name);
                    }
                }
            }
        }
        names
    }

    /// Check if ORDER BY term is valid given a list of column names (for early validation)
    fn is_valid_order_by_for_columns(&self, expr: &Expr, column_names: &[String]) -> bool {
        match expr {
            // Integer literal = column position (1-based)
            Expr::Literal(Literal::Integer(n)) => {
                let pos = *n as usize;
                pos >= 1 && pos <= column_names.len()
            }
            // Column reference - must match a column name
            Expr::Column(col_ref) => {
                let col_name = &col_ref.column;
                column_names
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(col_name))
            }
            // Other expressions - allow for now (will be validated during compilation)
            _ => true,
        }
    }

    /// Check if ORDER BY term is valid for compound SELECT (early validation)
    /// column_names includes names from ALL parts of the compound
    /// result_column_count is the number of result columns (from leftmost SELECT)
    fn is_valid_order_by_for_compound(
        &self,
        expr: &Expr,
        column_names: &[String],
        result_column_count: usize,
    ) -> bool {
        match expr {
            // Integer literal = column position (1-based), must be within result column count
            Expr::Literal(Literal::Integer(n)) => {
                let pos = *n as usize;
                pos >= 1 && pos <= result_column_count
            }
            // String literal in double quotes is treated as identifier in SQLite
            // This case is handled by the parser converting it to Column
            Expr::Literal(Literal::String(_)) => {
                // String literals are not valid ORDER BY terms for compound SELECTs
                // (they should be column references if quoted with double quotes)
                true // Let compilation handle this
            }
            // Column reference - must match a column name from any part of the compound
            Expr::Column(col_ref) => {
                let col_name = &col_ref.column;
                column_names
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(col_name))
            }
            // Other expressions - allow for now (will be validated during compilation)
            _ => true,
        }
    }

    /// Validate ORDER BY term for compound SELECT, returning error message if invalid
    /// Returns None if valid, Some(error_message) if invalid
    fn validate_order_by_for_compound(
        &self,
        expr: &Expr,
        column_names: &[String],
        result_column_count: usize,
        term_idx: usize,
    ) -> Option<String> {
        let ordinal = match term_idx {
            0 => "1st".to_string(),
            1 => "2nd".to_string(),
            2 => "3rd".to_string(),
            n => format!("{}th", n + 1),
        };

        match expr {
            // Integer literal = column position (1-based), must be within result column count
            Expr::Literal(Literal::Integer(n)) => {
                let pos = *n as usize;
                if pos >= 1 && pos <= result_column_count {
                    None // Valid
                } else {
                    Some(format!(
                        "{} ORDER BY term out of range - should be between 1 and {}",
                        ordinal, result_column_count
                    ))
                }
            }
            // Column reference - must match a column name from any part of the compound
            Expr::Column(col_ref) => {
                let col_name = &col_ref.column;
                if column_names
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(col_name))
                {
                    None // Valid
                } else {
                    Some(format!(
                        "{} ORDER BY term does not match any column in the result set",
                        ordinal
                    ))
                }
            }
            // Other expressions - allow for now (will be validated during compilation)
            _ => None,
        }
    }

    /// Count columns in a SelectCore, handling * expansion
    fn count_select_core_columns(&self, core: &SelectCore) -> usize {
        let mut count = 0;
        for col in &core.columns {
            match col {
                ResultColumn::Star => {
                    // For *, count all columns from all tables in FROM
                    if let Some(from) = &core.from {
                        if let Some(schema) = self.schema {
                            for table_ref in &from.tables {
                                if let TableRef::Table { name, .. } = table_ref {
                                    if let Some(table) = schema.table(&name.name) {
                                        count += table.columns.len();
                                    }
                                }
                            }
                        }
                    }
                    // If we couldn't determine, assume 1
                    if count == 0 {
                        count = 1;
                    }
                }
                ResultColumn::TableStar(table_name) => {
                    // For table.*, count columns from that table
                    if let Some(schema) = self.schema {
                        if let Some(table) = schema.table(table_name) {
                            count += table.columns.len();
                        } else {
                            count += 1; // Fallback
                        }
                    } else {
                        count += 1; // Fallback
                    }
                }
                ResultColumn::Expr { .. } => {
                    count += 1;
                }
            }
        }
        count.max(1) // At least 1 column
    }

    /// Count the number of terms in a compound SELECT (UNION/INTERSECT/EXCEPT)
    /// Returns 1 for a simple SELECT, and N for N SELECT statements joined by compound operators.
    fn count_compound_terms(body: &SelectBody) -> usize {
        match body {
            SelectBody::Select(_) => 1,
            SelectBody::Compound { left, right, .. } => {
                Self::count_compound_terms(left) + Self::count_compound_terms(right)
            }
        }
    }

    /// Compile FROM clause - open cursors for tables
    ///
    /// This converts the FROM clause to a flat SrcList (like SQLite) and then
    /// opens cursors for each table. Join constraints (ON/USING/NATURAL) are
    /// collected and processed after all tables are registered.
    fn compile_from_clause(&mut self, from: &FromClause) -> Result<()> {
        // Convert tree structure to flat SrcList (SQLite model)
        let src_list = from.to_src_list();

        // Open cursors for each source item
        for (i, item) in src_list.items.iter().enumerate() {
            self.compile_src_item(item, i)?;
        }

        // Check table count limit (matches SQLite's BMS check in where.c)
        // Only count tables added by this FROM clause (not outer query tables)
        let local_tables = self.tables.len() - self.outer_tables_boundary;
        if local_tables > MAX_TABLES_IN_JOIN {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("at most {} tables in a join", MAX_TABLES_IN_JOIN),
            ));
        }

        // Process join constraints (NATURAL, USING, ON) and add to join_conditions
        self.process_joins(&src_list)?;

        Ok(())
    }

    /// Compile a single source item from the SrcList
    fn compile_src_item(
        &mut self,
        item: &crate::parser::ast::SrcItem,
        _index: usize,
    ) -> Result<()> {
        use crate::parser::ast::TableSource;

        match &item.source {
            TableSource::Table(name) => {
                let table_name = &name.name;
                let table_name_lower = table_name.to_lowercase();
                let open_name = name.to_string();

                if let Some((cursor, columns)) = self.cte_cursors.get(&table_name_lower) {
                    let display_name = item.alias.clone().unwrap_or_else(|| table_name.clone());
                    self.tables.push(TableInfo {
                        name: display_name,
                        table_name: table_name.clone(),
                        cursor: *cursor,
                        schema_table: None,
                        is_subquery: true,
                        join_type: item.join_type,
                        subquery_columns: Some(columns.clone()),
                    });
                    return Ok(());
                }

                let cursor = self.alloc_cursor();

                if let Some(cte) = self.ctes.get(&table_name_lower).cloned() {
                    let columns = if self.recursive_ctes.contains(&table_name_lower) {
                        self.compile_recursive_cte(&cte, cursor, &table_name_lower)?
                    } else {
                        let subquery_cols = self.compile_subquery_to_ephemeral(
                            &cte.query,
                            cursor,
                            Some(&table_name_lower),
                        )?;
                        if let Some(explicit) = &cte.columns {
                            if explicit.len() != subquery_cols.len() {
                                return Err(Error::with_message(
                                    ErrorCode::Error,
                                    format!(
                                        "table {} has {} values for {} columns",
                                        cte.name,
                                        subquery_cols.len(),
                                        explicit.len()
                                    ),
                                ));
                            }
                            explicit.clone()
                        } else {
                            subquery_cols
                        }
                    };
                    let display_name = item.alias.clone().unwrap_or_else(|| table_name.clone());
                    self.tables.push(TableInfo {
                        name: display_name,
                        table_name: table_name.clone(),
                        cursor,
                        schema_table: None,
                        is_subquery: true,
                        join_type: item.join_type,
                        subquery_columns: Some(columns),
                    });
                    return Ok(());
                }

                // Check if this is a view - expand views as subqueries
                // When inside a main view expansion, only check main schema for unqualified names
                // (SQLite: views bind to objects in their own database)
                // If there's an explicit database qualifier, respect it
                let view_opt = if let Some(ref schema_name) = name.schema {
                    // Explicit database qualifier
                    let schema_lower = schema_name.to_lowercase();
                    if schema_lower == "temp" {
                        self.temp_schema
                            .and_then(|s| s.views.get(&table_name_lower))
                    } else {
                        // main or other database - use main schema
                        self.schema.and_then(|s| s.views.get(&table_name_lower))
                    }
                } else if self.main_view_depth > 0 {
                    // Unqualified name inside main view: only look in main schema
                    self.schema.and_then(|s| s.views.get(&table_name_lower))
                } else {
                    // Unqualified name at top-level or in temp view: check temp first, then main
                    self.temp_schema
                        .and_then(|s| s.views.get(&table_name_lower))
                        .or_else(|| self.schema.and_then(|s| s.views.get(&table_name_lower)))
                };

                if let Some(view) = view_opt {
                    // Check if view access is disabled (temp views are always allowed)
                    if !self.enable_view && view.db_idx != 1 {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("access to view \"{}\" prohibited", view.name),
                        ));
                    }

                    // Check for circular view definition
                    if self.expanding_views.contains(&table_name_lower) {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("view {} is circularly defined", view.name),
                        ));
                    }

                    // Mark this view as being expanded
                    self.expanding_views.insert(table_name_lower.clone());

                    // Track if we're entering a main database view
                    let is_main_view = view.db_idx == 0;
                    if is_main_view {
                        self.main_view_depth += 1;
                    }

                    let view_select = (*view.select).clone();
                    let view_alias = item.alias.clone().unwrap_or_else(|| table_name.clone());
                    let view_columns = view.columns.clone();

                    // Compile view's SELECT as a subquery into ephemeral table
                    let result = self.compile_subquery_to_ephemeral(&view_select, cursor, None);

                    // Remove from expanding set and restore depth (whether success or failure)
                    self.expanding_views.remove(&table_name_lower);
                    if is_main_view {
                        self.main_view_depth -= 1;
                    }

                    let subquery_col_names = result?;

                    // Use view's explicit column names if defined, otherwise use subquery column names
                    let final_col_names = if let Some(explicit_cols) = view_columns {
                        // View has explicit column names like CREATE VIEW v(c1, c2) AS ...
                        explicit_cols
                    } else {
                        subquery_col_names
                    };

                    self.tables.push(TableInfo {
                        name: view_alias,
                        table_name: String::new(),
                        cursor,
                        schema_table: None,
                        is_subquery: true,
                        join_type: item.join_type,
                        subquery_columns: Some(final_col_names),
                    });
                    return Ok(());
                }

                // Look up table in schema if available
                let schema_table = if name.schema.is_some() {
                    self.lookup_table_schema_qualified(name)
                } else {
                    self.lookup_table_schema(&table_name_lower)
                };

                // If we have a schema and the table doesn't exist, return error
                // (Skip this check if no schema is set - for backwards compatibility with unit tests)
                if self.schema.is_some() && schema_table.is_none() {
                    // Use qualified name with database prefix for error message
                    // Only add "main." prefix when inside a view expansion (for consistency with SQLite)
                    let qualified_name = if let Some(ref schema) = name.schema {
                        format!("{}.{}", schema, table_name)
                    } else if self.main_view_depth > 0 {
                        // Inside a view expansion - use qualified name
                        format!("main.{}", table_name)
                    } else {
                        // Top-level query - use plain name
                        table_name.clone()
                    };
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        format!("no such table: {}", qualified_name),
                    ));
                }

                // Emit OpenRead for the table
                self.emit(Opcode::OpenRead, cursor, 0, 0, P4::Text(open_name));

                let display_name = item.alias.clone().unwrap_or_else(|| table_name.clone());
                self.tables.push(TableInfo {
                    name: display_name,
                    table_name: table_name.clone(),
                    cursor,
                    schema_table,
                    is_subquery: false,
                    join_type: item.join_type,
                    subquery_columns: None,
                });
            }
            TableSource::Subquery(query) => {
                let cursor = self.alloc_cursor();
                let subquery_col_names = self.compile_subquery_to_ephemeral(query, cursor, None)?;

                let subquery_name = item.alias.clone().unwrap_or_else(|| {
                    let name = format!("(subquery-{})", self.next_subquery);
                    self.next_subquery += 1;
                    name
                });
                self.tables.push(TableInfo {
                    name: subquery_name,
                    table_name: String::new(),
                    cursor,
                    schema_table: None,
                    is_subquery: true,
                    join_type: item.join_type,
                    subquery_columns: Some(subquery_col_names),
                });
            }
            TableSource::TableFunction { name, args: _ } => {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("Table-valued function {} not yet supported", name),
                ));
            }
        }
        Ok(())
    }

    /// Look up table schema, returning None if not found
    fn lookup_table_schema(&self, table_name_lower: &str) -> Option<std::sync::Arc<Table>> {
        if let Some(db_idx) = self.sqlite_master_db_idx(table_name_lower, None) {
            return Some(self.sqlite_master_table(table_name_lower, db_idx));
        }

        if let Some(temp_schema) = self.temp_schema {
            if let Some(table) = temp_schema.table(table_name_lower) {
                return Some(table.clone());
            }
        }
        if let Some(schema) = self.schema {
            if let Some(table) = schema.table(table_name_lower) {
                return Some(table.clone());
            }
        }
        for (_name, schema) in &self.attached_schemas {
            if let Some(table) = schema.table(table_name_lower) {
                return Some(table.clone());
            }
        }
        None
    }

    fn lookup_table_schema_qualified(
        &self,
        name: &crate::parser::ast::QualifiedName,
    ) -> Option<std::sync::Arc<Table>> {
        let table_name_lower = name.name.to_lowercase();
        if let Some(db_idx) = self.sqlite_master_db_idx(&table_name_lower, name.schema.as_deref()) {
            return Some(self.sqlite_master_table(&table_name_lower, db_idx));
        }
        match name.schema.as_deref() {
            Some("temp") => self
                .temp_schema
                .and_then(|schema| schema.table(&table_name_lower).map(|t| t.clone())),
            Some("main") => self
                .schema
                .and_then(|schema| schema.table(&table_name_lower).map(|t| t.clone())),
            Some(schema_name) => self
                .attached_schemas
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(schema_name))
                .and_then(|(_, schema)| schema.table(&table_name_lower).map(|t| t.clone())),
            None => self.lookup_table_schema(&table_name_lower),
        }
    }

    fn sqlite_master_db_idx(&self, table_name_lower: &str, schema: Option<&str>) -> Option<i32> {
        let is_sqlite_master = table_name_lower == "sqlite_master"
            || table_name_lower == "sqlite_schema"
            || table_name_lower == "sqlite_temp_master"
            || table_name_lower == "sqlite_temp_schema";
        if !is_sqlite_master {
            return None;
        }
        if table_name_lower.contains("temp") {
            return Some(1);
        }
        match schema {
            Some("temp") => Some(1),
            _ => Some(0),
        }
    }

    fn sqlite_master_table(&self, table_name_lower: &str, db_idx: i32) -> std::sync::Arc<Table> {
        use crate::schema::Column;

        std::sync::Arc::new(Table {
            name: table_name_lower.to_string(),
            db_idx,
            root_page: 1,
            columns: vec![
                Column {
                    name: "type".to_string(),
                    type_name: Some("TEXT".to_string()),
                    affinity: Affinity::Text,
                    ..Default::default()
                },
                Column {
                    name: "name".to_string(),
                    type_name: Some("TEXT".to_string()),
                    affinity: Affinity::Text,
                    ..Default::default()
                },
                Column {
                    name: "tbl_name".to_string(),
                    type_name: Some("TEXT".to_string()),
                    affinity: Affinity::Text,
                    ..Default::default()
                },
                Column {
                    name: "rootpage".to_string(),
                    type_name: Some("INTEGER".to_string()),
                    affinity: Affinity::Integer,
                    ..Default::default()
                },
                Column {
                    name: "sql".to_string(),
                    type_name: Some("TEXT".to_string()),
                    affinity: Affinity::Text,
                    ..Default::default()
                },
            ],
            primary_key: None,
            indexes: Vec::new(),
            without_rowid: false,
            strict: false,
            is_virtual: false,
            virtual_module: None,
            virtual_args: Vec::new(),
            foreign_keys: Vec::new(),
            checks: Vec::new(),
            autoincrement: false,
            sql: None,
            row_estimate: 0,
        })
    }

    fn table_name_matches(table: &TableInfo, name: &str) -> bool {
        // Only match on table.name (which is the alias if one was provided, or the table name otherwise)
        // When a table is aliased (e.g., "t1 AS t2"), only the alias ("t2") should match,
        // not the original table name ("t1"). This follows SQL standard behavior.
        table.name.eq_ignore_ascii_case(name)
    }

    fn column_index_in_table(&self, table: &TableInfo, column: &str) -> Option<i32> {
        if is_rowid_alias(column) {
            if let Some(schema_table) = &table.schema_table {
                if !schema_table.without_rowid {
                    return Some(-1);
                }
            }
        }

        if let Some(schema_table) = &table.schema_table {
            // Check if the column is the INTEGER PRIMARY KEY (rowid alias)
            if let Some(ipk_col_idx) = schema_table.rowid_alias_column() {
                if schema_table.columns[ipk_col_idx]
                    .name
                    .eq_ignore_ascii_case(column)
                {
                    return Some(-1); // Return -1 for rowid alias columns
                }
            }

            // For FTS3/FTS4/FTS5 tables, the table name itself is a valid pseudo-column
            // This is used in functions like snippet(tablename), offsets(tablename), etc.
            if let Some(ref module) = schema_table.virtual_module {
                if (module.eq_ignore_ascii_case("fts3")
                    || module.eq_ignore_ascii_case("fts4")
                    || module.eq_ignore_ascii_case("fts5"))
                    && table.table_name.eq_ignore_ascii_case(column)
                {
                    // Return -2 as a sentinel for "FTS table content" pseudo-column
                    // The actual value doesn't matter much as snippet() uses FTS context
                    return Some(-2);
                }
            }

            return schema_table
                .columns
                .iter()
                .position(|col| col.name.eq_ignore_ascii_case(column))
                .map(|idx| idx as i32);
        }

        if let Some(cols) = &table.subquery_columns {
            return cols
                .iter()
                .position(|col| col.eq_ignore_ascii_case(column))
                .map(|idx| idx as i32);
        }

        None
    }

    fn is_column_coalesced(&self, table_idx: usize, column_lower: &str) -> bool {
        self.coalesced_columns
            .get(&table_idx)
            .map(|cols| cols.contains(column_lower))
            .unwrap_or(false)
    }

    /// Get the affinity for a comparison operation.
    /// SQLite affinity rules for comparisons:
    /// - If either operand has NUMERIC affinity, use NUMERIC for coercion
    /// - If either operand has TEXT affinity (and neither has NUMERIC), use TEXT
    /// - Otherwise use BLOB (strict type ordering)
    fn get_comparison_affinity(&self, left: &Expr, right: &Expr) -> u16 {
        let left_affinity = self.get_expr_affinity(left);
        let right_affinity = self.get_expr_affinity(right);

        // If either side has numeric affinity, use NUMERIC for coercion
        if Self::is_numeric_affinity(left_affinity) || Self::is_numeric_affinity(right_affinity) {
            vdbe_affinity::NUMERIC
        } else if Self::is_text_affinity(left_affinity) || Self::is_text_affinity(right_affinity) {
            // If either side has TEXT affinity, use TEXT for coercion
            // This makes integer literals compare as text when compared with TEXT columns
            vdbe_affinity::TEXT
        } else {
            vdbe_affinity::BLOB
        }
    }

    /// Check if affinity is TEXT
    fn is_text_affinity(affinity: Option<Affinity>) -> bool {
        matches!(affinity, Some(Affinity::Text))
    }

    /// Get the affinity of an expression (for comparison purposes).
    /// Returns Some(Affinity) if the expression is a column with known affinity.
    fn get_expr_affinity(&self, expr: &Expr) -> Option<Affinity> {
        match expr {
            Expr::Column(col_ref) => self.get_column_affinity(col_ref),
            Expr::Parens(inner) => self.get_expr_affinity(inner),
            Expr::Cast { type_name, .. } => Some(Self::type_name_to_affinity(&type_name.name)),
            // Literals have their natural type, not numeric affinity for coercion purposes
            _ => None,
        }
    }

    /// Get the affinity of a column reference.
    fn get_column_affinity(&self, col_ref: &ColumnRef) -> Option<Affinity> {
        // Find the table for this column
        let tables_to_search: Vec<_> = if let Some(table_name) = &col_ref.table {
            self.tables
                .iter()
                .filter(|t| Self::table_name_matches(t, table_name))
                .collect()
        } else {
            // Search all tables for unqualified column
            self.tables.iter().collect()
        };

        for table in tables_to_search {
            if let Some(schema_table) = &table.schema_table {
                for col in &schema_table.columns {
                    if col.name.eq_ignore_ascii_case(&col_ref.column) {
                        return Some(col.affinity);
                    }
                }
            }
        }
        None
    }

    /// Check if an affinity is numeric (INTEGER, REAL, or NUMERIC)
    fn is_numeric_affinity(affinity: Option<Affinity>) -> bool {
        matches!(
            affinity,
            Some(Affinity::Integer) | Some(Affinity::Real) | Some(Affinity::Numeric)
        )
    }

    /// Get the collation of an expression (from explicit COLLATE or column definition)
    fn get_expr_collation(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Collate { collation, .. } => Some(collation.clone()),
            Expr::Column(col_ref) => self.get_column_collation(col_ref),
            Expr::Parens(inner) => self.get_expr_collation(inner),
            _ => None,
        }
    }

    /// Get the collation of a column reference from its schema definition
    fn get_column_collation(&self, col_ref: &ColumnRef) -> Option<String> {
        // Find the table for this column
        let tables_to_search: Vec<_> = if let Some(table_name) = &col_ref.table {
            self.tables
                .iter()
                .filter(|t| Self::table_name_matches(t, table_name))
                .collect()
        } else {
            // Search all tables for unqualified column
            self.tables.iter().collect()
        };

        for table in tables_to_search {
            if let Some(schema_table) = &table.schema_table {
                for col in &schema_table.columns {
                    if col.name.eq_ignore_ascii_case(&col_ref.column) {
                        // Only return non-default collation
                        if !col.collation.eq_ignore_ascii_case("BINARY") {
                            return Some(col.collation.clone());
                        }
                        return None;
                    }
                }
            }
        }
        None
    }

    /// Convert a type name to an affinity
    fn type_name_to_affinity(type_name: &str) -> Affinity {
        let upper = type_name.to_uppercase();
        if upper.contains("INT") {
            Affinity::Integer
        } else if upper.contains("CHAR")
            || upper.contains("CLOB")
            || upper.contains("TEXT")
            || upper.contains("VARCHAR")
        {
            Affinity::Text
        } else if upper.contains("BLOB") || upper.is_empty() {
            Affinity::Blob
        } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
            Affinity::Real
        } else {
            Affinity::Numeric
        }
    }

    /// Build a QueryPlanner from the current table metadata
    fn build_query_planner(&self) -> Option<QueryPlanner> {
        let mut planner = QueryPlanner::new();

        // Add tables to the planner (only local tables, skip outer correlation context)
        for (i, table) in self
            .tables
            .iter()
            .enumerate()
            .skip(self.outer_tables_boundary)
        {
            // Skip subqueries - they don't have schema indexes
            if table.is_subquery {
                continue;
            }

            // Get row estimate from schema or use default
            let estimated_rows = table
                .schema_table
                .as_ref()
                .map(|t| {
                    if t.row_estimate > 0 {
                        t.row_estimate
                    } else {
                        1000
                    }
                })
                .unwrap_or(1000);

            planner.add_table(
                table.table_name.clone(),
                Some(table.name.clone()),
                estimated_rows,
            );

            let table_idx = i - self.outer_tables_boundary;

            // Set table column names for column resolution
            if let Some(schema_table) = &table.schema_table {
                let columns: Vec<String> = schema_table
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();
                planner.set_table_columns(table_idx, columns);
                planner.set_table_rowid(table_idx, !schema_table.without_rowid);

                // Set column affinities for LIKE optimization check
                // LIKE index optimization is only valid for TEXT columns
                let affinities: Vec<String> = schema_table
                    .columns
                    .iter()
                    .map(|c| format!("{:?}", c.affinity))
                    .collect();
                planner.set_table_column_affinities(table_idx, affinities);

                // Check for INTEGER PRIMARY KEY column (rowid alias)
                // This is a single-column INTEGER PRIMARY KEY
                if !schema_table.without_rowid {
                    if let Some(ref pk_cols) = schema_table.primary_key {
                        if pk_cols.len() == 1 {
                            let pk_col_idx = pk_cols[0];
                            if pk_col_idx < schema_table.columns.len() {
                                let col = &schema_table.columns[pk_col_idx];
                                // Check if the column is INTEGER type
                                if col.affinity == Affinity::Integer {
                                    planner.set_table_ipk(table_idx, pk_col_idx as i32);
                                }
                            }
                        }
                    }
                }

                // Add indexes for this table from both the schema's global indexes map
                // and the table's indexes Vec. Prefer schema.indexes as it has resolved column indices.
                let table_name_lower = table.table_name.to_lowercase();
                let mut added_indexes: std::collections::HashSet<String> =
                    std::collections::HashSet::new();

                // First, look up indexes from the schema's global index map
                // These have resolved column_idx values from parse_create_index_sql
                if let Some(schema) = self.schema {
                    for (_name, idx) in schema.indexes.iter() {
                        if idx.table.eq_ignore_ascii_case(&table_name_lower) {
                            let index_cols: Vec<i32> =
                                idx.columns.iter().map(|ic| ic.column_idx).collect();
                            // Get collations for each index column
                            // Use index column's explicit collation if set, otherwise fall back to table column collation
                            // Empty collation means "no explicit COLLATE clause" - inherit from table
                            let collations: Vec<String> = idx
                                .columns
                                .iter()
                                .map(|ic| {
                                    if !ic.collation.is_empty() {
                                        // Index has explicit collation (including BINARY)
                                        ic.collation.to_uppercase()
                                    } else if ic.column_idx >= 0 {
                                        // No explicit collation, inherit from table column
                                        schema_table
                                            .columns
                                            .get(ic.column_idx as usize)
                                            .map(|c| {
                                                if !c.collation.is_empty() {
                                                    c.collation.to_uppercase()
                                                } else {
                                                    "BINARY".to_string()
                                                }
                                            })
                                            .unwrap_or_else(|| "BINARY".to_string())
                                    } else {
                                        "BINARY".to_string()
                                    }
                                })
                                .collect();

                            // An index is covering if it contains all table columns
                            // This is a simple heuristic - full detection requires
                            // knowing which columns the query actually needs
                            let num_table_cols = schema_table.columns.len();
                            let is_covering = index_cols.len() >= num_table_cols
                                && (0..num_table_cols as i32).all(|c| index_cols.contains(&c));

                            planner.add_index(
                                table_idx,
                                IndexInfo {
                                    name: idx.name.clone(),
                                    columns: index_cols.clone(),
                                    collations,
                                    is_primary: idx.is_primary_key,
                                    is_unique: idx.unique,
                                    is_covering,
                                    stats: idx.stats.clone(),
                                },
                            );
                            added_indexes.insert(idx.name.to_lowercase());
                        }
                    }
                }

                // Then, add any indexes from schema_table.indexes that weren't in schema.indexes
                // Only add if column_idx values are resolved (not -1)
                for index in &schema_table.indexes {
                    if added_indexes.contains(&index.name.to_lowercase()) {
                        continue;
                    }

                    let index_cols: Vec<i32> =
                        index.columns.iter().map(|ic| ic.column_idx).collect();

                    // Skip if any column_idx is unresolved (-1)
                    if index_cols.iter().any(|&c| c < 0) {
                        continue;
                    }

                    // Get collations for each index column
                    // Use index column's explicit collation if set, otherwise inherit from table
                    let collations: Vec<String> = index
                        .columns
                        .iter()
                        .map(|ic| {
                            if !ic.collation.is_empty() {
                                // Index has explicit collation (including BINARY)
                                ic.collation.to_uppercase()
                            } else if ic.column_idx >= 0 {
                                // No explicit collation, inherit from table column
                                schema_table
                                    .columns
                                    .get(ic.column_idx as usize)
                                    .map(|c| {
                                        if !c.collation.is_empty() {
                                            c.collation.to_uppercase()
                                        } else {
                                            "BINARY".to_string()
                                        }
                                    })
                                    .unwrap_or_else(|| "BINARY".to_string())
                            } else {
                                "BINARY".to_string()
                            }
                        })
                        .collect();

                    // Check if index covers all table columns
                    let num_table_cols = schema_table.columns.len();
                    let is_covering = index_cols.len() >= num_table_cols
                        && (0..num_table_cols as i32).all(|c| index_cols.contains(&c));

                    planner.add_index(
                        table_idx,
                        IndexInfo {
                            name: index.name.clone(),
                            columns: index_cols,
                            collations,
                            is_primary: index.is_primary_key,
                            is_unique: index.unique,
                            is_covering,
                            stats: index.stats.clone(),
                        },
                    );
                }
            }
        }

        Some(planner)
    }

    /// Analyze WHERE clause to get query plan
    fn analyze_query_plan(&mut self, where_clause: Option<&Expr>) -> Result<Option<WhereInfo>> {
        // Build planner from table metadata
        let mut planner = match self.build_query_planner() {
            Some(p) => p,
            None => return Ok(None),
        };

        // Set case_sensitive_like for LIKE index optimization
        planner.set_case_sensitive_like(self.case_sensitive_like);

        // Resolve aliases in WHERE clause so the planner can recognize indexed columns
        // e.g., "w AS abc ... WHERE abc=10" should resolve abc to w for index matching
        let resolved_where = where_clause.map(|expr| self.resolve_where_aliases(expr));

        // Analyze WHERE clause
        if planner.analyze_where(resolved_where.as_ref()).is_err() {
            // On error, fall back to no optimization
            return Ok(None);
        }

        // Find best plan
        match planner.find_best_plan() {
            Ok(info) => Ok(Some(info)),
            Err(_) => Ok(None),
        }
    }

    /// Check if ORDER BY would be satisfied by an index scan
    /// Returns true if the index scan produces rows in the required order
    /// This method pre-populates tables from the SelectCore to analyze the query plan
    fn check_order_by_satisfied(&mut self, core: &SelectCore, order_by: &[OrderingTerm]) -> bool {
        // Only handle simple cases: single ORDER BY column, ASC order
        if order_by.len() != 1 {
            return false;
        }

        let order_term = &order_by[0];

        // ORDER BY must be ASC for index to satisfy it (indexes are stored in ascending order)
        // DESC would require reverse scan which we don't support yet
        if order_term.order == SortOrder::Desc {
            return false;
        }

        // Get the ORDER BY column name - handle both direct column refs and positional refs
        let order_col = match &order_term.expr {
            Expr::Column(col_ref) => col_ref.column.to_lowercase(),
            Expr::Literal(Literal::Integer(n)) => {
                // ORDER BY 1 means first column in SELECT list
                let idx = (*n as usize).saturating_sub(1);
                // Get the column from SELECT list
                if idx < core.columns.len() {
                    match &core.columns[idx] {
                        ResultColumn::Expr { expr, .. } => match expr {
                            Expr::Column(col_ref) => col_ref.column.to_lowercase(),
                            _ => return false,
                        },
                        _ => return false,
                    }
                } else {
                    return false;
                }
            }
            _ => return false,
        };

        // Extract FROM clause
        let from = match &core.from {
            Some(from) => from,
            None => return false,
        };

        // Get the source items from FROM clause
        let src_list = from.to_src_list();
        if src_list.items.is_empty() {
            return false;
        }

        use crate::parser::ast::TableSource;

        // Get table qualifier from ORDER BY column reference if present
        let order_table = match &order_term.expr {
            Expr::Column(col_ref) => col_ref.table.as_ref().map(|t| t.to_lowercase()),
            _ => None,
        };

        // For multi-table queries, find the table that the ORDER BY column belongs to
        // Then check if that table has an index on the ORDER BY column
        for item in &src_list.items {
            let table_name = match &item.source {
                TableSource::Table(name) => &name.name,
                _ => continue, // Skip subqueries and table functions
            };
            let table_name_lower = table_name.to_lowercase();
            let alias = item.alias.as_ref().map(|a| a.to_lowercase());

            // If ORDER BY specifies a table, check if this is the right one
            if let Some(ref order_tbl) = order_table {
                if table_name_lower != *order_tbl && alias.as_ref().map_or(true, |a| a != order_tbl)
                {
                    continue; // Not the table we're looking for
                }
            }

            // Look up schema table
            let schema_table = match self.lookup_table_schema(&table_name_lower) {
                Some(t) => t,
                None => continue,
            };

            // Check if this table has a column matching ORDER BY
            let has_col = schema_table
                .columns
                .iter()
                .any(|c| c.name.to_lowercase() == order_col);
            if !has_col {
                continue;
            }

            // Temporarily add table info for query plan analysis
            let display_name = item.alias.clone().unwrap_or_else(|| table_name.clone());
            self.tables.push(TableInfo {
                name: display_name,
                table_name: table_name.clone(),
                cursor: 0, // Placeholder - not used for analysis
                schema_table: Some(schema_table.clone()),
                is_subquery: false,
                join_type: item.join_type,
                subquery_columns: None,
            });

            // Analyze the query plan for this table
            let result = self.check_order_by_satisfied_inner(
                &schema_table,
                &table_name_lower,
                core.where_clause.as_deref(),
                &order_col,
            );

            // Clear temporary tables
            self.tables.clear();

            if result {
                return true;
            }
        }

        false
    }

    /// Inner helper for check_order_by_satisfied after tables are set up
    fn check_order_by_satisfied_inner(
        &mut self,
        schema_table: &std::sync::Arc<Table>,
        table_name: &str,
        where_clause: Option<&Expr>,
        order_col: &str,
    ) -> bool {
        // First, check if WHERE clause analysis already uses an index that satisfies ORDER BY
        if let Some(where_expr) = where_clause {
            let plan_result = self.analyze_query_plan(Some(where_expr));
            if let Ok(Some(info)) = plan_result {
                if !info.levels.is_empty() {
                    let level = &info.levels[0];
                    if let WherePlan::IndexScan {
                        index_name,
                        has_range,
                        ..
                    } = &level.plan
                    {
                        if *has_range {
                            // Check if index first column matches ORDER BY
                            if self.index_first_column_matches(schema_table, index_name, order_col)
                            {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        // Check if ORDER BY is on the INTEGER PRIMARY KEY (rowid)
        // Table scans are naturally in rowid order, so no sorting needed
        if let Some(ipk_idx) = schema_table.rowid_alias_column() {
            let ipk_col_name = &schema_table.columns[ipk_idx].name;
            if ipk_col_name.eq_ignore_ascii_case(order_col) {
                // ORDER BY is on the rowid column - natural table scan order works
                return true;
            }
        }

        // If no WHERE clause or WHERE doesn't use a suitable index,
        // search ALL indexes on the table for one that matches ORDER BY
        // This allows index scan to be used purely for ordering
        for index in &schema_table.indexes {
            if let Some(first_col) = index.columns.first() {
                // Check if first index column matches ORDER BY column
                let col_idx = first_col.column_idx;
                if col_idx >= 0 && (col_idx as usize) < schema_table.columns.len() {
                    let idx_col_name = &schema_table.columns[col_idx as usize].name;
                    if idx_col_name.eq_ignore_ascii_case(order_col) {
                        // Found an index that can satisfy ORDER BY
                        // Store table and index for use during code generation
                        self.order_by_index = Some((table_name.to_string(), index.name.clone()));
                        return true;
                    }
                }
            }
        }

        // Also check schema's global indexes
        if let Some(schema) = self.schema {
            let table_name_lower = schema_table.name.to_lowercase();
            for (_name, idx) in schema.indexes.iter() {
                if idx.table.to_lowercase() == table_name_lower {
                    if let Some(first_col) = idx.columns.first() {
                        let col_idx = first_col.column_idx;
                        if col_idx >= 0 && (col_idx as usize) < schema_table.columns.len() {
                            let idx_col_name = &schema_table.columns[col_idx as usize].name;
                            if idx_col_name.eq_ignore_ascii_case(order_col) {
                                self.order_by_index =
                                    Some((table_name.to_string(), idx.name.clone()));
                                return true;
                            }
                        }
                    }
                }
            }
        }

        false
    }

    /// Helper to check if an index's first column matches the ORDER BY column
    fn index_first_column_matches(
        &self,
        schema_table: &std::sync::Arc<Table>,
        index_name: &str,
        order_col: &str,
    ) -> bool {
        // Check table's indexes
        for index in &schema_table.indexes {
            if index.name.eq_ignore_ascii_case(index_name) {
                if let Some(first_col) = index.columns.first() {
                    let col_idx = first_col.column_idx;
                    if col_idx >= 0 && (col_idx as usize) < schema_table.columns.len() {
                        let idx_col_name = &schema_table.columns[col_idx as usize].name;
                        if idx_col_name.eq_ignore_ascii_case(order_col) {
                            return true;
                        }
                    }
                }
            }
        }

        // Check schema's global indexes
        if let Some(schema) = self.schema {
            for (_name, idx) in schema.indexes.iter() {
                if idx.name.eq_ignore_ascii_case(index_name) {
                    if let Some(first_col) = idx.columns.first() {
                        let col_idx = first_col.column_idx;
                        if col_idx >= 0 && (col_idx as usize) < schema_table.columns.len() {
                            let idx_col_name = &schema_table.columns[col_idx as usize].name;
                            if idx_col_name.eq_ignore_ascii_case(order_col) {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        false
    }

    /// Resolve aliases in ORDER BY expressions by substituting alias references
    /// with reads from the result column registers.
    /// For expressions like ORDER BY 10-(x+y), this substitutes x and y with their
    /// corresponding result column values.
    fn resolve_order_by_aliases(&self, expr: &Expr, base_reg: i32, count: usize) -> Expr {
        use crate::parser::ast::WhenClause;
        match expr {
            Expr::Column(col_ref) if col_ref.table.is_none() => {
                // Check if this matches a result column name (alias)
                if let Some(col_idx) = self
                    .result_column_names
                    .iter()
                    .position(|name| name.eq_ignore_ascii_case(&col_ref.column))
                {
                    // Return a column index literal that will be handled by the column index code path
                    // We return the original since it was already handled above
                    return expr.clone();
                }
                // Not an alias - return as-is
                expr.clone()
            }
            Expr::Binary { op, left, right } => Expr::Binary {
                op: *op,
                left: Box::new(self.resolve_order_by_aliases(left, base_reg, count)),
                right: Box::new(self.resolve_order_by_aliases(right, base_reg, count)),
            },
            Expr::Unary { op, expr: inner } => Expr::Unary {
                op: *op,
                expr: Box::new(self.resolve_order_by_aliases(inner, base_reg, count)),
            },
            Expr::Parens(inner) => Expr::Parens(Box::new(
                self.resolve_order_by_aliases(inner, base_reg, count),
            )),
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => Expr::Case {
                operand: operand
                    .as_ref()
                    .map(|e| Box::new(self.resolve_order_by_aliases(e, base_reg, count))),
                when_clauses: when_clauses
                    .iter()
                    .map(|wc| WhenClause {
                        when: Box::new(self.resolve_order_by_aliases(&wc.when, base_reg, count)),
                        then: Box::new(self.resolve_order_by_aliases(&wc.then, base_reg, count)),
                    })
                    .collect(),
                else_clause: else_clause
                    .as_ref()
                    .map(|e| Box::new(self.resolve_order_by_aliases(e, base_reg, count))),
            },
            Expr::Function(func) => {
                let args = match &func.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => {
                        crate::parser::ast::FunctionArgs::Exprs(
                            exprs
                                .iter()
                                .map(|e| self.resolve_order_by_aliases(e, base_reg, count))
                                .collect(),
                        )
                    }
                    other => other.clone(),
                };
                Expr::Function(crate::parser::ast::FunctionCall {
                    name: func.name.clone(),
                    args,
                    distinct: func.distinct,
                    filter: func.filter.clone(),
                    over: func.over.clone(),
                })
            }
            _ => expr.clone(),
        }
    }

    /// Resolve result column aliases in a WHERE expression for query planning.
    /// This replaces alias references with their underlying column expressions
    /// so the query planner can match them to indexed columns.
    fn resolve_where_aliases(&self, expr: &Expr) -> Expr {
        use crate::parser::ast::{InList, WhenClause};
        match expr {
            Expr::Column(col_ref) if col_ref.table.is_none() => {
                // Check if this is an alias
                let col_lower = col_ref.column.to_lowercase();
                if let Some(resolved) = self.alias_expressions.get(&col_lower) {
                    // Return the resolved expression
                    resolved.clone()
                } else {
                    expr.clone()
                }
            }
            Expr::Binary { op, left, right } => Expr::Binary {
                op: *op,
                left: Box::new(self.resolve_where_aliases(left)),
                right: Box::new(self.resolve_where_aliases(right)),
            },
            Expr::Unary { op, expr: inner } => Expr::Unary {
                op: *op,
                expr: Box::new(self.resolve_where_aliases(inner)),
            },
            Expr::Parens(inner) => Expr::Parens(Box::new(self.resolve_where_aliases(inner))),
            Expr::In {
                expr: inner,
                list,
                negated,
            } => {
                let resolved_list = match list {
                    InList::Values(exprs) => InList::Values(
                        exprs
                            .iter()
                            .map(|e| self.resolve_where_aliases(e))
                            .collect(),
                    ),
                    other => other.clone(),
                };
                Expr::In {
                    expr: Box::new(self.resolve_where_aliases(inner)),
                    list: resolved_list,
                    negated: *negated,
                }
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                negated,
            } => Expr::Between {
                expr: Box::new(self.resolve_where_aliases(inner)),
                low: Box::new(self.resolve_where_aliases(low)),
                high: Box::new(self.resolve_where_aliases(high)),
                negated: *negated,
            },
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => Expr::Case {
                operand: operand
                    .as_ref()
                    .map(|e| Box::new(self.resolve_where_aliases(e))),
                when_clauses: when_clauses
                    .iter()
                    .map(|wc| WhenClause {
                        when: Box::new(self.resolve_where_aliases(&wc.when)),
                        then: Box::new(self.resolve_where_aliases(&wc.then)),
                    })
                    .collect(),
                else_clause: else_clause
                    .as_ref()
                    .map(|e| Box::new(self.resolve_where_aliases(e))),
            },
            Expr::Function(func) => {
                let args = match &func.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => {
                        crate::parser::ast::FunctionArgs::Exprs(
                            exprs
                                .iter()
                                .map(|e| self.resolve_where_aliases(e))
                                .collect(),
                        )
                    }
                    other => other.clone(),
                };
                Expr::Function(crate::parser::ast::FunctionCall {
                    name: func.name.clone(),
                    args,
                    distinct: func.distinct,
                    filter: func.filter.clone(),
                    over: func.over.clone(),
                })
            }
            // For other expression types, return as-is
            _ => expr.clone(),
        }
    }

    /// Get the plan for a specific table from WhereInfo
    fn get_table_plan<'a>(
        &self,
        where_info: &'a WhereInfo,
        table_cursor: i32,
    ) -> Option<&'a WhereLevel> {
        // Find the table index for this cursor
        for (i, table) in self
            .tables
            .iter()
            .enumerate()
            .skip(self.outer_tables_boundary)
        {
            if table.cursor == table_cursor {
                let local_idx = i - self.outer_tables_boundary;
                return where_info.levels.get(local_idx);
            }
        }
        None
    }

    /// Emit code for an index scan loop start
    /// Returns (index_cursor, loop_label, key_base_reg, key_count)
    fn emit_index_scan_start(
        &mut self,
        table_cursor: i32,
        level: &WhereLevel,
        index_name: &str,
        eq_cols: i32,
        skip_label: i32,
    ) -> Result<(i32, i32, i32, i32)> {
        // Allocate index cursor
        let index_cursor = self.alloc_cursor();
        self.index_cursors.insert(table_cursor, index_cursor);

        // Open the index
        self.emit(
            Opcode::OpenRead,
            index_cursor,
            0,
            0,
            P4::Text(index_name.to_string()),
        );

        let loop_label = self.alloc_label();
        let key_base_reg;
        let key_count;

        if eq_cols > 0 {
            // Build the index key from equality terms
            // We need to compile the RHS of each equality term in the level's used_terms
            key_base_reg = self.next_reg;
            key_count = eq_cols;

            // For now, emit a placeholder key - the actual key building will be done
            // when we have the WHERE terms available during the main compile loop
            // We'll revisit this when integrating with the main loop

            // Seek to first matching key
            self.emit(
                Opcode::SeekGE,
                index_cursor,
                skip_label,
                key_base_reg,
                P4::Int64(eq_cols as i64),
            );
        } else {
            // No equality constraints - rewind to start
            key_base_reg = 0;
            key_count = 0;
            self.emit(Opcode::Rewind, index_cursor, skip_label, 0, P4::Unused);
        }

        // Mark loop start
        self.resolve_label(loop_label, self.current_addr());

        // Set up deferred seek from index to table
        // Build alt-map for covering index optimization
        let alt_map_p4 = if let Some(alt_map) = self.build_index_alt_map(table_cursor, index_name) {
            P4::IntArray(alt_map)
        } else {
            P4::Unused
        };
        self.emit(
            Opcode::DeferredSeek,
            table_cursor,
            0,
            index_cursor,
            alt_map_p4,
        );

        Ok((index_cursor, loop_label, key_base_reg, key_count))
    }

    /// Emit code for rowid equality lookup (single row)
    fn emit_rowid_eq_lookup(
        &mut self,
        table_cursor: i32,
        rowid_reg: i32,
        skip_label: i32,
    ) -> Result<()> {
        // SeekRowid positions cursor at exact rowid or jumps to skip_label if not found
        self.emit(
            Opcode::SeekRowid,
            table_cursor,
            skip_label,
            rowid_reg,
            P4::Unused,
        );
        Ok(())
    }

    /// Emit code for rowid range scan start
    fn emit_rowid_range_start(
        &mut self,
        table_cursor: i32,
        has_start: bool,
        start_reg: Option<i32>,
        skip_label: i32,
    ) -> Result<i32> {
        let loop_label = self.alloc_label();

        if has_start {
            if let Some(reg) = start_reg {
                // SeekGE positions at first row >= start value
                self.emit(Opcode::SeekGE, table_cursor, skip_label, reg, P4::Unused);
            } else {
                // No start register provided, rewind to beginning
                self.emit(Opcode::Rewind, table_cursor, skip_label, 0, P4::Unused);
            }
        } else {
            // No start constraint - rewind to beginning
            self.emit(Opcode::Rewind, table_cursor, skip_label, 0, P4::Unused);
        }

        self.resolve_label(loop_label, self.current_addr());
        Ok(loop_label)
    }

    /// Emit the loop end code for an index scan (IdxGT check + Next)
    fn emit_index_scan_end(
        &mut self,
        index_cursor: i32,
        loop_label: i32,
        key_base_reg: i32,
        key_count: i32,
        done_label: i32,
    ) {
        if key_count > 0 {
            // IdxGT: jump to done_label if current index entry > key
            // This ensures we stay within the equality prefix range
            self.emit(
                Opcode::IdxGT,
                index_cursor,
                done_label,
                key_base_reg,
                P4::Int64(key_count as i64),
            );
        }

        // Advance to next index entry
        self.emit(Opcode::Next, index_cursor, loop_label, 0, P4::Unused);
    }

    /// Build alt-map for covering index optimization
    /// The alt-map redirects Column reads from table cursor to index cursor
    /// when the needed column is present in the index.
    ///
    /// alt_map[table_col_idx] = index_col_position, or -1 if not in index
    fn build_index_alt_map(&self, table_cursor: i32, index_name: &str) -> Option<Vec<i64>> {
        // Find the table info for this cursor
        let table_info = self.tables.iter().find(|t| t.cursor == table_cursor)?;

        // Get the schema table for column count
        let schema_table = table_info.schema_table.as_ref()?;
        let num_columns = schema_table.columns.len();

        // Look up the index columns from schema
        let index_columns: Vec<i32> = if let Some(schema) = self.schema {
            // First try the global schema.indexes
            if let Some(idx) = schema.indexes.get(&index_name.to_lowercase()) {
                idx.columns.iter().map(|ic| ic.column_idx).collect()
            } else {
                // Fall back to schema_table.indexes
                schema_table
                    .indexes
                    .iter()
                    .find(|idx| idx.name.eq_ignore_ascii_case(index_name))
                    .map(|idx| idx.columns.iter().map(|ic| ic.column_idx).collect())?
            }
        } else {
            // No schema available, try schema_table.indexes
            schema_table
                .indexes
                .iter()
                .find(|idx| idx.name.eq_ignore_ascii_case(index_name))
                .map(|idx| idx.columns.iter().map(|ic| ic.column_idx).collect())?
        };

        // Build the alt_map: for each table column, find its position in the index
        let mut alt_map = vec![-1i64; num_columns];
        for (index_pos, &table_col) in index_columns.iter().enumerate() {
            if table_col >= 0 && (table_col as usize) < num_columns {
                alt_map[table_col as usize] = index_pos as i64;
            }
        }

        Some(alt_map)
    }

    /// Find equality terms for index columns in the WHERE info
    /// Returns Vec of (column_index, term_expr) for building the index key
    fn find_index_equality_terms<'a>(
        &'a self,
        where_info: &'a WhereInfo,
        level: &'a WhereLevel,
        _index_name: &str,
    ) -> Vec<(i32, &'a Expr)> {
        let mut result = Vec::new();
        let table_idx = level.from_idx;

        for &term_idx in &level.used_terms {
            if let Some(term) = where_info.terms.get(term_idx as usize) {
                if term.is_equality() {
                    if let Expr::Binary { left, right, .. } = term.expr.as_ref() {
                        // Check if left_col matches this table's index column
                        if let Some((ti, col_idx)) = term.left_col {
                            if ti == table_idx {
                                // left side is index column, right side is the value
                                result.push((col_idx, right.as_ref()));
                                continue;
                            }
                        }
                        // Check if right_col matches this table's index column (for join conditions like s=y)
                        if let Some((ti, col_idx)) = term.right_col {
                            if ti == table_idx {
                                // right side is index column, left side is the value
                                result.push((col_idx, left.as_ref()));
                            }
                        }
                    }
                }
            }
        }

        result
    }

    /// Find range bounds (start and end) from WHERE clause for index range scan
    /// Returns ((start_expr, is_strict), (end_expr, is_strict)) for BETWEEN-like constraints
    /// is_strict is true for > and <, false for >= and <=
    fn find_range_bounds(
        &self,
        where_info: &WhereInfo,
        level: &WhereLevel,
    ) -> (Option<(Expr, bool)>, Option<(Expr, bool)>) {
        let mut start_bound = None;
        let mut end_bound = None;

        for &term_idx in &level.used_terms {
            if let Some(term) = where_info.terms.get(term_idx as usize) {
                if term.is_range() {
                    if let Expr::Binary { op, right, .. } = term.expr.as_ref() {
                        match op {
                            BinaryOp::Gt => {
                                // Strict start bound: col > val
                                start_bound = Some((right.as_ref().clone(), true));
                            }
                            BinaryOp::Ge => {
                                // Inclusive start bound: col >= val
                                start_bound = Some((right.as_ref().clone(), false));
                            }
                            BinaryOp::Lt => {
                                // Strict end bound: col < val
                                end_bound = Some((right.as_ref().clone(), true));
                            }
                            BinaryOp::Le => {
                                // Inclusive end bound: col <= val
                                end_bound = Some((right.as_ref().clone(), false));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        (start_bound, end_bound)
    }

    /// Check if a WHERE term should be filtered at runtime
    /// (i.e., not already consumed by an index seek)
    fn is_runtime_filter_term(&self, where_info: &WhereInfo, term_idx: i32) -> bool {
        // Check if this term is used by any level's index seek
        for level in &where_info.levels {
            if level.used_terms.contains(&term_idx) {
                match &level.plan {
                    WherePlan::IndexScan { eq_cols, .. } if *eq_cols > 0 => {
                        // Term is consumed by index seek - don't filter at runtime
                        return false;
                    }
                    WherePlan::RowidEq
                    | WherePlan::PrimaryKey { .. }
                    | WherePlan::RowidIn { .. } => {
                        // Term is consumed by rowid/pk lookup or IN - don't filter at runtime
                        return false;
                    }
                    _ => {}
                }
            }
        }
        true
    }

    /// Process join constraints (NATURAL, USING, ON) and generate WHERE conditions
    ///
    /// This matches SQLite's sqlite3ProcessJoin() function from select.c:
    /// - NATURAL joins: find common columns between tables and generate equalities
    /// - USING: generate equalities for specified columns
    /// - ON: use the expression directly
    fn process_joins(&mut self, src_list: &crate::parser::ast::SrcList) -> Result<()> {
        use crate::parser::ast::{BinaryOp, ColumnRef, Expr};
        use std::collections::HashSet;

        // Check if there's a RIGHT JOIN anywhere in the src_list.
        // If so, ON clauses must not reference tables to their right.
        // This matches SQLite's hasRightJoin() check in selectCheckOnClauses().
        let has_right_join = src_list
            .items
            .iter()
            .any(|item| item.join_type.contains(JoinFlags::RIGHT));

        for (i, item) in src_list.items.iter().enumerate() {
            if i == 0 {
                // First table has no join with previous
                continue;
            }

            let current_table = &self.tables[i];

            // Handle NATURAL join - find common columns
            if item.join_type.is_natural() {
                let common_cols = self.find_common_columns(i);

                // Track common columns to exclude from * expansion for current (right) table
                let excluded: HashSet<String> =
                    common_cols.iter().map(|s| s.to_lowercase()).collect();
                if !excluded.is_empty() {
                    self.coalesced_columns.insert(i, excluded);
                }

                for col_name in common_cols {
                    // Generate: prev_table.col = current_table.col
                    let left_expr = Expr::Column(ColumnRef {
                        database: None,
                        table: Some(self.tables[i - 1].name.clone()),
                        column: col_name.clone(),
                        column_index: None,
                        source_text: None,
                    });
                    let right_expr = Expr::Column(ColumnRef {
                        database: None,
                        table: Some(current_table.name.clone()),
                        column: col_name,
                        column_index: None,
                        source_text: None,
                    });
                    let eq_expr = Expr::Binary {
                        op: BinaryOp::Eq,
                        left: Box::new(left_expr),
                        right: Box::new(right_expr),
                    };
                    self.join_conditions.push(eq_expr);
                }
            }
            // Handle USING clause
            else if let Some(using_cols) = &item.using_columns {
                // Track USING columns to exclude from * expansion for current (right) table
                let excluded: HashSet<String> =
                    using_cols.iter().map(|s| s.to_lowercase()).collect();
                if !excluded.is_empty() {
                    self.coalesced_columns.insert(i, excluded);
                }

                // For self-joins (same table name without distinct aliases),
                // use synthetic table identifiers that encode the table index
                // Format: __tbl_idx_N__ where N is the index in self.tables
                let prev_table = &self.tables[i - 1];
                let left_table_id = if prev_table.name == current_table.name {
                    // Self-join: use synthetic identifier to disambiguate
                    format!("__tbl_idx_{}__", i - 1)
                } else {
                    prev_table.name.clone()
                };
                let right_table_id = if prev_table.name == current_table.name {
                    format!("__tbl_idx_{}__", i)
                } else {
                    current_table.name.clone()
                };

                for col_name in using_cols {
                    // Generate: prev_table.col = current_table.col
                    let left_expr = Expr::Column(ColumnRef {
                        database: None,
                        table: Some(left_table_id.clone()),
                        column: col_name.clone(),
                        column_index: None,
                        source_text: None,
                    });
                    let right_expr = Expr::Column(ColumnRef {
                        database: None,
                        table: Some(right_table_id.clone()),
                        column: col_name.clone(),
                        column_index: None,
                        source_text: None,
                    });
                    let eq_expr = Expr::Binary {
                        op: BinaryOp::Eq,
                        left: Box::new(left_expr),
                        right: Box::new(right_expr),
                    };
                    self.join_conditions.push(eq_expr);
                }
            }
            // Handle ON clause
            else if let Some(on_expr) = &item.on_clause {
                // Check if this ON clause needs validation for forward references.
                // SQLite validates ON clauses that are either:
                // - On an OUTER join (LEFT, RIGHT, or FULL), or
                // - On an INNER join when there's a RIGHT join somewhere in the FROM
                let is_outer = item.join_type.is_outer();
                let needs_validation = is_outer || has_right_join;

                if needs_validation {
                    // Validate that ON clause doesn't reference tables to its right
                    self.validate_on_clause_references(on_expr, i, src_list)?;
                }

                // on_expr is &Box<Expr>, so we need to deref twice to get &Expr
                self.join_conditions.push((**on_expr).clone());
            }
        }

        Ok(())
    }

    /// Validate that an ON clause doesn't reference tables to its right.
    ///
    /// This matches SQLite's selectCheckOnClauses() function from select.c.
    /// When there's a RIGHT or FULL JOIN in the FROM clause, ON clauses must
    /// not reference tables that appear later in the join sequence.
    fn validate_on_clause_references(
        &self,
        on_expr: &Expr,
        join_index: usize,
        src_list: &crate::parser::ast::SrcList,
    ) -> Result<()> {
        // Collect all column references in the ON expression (including subqueries)
        let columns = self.collect_on_clause_columns(on_expr);

        // Build a set of table names that appear AFTER this join position
        let tables_to_right: HashSet<String> = src_list
            .items
            .iter()
            .skip(join_index + 1)
            .map(|item| {
                let name = item.alias.clone().unwrap_or_else(|| match &item.source {
                    crate::parser::ast::TableSource::Table(qn) => qn.name.clone(),
                    crate::parser::ast::TableSource::Subquery(_) => "subquery".to_string(),
                    crate::parser::ast::TableSource::TableFunction { name, .. } => name.clone(),
                });
                name.to_lowercase()
            })
            .collect();

        // Also include the actual table names (not just aliases) for tables to the right
        let table_names_to_right: HashSet<String> = src_list
            .items
            .iter()
            .skip(join_index + 1)
            .filter_map(|item| match &item.source {
                crate::parser::ast::TableSource::Table(qn) => Some(qn.name.to_lowercase()),
                _ => None,
            })
            .collect();

        // Check each column reference
        for col_ref in &columns {
            if let Some(table_name) = &col_ref.table {
                let table_lower = table_name.to_lowercase();
                if tables_to_right.contains(&table_lower)
                    || table_names_to_right.contains(&table_lower)
                {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "ON clause references tables to its right",
                    ));
                }
            } else {
                // Column without explicit table qualifier - check if it could only
                // resolve to a table on the right
                let col_name = col_ref.column.to_lowercase();

                // Check if column exists in any table to the left (index <= join_index)
                let mut found_left = false;
                for idx in 0..=join_index {
                    if idx < self.tables.len() {
                        let table = &self.tables[idx];
                        if self.table_has_column(table, &col_name) {
                            found_left = true;
                            break;
                        }
                    }
                }

                // Check if column exists ONLY in tables to the right
                if !found_left {
                    for idx in (join_index + 1)..self.tables.len() {
                        let table = &self.tables[idx];
                        if self.table_has_column(table, &col_name) {
                            return Err(Error::with_message(
                                ErrorCode::Error,
                                "ON clause references tables to its right",
                            ));
                        }
                    }

                    // Check if this is a result column alias that references tables to the right
                    // This handles cases like: SELECT c+d AS cd FROM t1 LEFT JOIN t2 ON (cd=5) CROSS JOIN t3
                    // where cd is an alias for c+d, and c,d come from t3
                    if let Some(alias_expr) = self.alias_expressions.get(&col_name) {
                        // Collect all columns from the alias expression
                        let alias_columns = self.collect_on_clause_columns(alias_expr);
                        for alias_col in alias_columns {
                            // Check if any column in the alias expression comes from a table to the right
                            if let Some(alias_table) = &alias_col.table {
                                let alias_table_lower = alias_table.to_lowercase();
                                if tables_to_right.contains(&alias_table_lower)
                                    || table_names_to_right.contains(&alias_table_lower)
                                {
                                    return Err(Error::with_message(
                                        ErrorCode::Error,
                                        "ON clause references tables to its right",
                                    ));
                                }
                            } else {
                                // Unqualified column in alias - check if it resolves to right tables
                                let alias_col_name = alias_col.column.to_lowercase();
                                let mut alias_found_left = false;
                                for idx in 0..=join_index {
                                    if idx < self.tables.len() {
                                        let table = &self.tables[idx];
                                        if self.table_has_column(table, &alias_col_name) {
                                            alias_found_left = true;
                                            break;
                                        }
                                    }
                                }
                                if !alias_found_left {
                                    for idx in (join_index + 1)..self.tables.len() {
                                        let table = &self.tables[idx];
                                        if self.table_has_column(table, &alias_col_name) {
                                            return Err(Error::with_message(
                                                ErrorCode::Error,
                                                "ON clause references tables to its right",
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Collect all column references from an expression, including nested subqueries.
    /// This recursively walks the expression tree.
    fn collect_on_clause_columns(&self, expr: &Expr) -> Vec<ColumnRef> {
        let mut columns = Vec::new();
        self.collect_columns_recursive(expr, &mut columns);
        columns
    }

    fn collect_columns_recursive(&self, expr: &Expr, columns: &mut Vec<ColumnRef>) {
        use crate::parser::ast::{FunctionArgs, InList};

        match expr {
            Expr::Column(col_ref) => {
                columns.push(col_ref.clone());
            }
            Expr::Binary { left, right, .. } | Expr::IsDistinct { left, right, .. } => {
                self.collect_columns_recursive(left, columns);
                self.collect_columns_recursive(right, columns);
            }
            Expr::Unary { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::Collate { expr, .. }
            | Expr::IsNull { expr, .. }
            | Expr::Parens(expr) => {
                self.collect_columns_recursive(expr, columns);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.collect_columns_recursive(expr, columns);
                self.collect_columns_recursive(low, columns);
                self.collect_columns_recursive(high, columns);
            }
            Expr::In { expr, list, .. } => {
                self.collect_columns_recursive(expr, columns);
                match list {
                    InList::Values(values) => {
                        for v in values {
                            self.collect_columns_recursive(v, columns);
                        }
                    }
                    InList::Subquery(select) => {
                        // For subqueries in IN, we need to check columns in the subquery's WHERE
                        // that reference outer tables
                        self.collect_columns_from_select(select, columns);
                    }
                    InList::Table(_) => {}
                }
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.collect_columns_recursive(expr, columns);
                self.collect_columns_recursive(pattern, columns);
                if let Some(esc) = escape {
                    self.collect_columns_recursive(esc, columns);
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    self.collect_columns_recursive(op, columns);
                }
                for clause in when_clauses {
                    self.collect_columns_recursive(&clause.when, columns);
                    self.collect_columns_recursive(&clause.then, columns);
                }
                if let Some(el) = else_clause {
                    self.collect_columns_recursive(el, columns);
                }
            }
            Expr::Function(func) => {
                if let FunctionArgs::Exprs(args) = &func.args {
                    for arg in args {
                        self.collect_columns_recursive(arg, columns);
                    }
                }
                if let Some(filter) = &func.filter {
                    self.collect_columns_recursive(filter, columns);
                }
            }
            Expr::Subquery(select)
            | Expr::Exists {
                subquery: select, ..
            } => {
                // For subqueries, collect columns that could be correlated references
                self.collect_columns_from_select(select, columns);
            }
            Expr::Literal(_) | Expr::Variable(_) | Expr::Raise { .. } => {}
        }
    }

    /// Collect columns from a SELECT that might be correlated references to outer query
    fn collect_columns_from_select(&self, select: &SelectStmt, columns: &mut Vec<ColumnRef>) {
        // Walk the SELECT body looking for column references
        self.collect_columns_from_select_body(&select.body, columns);

        // Also check ORDER BY and LIMIT
        if let Some(order_by) = &select.order_by {
            for term in order_by {
                self.collect_columns_recursive(&term.expr, columns);
            }
        }
        if let Some(limit) = &select.limit {
            self.collect_columns_recursive(&limit.limit, columns);
            if let Some(offset) = &limit.offset {
                self.collect_columns_recursive(offset, columns);
            }
        }
    }

    fn collect_columns_from_select_body(&self, body: &SelectBody, columns: &mut Vec<ColumnRef>) {
        match body {
            SelectBody::Select(core) => {
                // Check result columns
                for col in &core.columns {
                    if let ResultColumn::Expr { expr, .. } = col {
                        self.collect_columns_recursive(expr, columns);
                    }
                }
                // Check WHERE clause
                if let Some(where_clause) = &core.where_clause {
                    self.collect_columns_recursive(where_clause, columns);
                }
                // Check GROUP BY
                if let Some(group_by) = &core.group_by {
                    for expr in group_by {
                        self.collect_columns_recursive(expr, columns);
                    }
                }
                // Check HAVING
                if let Some(having) = &core.having {
                    self.collect_columns_recursive(having, columns);
                }
                // Check FROM clause for nested subqueries and ON clauses
                if let Some(from) = &core.from {
                    self.collect_columns_from_from_clause(from, columns);
                }
            }
            SelectBody::Compound { left, right, .. } => {
                self.collect_columns_from_select_body(left, columns);
                self.collect_columns_from_select_body(right, columns);
            }
        }
    }

    fn collect_columns_from_from_clause(&self, from: &FromClause, columns: &mut Vec<ColumnRef>) {
        for table_ref in &from.tables {
            self.collect_columns_from_table_ref(table_ref, columns);
        }
    }

    fn collect_columns_from_table_ref(&self, table_ref: &TableRef, columns: &mut Vec<ColumnRef>) {
        match table_ref {
            TableRef::Table { .. } => {}
            TableRef::Subquery { query, .. } => {
                self.collect_columns_from_select(query, columns);
            }
            TableRef::Join {
                left,
                right,
                constraint,
                ..
            } => {
                self.collect_columns_from_table_ref(left, columns);
                self.collect_columns_from_table_ref(right, columns);
                if let Some(crate::parser::ast::JoinConstraint::On(expr)) = constraint {
                    self.collect_columns_recursive(expr, columns);
                }
            }
            TableRef::TableFunction { args, .. } => {
                for arg in args {
                    self.collect_columns_recursive(arg, columns);
                }
            }
            TableRef::Parens(inner) => {
                self.collect_columns_from_table_ref(inner, columns);
            }
        }
    }

    /// Check if a table has a column with the given name
    fn table_has_column(&self, table: &TableInfo, col_name: &str) -> bool {
        if let Some(schema) = &table.schema_table {
            schema
                .columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(col_name))
        } else if let Some(subq_cols) = &table.subquery_columns {
            subq_cols.iter().any(|c| c.eq_ignore_ascii_case(col_name))
        } else {
            false
        }
    }

    /// Find column names that exist in both the current table and any previous table
    fn find_common_columns(&self, current_idx: usize) -> Vec<String> {
        let mut common = Vec::new();
        let current_table = &self.tables[current_idx];

        // Get columns from current table
        let current_cols: Vec<String> = if let Some(schema) = &current_table.schema_table {
            schema
                .columns
                .iter()
                .map(|c| c.name.to_lowercase())
                .collect()
        } else if let Some(subq_cols) = &current_table.subquery_columns {
            subq_cols.iter().map(|c| c.to_lowercase()).collect()
        } else {
            return common;
        };

        // Check against all previous tables
        for prev_idx in 0..current_idx {
            let prev_table = &self.tables[prev_idx];
            let prev_cols: Vec<String> = if let Some(schema) = &prev_table.schema_table {
                schema
                    .columns
                    .iter()
                    .map(|c| c.name.to_lowercase())
                    .collect()
            } else if let Some(subq_cols) = &prev_table.subquery_columns {
                subq_cols.iter().map(|c| c.to_lowercase()).collect()
            } else {
                continue;
            };

            // Find intersection
            for col in &current_cols {
                if prev_cols.contains(col) && !common.contains(col) {
                    common.push(col.clone());
                }
            }
        }

        common
    }

    /// Merge collected join conditions with the original WHERE clause
    ///
    /// Returns a new WHERE expression that combines:
    /// - The original WHERE clause (if any)
    /// - All join conditions from NATURAL/USING/ON clauses
    ///
    /// Conditions are combined with AND.
    fn merge_join_conditions(&mut self, original_where: Option<Expr>) -> Option<Expr> {
        if self.join_conditions.is_empty() {
            return original_where;
        }

        // Take ownership of join conditions
        let conditions = std::mem::take(&mut self.join_conditions);

        // Build combined expression: original_where AND cond1 AND cond2 AND ...
        let mut result = original_where;

        for cond in conditions {
            result = Some(match result {
                Some(existing) => Expr::Binary {
                    op: BinaryOp::And,
                    left: Box::new(existing),
                    right: Box::new(cond),
                },
                None => cond,
            });
        }

        result
    }

    /// Compile a table reference
    fn compile_table_ref(&mut self, table_ref: &TableRef, join_type: JoinType) -> Result<()> {
        match table_ref {
            TableRef::Table { name, alias, .. } => {
                // Use just the table name for internal lookups
                let table_name = &name.name;
                let table_name_lower = table_name.to_lowercase();
                // Use full qualified name (schema.table) for VDBE if schema is specified
                let qualified_name = name.to_string();

                // Check cte_cursors first (recursive CTE self-reference uses existing cursor)
                if let Some((cursor, columns)) = self.cte_cursors.get(&table_name_lower) {
                    let display_name = alias.clone().unwrap_or_else(|| table_name.clone());
                    self.tables.push(TableInfo {
                        name: display_name,
                        table_name: table_name.clone(),
                        cursor: *cursor,
                        schema_table: None,
                        is_subquery: true,
                        join_type,
                        subquery_columns: Some(columns.clone()),
                    });
                    return Ok(());
                }

                // Check if this is a CTE reference (before views, so CTEs take precedence)
                if let Some(cte) = self.ctes.get(&table_name_lower).cloned() {
                    let cursor = self.alloc_cursor();
                    let columns = if self.recursive_ctes.contains(&table_name_lower) {
                        self.compile_recursive_cte(&cte, cursor, &table_name_lower)?
                    } else {
                        let subquery_cols = self.compile_subquery_to_ephemeral(
                            &cte.query,
                            cursor,
                            Some(&table_name_lower),
                        )?;
                        if let Some(explicit) = &cte.columns {
                            if explicit.len() != subquery_cols.len() {
                                return Err(Error::with_message(
                                    ErrorCode::Error,
                                    format!(
                                        "table {} has {} values for {} columns",
                                        cte.name,
                                        subquery_cols.len(),
                                        explicit.len()
                                    ),
                                ));
                            }
                            explicit.clone()
                        } else {
                            subquery_cols
                        }
                    };
                    let display_name = alias.clone().unwrap_or_else(|| table_name.clone());
                    self.tables.push(TableInfo {
                        name: display_name,
                        table_name: table_name.clone(),
                        cursor,
                        schema_table: None,
                        is_subquery: true,
                        join_type,
                        subquery_columns: Some(columns),
                    });
                    return Ok(());
                }

                let cursor = self.alloc_cursor();

                // Look up table in schema if available
                let schema_table = if table_name_lower == "sqlite_master"
                    || table_name_lower == "sqlite_schema"
                    || table_name_lower == "sqlite_temp_master"
                    || table_name_lower == "sqlite_temp_schema"
                {
                    // Create a virtual schema for sqlite_master/sqlite_temp_master
                    use crate::schema::{Affinity, Column, Table};
                    let is_temp = table_name_lower.contains("temp");
                    Some(std::sync::Arc::new(Table {
                        name: table_name_lower.clone(),
                        db_idx: if is_temp { 1 } else { 0 },
                        root_page: 1,
                        columns: vec![
                            Column {
                                name: "type".to_string(),
                                type_name: Some("TEXT".to_string()),
                                affinity: Affinity::Text,
                                not_null: false,
                                not_null_conflict: None,
                                default_value: None,
                                collation: "BINARY".to_string(),
                                is_primary_key: false,
                                is_unique: false,
                                unique_conflict: None,
                                is_hidden: false,
                                generated: None,
                            },
                            Column {
                                name: "name".to_string(),
                                type_name: Some("TEXT".to_string()),
                                affinity: Affinity::Text,
                                not_null: false,
                                not_null_conflict: None,
                                default_value: None,
                                collation: "BINARY".to_string(),
                                is_primary_key: false,
                                is_unique: false,
                                unique_conflict: None,
                                is_hidden: false,
                                generated: None,
                            },
                            Column {
                                name: "tbl_name".to_string(),
                                type_name: Some("TEXT".to_string()),
                                affinity: Affinity::Text,
                                not_null: false,
                                not_null_conflict: None,
                                default_value: None,
                                collation: "BINARY".to_string(),
                                is_primary_key: false,
                                is_unique: false,
                                unique_conflict: None,
                                is_hidden: false,
                                generated: None,
                            },
                            Column {
                                name: "rootpage".to_string(),
                                type_name: Some("INTEGER".to_string()),
                                affinity: Affinity::Integer,
                                not_null: false,
                                not_null_conflict: None,
                                default_value: None,
                                collation: "BINARY".to_string(),
                                is_primary_key: false,
                                is_unique: false,
                                unique_conflict: None,
                                is_hidden: false,
                                generated: None,
                            },
                            Column {
                                name: "sql".to_string(),
                                type_name: Some("TEXT".to_string()),
                                affinity: Affinity::Text,
                                not_null: false,
                                not_null_conflict: None,
                                default_value: None,
                                collation: "BINARY".to_string(),
                                is_primary_key: false,
                                is_unique: false,
                                unique_conflict: None,
                                is_hidden: false,
                                generated: None,
                            },
                        ],
                        primary_key: None,
                        indexes: Vec::new(),
                        foreign_keys: Vec::new(),
                        checks: Vec::new(),
                        without_rowid: false,
                        strict: false,
                        is_virtual: false,
                        virtual_module: None,
                        virtual_args: Vec::new(),
                        autoincrement: false,
                        sql: None,
                        row_estimate: 0,
                    }))
                } else if table_name_lower == "sqlite_stat1" {
                    // Create virtual schema for sqlite_stat1
                    use crate::schema::{Affinity, Column, Table};
                    Some(std::sync::Arc::new(Table {
                        name: "sqlite_stat1".to_string(),
                        db_idx: 0,
                        root_page: 0, // Virtual table, no root page
                        columns: vec![
                            Column {
                                name: "tbl".to_string(),
                                type_name: Some("TEXT".to_string()),
                                affinity: Affinity::Text,
                                not_null: false,
                                not_null_conflict: None,
                                default_value: None,
                                collation: "BINARY".to_string(),
                                is_primary_key: false,
                                is_unique: false,
                                unique_conflict: None,
                                is_hidden: false,
                                generated: None,
                            },
                            Column {
                                name: "idx".to_string(),
                                type_name: Some("TEXT".to_string()),
                                affinity: Affinity::Text,
                                not_null: false,
                                not_null_conflict: None,
                                default_value: None,
                                collation: "BINARY".to_string(),
                                is_primary_key: false,
                                is_unique: false,
                                unique_conflict: None,
                                is_hidden: false,
                                generated: None,
                            },
                            Column {
                                name: "stat".to_string(),
                                type_name: Some("TEXT".to_string()),
                                affinity: Affinity::Text,
                                not_null: false,
                                not_null_conflict: None,
                                default_value: None,
                                collation: "BINARY".to_string(),
                                is_primary_key: false,
                                is_unique: false,
                                unique_conflict: None,
                                is_hidden: false,
                                generated: None,
                            },
                        ],
                        primary_key: None,
                        indexes: Vec::new(),
                        foreign_keys: Vec::new(),
                        checks: Vec::new(),
                        without_rowid: false,
                        strict: false,
                        is_virtual: false,
                        virtual_module: None,
                        virtual_args: Vec::new(),
                        autoincrement: false,
                        sql: None,
                        row_estimate: 0,
                    }))
                } else {
                    // First check if this is a view in main or temp schema
                    // Handle explicit database qualifiers and main_view_depth
                    let view_opt = if let Some(ref schema_name) = name.schema {
                        // Explicit database qualifier - respect it
                        let schema_lower = schema_name.to_lowercase();
                        if schema_lower == "temp" {
                            self.temp_schema
                                .and_then(|s| s.views.get(&table_name_lower))
                        } else {
                            self.schema.and_then(|s| s.views.get(&table_name_lower))
                        }
                    } else if self.main_view_depth > 0 {
                        // Unqualified name inside main view: only look in main schema
                        self.schema.and_then(|s| s.views.get(&table_name_lower))
                    } else {
                        // Unqualified name at top-level: check temp first, then main
                        self.temp_schema
                            .and_then(|s| s.views.get(&table_name_lower))
                            .or_else(|| self.schema.and_then(|s| s.views.get(&table_name_lower)))
                    };

                    if let Some(view) = view_opt {
                        // Check if view access is disabled (temp views are always allowed)
                        if !self.enable_view && view.db_idx != 1 {
                            return Err(Error::with_message(
                                ErrorCode::Error,
                                format!("access to view \"{}\" prohibited", view.name),
                            ));
                        }

                        // Check for circular view definition
                        if self.expanding_views.contains(&table_name_lower) {
                            return Err(Error::with_message(
                                ErrorCode::Error,
                                format!("view {} is circularly defined", view.name),
                            ));
                        }

                        // Mark this view as being expanded
                        self.expanding_views.insert(table_name_lower.clone());

                        // Track if we're entering a main database view
                        let is_main_view = view.db_idx == 0;
                        if is_main_view {
                            self.main_view_depth += 1;
                        }

                        // Expand view as subquery
                        let view_select = (*view.select).clone();
                        let view_alias = alias.clone().unwrap_or_else(|| table_name.clone());
                        let view_columns = view.columns.clone();

                        // Compile view's SELECT as a subquery
                        let cursor = self.alloc_cursor();
                        self.emit(Opcode::OpenEphemeral, cursor, 0, 0, P4::Unused);

                        let subquery_dest = SelectDest::EphemTable { cursor };
                        let mut subcompiler = if let Some(schema) = self.schema {
                            SelectCompiler::with_schema(schema)
                        } else {
                            SelectCompiler::new()
                        };
                        if let Some(temp_schema) = self.temp_schema {
                            subcompiler.set_temp_schema(temp_schema);
                        }
                        subcompiler.next_reg = self.next_reg;
                        subcompiler.next_cursor = self.next_cursor;
                        // Propagate expanding_views for circular view detection
                        subcompiler.expanding_views = self.expanding_views.clone();
                        // Propagate main_view_depth
                        subcompiler.main_view_depth = self.main_view_depth;
                        // Propagate enable_view flag
                        subcompiler.enable_view = self.enable_view;
                        subcompiler
                            .set_column_name_flags(self.short_column_names, self.full_column_names);
                        let result = subcompiler.compile(&view_select, &subquery_dest);

                        // Remove from expanding set and restore depth (whether success or failure)
                        self.expanding_views.remove(&table_name_lower);
                        if is_main_view {
                            self.main_view_depth -= 1;
                        }

                        let subquery_ops = result?;

                        // Capture view's result column names for * expansion
                        let subquery_col_names = subcompiler.result_column_names.clone();

                        // Inline the subquery ops (skip Init/Halt/Transaction/Goto wrapper)
                        for op in subquery_ops {
                            if op.opcode != Opcode::Halt
                                && op.opcode != Opcode::Init
                                && op.opcode != Opcode::Transaction
                                && op.opcode != Opcode::Goto
                            {
                                self.ops.push(op);
                            }
                        }

                        self.next_reg = subcompiler.next_reg;
                        self.next_cursor = subcompiler.next_cursor;

                        // Use view's explicit column names if defined, otherwise use subquery column names
                        let final_col_names = if let Some(explicit_cols) = view_columns {
                            // View has explicit column names like CREATE VIEW v(c1, c2) AS ...
                            explicit_cols
                        } else {
                            subquery_col_names
                        };

                        self.tables.push(TableInfo {
                            name: view_alias,
                            table_name: String::new(),
                            cursor,
                            schema_table: None,
                            is_subquery: true,
                            join_type,
                            subquery_columns: Some(final_col_names),
                        });
                        return Ok(());
                    }

                    // Check if table exists (but not for sqlite_ internal tables)
                    if let Some(schema) = self.schema {
                        if !table_name_lower.starts_with("sqlite_")
                            && !schema.tables.contains_key(&table_name_lower)
                        {
                            // Also check temp schema for temp tables
                            let in_temp = self
                                .temp_schema
                                .map(|s| s.tables.contains_key(&table_name_lower))
                                .unwrap_or(false);
                            // Also check attached schemas
                            let in_attached = name.schema.as_ref().map_or(false, |schema_name| {
                                self.attached_schemas.iter().any(|(db_name, db_schema)| {
                                    db_name.eq_ignore_ascii_case(schema_name)
                                        && db_schema.tables.contains_key(&table_name_lower)
                                })
                            });
                            if !in_temp && !in_attached {
                                return Err(Error::with_message(
                                    ErrorCode::Error,
                                    format!("no such table: {}", qualified_name),
                                ));
                            }
                        }
                    }

                    // Look up table schema from main or temp schema
                    self.schema
                        .and_then(|s| s.tables.get(&table_name_lower).cloned())
                        .or_else(|| {
                            self.temp_schema
                                .and_then(|s| s.tables.get(&table_name_lower).cloned())
                        })
                };

                // Open the table (read mode)
                // Use qualified_name to support attached database references like "db.table"
                self.emit(
                    Opcode::OpenRead,
                    cursor,
                    0,
                    0,
                    P4::Text(qualified_name.clone()),
                );

                self.tables.push(TableInfo {
                    name: alias.clone().unwrap_or_else(|| table_name.clone()),
                    table_name: table_name.clone(),
                    cursor,
                    schema_table,
                    is_subquery: false,
                    join_type,
                    subquery_columns: None,
                });
            }
            TableRef::Subquery { query, alias } => {
                // Compile subquery as coroutine
                let cursor = self.alloc_cursor();
                self.emit(Opcode::OpenEphemeral, cursor, 0, 0, P4::Unused);

                // Compile subquery into ephemeral table
                let subquery_dest = SelectDest::EphemTable { cursor };
                let mut subcompiler = if let Some(schema) = self.schema {
                    SelectCompiler::with_schema(schema)
                } else {
                    SelectCompiler::new()
                };
                subcompiler.next_reg = self.next_reg;
                subcompiler.next_cursor = self.next_cursor;
                // Pass column naming settings to subquery compiler
                subcompiler.set_column_name_flags(self.short_column_names, self.full_column_names);
                let subquery_ops = subcompiler.compile(query, &subquery_dest)?;

                // Capture subquery result column names for * expansion
                let subquery_col_names = subcompiler.result_column_names.clone();

                // Inline the subquery ops (skip Init/Halt/Transaction/Goto wrapper)
                for op in subquery_ops {
                    if op.opcode != Opcode::Halt
                        && op.opcode != Opcode::Init
                        && op.opcode != Opcode::Transaction
                        && op.opcode != Opcode::Goto
                    {
                        self.ops.push(op);
                    }
                }

                self.next_reg = subcompiler.next_reg;
                self.next_cursor = subcompiler.next_cursor;

                // SQLite uses "(subquery-N)" format for anonymous subqueries
                let subquery_name = alias.clone().unwrap_or_else(|| {
                    let name = format!("(subquery-{})", self.next_subquery);
                    self.next_subquery += 1;
                    name
                });
                self.tables.push(TableInfo {
                    name: subquery_name,
                    table_name: String::new(),
                    cursor,
                    schema_table: None,
                    is_subquery: true,
                    join_type,
                    subquery_columns: Some(subquery_col_names),
                });
            }
            TableRef::Join {
                left,
                join_type: jt,
                right,
                constraint: _,
            } => {
                // Compile left side (no join type - it's the base)
                self.compile_table_ref(left, JoinFlags::empty())?;
                // Compile right side with join type
                self.compile_table_ref(right, *jt)?;
                // Join constraint is handled in WHERE clause processing
            }
            TableRef::Parens(inner) => {
                self.compile_table_ref(inner, join_type)?;
            }
            TableRef::TableFunction {
                name,
                args: _,
                alias: _,
            } => {
                // Table-valued functions are more complex
                // For now, treat as error
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("Table-valued function {} not yet supported", name),
                ));
            }
        }
        Ok(())
    }

    /// Pre-scan result columns to extract alias expressions for WHERE clause resolution
    /// SQLite allows referencing result column aliases in WHERE as an extension,
    /// but ONLY if the alias doesn't shadow an actual table column name.
    fn prescan_result_aliases(&mut self, columns: &[ResultColumn]) {
        self.alias_expressions.clear();
        for col in columns {
            if let ResultColumn::Expr { expr, alias } = col {
                if let Some(alias_name) = alias {
                    let alias_lower = alias_name.to_lowercase();
                    // Don't add alias if it shadows a table column name
                    // SQLite's standard behavior is to use table columns in WHERE
                    let shadows_column = self
                        .tables
                        .iter()
                        .skip(self.outer_tables_boundary)
                        .any(|tinfo| self.column_index_in_table(tinfo, &alias_lower).is_some());
                    if !shadows_column {
                        self.alias_expressions.insert(alias_lower, expr.clone());
                    }
                }
            }
        }
    }

    /// Compile result columns
    fn compile_result_columns(&mut self, columns: &[ResultColumn]) -> Result<(i32, usize)> {
        // Clear alias_expressions to avoid result column aliases interfering with
        // column resolution within the result columns themselves.
        // alias_expressions was populated by prescan_result_aliases for WHERE clause
        // resolution, but should not affect result column compilation.
        self.alias_expressions.clear();

        // Track result registers explicitly since they may not be contiguous
        // (function arguments allocate intermediate registers)
        let mut result_regs: Vec<i32> = Vec::new();

        for col in columns {
            match col {
                ResultColumn::Star => {
                    // Expand * to all columns from all tables using schema
                    // Skip coalesced columns from NATURAL/USING joins (they're shown from the left table)
                    let tables_snapshot: Vec<_> = self.tables.clone();
                    let coalesced_snapshot = self.coalesced_columns.clone();

                    // Check if there are any tables to expand - SELECT * requires at least one table
                    if tables_snapshot.is_empty() {
                        return Err(Error::with_message(ErrorCode::Error, "no tables specified"));
                    }

                    for (table_idx, table) in tables_snapshot.iter().enumerate() {
                        // Get the set of columns to exclude for this table (if any)
                        let excluded_cols = coalesced_snapshot.get(&table_idx);

                        if let Some(schema_table) = &table.schema_table {
                            // Regular table - expand from schema
                            for (col_idx, col_def) in schema_table.columns.iter().enumerate() {
                                // Skip coalesced columns (from NATURAL/USING on right table)
                                if let Some(excluded) = excluded_cols {
                                    if excluded.contains(&col_def.name.to_lowercase()) {
                                        continue;
                                    }
                                }

                                let reg = self.alloc_reg();

                                // Check if this is a VIRTUAL generated column
                                // Use VColumn for virtual tables
                                let col_opcode = if schema_table.is_virtual {
                                    Opcode::VColumn
                                } else {
                                    Opcode::Column
                                };

                                if let Some(ref gen) = col_def.generated {
                                    if gen.storage == GeneratedStorage::Virtual {
                                        // Compile the generated expression
                                        let gen_expr = Self::convert_schema_expr_to_ast(&gen.expr);
                                        self.compile_expr(&gen_expr, reg)?;
                                    } else {
                                        // STORED generated column - read normally
                                        self.emit(
                                            col_opcode,
                                            table.cursor,
                                            col_idx as i32,
                                            reg,
                                            P4::Unused,
                                        );
                                    }
                                } else if let Some(ipk_idx) = schema_table.rowid_alias_column() {
                                    // Check if this is the INTEGER PRIMARY KEY column (rowid alias)
                                    // If so, emit Rowid instead of Column since IPK isn't stored in the table
                                    if col_idx == ipk_idx {
                                        self.emit(Opcode::Rowid, table.cursor, reg, 0, P4::Unused);
                                    } else {
                                        self.emit(
                                            col_opcode,
                                            table.cursor,
                                            col_idx as i32,
                                            reg,
                                            P4::Unused,
                                        );
                                    }
                                } else {
                                    self.emit(
                                        col_opcode,
                                        table.cursor,
                                        col_idx as i32,
                                        reg,
                                        P4::Unused,
                                    );
                                }
                                // Generate column name based on PRAGMA settings
                                // For * expansion, only short_column_names matters:
                                //  - short_column_names=ON: just column name
                                //  - short_column_names=OFF: use alias.column (regardless of full_column_names)
                                let col_name = if self.short_column_names {
                                    // short_column_names=ON: always use just column name for *
                                    col_def.name.clone()
                                } else {
                                    // short_column_names=OFF: use alias prefix (table.name is alias or table name)
                                    format!("{}.{}", table.name, col_def.name)
                                };
                                self.result_column_names.push(col_name);
                                result_regs.push(reg);
                            }
                        } else if let Some(subquery_cols) = &table.subquery_columns {
                            // Subquery - expand from captured column names
                            for (col_idx, subquery_col_name) in subquery_cols.iter().enumerate() {
                                // Skip coalesced columns (from NATURAL/USING on right table)
                                if let Some(excluded) = excluded_cols {
                                    if excluded.contains(&subquery_col_name.to_lowercase()) {
                                        continue;
                                    }
                                }

                                let reg = self.alloc_reg();
                                self.emit(
                                    Opcode::Column,
                                    table.cursor,
                                    col_idx as i32,
                                    reg,
                                    P4::Unused,
                                );
                                // Generate column name - use subquery alias prefix when short_column_names=OFF
                                let col_name = if self.short_column_names {
                                    subquery_col_name.clone()
                                } else {
                                    format!("{}.{}", table.name, subquery_col_name)
                                };
                                self.result_column_names.push(col_name);
                                result_regs.push(reg);
                            }
                        }
                    }
                }
                ResultColumn::TableStar(table_name) => {
                    // Expand table.* to columns from specific table
                    // Note: Only match on table.name (alias if provided, or original name if no alias)
                    // SQLite doesn't allow using the original table name when an alias is provided
                    let tables_snapshot: Vec<_> = self.tables.clone();
                    let mut found = false;
                    for table in &tables_snapshot {
                        if table.name.eq_ignore_ascii_case(table_name) {
                            found = true;
                            if let Some(schema_table) = &table.schema_table {
                                // Regular table - expand from schema
                                for (col_idx, col_def) in schema_table.columns.iter().enumerate() {
                                    let reg = self.alloc_reg();

                                    // Check if this is a VIRTUAL generated column
                                    if let Some(ref gen) = col_def.generated {
                                        if gen.storage == GeneratedStorage::Virtual {
                                            // Compile the generated expression
                                            let gen_expr =
                                                Self::convert_schema_expr_to_ast(&gen.expr);
                                            self.compile_expr(&gen_expr, reg)?;
                                        } else {
                                            // STORED generated column - read normally
                                            self.emit(
                                                Opcode::Column,
                                                table.cursor,
                                                col_idx as i32,
                                                reg,
                                                P4::Unused,
                                            );
                                        }
                                    } else if let Some(ipk_idx) = schema_table.rowid_alias_column()
                                    {
                                        // Check if this is the INTEGER PRIMARY KEY column (rowid alias)
                                        if col_idx == ipk_idx {
                                            self.emit(
                                                Opcode::Rowid,
                                                table.cursor,
                                                reg,
                                                0,
                                                P4::Unused,
                                            );
                                        } else {
                                            self.emit(
                                                Opcode::Column,
                                                table.cursor,
                                                col_idx as i32,
                                                reg,
                                                P4::Unused,
                                            );
                                        }
                                    } else {
                                        self.emit(
                                            Opcode::Column,
                                            table.cursor,
                                            col_idx as i32,
                                            reg,
                                            P4::Unused,
                                        );
                                    }
                                    self.result_column_names.push(col_def.name.clone());
                                    result_regs.push(reg);
                                }
                            } else if let Some(subquery_cols) = &table.subquery_columns {
                                // Subquery - expand from captured column names
                                for (col_idx, subquery_col_name) in subquery_cols.iter().enumerate()
                                {
                                    let reg = self.alloc_reg();
                                    self.emit(
                                        Opcode::Column,
                                        table.cursor,
                                        col_idx as i32,
                                        reg,
                                        P4::Unused,
                                    );
                                    self.result_column_names.push(subquery_col_name.clone());
                                    result_regs.push(reg);
                                }
                            }
                            break;
                        }
                    }
                    if !found {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("no such table: {}", table_name),
                        ));
                    }
                }
                ResultColumn::Expr { expr, alias } => {
                    let reg = self.alloc_reg();
                    self.compile_expr(expr, reg)?;
                    result_regs.push(reg);

                    let name = alias
                        .clone()
                        .unwrap_or_else(|| self.expr_to_name(expr, result_regs.len()));
                    self.result_column_names.push(name.clone());
                    // Collect aliases for later - don't populate result_aliases yet
                    // to avoid subsequent columns incorrectly resolving to earlier aliases
                    self.columns.push(ColumnInfo {
                        name,
                        table: None,
                        affinity: Affinity::Blob,
                        reg,
                        alias: alias.clone(),
                    });
                }
            }
        }

        // Now populate result_aliases for ORDER BY resolution
        // This must happen AFTER all result columns are compiled
        for col_info in &self.columns {
            if let Some(alias_name) = &col_info.alias {
                self.result_aliases
                    .insert(alias_name.to_lowercase(), col_info.reg);
            }
        }

        // Check if result registers are contiguous
        let count = result_regs.len();
        if count == 0 {
            return Ok((self.next_reg, 0));
        }

        let base_reg = result_regs[0];
        let mut contiguous = true;
        for (i, &reg) in result_regs.iter().enumerate() {
            if reg != base_reg + i as i32 {
                contiguous = false;
                break;
            }
        }

        if contiguous {
            // Registers are already contiguous
            Ok((base_reg, count))
        } else {
            // Copy result values to contiguous registers
            let new_base = self.next_reg;
            for &src_reg in result_regs.iter() {
                let dest_reg = self.alloc_reg();
                // Only copy if not already in the right place
                if src_reg != dest_reg {
                    self.emit(Opcode::Copy, src_reg, dest_reg, 0, P4::Unused);
                }
            }
            Ok((new_base, count))
        }
    }

    /// Convert an expression to a column name, respecting PRAGMA settings
    fn expr_to_name(&self, expr: &Expr, index: usize) -> String {
        match expr {
            // COLLATE expressions inherit the name of their underlying expression
            // e.g., "x COLLATE rtrim" should be named "x" for column matching
            Expr::Collate { expr: inner, .. } => self.expr_to_name(inner, index),
            Expr::Column(col) => {
                // Handle column naming based on PRAGMA settings
                // full_column_names=ON: use "realTable.column"
                // short_column_names=ON (default): use just "column"
                // Both OFF: use original format

                if self.full_column_names {
                    // full_column_names takes precedence - use real table name
                    let real_table_name = if let Some(alias_or_name) = &col.table {
                        // Look up the real table name from the alias
                        self.tables
                            .iter()
                            .find(|t| t.name.eq_ignore_ascii_case(alias_or_name))
                            .map(|t| {
                                // Use real table name, not alias (unless it's a subquery)
                                if t.table_name.is_empty() {
                                    t.name.clone() // Subquery - use alias
                                } else {
                                    t.table_name.clone()
                                }
                            })
                            .unwrap_or_else(|| alias_or_name.clone())
                    } else {
                        // No table specified - try to find which table has this column
                        self.tables
                            .iter()
                            .find(|t| {
                                t.schema_table.as_ref().map_or(false, |st| {
                                    st.columns
                                        .iter()
                                        .any(|c| c.name.eq_ignore_ascii_case(&col.column))
                                })
                            })
                            .map(|t| t.table_name.clone())
                            .unwrap_or_default()
                    };

                    if real_table_name.is_empty() {
                        col.column.clone()
                    } else {
                        format!("{}.{}", real_table_name, col.column)
                    }
                } else if self.short_column_names {
                    // short_column_names=ON (default): just column name
                    col.column.clone()
                } else {
                    // Both OFF: use original source text to preserve whitespace
                    if let Some(ref source) = col.source_text {
                        return source.clone();
                    }
                    // Fallback: reconstruct from table/column names
                    let real_table_name = if let Some(alias_or_name) = &col.table {
                        self.tables
                            .iter()
                            .find(|t| t.name.eq_ignore_ascii_case(alias_or_name))
                            .map(|t| {
                                if t.table_name.is_empty() {
                                    t.name.clone()
                                } else {
                                    t.table_name.clone()
                                }
                            })
                            .unwrap_or_else(|| alias_or_name.clone())
                    } else {
                        self.tables
                            .iter()
                            .find(|t| {
                                t.schema_table.as_ref().map_or(false, |st| {
                                    st.columns
                                        .iter()
                                        .any(|c| c.name.eq_ignore_ascii_case(&col.column))
                                })
                            })
                            .map(|t| t.table_name.clone())
                            .unwrap_or_default()
                    };

                    if real_table_name.is_empty() {
                        col.column.clone()
                    } else {
                        format!("{}.{}", real_table_name, col.column)
                    }
                }
            }
            _ => self.expr_to_string(expr),
        }
    }

    /// Convert an expression to its SQL string representation
    fn expr_to_string(&self, expr: &Expr) -> String {
        match expr {
            Expr::Column(col) => {
                if let Some(table) = &col.table {
                    format!("{}.{}", table, col.column)
                } else {
                    col.column.clone()
                }
            }
            Expr::Literal(lit) => match lit {
                Literal::Integer(i) => i.to_string(),
                Literal::Float(f) => f.to_string(),
                Literal::String(s) => format!("'{}'", s),
                Literal::Blob(b) => format!(
                    "X'{}'",
                    b.iter()
                        .map(|byte| format!("{:02X}", byte))
                        .collect::<String>()
                ),
                Literal::Null => "NULL".to_string(),
                Literal::Bool(b) => {
                    if *b {
                        "TRUE".to_string()
                    } else {
                        "FALSE".to_string()
                    }
                }
                Literal::CurrentTime => "CURRENT_TIME".to_string(),
                Literal::CurrentDate => "CURRENT_DATE".to_string(),
                Literal::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
            },
            Expr::Function(func) => {
                use crate::parser::ast::FunctionArgs;
                let args_str = match &func.args {
                    FunctionArgs::Star => "*".to_string(),
                    FunctionArgs::Exprs(exprs) => exprs
                        .iter()
                        .map(|e| self.expr_to_string(e))
                        .collect::<Vec<_>>()
                        .join(","),
                };
                format!("{}({})", func.name.to_lowercase(), args_str)
            }
            Expr::Binary { op, left, right } => {
                let op_str = match op {
                    BinaryOp::Add => "+",
                    BinaryOp::Sub => "-",
                    BinaryOp::Mul => "*",
                    BinaryOp::Div => "/",
                    BinaryOp::Mod => "%",
                    BinaryOp::Concat => "||",
                    BinaryOp::Eq => "=",
                    BinaryOp::Ne => "<>",
                    BinaryOp::Lt => "<",
                    BinaryOp::Le => "<=",
                    BinaryOp::Gt => ">",
                    BinaryOp::Ge => ">=",
                    BinaryOp::And => " AND ",
                    BinaryOp::Or => " OR ",
                    BinaryOp::BitAnd => "&",
                    BinaryOp::BitOr => "|",
                    BinaryOp::ShiftLeft => "<<",
                    BinaryOp::ShiftRight => ">>",
                    BinaryOp::Is => " IS ",
                    BinaryOp::IsNot => " IS NOT ",
                    BinaryOp::JsonExtract => "->",
                    BinaryOp::JsonExtractText => "->>",
                };
                format!(
                    "{}{}{}",
                    self.expr_to_string(left),
                    op_str,
                    self.expr_to_string(right)
                )
            }
            Expr::Unary { op, expr } => {
                use crate::parser::ast::UnaryOp;
                let op_str = match op {
                    UnaryOp::Neg => "-",
                    UnaryOp::Pos => "+",
                    UnaryOp::Not => "NOT ",
                    UnaryOp::BitNot => "~",
                };
                format!("{}{}", op_str, self.expr_to_string(expr))
            }
            Expr::Cast { expr, type_name } => {
                format!("CAST({} AS {})", self.expr_to_string(expr), type_name.name)
            }
            Expr::Collate { expr, collation } => {
                format!("{} COLLATE {}", self.expr_to_string(expr), collation)
            }
            _ => "?".to_string(),
        }
    }

    /// Compile WHERE condition with short-circuit evaluation
    ///
    /// For AND: evaluate left, jump to skip if false, then evaluate right
    /// For OR: evaluate left, jump to success if true, then evaluate right
    ///
    /// This avoids unnecessary evaluation of the right side when the result
    /// is already determined by the left side.
    fn compile_where_condition(&mut self, expr: &Expr, skip_label: i32) -> Result<()> {
        match expr {
            // Constant false: always skip (WHERE 0)
            Expr::Literal(Literal::Integer(0)) => {
                // Unconditionally jump to skip - this row will never match
                self.emit(Opcode::Goto, 0, skip_label, 0, P4::Unused);
                Ok(())
            }

            // Constant false: float zero
            Expr::Literal(Literal::Float(f)) if *f == 0.0 => {
                self.emit(Opcode::Goto, 0, skip_label, 0, P4::Unused);
                Ok(())
            }

            // Constant true: no check needed (WHERE 1)
            Expr::Literal(Literal::Integer(n)) if *n != 0 => {
                // Always true - no jump needed, fall through
                Ok(())
            }

            // Constant true: non-zero float
            Expr::Literal(Literal::Float(f)) if *f != 0.0 => {
                // Always true - no jump needed, fall through
                Ok(())
            }

            // Short-circuit AND: if left is false, skip right entirely
            Expr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => {
                // Evaluate left side - if false, jump to skip_label
                self.compile_where_condition(left, skip_label)?;
                // If we get here, left was true - now evaluate right side
                self.compile_where_condition(right, skip_label)?;
                Ok(())
            }

            // Short-circuit OR: if left is true, skip right entirely
            Expr::Binary {
                op: BinaryOp::Or,
                left,
                right,
            } => {
                // For OR, we need a "success" label to jump to when left is true
                let success_label = self.alloc_label();

                // Evaluate left side
                let left_reg = self.alloc_reg();
                self.compile_expr(left, left_reg)?;
                // If left is true (non-zero), jump to success
                self.emit(Opcode::If, left_reg, success_label, 0, P4::Unused);

                // Left was false, evaluate right side
                let right_reg = self.alloc_reg();
                self.compile_expr(right, right_reg)?;
                // If right is also false, jump to skip_label
                self.emit(Opcode::IfNot, right_reg, skip_label, 1, P4::Unused);

                // Either left was true (jumped here) or right was true (fell through)
                self.resolve_label(success_label, self.current_addr());
                Ok(())
            }

            // For parentheses, unwrap and recurse
            Expr::Parens(inner) => self.compile_where_condition(inner, skip_label),

            // For all other expressions, compile normally and check result
            _ => {
                let reg = self.alloc_reg();
                self.compile_expr(expr, reg)?;
                // If false (0), jump to skip_label
                self.emit(Opcode::IfNot, reg, skip_label, 1, P4::Unused);
                Ok(())
            }
        }
    }

    /// Populate result_column_names from the SELECT column list without generating code.
    /// Used when we need column names for subquery resolution but won't output any rows
    /// (e.g., constant false WHERE clause).
    fn populate_result_column_names(&mut self, columns: &[ResultColumn]) {
        let mut col_idx = 0;
        for col in columns {
            match col {
                ResultColumn::Expr { expr, alias } => {
                    col_idx += 1;
                    let name = alias
                        .clone()
                        .unwrap_or_else(|| self.expr_to_name(expr, col_idx));
                    self.result_column_names.push(name);
                }
                ResultColumn::Star => {
                    // Expand all columns from all tables
                    for table in &self.tables.clone() {
                        if let Some(schema_table) = &table.schema_table {
                            for col_def in &schema_table.columns {
                                let col_name = if self.short_column_names {
                                    col_def.name.clone()
                                } else {
                                    format!("{}.{}", table.name, col_def.name)
                                };
                                self.result_column_names.push(col_name);
                            }
                        } else if let Some(subquery_cols) = &table.subquery_columns {
                            for subquery_col_name in subquery_cols {
                                let col_name = if self.short_column_names {
                                    subquery_col_name.clone()
                                } else {
                                    format!("{}.{}", table.name, subquery_col_name)
                                };
                                self.result_column_names.push(col_name);
                            }
                        }
                    }
                }
                ResultColumn::TableStar(table_name) => {
                    // Expand columns from specific table
                    if let Some(table) = self
                        .tables
                        .iter()
                        .find(|t| t.name.eq_ignore_ascii_case(table_name))
                    {
                        if let Some(schema_table) = &table.schema_table {
                            for col_def in &schema_table.columns {
                                let col_name = if self.short_column_names {
                                    col_def.name.clone()
                                } else {
                                    format!("{}.{}", table.name, col_def.name)
                                };
                                self.result_column_names.push(col_name);
                            }
                        } else if let Some(subquery_cols) = &table.subquery_columns {
                            for subquery_col_name in subquery_cols {
                                let col_name = if self.short_column_names {
                                    subquery_col_name.clone()
                                } else {
                                    format!("{}.{}", table.name, subquery_col_name)
                                };
                                self.result_column_names.push(col_name);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Check if a WHERE clause is constant false (e.g., WHERE 0)
    /// If so, no rows can match and we can skip all loop generation.
    fn is_constant_false_where(&self, where_clause: Option<&Expr>) -> bool {
        match where_clause {
            None => false, // No WHERE means all rows match
            Some(expr) => self.is_constant_false_expr(expr),
        }
    }

    /// Check if an expression is constant false
    fn is_constant_false_expr(&self, expr: &Expr) -> bool {
        match expr {
            // Integer 0 is false
            Expr::Literal(Literal::Integer(0)) => true,
            // Float 0.0 is false
            Expr::Literal(Literal::Float(f)) if *f == 0.0 => true,
            // AND: if either side is constant false, result is false
            Expr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => self.is_constant_false_expr(left) || self.is_constant_false_expr(right),
            // Parentheses: unwrap
            Expr::Parens(inner) => self.is_constant_false_expr(inner),
            // Anything else is not obviously constant false
            _ => false,
        }
    }

    /// Compile only the runtime filter terms from WhereInfo
    ///
    /// This skips terms that were consumed by index seeks, avoiding
    /// redundant re-evaluation of conditions already satisfied by the index.
    /// Terms are sorted by eval_cost (cheapest first) for optimal short-circuit
    /// behavior, then compiled with short-circuit AND evaluation.
    fn compile_runtime_filter_terms(
        &mut self,
        where_info: &WhereInfo,
        skip_label: i32,
    ) -> Result<bool> {
        // Collect term indices that were consumed by index seeks
        let consumed_terms: std::collections::HashSet<i32> = where_info
            .levels
            .iter()
            .flat_map(|level| level.used_terms.iter().copied())
            .collect();

        // Check if any level is using an index scan with range bounds
        // This indicates LIKE index optimization is active
        let using_index_range = where_info.levels.iter().any(|level| {
            matches!(
                &level.plan,
                WherePlan::IndexScan {
                    range_start: Some(_),
                    ..
                } | WherePlan::IndexScan {
                    range_end: Some(_),
                    ..
                }
            )
        });

        // Collect non-consumed, non-virtual terms
        // Also skip LIKE terms that are fully satisfied by index optimization
        let mut filter_terms: Vec<&WhereTerm> = where_info
            .terms
            .iter()
            .filter(|term| {
                // Skip consumed terms
                if consumed_terms.contains(&term.idx) {
                    return false;
                }
                // Skip virtual terms
                if term.flags.contains(WhereTermFlags::VIRTUAL) {
                    return false;
                }
                // Skip LIKE terms that are fully satisfied by index range bounds
                if using_index_range && term.flags.contains(WhereTermFlags::LIKE_OPT_COMPLETE) {
                    return false;
                }
                true
            })
            .collect();

        // Sort by eval_cost - cheapest terms first for better short-circuit behavior
        filter_terms.sort_by_key(|term| term.eval_cost);

        // Compile each term with short-circuit AND
        let any_compiled = !filter_terms.is_empty();
        for term in filter_terms {
            // Compile this term - if false, jump to skip_label
            self.compile_where_condition(&term.expr, skip_label)?;
        }

        Ok(any_compiled)
    }

    fn split_virtual_filter(&self, expr: &Expr) -> (Option<Fts3MatchFilter>, Option<Expr>) {
        if self.is_fts3tokenize_table() {
            return self.split_fts3_tokenize_filter(expr);
        }
        if self.is_fts5_table() {
            return self.split_fts5_match_filter(expr);
        }
        self.split_fts3_match_filter(expr)
    }

    fn split_fts3_tokenize_filter(&self, expr: &Expr) -> (Option<Fts3MatchFilter>, Option<Expr>) {
        if let Some(filter) = self.extract_fts3_tokenize_filter(expr) {
            return (Some(filter), None);
        }
        if let Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } = expr
        {
            if let Some(filter) = self.extract_fts3_tokenize_filter(left) {
                return (Some(filter), Some(*right.clone()));
            }
            if let Some(filter) = self.extract_fts3_tokenize_filter(right) {
                return (Some(filter), Some(*left.clone()));
            }
        }
        (None, Some(expr.clone()))
    }

    fn split_fts3_match_filter(&self, expr: &Expr) -> (Option<Fts3MatchFilter>, Option<Expr>) {
        if let Some(filter) = self.extract_fts3_match_filter(expr) {
            return (Some(filter), None);
        }
        if let Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } = expr
        {
            if let (Some(left_filter), Some(right_filter)) = (
                self.extract_fts3_match_filter(left),
                self.extract_fts3_match_filter(right),
            ) {
                if let (Some(left_text), Some(right_text)) = (
                    filter_literal_text(&left_filter.pattern),
                    filter_literal_text(&right_filter.pattern),
                ) {
                    return (
                        Some(Fts3MatchFilter {
                            cursor: left_filter.cursor,
                            pattern: Expr::Literal(Literal::String(format!(
                                "{} AND {}",
                                left_text, right_text
                            ))),
                        }),
                        None,
                    );
                }
            }
            if let Some(filter) = self.extract_fts3_match_filter(left) {
                return (Some(filter), Some(*right.clone()));
            }
            if let Some(filter) = self.extract_fts3_match_filter(right) {
                return (Some(filter), Some(*left.clone()));
            }
        } else if let Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } = expr
        {
            if let (Some(left_filter), Some(right_filter)) = (
                self.extract_fts3_match_filter(left),
                self.extract_fts3_match_filter(right),
            ) {
                if let (Some(left_text), Some(right_text)) = (
                    filter_literal_text(&left_filter.pattern),
                    filter_literal_text(&right_filter.pattern),
                ) {
                    return (
                        Some(Fts3MatchFilter {
                            cursor: left_filter.cursor,
                            pattern: Expr::Literal(Literal::String(format!(
                                "{} OR {}",
                                left_text, right_text
                            ))),
                        }),
                        None,
                    );
                }
            }
        }
        (None, Some(expr.clone()))
    }

    fn split_fts5_match_filter(&self, expr: &Expr) -> (Option<Fts3MatchFilter>, Option<Expr>) {
        if let Some(filter) = self.extract_fts5_match_filter(expr) {
            return (Some(filter), None);
        }
        if let Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } = expr
        {
            if let (Some(left_filter), Some(right_filter)) = (
                self.extract_fts5_match_filter(left),
                self.extract_fts5_match_filter(right),
            ) {
                if let (Some(left_text), Some(right_text)) = (
                    filter_literal_text(&left_filter.pattern),
                    filter_literal_text(&right_filter.pattern),
                ) {
                    return (
                        Some(Fts3MatchFilter {
                            cursor: left_filter.cursor,
                            pattern: Expr::Literal(Literal::String(format!(
                                "{} AND {}",
                                left_text, right_text
                            ))),
                        }),
                        None,
                    );
                }
            }
            if let Some(filter) = self.extract_fts5_match_filter(left) {
                return (Some(filter), Some(*right.clone()));
            }
            if let Some(filter) = self.extract_fts5_match_filter(right) {
                return (Some(filter), Some(*left.clone()));
            }
        } else if let Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } = expr
        {
            if let (Some(left_filter), Some(right_filter)) = (
                self.extract_fts5_match_filter(left),
                self.extract_fts5_match_filter(right),
            ) {
                if let (Some(left_text), Some(right_text)) = (
                    filter_literal_text(&left_filter.pattern),
                    filter_literal_text(&right_filter.pattern),
                ) {
                    return (
                        Some(Fts3MatchFilter {
                            cursor: left_filter.cursor,
                            pattern: Expr::Literal(Literal::String(format!(
                                "{} OR {}",
                                left_text, right_text
                            ))),
                        }),
                        None,
                    );
                }
            }
        }
        (None, Some(expr.clone()))
    }

    fn extract_fts3_match_filter(&self, expr: &Expr) -> Option<Fts3MatchFilter> {
        if self.tables.len() != 1 {
            return None;
        }
        let table = self.tables.first()?;
        let schema_table = table.schema_table.as_ref()?;
        if !schema_table.is_virtual {
            return None;
        }
        let module = schema_table
            .virtual_module
            .as_ref()
            .map(|name| name.to_ascii_lowercase())?;
        if module != "fts3" {
            return None;
        }

        if let Expr::Like {
            expr: left,
            pattern,
            op: LikeOp::Match,
            negated: false,
            ..
        } = expr
        {
            match left.as_ref() {
                Expr::Column(col) => {
                    if let Some(ref table_name) = col.table {
                        if !table_name.eq_ignore_ascii_case(&table.table_name) {
                            return None;
                        }
                    } else if !col.column.eq_ignore_ascii_case(&table.table_name) {
                        return None;
                    }
                    return Some(Fts3MatchFilter {
                        cursor: table.cursor,
                        pattern: (*pattern.clone()),
                    });
                }
                _ => {}
            }
        }
        None
    }

    fn extract_fts5_match_filter(&self, expr: &Expr) -> Option<Fts3MatchFilter> {
        if self.tables.len() != 1 {
            return None;
        }
        let table = self.tables.first()?;
        let schema_table = table.schema_table.as_ref()?;
        if !schema_table.is_virtual {
            return None;
        }
        let module = schema_table
            .virtual_module
            .as_ref()
            .map(|name| name.to_ascii_lowercase())?;
        if module != "fts5" {
            return None;
        }

        if let Expr::Like {
            expr: left,
            pattern,
            op: LikeOp::Match,
            negated: false,
            ..
        } = expr
        {
            match left.as_ref() {
                Expr::Column(col) => {
                    if let Some(ref table_name) = col.table {
                        if !table_name.eq_ignore_ascii_case(&table.table_name) {
                            return None;
                        }
                    } else if !col.column.eq_ignore_ascii_case(&table.table_name) {
                        return None;
                    }
                    return Some(Fts3MatchFilter {
                        cursor: table.cursor,
                        pattern: (*pattern.clone()),
                    });
                }
                _ => {}
            }
        }
        None
    }

    fn extract_fts3_tokenize_filter(&self, expr: &Expr) -> Option<Fts3MatchFilter> {
        if self.tables.len() != 1 {
            return None;
        }
        let table = self.tables.first()?;
        let schema_table = table.schema_table.as_ref()?;
        if !schema_table.is_virtual {
            return None;
        }
        let module = schema_table
            .virtual_module
            .as_ref()
            .map(|name| name.to_ascii_lowercase())?;
        if module != "fts3tokenize" {
            return None;
        }

        if let Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } = expr
        {
            if self.is_fts3tokenize_input_column(left, table) {
                return Some(Fts3MatchFilter {
                    cursor: table.cursor,
                    pattern: (*right.clone()),
                });
            }
            if self.is_fts3tokenize_input_column(right, table) {
                return Some(Fts3MatchFilter {
                    cursor: table.cursor,
                    pattern: (*left.clone()),
                });
            }
        }
        None
    }

    fn is_fts3tokenize_table(&self) -> bool {
        let Some(table) = self.tables.first() else {
            return false;
        };
        let Some(schema_table) = table.schema_table.as_ref() else {
            return false;
        };
        schema_table
            .virtual_module
            .as_ref()
            .map(|name| name.eq_ignore_ascii_case("fts3tokenize"))
            .unwrap_or(false)
    }

    fn is_fts5_table(&self) -> bool {
        let Some(table) = self.tables.first() else {
            return false;
        };
        let Some(schema_table) = table.schema_table.as_ref() else {
            return false;
        };
        schema_table
            .virtual_module
            .as_ref()
            .map(|name| name.eq_ignore_ascii_case("fts5"))
            .unwrap_or(false)
    }

    fn is_fts3tokenize_input_column(&self, expr: &Expr, table: &TableInfo) -> bool {
        let Expr::Column(col) = expr else {
            return false;
        };
        if let Some(ref table_name) = col.table {
            if !table_name.eq_ignore_ascii_case(&table.table_name) {
                return false;
            }
        }
        col.column.eq_ignore_ascii_case("input")
    }

    fn is_fts3_match(&self, expr: &Expr) -> bool {
        self.extract_fts3_match_filter(expr).is_some()
    }

    /// Plan access to a virtual table using xBestIndex
    ///
    /// This extracts constraints from the WHERE clause that apply to the virtual table,
    /// calls best_index on the vtab instance, and returns a VFilterPlan.
    fn plan_virtual_table_access(
        &mut self,
        table: &TableInfo,
        where_clause: Option<&Expr>,
    ) -> Result<Option<VFilterPlan>> {
        // Need vtab_registry and schema_table
        let registry = match &self.vtab_registry {
            Some(r) => r.clone(),
            None => return Ok(None),
        };
        let schema_table = match &table.schema_table {
            Some(t) => t.clone(),
            None => return Ok(None),
        };
        if !schema_table.is_virtual {
            return Ok(None);
        }

        // Get the vtab instance
        let vtab = match registry.get_instance("main", &table.table_name) {
            Ok(Some(v)) => v,
            _ => return Ok(None),
        };

        // Extract constraints from WHERE clause
        let mut constraints = Vec::new();
        let mut constraint_exprs = Vec::new(); // Track exprs for each constraint

        if let Some(where_expr) = where_clause {
            self.extract_vtab_constraints(
                where_expr,
                table,
                &schema_table,
                &mut constraints,
                &mut constraint_exprs,
            );
        }

        // Build IndexInfo for best_index
        let mut index_info = VtabIndexInfo::new(constraints, vec![]);

        // Call best_index
        vtab.best_index(&mut index_info)?;

        // Build VFilterPlan from the result
        let mut plan_constraints = Vec::new();
        for (i, usage) in index_info.constraint_usage.iter().enumerate() {
            if usage.arg_index > 0 {
                // This constraint is used - allocate register for the value
                let value_reg = self.alloc_reg();

                // Compile the constraint expression into the register
                if i < constraint_exprs.len() {
                    self.compile_expr(&constraint_exprs[i], value_reg)?;
                }

                plan_constraints.push(VFilterConstraint {
                    col_idx: index_info.constraints[i].col_idx,
                    op: index_info.constraints[i].op,
                    value_reg,
                    omit: usage.omit,
                });
            }
        }

        // Only return a plan if best_index chose a non-trivial index
        // (idx_num != 0 or we have constraints)
        if index_info.idx_num != 0 || !plan_constraints.is_empty() {
            Ok(Some(VFilterPlan {
                idx_num: index_info.idx_num,
                idx_str: index_info.idx_str,
                constraints: plan_constraints,
            }))
        } else {
            Ok(None)
        }
    }

    /// Extract the collation name from an expression.
    /// If the expression is `expr COLLATE name`, returns `name` (uppercased).
    /// If the expression is a column reference, looks up the column's collation from schema.
    /// For positional ORDER BY (e.g., `ORDER BY 1`), resolves to the result column.
    /// Otherwise returns "BINARY" as the default collation.
    fn extract_collation_from_expr(&self, expr: &Expr, body: Option<&SelectBody>) -> String {
        match expr {
            Expr::Collate { collation, .. } => collation.to_uppercase(),
            Expr::Column(col_ref) => {
                // First check if this column name is an alias in the SELECT list
                if col_ref.table.is_none() {
                    if let Some(SelectBody::Select(core)) = body {
                        let col_lower = col_ref.column.to_lowercase();
                        for result_col in &core.columns {
                            if let ResultColumn::Expr {
                                alias,
                                expr: col_expr,
                            } = result_col
                            {
                                if let Some(al) = alias {
                                    if al.to_lowercase() == col_lower {
                                        // Found alias - recursively get collation from underlying expr
                                        return self.extract_collation_from_expr(col_expr, body);
                                    }
                                }
                            }
                        }
                    }
                }
                // Look up the column's collation from schema
                self.lookup_column_collation(col_ref.table.as_deref(), &col_ref.column)
            }
            Expr::Literal(Literal::Integer(n)) => {
                // Position-based ORDER BY (e.g., ORDER BY 1)
                // Resolve to the result column and get its collation
                if let Some(SelectBody::Select(core)) = body {
                    let idx = (*n as usize).saturating_sub(1);
                    // Count actual column positions accounting for * expansion
                    let mut col_pos = 0;
                    for result_col in &core.columns {
                        match result_col {
                            ResultColumn::Expr { expr: col_expr, .. } => {
                                if col_pos == idx {
                                    return self.extract_collation_from_expr(col_expr, body);
                                }
                                col_pos += 1;
                            }
                            ResultColumn::Star => {
                                // SELECT * - need to expand from FROM clause
                                if let Some(from) = &core.from {
                                    let count = self.count_columns_from_from(from);
                                    if idx < col_pos + count {
                                        // The position is within this * expansion
                                        let offset = idx - col_pos;
                                        if let Some(collation) =
                                            self.get_column_collation_from_from(from, offset)
                                        {
                                            return collation;
                                        }
                                    }
                                    col_pos += count;
                                }
                            }
                            ResultColumn::TableStar(table_name) => {
                                // SELECT table.* - need to expand for specific table
                                if let Some(from) = &core.from {
                                    let count = self.count_columns_from_table(from, table_name);
                                    if idx < col_pos + count {
                                        let offset = idx - col_pos;
                                        if let Some(collation) = self
                                            .get_column_collation_from_table(
                                                from, table_name, offset,
                                            )
                                        {
                                            return collation;
                                        }
                                    }
                                    col_pos += count;
                                }
                            }
                        }
                    }
                }
                "BINARY".to_string()
            }
            _ => "BINARY".to_string(),
        }
    }

    /// Count how many columns the FROM clause would expand to
    fn count_columns_from_from(&self, from: &FromClause) -> usize {
        let mut count = 0;
        for table_ref in &from.tables {
            count += self.count_columns_from_table_ref(table_ref);
        }
        count
    }

    fn count_columns_from_table_ref(&self, table_ref: &TableRef) -> usize {
        match table_ref {
            TableRef::Table { name, .. } => {
                let table_name = &name.name;
                if let Some(schema) = self.schema {
                    if let Some(table) = schema.tables.get(&table_name.to_lowercase()) {
                        return table.columns.len();
                    }
                }
                0
            }
            TableRef::Subquery { .. } => 0, // Would need more complex handling
            TableRef::TableFunction { .. } => 0,
            TableRef::Join { left, right, .. } => {
                self.count_columns_from_table_ref(left) + self.count_columns_from_table_ref(right)
            }
            _ => 0,
        }
    }

    fn count_columns_from_table(&self, from: &FromClause, table_name: &str) -> usize {
        let tbl_lower = table_name.to_lowercase();
        for table_ref in &from.tables {
            if let TableRef::Table { name, alias, .. } = table_ref {
                let matches = name.name.to_lowercase() == tbl_lower
                    || alias.as_ref().map(|a| a.to_lowercase()) == Some(tbl_lower.clone());
                if matches {
                    return self.count_columns_from_table_ref(table_ref);
                }
            }
        }
        0
    }

    /// Get the collation for a specific column at offset within the FROM clause expansion
    fn get_column_collation_from_from(&self, from: &FromClause, offset: usize) -> Option<String> {
        let mut current_offset = 0;
        for table_ref in &from.tables {
            let count = self.count_columns_from_table_ref(table_ref);
            if offset < current_offset + count {
                let local_offset = offset - current_offset;
                return self.get_column_collation_from_table_ref(table_ref, local_offset);
            }
            current_offset += count;
        }
        None
    }

    fn get_column_collation_from_table_ref(
        &self,
        table_ref: &TableRef,
        offset: usize,
    ) -> Option<String> {
        match table_ref {
            TableRef::Table { name, .. } => {
                let table_name = &name.name;
                if let Some(schema) = self.schema {
                    if let Some(table) = schema.tables.get(&table_name.to_lowercase()) {
                        if let Some(col) = table.columns.get(offset) {
                            if col.collation.is_empty() || col.collation == "BINARY" {
                                return Some("BINARY".to_string());
                            }
                            return Some(col.collation.to_uppercase());
                        }
                    }
                }
                None
            }
            TableRef::Join { left, right, .. } => {
                let left_count = self.count_columns_from_table_ref(left);
                if offset < left_count {
                    self.get_column_collation_from_table_ref(left, offset)
                } else {
                    self.get_column_collation_from_table_ref(right, offset - left_count)
                }
            }
            _ => None,
        }
    }

    fn get_column_collation_from_table(
        &self,
        from: &FromClause,
        table_name: &str,
        offset: usize,
    ) -> Option<String> {
        let tbl_lower = table_name.to_lowercase();
        for table_ref in &from.tables {
            if let TableRef::Table { name, alias, .. } = table_ref {
                let matches = name.name.to_lowercase() == tbl_lower
                    || alias.as_ref().map(|a| a.to_lowercase()) == Some(tbl_lower.clone());
                if matches {
                    return self.get_column_collation_from_table_ref(table_ref, offset);
                }
            }
        }
        None
    }

    /// Look up the collation for a column from the schema.
    fn lookup_column_collation(&self, table_name: Option<&str>, column_name: &str) -> String {
        let col_name_lower = column_name.to_lowercase();

        // First, try looking in self.tables (if populated)
        for table_info in &self.tables {
            // If a specific table name is given, check if it matches
            if let Some(tbl) = table_name {
                let tbl_lower = tbl.to_lowercase();
                if table_info.name.to_lowercase() != tbl_lower
                    && table_info.table_name.to_lowercase() != tbl_lower
                {
                    continue;
                }
            }

            // Look up the column in the schema
            if let Some(schema_table) = &table_info.schema_table {
                for col in &schema_table.columns {
                    if col.name.to_lowercase() == col_name_lower {
                        if col.collation.is_empty() || col.collation == "BINARY" {
                            return "BINARY".to_string();
                        }
                        return col.collation.to_uppercase();
                    }
                }
            }
        }

        // If self.tables is empty or didn't find the column, try the global schema
        if let Some(schema) = self.schema {
            // If a specific table name is given, look it up directly
            if let Some(tbl_name) = table_name {
                let tbl_lower = tbl_name.to_lowercase();
                if let Some(table) = schema.tables.get(&tbl_lower) {
                    for col in &table.columns {
                        if col.name.to_lowercase() == col_name_lower {
                            if col.collation.is_empty() || col.collation == "BINARY" {
                                return "BINARY".to_string();
                            }
                            return col.collation.to_uppercase();
                        }
                    }
                }
            } else {
                // No table name given - search all tables for the column
                for table in schema.tables.values() {
                    for col in &table.columns {
                        if col.name.to_lowercase() == col_name_lower {
                            if col.collation.is_empty() || col.collation == "BINARY" {
                                // Keep searching - might find another table with collation
                                continue;
                            }
                            return col.collation.to_uppercase();
                        }
                    }
                }
            }
        }

        // Column not found or no explicit collation - use BINARY
        "BINARY".to_string()
    }

    /// Extract constraints from WHERE clause applicable to a virtual table
    fn extract_vtab_constraints(
        &self,
        expr: &Expr,
        table: &TableInfo,
        schema_table: &crate::schema::Table,
        constraints: &mut Vec<VtabIndexConstraint>,
        exprs: &mut Vec<Expr>,
    ) {
        match expr {
            Expr::Binary { op, left, right } => {
                // Check for AND - recurse into both sides
                if *op == BinaryOp::And {
                    self.extract_vtab_constraints(left, table, schema_table, constraints, exprs);
                    self.extract_vtab_constraints(right, table, schema_table, constraints, exprs);
                    return;
                }

                // Check if this is a comparison involving a column from this table
                // Skip column-to-column comparisons within the same table - these can't be
                // pushed down to the vtab and must be evaluated post-filter by the VDBE
                let (col_idx, value_expr, constraint_op) = if let Some((_col, idx)) =
                    self.get_vtab_column(left, table, schema_table)
                {
                    // Left side is a vtab column - check right side is NOT a vtab column
                    if self.get_vtab_column(right, table, schema_table).is_some() {
                        // Both sides are columns from this table - can't push down
                        return;
                    }
                    let op = self.binary_op_to_constraint_op(op);
                    if let Some(op) = op {
                        (idx, right.as_ref(), op)
                    } else {
                        return;
                    }
                } else if let Some((_col, idx)) = self.get_vtab_column(right, table, schema_table) {
                    // Right side is a vtab column - check left side is NOT a vtab column
                    if self.get_vtab_column(left, table, schema_table).is_some() {
                        // Both sides are columns from this table - can't push down
                        return;
                    }
                    // Reverse the operator for col on right side
                    let op = self.binary_op_to_constraint_op_reversed(op);
                    if let Some(op) = op {
                        (idx, left.as_ref(), op)
                    } else {
                        return;
                    }
                } else {
                    return;
                };

                constraints.push(VtabIndexConstraint::new(col_idx, constraint_op, true));
                exprs.push(value_expr.clone());
            }
            Expr::Like {
                expr: left,
                pattern,
                op: LikeOp::Match,
                ..
            } => {
                // MATCH constraint - used by FTS
                if let Some((_, col_idx)) = self.get_vtab_column(left, table, schema_table) {
                    constraints.push(VtabIndexConstraint::new(
                        col_idx,
                        SQLITE_INDEX_CONSTRAINT_MATCH,
                        true,
                    ));
                    exprs.push(*pattern.clone());
                }
            }
            Expr::Like {
                expr: left,
                pattern,
                op: LikeOp::Like,
                ..
            } => {
                // LIKE constraint
                if let Some((_, col_idx)) = self.get_vtab_column(left, table, schema_table) {
                    constraints.push(VtabIndexConstraint::new(
                        col_idx,
                        SQLITE_INDEX_CONSTRAINT_LIKE,
                        true,
                    ));
                    exprs.push(*pattern.clone());
                }
            }
            _ => {}
        }
    }

    /// Get column index if expr references a column from the given virtual table
    fn get_vtab_column(
        &self,
        expr: &Expr,
        table: &TableInfo,
        schema_table: &crate::schema::Table,
    ) -> Option<(String, i32)> {
        let Expr::Column(col) = expr else {
            return None;
        };

        // Check table qualifier if present
        if let Some(ref tbl) = col.table {
            if !tbl.eq_ignore_ascii_case(&table.name)
                && !tbl.eq_ignore_ascii_case(&table.table_name)
            {
                return None;
            }
        }

        // Find column index
        let col_lower = col.column.to_lowercase();

        // Check for rowid/id aliases
        if col_lower == "rowid" || col_lower == "_rowid_" || col_lower == "oid" {
            return Some((col.column.clone(), -1)); // -1 is rowid
        }

        // Look up in schema table columns
        for (i, schema_col) in schema_table.columns.iter().enumerate() {
            if schema_col.name.eq_ignore_ascii_case(&col.column) {
                return Some((col.column.clone(), i as i32));
            }
        }

        None
    }

    /// Convert BinaryOp to vtab constraint operator
    fn binary_op_to_constraint_op(&self, op: &BinaryOp) -> Option<u8> {
        match op {
            BinaryOp::Eq => Some(SQLITE_INDEX_CONSTRAINT_EQ),
            BinaryOp::Gt => Some(SQLITE_INDEX_CONSTRAINT_GT),
            BinaryOp::Ge => Some(SQLITE_INDEX_CONSTRAINT_GE),
            BinaryOp::Lt => Some(SQLITE_INDEX_CONSTRAINT_LT),
            BinaryOp::Le => Some(SQLITE_INDEX_CONSTRAINT_LE),
            _ => None,
        }
    }

    /// Convert BinaryOp to vtab constraint operator (reversed for column on right)
    fn binary_op_to_constraint_op_reversed(&self, op: &BinaryOp) -> Option<u8> {
        match op {
            BinaryOp::Eq => Some(SQLITE_INDEX_CONSTRAINT_EQ),
            BinaryOp::Gt => Some(SQLITE_INDEX_CONSTRAINT_LT), // a > col means col < a
            BinaryOp::Ge => Some(SQLITE_INDEX_CONSTRAINT_LE),
            BinaryOp::Lt => Some(SQLITE_INDEX_CONSTRAINT_GT),
            BinaryOp::Le => Some(SQLITE_INDEX_CONSTRAINT_GE),
            _ => None,
        }
    }

    /// Compile an expression into a register
    fn compile_expr(&mut self, expr: &Expr, dest_reg: i32) -> Result<()> {
        match expr {
            Expr::Literal(lit) => {
                match lit {
                    crate::parser::ast::Literal::Null => {
                        self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                    }
                    crate::parser::ast::Literal::Integer(n) => {
                        // Use Int64 for values that don't fit in i32
                        if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                            self.emit(Opcode::Integer, *n as i32, dest_reg, 0, P4::Unused);
                        } else {
                            self.emit(Opcode::Int64, 0, dest_reg, 0, P4::Int64(*n));
                        }
                    }
                    crate::parser::ast::Literal::Float(f) => {
                        self.emit(Opcode::Real, 0, dest_reg, 0, P4::Real(*f));
                    }
                    crate::parser::ast::Literal::String(s) => {
                        self.emit(Opcode::String8, 0, dest_reg, 0, P4::Text(s.clone()));
                    }
                    crate::parser::ast::Literal::Blob(b) => {
                        self.emit(
                            Opcode::Blob,
                            b.len() as i32,
                            dest_reg,
                            0,
                            P4::Blob(b.clone()),
                        );
                    }
                    crate::parser::ast::Literal::CurrentTime
                    | crate::parser::ast::Literal::CurrentDate
                    | crate::parser::ast::Literal::CurrentTimestamp => {
                        // These would call built-in functions
                        self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                    }
                    crate::parser::ast::Literal::Bool(b) => {
                        self.emit(
                            Opcode::Integer,
                            if *b { 1 } else { 0 },
                            dest_reg,
                            0,
                            P4::Unused,
                        );
                    }
                }
            }
            Expr::Column(col_ref) => {
                // Validate database qualifier if present
                // SQLite only allows: main, temp, or attached database aliases
                // RustQL currently only supports 'main' database
                if let Some(ref db_name) = col_ref.database {
                    if !db_name.eq_ignore_ascii_case("main") {
                        // Format the full qualified name for the error message
                        let full_name = if let Some(ref tbl) = col_ref.table {
                            format!("{}.{}.{}", db_name, tbl, col_ref.column)
                        } else {
                            format!("{}.{}", db_name, col_ref.column)
                        };
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("no such column: {}", full_name),
                        ));
                    }
                }

                // Check if this is a result column alias (for ORDER BY expressions)
                if col_ref.table.is_none() {
                    let alias_lower = col_ref.column.to_lowercase();
                    // First check result_aliases (post-result-column compilation)
                    if let Some(&alias_reg) = self.result_aliases.get(&alias_lower) {
                        self.emit(Opcode::SCopy, alias_reg, dest_reg, 0, P4::Unused);
                        return Ok(());
                    }
                    // Then check alias_expressions (for WHERE clause before result columns)
                    // Avoid infinite recursion when alias name matches a column name
                    // or when we're already resolving this alias
                    if !self.resolving_aliases.contains(&alias_lower) {
                        if let Some(alias_expr) = self.alias_expressions.get(&alias_lower).cloned()
                        {
                            // Don't recurse if the alias expression is just the same column reference
                            let is_same_column = matches!(&alias_expr, Expr::Column(c)
                                if c.table.is_none() && c.column.eq_ignore_ascii_case(&col_ref.column));
                            if !is_same_column {
                                // Mark this alias as being resolved to prevent infinite recursion
                                self.resolving_aliases.insert(alias_lower.clone());
                                let result = self.compile_expr(&alias_expr, dest_reg);
                                self.resolving_aliases.remove(&alias_lower);
                                return result;
                            }
                        }
                    }
                }

                // Check if this column is a GROUP BY column during finalization
                // This handles expressions like log*2+1 where log is the GROUP BY key
                // Only use group_column_regs for unqualified column references,
                // because group_column_regs only stores unqualified names and can't
                // distinguish between e.g. a.x and b.x in self-joins
                if col_ref.table.is_none() {
                    let col_name_lower = col_ref.column.to_lowercase();
                    if let Some(&group_reg) = self.group_column_regs.get(&col_name_lower) {
                        self.emit(Opcode::SCopy, group_reg, dest_reg, 0, P4::Unused);
                        return Ok(());
                    }
                }

                // Find the table and column index
                let (cursor, col_idx) = if let Some(table) = &col_ref.table {
                    // Check for synthetic table identifier used for self-join disambiguation
                    // Format: __tbl_idx_N__ where N is the table index
                    if let Some(idx_str) = table
                        .strip_prefix("__tbl_idx_")
                        .and_then(|s| s.strip_suffix("__"))
                    {
                        if let Ok(table_idx) = idx_str.parse::<usize>() {
                            if table_idx < self.tables.len() {
                                let tinfo = &self.tables[table_idx];
                                let col_idx = self
                                    .column_index_in_table(tinfo, &col_ref.column)
                                    .ok_or_else(|| {
                                        Error::with_message(
                                            ErrorCode::Error,
                                            format!(
                                                "no such column: {}.{}",
                                                tinfo.name, col_ref.column
                                            ),
                                        )
                                    })?;
                                (tinfo.cursor, col_idx)
                            } else {
                                return Err(Error::with_message(
                                    ErrorCode::Error,
                                    format!("invalid table index: {}", table_idx),
                                ));
                            }
                        } else {
                            return Err(Error::with_message(
                                ErrorCode::Error,
                                format!("invalid table identifier: {}", table),
                            ));
                        }
                    } else {
                        // Normal table name resolution with scoping (local first, then outer)
                        let mut local_matches = Vec::new();
                        for (idx, tinfo) in self
                            .tables
                            .iter()
                            .enumerate()
                            .skip(self.outer_tables_boundary)
                        {
                            if Self::table_name_matches(tinfo, table) {
                                local_matches.push(idx);
                            }
                        }

                        let mut matching_tables = if local_matches.is_empty() {
                            let mut outer_matches = Vec::new();
                            for (idx, tinfo) in self
                                .tables
                                .iter()
                                .enumerate()
                                .take(self.outer_tables_boundary)
                            {
                                if Self::table_name_matches(tinfo, table) {
                                    outer_matches.push(idx);
                                }
                            }
                            outer_matches
                        } else {
                            local_matches
                        };

                        if matching_tables.len() > 1 {
                            // Check if the column is a USING/NATURAL join column (coalesced)
                            // Coalesced columns are not ambiguous because they have the same value
                            // across all joined tables
                            let col_lower = col_ref.column.to_lowercase();
                            let is_coalesced = matching_tables.iter().any(|&idx| {
                                self.coalesced_columns
                                    .get(&idx)
                                    .is_some_and(|cols| cols.contains(&col_lower))
                            });
                            if !is_coalesced {
                                return Err(Error::with_message(
                                    ErrorCode::Error,
                                    format!("ambiguous column name: {}.{}", table, col_ref.column),
                                ));
                            }
                            // For coalesced columns, just use the first matching table
                        }

                        if let Some(table_idx) = matching_tables.pop() {
                            let tinfo = &self.tables[table_idx];
                            let idx = col_ref
                                .column_index
                                .or_else(|| self.column_index_in_table(tinfo, &col_ref.column))
                                .ok_or_else(|| {
                                    Error::with_message(
                                        ErrorCode::Error,
                                        format!("no such column: {}.{}", table, col_ref.column),
                                    )
                                })?;
                            (tinfo.cursor, idx)
                        } else if self.schema.is_none() {
                            let cursor = self.tables.first().map(|t| t.cursor).unwrap_or(0);
                            (cursor, col_ref.column_index.unwrap_or(0))
                        } else {
                            return Err(Error::with_message(
                                ErrorCode::Error,
                                format!("no such column: {}.{}", table, col_ref.column),
                            ));
                        }
                    }
                } else {
                    // No table specified - search local tables first, then outer
                    let col_lower = col_ref.column.to_lowercase();
                    let mut matches = Vec::new();
                    for (table_idx, tinfo) in self
                        .tables
                        .iter()
                        .enumerate()
                        .skip(self.outer_tables_boundary)
                    {
                        if self.is_column_coalesced(table_idx, &col_lower) {
                            continue;
                        }
                        if let Some(idx) = self.column_index_in_table(tinfo, &col_ref.column) {
                            matches.push((tinfo.cursor, idx));
                        }
                    }

                    if matches.len() > 1 {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("ambiguous column name: {}", col_ref.column),
                        ));
                    }

                    if matches.is_empty() {
                        // Search outer tables in REVERSE order (closest scope first)
                        // to match SQLite's behavior of preferring the nearest enclosing scope
                        for table_idx in (0..self.outer_tables_boundary).rev() {
                            let tinfo = &self.tables[table_idx];
                            if self.is_column_coalesced(table_idx, &col_lower) {
                                continue;
                            }
                            if let Some(idx) = self.column_index_in_table(tinfo, &col_ref.column) {
                                matches.push((tinfo.cursor, idx));
                                // Stop at first match - prefer closest scope
                                break;
                            }
                        }
                        // No ambiguity check needed since we stop at first match
                    }

                    if let Some((cursor, idx)) = matches.pop() {
                        (cursor, idx)
                    } else if self.schema.is_none() {
                        let cursor = self.tables.first().map(|t| t.cursor).unwrap_or(0);
                        (cursor, col_ref.column_index.unwrap_or(0))
                    } else {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("no such column: {}", col_ref.column),
                        ));
                    }
                };

                // Check if we have a saved value for this column (from simple aggregate loop)
                if let Some(ref saved_regs) = self.saved_column_regs {
                    if let Some(&saved_reg) = saved_regs.get(&(cursor, col_idx)) {
                        // Use the saved value instead of reading from cursor
                        self.emit(Opcode::SCopy, saved_reg, dest_reg, 0, P4::Unused);
                        return Ok(());
                    }
                }

                if col_idx == -1 {
                    // Rowid alias
                    self.emit(Opcode::Rowid, cursor, dest_reg, 0, P4::Unused);
                } else if col_idx == -2 {
                    // FTS pseudo-column (table name used as column in snippet(), etc.)
                    // Return NULL - the actual value is not used by FTS functions
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                } else if col_idx < 0 {
                    // Other negative values - treat as rowid for compatibility
                    self.emit(Opcode::Rowid, cursor, dest_reg, 0, P4::Unused);
                } else {
                    // Check if this column is a VIRTUAL generated column
                    // If so, we need to compile the expression instead of reading from storage
                    let table_info = self.tables.iter().find(|t| t.cursor == cursor);
                    let generated_expr = table_info
                        .and_then(|ti| ti.schema_table.as_ref())
                        .and_then(|schema_table| schema_table.columns.get(col_idx as usize))
                        .and_then(|col| col.generated.as_ref())
                        .filter(|gen| gen.storage == GeneratedStorage::Virtual)
                        .map(|gen| Self::convert_schema_expr_to_ast(&gen.expr));

                    // Check if this is a virtual table
                    let is_virtual_table = table_info
                        .and_then(|ti| ti.schema_table.as_ref())
                        .map(|st| st.is_virtual)
                        .unwrap_or(false);

                    if let Some(gen_expr) = generated_expr {
                        // Compile the generated column expression instead of Column opcode
                        self.compile_expr(&gen_expr, dest_reg)?;
                    } else if is_virtual_table {
                        // Use VColumn for virtual tables
                        self.emit(
                            Opcode::VColumn,
                            cursor,
                            col_idx,
                            dest_reg,
                            P4::Text(col_ref.column.clone()),
                        );
                    } else {
                        self.emit(
                            Opcode::Column,
                            cursor,
                            col_idx,
                            dest_reg,
                            P4::Text(col_ref.column.clone()),
                        );
                    }
                }
            }
            Expr::Binary { op, left, right } => {
                let left_reg = self.alloc_reg();
                let right_reg = self.alloc_reg();
                self.compile_expr(left, left_reg)?;
                self.compile_expr(right, right_reg)?;

                // Check if this is a comparison operation (jump-based opcodes)
                let is_comparison = matches!(
                    op,
                    BinaryOp::Eq
                        | BinaryOp::Ne
                        | BinaryOp::Lt
                        | BinaryOp::Le
                        | BinaryOp::Gt
                        | BinaryOp::Ge
                );

                // Check if this is IS/IS NOT (same as comparison but with NULLEQ flag)
                let is_is_comparison = matches!(op, BinaryOp::Is | BinaryOp::IsNot);

                if is_comparison || is_is_comparison {
                    // Determine affinity for comparison based on operand types
                    // If either operand is a column with numeric affinity, use NUMERIC (2)
                    // Otherwise use BLOB (0) for type ordering
                    let cmp_affinity = self.get_comparison_affinity(left, right);

                    // Comparison opcodes are jump-based: Eq P1 P2 P3 means
                    // "if r[P1] == r[P3], jump to P2"
                    // We need to produce a 0/1 boolean result in dest_reg
                    // For regular comparisons: if either operand is NULL, result is NULL
                    // For IS/IS NOT: NULL IS NULL = 1, NULL IS NOT NULL = 0 (uses NULLEQ flag)
                    let cmp_opcode = match op {
                        BinaryOp::Eq | BinaryOp::Is => Opcode::Eq,
                        BinaryOp::Ne | BinaryOp::IsNot => Opcode::Ne,
                        BinaryOp::Lt => Opcode::Lt,
                        BinaryOp::Le => Opcode::Le,
                        BinaryOp::Gt => Opcode::Gt,
                        BinaryOp::Ge => Opcode::Ge,
                        _ => unreachable!(),
                    };

                    // Allocate labels for control flow
                    let true_label = self.alloc_label();
                    let end_label = self.alloc_label();

                    if is_is_comparison {
                        // IS/IS NOT: use NULLEQ flag (0x80) so NULL IS NULL returns true
                        // No NULL check needed - the comparison handles it
                        self.emit(Opcode::Integer, 0, dest_reg, 0, P4::Unused);
                        self.emit_with_p5(
                            cmp_opcode,
                            right_reg,
                            true_label,
                            left_reg,
                            P4::Unused,
                            0x80,
                        );
                        self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);
                        self.resolve_label(true_label, self.current_addr());
                        self.emit(Opcode::Integer, 1, dest_reg, 0, P4::Unused);
                        self.resolve_label(end_label, self.current_addr());
                    } else {
                        // Regular comparison: NULL check first
                        let null_label = self.alloc_label();

                        // Check if left operand is NULL - if so, result is NULL
                        self.emit(Opcode::IsNull, left_reg, null_label, 0, P4::Unused);
                        // Check if right operand is NULL - if so, result is NULL
                        self.emit(Opcode::IsNull, right_reg, null_label, 0, P4::Unused);

                        // Neither is NULL - set result to 0 (false) initially
                        self.emit(Opcode::Integer, 0, dest_reg, 0, P4::Unused);

                        // Compare: if condition is true, jump to true_label
                        // Comparison opcode format: P1=right operand, P2=jump target, P3=left operand
                        // Lt P1 P2 P3 means "jump to P2 if r[P3] < r[P1]"
                        // P5 contains affinity for type coercion
                        self.emit_with_p5(
                            cmp_opcode,
                            right_reg,
                            true_label,
                            left_reg,
                            P4::Unused,
                            cmp_affinity,
                        );

                        // Fall through means false - goto end
                        self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);

                        // True path: set result to 1
                        self.resolve_label(true_label, self.current_addr());
                        self.emit(Opcode::Integer, 1, dest_reg, 0, P4::Unused);
                        self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);

                        // Null path: set result to NULL
                        self.resolve_label(null_label, self.current_addr());
                        self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);

                        // End label
                        self.resolve_label(end_label, self.current_addr());
                    }
                } else if matches!(op, BinaryOp::JsonExtract | BinaryOp::JsonExtractText) {
                    // JSON extraction operators -> and ->>
                    // These are converted to json_extract function calls
                    // -> returns JSON, ->> returns text
                    let func_name = if *op == BinaryOp::JsonExtract {
                        "json_extract"
                    } else {
                        "json_extract" // ->> is json_extract with implicit text conversion
                    };
                    self.emit(
                        Opcode::Function,
                        2,
                        left_reg,
                        dest_reg,
                        P4::Text(func_name.to_string()),
                    );
                    // For ->>, we need to convert to text (SQLite does this implicitly)
                    if *op == BinaryOp::JsonExtractText {
                        // The result is already text from json_extract in most cases
                        // but we add explicit cast for safety
                        self.emit(Opcode::Cast, dest_reg, Affinity::Text as i32, 0, P4::Unused);
                    }
                } else {
                    // Arithmetic and other value-producing operations
                    let opcode = match op {
                        BinaryOp::Add => Opcode::Add,
                        BinaryOp::Sub => Opcode::Subtract,
                        BinaryOp::Mul => Opcode::Multiply,
                        BinaryOp::Div => Opcode::Divide,
                        BinaryOp::Mod => Opcode::Remainder,
                        BinaryOp::And => Opcode::And,
                        BinaryOp::Or => Opcode::Or,
                        BinaryOp::BitAnd => Opcode::BitAnd,
                        BinaryOp::BitOr => Opcode::BitOr,
                        BinaryOp::ShiftLeft => Opcode::ShiftLeft,
                        BinaryOp::ShiftRight => Opcode::ShiftRight,
                        BinaryOp::Concat => Opcode::Concat,
                        _ => Opcode::Noop,
                    };

                    // Arithmetic opcodes: P1=right operand, P2=left operand, P3=dest
                    // Add/Sub/Mul/Div compute r[P2] op r[P1] and store in r[P3]
                    self.emit(opcode, right_reg, left_reg, dest_reg, P4::Unused);
                }
            }
            Expr::Unary { op, expr: inner } => {
                self.compile_expr(inner, dest_reg)?;
                match op {
                    crate::parser::ast::UnaryOp::Neg => {
                        // SQLite: 0 - value using Subtract
                        let zero_reg = self.alloc_reg();
                        self.emit(Opcode::Integer, 0, zero_reg, 0, P4::Unused);
                        self.emit(Opcode::Subtract, dest_reg, zero_reg, dest_reg, P4::Unused);
                    }
                    crate::parser::ast::UnaryOp::Pos => {
                        // No-op
                    }
                    crate::parser::ast::UnaryOp::Not => {
                        self.emit(Opcode::Not, dest_reg, dest_reg, 0, P4::Unused);
                    }
                    crate::parser::ast::UnaryOp::BitNot => {
                        self.emit(Opcode::BitNot, dest_reg, dest_reg, 0, P4::Unused);
                    }
                }
            }
            Expr::Function(func_call) => {
                // Check if this is an aggregate function with pre-computed results
                let name_upper = func_call.name.to_uppercase();
                let arg_count = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                    crate::parser::ast::FunctionArgs::Star => 0,
                };

                // Handle likelihood/likely/unlikely - these are optimizer hints that
                // are completely eliminated during compilation. They just return their
                // first argument unchanged and don't appear in the VDBE bytecode at all.
                let is_likelihood_func =
                    matches!(name_upper.as_str(), "LIKELIHOOD" | "LIKELY" | "UNLIKELY");
                if is_likelihood_func {
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        // likelihood(X, Y) - validate Y is a constant between 0.0 and 1.0
                        if name_upper == "LIKELIHOOD" {
                            if exprs.len() != 2 {
                                return Err(Error::with_message(
                                    ErrorCode::Error,
                                    "wrong number of arguments to function likelihood()",
                                ));
                            }
                            // Validate second argument is a numeric constant in [0.0, 1.0]
                            let prob_expr = &exprs[1];
                            let prob_value = match prob_expr {
                                Expr::Literal(crate::parser::ast::Literal::Float(f)) => Some(*f),
                                Expr::Literal(crate::parser::ast::Literal::Integer(i)) => {
                                    Some(*i as f64)
                                }
                                _ => None, // Not a constant
                            };
                            match prob_value {
                                Some(p) if p >= 0.0 && p <= 1.0 => {
                                    // Valid - continue to compile first argument
                                }
                                Some(_) => {
                                    return Err(Error::with_message(
                                        ErrorCode::Error,
                                        "second argument to likelihood() must be a constant between 0.0 and 1.0",
                                    ));
                                }
                                None => {
                                    return Err(Error::with_message(
                                        ErrorCode::Error,
                                        "second argument to likelihood() must be a constant between 0.0 and 1.0",
                                    ));
                                }
                            }
                        }
                        // For all hint functions, just compile the first argument directly
                        // into the destination register, eliminating the function call
                        if !exprs.is_empty() {
                            self.compile_expr(&exprs[0], dest_reg)?;
                            return Ok(());
                        }
                    }
                    // Fallback to null if no arguments
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                    return Ok(());
                }

                // MIN/MAX with multiple args are scalar functions
                let is_multi_arg_min_max =
                    matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count > 1;
                let is_aggregate = !is_multi_arg_min_max
                    && crate::functions::is_aggregate_function(&func_call.name);

                // Validate aggregate function argument counts
                if is_aggregate {
                    if let Some(agg_info) =
                        crate::functions::get_aggregate_function(&func_call.name)
                    {
                        if arg_count < agg_info.min_args || arg_count > agg_info.max_args {
                            return Err(Error::with_message(
                                ErrorCode::Error,
                                format!(
                                    "wrong number of arguments to function {}()",
                                    func_call.name
                                ),
                            ));
                        }
                    }
                }

                if is_aggregate && self.agg_final_idx < self.agg_final_regs.len() {
                    // Use pre-computed aggregate result
                    let agg_reg = self.agg_final_regs[self.agg_final_idx];
                    self.agg_final_idx += 1;
                    self.emit(Opcode::Copy, agg_reg, dest_reg, 0, P4::Unused);
                } else {
                    // Check if function exists before compiling
                    // Note: Some functions like LAST_INSERT_ROWID need special VDBE handling
                    let is_connection_function =
                        func_call.name.eq_ignore_ascii_case("LAST_INSERT_ROWID")
                            || func_call.name.eq_ignore_ascii_case("CHANGES")
                            || func_call.name.eq_ignore_ascii_case("TOTAL_CHANGES");
                    #[cfg(feature = "tcl")]
                    let is_tcl_function = crate::tcl_ext::has_tcl_user_function(&func_call.name);
                    #[cfg(not(feature = "tcl"))]
                    let is_tcl_function = false;
                    let is_known_function = is_aggregate
                        || is_connection_function
                        || crate::functions::get_scalar_function(&func_call.name).is_some()
                        || is_tcl_function;
                    if !is_known_function {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("no such function: {}", func_call.name),
                        ));
                    }

                    // Compile as scalar function
                    // Pre-allocate contiguous registers for all arguments first,
                    // so nested function calls don't break the contiguity
                    let argc = match &func_call.args {
                        crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                        crate::parser::ast::FunctionArgs::Star => 0,
                    };
                    let arg_base = self.next_reg;
                    let arg_regs: Vec<i32> = (0..argc).map(|_| self.alloc_reg()).collect();

                    // Now compile each argument into its pre-allocated register
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        for (arg, &reg) in exprs.iter().zip(arg_regs.iter()) {
                            self.compile_expr(arg, reg)?;
                        }
                    }

                    self.emit(
                        Opcode::Function,
                        argc as i32,
                        arg_base,
                        dest_reg,
                        P4::Text(func_call.name.clone()),
                    );
                }
            }
            Expr::IsNull {
                expr: inner,
                negated,
            } => {
                // IsNull/NotNull are jump opcodes, so we need a jump pattern to produce boolean
                let true_label = self.alloc_label();
                let end_label = self.alloc_label();

                self.compile_expr(inner, dest_reg)?;

                // Jump to true_label if the condition matches
                if *negated {
                    // IS NOT NULL: jump if not null
                    self.emit(Opcode::NotNull, dest_reg, true_label, 0, P4::Unused);
                } else {
                    // IS NULL: jump if null
                    self.emit(Opcode::IsNull, dest_reg, true_label, 0, P4::Unused);
                }

                // Condition not matched - set to 0 and jump to end
                self.emit(Opcode::Integer, 0, dest_reg, 0, P4::Unused);
                self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);

                // Condition matched - set to 1
                self.resolve_label(true_label, self.current_addr());
                self.emit(Opcode::Integer, 1, dest_reg, 0, P4::Unused);

                self.resolve_label(end_label, self.current_addr());
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                let end_label = self.alloc_label();

                if let Some(op_expr) = operand {
                    let op_reg = self.alloc_reg();
                    self.compile_expr(op_expr, op_reg)?;

                    for clause in when_clauses {
                        let next_when_label = self.alloc_label();
                        let when_reg = self.alloc_reg();
                        self.compile_expr(&clause.when, when_reg)?;
                        self.emit(Opcode::Ne, op_reg, next_when_label, when_reg, P4::Unused);
                        // Set JUMPIFNULL flag so NULL comparisons jump to next WHEN clause
                        // (NULL compared to anything is unknown, so WHEN should not match)
                        if let Some(op) = self.ops.last_mut() {
                            op.p5 = crate::vdbe::ops::cmp_flags::JUMPIFNULL as u16;
                        }
                        self.compile_expr(&clause.then, dest_reg)?;
                        self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);
                        self.resolve_label(next_when_label, self.current_addr());
                    }
                } else {
                    for clause in when_clauses {
                        let next_when_label = self.alloc_label();
                        let when_reg = self.alloc_reg();
                        self.compile_expr(&clause.when, when_reg)?;
                        self.emit(Opcode::IfNot, when_reg, next_when_label, 1, P4::Unused);
                        self.compile_expr(&clause.then, dest_reg)?;
                        self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);
                        self.resolve_label(next_when_label, self.current_addr());
                    }
                }

                if let Some(else_expr) = else_clause {
                    self.compile_expr(else_expr, dest_reg)?;
                } else {
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                }

                self.resolve_label(end_label, self.current_addr());
            }
            Expr::Subquery(select) => {
                // Compile scalar subquery inline
                // Keep outer tables for correlation - save count to restore later
                let outer_tables_len = self.tables.len();
                let saved_boundary = self.outer_tables_boundary;
                let saved_has_agg = self.has_aggregates;
                let saved_has_window = self.has_window_functions;
                let saved_result_names_len = self.result_column_names.len();
                let saved_order_by_terms = self.order_by_terms.take();
                let saved_limit_counter_reg = self.limit_counter_reg.take();
                let saved_offset_counter_reg = self.offset_counter_reg.take();
                let saved_limit_done_label = self.limit_done_label.take();

                // Set boundary so subquery only loops over its own tables, not outer tables
                self.outer_tables_boundary = outer_tables_len;

                // Initialize result to NULL in case subquery returns no rows
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);

                // For scalar subqueries, we only want the FIRST row's value.
                // If the subquery doesn't have a LIMIT, we apply LIMIT 1 internally.
                // This matches SQLite behavior where (SELECT ...) returns only the first row.
                let needs_limit_1 = select.limit.is_none();
                if needs_limit_1 {
                    // Set up LIMIT 1 for this subquery
                    let limit_reg = self.alloc_reg();
                    self.emit(Opcode::Integer, 1, limit_reg, 0, P4::Unused);
                    self.limit_counter_reg = Some(limit_reg);
                    self.limit_done_label = Some(self.alloc_label());
                }

                // Compile the subquery with Set destination using compile_subselect
                // which handles ORDER BY and LIMIT properly
                // Outer tables remain available for correlated column references
                let sub_dest = SelectDest::Set { reg: dest_reg };
                self.compile_subselect(select, &sub_dest)?;

                // Resolve the limit done label if we added LIMIT 1
                if needs_limit_1 {
                    if let Some(done_label) = self.limit_done_label {
                        self.resolve_label(done_label, self.current_addr());
                    }
                }

                // Restore outer query state - remove subquery's tables and reset boundary
                self.tables.truncate(outer_tables_len);
                self.outer_tables_boundary = saved_boundary;
                self.has_aggregates = saved_has_agg;
                self.has_window_functions = saved_has_window;
                self.result_column_names.truncate(saved_result_names_len);
                self.order_by_terms = saved_order_by_terms;
                self.limit_counter_reg = saved_limit_counter_reg;
                self.offset_counter_reg = saved_offset_counter_reg;
                self.limit_done_label = saved_limit_done_label;
            }
            Expr::Exists { subquery, negated } => {
                // Compile EXISTS subquery
                // Keep outer tables for correlation - save count to restore later
                let outer_tables_len = self.tables.len();
                let saved_boundary = self.outer_tables_boundary;
                let saved_has_agg = self.has_aggregates;
                let saved_has_window = self.has_window_functions;
                let saved_result_names_len = self.result_column_names.len();

                // Set boundary so subquery only loops over its own tables, not outer tables
                self.outer_tables_boundary = outer_tables_len;

                // Initialize result to 0 (false) - will be set to 1 if any row is found
                self.emit(
                    Opcode::Integer,
                    if *negated { 1 } else { 0 },
                    dest_reg,
                    0,
                    P4::Unused,
                );

                // Compile the subquery body with Exists destination
                // Outer tables remain available for correlated column references
                let sub_dest = SelectDest::Exists { reg: dest_reg };
                self.compile_body(&subquery.body, &sub_dest)?;

                // If negated (NOT EXISTS), we need to invert the result
                // Exists destination sets reg to 1 if a row is found
                // For NOT EXISTS, we want 1 when no rows, 0 when rows found
                if *negated {
                    // Result was initialized to 1, Exists sets it to 1 on match
                    // We need to invert: if a row was found (reg==1 from Exists), set to 0
                    // This is handled by initializing to 1 (no rows case) and
                    // letting Exists set it to... wait, Exists sets it to 1 regardless
                    // Actually we need different logic for NOT EXISTS
                    // For now, let's use the simpler approach: Exists always sets 1 on match,
                    // so for NOT EXISTS we need to flip after
                    self.emit(Opcode::Not, dest_reg, dest_reg, 0, P4::Unused);
                }

                // Restore outer query state - remove subquery's tables and reset boundary
                self.tables.truncate(outer_tables_len);
                self.outer_tables_boundary = saved_boundary;
                self.has_aggregates = saved_has_agg;
                self.has_window_functions = saved_has_window;
                self.result_column_names.truncate(saved_result_names_len);
            }
            Expr::Like {
                expr: text_expr,
                pattern,
                op,
                negated,
                escape,
            } => {
                // Compile LIKE/GLOB expression using Function opcode (SQLite style)
                // Args must be in consecutive registers: [pattern, text, optional_escape]
                //
                // IMPORTANT: We must compile all expressions first, THEN allocate
                // consecutive registers for the function call. This is because
                // compile_expr for complex expressions (like Concat) uses intermediate
                // registers, which would break the consecutive layout.

                // 1. Compile pattern to a temporary register
                let temp_pattern = self.alloc_reg();
                self.compile_expr(pattern, temp_pattern)?;

                // 2. Compile text expression to a temporary register
                let temp_text = self.alloc_reg();
                self.compile_expr(text_expr, temp_text)?;

                // 3. Compile escape expression if present
                let temp_escape = if escape.is_some() {
                    let temp = self.alloc_reg();
                    self.compile_expr(escape.as_ref().unwrap(), temp)?;
                    Some(temp)
                } else {
                    None
                };

                // 4. Now allocate consecutive registers for the function call
                let argc = if temp_escape.is_some() { 3 } else { 2 };
                let args_base = self.alloc_reg();
                for _ in 1..argc {
                    self.alloc_reg();
                }

                // 5. Copy values to consecutive registers
                self.emit(Opcode::Copy, temp_pattern, args_base, 0, P4::Unused);
                self.emit(Opcode::Copy, temp_text, args_base + 1, 0, P4::Unused);
                if let Some(te) = temp_escape {
                    self.emit(Opcode::Copy, te, args_base + 2, 0, P4::Unused);
                }

                let func_name = match op {
                    crate::parser::ast::LikeOp::Like => "like",
                    crate::parser::ast::LikeOp::Glob => "glob",
                    crate::parser::ast::LikeOp::Regexp => "regexp",
                    crate::parser::ast::LikeOp::Match => "match",
                };

                // Function opcode: P1=argc, P2=arg_base, P3=dest, P4=func_name
                self.emit(
                    Opcode::Function,
                    argc,
                    args_base,
                    dest_reg,
                    P4::FuncDef(func_name.to_string()),
                );

                if *negated {
                    // Negate the result
                    self.emit(Opcode::Not, dest_reg, dest_reg, 0, P4::Unused);
                }
            }
            Expr::In {
                expr: val_expr,
                list,
                negated,
            } => {
                // Compile IN expression
                // SQLite NULL handling:
                // - If LHS is NULL → result is NULL
                // - If LHS matches any RHS value → TRUE (1)
                // - If LHS doesn't match and any RHS is NULL → NULL
                // - Otherwise → FALSE (0)
                let val_reg = self.alloc_reg();
                self.compile_expr(val_expr, val_reg)?;

                match list {
                    crate::parser::ast::InList::Values(values) => {
                        if values.is_empty() {
                            // Empty list - always false
                            self.emit(
                                Opcode::Integer,
                                if *negated { 1 } else { 0 },
                                dest_reg,
                                0,
                                P4::Unused,
                            );
                        } else {
                            let match_label = self.alloc_label();
                            let null_label = self.alloc_label();
                            let end_label = self.alloc_label();

                            // If LHS is NULL, jump to null result
                            self.emit(Opcode::IsNull, val_reg, null_label, 0, P4::Unused);

                            // Track if any RHS value is NULL (for proper NULL propagation)
                            // We use a register to track if we've seen a NULL in RHS
                            let saw_null_reg = self.alloc_reg();
                            self.emit(Opcode::Integer, 0, saw_null_reg, 0, P4::Unused);

                            for value in values {
                                let cmp_reg = self.alloc_reg();
                                self.compile_expr(value, cmp_reg)?;
                                // If equal, jump to match
                                self.emit(Opcode::Eq, val_reg, match_label, cmp_reg, P4::Unused);
                                // If RHS is NULL, mark that we saw a NULL
                                // (comparison with NULL doesn't jump, so we continue)
                                let skip_null_mark = self.alloc_label();
                                self.emit(Opcode::NotNull, cmp_reg, skip_null_mark, 0, P4::Unused);
                                self.emit(Opcode::Integer, 1, saw_null_reg, 0, P4::Unused);
                                self.resolve_label(skip_null_mark, self.current_addr());
                            }

                            // No match found - check if we saw any NULL in RHS
                            self.emit(Opcode::IfPos, saw_null_reg, null_label, 1, P4::Unused);

                            // No match and no NULLs - result is FALSE
                            self.emit(
                                Opcode::Integer,
                                if *negated { 1 } else { 0 },
                                dest_reg,
                                0,
                                P4::Unused,
                            );
                            self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);

                            // NULL result (LHS was NULL or no match but RHS had NULL)
                            self.resolve_label(null_label, self.current_addr());
                            self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                            self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);

                            // Match found
                            self.resolve_label(match_label, self.current_addr());
                            self.emit(
                                Opcode::Integer,
                                if *negated { 0 } else { 1 },
                                dest_reg,
                                0,
                                P4::Unused,
                            );

                            self.resolve_label(end_label, self.current_addr());
                        }
                    }
                    crate::parser::ast::InList::Subquery(subquery) => {
                        // Validate that subquery returns exactly 1 column
                        let subq_col_count = self.count_select_body_columns(&subquery.body);
                        if subq_col_count != 1 {
                            return Err(Error::with_message(
                                ErrorCode::Error,
                                format!(
                                    "sub-select returns {} columns - expected 1",
                                    subq_col_count
                                ),
                            ));
                        }

                        // Compile IN subquery using a fresh compilation context
                        // This ensures the subquery doesn't mutate our state
                        let subq_cursor = self.alloc_cursor();
                        self.emit(Opcode::OpenEphemeral, subq_cursor, 1, 0, P4::Unused);

                        // Create a fresh subcompiler for the subquery
                        let mut subcompiler = if let Some(schema) = self.schema {
                            SelectCompiler::with_schema(schema)
                        } else {
                            SelectCompiler::new()
                        };

                        // Propagate shared state
                        if let Some(temp_schema) = self.temp_schema {
                            subcompiler.set_temp_schema(temp_schema);
                        }
                        subcompiler.next_reg = self.next_reg;
                        subcompiler.next_cursor = self.next_cursor;
                        subcompiler.ctes = self.ctes.clone();
                        subcompiler.recursive_ctes = self.recursive_ctes.clone();
                        subcompiler.cte_cursors = self.cte_cursors.clone();
                        subcompiler.expanding_views = self.expanding_views.clone();
                        subcompiler.main_view_depth = self.main_view_depth;
                        subcompiler.enable_view = self.enable_view;
                        subcompiler
                            .set_column_name_flags(self.short_column_names, self.full_column_names);

                        // Copy outer tables for correlation - subquery can reference these
                        subcompiler.tables = self.tables.clone();
                        // Set boundary so subquery knows which tables are outer (for correlation)
                        // vs which it will add itself (for its own FROM clause)
                        subcompiler.outer_tables_boundary = self.tables.len();

                        // Compile subquery into ephemeral table
                        let subq_dest = SelectDest::EphemTable {
                            cursor: subq_cursor,
                        };
                        let subquery_ops = subcompiler.compile(subquery, &subq_dest)?;

                        // Update our register/cursor counters from subcompiler
                        self.next_reg = subcompiler.next_reg;
                        self.next_cursor = subcompiler.next_cursor;

                        // Inline the subquery ops (skip Init, Halt, Transaction, Goto wrapper)
                        let offset = self.ops.len() as i32;
                        let len = subquery_ops.len();
                        let mut skip_indices = std::collections::HashSet::new();

                        // Skip Init at 0
                        if !subquery_ops.is_empty() && subquery_ops[0].opcode == Opcode::Init {
                            skip_indices.insert(0);
                        }
                        // Skip footer: Halt, Transaction, Goto at end
                        if len >= 3 {
                            if subquery_ops[len - 1].opcode == Opcode::Goto {
                                skip_indices.insert(len - 1);
                            }
                            if subquery_ops[len - 2].opcode == Opcode::Transaction {
                                skip_indices.insert(len - 2);
                            }
                            if subquery_ops[len - 3].opcode == Opcode::Halt {
                                skip_indices.insert(len - 3);
                            }
                        }

                        // Build address mapping: old_addr -> new_addr
                        // Skipped instructions get mapped to the end of the inlined section
                        let mut addr_map: Vec<i32> = Vec::with_capacity(len);
                        let mut new_addr = offset;
                        for i in 0..len {
                            if skip_indices.contains(&i) {
                                // Skipped instruction - will map to end later
                                addr_map.push(-1);
                            } else {
                                addr_map.push(new_addr);
                                new_addr += 1;
                            }
                        }

                        // Calculate end address for the inlined section
                        let inlined_end = new_addr;

                        // Fix up -1 entries (skipped instructions) to point to end
                        for addr in &mut addr_map {
                            if *addr == -1 {
                                *addr = inlined_end;
                            }
                        }

                        for (old_addr, mut op) in subquery_ops.into_iter().enumerate() {
                            // Skip wrapper instructions only
                            if !skip_indices.contains(&old_addr) {
                                // Adjust P2 for jump instructions using the address map
                                if op.opcode.is_jump() {
                                    let target = op.p2 as usize;
                                    if target < addr_map.len() {
                                        op.p2 = addr_map[target];
                                    } else {
                                        // Jump beyond subquery - point to end
                                        op.p2 = inlined_end;
                                    }
                                    // Use P5 = 0xFFFF to mark as already resolved so resolve_labels skips it
                                    op.p5 = 0xFFFF;
                                }
                                self.ops.push(op);
                            }
                        }

                        // Check if value exists in ephemeral table
                        // Make a record from the value
                        let record_reg = self.alloc_reg();
                        self.emit(Opcode::MakeRecord, val_reg, 1, record_reg, P4::Unused);

                        let found_label = self.alloc_label();
                        let end_label = self.alloc_label();

                        // Found jumps if record exists in cursor
                        self.emit(
                            Opcode::Found,
                            subq_cursor,
                            found_label,
                            record_reg,
                            P4::Unused,
                        );

                        // Not found
                        self.emit(
                            Opcode::Integer,
                            if *negated { 1 } else { 0 },
                            dest_reg,
                            0,
                            P4::Unused,
                        );
                        self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);

                        // Found
                        self.resolve_label(found_label, self.current_addr());
                        self.emit(
                            Opcode::Integer,
                            if *negated { 0 } else { 1 },
                            dest_reg,
                            0,
                            P4::Unused,
                        );

                        self.resolve_label(end_label, self.current_addr());

                        // Close ephemeral table
                        self.emit(Opcode::Close, subq_cursor, 0, 0, P4::Unused);
                    }
                    crate::parser::ast::InList::Table(_) => {
                        // IN table - not yet implemented, return NULL
                        self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                    }
                }
            }
            Expr::Between {
                expr: val_expr,
                low,
                high,
                negated,
            } => {
                // Compile BETWEEN: val >= low AND val <= high
                // NULL semantics: if any operand is NULL during comparison, result is NULL
                // EXCEPT: if we can determine the value is definitely out of range, return FALSE
                //
                // Logic:
                // 1. If val > high (regardless of NULLs), result is FALSE
                // 2. If val < low (regardless of NULLs), result is FALSE
                // 3. If val is NULL, result is NULL
                // 4. If low is NULL or high is NULL (and we haven't jumped), result is NULL
                // 5. Otherwise, val is in range, result is TRUE

                // Extract collation from COLLATE wrapper or column definition
                let (inner_val_expr, collation) = match val_expr.as_ref() {
                    Expr::Collate {
                        expr: inner,
                        collation,
                    } => (inner.as_ref(), Some(collation.clone())),
                    _ => {
                        // If no explicit COLLATE, check if the expression is a column
                        // with a non-default collation in its schema definition
                        let col_collation = self.get_expr_collation(val_expr.as_ref());
                        (val_expr.as_ref(), col_collation)
                    }
                };

                let val_reg = self.alloc_reg();
                let low_reg = self.alloc_reg();
                let high_reg = self.alloc_reg();

                self.compile_expr(inner_val_expr, val_reg)?;
                self.compile_expr(low, low_reg)?;
                self.compile_expr(high, high_reg)?;

                let false_label = self.alloc_label();
                let null_label = self.alloc_label();
                let check_low_label = self.alloc_label();
                let check_nulls_label = self.alloc_label();
                let end_label = self.alloc_label();

                // Determine affinity for comparisons
                let low_affinity = self.get_comparison_affinity(inner_val_expr, low);
                let high_affinity = self.get_comparison_affinity(inner_val_expr, high);

                // First check if val is NULL - if so, result is NULL
                self.emit_with_p5(Opcode::IsNull, val_reg, null_label, 0, P4::Unused, 0);

                // Check val > high first (skip if high is NULL - can't determine yet)
                self.emit(Opcode::IsNull, high_reg, check_low_label, 0, P4::Unused);
                // Gt P1 P2 P3: jumps if r[P3] > r[P1], so P1=high, P3=val
                if let Some(ref coll) = collation {
                    self.emit_with_p5(
                        Opcode::Gt,
                        high_reg,
                        false_label,
                        val_reg,
                        P4::Collation(coll.clone()),
                        0, // Use collation instead of affinity
                    );
                } else {
                    self.emit_with_p5(
                        Opcode::Gt,
                        high_reg,
                        false_label,
                        val_reg,
                        P4::Unused,
                        high_affinity,
                    );
                }

                // Check val < low (skip if low is NULL - can't determine yet)
                self.resolve_label(check_low_label, self.current_addr());
                self.emit(Opcode::IsNull, low_reg, check_nulls_label, 0, P4::Unused);
                // Lt P1 P2 P3: jumps if r[P3] < r[P1], so P1=low, P3=val
                if let Some(ref coll) = collation {
                    self.emit_with_p5(
                        Opcode::Lt,
                        low_reg,
                        false_label,
                        val_reg,
                        P4::Collation(coll.clone()),
                        0, // Use collation instead of affinity
                    );
                } else {
                    self.emit_with_p5(
                        Opcode::Lt,
                        low_reg,
                        false_label,
                        val_reg,
                        P4::Unused,
                        low_affinity,
                    );
                }

                // If we reach here, val is not definitively out of range
                // Check if either bound is NULL - if so, result is NULL
                self.resolve_label(check_nulls_label, self.current_addr());
                self.emit(Opcode::IsNull, low_reg, null_label, 0, P4::Unused);
                self.emit(Opcode::IsNull, high_reg, null_label, 0, P4::Unused);

                // Success - in range (result is TRUE)
                self.emit(
                    Opcode::Integer,
                    if *negated { 0 } else { 1 },
                    dest_reg,
                    0,
                    P4::Unused,
                );
                self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);

                // NULL result - comparison involved NULL
                self.resolve_label(null_label, self.current_addr());
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);

                // FALSE result - definitely not in range
                self.resolve_label(false_label, self.current_addr());
                self.emit(
                    Opcode::Integer,
                    if *negated { 1 } else { 0 },
                    dest_reg,
                    0,
                    P4::Unused,
                );

                self.resolve_label(end_label, self.current_addr());
            }
            Expr::Parens(inner) => {
                // Parenthesized expression - just compile the inner expression
                self.compile_expr(inner, dest_reg)?;
            }
            Expr::Collate { expr, .. } => {
                // COLLATE affects comparison/sorting, but doesn't change the value
                self.compile_expr(expr, dest_reg)?;
            }
            Expr::Variable(var) => {
                // Emit Variable opcode to read bound parameter
                let param_idx = match var {
                    crate::parser::ast::Variable::Numbered(Some(idx)) => *idx,
                    crate::parser::ast::Variable::Numbered(None) => {
                        // Unnamed parameter - use next sequential index
                        let idx = self.next_unnamed_param;
                        self.next_unnamed_param += 1;
                        idx
                    }
                    crate::parser::ast::Variable::Named { prefix, name } => {
                        // Look up named parameter in param_names
                        let full_name = format!("{}{}", prefix, name);
                        self.param_names
                            .iter()
                            .position(|n| n.as_deref() == Some(&full_name))
                            .map(|i| (i + 1) as i32) // 1-based index
                            .unwrap_or(1) // Default to 1 if not found
                    }
                };
                self.emit(Opcode::Variable, param_idx, dest_reg, 0, P4::Unused);
            }
            Expr::Cast { expr, type_name } => {
                // Compile the expression first
                self.compile_expr(expr, dest_reg)?;
                // Then apply the cast using the Cast opcode
                // P2 is affinity: 'A'=BLOB, 'B'=TEXT, 'C'=NUMERIC, 'D'=INTEGER, 'E'=REAL
                let affinity = match type_name.name.to_uppercase().as_str() {
                    "TEXT" | "VARCHAR" | "CHAR" | "CLOB" | "NCHAR" | "NVARCHAR" => b'B', // TEXT
                    "INTEGER" | "INT" | "TINYINT" | "SMALLINT" | "MEDIUMINT" | "BIGINT"
                    | "INT2" | "INT8" => b'D', // INTEGER
                    "REAL" | "DOUBLE" | "FLOAT" => b'E',                                 // REAL
                    "NUMERIC" | "DECIMAL" => b'C',                                       // NUMERIC
                    "BLOB" | "NONE" => b'A',                                             // BLOB
                    _ => {
                        // Check for type names with size like VARCHAR(255)
                        let name_upper = type_name.name.to_uppercase();
                        if name_upper.starts_with("VARCHAR")
                            || name_upper.starts_with("CHAR")
                            || name_upper.starts_with("TEXT")
                        {
                            b'B' // TEXT
                        } else if name_upper.starts_with("INT") {
                            b'D' // INTEGER
                        } else {
                            b'C' // Default to NUMERIC
                        }
                    }
                };
                self.emit(Opcode::Cast, dest_reg, affinity as i32, 0, P4::Unused);
            }
            _ => {
                // For other expression types, emit NULL as placeholder
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
            }
        }
        Ok(())
    }

    /// Compile ORDER BY output - sort the sorter and output rows
    fn compile_order_by_output(
        &mut self,
        order_by: &[OrderingTerm],
        sorter_cursor: i32,
        dest: &SelectDest,
    ) -> Result<()> {
        // Sort the sorter
        let sort_done_label = self.alloc_label();
        self.emit(
            Opcode::SorterSort,
            sorter_cursor,
            sort_done_label,
            0,
            P4::Unused,
        );

        // Loop through sorted rows - use label to avoid collision with resolve_labels
        let sorter_loop_start_label = self.alloc_label();
        self.resolve_label(sorter_loop_start_label, self.current_addr());

        // Handle OFFSET: skip rows until offset counter reaches 0
        if let Some(offset_reg) = self.offset_counter_reg {
            let after_offset = self.alloc_label();
            // Use Le (Less or Equal) to check if offset <= 0
            // This handles negative offsets correctly (treated as 0)
            let zero_reg = self.alloc_reg();
            self.emit(Opcode::Integer, 0, zero_reg, 0, P4::Unused);
            self.emit(Opcode::Le, zero_reg, after_offset, offset_reg, P4::Unused);
            // offset > 0: Decrement offset and skip this row
            self.emit(Opcode::AddImm, offset_reg, -1, 0, P4::Unused);
            self.emit(
                Opcode::SorterNext,
                sorter_cursor,
                sorter_loop_start_label,
                0,
                P4::Unused,
            );
            // If SorterNext falls through (no more rows), we're done
            self.emit(Opcode::Goto, 0, sort_done_label, 0, P4::Unused);
            self.resolve_label(after_offset, self.current_addr());
        }

        // Handle LIMIT: check if we've output enough rows
        // Negative LIMIT means no limit (return all rows)
        // LIMIT 0 means no rows
        if let Some(limit_reg) = self.limit_counter_reg {
            if let Some(done_label) = self.limit_done_label {
                // IfNot jumps if limit is 0 (or NULL), which is correct for positive limits
                // For negative limits, IfNot won't jump (negative is truthy) so we continue
                self.emit(Opcode::IfNot, limit_reg, done_label, 0, P4::Unused);
            }
        }

        // Get the row data from sorter into a register
        let record_reg = self.alloc_reg();
        self.emit(Opcode::SorterData, sorter_cursor, record_reg, 0, P4::Unused);

        // Decode the record: [ORDER BY keys..., result columns...]
        // We need to skip the ORDER BY keys and only output result columns
        let num_order_by_cols = order_by.len();
        let num_result_cols = self.result_column_names.len();
        let total_cols = num_order_by_cols + num_result_cols;

        // Use OpenPseudo + Column to decode (SQLite-aligned approach)
        // OpenPseudo creates a pseudo-cursor from record in P2 with P3 columns
        let pseudo_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenPseudo,
            pseudo_cursor,
            record_reg,
            total_cols as i32,
            P4::Unused,
        );

        // Extract each column using Column opcode
        let all_base_reg = self.alloc_regs(total_cols);
        for i in 0..total_cols {
            self.emit(
                Opcode::Column,
                pseudo_cursor,
                i as i32,
                all_base_reg + i as i32,
                P4::Unused,
            );
        }

        // Result columns start after ORDER BY keys
        let result_base_reg = all_base_reg + num_order_by_cols as i32;

        // Output the result columns (skip ORDER BY keys)
        match dest {
            SelectDest::Table { cursor } | SelectDest::EphemTable { cursor } => {
                // Insert into ephemeral/regular table
                let record_reg = self.alloc_reg();
                let rowid_reg = self.alloc_reg();
                self.emit(
                    Opcode::MakeRecord,
                    result_base_reg,
                    num_result_cols as i32,
                    record_reg,
                    P4::Unused,
                );
                self.emit(Opcode::NewRowid, *cursor, rowid_reg, 0, P4::Unused);
                self.emit(Opcode::Insert, *cursor, record_reg, rowid_reg, P4::Unused);
            }
            SelectDest::EphemTableDistinct { cursor } => {
                // Insert into ephemeral table with DISTINCT
                let record_reg = self.alloc_reg();
                let rowid_reg = self.alloc_reg();
                self.emit(
                    Opcode::MakeRecord,
                    result_base_reg,
                    num_result_cols as i32,
                    record_reg,
                    P4::Unused,
                );
                let skip_label = self.alloc_label();
                self.emit(Opcode::Found, *cursor, skip_label, record_reg, P4::Unused);
                self.emit(Opcode::NewRowid, *cursor, rowid_reg, 0, P4::Unused);
                self.emit(Opcode::Insert, *cursor, record_reg, rowid_reg, P4::Unused);
                self.resolve_label(skip_label, self.current_addr());
            }
            SelectDest::Set { reg } => {
                // Copy first result column to destination register (scalar subquery)
                self.emit(Opcode::Copy, result_base_reg, *reg, 0, P4::Unused);
            }
            SelectDest::Exists { reg } => {
                // Set result to 1 (EXISTS found a row)
                self.emit(Opcode::Integer, 1, *reg, 0, P4::Unused);
            }
            _ => {
                // Output as result row
                self.emit(
                    Opcode::ResultRow,
                    result_base_reg,
                    num_result_cols as i32,
                    0,
                    P4::Unused,
                );
            }
        }

        // Decrement limit after output
        if let Some(limit_reg) = self.limit_counter_reg {
            self.emit(Opcode::AddImm, limit_reg, -1, 0, P4::Unused);
        }

        // Move to next sorted row
        self.emit(
            Opcode::SorterNext,
            sorter_cursor,
            sorter_loop_start_label,
            0,
            P4::Unused,
        );

        // Sorting done / limit done label
        self.resolve_label(sort_done_label, self.current_addr());
        if let Some(done_label) = self.limit_done_label {
            self.resolve_label(done_label, self.current_addr());
        }

        // Close the sorter
        self.emit(Opcode::Close, sorter_cursor, 0, 0, P4::Unused);

        Ok(())
    }

    /// Compile LIMIT/OFFSET
    fn compile_limit(&mut self, limit: &LimitClause) -> Result<()> {
        // Store limit in a register for checking during result output
        let limit_reg = self.alloc_reg();
        self.compile_expr(&limit.limit, limit_reg)?;
        // Validate that LIMIT is an integer - raises "datatype mismatch" for float/string
        // Use p5=0xFFFF to prevent P2=0 from being resolved as a label
        self.emit_with_p5(Opcode::MustBeInt, limit_reg, 0, 0, P4::Unused, 0xFFFF);
        self.limit_counter_reg = Some(limit_reg);

        // Allocate label to jump to when limit exhausted
        self.limit_done_label = Some(self.alloc_label());

        if let Some(offset) = &limit.offset {
            let offset_reg = self.alloc_reg();
            self.compile_expr(offset, offset_reg)?;
            // Validate that OFFSET is an integer
            self.emit_with_p5(Opcode::MustBeInt, offset_reg, 0, 0, P4::Unused, 0xFFFF);
            self.offset_counter_reg = Some(offset_reg);
        }

        Ok(())
    }

    /// Output a row with LIMIT/OFFSET enforcement.
    /// skip_label: where to jump if still in OFFSET phase (skip this row)
    fn output_row_with_limit(
        &mut self,
        dest: &SelectDest,
        base_reg: i32,
        count: usize,
        skip_label: i32,
    ) -> Result<()> {
        // Handle OFFSET: skip rows until offset counter reaches 0
        // Negative OFFSET is treated as 0 (no rows to skip)
        if let Some(offset_reg) = self.offset_counter_reg {
            let after_offset = self.alloc_label();
            // Use Le (Less or Equal) to check if offset <= 0
            // This handles negative offsets correctly (treated as 0)
            let zero_reg = self.alloc_reg();
            self.emit(Opcode::Integer, 0, zero_reg, 0, P4::Unused);
            self.emit(Opcode::Le, zero_reg, after_offset, offset_reg, P4::Unused);
            // offset > 0: Decrement offset and skip this row
            self.emit(Opcode::AddImm, offset_reg, -1, 0, P4::Unused);
            self.emit(Opcode::Goto, 0, skip_label, 0, P4::Unused);
            self.resolve_label(after_offset, self.current_addr());
        }

        // Handle LIMIT: check if we've output enough rows
        if let Some(limit_reg) = self.limit_counter_reg {
            if let Some(done_label) = self.limit_done_label {
                // If limit <= 0, we're done
                self.emit(Opcode::IfNot, limit_reg, done_label, 0, P4::Unused);
            }
        }

        // Output the row
        self.output_row(dest, base_reg, count)?;

        // Decrement limit after output
        if let Some(limit_reg) = self.limit_counter_reg {
            self.emit(Opcode::AddImm, limit_reg, -1, 0, P4::Unused);
        }

        Ok(())
    }

    /// Output a row to the destination
    fn output_row(&mut self, dest: &SelectDest, base_reg: i32, count: usize) -> Result<()> {
        match dest {
            SelectDest::Output => {
                self.emit(Opcode::ResultRow, base_reg, count as i32, 0, P4::Unused);
            }
            SelectDest::Mem { base_reg: dest_reg } => {
                for i in 0..count {
                    self.emit(
                        Opcode::Copy,
                        base_reg + i as i32,
                        *dest_reg + i as i32,
                        0,
                        P4::Unused,
                    );
                }
            }
            SelectDest::Table { cursor } | SelectDest::EphemTable { cursor } => {
                let record_reg = self.alloc_reg();
                let rowid_reg = self.alloc_reg();
                self.emit(
                    Opcode::MakeRecord,
                    base_reg,
                    count as i32,
                    record_reg,
                    P4::Unused,
                );
                self.emit(Opcode::NewRowid, *cursor, rowid_reg, 0, P4::Unused);
                self.emit(Opcode::Insert, *cursor, record_reg, rowid_reg, P4::Unused);
            }
            SelectDest::EphemTableDistinct { cursor } => {
                // Insert into ephemeral table with DISTINCT - skip duplicates
                let record_reg = self.alloc_reg();
                let rowid_reg = self.alloc_reg();
                self.emit(
                    Opcode::MakeRecord,
                    base_reg,
                    count as i32,
                    record_reg,
                    P4::Unused,
                );
                // Check if this row already exists - skip if found
                let skip_label = self.alloc_label();
                self.emit(Opcode::Found, *cursor, skip_label, record_reg, P4::Unused);
                // Row not found - insert it
                self.emit(Opcode::NewRowid, *cursor, rowid_reg, 0, P4::Unused);
                self.emit(Opcode::Insert, *cursor, record_reg, rowid_reg, P4::Unused);
                self.resolve_label(skip_label, self.current_addr());
            }
            SelectDest::Coroutine { reg } => {
                for i in 0..count {
                    self.emit(
                        Opcode::Copy,
                        base_reg + i as i32,
                        *reg + i as i32,
                        0,
                        P4::Unused,
                    );
                }
                self.emit(Opcode::Yield, *reg, 0, 0, P4::Unused);
            }
            SelectDest::Exists { reg } => {
                self.emit(Opcode::Integer, 1, *reg, 0, P4::Unused);
            }
            SelectDest::Set { reg } => {
                self.emit(Opcode::Copy, base_reg, *reg, 0, P4::Unused);
            }
            SelectDest::Sorter { cursor } => {
                // For ORDER BY, record format is: [ORDER BY keys..., result columns...]
                // This ensures proper sorting by key columns first
                let order_by_count = self.order_by_terms.as_ref().map(|v| v.len()).unwrap_or(0);

                if order_by_count > 0 {
                    // Compile ORDER BY expressions and store in registers
                    let key_base_reg = self.alloc_regs(order_by_count);
                    if let Some(order_by) = &self.order_by_terms.clone() {
                        for (i, term) in order_by.iter().enumerate() {
                            // Helper to generate ordinal error
                            let make_range_error = |term_num: usize, count: usize| {
                                let ordinal = match term_num {
                                    1 => "1st".to_string(),
                                    2 => "2nd".to_string(),
                                    3 => "3rd".to_string(),
                                    n => format!("{}th", n),
                                };
                                Error::with_message(
                                    ErrorCode::Error,
                                    format!(
                                        "{} ORDER BY term out of range - should be between 1 and {}",
                                        ordinal, count
                                    ),
                                )
                            };

                            // Handle ORDER BY column index (e.g., ORDER BY 1, ORDER BY 2)
                            // Also handles ORDER BY 1 COLLATE xyz by unwrapping the Collate
                            // These should reference result columns, not be literal values
                            let inner_expr = match &term.expr {
                                Expr::Collate { expr, .. } => expr.as_ref(),
                                other => other,
                            };
                            if let Expr::Literal(Literal::Integer(col_idx)) = inner_expr {
                                let col_idx = *col_idx as i32;
                                if col_idx >= 1 && col_idx <= count as i32 {
                                    // Copy from the result column (1-based index)
                                    self.emit(
                                        Opcode::SCopy,
                                        base_reg + col_idx - 1,
                                        key_base_reg + i as i32,
                                        0,
                                        P4::Unused,
                                    );
                                    continue;
                                } else {
                                    return Err(make_range_error(i + 1, count));
                                }
                            }

                            // Handle ORDER BY +N (unary plus on column index, e.g., ORDER BY +2)
                            // Unary plus on an integer should be treated the same as the integer
                            if let Expr::Unary {
                                op: crate::parser::ast::UnaryOp::Pos,
                                expr: inner,
                            } = &term.expr
                            {
                                if let Expr::Literal(Literal::Integer(col_idx)) = inner.as_ref() {
                                    let col_idx = *col_idx as i32;
                                    if col_idx >= 1 && col_idx <= count as i32 {
                                        // Copy from the result column (1-based index)
                                        self.emit(
                                            Opcode::SCopy,
                                            base_reg + col_idx - 1,
                                            key_base_reg + i as i32,
                                            0,
                                            P4::Unused,
                                        );
                                        continue;
                                    } else {
                                        return Err(make_range_error(i + 1, count));
                                    }
                                }
                            }

                            // Handle negative column indices (ORDER BY -1)
                            if let Expr::Unary {
                                op: crate::parser::ast::UnaryOp::Neg,
                                expr: inner,
                            } = &term.expr
                            {
                                if let Expr::Literal(Literal::Integer(_)) = inner.as_ref() {
                                    // Negative column indices are always out of range
                                    return Err(make_range_error(i + 1, count));
                                }
                            }

                            // Check if ORDER BY references a result column name (alias or column name)
                            // This handles GROUP BY queries where ORDER BY y refers to alias y = count(*)
                            if let Expr::Column(col_ref) = &term.expr {
                                if col_ref.table.is_none() {
                                    // Look for matching result column name
                                    if let Some(col_idx) = self
                                        .result_column_names
                                        .iter()
                                        .position(|name| name.eq_ignore_ascii_case(&col_ref.column))
                                    {
                                        // Copy from the result column (0-based index)
                                        self.emit(
                                            Opcode::SCopy,
                                            base_reg + col_idx as i32,
                                            key_base_reg + i as i32,
                                            0,
                                            P4::Unused,
                                        );
                                        continue;
                                    }
                                    // Also check compound_aliases (for aliases from other SELECTs in UNION)
                                    if let Some(&col_idx) =
                                        self.compound_aliases.get(&col_ref.column.to_lowercase())
                                    {
                                        self.emit(
                                            Opcode::SCopy,
                                            base_reg + col_idx as i32,
                                            key_base_reg + i as i32,
                                            0,
                                            P4::Unused,
                                        );
                                        continue;
                                    }
                                }
                            }

                            // For expressions containing aliases (e.g., ORDER BY 10-(x+y)),
                            // populate group_column_regs so compile_expr can resolve aliases
                            // to result column values
                            let saved_group_regs = self.group_column_regs.clone();
                            let saved_alias_exprs = self.alias_expressions.clone();

                            // Clear alias_expressions to prevent it from interfering
                            // with group_column_regs (alias_expressions is checked first
                            // in compile_expr, which would try to compile count(*)
                            // instead of reading from the result register)
                            self.alias_expressions.clear();

                            for (idx, name) in self.result_column_names.iter().enumerate() {
                                let name_lower = name.to_lowercase();
                                if !self.group_column_regs.contains_key(&name_lower) {
                                    self.group_column_regs
                                        .insert(name_lower, base_reg + idx as i32);
                                }
                            }
                            self.compile_expr(&term.expr, key_base_reg + i as i32)?;
                            self.group_column_regs = saved_group_regs;
                            self.alias_expressions = saved_alias_exprs;
                        }
                    }

                    // Copy result columns after ORDER BY keys
                    let full_base_reg = key_base_reg;
                    for i in 0..count {
                        self.emit(
                            Opcode::Copy,
                            base_reg + i as i32,
                            key_base_reg + order_by_count as i32 + i as i32,
                            0,
                            P4::Unused,
                        );
                    }

                    // Make record: ORDER BY keys + result columns
                    let record_reg = self.alloc_reg();
                    let total_cols = order_by_count + count;
                    self.emit(
                        Opcode::MakeRecord,
                        full_base_reg,
                        total_cols as i32,
                        record_reg,
                        P4::Unused,
                    );
                    self.emit(Opcode::SorterInsert, *cursor, record_reg, 0, P4::Unused);
                } else {
                    // No ORDER BY, just store result columns
                    let record_reg = self.alloc_reg();
                    self.emit(
                        Opcode::MakeRecord,
                        base_reg,
                        count as i32,
                        record_reg,
                        P4::Unused,
                    );
                    self.emit(Opcode::SorterInsert, *cursor, record_reg, 0, P4::Unused);
                }
            }
            SelectDest::Discard => {
                // Do nothing
            }
        }
        Ok(())
    }

    // ========================================================================
    // Aggregate helpers
    // ========================================================================

    fn check_for_aggregates(&self, core: &SelectCore) -> bool {
        // Check result columns for aggregate functions
        for col in &core.columns {
            if let ResultColumn::Expr { expr, .. } = col {
                if self.expr_has_aggregate(expr) {
                    return true;
                }
            }
        }
        false
    }

    fn expr_has_aggregate(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(func_call) => {
                let name_upper = func_call.name.to_uppercase();
                let arg_count = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                    crate::parser::ast::FunctionArgs::Star => 0,
                };
                // MIN/MAX with multiple args are scalar functions, not aggregates
                let is_agg = if matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count > 1 {
                    false
                } else {
                    matches!(
                        name_upper.as_str(),
                        "COUNT"
                            | "SUM"
                            | "AVG"
                            | "MIN"
                            | "MAX"
                            | "GROUP_CONCAT"
                            | "STRING_AGG"
                            | "TOTAL"
                    )
                };
                if is_agg {
                    return true;
                }
                // Also check function arguments for aggregates (e.g., coalesce(min(f1), 0))
                if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                    for arg in exprs {
                        if self.expr_has_aggregate(arg) {
                            return true;
                        }
                    }
                }
                false
            }
            Expr::Binary { left, right, .. } => {
                self.expr_has_aggregate(left) || self.expr_has_aggregate(right)
            }
            Expr::Unary { expr: inner, .. } => self.expr_has_aggregate(inner),
            _ => false,
        }
    }

    /// Validate that no result columns contain nested aggregates
    fn validate_no_nested_aggregates(&self, columns: &[ResultColumn]) -> Result<()> {
        for col in columns {
            if let ResultColumn::Expr { expr, .. } = col {
                if let Some(agg_name) = self.check_nested_aggregate(expr) {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        format!("misuse of aggregate function {}()", agg_name),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Check if expression has a nested aggregate (aggregate inside aggregate)
    /// Returns Some(function_name) if nested aggregate found
    fn check_nested_aggregate(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Function(func_call) => {
                let name_upper = func_call.name.to_uppercase();
                let arg_count = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                    crate::parser::ast::FunctionArgs::Star => 0,
                };
                // Check if this is an aggregate function
                let is_aggregate = if matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count > 1
                {
                    false
                } else {
                    crate::functions::is_aggregate_function(&func_call.name)
                };

                if is_aggregate {
                    // Check if any argument contains an aggregate
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        for arg in exprs {
                            if let Some(nested_name) = self.find_aggregate_in_expr(arg) {
                                return Some(nested_name);
                            }
                        }
                    }
                }
                // Not an aggregate, or no nested aggregate - check children
                if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                    for arg in exprs {
                        if let Some(nested) = self.check_nested_aggregate(arg) {
                            return Some(nested);
                        }
                    }
                }
                None
            }
            Expr::Binary { left, right, .. } => self
                .check_nested_aggregate(left)
                .or_else(|| self.check_nested_aggregate(right)),
            Expr::Unary { expr: inner, .. } => self.check_nested_aggregate(inner),
            _ => None,
        }
    }

    /// Find if expression contains an aggregate function, returning its name
    fn find_aggregate_in_expr(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Function(func_call) => {
                let name_upper = func_call.name.to_uppercase();
                let arg_count = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                    crate::parser::ast::FunctionArgs::Star => 0,
                };
                let is_aggregate = if matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count > 1
                {
                    false
                } else {
                    crate::functions::is_aggregate_function(&func_call.name)
                };
                if is_aggregate {
                    return Some(func_call.name.clone());
                }
                // Check arguments
                if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                    for arg in exprs {
                        if let Some(found) = self.find_aggregate_in_expr(arg) {
                            return Some(found);
                        }
                    }
                }
                None
            }
            Expr::Binary { left, right, .. } => self
                .find_aggregate_in_expr(left)
                .or_else(|| self.find_aggregate_in_expr(right)),
            Expr::Unary { expr: inner, .. } => self.find_aggregate_in_expr(inner),
            _ => None,
        }
    }

    /// Check if ORDER BY term is valid for compound SELECT
    /// Valid terms: column position numbers, column names, and expressions matching result columns
    fn is_valid_compound_order_by_term(&self, expr: &Expr) -> bool {
        match expr {
            // Integer literal = column position (1-based)
            Expr::Literal(Literal::Integer(n)) => {
                let pos = *n as usize;
                pos >= 1 && pos <= self.result_column_names.len()
            }
            // Column reference - must match a result column name OR a column from another SELECT in the UNION
            // In UNION queries, ORDER BY can reference columns that appear in any component SELECT
            // SQLite extension: the right SELECT's columns can be referenced too
            Expr::Column(col_ref) => {
                // Get the column name to match (ignore table qualifier for matching)
                let col_name = &col_ref.column;

                // Check if any result column name matches this column name
                // We need to compare just the column part, not table.column
                let in_result_cols = self.result_column_names.iter().any(|name| {
                    // Handle both simple names and qualified names (table.column)
                    let result_col = if let Some(pos) = name.rfind('.') {
                        &name[pos + 1..]
                    } else {
                        name.as_str()
                    };
                    result_col.eq_ignore_ascii_case(col_name)
                });

                // Also check compound_aliases (column names from right SELECTs in UNION)
                let in_compound_aliases =
                    self.compound_aliases.contains_key(&col_name.to_lowercase());

                in_result_cols || in_compound_aliases
            }
            // For complex expressions (like f2+101), check if they match a result column name
            // These must match exactly or be invalid
            _ => {
                // Try to convert expression to string and match against result column names
                let expr_str = self.expr_to_simple_string(expr);
                if expr_str.is_empty() {
                    // Can't determine - allow it (SQLite may do runtime check)
                    true
                } else {
                    self.result_column_names
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&expr_str))
                }
            }
        }
    }

    /// Convert expression to simple string for comparison (used for ORDER BY validation)
    fn expr_to_simple_string(&self, expr: &Expr) -> String {
        match expr {
            Expr::Column(col_ref) => {
                if let Some(table) = &col_ref.table {
                    format!("{}.{}", table, col_ref.column)
                } else {
                    col_ref.column.clone()
                }
            }
            Expr::Literal(lit) => match lit {
                Literal::Integer(n) => n.to_string(),
                Literal::Float(f) => f.to_string(),
                Literal::String(s) => s.clone(),
                Literal::Blob(b) => format!("x'{}'", hex::encode(b)),
                Literal::Null => "NULL".to_string(),
                Literal::CurrentTime => "CURRENT_TIME".to_string(),
                Literal::CurrentDate => "CURRENT_DATE".to_string(),
                Literal::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
                Literal::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            },
            Expr::Binary { op, left, right } => {
                let left_str = self.expr_to_simple_string(left);
                let right_str = self.expr_to_simple_string(right);
                let op_str = match op {
                    BinaryOp::Add => "+",
                    BinaryOp::Sub => "-",
                    BinaryOp::Mul => "*",
                    BinaryOp::Div => "/",
                    _ => "?",
                };
                format!("{}{}{}", left_str, op_str, right_str)
            }
            _ => String::new(),
        }
    }

    /// Collect aliases that refer to aggregate expressions
    fn collect_aggregate_aliases(&self, columns: &[ResultColumn]) -> Vec<String> {
        let mut aliases = Vec::new();
        for col in columns {
            if let ResultColumn::Expr { expr, alias } = col {
                if let Some(alias_name) = alias {
                    if self.expr_has_aggregate(expr) {
                        aliases.push(alias_name.to_lowercase());
                    }
                }
            }
        }
        aliases
    }

    /// Check if expression references any aggregate alias
    /// Returns Some(alias_name) if found
    fn find_aggregate_alias_in_expr<'a>(
        &self,
        expr: &Expr,
        aliases: &'a [String],
    ) -> Option<&'a String> {
        match expr {
            Expr::Column(col_ref) => {
                // If no table qualifier, check if column name matches an alias
                if col_ref.table.is_none() {
                    let col_lower = col_ref.column.to_lowercase();
                    aliases.iter().find(|a| **a == col_lower)
                } else {
                    None
                }
            }
            Expr::Binary { left, right, .. } => self
                .find_aggregate_alias_in_expr(left, aliases)
                .or_else(|| self.find_aggregate_alias_in_expr(right, aliases)),
            Expr::Unary { expr: inner, .. } => self.find_aggregate_alias_in_expr(inner, aliases),
            Expr::Function(func) => {
                if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func.args {
                    for arg in exprs {
                        if let Some(alias) = self.find_aggregate_alias_in_expr(arg, aliases) {
                            return Some(alias);
                        }
                    }
                }
                None
            }
            Expr::IsNull { expr: inner, .. } => self.find_aggregate_alias_in_expr(inner, aliases),
            Expr::Between {
                expr,
                low,
                high,
                negated: _,
            } => self
                .find_aggregate_alias_in_expr(expr, aliases)
                .or_else(|| self.find_aggregate_alias_in_expr(low, aliases))
                .or_else(|| self.find_aggregate_alias_in_expr(high, aliases)),
            Expr::In {
                expr,
                list,
                negated: _,
            } => {
                if let Some(alias) = self.find_aggregate_alias_in_expr(expr, aliases) {
                    return Some(alias);
                }
                if let crate::parser::ast::InList::Values(values) = list {
                    for item in values {
                        if let Some(alias) = self.find_aggregate_alias_in_expr(item, aliases) {
                            return Some(alias);
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Validate that WHERE clause does not reference aggregate aliases
    fn validate_no_aggregate_aliases_in_where(
        &self,
        where_clause: Option<&Expr>,
        columns: &[ResultColumn],
    ) -> Result<()> {
        if let Some(where_expr) = where_clause {
            let agg_aliases = self.collect_aggregate_aliases(columns);
            if !agg_aliases.is_empty() {
                if let Some(alias) = self.find_aggregate_alias_in_expr(where_expr, &agg_aliases) {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        format!("misuse of aliased aggregate {}", alias),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Validate that HAVING clause does not use aggregate aliases inside aggregate functions
    /// SQLite allows: SELECT min(f1) AS m FROM t GROUP BY f1 HAVING m > 5 (using alias outside agg)
    /// SQLite rejects: SELECT min(f1) AS m FROM t GROUP BY f1 HAVING max(m) > 5 (alias inside agg)
    fn validate_no_aggregate_alias_in_having_aggregate(
        &self,
        having: Option<&Expr>,
        columns: &[ResultColumn],
    ) -> Result<()> {
        if let Some(having_expr) = having {
            let agg_aliases = self.collect_aggregate_aliases(columns);
            if !agg_aliases.is_empty() {
                if let Some(alias) =
                    self.find_aggregate_alias_in_aggregate(having_expr, &agg_aliases)
                {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        format!("misuse of aliased aggregate {}", alias),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Find if any aggregate alias is used inside an aggregate function
    /// Returns the original alias name (preserving case) if found
    fn find_aggregate_alias_in_aggregate<'a>(
        &self,
        expr: &Expr,
        aliases: &'a [String],
    ) -> Option<String> {
        match expr {
            Expr::Function(func) => {
                let is_agg = crate::functions::is_aggregate_function(&func.name);
                if is_agg {
                    // Inside an aggregate - check if any alias is used
                    if let crate::parser::ast::FunctionArgs::Exprs(args) = &func.args {
                        for arg in args {
                            if let Some(alias) = self.find_aggregate_alias_in_expr(arg, aliases) {
                                // Return the alias with its original case from the column definition
                                return Some(alias.clone());
                            }
                        }
                    }
                }
                // Also recurse into function arguments for non-aggregate functions
                // (e.g., coalesce(max(m), 0))
                if let crate::parser::ast::FunctionArgs::Exprs(args) = &func.args {
                    for arg in args {
                        if let Some(alias) = self.find_aggregate_alias_in_aggregate(arg, aliases) {
                            return Some(alias);
                        }
                    }
                }
                None
            }
            Expr::Binary { left, right, .. } => self
                .find_aggregate_alias_in_aggregate(left, aliases)
                .or_else(|| self.find_aggregate_alias_in_aggregate(right, aliases)),
            Expr::Unary { expr: inner, .. } => {
                self.find_aggregate_alias_in_aggregate(inner, aliases)
            }
            Expr::IsNull { expr: inner, .. } => {
                self.find_aggregate_alias_in_aggregate(inner, aliases)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated: _,
            } => self
                .find_aggregate_alias_in_aggregate(expr, aliases)
                .or_else(|| self.find_aggregate_alias_in_aggregate(low, aliases))
                .or_else(|| self.find_aggregate_alias_in_aggregate(high, aliases)),
            Expr::In {
                expr,
                list,
                negated: _,
            } => {
                if let Some(alias) = self.find_aggregate_alias_in_aggregate(expr, aliases) {
                    return Some(alias);
                }
                if let crate::parser::ast::InList::Values(values) = list {
                    for item in values {
                        if let Some(alias) = self.find_aggregate_alias_in_aggregate(item, aliases) {
                            return Some(alias);
                        }
                    }
                }
                None
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    if let Some(alias) = self.find_aggregate_alias_in_aggregate(op, aliases) {
                        return Some(alias);
                    }
                }
                for clause in when_clauses {
                    if let Some(alias) =
                        self.find_aggregate_alias_in_aggregate(&clause.when, aliases)
                    {
                        return Some(alias);
                    }
                    if let Some(alias) =
                        self.find_aggregate_alias_in_aggregate(&clause.then, aliases)
                    {
                        return Some(alias);
                    }
                }
                if let Some(else_expr) = else_clause {
                    if let Some(alias) = self.find_aggregate_alias_in_aggregate(else_expr, aliases)
                    {
                        return Some(alias);
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn init_aggregates(&mut self, columns: &[ResultColumn]) -> Result<Vec<i32>> {
        let mut regs = Vec::new();
        for col in columns {
            if let ResultColumn::Expr { expr, .. } = col {
                // Count how many aggregate functions are in this expression
                // e.g., sum(n)/count(n) has 2 aggregates
                let num_aggs = self.count_aggregates_in_expr(expr);
                for _ in 0..num_aggs {
                    let reg = self.alloc_reg();
                    self.emit(Opcode::Null, 0, reg, 0, P4::Unused);
                    regs.push(reg);
                }
            }
        }
        Ok(regs)
    }

    fn accumulate_aggregates(&mut self, columns: &[ResultColumn], agg_regs: &[i32]) -> Result<()> {
        let mut agg_idx = 0;
        for col in columns {
            if let ResultColumn::Expr { expr, .. } = col {
                self.accumulate_aggregates_in_expr(expr, agg_regs, &mut agg_idx)?;
            }
        }
        Ok(())
    }

    /// Recursively accumulate aggregates in an expression
    fn accumulate_aggregates_in_expr(
        &mut self,
        expr: &Expr,
        agg_regs: &[i32],
        agg_idx: &mut usize,
    ) -> Result<()> {
        match expr {
            Expr::Function(func_call) => {
                let name_upper = func_call.name.to_uppercase();
                let arg_count = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                    crate::parser::ast::FunctionArgs::Star => 0,
                };

                // Check if this is an aggregate function
                let is_multi_arg_min_max =
                    matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count > 1;
                if is_multi_arg_min_max {
                    // Multi-arg min/max is scalar - recurse into arguments
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        for arg in exprs {
                            self.accumulate_aggregates_in_expr(arg, agg_regs, agg_idx)?;
                        }
                    }
                    return Ok(());
                }

                if matches!(
                    name_upper.as_str(),
                    "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                        | "GROUP_CONCAT"
                        | "STRING_AGG"
                        | "TOTAL"
                ) {
                    if *agg_idx >= agg_regs.len() {
                        return Ok(()); // No more aggregate registers
                    }
                    let reg = agg_regs[*agg_idx];

                    // Check argument count limits
                    let (min_args, max_args, skip_if_exceeded) = match name_upper.as_str() {
                        "COUNT" => (0, 1, false),
                        "SUM" | "AVG" | "TOTAL" => (1, 1, false),
                        "MIN" | "MAX" => (1, 1, true),
                        "GROUP_CONCAT" => (1, 2, false),
                        _ => (0, 255, false),
                    };

                    if arg_count < min_args {
                        return Err(crate::error::Error::with_message(
                            crate::error::ErrorCode::Error,
                            format!("wrong number of arguments to function {}()", func_call.name),
                        ));
                    }

                    if arg_count > max_args {
                        if skip_if_exceeded {
                            return Ok(());
                        }
                        return Err(crate::error::Error::with_message(
                            crate::error::ErrorCode::Error,
                            format!("wrong number of arguments to function {}()", func_call.name),
                        ));
                    }

                    // Compile ALL arguments into consecutive registers
                    let arg_base = self.next_reg;
                    let mut argc = 0;
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        for arg_expr in exprs {
                            let arg_reg = self.alloc_reg();
                            self.compile_expr(arg_expr, arg_reg)?;
                            argc += 1;
                        }
                    }
                    // For COUNT(*), initialize arg_base with 1 so it's not NULL
                    if argc == 0 && name_upper == "COUNT" {
                        let arg_reg = self.alloc_reg();
                        self.emit(Opcode::Integer, 1, arg_reg, 0, P4::Unused);
                        argc = 1;
                    }

                    // Emit aggregate step opcode
                    // P1 = argc, P2 = arg_base, P3 = accumulator register
                    self.emit(Opcode::AggStep, argc, arg_base, reg, P4::Text(name_upper));
                    *agg_idx += 1;
                } else {
                    // Non-aggregate function - recurse into arguments to find nested aggregates
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        for arg in exprs {
                            self.accumulate_aggregates_in_expr(arg, agg_regs, agg_idx)?;
                        }
                    }
                }
            }
            Expr::Binary { left, right, .. } => {
                self.accumulate_aggregates_in_expr(left, agg_regs, agg_idx)?;
                self.accumulate_aggregates_in_expr(right, agg_regs, agg_idx)?;
            }
            Expr::Unary { expr: inner, .. } => {
                self.accumulate_aggregates_in_expr(inner, agg_regs, agg_idx)?;
            }
            Expr::Parens(inner) => {
                self.accumulate_aggregates_in_expr(inner, agg_regs, agg_idx)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Accumulate aggregates with bare column affinity for MIN/MAX
    /// When MIN/MAX changes, save all column references from the current row
    fn accumulate_aggregates_with_bare_cols(
        &mut self,
        columns: &[ResultColumn],
        agg_regs: &[i32],
        changed_reg: i32,
        col_refs: &[(i32, i32, String)],
        saved_col_map: &HashMap<(i32, i32), i32>,
    ) -> Result<()> {
        let mut agg_idx = 0;
        for col in columns {
            if let ResultColumn::Expr { expr, .. } = col {
                self.accumulate_aggregates_in_expr_with_bare_cols(
                    expr,
                    agg_regs,
                    &mut agg_idx,
                    changed_reg,
                    col_refs,
                    saved_col_map,
                )?;
            }
        }
        Ok(())
    }

    /// Recursively accumulate aggregates with bare column affinity tracking
    fn accumulate_aggregates_in_expr_with_bare_cols(
        &mut self,
        expr: &Expr,
        agg_regs: &[i32],
        agg_idx: &mut usize,
        changed_reg: i32,
        col_refs: &[(i32, i32, String)],
        saved_col_map: &HashMap<(i32, i32), i32>,
    ) -> Result<()> {
        match expr {
            Expr::Function(func_call) => {
                let name_upper = func_call.name.to_uppercase();
                let arg_count = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                    crate::parser::ast::FunctionArgs::Star => 0,
                };

                // Check if this is an aggregate function
                let is_multi_arg_min_max =
                    matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count > 1;
                if is_multi_arg_min_max {
                    // Multi-arg min/max is scalar - recurse into arguments
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        for arg in exprs {
                            self.accumulate_aggregates_in_expr_with_bare_cols(
                                arg,
                                agg_regs,
                                agg_idx,
                                changed_reg,
                                col_refs,
                                saved_col_map,
                            )?;
                        }
                    }
                    return Ok(());
                }

                if matches!(
                    name_upper.as_str(),
                    "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                        | "GROUP_CONCAT"
                        | "STRING_AGG"
                        | "TOTAL"
                ) {
                    if *agg_idx >= agg_regs.len() {
                        return Ok(()); // No more aggregate registers
                    }
                    let reg = agg_regs[*agg_idx];

                    // Check argument count limits
                    let (min_args, max_args, skip_if_exceeded) = match name_upper.as_str() {
                        "COUNT" => (0, 1, false),
                        "SUM" | "AVG" | "TOTAL" => (1, 1, false),
                        "MIN" | "MAX" => (1, 1, true),
                        "GROUP_CONCAT" => (1, 2, false),
                        _ => (0, 255, false),
                    };

                    if arg_count < min_args {
                        return Err(crate::error::Error::with_message(
                            crate::error::ErrorCode::Error,
                            format!("wrong number of arguments to function {}()", func_call.name),
                        ));
                    }

                    if arg_count > max_args {
                        if skip_if_exceeded {
                            return Ok(());
                        }
                        return Err(crate::error::Error::with_message(
                            crate::error::ErrorCode::Error,
                            format!("wrong number of arguments to function {}()", func_call.name),
                        ));
                    }

                    // Compile ALL arguments into consecutive registers
                    let arg_base = self.next_reg;
                    let mut argc = 0;
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        for arg_expr in exprs {
                            let arg_reg = self.alloc_reg();
                            self.compile_expr(arg_expr, arg_reg)?;
                            argc += 1;
                        }
                    }
                    // For COUNT(*), initialize arg_base with 1 so it's not NULL
                    if argc == 0 && name_upper == "COUNT" {
                        let arg_reg = self.alloc_reg();
                        self.emit(Opcode::Integer, 1, arg_reg, 0, P4::Unused);
                        argc = 1;
                    }

                    // For MIN/MAX, use P5 to track when value changes
                    let is_min_max = matches!(name_upper.as_str(), "MIN" | "MAX");
                    if is_min_max {
                        // Emit AggStep with P5 pointing to changed_reg
                        self.emit(Opcode::AggStep, argc, arg_base, reg, P4::Text(name_upper));
                        // Set P5 to the changed_reg register number
                        if let Some(op) = self.ops.last_mut() {
                            op.p5 = changed_reg as u16;
                        }

                        // After AggStep, if changed_reg is set, save all column refs
                        let skip_save_label = self.alloc_label();
                        self.emit(Opcode::IfNot, changed_reg, skip_save_label, 0, P4::Unused);

                        // Save column values from current row
                        for (cursor, col_idx, _) in col_refs {
                            if let Some(&dest_reg) = saved_col_map.get(&(*cursor, *col_idx)) {
                                if *col_idx == -1 {
                                    self.emit(Opcode::Rowid, *cursor, dest_reg, 0, P4::Unused);
                                } else {
                                    self.emit(
                                        Opcode::Column,
                                        *cursor,
                                        *col_idx,
                                        dest_reg,
                                        P4::Unused,
                                    );
                                }
                            }
                        }

                        self.resolve_label(skip_save_label, self.current_addr());
                    } else {
                        // Regular aggregate - no bare column affinity
                        self.emit(Opcode::AggStep, argc, arg_base, reg, P4::Text(name_upper));
                    }
                    *agg_idx += 1;
                } else {
                    // Non-aggregate function - recurse into arguments to find nested aggregates
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        for arg in exprs {
                            self.accumulate_aggregates_in_expr_with_bare_cols(
                                arg,
                                agg_regs,
                                agg_idx,
                                changed_reg,
                                col_refs,
                                saved_col_map,
                            )?;
                        }
                    }
                }
            }
            Expr::Binary { left, right, .. } => {
                self.accumulate_aggregates_in_expr_with_bare_cols(
                    left,
                    agg_regs,
                    agg_idx,
                    changed_reg,
                    col_refs,
                    saved_col_map,
                )?;
                self.accumulate_aggregates_in_expr_with_bare_cols(
                    right,
                    agg_regs,
                    agg_idx,
                    changed_reg,
                    col_refs,
                    saved_col_map,
                )?;
            }
            Expr::Unary { expr: inner, .. } => {
                self.accumulate_aggregates_in_expr_with_bare_cols(
                    inner,
                    agg_regs,
                    agg_idx,
                    changed_reg,
                    col_refs,
                    saved_col_map,
                )?;
            }
            Expr::Parens(inner) => {
                self.accumulate_aggregates_in_expr_with_bare_cols(
                    inner,
                    agg_regs,
                    agg_idx,
                    changed_reg,
                    col_refs,
                    saved_col_map,
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Expand Star and TableStar in result columns to explicit column expressions.
    /// This is needed for GROUP BY queries where we need to handle each column individually.
    fn expand_result_columns(&self, columns: &[ResultColumn]) -> Vec<ResultColumn> {
        let mut expanded = Vec::new();

        for col in columns {
            match col {
                ResultColumn::Star => {
                    // Expand * to all columns from all tables
                    for table in &self.tables {
                        if let Some(schema_table) = &table.schema_table {
                            for col_def in &schema_table.columns {
                                expanded.push(ResultColumn::Expr {
                                    expr: Expr::Column(ColumnRef {
                                        database: None,
                                        table: Some(table.name.clone()),
                                        column: col_def.name.clone(),
                                        column_index: None,
                                        source_text: None,
                                    }),
                                    alias: None,
                                });
                            }
                        } else if let Some(subquery_cols) = &table.subquery_columns {
                            for col_name in subquery_cols {
                                expanded.push(ResultColumn::Expr {
                                    expr: Expr::Column(ColumnRef {
                                        database: None,
                                        table: Some(table.name.clone()),
                                        column: col_name.clone(),
                                        column_index: None,
                                        source_text: None,
                                    }),
                                    alias: None,
                                });
                            }
                        }
                    }
                }
                ResultColumn::TableStar(table_name) => {
                    // Expand table.* to columns from that table
                    for table in &self.tables {
                        if table.name.eq_ignore_ascii_case(table_name) {
                            if let Some(schema_table) = &table.schema_table {
                                for col_def in &schema_table.columns {
                                    expanded.push(ResultColumn::Expr {
                                        expr: Expr::Column(ColumnRef {
                                            database: None,
                                            table: Some(table.name.clone()),
                                            column: col_def.name.clone(),
                                            column_index: None,
                                            source_text: None,
                                        }),
                                        alias: None,
                                    });
                                }
                            } else if let Some(subquery_cols) = &table.subquery_columns {
                                for col_name in subquery_cols {
                                    expanded.push(ResultColumn::Expr {
                                        expr: Expr::Column(ColumnRef {
                                            database: None,
                                            table: Some(table.name.clone()),
                                            column: col_name.clone(),
                                            column_index: None,
                                            source_text: None,
                                        }),
                                        alias: None,
                                    });
                                }
                            }
                        }
                    }
                }
                ResultColumn::Expr { .. } => {
                    expanded.push(col.clone());
                }
            }
        }

        expanded
    }

    fn finalize_aggregates(
        &mut self,
        columns: &[ResultColumn],
        agg_regs: &[i32],
    ) -> Result<(i32, usize)> {
        self.finalize_aggregates_with_group(columns, agg_regs, None, 0)
    }

    fn finalize_aggregates_with_group(
        &mut self,
        columns: &[ResultColumn],
        agg_regs: &[i32],
        group_by: Option<&[Expr]>,
        group_regs: i32,
    ) -> Result<(i32, usize)> {
        // Expand Star and TableStar to explicit column expressions
        let expanded_columns = self.expand_result_columns(columns);
        let columns = &expanded_columns;

        // Pre-allocate all destination registers to ensure they are contiguous
        // This is important because expression compilation may allocate additional
        // temporary registers, which would make the result registers non-contiguous
        let num_columns = columns.len();
        let base_reg = self.next_reg;
        let dest_regs: Vec<i32> = (0..num_columns).map(|_| self.alloc_reg()).collect();

        let mut count = 0;
        let mut agg_idx = 0;

        // Populate group_column_regs for GROUP BY column substitution
        // This allows expressions like log*2+1 to read `log` from group registers
        if let Some(group_exprs) = group_by {
            for (i, group_expr) in group_exprs.iter().enumerate() {
                // Extract column name from simple column references
                if let Expr::Column(col_ref) = group_expr {
                    let col_name_lower = col_ref.column.to_lowercase();
                    self.group_column_regs
                        .insert(col_name_lower, group_regs + i as i32);
                }
            }
        }

        for (col_idx, col) in columns.iter().enumerate() {
            let dest_reg = dest_regs[col_idx];
            if let ResultColumn::Expr { expr, alias } = col {
                // Populate result_column_names for this column
                let col_name = alias
                    .clone()
                    .unwrap_or_else(|| self.expr_to_name(expr, count + 1));
                self.result_column_names.push(col_name);
                // Check if this column matches a GROUP BY expression
                if let Some(group_exprs) = group_by {
                    if let Some(idx) = self.find_matching_group_expr(expr, group_exprs) {
                        // Copy from the group register
                        self.emit(
                            Opcode::Copy,
                            group_regs + idx as i32,
                            dest_reg,
                            0,
                            P4::Unused,
                        );
                        count += 1;
                        continue;
                    }
                }

                if let Expr::Function(func_call) = expr {
                    let name_upper = func_call.name.to_uppercase();
                    let arg_count = match &func_call.args {
                        crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                        crate::parser::ast::FunctionArgs::Star => 0,
                    };
                    // MIN/MAX with multiple args are scalar functions
                    let is_multi_arg_min_max =
                        matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count > 1;
                    if !is_multi_arg_min_max
                        && matches!(
                            name_upper.as_str(),
                            "COUNT"
                                | "SUM"
                                | "AVG"
                                | "MIN"
                                | "MAX"
                                | "GROUP_CONCAT"
                                | "STRING_AGG"
                                | "TOTAL"
                        )
                    {
                        let agg_reg = agg_regs[agg_idx];
                        self.emit(Opcode::AggFinal, agg_reg, dest_reg, 0, P4::Text(name_upper));
                        // Track this finalized aggregate for HAVING clause compilation
                        self.agg_final_regs.push(dest_reg);
                        agg_idx += 1;
                    } else if self.expr_has_aggregate(expr) {
                        // Non-aggregate function with nested aggregates (e.g., coalesce(max(a), 'x'))
                        let num_aggs = self.count_aggregates_in_expr(expr);
                        self.agg_final_regs.clear();
                        self.agg_final_idx = 0;

                        // Emit AggFinal for each aggregate in this expression
                        for _ in 0..num_aggs {
                            if agg_idx < agg_regs.len() {
                                let agg_reg = agg_regs[agg_idx];
                                let result_reg = self.alloc_reg();
                                let agg_name = self
                                    .get_aggregate_name_at_index(expr, self.agg_final_regs.len());
                                self.emit(
                                    Opcode::AggFinal,
                                    agg_reg,
                                    result_reg,
                                    0,
                                    P4::Text(agg_name),
                                );
                                self.agg_final_regs.push(result_reg);
                                agg_idx += 1;
                            }
                        }

                        // Now compile the expression - it will use agg_final_regs
                        self.compile_expr(expr, dest_reg)?;

                        // Clear the aggregate context
                        self.agg_final_regs.clear();
                        self.agg_final_idx = 0;
                    } else {
                        // Non-aggregate function - check if it was saved in registers
                        if let Some((base_reg, ref indices)) = self.non_agg_saved_regs {
                            if col_idx < indices.len() {
                                if let Some(offset) = indices[col_idx] {
                                    // Copy from saved register
                                    let src_reg = base_reg + offset as i32;
                                    self.emit(Opcode::SCopy, src_reg, dest_reg, 0, P4::Unused);
                                } else {
                                    self.compile_expr(expr, dest_reg)?;
                                }
                            } else {
                                self.compile_expr(expr, dest_reg)?;
                            }
                        } else {
                            self.compile_expr(expr, dest_reg)?;
                        }
                    }
                } else if self.expr_has_aggregate(expr) {
                    // Expression contains nested aggregates - finalize them first
                    let num_aggs = self.count_aggregates_in_expr(expr);
                    self.agg_final_regs.clear();
                    self.agg_final_idx = 0;

                    // Emit AggFinal for each aggregate in this expression
                    for _ in 0..num_aggs {
                        if agg_idx < agg_regs.len() {
                            let agg_reg = agg_regs[agg_idx];
                            let result_reg = self.alloc_reg();
                            // Get the aggregate name for this index
                            let agg_name =
                                self.get_aggregate_name_at_index(expr, self.agg_final_regs.len());
                            self.emit(Opcode::AggFinal, agg_reg, result_reg, 0, P4::Text(agg_name));
                            self.agg_final_regs.push(result_reg);
                            agg_idx += 1;
                        }
                    }

                    // Now compile the expression - it will use agg_final_regs
                    self.compile_expr(expr, dest_reg)?;

                    // Clear the aggregate context
                    self.agg_final_regs.clear();
                    self.agg_final_idx = 0;
                } else {
                    // Non-aggregate expression - check if it was saved in registers
                    if let Some((base_reg, ref indices)) = self.non_agg_saved_regs {
                        if col_idx < indices.len() {
                            if let Some(offset) = indices[col_idx] {
                                // Copy from saved register
                                let src_reg = base_reg + offset as i32;
                                self.emit(Opcode::SCopy, src_reg, dest_reg, 0, P4::Unused);
                            } else {
                                self.compile_expr(expr, dest_reg)?;
                            }
                        } else {
                            self.compile_expr(expr, dest_reg)?;
                        }
                    } else {
                        self.compile_expr(expr, dest_reg)?;
                    }
                }
            }
            count += 1;
        }

        // NOTE: Do NOT clear group_column_regs here - HAVING clause needs it
        // The caller is responsible for clearing it after HAVING is compiled

        Ok((base_reg, count))
    }

    /// Find if an expression matches one of the GROUP BY expressions
    fn find_matching_group_expr(&self, expr: &Expr, group_by: &[Expr]) -> Option<usize> {
        for (i, group_expr) in group_by.iter().enumerate() {
            if self.exprs_equal(expr, group_expr) {
                return Some(i);
            }
        }
        None
    }

    /// Check if two expressions are structurally equal
    fn exprs_equal(&self, a: &Expr, b: &Expr) -> bool {
        match (a, b) {
            (Expr::Column(c1), Expr::Column(c2)) => {
                c1.table == c2.table && c1.column.to_uppercase() == c2.column.to_uppercase()
            }
            (Expr::Literal(l1), Expr::Literal(l2)) => l1 == l2,
            (Expr::Function(f1), Expr::Function(f2)) => {
                // Compare function names case-insensitively
                if !f1.name.eq_ignore_ascii_case(&f2.name) {
                    return false;
                }
                // Compare DISTINCT flags
                if f1.distinct != f2.distinct {
                    return false;
                }
                // Compare arguments
                match (&f1.args, &f2.args) {
                    (
                        crate::parser::ast::FunctionArgs::Star,
                        crate::parser::ast::FunctionArgs::Star,
                    ) => true,
                    (
                        crate::parser::ast::FunctionArgs::Exprs(args1),
                        crate::parser::ast::FunctionArgs::Exprs(args2),
                    ) => {
                        if args1.len() != args2.len() {
                            return false;
                        }
                        args1
                            .iter()
                            .zip(args2.iter())
                            .all(|(a, b)| self.exprs_equal(a, b))
                    }
                    _ => false,
                }
            }
            (Expr::Parens(e1), Expr::Parens(e2)) => self.exprs_equal(e1, e2),
            (Expr::Parens(e1), e2) => self.exprs_equal(e1, e2),
            (e1, Expr::Parens(e2)) => self.exprs_equal(e1, e2),
            (
                Expr::Binary {
                    op: op1,
                    left: left1,
                    right: right1,
                },
                Expr::Binary {
                    op: op2,
                    left: left2,
                    right: right2,
                },
            ) => op1 == op2 && self.exprs_equal(left1, left2) && self.exprs_equal(right1, right2),
            (
                Expr::Unary {
                    op: op1,
                    expr: expr1,
                },
                Expr::Unary {
                    op: op2,
                    expr: expr2,
                },
            ) => op1 == op2 && self.exprs_equal(expr1, expr2),
            _ => false,
        }
    }

    /// Count aggregates in an expression
    fn count_aggregates_in_expr(&self, expr: &Expr) -> usize {
        match expr {
            Expr::Function(func_call) => {
                let name_upper = func_call.name.to_uppercase();
                let arg_count = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                    crate::parser::ast::FunctionArgs::Star => 0,
                };
                let is_multi_arg_min_max =
                    matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count > 1;
                if !is_multi_arg_min_max
                    && matches!(
                        name_upper.as_str(),
                        "COUNT"
                            | "SUM"
                            | "AVG"
                            | "MIN"
                            | "MAX"
                            | "GROUP_CONCAT"
                            | "STRING_AGG"
                            | "TOTAL"
                    )
                {
                    1
                } else {
                    // Non-aggregate function - recurse into arguments to find nested aggregates
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        exprs.iter().map(|e| self.count_aggregates_in_expr(e)).sum()
                    } else {
                        0
                    }
                }
            }
            Expr::Binary { left, right, .. } => {
                self.count_aggregates_in_expr(left) + self.count_aggregates_in_expr(right)
            }
            Expr::Unary { expr, .. } => self.count_aggregates_in_expr(expr),
            Expr::Parens(inner) => self.count_aggregates_in_expr(inner),
            _ => 0,
        }
    }

    /// Check if any result column contains a MIN or MAX aggregate function
    /// Used to determine whether bare column affinity applies
    fn has_min_max_aggregate(&self, columns: &[ResultColumn]) -> bool {
        for col in columns {
            if let ResultColumn::Expr { expr, .. } = col {
                if self.expr_has_min_max(expr) {
                    return true;
                }
            }
        }
        false
    }

    /// Check if an expression contains MIN or MAX aggregate
    fn expr_has_min_max(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(func_call) => {
                let name_upper = func_call.name.to_uppercase();
                let arg_count = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                    crate::parser::ast::FunctionArgs::Star => 0,
                };
                // Only single-arg MIN/MAX are aggregates
                let is_aggregate_min_max =
                    matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count == 1;
                if is_aggregate_min_max {
                    return true;
                }
                // Recurse into function arguments
                if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                    for arg in exprs {
                        if self.expr_has_min_max(arg) {
                            return true;
                        }
                    }
                }
                false
            }
            Expr::Binary { left, right, .. } => {
                self.expr_has_min_max(left) || self.expr_has_min_max(right)
            }
            Expr::Unary { expr, .. } => self.expr_has_min_max(expr),
            Expr::Parens(inner) => self.expr_has_min_max(inner),
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    if self.expr_has_min_max(op) {
                        return true;
                    }
                }
                for clause in when_clauses {
                    if self.expr_has_min_max(&clause.when) || self.expr_has_min_max(&clause.then) {
                        return true;
                    }
                }
                if let Some(ec) = else_clause {
                    if self.expr_has_min_max(ec) {
                        return true;
                    }
                }
                false
            }
            _ => false,
        }
    }

    /// Get the name of the aggregate function at a given index in expression traversal order
    fn get_aggregate_name_at_index(&self, expr: &Expr, target_idx: usize) -> String {
        let mut current_idx = 0;
        self.find_aggregate_name(expr, target_idx, &mut current_idx)
            .unwrap_or_else(|| "COUNT".to_string())
    }

    fn find_aggregate_name(
        &self,
        expr: &Expr,
        target_idx: usize,
        current_idx: &mut usize,
    ) -> Option<String> {
        match expr {
            Expr::Function(func_call) => {
                let name_upper = func_call.name.to_uppercase();
                let arg_count = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                    crate::parser::ast::FunctionArgs::Star => 0,
                };
                let is_multi_arg_min_max =
                    matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count > 1;
                if !is_multi_arg_min_max
                    && matches!(
                        name_upper.as_str(),
                        "COUNT"
                            | "SUM"
                            | "AVG"
                            | "MIN"
                            | "MAX"
                            | "GROUP_CONCAT"
                            | "STRING_AGG"
                            | "TOTAL"
                    )
                {
                    if *current_idx == target_idx {
                        return Some(name_upper);
                    }
                    *current_idx += 1;
                    None
                } else {
                    // Non-aggregate function - recurse into arguments
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        for arg in exprs {
                            if let Some(name) =
                                self.find_aggregate_name(arg, target_idx, current_idx)
                            {
                                return Some(name);
                            }
                        }
                    }
                    None
                }
            }
            Expr::Binary { left, right, .. } => self
                .find_aggregate_name(left, target_idx, current_idx)
                .or_else(|| self.find_aggregate_name(right, target_idx, current_idx)),
            Expr::Unary { expr, .. } => self.find_aggregate_name(expr, target_idx, current_idx),
            Expr::Parens(inner) => self.find_aggregate_name(inner, target_idx, current_idx),
            _ => None,
        }
    }

    fn reset_aggregates(&mut self, agg_regs: &[i32]) -> Result<()> {
        for &reg in agg_regs {
            self.emit(Opcode::Null, 0, reg, 0, P4::Unused);
        }
        Ok(())
    }

    /// Count the number of aggregate arguments in result columns without compiling
    /// Recursively searches through expressions to find nested aggregates
    fn count_aggregate_args(&self, columns: &[ResultColumn]) -> usize {
        let mut count = 0;
        for col in columns {
            if let ResultColumn::Expr { expr, .. } = col {
                count += self.count_aggregate_args_in_expr(expr);
            }
        }
        count
    }

    /// Recursively count aggregate arguments in an expression
    fn count_aggregate_args_in_expr(&self, expr: &Expr) -> usize {
        match expr {
            Expr::Function(func_call) => {
                let name_upper = func_call.name.to_uppercase();
                let arg_count = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                    crate::parser::ast::FunctionArgs::Star => 0,
                };
                let is_multi_arg_min_max =
                    matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count > 1;
                if !is_multi_arg_min_max
                    && matches!(
                        name_upper.as_str(),
                        "COUNT"
                            | "SUM"
                            | "AVG"
                            | "MIN"
                            | "MAX"
                            | "GROUP_CONCAT"
                            | "STRING_AGG"
                            | "TOTAL"
                    )
                {
                    // This is an aggregate - count its arguments
                    arg_count
                } else {
                    // Non-aggregate function - recurse into arguments
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        exprs
                            .iter()
                            .map(|e| self.count_aggregate_args_in_expr(e))
                            .sum()
                    } else {
                        0
                    }
                }
            }
            Expr::Binary { left, right, .. } => {
                self.count_aggregate_args_in_expr(left) + self.count_aggregate_args_in_expr(right)
            }
            Expr::Unary { expr, .. } => self.count_aggregate_args_in_expr(expr),
            Expr::Parens(inner) => self.count_aggregate_args_in_expr(inner),
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                let mut count = 0;
                if let Some(op) = operand {
                    count += self.count_aggregate_args_in_expr(op);
                }
                for clause in when_clauses {
                    count += self.count_aggregate_args_in_expr(&clause.when);
                    count += self.count_aggregate_args_in_expr(&clause.then);
                }
                if let Some(else_expr) = else_clause {
                    count += self.count_aggregate_args_in_expr(else_expr);
                }
                count
            }
            Expr::Cast { expr, .. } => self.count_aggregate_args_in_expr(expr),
            Expr::Collate { expr, .. } => self.count_aggregate_args_in_expr(expr),
            _ => 0,
        }
    }

    fn compile_aggregate_args(&mut self, columns: &[ResultColumn]) -> Result<(i32, usize)> {
        let base_reg = self.next_reg;
        let mut count = 0;
        for col in columns {
            if let ResultColumn::Expr { expr, .. } = col {
                count += self.compile_aggregate_args_in_expr(expr)?;
            }
        }
        Ok((base_reg, count))
    }

    /// Recursively compile aggregate arguments in an expression
    fn compile_aggregate_args_in_expr(&mut self, expr: &Expr) -> Result<usize> {
        match expr {
            Expr::Function(func_call) => {
                let name_upper = func_call.name.to_uppercase();
                let arg_count = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                    crate::parser::ast::FunctionArgs::Star => 0,
                };
                let is_multi_arg_min_max =
                    matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count > 1;
                if !is_multi_arg_min_max
                    && matches!(
                        name_upper.as_str(),
                        "COUNT"
                            | "SUM"
                            | "AVG"
                            | "MIN"
                            | "MAX"
                            | "GROUP_CONCAT"
                            | "STRING_AGG"
                            | "TOTAL"
                    )
                {
                    // This is an aggregate - compile its arguments
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        for arg in exprs {
                            let reg = self.alloc_reg();
                            self.compile_expr(arg, reg)?;
                        }
                        Ok(exprs.len())
                    } else {
                        Ok(0)
                    }
                } else {
                    // Non-aggregate function - recurse into arguments
                    let mut count = 0;
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        for arg in exprs {
                            count += self.compile_aggregate_args_in_expr(arg)?;
                        }
                    }
                    Ok(count)
                }
            }
            Expr::Binary { left, right, .. } => {
                let left_count = self.compile_aggregate_args_in_expr(left)?;
                let right_count = self.compile_aggregate_args_in_expr(right)?;
                Ok(left_count + right_count)
            }
            Expr::Unary { expr, .. } => self.compile_aggregate_args_in_expr(expr),
            Expr::Parens(inner) => self.compile_aggregate_args_in_expr(inner),
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                let mut count = 0;
                if let Some(op) = operand {
                    count += self.compile_aggregate_args_in_expr(op)?;
                }
                for clause in when_clauses {
                    count += self.compile_aggregate_args_in_expr(&clause.when)?;
                    count += self.compile_aggregate_args_in_expr(&clause.then)?;
                }
                if let Some(else_expr) = else_clause {
                    count += self.compile_aggregate_args_in_expr(else_expr)?;
                }
                Ok(count)
            }
            Expr::Cast { expr, .. } => self.compile_aggregate_args_in_expr(expr),
            Expr::Collate { expr, .. } => self.compile_aggregate_args_in_expr(expr),
            _ => Ok(0),
        }
    }

    /// Count non-aggregate result columns that are not GROUP BY columns
    /// These need to be stored in the sorter for later retrieval
    fn count_non_agg_result_cols(
        &self,
        columns: &[ResultColumn],
        group_by: Option<&[Expr]>,
    ) -> usize {
        // Expand Star and TableStar to explicit column expressions
        let expanded_columns = self.expand_result_columns(columns);
        let columns = &expanded_columns;

        let mut count = 0;
        for col in columns {
            if let ResultColumn::Expr { expr, .. } = col {
                if !self.expr_has_aggregate(expr) {
                    // Check if this is a GROUP BY column
                    let is_group_col = group_by
                        .map(|gb| self.find_matching_group_expr(expr, gb).is_some())
                        .unwrap_or(false);
                    if !is_group_col {
                        count += 1;
                    }
                }
            }
        }
        count
    }

    /// Compile non-aggregate result expressions into registers (to store in sorter)
    /// Returns (base_reg, count, indices) where indices maps result column index to sorter column offset
    fn compile_non_agg_result_cols(
        &mut self,
        columns: &[ResultColumn],
        group_by: Option<&[Expr]>,
    ) -> Result<(i32, usize, Vec<Option<usize>>)> {
        // Expand Star and TableStar to explicit column expressions
        let expanded_columns = self.expand_result_columns(columns);
        let columns = &expanded_columns;

        // First pass: count how many non-agg columns we have
        let mut non_agg_count = 0;
        let mut is_non_agg: Vec<bool> = Vec::with_capacity(columns.len());

        for col in columns {
            if let ResultColumn::Expr { expr, .. } = col {
                if !self.expr_has_aggregate(expr) {
                    // Check if this is a GROUP BY column
                    let is_group_col = group_by
                        .map(|gb| self.find_matching_group_expr(expr, gb).is_some())
                        .unwrap_or(false);
                    if !is_group_col {
                        is_non_agg.push(true);
                        non_agg_count += 1;
                    } else {
                        is_non_agg.push(false);
                    }
                } else {
                    is_non_agg.push(false);
                }
            } else {
                is_non_agg.push(false);
            }
        }

        // Pre-allocate all destination registers to ensure they're contiguous
        // compile_expr may allocate temp registers, so we must reserve space first
        let base_reg = self.alloc_regs(non_agg_count);

        // Second pass: compile non-agg expressions into pre-allocated registers
        let mut count = 0;
        let mut indices = Vec::with_capacity(columns.len());

        for (i, col) in columns.iter().enumerate() {
            if is_non_agg[i] {
                if let ResultColumn::Expr { expr, .. } = col {
                    let dest_reg = base_reg + count as i32;
                    self.compile_expr(expr, dest_reg)?;
                    indices.push(Some(count));
                    count += 1;
                } else {
                    indices.push(None);
                }
            } else {
                indices.push(None);
            }
        }
        Ok((base_reg, count, indices))
    }

    fn accumulate_from_sorter(
        &mut self,
        cursor: i32,
        columns: &[ResultColumn],
        agg_regs: &[i32],
        col_offset: usize,
    ) -> Result<()> {
        let mut agg_idx = 0;
        let mut col_idx = col_offset;
        for col in columns {
            if let ResultColumn::Expr { expr, .. } = col {
                self.accumulate_from_sorter_in_expr(
                    cursor,
                    expr,
                    agg_regs,
                    &mut agg_idx,
                    &mut col_idx,
                )?;
            }
        }
        Ok(())
    }

    /// Recursively accumulate aggregates in an expression from sorter data
    fn accumulate_from_sorter_in_expr(
        &mut self,
        cursor: i32,
        expr: &Expr,
        agg_regs: &[i32],
        agg_idx: &mut usize,
        col_idx: &mut usize,
    ) -> Result<()> {
        match expr {
            Expr::Function(func_call) => {
                let name_upper = func_call.name.to_uppercase();
                let arg_count = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                    crate::parser::ast::FunctionArgs::Star => 0,
                };
                // MIN/MAX with multiple args are scalar functions
                let is_multi_arg_min_max =
                    matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count > 1;
                if !is_multi_arg_min_max
                    && matches!(
                        name_upper.as_str(),
                        "COUNT"
                            | "SUM"
                            | "AVG"
                            | "MIN"
                            | "MAX"
                            | "GROUP_CONCAT"
                            | "STRING_AGG"
                            | "TOTAL"
                    )
                {
                    // For COUNT(*) (arg_count == 0), use a constant
                    // For other cases, read ALL arguments from sorter
                    let arg_base = self.next_reg;
                    let argc;
                    if arg_count == 0 && name_upper == "COUNT" {
                        let arg_reg = self.alloc_reg();
                        self.emit(Opcode::Integer, 1, arg_reg, 0, P4::Unused);
                        argc = 1;
                    } else {
                        argc = arg_count;
                        for _ in 0..arg_count {
                            let arg_reg = self.alloc_reg();
                            self.emit(Opcode::Column, cursor, *col_idx as i32, arg_reg, P4::Unused);
                            *col_idx += 1;
                        }
                    }
                    // Emit AggStep with: P1=argc, P2=arg_base, P3=accumulator
                    self.emit(
                        Opcode::AggStep,
                        argc as i32,
                        arg_base,
                        agg_regs[*agg_idx],
                        P4::Text(name_upper),
                    );
                    *agg_idx += 1;
                } else {
                    // Non-aggregate function - recurse into arguments
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        for arg in exprs {
                            self.accumulate_from_sorter_in_expr(
                                cursor, arg, agg_regs, agg_idx, col_idx,
                            )?;
                        }
                    }
                }
            }
            Expr::Binary { left, right, .. } => {
                self.accumulate_from_sorter_in_expr(cursor, left, agg_regs, agg_idx, col_idx)?;
                self.accumulate_from_sorter_in_expr(cursor, right, agg_regs, agg_idx, col_idx)?;
            }
            Expr::Unary { expr, .. } => {
                self.accumulate_from_sorter_in_expr(cursor, expr, agg_regs, agg_idx, col_idx)?;
            }
            Expr::Parens(inner) => {
                self.accumulate_from_sorter_in_expr(cursor, inner, agg_regs, agg_idx, col_idx)?;
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    self.accumulate_from_sorter_in_expr(cursor, op, agg_regs, agg_idx, col_idx)?;
                }
                for clause in when_clauses {
                    self.accumulate_from_sorter_in_expr(
                        cursor,
                        &clause.when,
                        agg_regs,
                        agg_idx,
                        col_idx,
                    )?;
                    self.accumulate_from_sorter_in_expr(
                        cursor,
                        &clause.then,
                        agg_regs,
                        agg_idx,
                        col_idx,
                    )?;
                }
                if let Some(else_expr) = else_clause {
                    self.accumulate_from_sorter_in_expr(
                        cursor, else_expr, agg_regs, agg_idx, col_idx,
                    )?;
                }
            }
            Expr::Cast { expr, .. } => {
                self.accumulate_from_sorter_in_expr(cursor, expr, agg_regs, agg_idx, col_idx)?;
            }
            Expr::Collate { expr, .. } => {
                self.accumulate_from_sorter_in_expr(cursor, expr, agg_regs, agg_idx, col_idx)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn compile_expressions(&mut self, exprs: &[Expr]) -> Result<(i32, usize)> {
        // Pre-allocate all destination registers first to ensure they're contiguous.
        // compile_expr may allocate temporary registers internally, so allocating
        // one-at-a-time would result in non-contiguous dest registers.
        let base_reg = self.alloc_regs(exprs.len());
        for (i, expr) in exprs.iter().enumerate() {
            self.compile_expr(expr, base_reg + i as i32)?;
        }
        Ok((base_reg, exprs.len()))
    }

    // ========================================================================
    // Compound select helpers
    // ========================================================================

    fn merge_distinct(&mut self, left: i32, right: i32) -> Result<()> {
        // Iterate through right cursor and insert rows into left cursor
        // Skip rows that already exist in left (for DISTINCT behavior)
        let done_label = self.alloc_label();
        self.emit(Opcode::Rewind, right, done_label, 0, P4::Unused);

        let loop_label = self.alloc_label();
        self.resolve_label(loop_label, self.current_addr());

        // Get all columns from the right row
        let col_count = if self.compound_column_count > 0 {
            self.compound_column_count
        } else {
            1
        };

        let base_reg = self.next_reg;
        for i in 0..col_count {
            let reg = self.alloc_reg();
            self.emit(Opcode::Column, right, i as i32, reg, P4::Unused);
        }

        // Make a record to check for duplicates
        let record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            base_reg,
            col_count as i32,
            record_reg,
            P4::Unused,
        );

        // Skip this row if it already exists in left (NotFound jumps if NOT found)
        let skip_label = self.alloc_label();
        self.emit(Opcode::Found, left, skip_label, record_reg, P4::Unused);

        // Row not found - insert it
        let rowid_reg = self.alloc_reg();
        self.emit(Opcode::NewRowid, left, rowid_reg, 0, P4::Unused);
        self.emit(Opcode::Insert, left, record_reg, rowid_reg, P4::Unused);

        self.resolve_label(skip_label, self.current_addr());
        self.emit(Opcode::Next, right, loop_label, 0, P4::Unused);
        self.resolve_label(done_label, self.current_addr());
        Ok(())
    }

    fn intersect_tables(&mut self, _left: i32, _right: i32) -> Result<()> {
        // Placeholder - keep only rows in both tables
        Ok(())
    }

    fn except_tables(&mut self, _left: i32, _right: i32) -> Result<()> {
        // Placeholder - remove right table rows from left
        Ok(())
    }

    fn output_ephemeral_table(&mut self, cursor: i32, dest: &SelectDest) -> Result<()> {
        let done_label = self.alloc_label();
        self.emit(Opcode::Rewind, cursor, done_label, 0, P4::Unused);

        // Use label to avoid collision with resolve_labels
        let loop_start_label = self.alloc_label();
        self.resolve_label(loop_start_label, self.current_addr());

        // Handle OFFSET: skip rows until offset counter reaches 0
        if let Some(offset_reg) = self.offset_counter_reg {
            let after_offset = self.alloc_label();
            // Check if offset <= 0
            let zero_reg = self.alloc_reg();
            self.emit(Opcode::Integer, 0, zero_reg, 0, P4::Unused);
            self.emit(Opcode::Le, zero_reg, after_offset, offset_reg, P4::Unused);
            // offset > 0: Decrement and skip this row
            self.emit(Opcode::AddImm, offset_reg, -1, 0, P4::Unused);
            self.emit(Opcode::Next, cursor, loop_start_label, 0, P4::Unused);
            // If Next falls through, we're done
            self.emit(Opcode::Goto, 0, done_label, 0, P4::Unused);
            self.resolve_label(after_offset, self.current_addr());
        }

        // Handle LIMIT: check if we've output enough rows
        if let Some(limit_reg) = self.limit_counter_reg {
            if let Some(limit_done) = self.limit_done_label {
                self.emit(Opcode::IfNot, limit_reg, limit_done, 0, P4::Unused);
            }
        }

        // Get all columns from the ephemeral table row
        let col_count = if self.compound_column_count > 0 {
            self.compound_column_count
        } else {
            1 // Default to 1 if not set
        };

        let base_reg = self.next_reg;
        for i in 0..col_count {
            let reg = self.alloc_reg();
            self.emit(Opcode::Column, cursor, i as i32, reg, P4::Unused);
        }

        // Output based on destination
        self.output_row(dest, base_reg, col_count)?;

        // Decrement limit counter
        if let Some(limit_reg) = self.limit_counter_reg {
            self.emit(Opcode::AddImm, limit_reg, -1, 0, P4::Unused);
        }

        self.emit(Opcode::Next, cursor, loop_start_label, 0, P4::Unused);
        self.resolve_label(done_label, self.current_addr());

        // Resolve LIMIT done label (jump here when limit exhausted)
        if let Some(limit_done) = self.limit_done_label {
            self.resolve_label(limit_done, self.current_addr());
        }

        Ok(())
    }

    /// Output ephemeral table in sorted order (for UNION which requires sorted, distinct output)
    fn output_ephemeral_table_sorted(&mut self, cursor: i32, dest: &SelectDest) -> Result<()> {
        let col_count = if self.compound_column_count > 0 {
            self.compound_column_count
        } else {
            1
        };

        // Create sorter for sorted output
        let sorter_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenEphemeral,
            sorter_cursor,
            col_count as i32,
            0,
            P4::Unused,
        );

        // Read all rows from ephemeral table into sorter
        let done_label = self.alloc_label();
        self.emit(Opcode::Rewind, cursor, done_label, 0, P4::Unused);

        let loop_start_label = self.alloc_label();
        self.resolve_label(loop_start_label, self.current_addr());

        // Get columns from ephemeral table
        let base_reg = self.next_reg;
        for i in 0..col_count {
            let reg = self.alloc_reg();
            self.emit(Opcode::Column, cursor, i as i32, reg, P4::Unused);
        }

        // Make record and insert into sorter
        let record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            base_reg,
            col_count as i32,
            record_reg,
            P4::Unused,
        );
        self.emit(
            Opcode::SorterInsert,
            sorter_cursor,
            record_reg,
            0,
            P4::Unused,
        );

        self.emit(Opcode::Next, cursor, loop_start_label, 0, P4::Unused);
        self.resolve_label(done_label, self.current_addr());

        // Sort the data
        let sort_done_label = self.alloc_label();
        self.emit(
            Opcode::SorterSort,
            sorter_cursor,
            sort_done_label,
            0,
            P4::Unused,
        );

        // Output sorted rows
        let sorter_loop_label = self.alloc_label();
        self.resolve_label(sorter_loop_label, self.current_addr());

        // Handle OFFSET: skip rows until offset counter reaches 0
        if let Some(offset_reg) = self.offset_counter_reg {
            let after_offset = self.alloc_label();
            // Check if offset <= 0
            let zero_reg = self.alloc_reg();
            self.emit(Opcode::Integer, 0, zero_reg, 0, P4::Unused);
            self.emit(Opcode::Le, zero_reg, after_offset, offset_reg, P4::Unused);
            // offset > 0: Decrement and skip this row
            self.emit(Opcode::AddImm, offset_reg, -1, 0, P4::Unused);
            self.emit(
                Opcode::SorterNext,
                sorter_cursor,
                sorter_loop_label,
                0,
                P4::Unused,
            );
            // If SorterNext falls through, we're done
            self.emit(Opcode::Goto, 0, sort_done_label, 0, P4::Unused);
            self.resolve_label(after_offset, self.current_addr());
        }

        // Handle LIMIT: check if we've output enough rows
        if let Some(limit_reg) = self.limit_counter_reg {
            if let Some(done_label) = self.limit_done_label {
                self.emit(Opcode::IfNot, limit_reg, done_label, 0, P4::Unused);
            }
        }

        // Get row data from sorter
        let sorter_data_reg = self.alloc_reg();
        self.emit(
            Opcode::SorterData,
            sorter_cursor,
            sorter_data_reg,
            0,
            P4::Unused,
        );

        // Use OpenPseudo + Column to decode (SQLite-aligned approach)
        let pseudo_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenPseudo,
            pseudo_cursor,
            sorter_data_reg,
            col_count as i32,
            P4::Unused,
        );

        // Extract each column using Column opcode
        let out_base_reg = self.alloc_regs(col_count);
        for i in 0..col_count {
            self.emit(
                Opcode::Column,
                pseudo_cursor,
                i as i32,
                out_base_reg + i as i32,
                P4::Unused,
            );
        }

        // Output the row
        self.output_row(dest, out_base_reg, col_count)?;

        // Decrement limit counter
        if let Some(limit_reg) = self.limit_counter_reg {
            self.emit(Opcode::AddImm, limit_reg, -1, 0, P4::Unused);
        }

        self.emit(
            Opcode::SorterNext,
            sorter_cursor,
            sorter_loop_label,
            0,
            P4::Unused,
        );
        self.resolve_label(sort_done_label, self.current_addr());

        // Resolve LIMIT done label (jump here when limit exhausted)
        if let Some(done_label) = self.limit_done_label {
            self.resolve_label(done_label, self.current_addr());
        }

        self.emit(Opcode::Close, sorter_cursor, 0, 0, P4::Unused);

        Ok(())
    }

    fn output_ephemeral_table_intersect(
        &mut self,
        left_cursor: i32,
        right_cursor: i32,
        dest: &SelectDest,
        needs_sorted_output: bool,
    ) -> Result<()> {
        // INTERSECT: Output only rows from left that also appear in right
        let col_count = if self.compound_column_count > 0 {
            self.compound_column_count
        } else {
            1
        };

        if needs_sorted_output {
            // Create a temp table for filtered results, then sort
            let temp_cursor = self.alloc_cursor();
            self.emit(Opcode::OpenEphemeral, temp_cursor, 0, 0, P4::Unused);

            // Filter: iterate left, output to temp if found in right
            let left_done_label = self.alloc_label();
            self.emit(Opcode::Rewind, left_cursor, left_done_label, 0, P4::Unused);

            let left_loop_label = self.alloc_label();
            self.resolve_label(left_loop_label, self.current_addr());

            let base_reg = self.next_reg;
            for i in 0..col_count {
                let reg = self.alloc_reg();
                self.emit(Opcode::Column, left_cursor, i as i32, reg, P4::Unused);
            }

            let record_reg = self.alloc_reg();
            self.emit(
                Opcode::MakeRecord,
                base_reg,
                col_count as i32,
                record_reg,
                P4::Unused,
            );

            let skip_label = self.alloc_label();
            self.emit(
                Opcode::NotFound,
                right_cursor,
                skip_label,
                record_reg,
                P4::Unused,
            );

            // Found in right - insert into temp
            let rowid_reg = self.alloc_reg();
            self.emit(Opcode::NewRowid, temp_cursor, rowid_reg, 0, P4::Unused);
            self.emit(
                Opcode::Insert,
                temp_cursor,
                record_reg,
                rowid_reg,
                P4::Unused,
            );

            self.resolve_label(skip_label, self.current_addr());
            self.emit(Opcode::Next, left_cursor, left_loop_label, 0, P4::Unused);
            self.resolve_label(left_done_label, self.current_addr());

            // Now sort temp and output
            self.emit(Opcode::Close, left_cursor, 0, 0, P4::Unused);
            let sorter_cursor = self.alloc_cursor();
            self.emit(
                Opcode::OpenEphemeral,
                sorter_cursor,
                col_count as i32,
                0,
                P4::Unused,
            );

            let sort_done = self.alloc_label();
            self.emit(Opcode::Rewind, temp_cursor, sort_done, 0, P4::Unused);

            let sort_loop = self.alloc_label();
            self.resolve_label(sort_loop, self.current_addr());

            let sort_base_reg = self.next_reg;
            for i in 0..col_count {
                let reg = self.alloc_reg();
                self.emit(Opcode::Column, temp_cursor, i as i32, reg, P4::Unused);
            }

            let sort_record_reg = self.alloc_reg();
            self.emit(
                Opcode::MakeRecord,
                sort_base_reg,
                col_count as i32,
                sort_record_reg,
                P4::Unused,
            );

            let sort_rowid_reg = self.alloc_reg();
            self.emit(
                Opcode::NewRowid,
                sorter_cursor,
                sort_rowid_reg,
                0,
                P4::Unused,
            );
            self.emit(
                Opcode::Insert,
                sorter_cursor,
                sort_record_reg,
                sort_rowid_reg,
                P4::Unused,
            );

            self.emit(Opcode::Next, temp_cursor, sort_loop, 0, P4::Unused);
            self.resolve_label(sort_done, self.current_addr());

            self.emit(Opcode::Close, temp_cursor, 0, 0, P4::Unused);

            // Output sorted results
            self.output_ephemeral_table(sorter_cursor, dest)?;
            self.emit(Opcode::Close, sorter_cursor, 0, 0, P4::Unused);
        } else {
            // No sorting needed - just filter and output
            let done_label = self.alloc_label();
            self.emit(Opcode::Rewind, left_cursor, done_label, 0, P4::Unused);

            let loop_label = self.alloc_label();
            self.resolve_label(loop_label, self.current_addr());

            let base_reg = self.next_reg;
            for i in 0..col_count {
                let reg = self.alloc_reg();
                self.emit(Opcode::Column, left_cursor, i as i32, reg, P4::Unused);
            }

            let record_reg = self.alloc_reg();
            self.emit(
                Opcode::MakeRecord,
                base_reg,
                col_count as i32,
                record_reg,
                P4::Unused,
            );

            let skip_label = self.alloc_label();
            self.emit(
                Opcode::NotFound,
                right_cursor,
                skip_label,
                record_reg,
                P4::Unused,
            );

            // Found in right - output this row
            self.output_row(dest, base_reg, col_count)?;

            self.resolve_label(skip_label, self.current_addr());
            self.emit(Opcode::Next, left_cursor, loop_label, 0, P4::Unused);
            self.resolve_label(done_label, self.current_addr());
        }

        Ok(())
    }

    fn output_ephemeral_table_except(
        &mut self,
        left_cursor: i32,
        right_cursor: i32,
        dest: &SelectDest,
        needs_sorted_output: bool,
    ) -> Result<()> {
        // EXCEPT: Output only rows from left that do NOT appear in right
        let col_count = if self.compound_column_count > 0 {
            self.compound_column_count
        } else {
            1
        };

        if needs_sorted_output {
            // Create a temp table for filtered results, then sort
            let temp_cursor = self.alloc_cursor();
            self.emit(Opcode::OpenEphemeral, temp_cursor, 0, 0, P4::Unused);

            // Filter: iterate left, output to temp if NOT found in right
            let left_done_label = self.alloc_label();
            self.emit(Opcode::Rewind, left_cursor, left_done_label, 0, P4::Unused);

            let left_loop_label = self.alloc_label();
            self.resolve_label(left_loop_label, self.current_addr());

            let base_reg = self.next_reg;
            for i in 0..col_count {
                let reg = self.alloc_reg();
                self.emit(Opcode::Column, left_cursor, i as i32, reg, P4::Unused);
            }

            let record_reg = self.alloc_reg();
            self.emit(
                Opcode::MakeRecord,
                base_reg,
                col_count as i32,
                record_reg,
                P4::Unused,
            );

            let skip_label = self.alloc_label();
            self.emit(
                Opcode::Found,
                right_cursor,
                skip_label,
                record_reg,
                P4::Unused,
            );

            // NOT found in right - insert into temp
            let rowid_reg = self.alloc_reg();
            self.emit(Opcode::NewRowid, temp_cursor, rowid_reg, 0, P4::Unused);
            self.emit(
                Opcode::Insert,
                temp_cursor,
                record_reg,
                rowid_reg,
                P4::Unused,
            );

            self.resolve_label(skip_label, self.current_addr());
            self.emit(Opcode::Next, left_cursor, left_loop_label, 0, P4::Unused);
            self.resolve_label(left_done_label, self.current_addr());

            // Now sort temp and output
            self.emit(Opcode::Close, left_cursor, 0, 0, P4::Unused);
            let sorter_cursor = self.alloc_cursor();
            self.emit(
                Opcode::OpenEphemeral,
                sorter_cursor,
                col_count as i32,
                0,
                P4::Unused,
            );

            let sort_done = self.alloc_label();
            self.emit(Opcode::Rewind, temp_cursor, sort_done, 0, P4::Unused);

            let sort_loop = self.alloc_label();
            self.resolve_label(sort_loop, self.current_addr());

            let sort_base_reg = self.next_reg;
            for i in 0..col_count {
                let reg = self.alloc_reg();
                self.emit(Opcode::Column, temp_cursor, i as i32, reg, P4::Unused);
            }

            let sort_record_reg = self.alloc_reg();
            self.emit(
                Opcode::MakeRecord,
                sort_base_reg,
                col_count as i32,
                sort_record_reg,
                P4::Unused,
            );

            let sort_rowid_reg = self.alloc_reg();
            self.emit(
                Opcode::NewRowid,
                sorter_cursor,
                sort_rowid_reg,
                0,
                P4::Unused,
            );
            self.emit(
                Opcode::Insert,
                sorter_cursor,
                sort_record_reg,
                sort_rowid_reg,
                P4::Unused,
            );

            self.emit(Opcode::Next, temp_cursor, sort_loop, 0, P4::Unused);
            self.resolve_label(sort_done, self.current_addr());

            self.emit(Opcode::Close, temp_cursor, 0, 0, P4::Unused);

            // Output sorted results
            self.output_ephemeral_table(sorter_cursor, dest)?;
            self.emit(Opcode::Close, sorter_cursor, 0, 0, P4::Unused);
        } else {
            // No sorting needed - just filter and output
            let done_label = self.alloc_label();
            self.emit(Opcode::Rewind, left_cursor, done_label, 0, P4::Unused);

            let loop_label = self.alloc_label();
            self.resolve_label(loop_label, self.current_addr());

            let base_reg = self.next_reg;
            for i in 0..col_count {
                let reg = self.alloc_reg();
                self.emit(Opcode::Column, left_cursor, i as i32, reg, P4::Unused);
            }

            let record_reg = self.alloc_reg();
            self.emit(
                Opcode::MakeRecord,
                base_reg,
                col_count as i32,
                record_reg,
                P4::Unused,
            );

            let skip_label = self.alloc_label();
            self.emit(
                Opcode::Found,
                right_cursor,
                skip_label,
                record_reg,
                P4::Unused,
            );

            // NOT found in right - output this row
            self.output_row(dest, base_reg, col_count)?;

            self.resolve_label(skip_label, self.current_addr());
            self.emit(Opcode::Next, left_cursor, loop_label, 0, P4::Unused);
            self.resolve_label(done_label, self.current_addr());
        }

        Ok(())
    }

    // ========================================================================
    // Utility methods
    // ========================================================================

    fn alloc_reg(&mut self) -> i32 {
        let reg = self.next_reg;
        self.next_reg += 1;
        reg
    }

    fn alloc_regs(&mut self, count: usize) -> i32 {
        let base = self.next_reg;
        self.next_reg += count as i32;
        base
    }

    fn alloc_cursor(&mut self) -> i32 {
        let cursor = self.next_cursor;
        self.next_cursor += 1;
        cursor
    }

    fn alloc_label(&mut self) -> i32 {
        let label = self.next_label;
        self.next_label += 1;
        self.labels.insert(label, None);
        label
    }

    /// Extract the collation name from an expression.
    /// If the expression is Expr::Collate { collation, ... }, return the collation name.
    /// Otherwise, return "BINARY" as the default.
    fn extract_collation(expr: &Expr) -> String {
        match expr {
            Expr::Collate { collation, .. } => collation.to_uppercase(),
            _ => "BINARY".to_string(),
        }
    }

    /// Unwrap any COLLATE wrapper to get the inner expression.
    /// This is used to check if an ORDER BY term is actually a column index.
    fn unwrap_collate(expr: &Expr) -> &Expr {
        match expr {
            Expr::Collate { expr: inner, .. } => Self::unwrap_collate(inner),
            _ => expr,
        }
    }

    fn resolve_label(&mut self, label: i32, addr: usize) {
        self.labels.insert(label, Some(addr as i32));
    }

    fn current_addr(&self) -> usize {
        self.ops.len()
    }

    fn emit(&mut self, opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: P4) {
        self.ops.push(VdbeOp::with_p4(opcode, p1, p2, p3, p4));
    }

    fn emit_with_p5(&mut self, opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: P4, p5: u16) {
        self.ops
            .push(VdbeOp::with_p4(opcode, p1, p2, p3, p4).with_p5(p5));
    }

    fn resolve_labels(&mut self) -> Result<()> {
        // Resolve all label references in jump instructions
        for op in &mut self.ops {
            if op.opcode.is_jump() {
                // Skip ops that were already resolved (inlined from subqueries)
                // These are marked with p5 = 0xFFFF
                if op.p5 == 0xFFFF {
                    op.p5 = 0; // Clear the marker
                    continue;
                }
                if let Some(Some(addr)) = self.labels.get(&op.p2) {
                    op.p2 = *addr;
                }
            }
        }
        Ok(())
    }
}

impl Default for SelectCompiler<'_> {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Compile a SELECT statement to VDBE opcodes
pub fn compile_select(select: &SelectStmt) -> Result<Vec<VdbeOp>> {
    let mut compiler = SelectCompiler::new();
    compiler.compile(select, &SelectDest::Output)
}

/// Compile a SELECT statement with custom destination
pub fn compile_select_to(select: &SelectStmt, dest: &SelectDest) -> Result<Vec<VdbeOp>> {
    let mut compiler = SelectCompiler::new();
    compiler.compile(select, dest)
}

impl<'a> SelectCompiler<'a> {
    /// Compile a SELECT statement for use in INSERT...SELECT context
    /// Returns ops without Init/Halt wrapper, suitable for inlining
    pub fn compile_for_insert(
        &mut self,
        select: &SelectStmt,
        dest: &SelectDest,
    ) -> Result<Vec<VdbeOp>> {
        // Handle WITH clause (CTEs) if present
        if let Some(with) = &select.with {
            self.process_with_clause(with)?;
        }

        // Compile the body directly without Init/Halt
        self.compile_body(&select.body, dest)?;

        // Handle ORDER BY and LIMIT if present (for simple cases)
        // For scalar subqueries this is usually not needed

        Ok(std::mem::take(&mut self.ops))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{ColumnRef, Literal, QualifiedName};

    #[test]
    fn test_compile_simple_select() {
        let select = SelectStmt::simple(vec![ResultColumn::Expr {
            expr: Expr::Literal(Literal::Integer(1)),
            alias: None,
        }]);

        let ops = compile_select(&select).unwrap();
        assert!(!ops.is_empty());

        // Should have at least Integer and ResultRow opcodes
        let has_integer = ops.iter().any(|op| op.opcode == Opcode::Integer);
        let has_result_row = ops.iter().any(|op| op.opcode == Opcode::ResultRow);
        assert!(has_integer);
        assert!(has_result_row);
    }

    #[test]
    fn test_compile_select_with_table() {
        let select = SelectStmt {
            with: None,
            body: SelectBody::Select(SelectCore {
                distinct: Distinct::All,
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    tables: vec![TableRef::Table {
                        name: QualifiedName::new("users"),
                        alias: None,
                        indexed_by: None,
                    }],
                }),
                where_clause: None,
                group_by: None,
                having: None,
                window: None,
            }),
            order_by: None,
            limit: None,
        };

        let ops = compile_select(&select).unwrap();

        // Should have OpenRead for the table
        let has_open_read = ops.iter().any(|op| op.opcode == Opcode::OpenRead);
        assert!(has_open_read);
    }

    #[test]
    fn test_compile_select_with_where() {
        let select = SelectStmt {
            with: None,
            body: SelectBody::Select(SelectCore {
                distinct: Distinct::All,
                columns: vec![ResultColumn::Expr {
                    expr: Expr::Column(ColumnRef {
                        database: None,
                        table: None,
                        column: "name".to_string(),
                        column_index: None,
                        source_text: None,
                    }),
                    alias: None,
                }],
                from: Some(FromClause {
                    tables: vec![TableRef::Table {
                        name: QualifiedName::new("users"),
                        alias: None,
                        indexed_by: None,
                    }],
                }),
                where_clause: Some(Box::new(Expr::Binary {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Column(ColumnRef {
                        database: None,
                        table: None,
                        column: "age".to_string(),
                        column_index: None,
                        source_text: None,
                    })),
                    right: Box::new(Expr::Literal(Literal::Integer(18))),
                })),
                group_by: None,
                having: None,
                window: None,
            }),
            order_by: None,
            limit: None,
        };

        let ops = compile_select(&select).unwrap();

        // Should have comparison opcode
        let has_gt = ops.iter().any(|op| op.opcode == Opcode::Gt);
        assert!(has_gt);
    }

    #[test]
    fn test_select_dest_variants() {
        let select = SelectStmt::simple(vec![ResultColumn::Expr {
            expr: Expr::Literal(Literal::Integer(42)),
            alias: None,
        }]);

        // Test Output destination
        let ops = compile_select_to(&select, &SelectDest::Output).unwrap();
        let has_result_row = ops.iter().any(|op| op.opcode == Opcode::ResultRow);
        assert!(has_result_row);

        // Test Exists destination
        let ops = compile_select_to(&select, &SelectDest::Exists { reg: 1 }).unwrap();
        let has_integer = ops
            .iter()
            .any(|op| op.opcode == Opcode::Integer && op.p1 == 1);
        assert!(has_integer);
    }

    #[test]
    fn test_compile_union() {
        let left = SelectBody::Select(SelectCore {
            distinct: Distinct::All,
            columns: vec![ResultColumn::Expr {
                expr: Expr::Literal(Literal::Integer(1)),
                alias: None,
            }],
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            window: None,
        });

        let right = SelectBody::Select(SelectCore {
            distinct: Distinct::All,
            columns: vec![ResultColumn::Expr {
                expr: Expr::Literal(Literal::Integer(2)),
                alias: None,
            }],
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            window: None,
        });

        let select = SelectStmt {
            with: None,
            body: SelectBody::Compound {
                op: CompoundOp::UnionAll,
                left: Box::new(left),
                right: Box::new(right),
            },
            order_by: None,
            limit: None,
        };

        let ops = compile_select(&select).unwrap();

        // Should have OpenEphemeral for union processing
        let has_ephemeral = ops.iter().any(|op| op.opcode == Opcode::OpenEphemeral);
        assert!(has_ephemeral);
    }

    #[test]
    fn test_compile_select_with_limit() {
        use crate::parser::ast::LimitClause;

        let select = SelectStmt {
            with: None,
            body: SelectBody::Select(SelectCore {
                distinct: Distinct::All,
                columns: vec![ResultColumn::Expr {
                    expr: Expr::Literal(Literal::Integer(1)),
                    alias: None,
                }],
                from: Some(FromClause {
                    tables: vec![TableRef::Table {
                        name: QualifiedName::new("test"),
                        alias: None,
                        indexed_by: None,
                    }],
                }),
                where_clause: None,
                group_by: None,
                having: None,
                window: None,
            }),
            order_by: None,
            limit: Some(LimitClause {
                limit: Box::new(Expr::Literal(Literal::Integer(10))),
                offset: None,
            }),
        };

        let ops = compile_select(&select).unwrap();

        // Should have Integer to load the limit
        let has_integer = ops
            .iter()
            .any(|op| op.opcode == Opcode::Integer && op.p1 == 10);
        assert!(
            has_integer,
            "Should have Integer opcode to load LIMIT value 10"
        );

        // Should have IfNot opcode for limit check
        let has_ifnot = ops.iter().any(|op| op.opcode == Opcode::IfNot);
        assert!(has_ifnot, "Should have IfNot opcode for limit check");

        // Should have AddImm to decrement limit counter
        let has_addimm = ops.iter().any(|op| op.opcode == Opcode::AddImm);
        assert!(has_addimm, "Should have AddImm opcode to decrement limit");
    }
}
