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
use crate::schema::{Affinity, Table};
use crate::vdbe::ops::{affinity as vdbe_affinity, Opcode, VdbeOp, P4};

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
            limit_counter_reg: None,
            offset_counter_reg: None,
            limit_done_label: None,
            order_by_terms: None,
            agg_final_regs: Vec::new(),
            agg_final_idx: 0,
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
        }
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
            limit_counter_reg: None,
            offset_counter_reg: None,
            limit_done_label: None,
            order_by_terms: None,
            agg_final_regs: Vec::new(),
            agg_final_idx: 0,
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
        }
    }

    /// Set column naming flags from PRAGMA settings
    pub fn set_column_name_flags(&mut self, short_column_names: bool, full_column_names: bool) {
        self.short_column_names = short_column_names;
        self.full_column_names = full_column_names;
    }

    /// Get the expanded column names after compilation
    pub fn column_names(&self) -> &[String] {
        &self.result_column_names
    }

    /// Compile a SELECT statement
    pub fn compile(&mut self, select: &SelectStmt, dest: &SelectDest) -> Result<Vec<VdbeOp>> {
        // Handle WITH clause (CTEs)
        if let Some(with) = &select.with {
            self.process_with_clause(with)?;
        }

        // Check for aggregates in ORDER BY without GROUP BY
        if let Some(order_by) = &select.order_by {
            let has_group_by = match &select.body {
                SelectBody::Select(core) => core.group_by.is_some(),
                SelectBody::Compound { .. } => false,
            };
            if !has_group_by {
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
        let (actual_dest, sorter_cursor, order_by_cols) = if let Some(order_by) = &select.order_by {
            if is_simple_aggregate {
                // Simple aggregate query - ignore ORDER BY
                (dest.clone(), None, None)
            } else {
                let sorter_cursor = self.alloc_cursor();
                let num_cols = order_by.len();
                // Open ephemeral table for sorting
                self.emit(
                    Opcode::OpenEphemeral,
                    sorter_cursor,
                    num_cols as i32,
                    0,
                    P4::Unused,
                );
                // Configure sort directions (0=ASC, 1=DESC)
                let sort_dirs: Vec<u8> = order_by
                    .iter()
                    .map(|t| if t.order == SortOrder::Desc { 1 } else { 0 })
                    .collect();
                self.emit(
                    Opcode::SorterConfig,
                    sorter_cursor,
                    0,
                    0,
                    P4::Blob(sort_dirs),
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

        // Compile the body with appropriate destination
        self.compile_body(&select.body, &actual_dest)?;

        // For compound SELECTs with ORDER BY, validate that ORDER BY terms match result columns
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

        // Resolve all labels
        self.resolve_labels()?;

        Ok(std::mem::take(&mut self.ops))
    }

    /// Process WITH clause
    fn process_with_clause(&mut self, with: &WithClause) -> Result<()> {
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
            // Open ephemeral table for sorting
            self.emit(
                Opcode::OpenEphemeral,
                sorter_cursor,
                num_cols as i32,
                0,
                P4::Unused,
            );
            // Configure sort directions (0=ASC, 1=DESC)
            let sort_dirs: Vec<u8> = order_by
                .iter()
                .map(|t| if t.order == SortOrder::Desc { 1 } else { 0 })
                .collect();
            self.emit(
                Opcode::SorterConfig,
                sorter_cursor,
                0,
                0,
                P4::Blob(sort_dirs),
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
        subcompiler.next_reg = self.next_reg;
        subcompiler.next_cursor = self.next_cursor;
        subcompiler.ctes = self.ctes.clone();
        subcompiler.recursive_ctes = self.recursive_ctes.clone();
        subcompiler.cte_cursors = self.cte_cursors.clone();
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

        // Inline the subquery ops (skip Halt)
        // Adjust jump addresses by the current offset
        // Mark inlined jump ops so resolve_labels doesn't reprocess them
        let offset = self.ops.len() as i32;
        for mut op in subquery_ops {
            if op.opcode != Opcode::Halt {
                // Adjust P2 for jump instructions
                // The subcompiler already resolved labels, so P2 contains actual addresses
                // We need to adjust by offset and mark as resolved
                if op.opcode.is_jump() {
                    op.p2 += offset;
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

        let offset = self.ops.len() as i32;
        for mut op in recursive_ops {
            if op.opcode != Opcode::Halt {
                if op.opcode.is_jump() {
                    op.p2 += offset;
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

        // Process FROM clause - open cursors
        if let Some(from) = &core.from {
            self.compile_from_clause(from)?;
        }

        // Generate the main query loop
        if self.has_window_functions {
            self.compile_with_window_functions(core, dest)
        } else if self.has_aggregates && core.group_by.is_some() {
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

        // Merge join conditions (from NATURAL/USING/ON) with WHERE clause
        // This follows SQLite's approach of adding join conditions to pWhere
        let remaining_where = self.merge_join_conditions(original_where);

        // Check for constant-false WHERE clause (e.g., WHERE 0)
        // In this case, skip all loop generation and return immediately - no rows match
        if self.is_constant_false_where(remaining_where.as_ref()) {
            // Generate no rows - jump to end immediately
            // The result is empty, no output needed
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

        // Build iteration order - use optimizer's order if available, else FROM clause order
        // The optimizer reorders tables by cost (cheapest first), so info.levels[0] is the
        // table to scan first (outer loop), and the last level is the innermost loop.
        let iteration_order: Vec<usize> = if let Some(info) = &where_info {
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

        // Now emit the Rewind/loop structure (or index seek structure based on plan)
        // Iterate in optimizer order: iteration_order[loop_pos] gives the FROM clause index
        for (loop_pos, &from_idx) in iteration_order.iter().enumerate() {
            let cursor = table_cursors[from_idx];

            // Handle FTS3 filter if applicable
            if let Some(filter) = &fts3_filter {
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
            }

            // For the outermost table, jump to done_all on empty
            // For inner tables, jump to next_outer (advance outer cursor)
            let skip_label = self.alloc_label();

            // Check if we have a query plan for this table
            // levels are already in optimizer order, so loop_pos indexes directly into levels
            let plan = where_info
                .as_ref()
                .and_then(|info| info.levels.get(loop_pos))
                .map(|level| &level.plan);

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
                    } else {
                        // No start bound - start from beginning
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

                                    // Insert as row with explicit rowid
                                    self.emit(
                                        Opcode::InsertInt,
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
        let where_skip_label = if let Some(info) = &where_info {
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

        // Close cursors (including index cursors)
        for (i, cursor) in table_cursors.iter().enumerate() {
            // Close index cursor first if we used an index scan
            let (is_index_scan, index_cursor, _, _, _) = scan_info
                .get(i)
                .copied()
                .unwrap_or((false, None, 0, 0, false));
            if is_index_scan {
                if let Some(idx_cursor) = index_cursor {
                    self.emit(Opcode::Close, idx_cursor, 0, 0, P4::Unused);
                }
            }
            // Close table cursor
            self.emit(Opcode::Close, *cursor, 0, 0, P4::Unused);
        }

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
        // Create a WindowCompiler to analyze and compile window functions
        let mut window_compiler = WindowCompiler::new(self.next_reg, self.next_cursor);

        // Collect window function information
        let window_funcs = window_compiler.collect_window_functions(core)?;

        if window_funcs.is_empty() {
            // No window functions after all, fall back to regular compilation
            return self.compile_simple_select(core, dest);
        }

        // Group by window specification
        let windows = window_compiler.group_by_window(window_funcs)?;

        // Update our register/cursor counters
        self.next_reg = window_compiler.next_reg();
        self.next_cursor = window_compiler.next_cursor();

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

        // Generate Rewind for each table cursor
        let mut rewind_labels: Vec<i32> = Vec::with_capacity(table_cursors.len());
        for cursor in &table_cursors {
            let label = self.alloc_label();
            self.emit(Opcode::Rewind, *cursor, label, 0, P4::Unused);
            rewind_labels.push(label);
        }

        // Use label to avoid collision with resolve_labels
        let loop_start_label = self.alloc_label();
        self.resolve_label(loop_start_label, self.current_addr());

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

        // Next loop
        for (i, cursor) in table_cursors.iter().enumerate().rev() {
            self.emit(Opcode::Next, *cursor, loop_start_label, 0, P4::Unused);
            self.resolve_label(rewind_labels[i], self.current_addr());
        }

        // Step 3: Now process window functions
        // For each window group, sort by PARTITION BY + ORDER BY, then compute
        let _window_ops = window_compiler.take_ops();
        window_compiler.compile_window_functions(&windows, result_base, result_count)?;
        let window_ops = window_compiler.take_ops();

        // Add window operations to our ops
        for op in window_ops {
            self.ops.push(op);
        }

        // Step 4: Read from ephemeral table and output with window results
        let done_label = self.alloc_label();
        self.emit(Opcode::Rewind, eph_cursor, done_label, 0, P4::Unused);

        let read_loop = self.current_addr();

        // Read column values
        for i in 0..result_count {
            self.emit(
                Opcode::Column,
                eph_cursor,
                i as i32,
                result_base + i as i32,
                P4::Unused,
            );
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
                                self.emit(
                                    Opcode::Column,
                                    table.cursor,
                                    col_idx as i32,
                                    reg,
                                    P4::Unused,
                                );
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
                                for (col_idx, _) in schema_table.columns.iter().enumerate() {
                                    let reg = self.alloc_reg();
                                    self.emit(
                                        Opcode::Column,
                                        table.cursor,
                                        col_idx as i32,
                                        reg,
                                        P4::Unused,
                                    );
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
        // Initialize aggregate accumulators
        let agg_regs = self.init_aggregates(&core.columns)?;

        // Collect table cursors to avoid borrow checker issues
        let table_cursors: Vec<i32> = self
            .tables
            .iter()
            .skip(self.outer_tables_boundary)
            .map(|t| t.cursor)
            .collect();

        // Generate Rewind for each table cursor
        let mut rewind_labels: Vec<i32> = Vec::with_capacity(table_cursors.len());
        for cursor in &table_cursors {
            let label = self.alloc_label();
            self.emit(Opcode::Rewind, *cursor, label, 0, P4::Unused);
            rewind_labels.push(label);
        }

        // Use label to avoid collision with resolve_labels
        let loop_start_label = self.alloc_label();
        self.resolve_label(loop_start_label, self.current_addr());

        // Evaluate WHERE clause
        let where_skip_label = if let Some(where_expr) = &core.where_clause {
            let label = self.alloc_label();
            self.compile_where_condition(where_expr, label)?;
            Some(label)
        } else {
            None
        };

        // Accumulate aggregates
        self.accumulate_aggregates(&core.columns, &agg_regs)?;

        // WHERE skip target
        if let Some(label) = where_skip_label {
            self.resolve_label(label, self.current_addr());
        }

        // Next loop
        for (i, cursor) in table_cursors.iter().enumerate().rev() {
            self.emit(Opcode::Next, *cursor, loop_start_label, 0, P4::Unused);
            self.resolve_label(rewind_labels[i], self.current_addr());
        }

        // Finalize aggregates
        let result_regs = self.finalize_aggregates(&core.columns, &agg_regs)?;

        // Output single row
        self.output_row(dest, result_regs.0, result_regs.1)?;

        // Close cursors
        for cursor in &table_cursors {
            self.emit(Opcode::Close, *cursor, 0, 0, P4::Unused);
        }

        Ok(())
    }

    /// Compile SELECT with GROUP BY
    fn compile_grouped_aggregate(&mut self, core: &SelectCore, dest: &SelectDest) -> Result<()> {
        let group_by = core.group_by.as_ref().unwrap();

        // Count total columns needed in sorter: group columns + aggregate arguments
        let num_group_cols = group_by.len();
        let num_agg_args = self.count_aggregate_args(&core.columns);
        let total_sorter_cols = num_group_cols + num_agg_args;

        // Open sorter for grouping
        let sorter_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenEphemeral,
            sorter_cursor,
            total_sorter_cols as i32,
            0,
            P4::Unused,
        );

        // Collect table cursors to avoid borrow checker issues
        let table_cursors: Vec<i32> = self.tables.iter().map(|t| t.cursor).collect();

        // Generate Rewind for each table cursor
        let mut rewind_labels: Vec<i32> = Vec::with_capacity(table_cursors.len());
        for cursor in &table_cursors {
            let label = self.alloc_label();
            self.emit(Opcode::Rewind, *cursor, label, 0, P4::Unused);
            rewind_labels.push(label);
        }

        // Use label to avoid collision with resolve_labels
        let loop_start_label = self.alloc_label();
        self.resolve_label(loop_start_label, self.current_addr());

        // Evaluate WHERE clause
        let where_skip_label = if let Some(where_expr) = &core.where_clause {
            let label = self.alloc_label();
            self.compile_where_condition(where_expr, label)?;
            Some(label)
        } else {
            None
        };

        // Evaluate GROUP BY expressions and store in sorter
        let group_regs = self.compile_expressions(group_by)?;

        // Evaluate aggregate arguments
        let agg_arg_regs = self.compile_aggregate_args(&core.columns)?;

        // Make record and insert into sorter
        let total_cols = group_regs.1 + agg_arg_regs.1;
        let record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            group_regs.0,
            total_cols as i32,
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

        // WHERE skip target
        if let Some(label) = where_skip_label {
            self.resolve_label(label, self.current_addr());
        }

        // Next loop (in reverse order)
        for (i, cursor) in table_cursors.iter().enumerate().rev() {
            self.emit(Opcode::Next, *cursor, loop_start_label, 0, P4::Unused);
            self.resolve_label(rewind_labels[i], self.current_addr());
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

        // Initialize aggregates
        let agg_regs = self.init_aggregates(&core.columns)?;

        // Previous group key registers
        let prev_group_regs = self.alloc_regs(num_group_cols);
        self.emit(
            Opcode::Null,
            0,
            prev_group_regs,
            prev_group_regs + num_group_cols as i32 - 1,
            P4::Unused,
        );

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

        // Compare with previous group
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

        // New group - output previous group if not first
        // Skip output if prev_group is NULL (first group)
        // Use IsNull instead of IfNot because 0 is a valid group key but IfNot treats 0 as falsy
        let first_group_label = self.alloc_label();
        self.emit(
            Opcode::IsNull,
            prev_group_regs,
            first_group_label,
            0,
            P4::Unused,
        );

        // Finalize and output previous group
        // Save column names length - finalize_aggregates_with_group adds to result_column_names
        // but we only want the names added once (first iteration only)
        let saved_result_column_names_prev = self.result_column_names.len();
        let result_regs = self.finalize_aggregates_with_group(
            &core.columns,
            &agg_regs,
            Some(group_by),
            prev_group_regs,
        )?;
        // Only keep names from first group output (truncate to saved length unless this is first)
        if saved_result_column_names_prev > 0 {
            self.result_column_names
                .truncate(saved_result_column_names_prev);
        }

        // HAVING clause
        if let Some(having) = &core.having {
            let skip_output_label = self.alloc_label();
            self.compile_where_condition(having, skip_output_label)?;
            self.output_row(dest, result_regs.0, result_regs.1)?;
            self.resolve_label(skip_output_label, self.current_addr());
        } else {
            self.output_row(dest, result_regs.0, result_regs.1)?;
        }

        self.resolve_label(first_group_label, self.current_addr());

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

        self.resolve_label(same_group_label, self.current_addr());

        // Accumulate current row into aggregates
        let agg_col_start = num_group_cols;
        self.accumulate_from_sorter(sorter_cursor, &core.columns, &agg_regs, agg_col_start)?;

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
            Some(group_by),
            prev_group_regs,
        )?;
        // Restore column names (don't double-count for final group output)
        self.result_column_names.truncate(saved_result_column_names);
        if let Some(having) = &core.having {
            let skip_output_label = self.alloc_label();
            self.compile_where_condition(having, skip_output_label)?;
            self.output_row(dest, result_regs.0, result_regs.1)?;
            self.resolve_label(skip_output_label, self.current_addr());
        } else {
            self.output_row(dest, result_regs.0, result_regs.1)?;
        }

        self.resolve_label(sort_done_label, self.current_addr());

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
        self.tables.clear();
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
                // Just add right side to same table
                self.compile_body(right, &left_dest)?;
            }
            CompoundOp::Union => {
                // Clear tables before compiling right side
                self.tables.clear();
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
                if let Some(schema) = self.schema {
                    if let Some(view) = schema.views.get(&table_name_lower) {
                        let view_select = (*view.select).clone();
                        let view_alias = item.alias.clone().unwrap_or_else(|| table_name.clone());

                        // Compile view's SELECT as a subquery into ephemeral table
                        let subquery_col_names =
                            self.compile_subquery_to_ephemeral(&view_select, cursor, None)?;

                        self.tables.push(TableInfo {
                            name: view_alias,
                            table_name: String::new(),
                            cursor,
                            schema_table: None,
                            is_subquery: true,
                            join_type: item.join_type,
                            subquery_columns: Some(subquery_col_names),
                        });
                        return Ok(());
                    }
                }

                // Look up table in schema if available
                let schema_table = self.lookup_table_schema(&table_name_lower);

                // Emit OpenRead for the table
                self.emit(Opcode::OpenRead, cursor, 0, 0, P4::Text(table_name.clone()));

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
        use crate::schema::Column;

        if table_name_lower == "sqlite_master" {
            // Create a virtual schema for sqlite_master
            Some(std::sync::Arc::new(Table {
                name: "sqlite_master".to_string(),
                db_idx: 0,
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
            }))
        } else if let Some(schema) = self.schema {
            schema.table(table_name_lower).map(|t| t.clone())
        } else {
            None
        }
    }

    fn table_name_matches(table: &TableInfo, name: &str) -> bool {
        table.name.eq_ignore_ascii_case(name) || table.table_name.eq_ignore_ascii_case(name)
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
    /// If either operand is a column with numeric affinity (INTEGER, REAL, NUMERIC),
    /// returns NUMERIC affinity to enable type coercion.
    /// Otherwise returns BLOB (0) for strict type ordering.
    fn get_comparison_affinity(&self, left: &Expr, right: &Expr) -> u16 {
        let left_affinity = self.get_expr_affinity(left);
        let right_affinity = self.get_expr_affinity(right);

        // If either side has numeric affinity, use NUMERIC for coercion
        if Self::is_numeric_affinity(left_affinity) || Self::is_numeric_affinity(right_affinity) {
            vdbe_affinity::NUMERIC
        } else {
            vdbe_affinity::BLOB
        }
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

                    // Check if index covers all table columns
                    let num_table_cols = schema_table.columns.len();
                    let is_covering = index_cols.len() >= num_table_cols
                        && (0..num_table_cols as i32).all(|c| index_cols.contains(&c));

                    planner.add_index(
                        table_idx,
                        IndexInfo {
                            name: index.name.clone(),
                            columns: index_cols,
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
                    });
                    let right_expr = Expr::Column(ColumnRef {
                        database: None,
                        table: Some(current_table.name.clone()),
                        column: col_name,
                        column_index: None,
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

                for col_name in using_cols {
                    // Generate: prev_table.col = current_table.col
                    let left_expr = Expr::Column(ColumnRef {
                        database: None,
                        table: Some(self.tables[i - 1].name.clone()),
                        column: col_name.clone(),
                        column_index: None,
                    });
                    let right_expr = Expr::Column(ColumnRef {
                        database: None,
                        table: Some(current_table.name.clone()),
                        column: col_name.clone(),
                        column_index: None,
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
                // on_expr is &Box<Expr>, so we need to deref twice to get &Expr
                self.join_conditions.push((**on_expr).clone());
            }
        }

        Ok(())
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
                let cursor = self.alloc_cursor();
                let table_name = &name.name;
                let table_name_lower = table_name.to_lowercase();

                // Look up table in schema if available
                let schema_table = if table_name_lower == "sqlite_master" {
                    // Create a virtual schema for sqlite_master
                    use crate::schema::{Affinity, Column, Table};
                    Some(std::sync::Arc::new(Table {
                        name: "sqlite_master".to_string(),
                        db_idx: 0,
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
                } else if let Some(schema) = self.schema {
                    // First check if this is a view
                    if let Some(view) = schema.views.get(&table_name_lower) {
                        // Expand view as subquery
                        let view_select = (*view.select).clone();
                        let view_alias = alias.clone().unwrap_or_else(|| table_name.clone());

                        // Compile view's SELECT as a subquery
                        let cursor = self.alloc_cursor();
                        self.emit(Opcode::OpenEphemeral, cursor, 0, 0, P4::Unused);

                        let subquery_dest = SelectDest::EphemTable { cursor };
                        let mut subcompiler = SelectCompiler::with_schema(schema);
                        subcompiler.next_reg = self.next_reg;
                        subcompiler.next_cursor = self.next_cursor;
                        subcompiler
                            .set_column_name_flags(self.short_column_names, self.full_column_names);
                        let subquery_ops = subcompiler.compile(&view_select, &subquery_dest)?;

                        // Capture view's result column names for * expansion
                        let subquery_col_names = subcompiler.result_column_names.clone();

                        // Inline the subquery ops
                        for op in subquery_ops {
                            if op.opcode != Opcode::Halt {
                                self.ops.push(op);
                            }
                        }

                        self.next_reg = subcompiler.next_reg;
                        self.next_cursor = subcompiler.next_cursor;

                        self.tables.push(TableInfo {
                            name: view_alias,
                            table_name: String::new(),
                            cursor,
                            schema_table: None,
                            is_subquery: true,
                            join_type,
                            subquery_columns: Some(subquery_col_names),
                        });
                        return Ok(());
                    }

                    // Check if table exists (but not for sqlite_ internal tables)
                    if !table_name_lower.starts_with("sqlite_")
                        && !schema.tables.contains_key(&table_name_lower)
                    {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("no such table: {}", table_name),
                        ));
                    }
                    schema.tables.get(&table_name_lower).cloned()
                } else {
                    None
                };

                // Open the table (read mode)
                self.emit(Opcode::OpenRead, cursor, 0, 0, P4::Text(table_name.clone()));

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

                // Inline the subquery ops
                for op in subquery_ops {
                    if op.opcode != Opcode::Halt {
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
    /// SQLite allows referencing result column aliases in WHERE as an extension
    fn prescan_result_aliases(&mut self, columns: &[ResultColumn]) {
        self.alias_expressions.clear();
        for col in columns {
            if let ResultColumn::Expr { expr, alias } = col {
                if let Some(alias_name) = alias {
                    self.alias_expressions
                        .insert(alias_name.to_lowercase(), expr.clone());
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
                                // Check if this is the INTEGER PRIMARY KEY column (rowid alias)
                                // If so, emit Rowid instead of Column since IPK isn't stored in the table
                                if let Some(ipk_idx) = schema_table.rowid_alias_column() {
                                    if col_idx == ipk_idx {
                                        self.emit(Opcode::Rowid, table.cursor, reg, 0, P4::Unused);
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
                                    // Check if this is the INTEGER PRIMARY KEY column (rowid alias)
                                    if let Some(ipk_idx) = schema_table.rowid_alias_column() {
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
    fn expr_to_name(&self, expr: &Expr, _index: usize) -> String {
        match expr {
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
                    // Both OFF: use real table name (like full_column_names)
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

        // Collect non-consumed, non-virtual terms
        let mut filter_terms: Vec<&WhereTerm> = where_info
            .terms
            .iter()
            .filter(|term| {
                !consumed_terms.contains(&term.idx) && !term.flags.contains(WhereTermFlags::VIRTUAL)
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
                    if let Some(alias_expr) = self.alias_expressions.get(&alias_lower).cloned() {
                        // Don't recurse if the alias expression is just the same column reference
                        let is_same_column = matches!(&alias_expr, Expr::Column(c)
                            if c.table.is_none() && c.column.eq_ignore_ascii_case(&col_ref.column));
                        if !is_same_column {
                            return self.compile_expr(&alias_expr, dest_reg);
                        }
                    }
                }

                // Find the table and column index
                let (cursor, col_idx) = if let Some(table) = &col_ref.table {
                    // Resolve table name with scoping (local first, then outer)
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
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("ambiguous column name: {}.{}", table, col_ref.column),
                        ));
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
                        for (table_idx, tinfo) in self
                            .tables
                            .iter()
                            .enumerate()
                            .take(self.outer_tables_boundary)
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

                if col_idx < 0 {
                    self.emit(Opcode::Rowid, cursor, dest_reg, 0, P4::Unused);
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
                        self.emit(Opcode::Negative, dest_reg, dest_reg, 0, P4::Unused);
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
                // MIN/MAX with multiple args are scalar functions
                let is_multi_arg_min_max =
                    matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count > 1;
                let is_aggregate = !is_multi_arg_min_max
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
                    );

                // Validate aggregate function argument counts
                if is_aggregate {
                    let (min_args, max_args) = match name_upper.as_str() {
                        "COUNT" => (0, 1),
                        "SUM" | "AVG" | "TOTAL" | "MIN" | "MAX" => (1, 1),
                        "GROUP_CONCAT" => (1, 2),
                        _ => (0, 255),
                    };
                    if arg_count < min_args || arg_count > max_args {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("wrong number of arguments to function {}()", func_call.name),
                        ));
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
                            format!("no such function: {}", func_call.name.to_uppercase()),
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

                // Set boundary so subquery only loops over its own tables, not outer tables
                self.outer_tables_boundary = outer_tables_len;

                // Initialize result to NULL in case subquery returns no rows
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);

                // Compile the subquery body with Set destination
                // Outer tables remain available for correlated column references
                let sub_dest = SelectDest::Set { reg: dest_reg };
                self.compile_body(&select.body, &sub_dest)?;

                // Restore outer query state - remove subquery's tables and reset boundary
                self.tables.truncate(outer_tables_len);
                self.outer_tables_boundary = saved_boundary;
                self.has_aggregates = saved_has_agg;
                self.has_window_functions = saved_has_window;
                self.result_column_names.truncate(saved_result_names_len);
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
                ..
            } => {
                // Compile LIKE/GLOB expression
                let text_reg = self.alloc_reg();
                let pattern_reg = self.alloc_reg();
                self.compile_expr(text_expr, text_reg)?;
                self.compile_expr(pattern, pattern_reg)?;

                let opcode = match op {
                    crate::parser::ast::LikeOp::Like => Opcode::Like,
                    crate::parser::ast::LikeOp::Glob => Opcode::Glob,
                    _ => Opcode::Like, // Fallback for Regexp/Match
                };

                // Like opcode: P1=text, P2=result, P3=pattern
                self.emit(opcode, text_reg, dest_reg, pattern_reg, P4::Unused);

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
                            let end_label = self.alloc_label();

                            for value in values {
                                let cmp_reg = self.alloc_reg();
                                self.compile_expr(value, cmp_reg)?;
                                // If equal, jump to match
                                self.emit(Opcode::Eq, val_reg, match_label, cmp_reg, P4::Unused);
                            }

                            // No match found
                            self.emit(
                                Opcode::Integer,
                                if *negated { 1 } else { 0 },
                                dest_reg,
                                0,
                                P4::Unused,
                            );
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
                        // Compile IN subquery using a fresh compilation context
                        // to avoid cursor conflicts with outer query
                        let subq_cursor = self.alloc_cursor();
                        self.emit(Opcode::OpenEphemeral, subq_cursor, 1, 0, P4::Unused);

                        // Keep outer tables for correlation - save counts to restore later
                        let outer_tables_len = self.tables.len();
                        let saved_boundary = self.outer_tables_boundary;
                        let saved_has_agg = self.has_aggregates;
                        let saved_has_window = self.has_window_functions;
                        let saved_order_by = std::mem::take(&mut self.order_by_terms);
                        let saved_limit_reg = self.limit_counter_reg.take();
                        let saved_offset_reg = self.offset_counter_reg.take();
                        let saved_limit_done = self.limit_done_label.take();
                        let saved_result_names_len = self.result_column_names.len();

                        // Set boundary so subquery only loops over its own tables, not outer tables
                        self.outer_tables_boundary = outer_tables_len;

                        // Compile full subquery (including ORDER BY/LIMIT) to fill ephemeral table
                        // Outer tables remain available for correlated column references
                        let subq_dest = SelectDest::EphemTable {
                            cursor: subq_cursor,
                        };
                        self.compile_subselect(subquery, &subq_dest)?;

                        // Restore outer query state - remove subquery's tables and reset boundary
                        self.tables.truncate(outer_tables_len);
                        self.outer_tables_boundary = saved_boundary;
                        self.has_aggregates = saved_has_agg;
                        self.has_window_functions = saved_has_window;
                        self.order_by_terms = saved_order_by;
                        self.limit_counter_reg = saved_limit_reg;
                        self.offset_counter_reg = saved_offset_reg;
                        self.limit_done_label = saved_limit_done;
                        self.result_column_names.truncate(saved_result_names_len);

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
                let val_reg = self.alloc_reg();
                let low_reg = self.alloc_reg();
                let high_reg = self.alloc_reg();

                self.compile_expr(val_expr, val_reg)?;
                self.compile_expr(low, low_reg)?;
                self.compile_expr(high, high_reg)?;

                let fail_label = self.alloc_label();
                let end_label = self.alloc_label();

                // Determine affinity for comparisons
                // Use the combined affinity from val_expr, low, and high
                let low_affinity = self.get_comparison_affinity(val_expr, low);
                let high_affinity = self.get_comparison_affinity(val_expr, high);

                // Check val >= low (fail if val < low)
                // Lt P1 P2 P3 jumps if r[P3] < r[P1], so P1=low, P3=val
                self.emit_with_p5(
                    Opcode::Lt,
                    low_reg,
                    fail_label,
                    val_reg,
                    P4::Unused,
                    low_affinity,
                );
                // Check val <= high (fail if val > high)
                // Gt P1 P2 P3 jumps if r[P3] > r[P1], so P1=high, P3=val
                self.emit_with_p5(
                    Opcode::Gt,
                    high_reg,
                    fail_label,
                    val_reg,
                    P4::Unused,
                    high_affinity,
                );

                // Success - in range
                self.emit(
                    Opcode::Integer,
                    if *negated { 0 } else { 1 },
                    dest_reg,
                    0,
                    P4::Unused,
                );
                self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);

                // Fail - not in range
                self.resolve_label(fail_label, self.current_addr());
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

        // Decode all columns into registers
        let all_base_reg = self.alloc_regs(total_cols);
        self.emit(
            Opcode::DecodeRecord,
            record_reg,
            all_base_reg,
            total_cols as i32,
            P4::Unused,
        );

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
        self.limit_counter_reg = Some(limit_reg);

        // Allocate label to jump to when limit exhausted
        self.limit_done_label = Some(self.alloc_label());

        if let Some(offset) = &limit.offset {
            let offset_reg = self.alloc_reg();
            self.compile_expr(offset, offset_reg)?;
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
                            // These should reference result columns, not be literal values
                            if let Expr::Literal(Literal::Integer(col_idx)) = &term.expr {
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

                            // For compound selects, check if ORDER BY references a result column name
                            // (e.g., ORDER BY x where x is an alias)
                            if self.is_compound {
                                if let Expr::Column(col_ref) = &term.expr {
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

                            self.compile_expr(&term.expr, key_base_reg + i as i32)?;
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
            // Column reference (simple identifier or table.column) - always allowed
            // SQLite allows referencing column names from any part of the UNION
            Expr::Column(_) => true,
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
                if self.expr_has_aggregate(expr) {
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
        let base_reg = self.next_reg;
        let mut count = 0;
        let mut agg_idx = 0;

        for col in columns {
            let dest_reg = self.alloc_reg();
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
                        self.compile_expr(expr, dest_reg)?;
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
                    self.compile_expr(expr, dest_reg)?;
                }
            }
            count += 1;
        }

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
    fn count_aggregate_args(&self, columns: &[ResultColumn]) -> usize {
        let mut count = 0;
        for col in columns {
            if let ResultColumn::Expr { expr, .. } = col {
                if let Expr::Function(func_call) = expr {
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        count += exprs.len();
                    }
                }
            }
        }
        count
    }

    fn compile_aggregate_args(&mut self, columns: &[ResultColumn]) -> Result<(i32, usize)> {
        let base_reg = self.next_reg;
        let mut count = 0;
        for col in columns {
            if let ResultColumn::Expr { expr, .. } = col {
                if let Expr::Function(func_call) = expr {
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        for arg in exprs {
                            let reg = self.alloc_reg();
                            self.compile_expr(arg, reg)?;
                            count += 1;
                        }
                    }
                }
            }
        }
        Ok((base_reg, count))
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
                                self.emit(
                                    Opcode::Column,
                                    cursor,
                                    col_idx as i32,
                                    arg_reg,
                                    P4::Unused,
                                );
                                col_idx += 1;
                            }
                        }
                        // Emit AggStep with: P1=argc, P2=arg_base, P3=accumulator
                        self.emit(
                            Opcode::AggStep,
                            argc as i32,
                            arg_base,
                            agg_regs[agg_idx],
                            P4::Text(name_upper),
                        );
                        agg_idx += 1;
                    }
                }
            }
        }
        Ok(())
    }

    fn compile_expressions(&mut self, exprs: &[Expr]) -> Result<(i32, usize)> {
        let base_reg = self.next_reg;
        for expr in exprs {
            let reg = self.alloc_reg();
            self.compile_expr(expr, reg)?;
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

        // Decode the record
        let out_base_reg = self.alloc_regs(col_count);
        self.emit(
            Opcode::DecodeRecord,
            sorter_data_reg,
            out_base_reg,
            col_count as i32,
            P4::Unused,
        );

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
