//! SELECT statement code generation
//!
//! This module generates VDBE opcodes for SELECT statements.
//! Corresponds to SQLite's select.c.

use std::collections::HashMap;

use crate::error::{Error, ErrorCode, Result};
use crate::executor::window::{select_has_window_functions, WindowCompiler};
use crate::parser::ast::{
    BinaryOp, ColumnRef, CompoundOp, Distinct, Expr, FromClause, JoinFlags, JoinType, LikeOp,
    LimitClause, Literal, OrderingTerm, ResultColumn, SelectBody, SelectCore, SelectStmt,
    SortOrder, TableRef, WithClause,
};
use crate::schema::{Affinity, Table};
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

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
    pub schema_table: Option<std::sync::Arc<Table>>,
    /// Is this from a subquery?
    pub is_subquery: bool,
    /// Join type (for joined tables)
    pub join_type: JoinType,
    /// Subquery result column names (for * expansion)
    pub subquery_columns: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
struct Fts3MatchFilter {
    cursor: i32,
    pattern: Expr,
}

fn filter_literal_text(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(Literal::String(text)) => Some(text.clone()),
        _ => None,
    }
}

fn is_rowid_alias(name: &str) -> bool {
    name.eq_ignore_ascii_case("rowid")
        || name.eq_ignore_ascii_case("_rowid_")
        || name.eq_ignore_ascii_case("oid")
}

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
    ctes: HashMap<String, SelectStmt>,
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
        }
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
            self.ctes.insert(cte.name.clone(), (*cte.query).clone());
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
        let table_cursors: Vec<i32> = self.tables.iter().map(|t| t.cursor).collect();
        let table_join_types: Vec<JoinType> = self.tables.iter().map(|t| t.join_type).collect();

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

        // Now emit the Rewind/loop structure
        for (i, cursor) in table_cursors.iter().enumerate() {
            // Handle FTS3 filter if applicable
            if let Some(filter) = &fts3_filter {
                if filter.cursor == *cursor {
                    match &filter.pattern {
                        Expr::Literal(Literal::String(text)) => {
                            self.emit(Opcode::VFilter, *cursor, 0, 0, P4::Text(text.clone()));
                        }
                        expr => {
                            let reg = self.alloc_reg();
                            self.compile_expr(expr, reg)?;
                            self.emit(Opcode::VFilter, *cursor, reg, 0, P4::Unused);
                        }
                    }
                }
            }

            // For the outermost table, jump to done_all on empty
            // For inner tables, jump to next_outer (advance outer cursor)
            let skip_label = self.alloc_label();
            self.emit(Opcode::Rewind, *cursor, skip_label, 0, P4::Unused);
            next_labels.push(skip_label);

            // Mark the loop start for this level
            let loop_label = self.alloc_label();
            self.resolve_label(loop_label, self.current_addr());
            loop_labels.push(loop_label);

            // Initialize found_match for the NEXT table (if it's an outer join)
            // This must be INSIDE the current loop (after loop_label) so it resets on each iteration
            if i + 1 < table_cursors.len() {
                if let Some(reg) = found_match_regs[i + 1] {
                    self.emit(Opcode::Integer, 0, reg, 0, P4::Unused);
                }
            }
        }

        // Inner loop start is the innermost loop label
        let loop_start_label = *loop_labels.last().unwrap_or(&self.alloc_label());

        // Evaluate WHERE clause
        let where_skip_label = if let Some(where_expr) = remaining_where.as_ref() {
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
        for i in (0..table_cursors.len()).rev() {
            let cursor = table_cursors[i];
            let loop_label = loop_labels[i];

            // Next jumps back to this table's loop start
            self.emit(Opcode::Next, cursor, loop_label, 0, P4::Unused);

            // For outer joins: if no match was found, emit null row
            // Both empty Rewind and exhausted Next come here
            if let Some(found_match_reg) = found_match_regs[i] {
                // Resolve the skip label HERE so Rewind jumps to check_match, not past it
                self.resolve_label(next_labels[i], self.current_addr());

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
                let null_result_regs = self.compile_result_columns(&core.columns)?;

                // Output the null row
                self.output_row(dest, null_result_regs.0, null_result_regs.1)?;

                // Skip null output target
                self.resolve_label(skip_null_output, self.current_addr());
            } else {
                // Non-outer join: resolve skip label after Next
                self.resolve_label(next_labels[i], self.current_addr());
            }
        }

        // Close cursors
        for cursor in &table_cursors {
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
                    let tables_snapshot: Vec<_> = self.tables.clone();
                    for table in &tables_snapshot {
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
        let first_group_label = self.alloc_label();
        self.emit(
            Opcode::IfNot,
            prev_group_regs,
            first_group_label,
            0,
            P4::Unused,
        );

        // Finalize and output previous group
        let result_regs = self.finalize_aggregates_with_group(
            &core.columns,
            &agg_regs,
            Some(group_by),
            prev_group_regs,
        )?;

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
        let result_regs = self.finalize_aggregates_with_group(
            &core.columns,
            &agg_regs,
            Some(group_by),
            prev_group_regs,
        )?;
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

        // Create ephemeral table for results
        let result_cursor = self.alloc_cursor();
        self.emit(Opcode::OpenEphemeral, result_cursor, 0, 0, P4::Unused);

        // Compile left side into ephemeral table
        // Clear tables and result column names to avoid accumulating from parent context
        self.tables.clear();
        self.result_column_names.clear();
        let left_dest = SelectDest::EphemTable {
            cursor: result_cursor,
        };
        self.compile_body(left, &left_dest)?;

        // Track column count from left side for output
        self.compound_column_count = self.result_column_names.len();
        // Save the left side's column names (right side will add more but we only want left's names)
        let saved_column_names = self.result_column_names.clone();

        // Track if we need sorted output (UNION, INTERSECT, EXCEPT all return sorted results)
        let needs_sorted_output = !matches!(op, CompoundOp::UnionAll);

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

                self.intersect_tables(result_cursor, right_cursor)?;
                self.emit(Opcode::Close, right_cursor, 0, 0, P4::Unused);
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

                self.except_tables(result_cursor, right_cursor)?;
                self.emit(Opcode::Close, right_cursor, 0, 0, P4::Unused);
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

        // Output results from ephemeral table
        // UNION/INTERSECT/EXCEPT return sorted results, UNION ALL does not
        if needs_sorted_output {
            self.output_ephemeral_table_sorted(result_cursor, dest)?;
        } else {
            self.output_ephemeral_table(result_cursor, dest)?;
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
                let cursor = self.alloc_cursor();
                let table_name = &name.name;
                let table_name_lower = table_name.to_lowercase();

                // Look up table in schema if available
                let schema_table = self.lookup_table_schema(&table_name_lower);

                // Emit OpenRead for the table
                if schema_table.is_some() {
                    self.emit(Opcode::OpenRead, cursor, 0, 0, P4::Text(table_name.clone()));
                } else {
                    // CTE or unknown table - will be handled elsewhere
                    self.emit(Opcode::OpenRead, cursor, 0, 0, P4::Text(table_name.clone()));
                }

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

                // Compile subquery to ephemeral table
                let (subquery_ops, subquery_col_names) = {
                    let mut sub_compiler = SelectCompiler::new();
                    if let Some(schema) = self.schema {
                        sub_compiler.schema = Some(schema);
                    }
                    sub_compiler.compile(query, &SelectDest::Output)?;
                    (sub_compiler.ops, sub_compiler.result_column_names)
                };

                // Create ephemeral table for subquery results
                self.emit(
                    Opcode::OpenEphemeral,
                    cursor,
                    subquery_col_names.len() as i32,
                    0,
                    P4::Unused,
                );

                // Execute subquery and populate ephemeral table
                let sub_base_reg = self.alloc_reg();
                for op in subquery_ops {
                    self.ops.push(op);
                }
                self.emit(Opcode::Insert, cursor, sub_base_reg, 0, P4::Unused);

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

    /// Process join constraints (NATURAL, USING, ON) and generate WHERE conditions
    ///
    /// This matches SQLite's sqlite3ProcessJoin() function from select.c:
    /// - NATURAL joins: find common columns between tables and generate equalities
    /// - USING: generate equalities for specified columns
    /// - ON: use the expression directly
    fn process_joins(&mut self, src_list: &crate::parser::ast::SrcList) -> Result<()> {
        use crate::parser::ast::{BinaryOp, ColumnRef, Expr};

        for (i, item) in src_list.items.iter().enumerate() {
            if i == 0 {
                // First table has no join with previous
                continue;
            }

            let current_table = &self.tables[i];

            // Handle NATURAL join - find common columns
            if item.join_type.is_natural() {
                let common_cols = self.find_common_columns(i);
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
        // Track result registers explicitly since they may not be contiguous
        // (function arguments allocate intermediate registers)
        let mut result_regs: Vec<i32> = Vec::new();

        for col in columns {
            match col {
                ResultColumn::Star => {
                    // Expand * to all columns from all tables using schema
                    let tables_snapshot: Vec<_> = self.tables.clone();
                    for table in &tables_snapshot {
                        if let Some(schema_table) = &table.schema_table {
                            // Regular table - expand from schema
                            for (col_idx, col_def) in schema_table.columns.iter().enumerate() {
                                let reg = self.alloc_reg();
                                self.emit(
                                    Opcode::Column,
                                    table.cursor,
                                    col_idx as i32,
                                    reg,
                                    P4::Unused,
                                );
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
                                    self.emit(
                                        Opcode::Column,
                                        table.cursor,
                                        col_idx as i32,
                                        reg,
                                        P4::Unused,
                                    );
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
                    // Track explicit aliases for ORDER BY resolution
                    if let Some(alias_name) = alias {
                        self.result_aliases.insert(alias_name.to_lowercase(), reg);
                    }
                    self.columns.push(ColumnInfo {
                        name,
                        table: None,
                        affinity: Affinity::Blob,
                        reg,
                    });
                }
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

    /// Compile WHERE condition
    fn compile_where_condition(&mut self, expr: &Expr, skip_label: i32) -> Result<()> {
        let reg = self.alloc_reg();
        self.compile_expr(expr, reg)?;
        // If false (0), jump to skip_label
        self.emit(Opcode::IfNot, reg, skip_label, 1, P4::Unused);
        Ok(())
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
                        self.emit(Opcode::Integer, *n as i32, dest_reg, 0, P4::Unused);
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

                if is_rowid_alias(&col_ref.column) {
                    let cursor = if let Some(table) = &col_ref.table {
                        self.tables
                            .iter()
                            .find(|t| {
                                t.name.eq_ignore_ascii_case(table)
                                    || t.table_name.eq_ignore_ascii_case(table)
                            })
                            .map(|t| t.cursor)
                    } else if self.tables.len() == 1 {
                        self.tables.first().map(|t| t.cursor)
                    } else {
                        None
                    };

                    if let Some(cursor) = cursor {
                        self.emit(Opcode::Rowid, cursor, dest_reg, 0, P4::Unused);
                        return Ok(());
                    }
                }

                // Find the table and column index
                let (cursor, col_idx) = if let Some(table) = &col_ref.table {
                    // Check for multiple tables with the same name/alias (ambiguous)
                    let matching_tables: Vec<_> = self
                        .tables
                        .iter()
                        .filter(|t| t.name.eq_ignore_ascii_case(table))
                        .collect();

                    if matching_tables.len() > 1 {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("ambiguous column name: {}.{}", table, col_ref.column),
                        ));
                    }

                    if let Some(tinfo) = matching_tables.first() {
                        // Use column_index if set, otherwise look up from schema
                        let idx = col_ref.column_index.unwrap_or_else(|| {
                            tinfo
                                .schema_table
                                .as_ref()
                                .and_then(|st| {
                                    st.columns
                                        .iter()
                                        .position(|c| c.name.eq_ignore_ascii_case(&col_ref.column))
                                })
                                .map(|i| i as i32)
                                .unwrap_or(0)
                        });
                        (tinfo.cursor, idx)
                    } else {
                        // Table not found, use defaults
                        (0, col_ref.column_index.unwrap_or(0))
                    }
                } else {
                    // No table specified - search all tables for column
                    // Must check for ambiguous column references
                    let mut found = None;
                    let mut match_count = 0;
                    for tinfo in &self.tables {
                        if let Some(st) = &tinfo.schema_table {
                            if let Some(idx) = st
                                .columns
                                .iter()
                                .position(|c| c.name.eq_ignore_ascii_case(&col_ref.column))
                            {
                                match_count += 1;
                                if found.is_none() {
                                    found = Some((tinfo.cursor, idx as i32));
                                }
                            }
                        }
                    }
                    if match_count > 1 {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("ambiguous column name: {}", col_ref.column),
                        ));
                    }
                    found.unwrap_or_else(|| {
                        // Fallback to first table with col_idx=0
                        let cursor = self.tables.first().map(|t| t.cursor).unwrap_or(0);
                        (cursor, col_ref.column_index.unwrap_or(0))
                    })
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

                if is_comparison {
                    // Comparison opcodes are jump-based: Eq P1 P2 P3 means
                    // "if r[P1] == r[P3], jump to P2"
                    // We need to produce a 0/1 boolean result in dest_reg
                    let cmp_opcode = match op {
                        BinaryOp::Eq => Opcode::Eq,
                        BinaryOp::Ne => Opcode::Ne,
                        BinaryOp::Lt => Opcode::Lt,
                        BinaryOp::Le => Opcode::Le,
                        BinaryOp::Gt => Opcode::Gt,
                        BinaryOp::Ge => Opcode::Ge,
                        _ => unreachable!(),
                    };

                    // Set result to 0 (false) initially
                    self.emit(Opcode::Integer, 0, dest_reg, 0, P4::Unused);

                    // Allocate labels for control flow
                    let true_label = self.alloc_label();
                    let end_label = self.alloc_label();

                    // Compare: if condition is true, jump to true_label
                    // Comparison opcode format: P1=right operand, P2=jump target, P3=left operand
                    // Lt P1 P2 P3 means "jump to P2 if r[P3] < r[P1]"
                    self.emit(cmp_opcode, right_reg, true_label, left_reg, P4::Unused);

                    // Fall through means false - goto end
                    self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);

                    // True path: set result to 1
                    self.resolve_label(true_label, self.current_addr());
                    self.emit(Opcode::Integer, 1, dest_reg, 0, P4::Unused);

                    // End label
                    self.resolve_label(end_label, self.current_addr());
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
                        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
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
                    let is_known_function = is_aggregate
                        || crate::functions::get_scalar_function(&func_call.name).is_some();
                    if !is_known_function {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("no such function: {}", func_call.name.to_uppercase()),
                        ));
                    }

                    // Compile as scalar function
                    let arg_base = self.next_reg;
                    let argc = match &func_call.args {
                        crate::parser::ast::FunctionArgs::Exprs(exprs) => {
                            for arg in exprs {
                                let reg = self.alloc_reg();
                                self.compile_expr(arg, reg)?;
                            }
                            exprs.len()
                        }
                        crate::parser::ast::FunctionArgs::Star => 0,
                    };

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
                // Save outer query state
                let saved_tables = std::mem::take(&mut self.tables);
                let saved_has_agg = self.has_aggregates;
                let saved_has_window = self.has_window_functions;
                let saved_result_names = std::mem::take(&mut self.result_column_names);

                // Initialize result to NULL in case subquery returns no rows
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);

                // Compile the subquery body with Set destination
                let sub_dest = SelectDest::Set { reg: dest_reg };
                self.compile_body(&select.body, &sub_dest)?;

                // Restore outer query state
                self.tables = saved_tables;
                self.has_aggregates = saved_has_agg;
                self.has_window_functions = saved_has_window;
                self.result_column_names = saved_result_names;
            }
            Expr::Exists { subquery, negated } => {
                // Compile EXISTS subquery
                // Save outer query state
                let saved_tables = std::mem::take(&mut self.tables);
                let saved_has_agg = self.has_aggregates;
                let saved_has_window = self.has_window_functions;
                let saved_result_names = std::mem::take(&mut self.result_column_names);

                // Initialize result to 0 (false) - will be set to 1 if any row is found
                self.emit(
                    Opcode::Integer,
                    if *negated { 1 } else { 0 },
                    dest_reg,
                    0,
                    P4::Unused,
                );

                // Compile the subquery body with Exists destination
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

                // Restore outer query state
                self.tables = saved_tables;
                self.has_aggregates = saved_has_agg;
                self.has_window_functions = saved_has_window;
                self.result_column_names = saved_result_names;
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

                        // Save outer query state (including result_column_names to avoid
                        // subquery columns being added to outer result set)
                        let saved_tables = std::mem::take(&mut self.tables);
                        let saved_has_agg = self.has_aggregates;
                        let saved_has_window = self.has_window_functions;
                        let saved_order_by = std::mem::take(&mut self.order_by_terms);
                        let saved_limit_reg = self.limit_counter_reg.take();
                        let saved_offset_reg = self.offset_counter_reg.take();
                        let saved_limit_done = self.limit_done_label.take();
                        let saved_result_names = std::mem::take(&mut self.result_column_names);

                        // Compile full subquery (including ORDER BY/LIMIT) to fill ephemeral table
                        let subq_dest = SelectDest::EphemTable {
                            cursor: subq_cursor,
                        };
                        self.compile_subselect(subquery, &subq_dest)?;

                        // Restore outer query state
                        self.tables = saved_tables;
                        self.has_aggregates = saved_has_agg;
                        self.has_window_functions = saved_has_window;
                        self.order_by_terms = saved_order_by;
                        self.limit_counter_reg = saved_limit_reg;
                        self.offset_counter_reg = saved_offset_reg;
                        self.limit_done_label = saved_limit_done;
                        self.result_column_names = saved_result_names;

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

                // Check val >= low (fail if val < low)
                // Lt P1 P2 P3 jumps if r[P3] < r[P1], so P1=low, P3=val
                self.emit(Opcode::Lt, low_reg, fail_label, val_reg, P4::Unused);
                // Check val <= high (fail if val > high)
                // Gt P1 P2 P3 jumps if r[P3] > r[P1], so P1=high, P3=val
                self.emit(Opcode::Gt, high_reg, fail_label, val_reg, P4::Unused);

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
            // If offset <= 0, skip the offset decrement
            self.emit(Opcode::IfNot, offset_reg, after_offset, 0, P4::Unused);
            // Decrement offset and skip this row
            self.emit(Opcode::AddImm, offset_reg, -1, 0, P4::Unused);
            self.emit(
                Opcode::SorterNext,
                sorter_cursor,
                sorter_loop_start_label,
                0,
                P4::Unused,
            );
            self.resolve_label(after_offset, self.current_addr());
        }

        // Handle LIMIT: check if we've output enough rows
        if let Some(limit_reg) = self.limit_counter_reg {
            if let Some(done_label) = self.limit_done_label {
                // If limit <= 0, we're done
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
                self.emit(
                    Opcode::MakeRecord,
                    result_base_reg,
                    num_result_cols as i32,
                    record_reg,
                    P4::Unused,
                );
                self.emit(Opcode::NewRowid, *cursor, result_base_reg, 0, P4::Unused);
                self.emit(
                    Opcode::Insert,
                    *cursor,
                    record_reg,
                    result_base_reg,
                    P4::Unused,
                );
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
        if let Some(offset_reg) = self.offset_counter_reg {
            let after_offset = self.alloc_label();
            // If offset <= 0, skip the offset decrement
            self.emit(Opcode::IfNot, offset_reg, after_offset, 0, P4::Unused);
            // Decrement offset and skip this row
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
                self.emit(
                    Opcode::MakeRecord,
                    base_reg,
                    count as i32,
                    record_reg,
                    P4::Unused,
                );
                self.emit(Opcode::NewRowid, *cursor, base_reg, 0, P4::Unused);
                self.emit(Opcode::Insert, *cursor, record_reg, base_reg, P4::Unused);
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
                        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
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
                        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
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
                        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
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
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
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

                    // Compile argument
                    let arg_reg = self.alloc_reg();
                    let mut has_arg = false;
                    if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                        if !exprs.is_empty() {
                            self.compile_expr(&exprs[0], arg_reg)?;
                            has_arg = true;
                        }
                    }
                    // For COUNT(*), initialize arg_reg with 1 so it's not NULL
                    if !has_arg && name_upper == "COUNT" {
                        self.emit(Opcode::Integer, 1, arg_reg, 0, P4::Unused);
                    }

                    // Emit aggregate step opcode
                    self.emit(Opcode::AggStep, arg_reg, reg, 0, P4::Text(name_upper));
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
                            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
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
                        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
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
                        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
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
                            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
                        )
                    {
                        let arg_reg = self.alloc_reg();
                        self.emit(Opcode::Column, cursor, col_idx as i32, arg_reg, P4::Unused);
                        self.emit(
                            Opcode::AggStep,
                            arg_reg,
                            agg_regs[agg_idx],
                            0,
                            P4::Text(name_upper),
                        );
                        agg_idx += 1;
                        col_idx += 1;
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

        self.emit(Opcode::Next, cursor, loop_start_label, 0, P4::Unused);
        self.resolve_label(done_label, self.current_addr());

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

        self.emit(
            Opcode::SorterNext,
            sorter_cursor,
            sorter_loop_label,
            0,
            P4::Unused,
        );
        self.resolve_label(sort_done_label, self.current_addr());

        self.emit(Opcode::Close, sorter_cursor, 0, 0, P4::Unused);

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

    fn resolve_labels(&mut self) -> Result<()> {
        // Resolve all label references in jump instructions
        for op in &mut self.ops {
            if op.opcode.is_jump() {
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
