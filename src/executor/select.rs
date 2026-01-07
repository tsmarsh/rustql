//! SELECT statement code generation
//!
//! This module generates VDBE opcodes for SELECT statements.
//! Corresponds to SQLite's select.c.

use std::collections::HashMap;

use crate::error::{Error, ErrorCode, Result};
use crate::executor::window::{select_has_window_functions, WindowCompiler};
use crate::parser::ast::{
    BinaryOp, CompoundOp, Distinct, Expr, FromClause, JoinType, LimitClause, OrderingTerm,
    ResultColumn, SelectBody, SelectCore, SelectStmt, TableRef, WithClause,
};
use crate::schema::{Affinity, Table};
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

// ============================================================================
// Select Destination
// ============================================================================

/// Where to send SELECT results
#[derive(Debug, Clone)]
pub enum SelectDest {
    /// Return results to caller (normal query)
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

impl Default for SelectDest {
    fn default() -> Self {
        SelectDest::Output
    }
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
    pub schema_table: Option<Table>,
    /// Is this from a subquery?
    pub is_subquery: bool,
    /// Join type (for joined tables)
    pub join_type: JoinType,
}

// ============================================================================
// Select Compiler State
// ============================================================================

/// State for SELECT compilation
pub struct SelectCompiler {
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
}

impl SelectCompiler {
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
        }
    }

    /// Compile a SELECT statement
    pub fn compile(&mut self, select: &SelectStmt, dest: &SelectDest) -> Result<Vec<VdbeOp>> {
        // Handle WITH clause (CTEs)
        if let Some(with) = &select.with {
            self.process_with_clause(with)?;
        }

        // Compile the body
        self.compile_body(&select.body, dest)?;

        // Handle ORDER BY (at top level)
        if let Some(order_by) = &select.order_by {
            self.compile_order_by(order_by, dest)?;
        }

        // Handle LIMIT/OFFSET
        if let Some(limit) = &select.limit {
            self.compile_limit(limit)?;
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

    /// Compile a simple SELECT (not compound)
    fn compile_select_core(&mut self, core: &SelectCore, dest: &SelectDest) -> Result<()> {
        // Check for aggregates and window functions
        self.has_aggregates = self.check_for_aggregates(core);
        self.has_window_functions = select_has_window_functions(core);

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
        // Determine if we need DISTINCT processing
        let distinct_cursor = if core.distinct == Distinct::Distinct {
            let cursor = self.alloc_cursor();
            // Open ephemeral table for distinct
            self.emit(Opcode::OpenEphemeral, cursor, 0, 0, P4::Unused);
            Some(cursor)
        } else {
            None
        };

        // Collect table cursors to avoid borrow checker issues
        let table_cursors: Vec<i32> = self.tables.iter().map(|t| t.cursor).collect();

        // Generate Rewind for each table cursor
        let mut rewind_labels: Vec<i32> = Vec::with_capacity(table_cursors.len());
        for cursor in &table_cursors {
            let label = self.alloc_label();
            self.emit(Opcode::Rewind, *cursor, label, 0, P4::Unused);
            rewind_labels.push(label);
        }

        // Inner loop start
        let loop_start = self.current_addr();

        // Evaluate WHERE clause
        let where_skip_label = if let Some(where_expr) = &core.where_clause {
            let label = self.alloc_label();
            self.compile_where_condition(where_expr, label)?;
            Some(label)
        } else {
            None
        };

        // Evaluate result columns
        let result_regs = self.compile_result_columns(&core.columns)?;

        // Handle DISTINCT
        if let Some(distinct_cursor) = distinct_cursor {
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
        }

        // Output the row
        self.output_row(dest, result_regs.0, result_regs.1)?;

        // WHERE skip target
        if let Some(label) = where_skip_label {
            self.resolve_label(label, self.current_addr());
        }

        // DISTINCT skip target
        if distinct_cursor.is_some() {
            // Label was already emitted inline
        }

        // Generate Next for each table (in reverse order for nested loops)
        for (i, cursor) in table_cursors.iter().enumerate().rev() {
            self.emit(Opcode::Next, *cursor, loop_start as i32, 0, P4::Unused);
            self.resolve_label(rewind_labels[i], self.current_addr());
        }

        // Close cursors
        for cursor in &table_cursors {
            self.emit(Opcode::Close, *cursor, 0, 0, P4::Unused);
        }

        if let Some(cursor) = distinct_cursor {
            self.emit(Opcode::Close, cursor, 0, 0, P4::Unused);
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

        let loop_start = self.current_addr();

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
            self.emit(Opcode::Next, *cursor, loop_start as i32, 0, P4::Unused);
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
                    let tables_snapshot: Vec<_> = self.tables.clone();
                    for table in &tables_snapshot {
                        if table.name.eq_ignore_ascii_case(table_name)
                            || table.table_name.eq_ignore_ascii_case(table_name)
                        {
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

        let loop_start = self.current_addr();

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
            self.emit(Opcode::Next, *cursor, loop_start as i32, 0, P4::Unused);
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

        // Open sorter for grouping
        let sorter_cursor = self.alloc_cursor();
        let num_group_cols = group_by.len();
        self.emit(
            Opcode::OpenEphemeral,
            sorter_cursor,
            num_group_cols as i32,
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

        let loop_start = self.current_addr();

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
            self.emit(Opcode::Next, *cursor, loop_start as i32, 0, P4::Unused);
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

        let sorter_loop_start = self.current_addr();

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
        self.emit(Opcode::Jump, same_group_label, 0, 0, P4::Unused);

        // New group - output previous group if not first
        let first_group_label = self.alloc_label();
        self.emit(
            Opcode::If,
            prev_group_regs,
            first_group_label,
            0,
            P4::Unused,
        );

        // Finalize and output previous group
        let result_regs = self.finalize_aggregates(&core.columns, &agg_regs)?;

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
            sorter_loop_start as i32,
            0,
            P4::Unused,
        );

        // Output final group
        let result_regs = self.finalize_aggregates(&core.columns, &agg_regs)?;
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
        let left_dest = SelectDest::EphemTable {
            cursor: result_cursor,
        };
        self.compile_body(left, &left_dest)?;

        match op {
            CompoundOp::UnionAll => {
                // Just add right side to same table
                self.compile_body(right, &left_dest)?;
            }
            CompoundOp::Union => {
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

        // Output results from ephemeral table
        self.output_ephemeral_table(result_cursor, dest)?;
        self.emit(Opcode::Close, result_cursor, 0, 0, P4::Unused);

        Ok(())
    }

    /// Compile FROM clause - open cursors for tables
    fn compile_from_clause(&mut self, from: &FromClause) -> Result<()> {
        for table_ref in &from.tables {
            self.compile_table_ref(table_ref, JoinType::Inner)?;
        }
        Ok(())
    }

    /// Compile a table reference
    fn compile_table_ref(&mut self, table_ref: &TableRef, join_type: JoinType) -> Result<()> {
        match table_ref {
            TableRef::Table { name, alias, .. } => {
                let cursor = self.alloc_cursor();
                let table_name = &name.name;

                // Open the table (read mode)
                self.emit(Opcode::OpenRead, cursor, 0, 0, P4::Text(table_name.clone()));

                self.tables.push(TableInfo {
                    name: alias.clone().unwrap_or_else(|| table_name.clone()),
                    table_name: table_name.clone(),
                    cursor,
                    schema_table: None, // Would be resolved from schema
                    is_subquery: false,
                    join_type,
                });
            }
            TableRef::Subquery { query, alias } => {
                // Compile subquery as coroutine
                let cursor = self.alloc_cursor();
                self.emit(Opcode::OpenEphemeral, cursor, 0, 0, P4::Unused);

                // Compile subquery into ephemeral table
                let subquery_dest = SelectDest::EphemTable { cursor };
                let mut subcompiler = SelectCompiler::new();
                subcompiler.next_reg = self.next_reg;
                subcompiler.next_cursor = self.next_cursor;
                let subquery_ops = subcompiler.compile(query, &subquery_dest)?;

                // Inline the subquery ops
                for op in subquery_ops {
                    if op.opcode != Opcode::Halt {
                        self.ops.push(op);
                    }
                }

                self.next_reg = subcompiler.next_reg;
                self.next_cursor = subcompiler.next_cursor;

                self.tables.push(TableInfo {
                    name: alias
                        .clone()
                        .unwrap_or_else(|| format!("subquery_{}", cursor)),
                    table_name: String::new(),
                    cursor,
                    schema_table: None,
                    is_subquery: true,
                    join_type,
                });
            }
            TableRef::Join {
                left,
                join_type: jt,
                right,
                constraint: _,
            } => {
                // Compile left side
                self.compile_table_ref(left, JoinType::Inner)?;
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
                    &format!("Table-valued function {} not yet supported", name),
                ));
            }
        }
        Ok(())
    }

    /// Compile result columns
    fn compile_result_columns(&mut self, columns: &[ResultColumn]) -> Result<(i32, usize)> {
        let base_reg = self.next_reg;
        let mut count = 0;

        for col in columns {
            match col {
                ResultColumn::Star => {
                    // All columns from all tables
                    let cursors: Vec<i32> = self.tables.iter().map(|t| t.cursor).collect();
                    for cursor in cursors {
                        // For each column in table, emit Column opcode
                        // In a real implementation, we'd get column count from schema
                        // For now, assume we don't know column count
                        let reg = self.alloc_reg();
                        self.emit(Opcode::Column, cursor, -1, reg, P4::Unused);
                        count += 1;
                    }
                }
                ResultColumn::TableStar(table_name) => {
                    // All columns from specific table
                    let cursor = self
                        .tables
                        .iter()
                        .find(|t| t.name == *table_name)
                        .map(|t| t.cursor);
                    if let Some(c) = cursor {
                        let reg = self.alloc_reg();
                        self.emit(Opcode::Column, c, -1, reg, P4::Unused);
                        count += 1;
                    }
                }
                ResultColumn::Expr { expr, alias } => {
                    let reg = self.alloc_reg();
                    self.compile_expr(expr, reg)?;
                    count += 1;

                    self.columns.push(ColumnInfo {
                        name: alias.clone().unwrap_or_else(|| format!("column{}", count)),
                        table: None,
                        affinity: Affinity::Blob,
                        reg,
                    });
                }
            }
        }

        Ok((base_reg, count))
    }

    /// Compile WHERE condition
    fn compile_where_condition(&mut self, expr: &Expr, skip_label: i32) -> Result<()> {
        let reg = self.alloc_reg();
        self.compile_expr(expr, reg)?;
        // If false (0), jump to skip_label
        self.emit(Opcode::IfNot, reg, skip_label, 1, P4::Unused);
        Ok(())
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
                // Find the table and column
                if let Some(table) = &col_ref.table {
                    if let Some(tinfo) = self.tables.iter().find(|t| t.name == *table) {
                        // Column index would come from schema
                        self.emit(
                            Opcode::Column,
                            tinfo.cursor,
                            0,
                            dest_reg,
                            P4::Text(col_ref.column.clone()),
                        );
                    }
                } else {
                    // Search all tables for column
                    if let Some(tinfo) = self.tables.first() {
                        self.emit(
                            Opcode::Column,
                            tinfo.cursor,
                            0,
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

                let opcode = match op {
                    BinaryOp::Add => Opcode::Add,
                    BinaryOp::Sub => Opcode::Subtract,
                    BinaryOp::Mul => Opcode::Multiply,
                    BinaryOp::Div => Opcode::Divide,
                    BinaryOp::Mod => Opcode::Remainder,
                    BinaryOp::Eq => Opcode::Eq,
                    BinaryOp::Ne => Opcode::Ne,
                    BinaryOp::Lt => Opcode::Lt,
                    BinaryOp::Le => Opcode::Le,
                    BinaryOp::Gt => Opcode::Gt,
                    BinaryOp::Ge => Opcode::Ge,
                    BinaryOp::And => Opcode::And,
                    BinaryOp::Or => Opcode::Or,
                    BinaryOp::BitAnd => Opcode::BitAnd,
                    BinaryOp::BitOr => Opcode::BitOr,
                    BinaryOp::ShiftLeft => Opcode::ShiftLeft,
                    BinaryOp::ShiftRight => Opcode::ShiftRight,
                    BinaryOp::Concat => Opcode::Concat,
                    _ => Opcode::Noop,
                };

                self.emit(opcode, left_reg, right_reg, dest_reg, P4::Unused);
            }
            Expr::Unary { op, expr: inner } => {
                self.compile_expr(inner, dest_reg)?;
                match op {
                    crate::parser::ast::UnaryOp::Neg => {
                        let zero_reg = self.alloc_reg();
                        self.emit(Opcode::Integer, 0, zero_reg, 0, P4::Unused);
                        self.emit(Opcode::Subtract, zero_reg, dest_reg, dest_reg, P4::Unused);
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
                // Compile function arguments
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
            Expr::IsNull {
                expr: inner,
                negated,
            } => {
                self.compile_expr(inner, dest_reg)?;
                if *negated {
                    self.emit(Opcode::NotNull, dest_reg, dest_reg, 0, P4::Unused);
                } else {
                    self.emit(Opcode::IsNull, dest_reg, dest_reg, 0, P4::Unused);
                }
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
                // Compile as scalar subquery
                let mut subcompiler = SelectCompiler::new();
                subcompiler.next_reg = self.next_reg;
                subcompiler.next_cursor = self.next_cursor;
                let sub_dest = SelectDest::Set { reg: dest_reg };
                let _ = subcompiler.compile(select, &sub_dest)?;
                self.next_reg = subcompiler.next_reg;
                self.next_cursor = subcompiler.next_cursor;
                // In real implementation, inline the ops
            }
            _ => {
                // For other expression types, emit NULL as placeholder
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
            }
        }
        Ok(())
    }

    /// Compile ORDER BY
    fn compile_order_by(&mut self, _order_by: &[OrderingTerm], _dest: &SelectDest) -> Result<()> {
        // In a real implementation, this would set up a sorter
        // For now, ORDER BY is handled in the simple select
        Ok(())
    }

    /// Compile LIMIT/OFFSET
    fn compile_limit(&mut self, limit: &LimitClause) -> Result<()> {
        // Store limit in a register for checking during result output
        let limit_reg = self.alloc_reg();
        self.compile_expr(&limit.limit, limit_reg)?;

        if let Some(offset) = &limit.offset {
            let offset_reg = self.alloc_reg();
            self.compile_expr(offset, offset_reg)?;
            // Subtract offset from limit for combined processing
            self.emit(Opcode::Add, limit_reg, offset_reg, limit_reg, P4::Unused);
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
                matches!(
                    name_upper.as_str(),
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
                )
            }
            Expr::Binary { left, right, .. } => {
                self.expr_has_aggregate(left) || self.expr_has_aggregate(right)
            }
            _ => false,
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
                if let Expr::Function(func_call) = expr {
                    let name_upper = func_call.name.to_uppercase();
                    if matches!(
                        name_upper.as_str(),
                        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
                    ) {
                        let reg = agg_regs[agg_idx];

                        // Compile argument
                        let arg_reg = self.alloc_reg();
                        if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                            if !exprs.is_empty() {
                                self.compile_expr(&exprs[0], arg_reg)?;
                            }
                        }

                        // Emit aggregate step opcode
                        self.emit(Opcode::AggStep, arg_reg, reg, 0, P4::Text(name_upper));
                        agg_idx += 1;
                    }
                }
            }
        }
        Ok(())
    }

    fn finalize_aggregates(
        &mut self,
        columns: &[ResultColumn],
        agg_regs: &[i32],
    ) -> Result<(i32, usize)> {
        let base_reg = self.next_reg;
        let mut count = 0;
        let mut agg_idx = 0;

        for col in columns {
            let dest_reg = self.alloc_reg();
            if let ResultColumn::Expr { expr, .. } = col {
                if let Expr::Function(func_call) = expr {
                    let name_upper = func_call.name.to_uppercase();
                    if matches!(
                        name_upper.as_str(),
                        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
                    ) {
                        let agg_reg = agg_regs[agg_idx];
                        self.emit(Opcode::AggFinal, agg_reg, dest_reg, 0, P4::Text(name_upper));
                        agg_idx += 1;
                    } else {
                        self.compile_expr(expr, dest_reg)?;
                    }
                } else {
                    self.compile_expr(expr, dest_reg)?;
                }
            }
            count += 1;
        }

        Ok((base_reg, count))
    }

    fn reset_aggregates(&mut self, agg_regs: &[i32]) -> Result<()> {
        for &reg in agg_regs {
            self.emit(Opcode::Null, 0, reg, 0, P4::Unused);
        }
        Ok(())
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
                    if matches!(
                        name_upper.as_str(),
                        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
                    ) {
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

    fn merge_distinct(&mut self, _left: i32, _right: i32) -> Result<()> {
        // Placeholder - merge two tables keeping distinct rows
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
        let loop_start = self.current_addr();

        // Get row from ephemeral table
        let data_reg = self.alloc_reg();
        self.emit(Opcode::Column, cursor, 0, data_reg, P4::Unused);

        // Output based on destination
        self.output_row(dest, data_reg, 1)?;

        self.emit(Opcode::Next, cursor, loop_start as i32, 0, P4::Unused);
        self.resolve_label(done_label, self.current_addr());

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

impl Default for SelectCompiler {
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
}
