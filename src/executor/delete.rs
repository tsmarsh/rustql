//! DELETE statement compilation
//!
//! This module compiles DELETE statements to VDBE bytecode.
//! Corresponds to delete.c in SQLite.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::parser::ast::{DeleteStmt, Expr, ResultColumn};
use crate::schema::{Schema, Trigger, TriggerEvent, TriggerTiming};
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

use super::column_mapping::ColumnMapper;
use super::select::{SelectCompiler, SelectDest};
use super::trigger::{find_matching_triggers, generate_trigger_code};

const OPFLAG_NCHANGE: u16 = 0x01;

/// Tracks an index cursor for index maintenance during DELETE
struct IndexCursor {
    /// Cursor number
    cursor: i32,
    /// Column indices in the index (in order)
    columns: Vec<i32>,
    /// Index name
    #[allow(dead_code)]
    name: String,
}

fn is_rowid_alias(name: &str) -> bool {
    name.eq_ignore_ascii_case("rowid")
        || name.eq_ignore_ascii_case("_rowid_")
        || name.eq_ignore_ascii_case("oid")
}

// ============================================================================
// DeleteCompiler
// ============================================================================

/// Compiles DELETE statements to VDBE opcodes
pub struct DeleteCompiler<'s> {
    /// Generated VDBE operations
    ops: Vec<VdbeOp>,

    /// Next register to allocate
    next_reg: i32,

    /// Next cursor to allocate
    next_cursor: i32,

    /// Next label
    next_label: i32,

    /// Labels pending resolution
    labels: HashMap<i32, Option<i32>>,

    /// Table cursor
    table_cursor: i32,

    /// Number of columns in target table
    num_columns: usize,

    /// Column name to index mapping
    column_map: HashMap<String, usize>,

    /// Unified column mapper (replaces column_map over time)
    mapper: Option<ColumnMapper>,

    /// Schema for column resolution
    schema: Option<&'s crate::schema::Schema>,

    /// Parameter names for bound parameter lookup
    param_names: Vec<Option<String>>,

    /// Next unnamed parameter index (1-based)
    next_unnamed_param: i32,

    /// Index cursors for maintaining indexes during delete
    index_cursors: Vec<IndexCursor>,

    /// BEFORE DELETE triggers
    before_triggers: Vec<Arc<Trigger>>,

    /// AFTER DELETE triggers
    after_triggers: Vec<Arc<Trigger>>,

    /// Name of table being deleted from (for subquery detection)
    target_table: String,

    /// Pre-computed subquery results (maps from subquery index to result register)
    /// Subqueries that reference the target table are pre-evaluated before the loop
    precomputed_subqueries: HashMap<usize, i32>,

    /// Counter for assigning unique indices to subqueries during scan
    subquery_counter: usize,

    /// Table name for correlated subquery support
    table_name: String,

    /// Table alias (if specified in DELETE ... AS alias)
    table_alias: Option<String>,

    /// Schema table info for correlated subquery column resolution
    schema_table: Option<Arc<crate::schema::Table>>,

    /// WITH clause for CTE support
    with_clause: Option<crate::parser::ast::WithClause>,
}

impl<'s> DeleteCompiler<'s> {
    /// Create a new DELETE compiler
    pub fn new() -> Self {
        DeleteCompiler {
            ops: Vec::new(),
            next_reg: 1,
            next_cursor: 0,
            next_label: -1,
            labels: HashMap::new(),
            table_cursor: 0,
            num_columns: 0,
            column_map: HashMap::new(),
            mapper: None,
            schema: None,
            param_names: Vec::new(),
            next_unnamed_param: 1,
            index_cursors: Vec::new(),
            before_triggers: Vec::new(),
            after_triggers: Vec::new(),
            target_table: String::new(),
            precomputed_subqueries: HashMap::new(),
            subquery_counter: 0,
            table_name: String::new(),
            table_alias: None,
            schema_table: None,
            with_clause: None,
        }
    }

    /// Create DELETE compiler with schema for column resolution
    pub fn with_schema(schema: &'s crate::schema::Schema) -> Self {
        DeleteCompiler {
            ops: Vec::new(),
            next_reg: 1,
            next_cursor: 0,
            next_label: -1,
            labels: HashMap::new(),
            table_cursor: 0,
            num_columns: 0,
            column_map: HashMap::new(),
            mapper: None,
            schema: Some(schema),
            param_names: Vec::new(),
            next_unnamed_param: 1,
            before_triggers: Vec::new(),
            after_triggers: Vec::new(),
            index_cursors: Vec::new(),
            target_table: String::new(),
            precomputed_subqueries: HashMap::new(),
            subquery_counter: 0,
            table_name: String::new(),
            table_alias: None,
            schema_table: None,
            with_clause: None,
        }
    }

    /// Set parameter names for Variable compilation
    pub fn set_param_names(&mut self, param_names: Vec<Option<String>>) {
        self.param_names = param_names;
    }

    /// Compile a DELETE statement
    pub fn compile(&mut self, delete: &DeleteStmt) -> Result<Vec<VdbeOp>> {
        // Check for system tables that cannot be modified
        let table_name_lower = delete.table.name.to_lowercase();
        if table_name_lower == "sqlite_master"
            || table_name_lower == "sqlite_schema"
            || table_name_lower == "sqlite_temp_master"
            || table_name_lower == "sqlite_temp_schema"
        {
            return Err(crate::error::Error::with_message(
                crate::error::ErrorCode::Error,
                format!("table {} may not be modified", delete.table.name),
            ));
        }

        // Store target table name for subquery reference detection
        self.target_table = table_name_lower.clone();

        // Store WITH clause for CTE support in subqueries
        self.with_clause = delete.with.clone();

        // Initialize
        self.emit(Opcode::Init, 0, 0, 0, P4::Unused);

        // Open table for writing (need write access to delete)
        self.table_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenWrite,
            self.table_cursor,
            0, // Root page (would come from schema)
            0,
            P4::Text(delete.table.name.clone()),
        );

        // Populate column map from schema if available
        if let Some(schema) = self.schema {
            if let Some(table) = schema.tables.get(&delete.table.name) {
                self.num_columns = table.columns.len();
                for (idx, col) in table.columns.iter().enumerate() {
                    self.column_map.insert(col.name.to_lowercase(), idx);
                }
            } else {
                // Table not found in schema - try lowercase
                let table_lower = delete.table.name.to_lowercase();
                if let Some(table) = schema.tables.get(&table_lower) {
                    self.num_columns = table.columns.len();
                    for (idx, col) in table.columns.iter().enumerate() {
                        self.column_map.insert(col.name.to_lowercase(), idx);
                    }
                } else {
                    // Fallback - assume 5 columns
                    self.num_columns = 5;
                    self.build_column_map();
                }
            }
        } else {
            // No schema - use placeholder
            self.num_columns = 5;
            self.build_column_map();
        }

        // Initialize ColumnMapper for validation
        if let Some(schema) = self.schema {
            self.mapper = Some(ColumnMapper::new(
                &delete.table.name,
                None, // DELETE doesn't use explicit column list
                0,    // source_count not used for DELETE validation
                Some(schema),
            )?);
        }

        // Store table info for correlated subquery support
        self.table_name = delete.table.name.clone();
        self.table_alias = delete.alias.clone();
        if let Some(schema) = self.schema {
            // Try to get schema table (case-insensitive lookup)
            self.schema_table = schema
                .tables
                .get(&delete.table.name)
                .or_else(|| schema.tables.get(&delete.table.name.to_lowercase()))
                .cloned();
        }

        // Open indexes for writing (for index maintenance)
        self.open_indexes_for_write(&delete.table.name)?;

        // Find matching triggers for DELETE
        if let Some(schema) = self.schema {
            self.before_triggers = find_matching_triggers(
                schema,
                &delete.table.name,
                TriggerTiming::Before,
                TriggerEvent::Delete,
                None,
            );
            self.after_triggers = find_matching_triggers(
                schema,
                &delete.table.name,
                TriggerTiming::After,
                TriggerEvent::Delete,
                None,
            );
        }

        // Compile the DELETE body
        self.compile_delete_body(delete)?;

        // Handle RETURNING clause
        if let Some(returning) = &delete.returning {
            self.compile_returning(returning)?;
        }

        // Close index cursors
        let index_cursor_ids: Vec<i32> = self.index_cursors.iter().map(|ic| ic.cursor).collect();
        for cursor in index_cursor_ids {
            self.emit(Opcode::Close, cursor, 0, 0, P4::Unused);
        }

        // Close table cursor
        self.emit(Opcode::Close, self.table_cursor, 0, 0, P4::Unused);

        // Halt
        self.emit(Opcode::Halt, 0, 0, 0, P4::Unused);

        // Resolve labels
        self.resolve_labels()?;

        Ok(std::mem::take(&mut self.ops))
    }

    /// Compile the DELETE body (loop over rows, delete matching ones)
    fn compile_delete_body(&mut self, delete: &DeleteStmt) -> Result<()> {
        // Check if we have ORDER BY and LIMIT
        // If so, we need a different approach: collect rowids first
        if delete.order_by.is_some() || delete.limit.is_some() {
            return self.compile_delete_with_limit(delete);
        }

        // Precompute subqueries that reference the target table BEFORE the loop
        // This ensures stable results even as rows are deleted
        if let Some(where_expr) = &delete.where_clause {
            self.precompute_subqueries(where_expr)?;
        }

        let has_before_triggers = !self.before_triggers.is_empty();
        let has_after_triggers = !self.after_triggers.is_empty();

        // Simple delete - just iterate and delete matching rows
        let loop_start_label = self.alloc_label();
        let loop_end_label = self.alloc_label();

        // Rewind to start of table
        self.emit(
            Opcode::Rewind,
            self.table_cursor,
            loop_end_label,
            0,
            P4::Unused,
        );

        // Loop start
        self.resolve_label(loop_start_label, self.current_addr() as i32);

        // Get rowid register for index maintenance
        let rowid_reg = self.alloc_reg();

        // Allocate registers for OLD row if we have triggers
        let old_base_reg = if has_before_triggers || has_after_triggers {
            let reg = self.alloc_reg();
            // Allocate additional registers for all columns
            for _ in 1..self.num_columns {
                self.alloc_reg();
            }
            Some(reg)
        } else {
            None
        };

        // If we have a WHERE clause, check the condition
        if let Some(where_expr) = &delete.where_clause {
            // Reset subquery counter so indices match precomputed values
            self.subquery_counter = 0;
            let skip_label = self.alloc_label();
            self.compile_where_check(where_expr, skip_label)?;

            // Get the rowid before deleting
            self.emit(Opcode::Rowid, self.table_cursor, rowid_reg, 0, P4::Unused);

            // Load OLD row values into registers if we have triggers
            if let Some(old_reg) = old_base_reg {
                self.emit_load_row(old_reg)?;
            }

            // Fire BEFORE DELETE triggers
            if has_before_triggers {
                self.emit_before_triggers(&delete.table.name, old_base_reg, rowid_reg)?;
            }

            // Delete from indexes first (before deleting the row)
            self.emit_index_deletes(rowid_reg);

            // Delete the row - set OPFLAG_NCHANGE to track deleted rows
            // Pass table name in P4 for trigger dispatch
            self.emit_with_p5(
                Opcode::Delete,
                self.table_cursor,
                0,
                0,
                P4::Table(delete.table.name.clone()),
                OPFLAG_NCHANGE,
            );

            // Fire AFTER DELETE triggers
            if has_after_triggers {
                self.emit_after_triggers(&delete.table.name, old_base_reg, rowid_reg)?;
            }

            // Skip label (for rows that don't match WHERE)
            self.resolve_label(skip_label, self.current_addr() as i32);
        } else {
            // No WHERE - delete every row
            // Get the rowid before deleting
            self.emit(Opcode::Rowid, self.table_cursor, rowid_reg, 0, P4::Unused);

            // Load OLD row values into registers if we have triggers
            if let Some(old_reg) = old_base_reg {
                self.emit_load_row(old_reg)?;
            }

            // Fire BEFORE DELETE triggers
            if has_before_triggers {
                self.emit_before_triggers(&delete.table.name, old_base_reg, rowid_reg)?;
            }

            // Delete from indexes first (before deleting the row)
            self.emit_index_deletes(rowid_reg);

            // Pass table name in P4 for trigger dispatch
            self.emit_with_p5(
                Opcode::Delete,
                self.table_cursor,
                0,
                0,
                P4::Table(delete.table.name.clone()),
                OPFLAG_NCHANGE,
            );

            // Fire AFTER DELETE triggers
            if has_after_triggers {
                self.emit_after_triggers(&delete.table.name, old_base_reg, rowid_reg)?;
            }
        }

        // Move to next row
        self.emit(
            Opcode::Next,
            self.table_cursor,
            loop_start_label,
            0,
            P4::Unused,
        );

        // Loop end
        self.resolve_label(loop_end_label, self.current_addr() as i32);

        Ok(())
    }

    /// Load the current row values into registers
    fn emit_load_row(&mut self, base_reg: i32) -> Result<()> {
        for i in 0..self.num_columns {
            self.emit(
                Opcode::Column,
                self.table_cursor,
                i as i32,
                base_reg + i as i32,
                P4::Unused,
            );
        }
        Ok(())
    }

    /// Emit BEFORE DELETE trigger calls
    fn emit_before_triggers(
        &mut self,
        table_name: &str,
        old_base_reg: Option<i32>,
        _rowid_reg: i32,
    ) -> Result<()> {
        // Use a label for the return address - will be resolved after all trigger ops are added
        let return_label = self.alloc_label();
        let num_cols = self.num_columns as i32;
        let triggers = self.before_triggers.clone();
        let schema = self.schema;

        let trigger_ops = generate_trigger_code(
            &triggers,
            schema,
            table_name,
            old_base_reg,
            None, // No NEW row for DELETE
            num_cols,
            &mut self.next_reg,
            &mut self.next_cursor,
            return_label, // Use label instead of fixed address
        )?;

        // Append trigger ops
        for op in trigger_ops {
            self.ops.push(op);
        }

        // Resolve the return label to point to the instruction after all trigger ops
        self.resolve_label(return_label, self.current_addr() as i32);
        Ok(())
    }

    /// Emit AFTER DELETE trigger calls
    fn emit_after_triggers(
        &mut self,
        table_name: &str,
        old_base_reg: Option<i32>,
        _rowid_reg: i32,
    ) -> Result<()> {
        // Use a label for the return address - will be resolved after all trigger ops are added
        let return_label = self.alloc_label();
        let num_cols = self.num_columns as i32;
        let triggers = self.after_triggers.clone();
        let schema = self.schema;

        let trigger_ops = generate_trigger_code(
            &triggers,
            schema,
            table_name,
            old_base_reg,
            None, // No NEW row for DELETE
            num_cols,
            &mut self.next_reg,
            &mut self.next_cursor,
            return_label, // Use label instead of fixed address
        )?;

        // Append trigger ops
        for op in trigger_ops {
            self.ops.push(op);
        }

        // Resolve the return label to point to the instruction after all trigger ops
        self.resolve_label(return_label, self.current_addr() as i32);
        Ok(())
    }

    /// Compile DELETE with ORDER BY and/or LIMIT
    fn compile_delete_with_limit(&mut self, delete: &DeleteStmt) -> Result<()> {
        // When DELETE has ORDER BY and/or LIMIT, we need to:
        // 1. Collect rowids in sorted order
        // 2. Apply limit
        // 3. Delete collected rowids

        // Create ephemeral table to store rowids to delete
        let ephemeral_cursor = self.alloc_cursor();
        self.emit(Opcode::OpenEphemeral, ephemeral_cursor, 1, 0, P4::Unused);

        // First pass: collect rowids
        let collect_loop_start = self.alloc_label();
        let collect_loop_end = self.alloc_label();

        self.emit(
            Opcode::Rewind,
            self.table_cursor,
            collect_loop_end,
            0,
            P4::Unused,
        );
        self.resolve_label(collect_loop_start, self.current_addr() as i32);

        // Check WHERE clause
        if let Some(where_expr) = &delete.where_clause {
            let skip_label = self.alloc_label();
            self.compile_where_check(where_expr, skip_label)?;

            // Store rowid in ephemeral table
            let rowid_reg = self.alloc_reg();
            self.emit(Opcode::Rowid, self.table_cursor, rowid_reg, 0, P4::Unused);

            // If we have ORDER BY, we need sort keys too
            // For now, just store the rowid
            let record_reg = self.alloc_reg();
            self.emit(Opcode::MakeRecord, rowid_reg, 1, record_reg, P4::Unused);
            self.emit(
                Opcode::IdxInsert,
                ephemeral_cursor,
                record_reg,
                0,
                P4::Unused,
            );

            self.resolve_label(skip_label, self.current_addr() as i32);
        } else {
            // Store all rowids
            let rowid_reg = self.alloc_reg();
            self.emit(Opcode::Rowid, self.table_cursor, rowid_reg, 0, P4::Unused);
            let record_reg = self.alloc_reg();
            self.emit(Opcode::MakeRecord, rowid_reg, 1, record_reg, P4::Unused);
            self.emit(
                Opcode::IdxInsert,
                ephemeral_cursor,
                record_reg,
                0,
                P4::Unused,
            );
        }

        self.emit(
            Opcode::Next,
            self.table_cursor,
            collect_loop_start,
            0,
            P4::Unused,
        );
        self.resolve_label(collect_loop_end, self.current_addr() as i32);

        // Second pass: delete collected rowids
        let delete_loop_start = self.alloc_label();
        let delete_loop_end = self.alloc_label();

        // Apply LIMIT if present
        let limit_reg = if let Some(ref limit_clause) = delete.limit {
            let reg = self.alloc_reg();
            self.compile_expr(&limit_clause.limit, reg)?;
            Some(reg)
        } else {
            None
        };

        // Counter for limit
        let counter_reg = self.alloc_reg();
        self.emit(Opcode::Integer, 0, counter_reg, 0, P4::Unused);

        self.emit(
            Opcode::Rewind,
            ephemeral_cursor,
            delete_loop_end,
            0,
            P4::Unused,
        );
        self.resolve_label(delete_loop_start, self.current_addr() as i32);

        // Check limit
        if let Some(limit_reg) = limit_reg {
            self.emit(
                Opcode::Ge,
                counter_reg,
                delete_loop_end,
                limit_reg,
                P4::Unused,
            );
        }

        // Get rowid from ephemeral table
        let rowid_reg = self.alloc_reg();
        self.emit(Opcode::Column, ephemeral_cursor, 0, rowid_reg, P4::Unused);

        // Seek to the row and delete it
        self.emit(
            Opcode::NotExists,
            self.table_cursor,
            delete_loop_start,
            rowid_reg,
            P4::Unused,
        );

        // Delete from indexes first (before deleting the row)
        self.emit_index_deletes(rowid_reg);

        // Pass table name in P4 for trigger dispatch
        self.emit_with_p5(
            Opcode::Delete,
            self.table_cursor,
            0,
            0,
            P4::Table(delete.table.name.clone()),
            OPFLAG_NCHANGE,
        );

        // Increment counter
        let one_reg = self.alloc_reg();
        self.emit(Opcode::Integer, 1, one_reg, 0, P4::Unused);
        self.emit(Opcode::Add, counter_reg, one_reg, counter_reg, P4::Unused);

        self.emit(
            Opcode::Next,
            ephemeral_cursor,
            delete_loop_start,
            0,
            P4::Unused,
        );
        self.resolve_label(delete_loop_end, self.current_addr() as i32);

        // Close ephemeral cursor
        self.emit(Opcode::Close, ephemeral_cursor, 0, 0, P4::Unused);

        Ok(())
    }

    /// Compile code to check WHERE clause condition
    fn compile_where_check(&mut self, where_expr: &Expr, skip_label: i32) -> Result<()> {
        // Validate columns in WHERE clause first (only if we have schema info)
        if self.schema.is_some() {
            self.validate_expr_columns(where_expr)?;
        }

        let cond_reg = self.alloc_reg();
        self.compile_expr(where_expr, cond_reg)?;

        // Jump to skip_label if condition is false (0) or NULL
        self.emit(Opcode::IfNot, cond_reg, skip_label, 1, P4::Unused);

        Ok(())
    }

    /// Validate that all column references in an expression exist in the table
    fn validate_expr_columns(&self, expr: &Expr) -> Result<()> {
        match expr {
            Expr::Column(col_ref) => {
                // Skip rowid aliases
                if is_rowid_alias(&col_ref.column) {
                    return Ok(());
                }

                // Check if column exists
                if !self.column_exists(&col_ref.column) {
                    return Err(crate::error::Error::with_message(
                        crate::error::ErrorCode::Error,
                        format!("no such column: {}", col_ref.column),
                    ));
                }
                Ok(())
            }
            Expr::Binary { left, right, .. } => {
                self.validate_expr_columns(left)?;
                self.validate_expr_columns(right)
            }
            Expr::Unary { expr: inner, .. } => self.validate_expr_columns(inner),
            Expr::Parens(inner) => self.validate_expr_columns(inner),
            Expr::Function(func_call) => {
                match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => {
                        for arg in exprs {
                            self.validate_expr_columns(arg)?;
                        }
                    }
                    _ => {}
                }
                Ok(())
            }
            Expr::IsNull { expr: inner, .. } => self.validate_expr_columns(inner),
            Expr::Between {
                expr: e, low, high, ..
            } => {
                self.validate_expr_columns(e)?;
                self.validate_expr_columns(low)?;
                self.validate_expr_columns(high)
            }
            Expr::In { expr: e, list, .. } => {
                self.validate_expr_columns(e)?;
                match list {
                    crate::parser::ast::InList::Values(exprs) => {
                        for val_expr in exprs {
                            self.validate_expr_columns(val_expr)?;
                        }
                    }
                    _ => {}
                }
                Ok(())
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    self.validate_expr_columns(op)?;
                }
                for when_clause in when_clauses {
                    self.validate_expr_columns(&when_clause.when)?;
                    self.validate_expr_columns(&when_clause.then)?;
                }
                if let Some(else_e) = else_clause {
                    self.validate_expr_columns(else_e)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Check if a column exists in the table
    fn column_exists(&self, col_name: &str) -> bool {
        // Check rowid aliases
        if is_rowid_alias(col_name) {
            return true;
        }

        // Try mapper first if available
        if let Some(mapper) = &self.mapper {
            if mapper.validate_column(col_name).is_ok() {
                return true;
            }
        }

        // Check in column_map (case-insensitive)
        let col_lower = col_name.to_lowercase();
        self.column_map.contains_key(&col_lower)
    }

    /// Build column index map
    fn build_column_map(&mut self) {
        // In a real implementation, we'd populate this from schema
        for i in 0..self.num_columns {
            self.column_map.insert(format!("col{}", i), i);
        }
    }

    /// Get column index by name
    fn get_column_index(&self, name: &str) -> Option<usize> {
        // Try mapper first if available
        if let Some(mapper) = &self.mapper {
            if let Ok(idx) = mapper.validate_column(name) {
                return Some(idx);
            }
        }
        // Try exact match in column_map
        if let Some(&idx) = self.column_map.get(name) {
            return Some(idx);
        }
        // Try case-insensitive match
        let name_lower = name.to_lowercase();
        if let Some(&idx) = self.column_map.get(&name_lower) {
            return Some(idx);
        }
        None
    }

    /// Compile RETURNING clause
    fn compile_returning(&mut self, returning: &[ResultColumn]) -> Result<()> {
        let base_reg = self.next_reg;

        for (i, col) in returning.iter().enumerate() {
            let reg = self.alloc_reg();
            match col {
                ResultColumn::Star => {
                    self.emit(Opcode::Column, self.table_cursor, i as i32, reg, P4::Unused);
                }
                ResultColumn::TableStar(_) => {
                    self.emit(Opcode::Column, self.table_cursor, i as i32, reg, P4::Unused);
                }
                ResultColumn::Expr { expr, .. } => {
                    self.compile_expr(expr, reg)?;
                }
            }
        }

        // Output the row
        self.emit(
            Opcode::ResultRow,
            base_reg,
            returning.len() as i32,
            0,
            P4::Unused,
        );

        Ok(())
    }

    /// Compile an expression
    fn compile_expr(&mut self, expr: &Expr, dest_reg: i32) -> Result<()> {
        match expr {
            Expr::Literal(lit) => match lit {
                crate::parser::ast::Literal::Null => {
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                }
                crate::parser::ast::Literal::Integer(n) => {
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
                crate::parser::ast::Literal::Bool(b) => {
                    self.emit(
                        Opcode::Integer,
                        if *b { 1 } else { 0 },
                        dest_reg,
                        0,
                        P4::Unused,
                    );
                }
                _ => {
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                }
            },
            Expr::Column(col_ref) => {
                if is_rowid_alias(&col_ref.column) {
                    self.emit(Opcode::Rowid, self.table_cursor, dest_reg, 0, P4::Unused);
                    return Ok(());
                }
                // Try to get column index from explicit annotation, schema, or column_map
                let col_idx = col_ref
                    .column_index
                    .or_else(|| self.get_column_index(&col_ref.column).map(|i| i as i32));

                match col_idx {
                    Some(idx) if idx < 0 => {
                        // Negative index indicates rowid
                        self.emit(Opcode::Rowid, self.table_cursor, dest_reg, 0, P4::Unused);
                    }
                    Some(idx) => {
                        self.emit(Opcode::Column, self.table_cursor, idx, dest_reg, P4::Unused);
                    }
                    None => {
                        // Column not found - emit with name in P4 for runtime resolution
                        self.emit(
                            Opcode::Column,
                            self.table_cursor,
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

                // Check if this is a comparison operator
                let is_comparison = matches!(
                    op,
                    crate::parser::ast::BinaryOp::Eq
                        | crate::parser::ast::BinaryOp::Ne
                        | crate::parser::ast::BinaryOp::Lt
                        | crate::parser::ast::BinaryOp::Le
                        | crate::parser::ast::BinaryOp::Gt
                        | crate::parser::ast::BinaryOp::Ge
                );

                if is_comparison {
                    // Comparison operators are jump-based in VDBE, so we need to
                    // convert them to produce a boolean result in dest_reg.
                    // Pattern:
                    //   1. Set dest_reg = 1 (true, assuming condition will be true)
                    //   2. Jump over "set false" if condition IS true
                    //   3. Set dest_reg = 0 (false)
                    //   4. done_label:

                    let done_label = self.alloc_label();

                    // Set dest_reg = 1 (true) by default
                    self.emit(Opcode::Integer, 1, dest_reg, 0, P4::Unused);

                    // Emit the comparison opcode - it jumps to done_label if true
                    // Comparison: jump to P2 if r[P3] op r[P1]
                    let opcode = match op {
                        crate::parser::ast::BinaryOp::Eq => Opcode::Eq,
                        crate::parser::ast::BinaryOp::Ne => Opcode::Ne,
                        crate::parser::ast::BinaryOp::Lt => Opcode::Lt,
                        crate::parser::ast::BinaryOp::Le => Opcode::Le,
                        crate::parser::ast::BinaryOp::Gt => Opcode::Gt,
                        crate::parser::ast::BinaryOp::Ge => Opcode::Ge,
                        _ => unreachable!(),
                    };
                    self.emit(opcode, right_reg, done_label, left_reg, P4::Unused);

                    // If we get here, condition was false - set dest_reg = 0
                    self.emit(Opcode::Integer, 0, dest_reg, 0, P4::Unused);

                    // done_label:
                    self.resolve_label(done_label, self.current_addr() as i32);
                } else {
                    // Non-comparison operators (arithmetic, logical, etc.)
                    let opcode = match op {
                        crate::parser::ast::BinaryOp::Add => Opcode::Add,
                        crate::parser::ast::BinaryOp::Sub => Opcode::Subtract,
                        crate::parser::ast::BinaryOp::Mul => Opcode::Multiply,
                        crate::parser::ast::BinaryOp::Div => Opcode::Divide,
                        crate::parser::ast::BinaryOp::Concat => Opcode::Concat,
                        crate::parser::ast::BinaryOp::And => Opcode::And,
                        crate::parser::ast::BinaryOp::Or => Opcode::Or,
                        crate::parser::ast::BinaryOp::BitAnd => Opcode::BitAnd,
                        crate::parser::ast::BinaryOp::BitOr => Opcode::BitOr,
                        crate::parser::ast::BinaryOp::Mod => Opcode::Remainder,
                        crate::parser::ast::BinaryOp::ShiftLeft => Opcode::ShiftLeft,
                        crate::parser::ast::BinaryOp::ShiftRight => Opcode::ShiftRight,
                        _ => Opcode::Add, // Default fallback
                    };

                    // Arithmetic: r[P2] op r[P1] stored in r[P3]
                    self.emit(opcode, right_reg, left_reg, dest_reg, P4::Unused);
                }
            }
            Expr::Unary { op, expr: inner } => {
                self.compile_expr(inner, dest_reg)?;
                match op {
                    crate::parser::ast::UnaryOp::Neg => {
                        self.emit(Opcode::Negative, dest_reg, dest_reg, 0, P4::Unused);
                    }
                    crate::parser::ast::UnaryOp::Not => {
                        self.emit(Opcode::Not, dest_reg, dest_reg, 0, P4::Unused);
                    }
                    crate::parser::ast::UnaryOp::BitNot => {
                        self.emit(Opcode::BitNot, dest_reg, dest_reg, 0, P4::Unused);
                    }
                    _ => {}
                }
            }
            Expr::Parens(inner) => {
                // Parenthesized expression - just compile the inner
                self.compile_expr(inner, dest_reg)?;
            }
            Expr::Function(func_call) => {
                // Validate function exists
                let name = &func_call.name;
                let is_aggregate = matches!(
                    name.to_uppercase().as_str(),
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
                );
                if !is_aggregate && crate::functions::get_scalar_function(name).is_none() {
                    return Err(crate::error::Error::with_message(
                        crate::error::ErrorCode::Error,
                        format!("no such function: {}", name),
                    ));
                }

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
                    let is_null_reg = self.alloc_reg();
                    self.emit(Opcode::IsNull, dest_reg, 0, is_null_reg, P4::Unused);
                    self.emit(Opcode::Not, is_null_reg, dest_reg, 0, P4::Unused);
                } else {
                    self.emit(Opcode::IsNull, dest_reg, 0, dest_reg, P4::Unused);
                }
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
            Expr::Subquery(select) => {
                // Check if this subquery references the target table and has been precomputed
                if self.select_references_table(select) {
                    // Look up precomputed value
                    let idx = self.subquery_counter;
                    self.subquery_counter += 1;
                    if let Some(&precomputed_reg) = self.precomputed_subqueries.get(&idx) {
                        // Use the precomputed value
                        self.emit(Opcode::Copy, precomputed_reg, dest_reg, 0, P4::Unused);
                        return Ok(());
                    }
                }

                // Compile scalar subquery inline (not precomputed)
                // Initialize dest_reg to NULL in case subquery returns no rows
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);

                // Use SelectCompiler to compile the subquery
                let mut sub_compiler = if let Some(schema) = self.schema {
                    SelectCompiler::with_schema(schema)
                } else {
                    SelectCompiler::new()
                };

                // Set starting register/cursor to avoid conflicts
                sub_compiler.set_register_base(dest_reg + 1, self.next_cursor);

                // Compile with Set destination - copies first column to dest_reg
                let sub_dest = SelectDest::Set { reg: dest_reg };
                let sub_ops = sub_compiler.compile(select, &sub_dest)?;

                // Get offsets for adjusting cursor and register numbers
                let cursor_offset = self.next_cursor;
                let base_addr = self.ops.len() as i32;

                // Inline the compiled ops, excluding Init/Halt
                for mut op in sub_ops {
                    if op.opcode == Opcode::Init || op.opcode == Opcode::Halt {
                        continue;
                    }

                    // Adjust jump addresses
                    if op.opcode.is_jump() && op.p2 > 0 {
                        op.p2 += base_addr;
                    }

                    // Adjust cursor numbers for table operations
                    if op.opcode == Opcode::OpenRead || op.opcode == Opcode::OpenWrite {
                        op.p1 += cursor_offset;
                    } else if matches!(
                        op.opcode,
                        Opcode::Rewind
                            | Opcode::Next
                            | Opcode::Column
                            | Opcode::Close
                            | Opcode::SeekGE
                            | Opcode::SeekGT
                            | Opcode::SeekLE
                            | Opcode::SeekLT
                            | Opcode::SeekRowid
                            | Opcode::IdxGE
                            | Opcode::IdxGT
                            | Opcode::IdxLE
                            | Opcode::IdxLT
                            | Opcode::Found
                            | Opcode::NotFound
                            | Opcode::SorterConfig
                            | Opcode::SorterInsert
                            | Opcode::SorterSort
                            | Opcode::SorterNext
                            | Opcode::SorterData
                            | Opcode::OpenEphemeral
                            | Opcode::OpenAutoindex
                    ) {
                        op.p1 += cursor_offset;
                    }

                    self.ops.push(op);
                }

                // Update cursor count to account for cursors used by SelectCompiler
                self.next_cursor += 5;
            }
            Expr::Exists { subquery, negated } => {
                // EXISTS subquery - returns 1 if any rows, 0 otherwise
                // For NOT EXISTS, returns 0 if any rows, 1 otherwise

                // Initialize dest_reg based on negation
                self.emit(
                    Opcode::Integer,
                    if *negated { 1 } else { 0 },
                    dest_reg,
                    0,
                    P4::Unused,
                );

                // Use SelectCompiler to compile the EXISTS subquery
                let mut sub_compiler = if let Some(schema) = self.schema {
                    SelectCompiler::with_schema(schema)
                } else {
                    SelectCompiler::new()
                };

                // Add the table being deleted as an outer table for correlation
                // This allows the subquery to reference columns from the current row
                let table_alias = self
                    .table_alias
                    .clone()
                    .unwrap_or_else(|| self.table_name.clone());
                sub_compiler.add_outer_table(
                    table_alias,
                    self.table_name.clone(),
                    self.table_cursor,
                    self.schema_table.clone(),
                );

                // Set starting register/cursor to avoid conflicts
                sub_compiler.set_register_base(dest_reg + 1, self.next_cursor);

                // Compile with Exists destination
                let sub_dest = SelectDest::Exists { reg: dest_reg };
                let sub_ops = sub_compiler.compile(subquery, &sub_dest)?;

                // Get offsets for adjusting cursor and register numbers
                let cursor_offset = self.next_cursor;
                let base_addr = self.ops.len() as i32;

                // Inline the compiled ops, excluding Init/Halt
                for mut op in sub_ops {
                    if op.opcode == Opcode::Init || op.opcode == Opcode::Halt {
                        continue;
                    }

                    // Adjust jump addresses
                    if op.opcode.is_jump() && op.p2 > 0 {
                        op.p2 += base_addr;
                    }

                    // Adjust cursor numbers for table operations
                    // Don't adjust the outer table cursor (self.table_cursor)
                    if op.opcode == Opcode::OpenRead || op.opcode == Opcode::OpenWrite {
                        op.p1 += cursor_offset;
                    } else if matches!(
                        op.opcode,
                        Opcode::Rewind
                            | Opcode::Next
                            | Opcode::Column
                            | Opcode::Close
                            | Opcode::Rowid
                            | Opcode::SeekGE
                            | Opcode::SeekGT
                            | Opcode::SeekLE
                            | Opcode::SeekLT
                            | Opcode::SeekRowid
                            | Opcode::IdxGE
                            | Opcode::IdxGT
                            | Opcode::IdxLE
                            | Opcode::IdxLT
                            | Opcode::Found
                            | Opcode::NotFound
                            | Opcode::SorterConfig
                            | Opcode::SorterInsert
                            | Opcode::SorterSort
                            | Opcode::SorterNext
                            | Opcode::SorterData
                            | Opcode::OpenEphemeral
                            | Opcode::OpenAutoindex
                    ) {
                        // Don't adjust if it's the outer table cursor
                        if op.p1 != self.table_cursor {
                            op.p1 += cursor_offset;
                        }
                    }

                    self.ops.push(op);
                }

                // If negated, invert the result
                if *negated {
                    self.emit(Opcode::Not, dest_reg, dest_reg, 0, P4::Unused);
                }

                // Update cursor count
                self.next_cursor += 5;
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
                            self.resolve_label(match_label, self.current_addr() as i32);
                            self.emit(
                                Opcode::Integer,
                                if *negated { 0 } else { 1 },
                                dest_reg,
                                0,
                                P4::Unused,
                            );

                            self.resolve_label(end_label, self.current_addr() as i32);
                        }
                    }
                    crate::parser::ast::InList::Subquery(subquery) => {
                        // Compile IN subquery using SelectCompiler
                        // The subcompiler will allocate cursors starting at self.next_cursor
                        let cursor_offset = self.next_cursor;

                        // The ephemeral table cursor will be at cursor_offset
                        // (subcompiler uses cursor 0 internally, which becomes cursor_offset after adjustment)
                        let subq_cursor = cursor_offset;

                        // Compile subquery to ephemeral table
                        let mut sub_compiler = if let Some(schema) = self.schema {
                            SelectCompiler::with_schema(schema)
                        } else {
                            SelectCompiler::new()
                        };

                        // Set starting register/cursor to avoid conflicts with val_reg
                        // Reserve cursor 0 for dest table, subcompiler uses cursors starting at 1
                        // After inlining with offset, cursor 0 becomes cursor_offset (the dest)
                        sub_compiler.set_register_base(self.next_reg, 1);

                        // Process DELETE's WITH clause so CTEs are available to the subquery
                        if let Some(with) = &self.with_clause {
                            sub_compiler.process_with_clause(with)?;
                        }

                        // Compile subquery to fill ephemeral table
                        // Open the dest ephemeral table at cursor 0 (will become cursor_offset after inline)
                        // EphemTable expects the cursor to be already open
                        self.emit(Opcode::OpenEphemeral, subq_cursor, 1, 0, P4::Unused);

                        // Use cursor 0 internally - it will be adjusted to cursor_offset when inlined
                        let sub_dest = SelectDest::EphemTable { cursor: 0 };
                        let sub_ops = sub_compiler.compile(subquery, &sub_dest)?;

                        // Inline the compiled ops, excluding Init/Halt
                        let base_addr = self.ops.len() as i32;
                        for mut op in sub_ops {
                            if op.opcode == Opcode::Init || op.opcode == Opcode::Halt {
                                continue;
                            }

                            // Adjust jump addresses
                            if op.opcode.is_jump() && op.p2 > 0 {
                                op.p2 += base_addr;
                            }

                            // Adjust cursor numbers for all cursor operations
                            if matches!(
                                op.opcode,
                                Opcode::OpenRead
                                    | Opcode::OpenWrite
                                    | Opcode::Rewind
                                    | Opcode::Next
                                    | Opcode::Column
                                    | Opcode::Close
                                    | Opcode::SeekGE
                                    | Opcode::SeekGT
                                    | Opcode::SeekLE
                                    | Opcode::SeekLT
                                    | Opcode::SeekRowid
                                    | Opcode::OpenEphemeral
                                    | Opcode::NewRowid
                                    | Opcode::Insert
                                    | Opcode::Delete
                                    | Opcode::Found
                                    | Opcode::NotFound
                                    | Opcode::IdxGE
                                    | Opcode::IdxGT
                                    | Opcode::IdxLE
                                    | Opcode::IdxLT
                                    | Opcode::IdxInsert
                                    | Opcode::SorterInsert
                                    | Opcode::SorterSort
                                    | Opcode::SorterNext
                                    | Opcode::SorterData
                                    | Opcode::Rowid
                            ) {
                                op.p1 += cursor_offset;
                            }

                            self.ops.push(op);
                        }

                        // Update cursor count based on what the subcompiler used
                        self.next_cursor += 5;

                        // Check if value exists in ephemeral table
                        let record_reg = self.alloc_reg();
                        self.emit(Opcode::MakeRecord, val_reg, 1, record_reg, P4::Unused);

                        let found_label = self.alloc_label();
                        let end_label = self.alloc_label();

                        // Found jumps if record exists in cursor
                        // Note: subq_cursor was allocated from DeleteCompiler's pool, so no offset needed
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
                        self.resolve_label(found_label, self.current_addr() as i32);
                        self.emit(
                            Opcode::Integer,
                            if *negated { 0 } else { 1 },
                            dest_reg,
                            0,
                            P4::Unused,
                        );

                        self.resolve_label(end_label, self.current_addr() as i32);
                    }
                    crate::parser::ast::InList::Table(table_name) => {
                        // IN with table name - not commonly used, default to false
                        self.emit(
                            Opcode::Integer,
                            if *negated { 1 } else { 0 },
                            dest_reg,
                            0,
                            P4::Unused,
                        );
                    }
                }
            }
            _ => {
                // Default to NULL for unsupported expressions
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
            }
        }
        Ok(())
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    fn alloc_reg(&mut self) -> i32 {
        let reg = self.next_reg;
        self.next_reg += 1;
        reg
    }

    fn alloc_cursor(&mut self) -> i32 {
        let cursor = self.next_cursor;
        self.next_cursor += 1;
        cursor
    }

    fn alloc_label(&mut self) -> i32 {
        let label = self.next_label;
        self.next_label -= 1;
        self.labels.insert(label, None);
        label
    }

    fn resolve_label(&mut self, label: i32, addr: i32) {
        self.labels.insert(label, Some(addr));
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
        for op in &mut self.ops {
            if op.opcode.is_jump() && op.p2 < 0 {
                if let Some(Some(addr)) = self.labels.get(&op.p2) {
                    op.p2 = *addr;
                }
            }
        }
        Ok(())
    }

    /// Check if an expression contains a subquery that references the target table
    fn expr_references_target_table(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Subquery(select) => self.select_references_table(select),
            Expr::Binary { left, right, .. } => {
                self.expr_references_target_table(left) || self.expr_references_target_table(right)
            }
            Expr::Unary { expr, .. } => self.expr_references_target_table(expr),
            Expr::Parens(inner) => self.expr_references_target_table(inner),
            Expr::IsNull { expr, .. } => self.expr_references_target_table(expr),
            Expr::Function(func) => {
                if let crate::parser::ast::FunctionArgs::Exprs(args) = &func.args {
                    args.iter().any(|e| self.expr_references_target_table(e))
                } else {
                    false
                }
            }
            Expr::In { expr, list, .. } => {
                let list_refs = match list {
                    crate::parser::ast::InList::Values(vals) => {
                        vals.iter().any(|e| self.expr_references_target_table(e))
                    }
                    crate::parser::ast::InList::Subquery(select) => {
                        self.select_references_table(select)
                    }
                    crate::parser::ast::InList::Table(_) => false,
                };
                self.expr_references_target_table(expr) || list_refs
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.expr_references_target_table(expr)
                    || self.expr_references_target_table(low)
                    || self.expr_references_target_table(high)
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                operand
                    .as_ref()
                    .map(|e| self.expr_references_target_table(e))
                    .unwrap_or(false)
                    || when_clauses.iter().any(|wc| {
                        self.expr_references_target_table(&wc.when)
                            || self.expr_references_target_table(&wc.then)
                    })
                    || else_clause
                        .as_ref()
                        .map(|e| self.expr_references_target_table(e))
                        .unwrap_or(false)
            }
            Expr::Exists { subquery, .. } => self.select_references_table(subquery),
            Expr::Cast { expr, .. } => self.expr_references_target_table(expr),
            _ => false,
        }
    }

    /// Check if a SELECT statement references the target table
    fn select_references_table(&self, select: &crate::parser::ast::SelectStmt) -> bool {
        self.select_body_references_table(&select.body)
    }

    /// Check if a SELECT body references the target table
    fn select_body_references_table(&self, body: &crate::parser::ast::SelectBody) -> bool {
        match body {
            crate::parser::ast::SelectBody::Select(core) => {
                // Check FROM clause
                if let Some(from) = &core.from {
                    if self.from_clause_references_table(from) {
                        return true;
                    }
                }
                false
            }
            crate::parser::ast::SelectBody::Compound { left, right, .. } => {
                self.select_body_references_table(left) || self.select_body_references_table(right)
            }
        }
    }

    /// Check if a FROM clause references the target table
    fn from_clause_references_table(&self, from: &crate::parser::ast::FromClause) -> bool {
        for table_ref in &from.tables {
            if self.table_ref_references_table(table_ref) {
                return true;
            }
        }
        false
    }

    /// Check if a table reference references the target table
    fn table_ref_references_table(&self, table_ref: &crate::parser::ast::TableRef) -> bool {
        match table_ref {
            crate::parser::ast::TableRef::Table { name, .. } => {
                name.name.eq_ignore_ascii_case(&self.target_table)
            }
            crate::parser::ast::TableRef::Subquery { query, .. } => {
                self.select_references_table(query)
            }
            crate::parser::ast::TableRef::Join { left, right, .. } => {
                self.table_ref_references_table(left) || self.table_ref_references_table(right)
            }
            crate::parser::ast::TableRef::TableFunction { .. } => false,
            crate::parser::ast::TableRef::Parens(inner) => self.table_ref_references_table(inner),
        }
    }

    /// Collect all subqueries from an expression that reference the target table
    /// Returns a list of (subquery_index, subquery)
    fn collect_target_table_subqueries<'a>(
        &mut self,
        expr: &'a Expr,
    ) -> Vec<(usize, &'a crate::parser::ast::SelectStmt)> {
        let mut result = Vec::new();
        self.collect_subqueries_recursive(expr, &mut result);
        result
    }

    fn collect_subqueries_recursive<'a>(
        &mut self,
        expr: &'a Expr,
        result: &mut Vec<(usize, &'a crate::parser::ast::SelectStmt)>,
    ) {
        match expr {
            Expr::Subquery(select) => {
                if self.select_references_table(select) {
                    let idx = self.subquery_counter;
                    self.subquery_counter += 1;
                    result.push((idx, select));
                }
            }
            Expr::Binary { left, right, .. } => {
                self.collect_subqueries_recursive(left, result);
                self.collect_subqueries_recursive(right, result);
            }
            Expr::Unary { expr, .. } => {
                self.collect_subqueries_recursive(expr, result);
            }
            Expr::Parens(inner) => {
                self.collect_subqueries_recursive(inner, result);
            }
            Expr::IsNull { expr, .. } => {
                self.collect_subqueries_recursive(expr, result);
            }
            Expr::Function(func) => {
                if let crate::parser::ast::FunctionArgs::Exprs(args) = &func.args {
                    for arg in args {
                        self.collect_subqueries_recursive(arg, result);
                    }
                }
            }
            Expr::In { expr, list, .. } => {
                self.collect_subqueries_recursive(expr, result);
                match list {
                    crate::parser::ast::InList::Values(vals) => {
                        for e in vals {
                            self.collect_subqueries_recursive(e, result);
                        }
                    }
                    crate::parser::ast::InList::Subquery(_) => {
                        // InSelect's select is handled differently (not as scalar subquery)
                    }
                    crate::parser::ast::InList::Table(_) => {}
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.collect_subqueries_recursive(expr, result);
                self.collect_subqueries_recursive(low, result);
                self.collect_subqueries_recursive(high, result);
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(e) = operand {
                    self.collect_subqueries_recursive(e, result);
                }
                for wc in when_clauses {
                    self.collect_subqueries_recursive(&wc.when, result);
                    self.collect_subqueries_recursive(&wc.then, result);
                }
                if let Some(e) = else_clause {
                    self.collect_subqueries_recursive(e, result);
                }
            }
            Expr::Cast { expr, .. } => {
                self.collect_subqueries_recursive(expr, result);
            }
            _ => {}
        }
    }

    /// Precompute subqueries that reference the target table
    /// This must be called BEFORE the main delete loop to ensure stable results
    fn precompute_subqueries(&mut self, where_expr: &Expr) -> Result<()> {
        // Reset counter for this compilation
        self.subquery_counter = 0;

        // Collect subqueries that reference the target table
        let subqueries = self.collect_target_table_subqueries(where_expr);

        for (idx, select) in subqueries {
            // Allocate a register for the precomputed result
            let dest_reg = self.alloc_reg();

            // Initialize to NULL
            self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);

            // Compile the subquery
            let mut sub_compiler = if let Some(schema) = self.schema {
                SelectCompiler::with_schema(schema)
            } else {
                SelectCompiler::new()
            };

            sub_compiler.set_register_base(dest_reg + 1, self.next_cursor);

            let sub_dest = SelectDest::Set { reg: dest_reg };
            let sub_ops = sub_compiler.compile(select, &sub_dest)?;

            let cursor_offset = self.next_cursor;
            let base_addr = self.ops.len() as i32;

            for mut op in sub_ops {
                if op.opcode == Opcode::Init || op.opcode == Opcode::Halt {
                    continue;
                }

                if op.opcode.is_jump() && op.p2 > 0 {
                    op.p2 += base_addr;
                }

                if op.opcode == Opcode::OpenRead || op.opcode == Opcode::OpenWrite {
                    op.p1 += cursor_offset;
                } else if matches!(
                    op.opcode,
                    Opcode::Rewind
                        | Opcode::Next
                        | Opcode::Column
                        | Opcode::Close
                        | Opcode::SeekGE
                        | Opcode::SeekGT
                        | Opcode::SeekLE
                        | Opcode::SeekLT
                        | Opcode::SeekRowid
                        | Opcode::IdxGE
                        | Opcode::IdxGT
                        | Opcode::IdxLE
                        | Opcode::IdxLT
                        | Opcode::Found
                        | Opcode::NotFound
                        | Opcode::SorterConfig
                        | Opcode::SorterInsert
                        | Opcode::SorterSort
                        | Opcode::SorterNext
                        | Opcode::SorterData
                        | Opcode::OpenEphemeral
                        | Opcode::OpenAutoindex
                ) {
                    op.p1 += cursor_offset;
                }

                self.ops.push(op);
            }

            self.next_cursor += 5;

            // Store the mapping from subquery index to result register
            self.precomputed_subqueries.insert(idx, dest_reg);
        }

        Ok(())
    }

    /// Open cursors for all indexes on the table
    fn open_indexes_for_write(&mut self, table_name: &str) -> Result<()> {
        // Get indexes from schema
        if let Some(schema) = self.schema {
            let table_name_lower = table_name.to_lowercase();

            // First check schema.indexes for indexes on this table
            for (_name, idx) in schema.indexes.iter() {
                if idx.table.eq_ignore_ascii_case(&table_name_lower) {
                    let cursor = self.alloc_cursor();
                    self.emit(
                        Opcode::OpenWrite,
                        cursor,
                        0, // Root page comes from schema lookup at runtime
                        0,
                        P4::Text(idx.name.clone()),
                    );

                    let columns: Vec<i32> = idx.columns.iter().map(|c| c.column_idx).collect();
                    self.index_cursors.push(IndexCursor {
                        cursor,
                        columns,
                        name: idx.name.clone(),
                    });
                }
            }

            // Also check table.indexes
            if let Some(table) = schema.tables.get(&table_name_lower) {
                for idx in &table.indexes {
                    // Skip if already added
                    if self
                        .index_cursors
                        .iter()
                        .any(|ic| ic.name.eq_ignore_ascii_case(&idx.name))
                    {
                        continue;
                    }

                    let cursor = self.alloc_cursor();
                    self.emit(Opcode::OpenWrite, cursor, 0, 0, P4::Text(idx.name.clone()));

                    let columns: Vec<i32> = idx.columns.iter().map(|c| c.column_idx).collect();
                    self.index_cursors.push(IndexCursor {
                        cursor,
                        columns,
                        name: idx.name.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Emit index delete operations before deleting a row
    /// rowid_reg is the register containing the rowid
    fn emit_index_deletes(&mut self, rowid_reg: i32) {
        // Clone index_cursors to avoid borrow issues
        let index_cursors: Vec<_> = self
            .index_cursors
            .iter()
            .map(|ic| (ic.cursor, ic.columns.clone()))
            .collect();

        for (cursor, columns) in index_cursors {
            // Build index key: indexed columns + rowid
            let key_base = self.alloc_regs(columns.len() + 1);

            // Read indexed columns from the table cursor
            for (i, col_idx) in columns.iter().enumerate() {
                if *col_idx >= 0 {
                    // Read column value from table
                    self.emit(
                        Opcode::Column,
                        self.table_cursor,
                        *col_idx,
                        key_base + i as i32,
                        P4::Unused,
                    );
                } else {
                    // Expression index - not supported yet, use null
                    self.emit(Opcode::Null, 0, key_base + i as i32, 0, P4::Unused);
                }
            }

            // Copy rowid as the last key component
            let rowid_pos = key_base + columns.len() as i32;
            self.emit(Opcode::Copy, rowid_reg, rowid_pos, 0, P4::Unused);

            // Make the index record (key to find and delete)
            let record_reg = self.alloc_reg();
            self.emit(
                Opcode::MakeRecord,
                key_base,
                (columns.len() + 1) as i32,
                record_reg,
                P4::Unused,
            );

            // Delete from index
            self.emit(Opcode::IdxDelete, cursor, record_reg, 0, P4::Unused);
        }
    }

    fn alloc_regs(&mut self, n: usize) -> i32 {
        let base = self.next_reg;
        self.next_reg += n as i32;
        base
    }
}

impl Default for DeleteCompiler<'_> {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Compile a DELETE statement to VDBE opcodes
pub fn compile_delete(delete: &DeleteStmt) -> Result<Vec<VdbeOp>> {
    let mut compiler = DeleteCompiler::new();
    compiler.compile(delete)
}

/// Compile a DELETE statement to VDBE opcodes with schema access
pub fn compile_delete_with_schema(
    delete: &DeleteStmt,
    schema: &crate::schema::Schema,
) -> Result<Vec<VdbeOp>> {
    let mut compiler = DeleteCompiler::with_schema(schema);
    compiler.compile(delete)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{BinaryOp, ColumnRef, Literal, QualifiedName};

    #[test]
    fn test_delete_compiler_new() {
        let compiler = DeleteCompiler::new();
        assert!(compiler.ops.is_empty());
        assert_eq!(compiler.next_reg, 1);
    }

    #[test]
    fn test_compile_delete_all() {
        // DELETE FROM users
        let delete = DeleteStmt {
            with: None,
            table: QualifiedName::new("users"),
            alias: None,
            indexed_by: None,
            where_clause: None,
            returning: None,
            order_by: None,
            limit: None,
        };

        let ops = compile_delete(&delete).unwrap();
        assert!(!ops.is_empty());

        // Should have Init, OpenWrite, Rewind, Delete, Next, Close, Halt
        assert!(ops.iter().any(|op| op.opcode == Opcode::Init));
        assert!(ops.iter().any(|op| op.opcode == Opcode::OpenWrite));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Rewind));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Delete));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Next));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Close));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Halt));
    }

    #[test]
    fn test_compile_delete_with_where() {
        // DELETE FROM users WHERE id = 1
        let delete = DeleteStmt {
            with: None,
            table: QualifiedName::new("users"),
            alias: None,
            indexed_by: None,
            where_clause: Some(Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Column(ColumnRef {
                    database: None,
                    table: None,
                    column: "id".to_string(),
                    column_index: None,
                })),
                right: Box::new(Expr::Literal(Literal::Integer(1))),
            })),
            returning: None,
            order_by: None,
            limit: None,
        };

        let ops = compile_delete(&delete).unwrap();
        assert!(!ops.is_empty());

        // Should have IfNot for WHERE check
        assert!(ops.iter().any(|op| op.opcode == Opcode::IfNot));
        // Should have Eq for the WHERE comparison
        assert!(ops.iter().any(|op| op.opcode == Opcode::Eq));
        // Should have Delete
        assert!(ops.iter().any(|op| op.opcode == Opcode::Delete));
    }

    #[test]
    fn test_compile_delete_with_limit() {
        // DELETE FROM users LIMIT 10
        use crate::parser::ast::LimitClause;

        let delete = DeleteStmt {
            with: None,
            table: QualifiedName::new("users"),
            alias: None,
            indexed_by: None,
            where_clause: None,
            returning: None,
            order_by: None,
            limit: Some(LimitClause {
                limit: Box::new(Expr::Literal(Literal::Integer(10))),
                offset: None,
            }),
        };

        let ops = compile_delete(&delete).unwrap();
        assert!(!ops.is_empty());

        // Should have OpenEphemeral for collecting rowids
        assert!(ops.iter().any(|op| op.opcode == Opcode::OpenEphemeral));
        // Should have Delete
        assert!(ops.iter().any(|op| op.opcode == Opcode::Delete));
    }

    #[test]
    fn test_compile_delete_where_with_and() {
        // DELETE FROM users WHERE active = 0 AND created < 1000
        let delete = DeleteStmt {
            with: None,
            table: QualifiedName::new("users"),
            alias: None,
            indexed_by: None,
            where_clause: Some(Box::new(Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(Expr::Column(ColumnRef {
                        database: None,
                        table: None,
                        column: "active".to_string(),
                        column_index: None,
                    })),
                    right: Box::new(Expr::Literal(Literal::Integer(0))),
                }),
                right: Box::new(Expr::Binary {
                    op: BinaryOp::Lt,
                    left: Box::new(Expr::Column(ColumnRef {
                        database: None,
                        table: None,
                        column: "created".to_string(),
                        column_index: None,
                    })),
                    right: Box::new(Expr::Literal(Literal::Integer(1000))),
                }),
            })),
            returning: None,
            order_by: None,
            limit: None,
        };

        let ops = compile_delete(&delete).unwrap();
        assert!(!ops.is_empty());

        // Should have And opcode
        assert!(ops.iter().any(|op| op.opcode == Opcode::And));
        // Should have multiple comparison opcodes
        assert!(ops.iter().any(|op| op.opcode == Opcode::Eq));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Lt));
    }

    #[test]
    fn test_compile_delete_where_is_null() {
        // DELETE FROM users WHERE email IS NULL
        let delete = DeleteStmt {
            with: None,
            table: QualifiedName::new("users"),
            alias: None,
            indexed_by: None,
            where_clause: Some(Box::new(Expr::IsNull {
                expr: Box::new(Expr::Column(ColumnRef {
                    database: None,
                    table: None,
                    column: "email".to_string(),
                    column_index: None,
                })),
                negated: false,
            })),
            returning: None,
            order_by: None,
            limit: None,
        };

        let ops = compile_delete(&delete).unwrap();
        assert!(!ops.is_empty());

        // Should have IsNull opcode
        assert!(ops.iter().any(|op| op.opcode == Opcode::IsNull));
    }

    #[test]
    fn test_compile_delete_table_name_in_opcode() {
        let delete = DeleteStmt {
            with: None,
            table: QualifiedName::new("my_table"),
            alias: None,
            indexed_by: None,
            where_clause: None,
            returning: None,
            order_by: None,
            limit: None,
        };

        let ops = compile_delete(&delete).unwrap();

        // Check that OpenWrite has the table name
        let open_write_op = ops.iter().find(|op| op.opcode == Opcode::OpenWrite);
        assert!(open_write_op.is_some());
        if let Some(op) = open_write_op {
            if let P4::Text(name) = &op.p4 {
                assert_eq!(name, "my_table");
            } else {
                panic!("Expected P4::Text");
            }
        }
    }
}
