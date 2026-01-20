//! UPDATE statement compilation
//!
//! This module compiles UPDATE statements to VDBE bytecode.
//! Corresponds to update.c in SQLite.
//!
//! Uses SQLite's two-phase UPDATE pattern:
//! Phase 1: Scan table and collect rowids into an ephemeral table
//! Phase 2: Iterate through ephemeral table and update each row
//!
//! This prevents hangs caused by modifying the btree while iterating it.

use std::collections::HashMap;

use crate::error::Result;
use crate::parser::ast::{ConflictAction, Expr, ResultColumn, UpdateStmt};
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

/// Flag to indicate that this operation should update the change counter
const OPFLAG_NCHANGE: u16 = 0x01;

fn is_rowid_alias(name: &str) -> bool {
    name.eq_ignore_ascii_case("rowid")
        || name.eq_ignore_ascii_case("_rowid_")
        || name.eq_ignore_ascii_case("oid")
}

// ============================================================================
// UpdateCompiler
// ============================================================================

/// Compiles UPDATE statements to VDBE opcodes
pub struct UpdateCompiler<'s> {
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

    /// Table cursor (for expression compilation)
    table_cursor: i32,

    /// Number of columns in target table
    num_columns: usize,

    /// Column name to index mapping (lowercase name -> index)
    column_map: HashMap<String, usize>,

    /// Schema for name resolution (optional)
    schema: Option<&'s crate::schema::Schema>,

    /// Table name being updated
    table_name: String,

    /// Table alias (e.g., UPDATE t1 AS xyz -> alias is "xyz")
    table_alias: Option<String>,

    /// Base register where column values are loaded (for expression compilation)
    /// When set, column references should use these registers instead of emitting Column opcodes
    column_data_base: Option<i32>,
}

impl<'s> UpdateCompiler<'s> {
    /// Create a new UPDATE compiler
    pub fn new() -> Self {
        UpdateCompiler {
            ops: Vec::new(),
            next_reg: 1,
            next_cursor: 0,
            next_label: -1,
            labels: HashMap::new(),
            table_cursor: 0,
            num_columns: 0,
            column_map: HashMap::new(),
            schema: None,
            table_name: String::new(),
            table_alias: None,
            column_data_base: None,
        }
    }

    /// Create a new UPDATE compiler with schema access
    pub fn with_schema(schema: &'s crate::schema::Schema) -> Self {
        UpdateCompiler {
            ops: Vec::new(),
            next_reg: 1,
            next_cursor: 0,
            next_label: -1,
            labels: HashMap::new(),
            table_cursor: 0,
            num_columns: 0,
            column_map: HashMap::new(),
            schema: Some(schema),
            table_name: String::new(),
            table_alias: None,
            column_data_base: None,
        }
    }

    /// Compile an UPDATE statement
    pub fn compile(&mut self, update: &UpdateStmt) -> Result<Vec<VdbeOp>> {
        // Store table name and alias for later reference
        self.table_name = update.table.name.clone();
        self.table_alias = update.alias.clone();

        // Check for system tables that cannot be modified
        let table_name_lower = update.table.name.to_lowercase();
        if table_name_lower == "sqlite_master"
            || table_name_lower == "sqlite_schema"
            || table_name_lower == "sqlite_temp_master"
            || table_name_lower == "sqlite_temp_schema"
        {
            return Err(crate::error::Error::with_message(
                crate::error::ErrorCode::Error,
                format!("table {} may not be modified", update.table.name),
            ));
        }

        // Look up table info from schema
        let (root_page, num_columns) = self.lookup_table_info(&update.table.name)?;
        self.num_columns = num_columns;

        // Build column map from schema
        self.build_column_map_from_schema(&update.table.name);

        // Validate all column names in assignments exist
        if self.schema.is_some() {
            for assignment in &update.assignments {
                // Validate the target column(s)
                for col_name in &assignment.columns {
                    if self.get_column_index(col_name).is_none() {
                        return Err(crate::error::Error::with_message(
                            crate::error::ErrorCode::Error,
                            format!("no such column: {}", col_name),
                        ));
                    }
                }
                // Validate column references in the expression
                self.validate_expr_columns(&assignment.expr)?;
            }
            // Validate column references in WHERE clause
            if let Some(ref where_expr) = update.where_clause {
                self.validate_expr_columns(where_expr)?;
            }
        }

        // Initialize
        self.emit(Opcode::Init, 0, 0, 0, P4::Unused);

        // Handle conflict action
        let conflict_action = update.or_action.unwrap_or(ConflictAction::Abort);

        // Compile the two-phase UPDATE
        self.compile_update_two_phase(update, root_page, conflict_action)?;

        // Handle RETURNING clause
        if let Some(returning) = &update.returning {
            self.compile_returning(returning)?;
        }

        // Halt
        self.emit(Opcode::Halt, 0, 0, 0, P4::Unused);

        // Resolve labels
        self.resolve_labels()?;

        Ok(std::mem::take(&mut self.ops))
    }

    /// Compile a two-phase UPDATE following SQLite's pattern
    ///
    /// Phase 1: Collection
    ///   - Open read cursor on table
    ///   - Open ephemeral table for rowid storage
    ///   - Scan table, check WHERE, store matching rowids in ephemeral
    ///   - Close read cursor
    ///
    /// Phase 2: Update
    ///   - Open write cursor on table
    ///   - Iterate through ephemeral table
    ///   - For each rowid: seek, read, modify, delete old, insert new
    ///   - Close cursors
    fn compile_update_two_phase(
        &mut self,
        update: &UpdateStmt,
        root_page: u32,
        conflict_action: ConflictAction,
    ) -> Result<()> {
        // Allocate cursors
        let read_cursor = self.alloc_cursor(); // For phase 1 scanning
        let eph_cursor = self.alloc_cursor(); // Ephemeral table for rowids
        let write_cursor = self.alloc_cursor(); // For phase 2 modifications

        // Store write cursor as the main table cursor for expression compilation
        self.table_cursor = write_cursor;

        // Register to store rowid during collection
        let rowid_reg = self.alloc_reg();

        // Labels
        let phase1_loop_start = self.alloc_label();
        let phase1_loop_end = self.alloc_label();
        let phase2_loop_start = self.alloc_label();
        let phase2_loop_end = self.alloc_label();
        let phase2_continue = self.alloc_label();

        // ========================================================================
        // Phase 1: Collection - Scan table and collect rowids
        // ========================================================================

        // Open table for reading
        self.emit(
            Opcode::OpenRead,
            read_cursor,
            root_page as i32,
            self.num_columns as i32,
            P4::Text(update.table.name.clone()),
        );

        // Open ephemeral table to store rowids
        // P2 = number of columns in ephemeral table (1 for rowid)
        self.emit(Opcode::OpenEphemeral, eph_cursor, 1, 0, P4::Unused);

        // Rewind read cursor to start of table
        self.emit(Opcode::Rewind, read_cursor, phase1_loop_end, 0, P4::Unused);

        // Phase 1 loop start
        self.resolve_label(phase1_loop_start, self.current_addr() as i32);

        // Check WHERE clause if present
        if let Some(where_expr) = &update.where_clause {
            let skip_label = self.alloc_label();

            // Temporarily use read cursor for WHERE evaluation
            let saved_cursor = self.table_cursor;
            self.table_cursor = read_cursor;

            let cond_reg = self.alloc_reg();
            self.compile_expr(where_expr, cond_reg)?;

            // Restore cursor
            self.table_cursor = saved_cursor;

            // Jump to skip if condition is false
            self.emit(Opcode::IfNot, cond_reg, skip_label, 1, P4::Unused);

            // Get rowid and store in ephemeral table
            self.emit(Opcode::Rowid, read_cursor, rowid_reg, 0, P4::Unused);

            // Make a record containing just the rowid
            let record_reg = self.alloc_reg();
            self.emit(Opcode::MakeRecord, rowid_reg, 1, record_reg, P4::Unused);

            // Insert into ephemeral table (rowid as key, record as data)
            self.emit(
                Opcode::Insert,
                eph_cursor,
                record_reg,
                rowid_reg,
                P4::Unused,
            );

            // Skip label
            self.resolve_label(skip_label, self.current_addr() as i32);
        } else {
            // No WHERE - collect all rowids
            self.emit(Opcode::Rowid, read_cursor, rowid_reg, 0, P4::Unused);

            let record_reg = self.alloc_reg();
            self.emit(Opcode::MakeRecord, rowid_reg, 1, record_reg, P4::Unused);
            self.emit(
                Opcode::Insert,
                eph_cursor,
                record_reg,
                rowid_reg,
                P4::Unused,
            );
        }

        // Move to next row in read cursor
        self.emit(Opcode::Next, read_cursor, phase1_loop_start, 0, P4::Unused);

        // Phase 1 loop end
        self.resolve_label(phase1_loop_end, self.current_addr() as i32);

        // Close read cursor - we're done scanning
        self.emit(Opcode::Close, read_cursor, 0, 0, P4::Unused);

        // ========================================================================
        // Phase 2: Update - Iterate through collected rowids and update
        // ========================================================================

        // Open table for writing
        self.emit(
            Opcode::OpenWrite,
            write_cursor,
            root_page as i32,
            self.num_columns as i32,
            P4::Text(update.table.name.clone()),
        );

        // Rewind ephemeral cursor to start
        self.emit(Opcode::Rewind, eph_cursor, phase2_loop_end, 0, P4::Unused);

        // Phase 2 loop start
        self.resolve_label(phase2_loop_start, self.current_addr() as i32);

        // Get the rowid from ephemeral table
        // The rowid was used as the key when inserting, so we can get it with Rowid
        let update_rowid_reg = self.alloc_reg();
        self.emit(Opcode::Rowid, eph_cursor, update_rowid_reg, 0, P4::Unused);

        // Seek to the row using NotExists
        // If row doesn't exist (deleted by another operation), skip to next
        self.emit(
            Opcode::NotExists,
            write_cursor,
            phase2_continue,
            update_rowid_reg,
            P4::Unused,
        );

        // Now cursor is positioned at the row - perform the update
        self.compile_row_update_phase2(update, write_cursor, update_rowid_reg, conflict_action)?;

        // Phase 2 continue label (skip point for NotExists)
        self.resolve_label(phase2_continue, self.current_addr() as i32);

        // Move to next row in ephemeral cursor
        self.emit(Opcode::Next, eph_cursor, phase2_loop_start, 0, P4::Unused);

        // Phase 2 loop end
        self.resolve_label(phase2_loop_end, self.current_addr() as i32);

        // Close cursors
        self.emit(Opcode::Close, write_cursor, 0, 0, P4::Unused);
        self.emit(Opcode::Close, eph_cursor, 0, 0, P4::Unused);

        Ok(())
    }

    /// Compile code to update a single row in phase 2
    /// The cursor is already positioned at the row via NotExists
    fn compile_row_update_phase2(
        &mut self,
        update: &UpdateStmt,
        cursor: i32,
        rowid_reg: i32,
        conflict_action: ConflictAction,
    ) -> Result<()> {
        // Allocate registers for all column values
        let data_base = self.next_reg;
        let _data_regs = self.alloc_regs(self.num_columns);

        // Read all current column values from the positioned cursor
        for i in 0..self.num_columns {
            let reg = data_base + i as i32;
            self.emit(Opcode::Column, cursor, i as i32, reg, P4::Unused);
        }

        // Set column_data_base so compile_expr knows to use registers for column refs
        self.column_data_base = Some(data_base);

        // Two-phase assignment to handle cases like SET F2=f1, F1=f2 correctly:
        // Phase 1: Compile all expressions to temporary registers (using ORIGINAL values)
        // Phase 2: Copy from temp registers to data registers
        // This ensures all expressions see the original values, not partially modified ones.
        let mut temp_assignments: Vec<(i32, i32)> = Vec::new(); // (dest_col_idx, temp_reg)

        for assignment in &update.assignments {
            for col_name in &assignment.columns {
                if let Some(col_idx) = self.get_column_index(col_name) {
                    // Column found in schema - compile to a temp register
                    let temp_reg = self.alloc_reg();
                    self.compile_expr(&assignment.expr, temp_reg)?;
                    temp_assignments.push((col_idx as i32, temp_reg));
                } else if self.schema.is_none() {
                    // No schema available - use a fallback register
                    let fallback_idx = assignment
                        .columns
                        .iter()
                        .position(|c| c == col_name)
                        .unwrap_or(0) as i32;
                    let temp_reg = self.alloc_reg();
                    self.compile_expr(&assignment.expr, temp_reg)?;
                    temp_assignments.push((fallback_idx, temp_reg));
                } else {
                    // Schema exists but column not found - this is an error
                    return Err(crate::error::Error::with_message(
                        crate::error::ErrorCode::Error,
                        format!("no such column: {}", col_name),
                    ));
                }
            }
        }

        // Phase 2: Copy temp values to data registers
        // Also track which columns are being updated for UNIQUE checks
        let mut updated_columns: Vec<usize> = Vec::new();
        for (col_idx, temp_reg) in temp_assignments {
            let dest_reg = data_base + col_idx;
            self.emit(Opcode::SCopy, temp_reg, dest_reg, 0, P4::Unused);
            updated_columns.push(col_idx as usize);
        }

        // Clear column_data_base since we're done with expressions
        self.column_data_base = None;

        // Check UNIQUE constraints before making changes
        self.emit_unique_constraint_checks(
            cursor,
            rowid_reg,
            data_base,
            &updated_columns,
            conflict_action,
        )?;

        // Handle conflict action
        self.emit_conflict_check(conflict_action)?;

        // Delete the old row
        self.emit(Opcode::Delete, cursor, 0, 0, P4::Unused);

        // Make new record from updated values
        let record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            data_base,
            self.num_columns as i32,
            record_reg,
            P4::Unused,
        );

        // Insert the updated row with same rowid
        // Set OPFLAG_NCHANGE so the changes counter is updated
        let flags = self.conflict_flags(conflict_action);
        self.emit_with_p5(
            Opcode::Insert,
            cursor,
            record_reg,
            rowid_reg,
            P4::Int64(flags),
            OPFLAG_NCHANGE,
        );

        Ok(())
    }

    /// Look up table info from schema
    fn lookup_table_info(&self, table_name: &str) -> Result<(u32, usize)> {
        if let Some(schema) = self.schema {
            let table_name_lower = table_name.to_lowercase();
            if let Some(table) = schema.tables.get(&table_name_lower) {
                return Ok((table.root_page, table.columns.len()));
            }
            // Table not found in schema - return error
            return Err(crate::error::Error::with_message(
                crate::error::ErrorCode::Error,
                format!("no such table: {}", table_name),
            ));
        }
        // No schema available - use defaults (root page 0 means runtime lookup)
        Ok((0, 10)) // Default to 10 columns max
    }

    /// Build column map from schema
    fn build_column_map_from_schema(&mut self, table_name: &str) {
        self.column_map.clear();
        if let Some(schema) = self.schema {
            let table_name_lower = table_name.to_lowercase();
            if let Some(table) = schema.tables.get(&table_name_lower) {
                for (i, col) in table.columns.iter().enumerate() {
                    self.column_map.insert(col.name.to_lowercase(), i);
                }
                return;
            }
        }
        // No schema - create placeholder entries
        for i in 0..self.num_columns {
            self.column_map.insert(format!("col{}", i), i);
        }
    }

    /// Get column index by name
    fn get_column_index(&self, name: &str) -> Option<usize> {
        let name_lower = name.to_lowercase();
        self.column_map.get(&name_lower).copied()
    }

    /// Validate that all column references in an expression exist
    fn validate_expr_columns(&self, expr: &Expr) -> Result<()> {
        match expr {
            Expr::Column(col_ref) => {
                // Check for table-qualified column (e.g., test2.f1 or xyz.f1)
                if let Some(ref prefix) = col_ref.table {
                    // The prefix must match either the table name or the alias
                    let prefix_lower = prefix.to_lowercase();
                    let matches_table = prefix_lower == self.table_name.to_lowercase();
                    let matches_alias = self
                        .table_alias
                        .as_ref()
                        .map(|a| a.to_lowercase() == prefix_lower)
                        .unwrap_or(false);

                    if !matches_table && !matches_alias {
                        return Err(crate::error::Error::with_message(
                            crate::error::ErrorCode::Error,
                            format!("no such column: {}.{}", prefix, col_ref.column),
                        ));
                    }
                }

                // Skip rowid aliases - they're always valid
                if is_rowid_alias(&col_ref.column) {
                    return Ok(());
                }

                // Check if column exists in the table
                if self.get_column_index(&col_ref.column).is_none() {
                    // Format error message with table prefix if present
                    let col_name = if let Some(ref table_name) = col_ref.table {
                        format!("{}.{}", table_name, col_ref.column)
                    } else {
                        col_ref.column.clone()
                    };
                    return Err(crate::error::Error::with_message(
                        crate::error::ErrorCode::Error,
                        format!("no such column: {}", col_name),
                    ));
                }
                Ok(())
            }
            Expr::Binary { left, right, .. } => {
                self.validate_expr_columns(left)?;
                self.validate_expr_columns(right)
            }
            Expr::Unary { expr: inner, .. } => self.validate_expr_columns(inner),
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
                // Validate function arguments
                if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                    for arg in exprs {
                        self.validate_expr_columns(arg)?;
                    }
                }
                Ok(())
            }
            Expr::IsNull { expr: inner, .. } => self.validate_expr_columns(inner),
            Expr::Between {
                expr, low, high, ..
            } => {
                self.validate_expr_columns(expr)?;
                self.validate_expr_columns(low)?;
                self.validate_expr_columns(high)
            }
            Expr::In { expr, list, .. } => {
                self.validate_expr_columns(expr)?;
                if let crate::parser::ast::InList::Values(values) = list {
                    for v in values {
                        self.validate_expr_columns(v)?;
                    }
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
                if let Some(else_expr) = else_clause {
                    self.validate_expr_columns(else_expr)?;
                }
                Ok(())
            }
            Expr::Cast { expr: inner, .. } => self.validate_expr_columns(inner),
            Expr::Collate { expr: inner, .. } => self.validate_expr_columns(inner),
            // Literals, parameters, and other expressions don't have column references
            _ => Ok(()),
        }
    }

    /// Compile RETURNING clause
    fn compile_returning(&mut self, returning: &[ResultColumn]) -> Result<()> {
        let base_reg = self.next_reg;

        for (i, col) in returning.iter().enumerate() {
            let reg = self.alloc_reg();
            match col {
                ResultColumn::Star => {
                    // Return all columns
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

    /// Emit conflict checking code
    fn emit_conflict_check(&mut self, action: ConflictAction) -> Result<()> {
        match action {
            ConflictAction::Abort => {
                // Default behavior - abort on constraint violation
            }
            ConflictAction::Rollback => {
                // Will be handled by the Insert opcode flags
            }
            ConflictAction::Fail => {
                // Will be handled by the Insert opcode flags
            }
            ConflictAction::Ignore => {
                // Skip row on conflict
            }
            ConflictAction::Replace => {
                // Delete existing row with same key
            }
        }
        Ok(())
    }

    /// Emit UNIQUE constraint checking code
    /// For each UNIQUE index/constraint that involves columns being updated,
    /// generate code to scan the table and check for duplicate values
    fn emit_unique_constraint_checks(
        &mut self,
        _write_cursor: i32,
        rowid_reg: i32,
        data_base: i32,
        updated_columns: &[usize],
        conflict_action: ConflictAction,
    ) -> Result<()> {
        let schema = match self.schema {
            Some(s) => s,
            None => return Ok(()), // No schema, can't check constraints
        };

        let table = match schema.tables.get(&self.table_name.to_lowercase()) {
            Some(t) => t.clone(),
            None => return Ok(()), // Table not found in schema
        };

        // Find all UNIQUE indexes on this table
        let mut unique_checks: Vec<(String, Vec<usize>)> = Vec::new(); // (index_name, column_indices)

        for index in &table.indexes {
            if !index.unique {
                continue;
            }

            // Check if any of the indexed columns are being updated
            let indexed_col_indices: Vec<usize> = index
                .columns
                .iter()
                .filter_map(|ic| {
                    if ic.column_idx >= 0 {
                        Some(ic.column_idx as usize)
                    } else {
                        None
                    }
                })
                .collect();

            let has_updated_column = indexed_col_indices
                .iter()
                .any(|idx| updated_columns.contains(idx));

            if has_updated_column {
                unique_checks.push((index.name.clone(), indexed_col_indices));
            }
        }

        // Also check for UNIQUE constraints defined directly on columns (column.is_unique)
        // and PRIMARY KEY columns (which also imply uniqueness)
        for (col_idx, col) in table.columns.iter().enumerate() {
            if !updated_columns.contains(&col_idx) {
                continue;
            }

            // Check if column has direct UNIQUE constraint or is PRIMARY KEY
            if col.is_unique || col.is_primary_key {
                // Check if already covered by an explicit unique index
                let has_unique_index = unique_checks
                    .iter()
                    .any(|(_, cols)| cols.len() == 1 && cols[0] == col_idx);

                if !has_unique_index {
                    // Add this column's UNIQUE constraint to checks
                    unique_checks.push((col.name.clone(), vec![col_idx]));
                }
            }
        }

        // Generate constraint checking code for each unique constraint
        // that involves updated columns
        let (root_page, _) = self.lookup_table_info(&self.table_name)?;

        for (idx_name, col_indices) in unique_checks {
            // Determine column name(s) for error message
            // Format: "table.col" for single column, "table.col1, table.col2" for multiple
            let col_name = if col_indices.len() == 1 {
                table
                    .columns
                    .get(col_indices[0])
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| "?".to_string())
            } else {
                col_indices
                    .iter()
                    .filter_map(|i| table.columns.get(*i))
                    .map(|c| format!("{}.{}", self.table_name, c.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            // For multi-column constraints, format is already "table.col1, table.col2",
            // so we need to adjust the error message format
            let error_col_name = if col_indices.len() == 1 {
                format!("{}.{}", self.table_name, col_name)
            } else {
                col_name.clone() // Already has table prefix on each column
            };

            // Open a read cursor to scan for duplicates
            let check_cursor = self.alloc_cursor();
            self.emit(
                Opcode::OpenRead,
                check_cursor,
                root_page as i32,
                self.num_columns as i32,
                P4::Text(self.table_name.clone()),
            );

            let scan_start = self.alloc_label();
            let scan_end = self.alloc_label();
            let conflict_label = self.alloc_label();
            let no_conflict_label = self.alloc_label();

            // Rewind to start of table
            self.emit(Opcode::Rewind, check_cursor, scan_end, 0, P4::Unused);

            // Scan loop
            self.resolve_label(scan_start, self.current_addr() as i32);

            // Get rowid of current row
            let check_rowid_reg = self.alloc_reg();
            self.emit(Opcode::Rowid, check_cursor, check_rowid_reg, 0, P4::Unused);

            // Skip if same rowid (we're updating this row, so it's ok)
            self.emit(
                Opcode::Eq,
                rowid_reg,
                no_conflict_label,
                check_rowid_reg,
                P4::Unused,
            );

            // Check if all UNIQUE columns match
            // For single-column UNIQUE: just compare
            // For multi-column UNIQUE: all must match
            let mut all_match_label = conflict_label;
            for (i, &col_idx) in col_indices.iter().enumerate() {
                let check_val_reg = self.alloc_reg();
                self.emit(
                    Opcode::Column,
                    check_cursor,
                    col_idx as i32,
                    check_val_reg,
                    P4::Unused,
                );

                // Compare with new value (in data_base + col_idx)
                let new_val_reg = data_base + col_idx as i32;

                if i == col_indices.len() - 1 {
                    // Last column - if equal, we have a conflict
                    self.emit(
                        Opcode::Eq,
                        new_val_reg,
                        all_match_label,
                        check_val_reg,
                        P4::Unused,
                    );
                } else {
                    // Not last column - if not equal, no conflict for this row
                    self.emit(
                        Opcode::Ne,
                        new_val_reg,
                        no_conflict_label,
                        check_val_reg,
                        P4::Unused,
                    );
                }
            }

            // No conflict for this row - move to next
            self.resolve_label(no_conflict_label, self.current_addr() as i32);
            self.emit(Opcode::Next, check_cursor, scan_start, 0, P4::Unused);

            // End of scan - close cursor and continue
            self.resolve_label(scan_end, self.current_addr() as i32);
            self.emit(Opcode::Close, check_cursor, 0, 0, P4::Unused);

            // Jump past the conflict handler
            let after_conflict = self.alloc_label();
            self.emit(Opcode::Goto, 0, after_conflict, 0, P4::Unused);

            // Conflict detected - handle according to action
            self.resolve_label(conflict_label, self.current_addr() as i32);
            self.emit(Opcode::Close, check_cursor, 0, 0, P4::Unused);

            match conflict_action {
                ConflictAction::Abort | ConflictAction::Rollback | ConflictAction::Fail => {
                    // Raise UNIQUE constraint error
                    let error_msg = format!("UNIQUE constraint failed: {}", error_col_name);
                    self.emit(
                        Opcode::Halt,
                        crate::error::ErrorCode::Constraint as i32,
                        2, // OE_Abort
                        0,
                        P4::Text(error_msg),
                    );
                }
                ConflictAction::Ignore => {
                    // Skip this row - jump past the delete/insert
                    // This needs to jump to the next row in the update loop
                    // For now, we'll just skip to after_conflict
                }
                ConflictAction::Replace => {
                    // Need to delete the conflicting row
                    // This is complex - for now just abort
                    let error_msg = format!("UNIQUE constraint failed: {}", error_col_name);
                    self.emit(
                        Opcode::Halt,
                        crate::error::ErrorCode::Constraint as i32,
                        2, // OE_Abort
                        0,
                        P4::Text(error_msg),
                    );
                }
            }

            self.resolve_label(after_conflict, self.current_addr() as i32);
        }

        Ok(())
    }

    /// Get Insert opcode flags for conflict action
    fn conflict_flags(&self, action: ConflictAction) -> i64 {
        match action {
            ConflictAction::Abort => 0,
            ConflictAction::Rollback => 1,
            ConflictAction::Fail => 2,
            ConflictAction::Ignore => 3,
            ConflictAction::Replace => 4,
        }
    }

    /// Compile an expression
    fn compile_expr(&mut self, expr: &Expr, dest_reg: i32) -> Result<()> {
        match expr {
            Expr::Literal(lit) => match lit {
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
                // Column reference - look up in current row
                if is_rowid_alias(&col_ref.column) {
                    self.emit(Opcode::Rowid, self.table_cursor, dest_reg, 0, P4::Unused);
                    return Ok(());
                }

                // Try to get column index from AST, then column map
                let col_idx = if let Some(idx) = col_ref.column_index {
                    idx
                } else if let Some(idx) = self.get_column_index(&col_ref.column) {
                    idx as i32
                } else {
                    // Column not found - emit Column opcode with name in P4 for runtime resolution
                    self.emit(
                        Opcode::Column,
                        self.table_cursor,
                        0, // Will be resolved at runtime via P4
                        dest_reg,
                        P4::Text(col_ref.column.clone()),
                    );
                    return Ok(());
                };

                if col_idx < 0 {
                    self.emit(Opcode::Rowid, self.table_cursor, dest_reg, 0, P4::Unused);
                } else if let Some(data_base) = self.column_data_base {
                    // We already have column values loaded in registers - copy from there
                    let src_reg = data_base + col_idx;
                    self.emit(Opcode::SCopy, src_reg, dest_reg, 0, P4::Unused);
                } else {
                    // Read column from cursor
                    self.emit(
                        Opcode::Column,
                        self.table_cursor,
                        col_idx,
                        dest_reg,
                        P4::Unused,
                    );
                }
            }
            Expr::Binary { op, left, right } => {
                let left_reg = self.alloc_reg();
                let right_reg = self.alloc_reg();
                self.compile_expr(left, left_reg)?;
                self.compile_expr(right, right_reg)?;

                // Check if this is a comparison operator (which are jump instructions)
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
                    // Comparison operators are jump instructions in VDBE
                    // We need to produce a boolean value using a jump pattern:
                    // 1. Default to 0 (false)
                    // 2. If comparison matches, jump to set_true
                    // 3. Jump past set_true
                    // 4. set_true: set 1
                    // 5. end:
                    let set_true_label = self.alloc_label();
                    let end_label = self.alloc_label();

                    // Default to false
                    self.emit(Opcode::Integer, 0, dest_reg, 0, P4::Unused);

                    // Comparison opcodes: P1=right operand, P2=jump target, P3=left operand
                    // Lt P1 P2 P3 means "jump to P2 if r[P3] < r[P1]"
                    let cmp_opcode = match op {
                        crate::parser::ast::BinaryOp::Eq => Opcode::Eq,
                        crate::parser::ast::BinaryOp::Ne => Opcode::Ne,
                        crate::parser::ast::BinaryOp::Lt => Opcode::Lt,
                        crate::parser::ast::BinaryOp::Le => Opcode::Le,
                        crate::parser::ast::BinaryOp::Gt => Opcode::Gt,
                        crate::parser::ast::BinaryOp::Ge => Opcode::Ge,
                        _ => unreachable!(),
                    };
                    self.emit(cmp_opcode, right_reg, set_true_label, left_reg, P4::Unused);

                    // Jump past set_true
                    self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);

                    // set_true: dest_reg = 1
                    self.resolve_label(set_true_label, self.current_addr() as i32);
                    self.emit(Opcode::Integer, 1, dest_reg, 0, P4::Unused);

                    // end:
                    self.resolve_label(end_label, self.current_addr() as i32);
                } else {
                    // Arithmetic and logical operators produce values directly
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
                    crate::parser::ast::UnaryOp::Not => {
                        self.emit(Opcode::Not, dest_reg, dest_reg, 0, P4::Unused);
                    }
                    crate::parser::ast::UnaryOp::BitNot => {
                        self.emit(Opcode::BitNot, dest_reg, dest_reg, 0, P4::Unused);
                    }
                    _ => {}
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
                    // IS NOT NULL
                    let is_null_reg = self.alloc_reg();
                    self.emit(Opcode::IsNull, dest_reg, 0, is_null_reg, P4::Unused);
                    self.emit(Opcode::Not, is_null_reg, dest_reg, 0, P4::Unused);
                } else {
                    // IS NULL
                    self.emit(Opcode::IsNull, dest_reg, 0, dest_reg, P4::Unused);
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                self.compile_case(operand, when_clauses, else_clause, dest_reg)?;
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
                            // Empty list - always false (or true if negated)
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
                        // For IN subquery in UPDATE, we need to compile the subquery
                        // and check if the value exists in the result set.
                        // This is complex - for now use a simplified approach:
                        // compile subquery into ephemeral table and use Found opcode
                        let subq_cursor = self.alloc_cursor();
                        self.emit(Opcode::OpenEphemeral, subq_cursor, 1, 0, P4::Unused);

                        // For subquery compilation, we need to use the select compiler
                        // Save and restore table cursor since subquery might use cursor 0
                        let saved_cursor = self.table_cursor;

                        // Compile subquery using the select compiler
                        // For now, emit a simple check - the subquery needs full compilation
                        // which requires integrating with select.rs
                        // As a workaround, we can iterate the subquery results manually
                        // TODO: proper subquery compilation integration

                        // Simplified: check if we can get the subquery to evaluate
                        // For the specific case of IN (SELECT col FROM table), we can
                        // open the table and scan it

                        if let crate::parser::ast::SelectBody::Select(sel) = &subquery.body {
                            if let Some(from) = &sel.from {
                                if from.tables.len() == 1 {
                                    if let crate::parser::ast::TableRef::Table { name, .. } =
                                        &from.tables[0]
                                    {
                                        // Get root page for the table
                                        if let Some(schema) = self.schema {
                                            if let Some(table_def) = schema.table(&name.name) {
                                                let root_page = table_def.root_page;
                                                let scan_cursor = self.alloc_cursor();

                                                // Open table for reading
                                                self.emit(
                                                    Opcode::OpenRead,
                                                    scan_cursor,
                                                    root_page as i32,
                                                    0,
                                                    P4::Int64(table_def.columns.len() as i64),
                                                );

                                                // Populate ephemeral table with subquery results
                                                let loop_start = self.alloc_label();
                                                let loop_end = self.alloc_label();

                                                self.emit(
                                                    Opcode::Rewind,
                                                    scan_cursor,
                                                    loop_end,
                                                    0,
                                                    P4::Unused,
                                                );
                                                self.resolve_label(
                                                    loop_start,
                                                    self.current_addr() as i32,
                                                );

                                                // Get the column from SELECT
                                                let col_reg = self.alloc_reg();
                                                let col_idx = if let Some(ResultColumn::Expr {
                                                    expr,
                                                    ..
                                                }) = sel.columns.first()
                                                {
                                                    if let Expr::Column(col_ref) = expr {
                                                        // Find column index
                                                        table_def
                                                            .columns
                                                            .iter()
                                                            .position(|c| c.name == col_ref.column)
                                                            .unwrap_or(0)
                                                    } else {
                                                        0
                                                    }
                                                } else {
                                                    0
                                                };

                                                self.emit(
                                                    Opcode::Column,
                                                    scan_cursor,
                                                    col_idx as i32,
                                                    col_reg,
                                                    P4::Unused,
                                                );

                                                // Insert into ephemeral table
                                                let record_reg = self.alloc_reg();
                                                self.emit(
                                                    Opcode::MakeRecord,
                                                    col_reg,
                                                    1,
                                                    record_reg,
                                                    P4::Unused,
                                                );
                                                let key_reg = self.alloc_reg();
                                                self.emit(
                                                    Opcode::NewRowid,
                                                    subq_cursor,
                                                    key_reg,
                                                    0,
                                                    P4::Unused,
                                                );
                                                self.emit(
                                                    Opcode::Insert,
                                                    subq_cursor,
                                                    record_reg,
                                                    key_reg,
                                                    P4::Unused,
                                                );

                                                // Next row
                                                self.emit(
                                                    Opcode::Next,
                                                    scan_cursor,
                                                    loop_start,
                                                    0,
                                                    P4::Unused,
                                                );
                                                self.resolve_label(
                                                    loop_end,
                                                    self.current_addr() as i32,
                                                );

                                                self.emit(
                                                    Opcode::Close,
                                                    scan_cursor,
                                                    0,
                                                    0,
                                                    P4::Unused,
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        self.table_cursor = saved_cursor;

                        // Check if value exists in ephemeral table
                        let record_reg = self.alloc_reg();
                        self.emit(Opcode::MakeRecord, val_reg, 1, record_reg, P4::Unused);

                        let found_label = self.alloc_label();
                        let end_label = self.alloc_label();

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
                    crate::parser::ast::InList::Table(_) => {
                        // Table IN - treat as false for now
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
            Expr::Exists { subquery, negated } => {
                // Compile EXISTS subquery for UPDATE
                // For a simple implementation, we scan the subquery table and check if any rows match

                // Initialize result based on negation
                // EXISTS: 0 (no rows found yet), NOT EXISTS: 1 (assumed true until row found)
                self.emit(
                    Opcode::Integer,
                    if *negated { 1 } else { 0 },
                    dest_reg,
                    0,
                    P4::Unused,
                );

                // Try to compile a simple EXISTS (SELECT ... FROM table WHERE condition)
                if let crate::parser::ast::SelectBody::Select(sel) = &subquery.body {
                    if let Some(from) = &sel.from {
                        if from.tables.len() == 1 {
                            if let crate::parser::ast::TableRef::Table { name, alias, .. } =
                                &from.tables[0]
                            {
                                if let Some(schema) = self.schema {
                                    if let Some(table_def) = schema.table(&name.name) {
                                        let root_page = table_def.root_page;
                                        let scan_cursor = self.alloc_cursor();

                                        // Open table for reading
                                        self.emit(
                                            Opcode::OpenRead,
                                            scan_cursor,
                                            root_page as i32,
                                            0,
                                            P4::Int64(table_def.columns.len() as i64),
                                        );

                                        let loop_start = self.alloc_label();
                                        let loop_end = self.alloc_label();
                                        let found_label = self.alloc_label();

                                        self.emit(
                                            Opcode::Rewind,
                                            scan_cursor,
                                            loop_end,
                                            0,
                                            P4::Unused,
                                        );
                                        self.resolve_label(loop_start, self.current_addr() as i32);

                                        // Evaluate WHERE clause if present
                                        if let Some(where_expr) = &sel.where_clause {
                                            // We need to evaluate the WHERE expression with the subquery table
                                            // For correlated subqueries, we need access to outer table columns
                                            // For now, compile simple conditions

                                            let cond_reg = self.alloc_reg();

                                            // Save current cursor and compile with subquery's cursor
                                            let saved_cursor = self.table_cursor;

                                            // Compile WHERE condition with subquery cursor
                                            // This is tricky for correlated subqueries
                                            // For correlated references, we need the outer table's cursor
                                            self.compile_exists_where(
                                                where_expr,
                                                cond_reg,
                                                scan_cursor,
                                                &table_def.columns,
                                                alias.as_deref().unwrap_or(&name.name),
                                            )?;

                                            self.table_cursor = saved_cursor;

                                            // Skip to next if condition is false
                                            self.emit(
                                                Opcode::IfNot,
                                                cond_reg,
                                                loop_end,
                                                1,
                                                P4::Unused,
                                            );
                                        }

                                        // Found a matching row
                                        self.emit(
                                            Opcode::Integer,
                                            if *negated { 0 } else { 1 },
                                            dest_reg,
                                            0,
                                            P4::Unused,
                                        );
                                        self.emit(Opcode::Goto, 0, found_label, 0, P4::Unused);

                                        // Loop to next row (only reached if no early exit)
                                        self.resolve_label(loop_end, self.current_addr() as i32);

                                        // No matching rows - keep initial value
                                        self.resolve_label(found_label, self.current_addr() as i32);

                                        self.emit(Opcode::Close, scan_cursor, 0, 0, P4::Unused);
                                    }
                                }
                            }
                        }
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

    /// Compile WHERE clause for EXISTS subquery with correlated reference support
    fn compile_exists_where(
        &mut self,
        expr: &Expr,
        dest_reg: i32,
        subq_cursor: i32,
        subq_columns: &[crate::schema::Column],
        subq_table_name: &str,
    ) -> Result<()> {
        match expr {
            Expr::Binary { op, left, right } => {
                let left_reg = self.alloc_reg();
                let right_reg = self.alloc_reg();

                self.compile_exists_operand(
                    left,
                    left_reg,
                    subq_cursor,
                    subq_columns,
                    subq_table_name,
                )?;
                self.compile_exists_operand(
                    right,
                    right_reg,
                    subq_cursor,
                    subq_columns,
                    subq_table_name,
                )?;

                // Emit comparison
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
                    let set_true_label = self.alloc_label();
                    let end_label = self.alloc_label();

                    // Default to false
                    self.emit(Opcode::Integer, 0, dest_reg, 0, P4::Unused);

                    let cmp_opcode = match op {
                        crate::parser::ast::BinaryOp::Eq => Opcode::Eq,
                        crate::parser::ast::BinaryOp::Ne => Opcode::Ne,
                        crate::parser::ast::BinaryOp::Lt => Opcode::Lt,
                        crate::parser::ast::BinaryOp::Le => Opcode::Le,
                        crate::parser::ast::BinaryOp::Gt => Opcode::Gt,
                        crate::parser::ast::BinaryOp::Ge => Opcode::Ge,
                        _ => Opcode::Eq,
                    };
                    self.emit(cmp_opcode, right_reg, set_true_label, left_reg, P4::Unused);

                    self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);

                    self.resolve_label(set_true_label, self.current_addr() as i32);
                    self.emit(Opcode::Integer, 1, dest_reg, 0, P4::Unused);

                    self.resolve_label(end_label, self.current_addr() as i32);
                } else {
                    // Non-comparison binary ops
                    self.emit(Opcode::Integer, 1, dest_reg, 0, P4::Unused);
                }
            }
            _ => {
                // Default to true (no condition)
                self.emit(Opcode::Integer, 1, dest_reg, 0, P4::Unused);
            }
        }
        Ok(())
    }

    /// Compile an operand in EXISTS WHERE clause, handling table references
    fn compile_exists_operand(
        &mut self,
        expr: &Expr,
        dest_reg: i32,
        subq_cursor: i32,
        subq_columns: &[crate::schema::Column],
        subq_table_name: &str,
    ) -> Result<()> {
        match expr {
            Expr::Column(col_ref) => {
                // Determine if this is a subquery table reference or outer table reference
                let is_subq_ref = col_ref
                    .table
                    .as_ref()
                    .map(|t| t.eq_ignore_ascii_case(subq_table_name))
                    .unwrap_or(false);

                if is_subq_ref {
                    // Reference to subquery table - use subq_cursor
                    let col_idx = subq_columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&col_ref.column))
                        .unwrap_or(0);
                    self.emit(
                        Opcode::Column,
                        subq_cursor,
                        col_idx as i32,
                        dest_reg,
                        P4::Unused,
                    );
                } else {
                    // Reference to outer table (possibly correlated) - use main table_cursor
                    // Find column in the main table being updated
                    if let Some(schema) = self.schema {
                        if let Some(table_def) = schema.table(&self.table_name) {
                            let col_idx = table_def
                                .columns
                                .iter()
                                .position(|c| c.name.eq_ignore_ascii_case(&col_ref.column))
                                .unwrap_or(0);
                            self.emit(
                                Opcode::Column,
                                self.table_cursor,
                                col_idx as i32,
                                dest_reg,
                                P4::Unused,
                            );
                        } else {
                            self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                        }
                    } else {
                        self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                    }
                }
            }
            Expr::Literal(lit) => match lit {
                crate::parser::ast::Literal::Integer(n) => {
                    self.emit(Opcode::Integer, *n as i32, dest_reg, 0, P4::Unused);
                }
                crate::parser::ast::Literal::Float(f) => {
                    self.emit(Opcode::Real, 0, dest_reg, 0, P4::Real(*f));
                }
                crate::parser::ast::Literal::String(s) => {
                    self.emit(Opcode::String8, 0, dest_reg, 0, P4::Text(s.clone()));
                }
                crate::parser::ast::Literal::Null => {
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                }
                _ => {
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                }
            },
            _ => {
                // For other expressions, use the default compile_expr
                self.compile_expr(expr, dest_reg)?;
            }
        }
        Ok(())
    }

    /// Compile CASE expression
    fn compile_case(
        &mut self,
        operand: &Option<Box<Expr>>,
        when_clauses: &[crate::parser::ast::WhenClause],
        else_clause: &Option<Box<Expr>>,
        dest_reg: i32,
    ) -> Result<()> {
        let end_label = self.alloc_label();

        // If there's an operand, evaluate it once
        let operand_reg = if let Some(op) = operand {
            let reg = self.alloc_reg();
            self.compile_expr(op, reg)?;
            Some(reg)
        } else {
            None
        };

        for when_clause in when_clauses {
            let next_when_label = self.alloc_label();

            // Evaluate WHEN condition
            let cond_reg = self.alloc_reg();
            self.compile_expr(&when_clause.when, cond_reg)?;

            // If we have an operand, compare with it
            if let Some(op_reg) = operand_reg {
                self.emit(Opcode::Ne, op_reg, next_when_label, cond_reg, P4::Unused);
            } else {
                // Direct boolean check
                self.emit(Opcode::IfNot, cond_reg, next_when_label, 1, P4::Unused);
            }

            // Evaluate THEN expression
            self.compile_expr(&when_clause.then, dest_reg)?;
            self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);

            self.resolve_label(next_when_label, self.current_addr() as i32);
        }

        // ELSE clause or NULL
        if let Some(else_expr) = else_clause {
            self.compile_expr(else_expr, dest_reg)?;
        } else {
            self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
        }

        self.resolve_label(end_label, self.current_addr() as i32);

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

    fn alloc_regs(&mut self, n: usize) -> i32 {
        let base = self.next_reg;
        self.next_reg += n as i32;
        base
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
}

impl Default for UpdateCompiler<'_> {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Compile an UPDATE statement to VDBE opcodes (without schema)
pub fn compile_update(update: &UpdateStmt) -> Result<Vec<VdbeOp>> {
    let mut compiler = UpdateCompiler::new();
    compiler.compile(update)
}

/// Compile an UPDATE statement to VDBE opcodes with schema access
pub fn compile_update_with_schema(
    update: &UpdateStmt,
    schema: &crate::schema::Schema,
) -> Result<Vec<VdbeOp>> {
    let mut compiler = UpdateCompiler::with_schema(schema);
    compiler.compile(update)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{Assignment, BinaryOp, ColumnRef, Literal, QualifiedName};

    #[test]
    fn test_update_compiler_new() {
        let compiler = UpdateCompiler::new();
        assert!(compiler.ops.is_empty());
        assert_eq!(compiler.next_reg, 1);
    }

    #[test]
    fn test_compile_simple_update() {
        let update = UpdateStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("users"),
            alias: None,
            indexed_by: None,
            assignments: vec![Assignment {
                columns: vec!["name".to_string()],
                expr: Expr::Literal(Literal::String("Alice".to_string())),
            }],
            from: None,
            where_clause: None,
            returning: None,
            order_by: None,
            limit: None,
        };

        let ops = compile_update(&update).unwrap();
        assert!(!ops.is_empty());

        // Should have Init, OpenRead, OpenEphemeral, Rewind, loop structure, Close, OpenWrite, etc.
        assert!(ops.iter().any(|op| op.opcode == Opcode::Init));
        assert!(ops.iter().any(|op| op.opcode == Opcode::OpenRead));
        assert!(ops.iter().any(|op| op.opcode == Opcode::OpenEphemeral));
        assert!(ops.iter().any(|op| op.opcode == Opcode::OpenWrite));
        assert!(ops.iter().any(|op| op.opcode == Opcode::NotExists));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Delete));
        assert!(ops.iter().any(|op| op.opcode == Opcode::MakeRecord));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Insert));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Next));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Close));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Halt));
    }

    #[test]
    fn test_compile_update_with_where() {
        let update = UpdateStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("users"),
            alias: None,
            indexed_by: None,
            assignments: vec![Assignment {
                columns: vec!["age".to_string()],
                expr: Expr::Literal(Literal::Integer(30)),
            }],
            from: None,
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

        let ops = compile_update(&update).unwrap();
        assert!(!ops.is_empty());

        // Should have IfNot for WHERE check
        assert!(ops.iter().any(|op| op.opcode == Opcode::IfNot));
        // Should have Eq for the WHERE comparison
        assert!(ops.iter().any(|op| op.opcode == Opcode::Eq));
    }

    #[test]
    fn test_conflict_flags() {
        let compiler = UpdateCompiler::new();
        assert_eq!(compiler.conflict_flags(ConflictAction::Abort), 0);
        assert_eq!(compiler.conflict_flags(ConflictAction::Rollback), 1);
        assert_eq!(compiler.conflict_flags(ConflictAction::Fail), 2);
        assert_eq!(compiler.conflict_flags(ConflictAction::Ignore), 3);
        assert_eq!(compiler.conflict_flags(ConflictAction::Replace), 4);
    }

    #[test]
    fn test_sqlite_master_modification_error() {
        use crate::schema::{Column, Schema, Table};
        use std::sync::Arc;

        let mut schema = Schema::new();
        // Add sqlite_master to schema (though it's a system table)
        let mut table = Table::new("sqlite_master");
        table.root_page = 1;
        table.columns = vec![
            Column::new("type"),
            Column::new("name"),
            Column::new("tbl_name"),
            Column::new("rootpage"),
            Column::new("sql"),
        ];
        schema
            .tables
            .insert("sqlite_master".to_string(), Arc::new(table));

        let update = UpdateStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("sqlite_master"),
            alias: None,
            indexed_by: None,
            assignments: vec![Assignment {
                columns: vec!["name".to_string()],
                expr: Expr::Literal(Literal::String("test".to_string())),
            }],
            from: None,
            where_clause: None,
            returning: None,
            order_by: None,
            limit: None,
        };

        let result = compile_update_with_schema(&update, &schema);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err
            .message
            .as_ref()
            .unwrap()
            .contains("may not be modified"));
    }

    #[test]
    fn test_no_such_column_error() {
        use crate::schema::{Column, Schema, Table};
        use std::sync::Arc;

        let mut schema = Schema::new();
        let mut table = Table::new("test1");
        table.root_page = 2;
        table.columns = vec![Column::new("f1"), Column::new("f2")];
        schema.tables.insert("test1".to_string(), Arc::new(table));

        // Try to update a column that doesn't exist
        let update = UpdateStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("test1"),
            alias: None,
            indexed_by: None,
            assignments: vec![Assignment {
                columns: vec!["f3".to_string()], // f3 doesn't exist
                expr: Expr::Literal(Literal::Integer(999)),
            }],
            from: None,
            where_clause: None,
            returning: None,
            order_by: None,
            limit: None,
        };

        let result = compile_update_with_schema(&update, &schema);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.as_ref().unwrap().contains("no such column: f3"));
    }

    #[test]
    fn test_no_such_column_in_expression() {
        use crate::schema::{Column, Schema, Table};
        use std::sync::Arc;

        let mut schema = Schema::new();
        let mut table = Table::new("test1");
        table.root_page = 2;
        table.columns = vec![Column::new("f1"), Column::new("f2")];
        schema.tables.insert("test1".to_string(), Arc::new(table));

        // SET f1=f3*2 where f3 doesn't exist in the expression
        let update = UpdateStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("test1"),
            alias: None,
            indexed_by: None,
            assignments: vec![Assignment {
                columns: vec!["f1".to_string()],
                expr: Expr::Binary {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::Column(ColumnRef {
                        database: None,
                        table: None,
                        column: "f3".to_string(), // f3 doesn't exist
                        column_index: None,
                    })),
                    right: Box::new(Expr::Literal(Literal::Integer(2))),
                },
            }],
            from: None,
            where_clause: None,
            returning: None,
            order_by: None,
            limit: None,
        };

        let result = compile_update_with_schema(&update, &schema);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.as_ref().unwrap().contains("no such column: f3"));
    }

    #[test]
    fn test_no_such_column_table_qualified() {
        use crate::schema::{Column, Schema, Table};
        use std::sync::Arc;

        let mut schema = Schema::new();
        let mut table = Table::new("test1");
        table.root_page = 2;
        table.columns = vec![Column::new("f1"), Column::new("f2")];
        schema.tables.insert("test1".to_string(), Arc::new(table));

        // SET f1=test2.f1*2 where test2 is a different table
        let update = UpdateStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("test1"),
            alias: None,
            indexed_by: None,
            assignments: vec![Assignment {
                columns: vec!["f1".to_string()],
                expr: Expr::Binary {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::Column(ColumnRef {
                        database: None,
                        table: Some("test2".to_string()), // test2 is not our table
                        column: "f1".to_string(),
                        column_index: None,
                    })),
                    right: Box::new(Expr::Literal(Literal::Integer(2))),
                },
            }],
            from: None,
            where_clause: None,
            returning: None,
            order_by: None,
            limit: None,
        };

        let result = compile_update_with_schema(&update, &schema);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err
            .message
            .as_ref()
            .unwrap()
            .contains("no such column: test2.f1"));
    }
}
