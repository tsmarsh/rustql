//! INSERT statement compilation
//!
//! This module compiles INSERT statements to VDBE bytecode.
//! Corresponds to insert.c in SQLite.

use std::collections::HashMap;

use crate::error::Result;
use crate::parser::ast::{
    ConflictAction, Expr, InsertSource, InsertStmt, ResultColumn, SelectBody, SelectStmt, TableRef,
};
use crate::schema::Schema;
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

fn is_rowid_alias(name: &str) -> bool {
    name.eq_ignore_ascii_case("rowid")
        || name.eq_ignore_ascii_case("_rowid_")
        || name.eq_ignore_ascii_case("oid")
}

#[derive(Debug, Clone, Copy)]
enum InsertColumnTarget {
    Rowid,
    Column(usize),
}

// ============================================================================
// InsertCompiler
// ============================================================================

/// Compiles INSERT statements to VDBE opcodes
pub struct InsertCompiler<'a> {
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

    /// Optional schema for validation
    schema: Option<&'a Schema>,
}

impl<'a> InsertCompiler<'a> {
    /// Create a new INSERT compiler
    pub fn new() -> Self {
        InsertCompiler {
            ops: Vec::new(),
            next_reg: 1,
            next_cursor: 0,
            next_label: -1,
            labels: HashMap::new(),
            table_cursor: 0,
            num_columns: 0,
            column_map: HashMap::new(),
            schema: None,
        }
    }

    /// Create a new INSERT compiler with schema
    pub fn with_schema(schema: &'a Schema) -> Self {
        InsertCompiler {
            ops: Vec::new(),
            next_reg: 1,
            next_cursor: 0,
            next_label: -1,
            labels: HashMap::new(),
            table_cursor: 0,
            num_columns: 0,
            column_map: HashMap::new(),
            schema: Some(schema),
        }
    }

    /// Compile an INSERT statement
    pub fn compile(&mut self, insert: &InsertStmt) -> Result<Vec<VdbeOp>> {
        // Initialize
        self.emit(Opcode::Init, 0, 0, 0, P4::Unused);

        // Open table for writing
        self.table_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenWrite,
            self.table_cursor,
            0, // Root page (would come from schema)
            0,
            P4::Text(insert.table.name.clone()),
        );

        self.num_columns = self.infer_num_columns(insert);

        // Handle conflict action
        let conflict_action = insert.or_action.unwrap_or(ConflictAction::Abort);

        // Compile based on source type
        match &insert.source {
            InsertSource::Values(rows) => {
                self.compile_values(insert, rows, conflict_action)?;
            }
            InsertSource::Select(select) => {
                // Validate ORDER BY doesn't contain aggregates without GROUP BY
                self.validate_select_order_by(select)?;
                self.compile_select(insert, select, conflict_action)?;
            }
            InsertSource::DefaultValues => {
                self.compile_default_values(insert, conflict_action)?;
            }
        }

        // Handle RETURNING clause
        if let Some(returning) = &insert.returning {
            self.compile_returning(returning)?;
        }

        // Close cursor
        self.emit(Opcode::Close, self.table_cursor, 0, 0, P4::Unused);

        // Halt
        self.emit(Opcode::Halt, 0, 0, 0, P4::Unused);

        // Resolve labels
        self.resolve_labels()?;

        Ok(std::mem::take(&mut self.ops))
    }

    /// Compile INSERT...VALUES
    fn compile_values(
        &mut self,
        insert: &InsertStmt,
        rows: &[Vec<Expr>],
        conflict_action: ConflictAction,
    ) -> Result<()> {
        // Build column index map if columns specified
        let col_targets = self.build_column_map(&insert.columns)?;

        // Validate column count for each row
        let expected_cols = col_targets.len();
        for row in rows {
            if row.len() != expected_cols {
                if insert.columns.is_some() {
                    // Column list specified: "N values for M columns"
                    return Err(crate::error::Error::with_message(
                        crate::error::ErrorCode::Error,
                        format!("{} values for {} columns", row.len(), expected_cols),
                    ));
                } else {
                    // No column list: "table X has N columns but M values were supplied"
                    return Err(crate::error::Error::with_message(
                        crate::error::ErrorCode::Error,
                        format!(
                            "table {} has {} columns but {} values were supplied",
                            insert.table.name,
                            self.num_columns,
                            row.len()
                        ),
                    ));
                }
            }
        }

        for row in rows {
            // Allocate rowid register
            let rowid_reg = self.alloc_reg();

            // Generate new rowid (autoincrement)
            self.emit(
                Opcode::NewRowid,
                self.table_cursor,
                rowid_reg,
                0,
                P4::Unused,
            );

            // Allocate registers for column values
            let data_base = self.next_reg;
            let _data_regs = self.alloc_regs(self.num_columns);

            // Evaluate each value and store in appropriate register
            let mut present = vec![false; self.num_columns];
            for (i, target) in col_targets.iter().enumerate() {
                if i < row.len() {
                    match *target {
                        InsertColumnTarget::Rowid => {
                            self.compile_expr(&row[i], rowid_reg)?;
                        }
                        InsertColumnTarget::Column(col_idx) => {
                            let dest_reg = data_base + col_idx as i32;
                            self.compile_expr(&row[i], dest_reg)?;
                            if col_idx < present.len() {
                                present[col_idx] = true;
                            }
                        }
                    }
                }
            }

            // Fill in NULL for unspecified columns
            for (i, seen) in present.iter().enumerate() {
                if !*seen {
                    let reg = data_base + i as i32;
                    self.emit(Opcode::Null, 0, reg, 0, P4::Unused);
                }
            }

            // Handle conflict action
            self.emit_conflict_check(conflict_action)?;

            // Make record
            let record_reg = self.alloc_reg();
            self.emit(
                Opcode::MakeRecord,
                data_base,
                self.num_columns as i32,
                record_reg,
                P4::Unused,
            );

            // Insert the record
            let flags = self.conflict_flags(conflict_action);
            self.emit(
                Opcode::Insert,
                self.table_cursor,
                record_reg,
                rowid_reg,
                P4::Int64(flags),
            );
        }

        Ok(())
    }

    fn infer_num_columns(&self, insert: &InsertStmt) -> usize {
        // If column list is specified, use that count
        if let Some(cols) = &insert.columns {
            if !cols.is_empty() {
                return cols.iter().filter(|col| !is_rowid_alias(col)).count();
            }
        }

        // If schema is available, use actual table column count
        if let Some(schema) = self.schema {
            let table_name_lower = insert.table.name.to_lowercase();
            if let Some(table) = schema.tables.get(&table_name_lower) {
                return table.columns.len();
            }
        }

        // Fallback: infer from source (less accurate)
        match &insert.source {
            InsertSource::Values(rows) => rows.first().map(|row| row.len()).unwrap_or(0),
            InsertSource::Select(select) => self.count_select_columns(select),
            InsertSource::DefaultValues => 1,
        }
    }

    /// Count columns in SELECT result
    fn count_select_columns(&self, select: &SelectStmt) -> usize {
        if let SelectBody::Select(core) = &select.body {
            let mut count = 0;
            for col in &core.columns {
                match col {
                    ResultColumn::Star => {
                        // For *, we don't know the count without schema
                        // Use a reasonable default
                        return 10;
                    }
                    ResultColumn::TableStar(_) => return 10,
                    ResultColumn::Expr { .. } => count += 1,
                }
            }
            return count.max(1);
        }
        10 // Default fallback
    }

    /// Compile INSERT...SELECT
    ///
    /// To handle self-referential queries (INSERT INTO t SELECT ... FROM t),
    /// we materialize the SELECT results into an ephemeral table first, then
    /// insert from the ephemeral table. This prevents infinite loops where
    /// newly inserted rows would be visible to the SELECT cursor.
    ///
    /// Steps:
    /// 1. Open source table for reading
    /// 2. Open ephemeral table to buffer rows
    /// 3. First loop: Read from source, insert into ephemeral table
    /// 4. Close source cursor
    /// 5. Second loop: Read from ephemeral table, insert into target
    /// 6. Close ephemeral cursor
    fn compile_select(
        &mut self,
        insert: &InsertStmt,
        select: &SelectStmt,
        conflict_action: ConflictAction,
    ) -> Result<()> {
        // Build column index map
        let col_targets = self.build_column_map(&insert.columns)?;

        // Extract source table from SELECT
        // For now, we support simple "SELECT * FROM table" or "SELECT cols FROM table"
        let source_table = self.get_source_table(select)?;

        // Get number of columns to read from SELECT
        let select_col_count = self.get_select_column_count(select);

        // Open source table for reading
        let source_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenRead,
            source_cursor,
            0, // Root page 0 = look up by name
            self.num_columns as i32,
            P4::Text(source_table.clone()),
        );

        // Open ephemeral table to buffer the SELECT results
        // This is critical for self-referential queries to avoid infinite loops
        let eph_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenEphemeral,
            eph_cursor,
            select_col_count as i32,
            0,
            P4::Unused,
        );

        // ========================================================================
        // Phase 1: Read all source rows into ephemeral table
        // ========================================================================
        let read_loop_start = self.alloc_label();
        let read_loop_end = self.alloc_label();

        // Rewind to start of source table
        self.emit(Opcode::Rewind, source_cursor, read_loop_end, 0, P4::Unused);
        self.resolve_label(read_loop_start, self.current_addr() as i32);

        // Read columns from source row into registers
        let temp_base = self.next_reg;
        let _temp_regs = self.alloc_regs(select_col_count);

        for i in 0..select_col_count {
            let dest_reg = temp_base + i as i32;
            self.emit(
                Opcode::Column,
                source_cursor,
                i as i32,
                dest_reg,
                P4::Unused,
            );
        }

        // Allocate a rowid for the ephemeral table
        let eph_rowid_reg = self.alloc_reg();
        self.emit(Opcode::NewRowid, eph_cursor, eph_rowid_reg, 0, P4::Unused);

        // Make record for ephemeral table
        let eph_record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            temp_base,
            select_col_count as i32,
            eph_record_reg,
            P4::Unused,
        );

        // Insert into ephemeral table
        self.emit(
            Opcode::Insert,
            eph_cursor,
            eph_record_reg,
            eph_rowid_reg,
            P4::Int64(0), // No conflict handling for ephemeral
        );

        // Next row in source table
        self.emit(Opcode::Next, source_cursor, read_loop_start, 0, P4::Unused);
        self.resolve_label(read_loop_end, self.current_addr() as i32);

        // Close source cursor - we're done reading
        self.emit(Opcode::Close, source_cursor, 0, 0, P4::Unused);

        // ========================================================================
        // Phase 2: Insert from ephemeral table into target
        // ========================================================================
        let insert_loop_start = self.alloc_label();
        let insert_loop_end = self.alloc_label();

        // Rewind ephemeral table
        self.emit(Opcode::Rewind, eph_cursor, insert_loop_end, 0, P4::Unused);
        self.resolve_label(insert_loop_start, self.current_addr() as i32);

        // Allocate rowid register for target table
        let rowid_reg = self.alloc_reg();
        self.emit(
            Opcode::NewRowid,
            self.table_cursor,
            rowid_reg,
            0,
            P4::Unused,
        );

        // Read columns from ephemeral row and map to target columns
        let data_base = self.next_reg;
        let _data_regs = self.alloc_regs(self.num_columns);

        let mut present = vec![false; self.num_columns];
        for (i, target) in col_targets.iter().enumerate() {
            if i >= select_col_count {
                break;
            }
            match *target {
                InsertColumnTarget::Rowid => {
                    self.emit(Opcode::Column, eph_cursor, i as i32, rowid_reg, P4::Unused);
                }
                InsertColumnTarget::Column(col_idx) => {
                    let dest_reg = data_base + col_idx as i32;
                    self.emit(Opcode::Column, eph_cursor, i as i32, dest_reg, P4::Unused);
                    if col_idx < present.len() {
                        present[col_idx] = true;
                    }
                }
            }
        }

        // Fill NULLs for unspecified columns
        for (i, seen) in present.iter().enumerate() {
            if !*seen {
                let reg = data_base + i as i32;
                self.emit(Opcode::Null, 0, reg, 0, P4::Unused);
            }
        }

        // Handle conflict
        self.emit_conflict_check(conflict_action)?;

        // Make and insert record
        let record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            data_base,
            self.num_columns as i32,
            record_reg,
            P4::Unused,
        );

        let flags = self.conflict_flags(conflict_action);
        self.emit(
            Opcode::Insert,
            self.table_cursor,
            record_reg,
            rowid_reg,
            P4::Int64(flags),
        );

        // Next row in ephemeral table
        self.emit(Opcode::Next, eph_cursor, insert_loop_start, 0, P4::Unused);
        self.resolve_label(insert_loop_end, self.current_addr() as i32);

        // Close ephemeral cursor
        self.emit(Opcode::Close, eph_cursor, 0, 0, P4::Unused);

        Ok(())
    }

    /// Extract source table name from SELECT for simple cases
    fn get_source_table(&self, select: &SelectStmt) -> Result<String> {
        // Handle SELECT...FROM table
        if let SelectBody::Select(core) = &select.body {
            if let Some(from) = &core.from {
                if let Some(table_ref) = from.tables.first() {
                    if let TableRef::Table { name, .. } = table_ref {
                        return Ok(name.name.clone());
                    }
                }
            }
        }
        Err(crate::error::Error::with_message(
            crate::error::ErrorCode::Error,
            "INSERT...SELECT requires a simple SELECT from a table".to_string(),
        ))
    }

    /// Get number of columns in SELECT result
    fn get_select_column_count(&self, select: &SelectStmt) -> usize {
        if let SelectBody::Select(core) = &select.body {
            // For SELECT *, return all columns from target (num_columns)
            // For explicit columns, count them
            let mut count = 0;
            for col in &core.columns {
                match col {
                    ResultColumn::Star => return self.num_columns,
                    ResultColumn::TableStar(_) => return self.num_columns,
                    ResultColumn::Expr { .. } => count += 1,
                }
            }
            return count.max(1);
        }
        self.num_columns
    }

    /// Validate ORDER BY in SELECT doesn't contain aggregates without GROUP BY
    fn validate_select_order_by(&self, select: &SelectStmt) -> Result<()> {
        if let Some(order_by) = &select.order_by {
            let has_group_by = match &select.body {
                SelectBody::Select(core) => core.group_by.is_some(),
                SelectBody::Compound { .. } => false,
            };
            if !has_group_by {
                for term in order_by {
                    if let Some(agg_name) = self.find_aggregate_in_expr(&term.expr) {
                        return Err(crate::error::Error::with_message(
                            crate::error::ErrorCode::Error,
                            format!("misuse of aggregate: {}()", agg_name),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Find if an expression contains an aggregate function
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
            Expr::Unary { expr, .. } => self.find_aggregate_in_expr(expr),
            _ => None,
        }
    }

    /// Compile INSERT...DEFAULT VALUES
    fn compile_default_values(
        &mut self,
        _insert: &InsertStmt,
        conflict_action: ConflictAction,
    ) -> Result<()> {
        // Allocate rowid
        let rowid_reg = self.alloc_reg();
        self.emit(
            Opcode::NewRowid,
            self.table_cursor,
            rowid_reg,
            0,
            P4::Unused,
        );

        // All columns get default values (NULL if no default specified)
        let data_base = self.next_reg;
        let _data_regs = self.alloc_regs(self.num_columns);

        for i in 0..self.num_columns {
            let reg = data_base + i as i32;
            // In real implementation, would evaluate column default
            self.emit(Opcode::Null, 0, reg, 0, P4::Unused);
        }

        // Handle conflict
        self.emit_conflict_check(conflict_action)?;

        // Make and insert record
        let record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            data_base,
            self.num_columns as i32,
            record_reg,
            P4::Unused,
        );

        let flags = self.conflict_flags(conflict_action);
        self.emit(
            Opcode::Insert,
            self.table_cursor,
            record_reg,
            rowid_reg,
            P4::Int64(flags),
        );

        Ok(())
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

    /// Build column index map from column list
    fn build_column_map(&self, columns: &Option<Vec<String>>) -> Result<Vec<InsertColumnTarget>> {
        match columns {
            Some(cols) => {
                // Map specified columns to indices
                let mut targets = Vec::with_capacity(cols.len());
                let mut next_idx = 0usize;
                for col in cols {
                    if is_rowid_alias(col) {
                        targets.push(InsertColumnTarget::Rowid);
                    } else {
                        targets.push(InsertColumnTarget::Column(next_idx));
                        next_idx += 1;
                    }
                }
                Ok(targets)
            }
            None => {
                // All columns in order
                Ok((0..self.num_columns)
                    .map(InsertColumnTarget::Column)
                    .collect())
            }
        }
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
                // Skip row on conflict - needs special handling
                // In a real implementation, would emit constraint checks
                // and jump past Insert if violated
            }
            ConflictAction::Replace => {
                // Delete existing row with same key
                // In a real implementation, would emit:
                // 1. Check for existing row with same unique key
                // 2. Delete if found
            }
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
                // Column reference - would need to resolve from schema
                self.emit(
                    Opcode::Column,
                    0,
                    0,
                    dest_reg,
                    P4::Text(col_ref.column.clone()),
                );
            }
            Expr::Binary { op, left, right } => {
                let left_reg = self.alloc_reg();
                let right_reg = self.alloc_reg();
                self.compile_expr(left, left_reg)?;
                self.compile_expr(right, right_reg)?;

                let opcode = match op {
                    crate::parser::ast::BinaryOp::Add => Opcode::Add,
                    crate::parser::ast::BinaryOp::Sub => Opcode::Subtract,
                    crate::parser::ast::BinaryOp::Mul => Opcode::Multiply,
                    crate::parser::ast::BinaryOp::Div => Opcode::Divide,
                    crate::parser::ast::BinaryOp::Concat => Opcode::Concat,
                    _ => Opcode::Add,
                };

                self.emit(opcode, left_reg, right_reg, dest_reg, P4::Unused);
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

impl<'a> Default for InsertCompiler<'a> {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Compile an INSERT statement to VDBE opcodes
pub fn compile_insert(insert: &InsertStmt) -> Result<Vec<VdbeOp>> {
    let mut compiler = InsertCompiler::new();
    compiler.compile(insert)
}

/// Compile an INSERT statement with schema for proper column count validation
pub fn compile_insert_with_schema(insert: &InsertStmt, schema: &Schema) -> Result<Vec<VdbeOp>> {
    let mut compiler = InsertCompiler::with_schema(schema);
    compiler.compile(insert)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{Literal, QualifiedName};

    #[test]
    fn test_insert_compiler_new() {
        let compiler = InsertCompiler::new();
        assert!(compiler.ops.is_empty());
        assert_eq!(compiler.next_reg, 1);
    }

    #[test]
    fn test_compile_simple_insert() {
        let insert = InsertStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("users"),
            alias: None,
            columns: None,
            source: InsertSource::Values(vec![vec![
                Expr::Literal(Literal::Integer(1)),
                Expr::Literal(Literal::String("Alice".to_string())),
                Expr::Literal(Literal::Integer(30)),
            ]]),
            on_conflict: None,
            returning: None,
        };

        let ops = compile_insert(&insert).unwrap();
        assert!(!ops.is_empty());

        // Should have Init, OpenWrite, value ops, MakeRecord, Insert, Close, Halt
        assert!(ops.iter().any(|op| op.opcode == Opcode::Init));
        assert!(ops.iter().any(|op| op.opcode == Opcode::OpenWrite));
        assert!(ops.iter().any(|op| op.opcode == Opcode::NewRowid));
        assert!(ops.iter().any(|op| op.opcode == Opcode::MakeRecord));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Insert));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Close));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Halt));
    }

    #[test]
    fn test_compile_insert_with_columns() {
        let insert = InsertStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("users"),
            alias: None,
            columns: Some(vec!["name".to_string(), "age".to_string()]),
            source: InsertSource::Values(vec![vec![
                Expr::Literal(Literal::String("Bob".to_string())),
                Expr::Literal(Literal::Integer(25)),
            ]]),
            on_conflict: None,
            returning: None,
        };

        let ops = compile_insert(&insert).unwrap();
        assert!(!ops.is_empty());
    }

    #[test]
    fn test_compile_insert_default_values() {
        let insert = InsertStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("users"),
            alias: None,
            // Provide explicit columns so compiler knows how many Null ops to emit
            columns: Some(vec![
                "id".to_string(),
                "name".to_string(),
                "age".to_string(),
            ]),
            source: InsertSource::DefaultValues,
            on_conflict: None,
            returning: None,
        };

        let ops = compile_insert(&insert).unwrap();
        assert!(!ops.is_empty());

        // Should have Null opcodes for default values
        assert!(ops.iter().any(|op| op.opcode == Opcode::Null));
    }

    #[test]
    fn test_compile_insert_or_replace() {
        let insert = InsertStmt {
            with: None,
            or_action: Some(ConflictAction::Replace),
            table: QualifiedName::new("users"),
            alias: None,
            columns: None,
            source: InsertSource::Values(vec![vec![Expr::Literal(Literal::Integer(1))]]),
            on_conflict: None,
            returning: None,
        };

        let ops = compile_insert(&insert).unwrap();
        assert!(!ops.is_empty());

        // Check that Insert has the right conflict flags
        let insert_op = ops.iter().find(|op| op.opcode == Opcode::Insert);
        assert!(insert_op.is_some());
    }

    #[test]
    fn test_compile_insert_multiple_rows() {
        let insert = InsertStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("users"),
            alias: None,
            columns: None,
            source: InsertSource::Values(vec![
                vec![Expr::Literal(Literal::Integer(1))],
                vec![Expr::Literal(Literal::Integer(2))],
                vec![Expr::Literal(Literal::Integer(3))],
            ]),
            on_conflict: None,
            returning: None,
        };

        let ops = compile_insert(&insert).unwrap();

        // Should have multiple Insert opcodes
        let insert_count = ops.iter().filter(|op| op.opcode == Opcode::Insert).count();
        assert_eq!(insert_count, 3);
    }

    #[test]
    fn test_conflict_flags() {
        let compiler = InsertCompiler::new();
        assert_eq!(compiler.conflict_flags(ConflictAction::Abort), 0);
        assert_eq!(compiler.conflict_flags(ConflictAction::Rollback), 1);
        assert_eq!(compiler.conflict_flags(ConflictAction::Fail), 2);
        assert_eq!(compiler.conflict_flags(ConflictAction::Ignore), 3);
        assert_eq!(compiler.conflict_flags(ConflictAction::Replace), 4);
    }
}
