//! UPDATE statement compilation
//!
//! This module compiles UPDATE statements to VDBE bytecode.
//! Corresponds to update.c in SQLite.

use std::collections::HashMap;

use crate::error::Result;
use crate::parser::ast::{
    Assignment, ConflictAction, Expr, ResultColumn, UpdateStmt,
};
use crate::vdbe::ops::{Opcode, P4, VdbeOp};

// ============================================================================
// UpdateCompiler
// ============================================================================

/// Compiles UPDATE statements to VDBE opcodes
pub struct UpdateCompiler {
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
}

impl UpdateCompiler {
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
        }
    }

    /// Compile an UPDATE statement
    pub fn compile(&mut self, update: &UpdateStmt) -> Result<Vec<VdbeOp>> {
        // Initialize
        self.emit(Opcode::Init, 0, 0, 0, P4::Unused);

        // Open table for reading and writing
        self.table_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenWrite,
            self.table_cursor,
            0, // Root page (would come from schema)
            0,
            P4::Text(update.table.name.clone()),
        );

        // For now, assume a simple table structure
        // In a real implementation, we'd look up the schema
        self.num_columns = 5; // Placeholder - typical table size

        // Build column map for lookups
        self.build_column_map();

        // Handle conflict action
        let conflict_action = update.or_action.unwrap_or(ConflictAction::Abort);

        // Compile the UPDATE
        self.compile_update_body(update, conflict_action)?;

        // Handle RETURNING clause
        if let Some(returning) = &update.returning {
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

    /// Compile the UPDATE body (loop over rows, apply assignments)
    fn compile_update_body(
        &mut self,
        update: &UpdateStmt,
        conflict_action: ConflictAction,
    ) -> Result<()> {
        // Allocate labels for the main loop
        let loop_start_label = self.alloc_label();
        let loop_end_label = self.alloc_label();

        // If there's a WHERE clause, we might use an index
        // For now, we do a full table scan
        if update.where_clause.is_some() {
            // Rewind to start of table
            self.emit(Opcode::Rewind, self.table_cursor, loop_end_label, 0, P4::Unused);
        } else {
            // No WHERE - update all rows
            self.emit(Opcode::Rewind, self.table_cursor, loop_end_label, 0, P4::Unused);
        }

        // Loop start
        self.resolve_label(loop_start_label, self.current_addr() as i32);

        // If we have a WHERE clause, check the condition
        if let Some(where_expr) = &update.where_clause {
            let skip_label = self.alloc_label();
            self.compile_where_check(where_expr, skip_label)?;

            // Compile the row update
            self.compile_row_update(update, conflict_action)?;

            // Skip label (for rows that don't match WHERE)
            self.resolve_label(skip_label, self.current_addr() as i32);
        } else {
            // No WHERE - update every row
            self.compile_row_update(update, conflict_action)?;
        }

        // Move to next row
        self.emit(Opcode::Next, self.table_cursor, loop_start_label, 0, P4::Unused);

        // Loop end
        self.resolve_label(loop_end_label, self.current_addr() as i32);

        Ok(())
    }

    /// Compile code to check WHERE clause condition
    fn compile_where_check(&mut self, where_expr: &Expr, skip_label: i32) -> Result<()> {
        let cond_reg = self.alloc_reg();
        self.compile_expr(where_expr, cond_reg)?;

        // Jump to skip_label if condition is false (0) or NULL
        self.emit(Opcode::IfNot, cond_reg, skip_label, 1, P4::Unused);

        Ok(())
    }

    /// Compile code to update a single row
    fn compile_row_update(
        &mut self,
        update: &UpdateStmt,
        conflict_action: ConflictAction,
    ) -> Result<()> {
        // Get the current rowid
        let rowid_reg = self.alloc_reg();
        self.emit(Opcode::Rowid, self.table_cursor, rowid_reg, 0, P4::Unused);

        // Allocate registers for all column values
        let data_base = self.next_reg;
        let _data_regs = self.alloc_regs(self.num_columns);

        // First, read all current column values
        for i in 0..self.num_columns {
            let reg = data_base + i as i32;
            self.emit(Opcode::Column, self.table_cursor, i as i32, reg, P4::Unused);
        }

        // Apply assignments - overwrite columns being updated
        for assignment in &update.assignments {
            for col_name in &assignment.columns {
                if let Some(&col_idx) = self.column_map.get(col_name) {
                    let dest_reg = data_base + col_idx as i32;
                    self.compile_expr(&assignment.expr, dest_reg)?;
                } else {
                    // Column not found in map - use ordinal position
                    // This is a fallback for our simplified implementation
                    let col_idx = self.get_column_index(col_name);
                    let dest_reg = data_base + col_idx as i32;
                    self.compile_expr(&assignment.expr, dest_reg)?;
                }
            }
        }

        // Handle conflict action
        self.emit_conflict_check(conflict_action)?;

        // Delete the old row
        self.emit(Opcode::Delete, self.table_cursor, 0, 0, P4::Unused);

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
        let flags = self.conflict_flags(conflict_action);
        self.emit(Opcode::Insert, self.table_cursor, record_reg, rowid_reg, P4::Int64(flags));

        Ok(())
    }

    /// Get column index by name (simplified lookup)
    fn get_column_index(&self, name: &str) -> usize {
        // In a real implementation, this would look up in schema
        // For now, we hash the name to get a consistent but arbitrary index
        let hash: usize = name.bytes().fold(0, |acc, b| acc.wrapping_add(b as usize));
        hash % self.num_columns
    }

    /// Build column index map
    fn build_column_map(&mut self) {
        // In a real implementation, we'd populate this from schema
        // For now, create placeholder entries
        for i in 0..self.num_columns {
            self.column_map.insert(format!("col{}", i), i);
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
        self.emit(Opcode::ResultRow, base_reg, returning.len() as i32, 0, P4::Unused);

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
                        self.emit(Opcode::Blob, b.len() as i32, dest_reg, 0, P4::Blob(b.clone()));
                    }
                    crate::parser::ast::Literal::Bool(b) => {
                        self.emit(Opcode::Integer, if *b { 1 } else { 0 }, dest_reg, 0, P4::Unused);
                    }
                    _ => {
                        self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                    }
                }
            }
            Expr::Column(col_ref) => {
                // Column reference - look up in current row
                if let Some(&col_idx) = self.column_map.get(&col_ref.column) {
                    self.emit(Opcode::Column, self.table_cursor, col_idx as i32, dest_reg, P4::Unused);
                } else {
                    let col_idx = self.get_column_index(&col_ref.column);
                    self.emit(Opcode::Column, self.table_cursor, col_idx as i32, dest_reg, P4::Unused);
                }
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
                    crate::parser::ast::BinaryOp::Eq => Opcode::Eq,
                    crate::parser::ast::BinaryOp::Ne => Opcode::Ne,
                    crate::parser::ast::BinaryOp::Lt => Opcode::Lt,
                    crate::parser::ast::BinaryOp::Le => Opcode::Le,
                    crate::parser::ast::BinaryOp::Gt => Opcode::Gt,
                    crate::parser::ast::BinaryOp::Ge => Opcode::Ge,
                    crate::parser::ast::BinaryOp::And => Opcode::And,
                    crate::parser::ast::BinaryOp::Or => Opcode::Or,
                    crate::parser::ast::BinaryOp::BitAnd => Opcode::BitAnd,
                    crate::parser::ast::BinaryOp::BitOr => Opcode::BitOr,
                    crate::parser::ast::BinaryOp::Mod => Opcode::Remainder,
                    crate::parser::ast::BinaryOp::ShiftLeft => Opcode::ShiftLeft,
                    crate::parser::ast::BinaryOp::ShiftRight => Opcode::ShiftRight,
                    _ => Opcode::Add, // Default fallback
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
            Expr::IsNull { expr: inner, negated } => {
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
            Expr::Case { operand, when_clauses, else_clause } => {
                self.compile_case(operand, when_clauses, else_clause, dest_reg)?;
            }
            _ => {
                // Default to NULL for unsupported expressions
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
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

impl Default for UpdateCompiler {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Compile an UPDATE statement to VDBE opcodes
pub fn compile_update(update: &UpdateStmt) -> Result<Vec<VdbeOp>> {
    let mut compiler = UpdateCompiler::new();
    compiler.compile(update)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{BinaryOp, ColumnRef, Literal, QualifiedName};

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

        // Should have Init, OpenWrite, Rewind, loop structure, Close, Halt
        assert!(ops.iter().any(|op| op.opcode == Opcode::Init));
        assert!(ops.iter().any(|op| op.opcode == Opcode::OpenWrite));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Rewind));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Column));
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
    fn test_compile_update_multiple_assignments() {
        let update = UpdateStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("users"),
            alias: None,
            indexed_by: None,
            assignments: vec![
                Assignment {
                    columns: vec!["name".to_string()],
                    expr: Expr::Literal(Literal::String("Bob".to_string())),
                },
                Assignment {
                    columns: vec!["age".to_string()],
                    expr: Expr::Literal(Literal::Integer(25)),
                },
                Assignment {
                    columns: vec!["active".to_string()],
                    expr: Expr::Literal(Literal::Bool(true)),
                },
            ],
            from: None,
            where_clause: None,
            returning: None,
            order_by: None,
            limit: None,
        };

        let ops = compile_update(&update).unwrap();
        assert!(!ops.is_empty());

        // Should have String8 for name, Integer for age and active
        assert!(ops.iter().any(|op| op.opcode == Opcode::String8));
        let int_count = ops.iter().filter(|op| op.opcode == Opcode::Integer).count();
        assert!(int_count >= 2);
    }

    #[test]
    fn test_compile_update_or_replace() {
        let update = UpdateStmt {
            with: None,
            or_action: Some(ConflictAction::Replace),
            table: QualifiedName::new("users"),
            alias: None,
            indexed_by: None,
            assignments: vec![Assignment {
                columns: vec!["name".to_string()],
                expr: Expr::Literal(Literal::String("New Name".to_string())),
            }],
            from: None,
            where_clause: None,
            returning: None,
            order_by: None,
            limit: None,
        };

        let ops = compile_update(&update).unwrap();
        assert!(!ops.is_empty());

        // Check that Insert has the right conflict flags (4 = Replace)
        let insert_op = ops.iter().find(|op| op.opcode == Opcode::Insert);
        assert!(insert_op.is_some());
        if let Some(op) = insert_op {
            if let P4::Int64(flags) = op.p4 {
                assert_eq!(flags, 4);
            }
        }
    }

    #[test]
    fn test_compile_update_with_expression() {
        // UPDATE users SET age = age + 1
        let update = UpdateStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("users"),
            alias: None,
            indexed_by: None,
            assignments: vec![Assignment {
                columns: vec!["age".to_string()],
                expr: Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Column(ColumnRef {
                        database: None,
                        table: None,
                        column: "age".to_string(),
                    })),
                    right: Box::new(Expr::Literal(Literal::Integer(1))),
                },
            }],
            from: None,
            where_clause: None,
            returning: None,
            order_by: None,
            limit: None,
        };

        let ops = compile_update(&update).unwrap();
        assert!(!ops.is_empty());

        // Should have Add opcode for the age + 1 expression
        assert!(ops.iter().any(|op| op.opcode == Opcode::Add));
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
    fn test_compile_update_set_null() {
        let update = UpdateStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("users"),
            alias: None,
            indexed_by: None,
            assignments: vec![Assignment {
                columns: vec!["email".to_string()],
                expr: Expr::Literal(Literal::Null),
            }],
            from: None,
            where_clause: None,
            returning: None,
            order_by: None,
            limit: None,
        };

        let ops = compile_update(&update).unwrap();
        assert!(!ops.is_empty());

        // Should have Null opcode for the NULL literal
        let null_count = ops.iter().filter(|op| op.opcode == Opcode::Null).count();
        assert!(null_count >= 1);
    }
}
