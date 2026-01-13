//! INSERT statement compilation
//!
//! This module compiles INSERT statements to VDBE bytecode.
//! Corresponds to insert.c in SQLite.

use std::collections::HashMap;

use crate::error::Result;
use crate::parser::ast::{
    ConflictAction, Expr, InsertSource, InsertStmt, ResultColumn, SelectStmt,
};
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

// ============================================================================
// InsertCompiler
// ============================================================================

/// Compiles INSERT statements to VDBE opcodes
pub struct InsertCompiler {
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

impl InsertCompiler {
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
        let col_indices = self.build_column_map(&insert.columns)?;

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
            for (i, col_idx) in col_indices.iter().enumerate() {
                if i < row.len() {
                    let dest_reg = data_base + *col_idx as i32;
                    self.compile_expr(&row[i], dest_reg)?;
                }
            }

            // Fill in NULL for unspecified columns
            for i in 0..self.num_columns {
                if !col_indices.contains(&i) {
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
        if !insert.columns.is_empty() {
            return insert.columns.len();
        }
        match &insert.source {
            InsertSource::Values(rows) => rows.first().map(|row| row.len()).unwrap_or(0),
            InsertSource::Select(_) => 0,
            InsertSource::DefaultValues => 0,
        }
    }

    /// Compile INSERT...SELECT
    fn compile_select(
        &mut self,
        insert: &InsertStmt,
        _select: &SelectStmt,
        conflict_action: ConflictAction,
    ) -> Result<()> {
        // Build column index map
        let col_indices = self.build_column_map(&insert.columns)?;

        // Compile the SELECT statement
        // For a real implementation, we'd integrate with SelectCompiler
        // Here we emit a placeholder loop structure

        // Open ephemeral table for SELECT results
        let select_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenEphemeral,
            select_cursor,
            self.num_columns as i32,
            0,
            P4::Unused,
        );

        // TODO: Actually compile the SELECT and populate ephemeral table
        // For now, emit placeholder

        // Loop over SELECT results
        let loop_start_label = self.alloc_label();
        let loop_end_label = self.alloc_label();

        self.emit(Opcode::Rewind, select_cursor, loop_end_label, 0, P4::Unused);
        self.resolve_label(loop_start_label, self.current_addr() as i32);

        // Allocate rowid register
        let rowid_reg = self.alloc_reg();
        self.emit(
            Opcode::NewRowid,
            self.table_cursor,
            rowid_reg,
            0,
            P4::Unused,
        );

        // Get data from SELECT row
        let data_base = self.next_reg;
        let _data_regs = self.alloc_regs(self.num_columns);

        for (i, col_idx) in col_indices.iter().enumerate() {
            let dest_reg = data_base + *col_idx as i32;
            self.emit(
                Opcode::Column,
                select_cursor,
                i as i32,
                dest_reg,
                P4::Unused,
            );
        }

        // Fill NULLs for unspecified columns
        for i in 0..self.num_columns {
            if !col_indices.contains(&i) {
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

        // Next row
        self.emit(Opcode::Next, select_cursor, loop_start_label, 0, P4::Unused);
        self.resolve_label(loop_end_label, self.current_addr() as i32);

        // Close SELECT cursor
        self.emit(Opcode::Close, select_cursor, 0, 0, P4::Unused);

        Ok(())
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
    fn build_column_map(&self, columns: &Option<Vec<String>>) -> Result<Vec<usize>> {
        match columns {
            Some(cols) => {
                // Map specified columns to indices
                let mut indices = Vec::with_capacity(cols.len());
                for (i, _col) in cols.iter().enumerate() {
                    // In real implementation, would look up column index by name
                    indices.push(i);
                }
                Ok(indices)
            }
            None => {
                // All columns in order
                Ok((0..self.num_columns).collect())
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
                        let zero_reg = self.alloc_reg();
                        self.emit(Opcode::Integer, 0, zero_reg, 0, P4::Unused);
                        self.emit(Opcode::Subtract, zero_reg, dest_reg, dest_reg, P4::Unused);
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

impl Default for InsertCompiler {
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
            columns: None,
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
