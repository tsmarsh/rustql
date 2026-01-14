//! DELETE statement compilation
//!
//! This module compiles DELETE statements to VDBE bytecode.
//! Corresponds to delete.c in SQLite.

use std::collections::HashMap;

use crate::error::Result;
use crate::parser::ast::{DeleteStmt, Expr, ResultColumn};
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

// ============================================================================
// DeleteCompiler
// ============================================================================

/// Compiles DELETE statements to VDBE opcodes
pub struct DeleteCompiler {
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

impl DeleteCompiler {
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
        }
    }

    /// Compile a DELETE statement
    pub fn compile(&mut self, delete: &DeleteStmt) -> Result<Vec<VdbeOp>> {
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

        // For now, assume a simple table structure
        // In a real implementation, we'd look up the schema
        self.num_columns = 5; // Placeholder

        // Build column map for lookups
        self.build_column_map();

        // Compile the DELETE body
        self.compile_delete_body(delete)?;

        // Handle RETURNING clause
        if let Some(returning) = &delete.returning {
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

    /// Compile the DELETE body (loop over rows, delete matching ones)
    fn compile_delete_body(&mut self, delete: &DeleteStmt) -> Result<()> {
        // Check if we have ORDER BY and LIMIT
        // If so, we need a different approach: collect rowids first
        if delete.order_by.is_some() || delete.limit.is_some() {
            return self.compile_delete_with_limit(delete);
        }

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

        // If we have a WHERE clause, check the condition
        if let Some(where_expr) = &delete.where_clause {
            let skip_label = self.alloc_label();
            self.compile_where_check(where_expr, skip_label)?;

            // Delete the row
            self.emit(Opcode::Delete, self.table_cursor, 0, 0, P4::Unused);

            // Skip label (for rows that don't match WHERE)
            self.resolve_label(skip_label, self.current_addr() as i32);
        } else {
            // No WHERE - delete every row
            self.emit(Opcode::Delete, self.table_cursor, 0, 0, P4::Unused);
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
        self.emit(Opcode::Delete, self.table_cursor, 0, 0, P4::Unused);

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
        let cond_reg = self.alloc_reg();
        self.compile_expr(where_expr, cond_reg)?;

        // Jump to skip_label if condition is false (0) or NULL
        self.emit(Opcode::IfNot, cond_reg, skip_label, 1, P4::Unused);

        Ok(())
    }

    /// Build column index map
    fn build_column_map(&mut self) {
        // In a real implementation, we'd populate this from schema
        for i in 0..self.num_columns {
            self.column_map.insert(format!("col{}", i), i);
        }
    }

    /// Get column index by name (simplified lookup)
    fn get_column_index(&self, name: &str) -> usize {
        if let Some(&idx) = self.column_map.get(name) {
            idx
        } else {
            let hash: usize = name.bytes().fold(0, |acc, b| acc.wrapping_add(b as usize));
            hash % self.num_columns
        }
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
                let col_idx = col_ref.column_index.unwrap_or_else(|| {
                    if let Some(&idx) = self.column_map.get(&col_ref.column) {
                        idx as i32
                    } else {
                        self.get_column_index(&col_ref.column) as i32
                    }
                });
                if col_idx < 0 {
                    self.emit(Opcode::Rowid, self.table_cursor, dest_reg, 0, P4::Unused);
                } else {
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

impl Default for DeleteCompiler {
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
