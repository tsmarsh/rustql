//! Expression Compilation
//!
//! This module compiles expression AST nodes into VDBE bytecode.
//! Based on SQLite's expr.c.

use crate::error::{Error, ErrorCode, Result};
use crate::schema::{BinaryOp, Expr, UnaryOp};
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

// ============================================================================
// Expression Compiler
// ============================================================================

/// Expression compiler generates VDBE bytecode from expressions
pub struct ExprCompiler {
    /// Generated instructions
    ops: Vec<VdbeOp>,
    /// Next available register
    next_reg: i32,
    /// Label counter for jumps
    next_label: i32,
    /// Unresolved labels (label_id -> instruction index)
    unresolved_labels: Vec<(i32, usize)>,
    /// Resolved labels (label_id -> instruction index)
    resolved_labels: Vec<(i32, i32)>,
}

impl ExprCompiler {
    /// Create a new expression compiler
    pub fn new() -> Self {
        Self {
            ops: Vec::new(),
            next_reg: 1, // Register 0 is reserved
            next_label: 0,
            unresolved_labels: Vec::new(),
            resolved_labels: Vec::new(),
        }
    }

    /// Create with a starting register
    pub fn with_start_register(start_reg: i32) -> Self {
        Self {
            next_reg: start_reg,
            ..Self::new()
        }
    }

    /// Allocate a new register
    pub fn alloc_reg(&mut self) -> i32 {
        let reg = self.next_reg;
        self.next_reg += 1;
        reg
    }

    /// Allocate multiple consecutive registers
    pub fn alloc_regs(&mut self, count: i32) -> i32 {
        let reg = self.next_reg;
        self.next_reg += count;
        reg
    }

    /// Get the next register that will be allocated
    pub fn peek_reg(&self) -> i32 {
        self.next_reg
    }

    /// Create a new label for jump targets
    pub fn make_label(&mut self) -> i32 {
        let label = self.next_label;
        self.next_label += 1;
        label
    }

    /// Add an instruction
    pub fn add_op(&mut self, opcode: Opcode, p1: i32, p2: i32, p3: i32) -> usize {
        let idx = self.ops.len();
        self.ops.push(VdbeOp::new(opcode, p1, p2, p3));
        idx
    }

    /// Add an instruction with P4
    pub fn add_op4(&mut self, opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: P4) -> usize {
        let idx = self.ops.len();
        self.ops.push(VdbeOp::with_p4(opcode, p1, p2, p3, p4));
        idx
    }

    /// Add an instruction with a label reference (to be resolved later)
    pub fn add_op_label(&mut self, opcode: Opcode, p1: i32, label: i32, p3: i32) -> usize {
        let idx = self.ops.len();
        self.ops.push(VdbeOp::new(opcode, p1, 0, p3)); // P2 will be patched
        self.unresolved_labels.push((label, idx));
        idx
    }

    /// Resolve a label to the current instruction position
    pub fn resolve_label(&mut self, label: i32) {
        let addr = self.ops.len() as i32;
        self.resolved_labels.push((label, addr));
    }

    /// Patch all unresolved label references
    pub fn patch_labels(&mut self) {
        for (label, idx) in &self.unresolved_labels {
            if let Some((_, addr)) = self.resolved_labels.iter().find(|(l, _)| l == label) {
                self.ops[*idx].p2 = *addr;
            }
        }
        self.unresolved_labels.clear();
    }

    /// Get the generated instructions
    pub fn take_ops(mut self) -> Vec<VdbeOp> {
        self.patch_labels();
        self.ops
    }

    /// Get current instruction count
    pub fn op_count(&self) -> usize {
        self.ops.len()
    }

    // ========================================================================
    // Expression Compilation
    // ========================================================================

    /// Compile expression and return the register containing the result
    pub fn compile_expr(&mut self, expr: &Expr) -> Result<i32> {
        let target = self.alloc_reg();
        self.compile_expr_target(expr, target)?;
        Ok(target)
    }

    /// Compile expression into a specific target register
    pub fn compile_expr_target(&mut self, expr: &Expr, target: i32) -> Result<()> {
        match expr {
            Expr::Null => {
                self.add_op(Opcode::Null, 0, target, 0);
            }

            Expr::Integer(i) => {
                if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                    self.add_op(Opcode::Integer, *i as i32, target, 0);
                } else {
                    self.add_op4(Opcode::Int64, 0, target, 0, P4::Int64(*i));
                }
            }

            Expr::Real(f) => {
                self.add_op4(Opcode::Real, 0, target, 0, P4::Real(*f));
            }

            Expr::String(s) => {
                self.add_op4(Opcode::String8, 0, target, 0, P4::Text(s.clone()));
            }

            Expr::Blob(b) => {
                self.add_op4(Opcode::Blob, b.len() as i32, target, 0, P4::Blob(b.clone()));
            }

            Expr::Column {
                table: _,
                column: _,
            } => {
                // Column references need cursor context
                // For now, this will be handled by the query compiler
                return Err(Error::with_message(
                    ErrorCode::Internal,
                    "column references require cursor context",
                ));
            }

            Expr::BinaryOp { left, op, right } => {
                self.compile_binary_expr(op, left, right, target)?;
            }

            Expr::UnaryOp { op, operand } => {
                self.compile_unary_expr(op, operand, target)?;
            }

            Expr::Function {
                name,
                args,
                distinct: _,
            } => {
                self.compile_function(name, args, target)?;
            }

            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                self.compile_case(operand, when_clauses, else_clause, target)?;
            }

            Expr::Cast { expr, type_name } => {
                self.compile_cast(expr, type_name, target)?;
            }

            Expr::In {
                expr,
                list,
                negated,
            } => {
                self.compile_in(expr, list, *negated, target)?;
            }

            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                self.compile_between(expr, low, high, *negated, target)?;
            }

            Expr::Like {
                expr,
                pattern,
                escape,
                negated,
            } => {
                self.compile_like(expr, pattern, escape.as_deref(), *negated, target)?;
            }

            Expr::IsNull { expr, negated } => {
                self.compile_is_null(expr, *negated, target)?;
            }

            Expr::Collate { expr, collation } => {
                // Compile the expression, then apply collation
                self.compile_expr_target(expr, target)?;
                self.add_op4(
                    Opcode::Affinity,
                    target,
                    0,
                    0,
                    P4::Collation(collation.clone()),
                );
            }

            Expr::Parameter { index, name: _ } => {
                // Variable binding - P1 is the parameter index
                let idx = index.unwrap_or(1);
                self.add_op(Opcode::Variable, idx, target, 0);
            }

            Expr::CurrentTime => {
                self.add_op4(
                    Opcode::Function,
                    0,
                    target,
                    0,
                    P4::FuncDef("current_time".to_string()),
                );
            }

            Expr::CurrentDate => {
                self.add_op4(
                    Opcode::Function,
                    0,
                    target,
                    0,
                    P4::FuncDef("current_date".to_string()),
                );
            }

            Expr::CurrentTimestamp => {
                self.add_op4(
                    Opcode::Function,
                    0,
                    target,
                    0,
                    P4::FuncDef("current_timestamp".to_string()),
                );
            }

            Expr::Subquery(_) | Expr::Exists { .. } => {
                // Subqueries need the full query compiler
                return Err(Error::with_message(
                    ErrorCode::Internal,
                    "subqueries require full query context",
                ));
            }
        }
        Ok(())
    }

    // ========================================================================
    // Binary Operators
    // ========================================================================

    fn compile_binary_expr(
        &mut self,
        op: &BinaryOp,
        left: &Expr,
        right: &Expr,
        target: i32,
    ) -> Result<()> {
        match op {
            // Arithmetic operators
            BinaryOp::Add
            | BinaryOp::Sub
            | BinaryOp::Mul
            | BinaryOp::Div
            | BinaryOp::Mod
            | BinaryOp::Concat
            | BinaryOp::BitAnd
            | BinaryOp::BitOr
            | BinaryOp::LeftShift
            | BinaryOp::RightShift => {
                let r1 = self.compile_expr(left)?;
                let r2 = self.compile_expr(right)?;

                let opcode = match op {
                    BinaryOp::Add => Opcode::Add,
                    BinaryOp::Sub => Opcode::Subtract,
                    BinaryOp::Mul => Opcode::Multiply,
                    BinaryOp::Div => Opcode::Divide,
                    BinaryOp::Mod => Opcode::Remainder,
                    BinaryOp::Concat => Opcode::Concat,
                    BinaryOp::BitAnd => Opcode::BitAnd,
                    BinaryOp::BitOr => Opcode::BitOr,
                    BinaryOp::LeftShift => Opcode::ShiftLeft,
                    BinaryOp::RightShift => Opcode::ShiftRight,
                    _ => unreachable!(),
                };

                self.add_op(opcode, r2, r1, target);
            }

            // Comparison operators
            BinaryOp::Eq
            | BinaryOp::Ne
            | BinaryOp::Lt
            | BinaryOp::Le
            | BinaryOp::Gt
            | BinaryOp::Ge
            | BinaryOp::Is
            | BinaryOp::IsNot => {
                self.compile_comparison(op, left, right, target)?;
            }

            // Logical operators (short-circuit)
            BinaryOp::And => {
                self.compile_and(left, right, target)?;
            }
            BinaryOp::Or => {
                self.compile_or(left, right, target)?;
            }

            // Pattern matching
            BinaryOp::Glob => {
                let r1 = self.compile_expr(left)?;
                let r2 = self.compile_expr(right)?;
                self.add_op(Opcode::Glob, r1, target, r2);
            }
            BinaryOp::Match => {
                // MATCH is for FTS
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "MATCH operator requires FTS context",
                ));
            }
            BinaryOp::Regexp => {
                let r1 = self.compile_expr(left)?;
                let r2 = self.compile_expr(right)?;
                self.add_op(Opcode::Regexp, r1, target, r2);
            }
        }
        Ok(())
    }

    fn compile_comparison(
        &mut self,
        op: &BinaryOp,
        left: &Expr,
        right: &Expr,
        target: i32,
    ) -> Result<()> {
        let r1 = self.compile_expr(left)?;
        let r2 = self.compile_expr(right)?;

        // Generate: if (cmp) goto true; result = 0; goto end; true: result = 1; end:
        let lbl_true = self.make_label();
        let lbl_end = self.make_label();

        let opcode = match op {
            BinaryOp::Eq | BinaryOp::Is => Opcode::Eq,
            BinaryOp::Ne | BinaryOp::IsNot => Opcode::Ne,
            BinaryOp::Lt => Opcode::Lt,
            BinaryOp::Le => Opcode::Le,
            BinaryOp::Gt => Opcode::Gt,
            BinaryOp::Ge => Opcode::Ge,
            _ => return Err(Error::new(ErrorCode::Internal)),
        };

        self.add_op_label(opcode, r1, lbl_true, r2);
        self.add_op(Opcode::Integer, 0, target, 0);
        self.add_op_label(Opcode::Goto, 0, lbl_end, 0);
        self.resolve_label(lbl_true);
        self.add_op(Opcode::Integer, 1, target, 0);
        self.resolve_label(lbl_end);

        Ok(())
    }

    fn compile_and(&mut self, left: &Expr, right: &Expr, target: i32) -> Result<()> {
        // Short-circuit AND:
        // eval left; if false, result = 0, skip right
        // eval right; result = right value

        let lbl_false = self.make_label();
        let lbl_end = self.make_label();

        // Evaluate left
        let r1 = self.compile_expr(left)?;

        // If false, jump to false label
        self.add_op_label(Opcode::IfNot, r1, lbl_false, 0);

        // Evaluate right
        self.compile_expr_target(right, target)?;
        self.add_op_label(Opcode::Goto, 0, lbl_end, 0);

        // False path
        self.resolve_label(lbl_false);
        self.add_op(Opcode::Integer, 0, target, 0);

        self.resolve_label(lbl_end);
        Ok(())
    }

    fn compile_or(&mut self, left: &Expr, right: &Expr, target: i32) -> Result<()> {
        // Short-circuit OR:
        // eval left; if true, result = 1, skip right
        // eval right; result = right value

        let lbl_true = self.make_label();
        let lbl_end = self.make_label();

        // Evaluate left
        let r1 = self.compile_expr(left)?;

        // If true, jump to true label
        self.add_op_label(Opcode::If, r1, lbl_true, 0);

        // Evaluate right
        self.compile_expr_target(right, target)?;
        self.add_op_label(Opcode::Goto, 0, lbl_end, 0);

        // True path
        self.resolve_label(lbl_true);
        self.add_op(Opcode::Integer, 1, target, 0);

        self.resolve_label(lbl_end);
        Ok(())
    }

    // ========================================================================
    // Unary Operators
    // ========================================================================

    fn compile_unary_expr(&mut self, op: &UnaryOp, operand: &Expr, target: i32) -> Result<()> {
        let r = self.compile_expr(operand)?;

        let opcode = match op {
            UnaryOp::Neg => Opcode::Negative,
            UnaryOp::Not => Opcode::Not,
            UnaryOp::BitNot => Opcode::BitNot,
            UnaryOp::Plus => {
                // Unary plus is a no-op, just copy
                self.add_op(Opcode::Copy, r, target, 0);
                return Ok(());
            }
        };

        self.add_op(opcode, r, target, 0);
        Ok(())
    }

    // ========================================================================
    // CASE Expression
    // ========================================================================

    fn compile_case(
        &mut self,
        operand: &Option<Box<Expr>>,
        when_clauses: &[(Expr, Expr)],
        else_clause: &Option<Box<Expr>>,
        target: i32,
    ) -> Result<()> {
        let lbl_end = self.make_label();

        // If there's a case operand, evaluate it first
        let operand_reg = if let Some(op) = operand {
            Some(self.compile_expr(op)?)
        } else {
            None
        };

        // Generate code for each WHEN clause
        for (when_expr, then_expr) in when_clauses {
            let lbl_next = self.make_label();

            if let Some(op_reg) = operand_reg {
                // Simple CASE: compare operand with WHEN value
                let when_reg = self.compile_expr(when_expr)?;
                self.add_op_label(Opcode::Ne, op_reg, lbl_next, when_reg);
            } else {
                // Searched CASE: evaluate WHEN as boolean
                let when_reg = self.compile_expr(when_expr)?;
                self.add_op_label(Opcode::IfNot, when_reg, lbl_next, 0);
            }

            // THEN clause
            self.compile_expr_target(then_expr, target)?;
            self.add_op_label(Opcode::Goto, 0, lbl_end, 0);

            self.resolve_label(lbl_next);
        }

        // ELSE clause (or NULL if not specified)
        if let Some(else_expr) = else_clause {
            self.compile_expr_target(else_expr, target)?;
        } else {
            self.add_op(Opcode::Null, 0, target, 0);
        }

        self.resolve_label(lbl_end);
        Ok(())
    }

    // ========================================================================
    // CAST Expression
    // ========================================================================

    fn compile_cast(&mut self, expr: &Expr, type_name: &str, target: i32) -> Result<()> {
        self.compile_expr_target(expr, target)?;

        // Determine affinity from type name
        let affinity = type_name_to_affinity(type_name);
        self.add_op4(
            Opcode::Cast,
            target,
            affinity as i32,
            0,
            P4::Text(type_name.to_string()),
        );

        Ok(())
    }

    // ========================================================================
    // IN Expression
    // ========================================================================

    fn compile_in(&mut self, expr: &Expr, list: &[Expr], negated: bool, target: i32) -> Result<()> {
        if list.is_empty() {
            // Empty IN list is always false (or true if negated)
            self.add_op(Opcode::Integer, if negated { 1 } else { 0 }, target, 0);
            return Ok(());
        }

        let lbl_found = self.make_label();
        let lbl_end = self.make_label();

        // Evaluate the expression
        let expr_reg = self.compile_expr(expr)?;

        // Check against each value in the list
        for item in list {
            let item_reg = self.compile_expr(item)?;
            self.add_op_label(Opcode::Eq, expr_reg, lbl_found, item_reg);
        }

        // Not found
        self.add_op(Opcode::Integer, if negated { 1 } else { 0 }, target, 0);
        self.add_op_label(Opcode::Goto, 0, lbl_end, 0);

        // Found
        self.resolve_label(lbl_found);
        self.add_op(Opcode::Integer, if negated { 0 } else { 1 }, target, 0);

        self.resolve_label(lbl_end);
        Ok(())
    }

    // ========================================================================
    // BETWEEN Expression
    // ========================================================================

    fn compile_between(
        &mut self,
        expr: &Expr,
        low: &Expr,
        high: &Expr,
        negated: bool,
        target: i32,
    ) -> Result<()> {
        let lbl_false = self.make_label();
        let lbl_end = self.make_label();

        // Evaluate expression
        let expr_reg = self.compile_expr(expr)?;
        let low_reg = self.compile_expr(low)?;
        let high_reg = self.compile_expr(high)?;

        // Check expr >= low
        self.add_op_label(Opcode::Lt, expr_reg, lbl_false, low_reg);

        // Check expr <= high
        self.add_op_label(Opcode::Gt, expr_reg, lbl_false, high_reg);

        // In range
        self.add_op(Opcode::Integer, if negated { 0 } else { 1 }, target, 0);
        self.add_op_label(Opcode::Goto, 0, lbl_end, 0);

        // Out of range
        self.resolve_label(lbl_false);
        self.add_op(Opcode::Integer, if negated { 1 } else { 0 }, target, 0);

        self.resolve_label(lbl_end);
        Ok(())
    }

    // ========================================================================
    // LIKE Expression
    // ========================================================================

    fn compile_like(
        &mut self,
        expr: &Expr,
        pattern: &Expr,
        escape: Option<&Expr>,
        negated: bool,
        target: i32,
    ) -> Result<()> {
        let expr_reg = self.compile_expr(expr)?;
        let pattern_reg = self.compile_expr(pattern)?;

        if escape.is_some() {
            // LIKE with ESCAPE requires special handling
            return Err(Error::with_message(
                ErrorCode::Error,
                "LIKE with ESCAPE not yet implemented",
            ));
        }

        // Generate LIKE opcode
        self.add_op(Opcode::Like, expr_reg, target, pattern_reg);

        if negated {
            // Negate the result
            self.add_op(Opcode::Not, target, target, 0);
        }

        Ok(())
    }

    // ========================================================================
    // IS NULL Expression
    // ========================================================================

    fn compile_is_null(&mut self, expr: &Expr, negated: bool, target: i32) -> Result<()> {
        let lbl_true = self.make_label();
        let lbl_end = self.make_label();

        let expr_reg = self.compile_expr(expr)?;

        let opcode = if negated {
            Opcode::NotNull
        } else {
            Opcode::IsNull
        };

        self.add_op_label(opcode, expr_reg, lbl_true, 0);
        self.add_op(Opcode::Integer, 0, target, 0);
        self.add_op_label(Opcode::Goto, 0, lbl_end, 0);

        self.resolve_label(lbl_true);
        self.add_op(Opcode::Integer, 1, target, 0);

        self.resolve_label(lbl_end);
        Ok(())
    }

    // ========================================================================
    // Function Calls
    // ========================================================================

    fn compile_function(&mut self, name: &str, args: &[Expr], target: i32) -> Result<()> {
        // Evaluate arguments into consecutive registers
        let args_base = self.peek_reg();
        for arg in args {
            self.compile_expr(arg)?;
        }

        // Generate function call
        self.add_op4(
            Opcode::Function,
            args.len() as i32,
            target,
            args_base,
            P4::FuncDef(name.to_string()),
        );

        Ok(())
    }
}

impl Default for ExprCompiler {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert type name to affinity code
fn type_name_to_affinity(type_name: &str) -> u8 {
    use crate::schema::type_affinity;
    use crate::schema::Affinity;

    match type_affinity(type_name) {
        Affinity::Integer => 1,
        Affinity::Real => 2,
        Affinity::Text => 3,
        Affinity::Blob => 4,
        Affinity::Numeric => 5,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile_null() {
        let mut compiler = ExprCompiler::new();
        let target = compiler.compile_expr(&Expr::Null).unwrap();
        let ops = compiler.take_ops();

        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].opcode, Opcode::Null);
        assert_eq!(ops[0].p2, target);
    }

    #[test]
    fn test_compile_integer_small() {
        let mut compiler = ExprCompiler::new();
        let target = compiler.compile_expr(&Expr::Integer(42)).unwrap();
        let ops = compiler.take_ops();

        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].opcode, Opcode::Integer);
        assert_eq!(ops[0].p1, 42);
        assert_eq!(ops[0].p2, target);
    }

    #[test]
    fn test_compile_integer_large() {
        let mut compiler = ExprCompiler::new();
        let large = i64::MAX;
        let target = compiler.compile_expr(&Expr::Integer(large)).unwrap();
        let ops = compiler.take_ops();

        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].opcode, Opcode::Int64);
        assert_eq!(ops[0].p4, P4::Int64(large));
        assert_eq!(ops[0].p2, target);
    }

    #[test]
    fn test_compile_string() {
        let mut compiler = ExprCompiler::new();
        let target = compiler
            .compile_expr(&Expr::String("hello".to_string()))
            .unwrap();
        let ops = compiler.take_ops();

        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].opcode, Opcode::String8);
        assert_eq!(ops[0].p4, P4::Text("hello".to_string()));
        assert_eq!(ops[0].p2, target);
    }

    #[test]
    fn test_compile_binary_add() {
        let mut compiler = ExprCompiler::new();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Integer(10)),
            op: BinaryOp::Add,
            right: Box::new(Expr::Integer(20)),
        };
        compiler.compile_expr(&expr).unwrap();
        let ops = compiler.take_ops();

        // Should have: Integer(10), Integer(20), Add
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[0].opcode, Opcode::Integer);
        assert_eq!(ops[0].p1, 10);
        assert_eq!(ops[1].opcode, Opcode::Integer);
        assert_eq!(ops[1].p1, 20);
        assert_eq!(ops[2].opcode, Opcode::Add);
    }

    #[test]
    fn test_compile_comparison() {
        let mut compiler = ExprCompiler::new();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Integer(5)),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Integer(10)),
        };
        compiler.compile_expr(&expr).unwrap();
        let ops = compiler.take_ops();

        // Should have: Integer(5), Integer(10), Lt(jump), Integer(0), Goto, Integer(1)
        assert!(ops.len() >= 4);
        assert_eq!(ops[0].opcode, Opcode::Integer);
        assert_eq!(ops[1].opcode, Opcode::Integer);
        assert_eq!(ops[2].opcode, Opcode::Lt);
    }

    #[test]
    fn test_compile_unary_neg() {
        let mut compiler = ExprCompiler::new();
        let expr = Expr::UnaryOp {
            op: UnaryOp::Neg,
            operand: Box::new(Expr::Integer(42)),
        };
        compiler.compile_expr(&expr).unwrap();
        let ops = compiler.take_ops();

        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].opcode, Opcode::Integer);
        assert_eq!(ops[1].opcode, Opcode::Negative);
    }

    #[test]
    fn test_compile_unary_not() {
        let mut compiler = ExprCompiler::new();
        let expr = Expr::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(Expr::Integer(1)),
        };
        compiler.compile_expr(&expr).unwrap();
        let ops = compiler.take_ops();

        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].opcode, Opcode::Integer);
        assert_eq!(ops[1].opcode, Opcode::Not);
    }

    #[test]
    fn test_compile_and_short_circuit() {
        let mut compiler = ExprCompiler::new();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Integer(0)),
            op: BinaryOp::And,
            right: Box::new(Expr::Integer(1)),
        };
        compiler.compile_expr(&expr).unwrap();
        let ops = compiler.take_ops();

        // Should have short-circuit logic with IfNot
        assert!(ops.iter().any(|op| op.opcode == Opcode::IfNot));
    }

    #[test]
    fn test_compile_or_short_circuit() {
        let mut compiler = ExprCompiler::new();
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Integer(1)),
            op: BinaryOp::Or,
            right: Box::new(Expr::Integer(0)),
        };
        compiler.compile_expr(&expr).unwrap();
        let ops = compiler.take_ops();

        // Should have short-circuit logic with If
        assert!(ops.iter().any(|op| op.opcode == Opcode::If));
    }

    #[test]
    fn test_compile_is_null() {
        let mut compiler = ExprCompiler::new();
        let expr = Expr::IsNull {
            expr: Box::new(Expr::Null),
            negated: false,
        };
        compiler.compile_expr(&expr).unwrap();
        let ops = compiler.take_ops();

        assert!(ops.iter().any(|op| op.opcode == Opcode::IsNull));
    }

    #[test]
    fn test_compile_case_simple() {
        let mut compiler = ExprCompiler::new();
        let expr = Expr::Case {
            operand: Some(Box::new(Expr::Integer(1))),
            when_clauses: vec![
                (Expr::Integer(1), Expr::String("one".to_string())),
                (Expr::Integer(2), Expr::String("two".to_string())),
            ],
            else_clause: Some(Box::new(Expr::String("other".to_string()))),
        };
        compiler.compile_expr(&expr).unwrap();
        let ops = compiler.take_ops();

        // Should have comparison and jump logic
        assert!(!ops.is_empty());
        assert!(ops.iter().any(|op| op.opcode == Opcode::Ne));
    }

    #[test]
    fn test_compile_between() {
        let mut compiler = ExprCompiler::new();
        let expr = Expr::Between {
            expr: Box::new(Expr::Integer(5)),
            low: Box::new(Expr::Integer(1)),
            high: Box::new(Expr::Integer(10)),
            negated: false,
        };
        compiler.compile_expr(&expr).unwrap();
        let ops = compiler.take_ops();

        // Should have Lt and Gt checks
        assert!(ops.iter().any(|op| op.opcode == Opcode::Lt));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Gt));
    }

    #[test]
    fn test_compile_in_list() {
        let mut compiler = ExprCompiler::new();
        let expr = Expr::In {
            expr: Box::new(Expr::Integer(2)),
            list: vec![Expr::Integer(1), Expr::Integer(2), Expr::Integer(3)],
            negated: false,
        };
        compiler.compile_expr(&expr).unwrap();
        let ops = compiler.take_ops();

        // Should have Eq comparisons
        assert!(ops.iter().filter(|op| op.opcode == Opcode::Eq).count() >= 3);
    }

    #[test]
    fn test_compile_function() {
        let mut compiler = ExprCompiler::new();
        let expr = Expr::Function {
            name: "abs".to_string(),
            args: vec![Expr::Integer(-42)],
            distinct: false,
        };
        compiler.compile_expr(&expr).unwrap();
        let ops = compiler.take_ops();

        assert!(ops.iter().any(|op| op.opcode == Opcode::Function));
    }

    #[test]
    fn test_alloc_regs() {
        let mut compiler = ExprCompiler::new();
        let first = compiler.alloc_reg();
        assert_eq!(first, 1);

        let batch = compiler.alloc_regs(5);
        assert_eq!(batch, 2);
        assert_eq!(compiler.peek_reg(), 7);
    }

    #[test]
    fn test_label_resolution() {
        let mut compiler = ExprCompiler::new();

        let lbl = compiler.make_label();
        compiler.add_op_label(Opcode::Goto, 0, lbl, 0);
        compiler.add_op(Opcode::Noop, 0, 0, 0);
        compiler.resolve_label(lbl);
        compiler.add_op(Opcode::Halt, 0, 0, 0);

        let ops = compiler.take_ops();

        // Goto should jump to instruction 2 (the Halt)
        assert_eq!(ops[0].p2, 2);
    }
}
