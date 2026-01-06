# Translate expr.c - Expression Evaluation

## Overview
Translate expression code generation which compiles expressions into VDBE bytecode.

## Source Reference
- `sqlite3/src/expr.c` - 7,668 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Functions

### Expression Compilation
- `sqlite3ExprCode()` - Generate code for expression
- `sqlite3ExprCodeTarget()` - Code into specific register
- `sqlite3ExprCodeCopy()` - Code with copy semantics
- `sqlite3ExprCodeAndCache()` - Code with caching

### Expression Types
```rust
impl<'a> Parse<'a> {
    /// Compile expression to VDBE code
    pub fn compile_expr(&mut self, expr: &Expr) -> Result<i32> {
        let target = self.alloc_mem();
        self.compile_expr_target(expr, target)?;
        Ok(target)
    }

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
            Expr::Float(f) => {
                self.add_op4(Opcode::Real, 0, target, 0, P4::Real(*f));
            }
            Expr::String(s) => {
                self.add_op4(Opcode::String8, 0, target, 0, P4::Text(s.clone()));
            }
            Expr::Blob(b) => {
                self.add_op4(Opcode::Blob, b.len() as i32, target, 0, P4::Blob(b.clone()));
            }
            Expr::Column(col) => {
                self.compile_column_ref(col, target)?;
            }
            Expr::Variable(var) => {
                self.compile_variable(var, target)?;
            }
            Expr::Binary { op, left, right } => {
                self.compile_binary_expr(op, left, right, target)?;
            }
            Expr::Unary { op, expr } => {
                self.compile_unary_expr(op, expr, target)?;
            }
            Expr::Function { name, args, filter, over } => {
                self.compile_function(name, args, filter, over, target)?;
            }
            Expr::Case { operand, when_clauses, else_clause } => {
                self.compile_case(operand, when_clauses, else_clause, target)?;
            }
            // ... other expression types
            _ => {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "unsupported expression type"
                ));
            }
        }
        Ok(())
    }
}
```

### Binary Operators
```rust
impl<'a> Parse<'a> {
    fn compile_binary_expr(
        &mut self,
        op: &BinaryOp,
        left: &Expr,
        right: &Expr,
        target: i32,
    ) -> Result<()> {
        // Compile operands
        let r1 = self.compile_expr(left)?;
        let r2 = self.compile_expr(right)?;

        // Generate operation
        let opcode = match op {
            BinaryOp::Add => Opcode::Add,
            BinaryOp::Sub => Opcode::Subtract,
            BinaryOp::Mul => Opcode::Multiply,
            BinaryOp::Div => Opcode::Divide,
            BinaryOp::Mod => Opcode::Remainder,
            BinaryOp::Concat => Opcode::Concat,
            BinaryOp::BitAnd => Opcode::BitAnd,
            BinaryOp::BitOr => Opcode::BitOr,
            BinaryOp::LShift => Opcode::ShiftLeft,
            BinaryOp::RShift => Opcode::ShiftRight,
            _ => return self.compile_comparison(op, r1, r2, target),
        };

        self.add_op(opcode, r2, r1, target);
        Ok(())
    }
}
```

### Comparison Operators
```rust
impl<'a> Parse<'a> {
    fn compile_comparison(
        &mut self,
        op: &BinaryOp,
        r1: i32,
        r2: i32,
        target: i32,
    ) -> Result<()> {
        // For comparisons, we generate: if (cmp) goto true; result = 0; goto end; true: result = 1; end:
        let lbl_true = self.make_label();
        let lbl_end = self.make_label();

        let opcode = match op {
            BinaryOp::Eq => Opcode::Eq,
            BinaryOp::Ne => Opcode::Ne,
            BinaryOp::Lt => Opcode::Lt,
            BinaryOp::Le => Opcode::Le,
            BinaryOp::Gt => Opcode::Gt,
            BinaryOp::Ge => Opcode::Ge,
            _ => return Err(Error::new(ErrorCode::Internal)),
        };

        self.add_op(opcode, r1, lbl_true, r2);
        self.add_op(Opcode::Integer, 0, target, 0);
        self.add_op(Opcode::Goto, 0, lbl_end, 0);
        self.resolve_label(lbl_true);
        self.add_op(Opcode::Integer, 1, target, 0);
        self.resolve_label(lbl_end);

        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] Literal code generation (NULL, INT, FLOAT, STRING, BLOB)
- [ ] Column reference code
- [ ] Variable binding code
- [ ] Arithmetic operators (+, -, *, /, %)
- [ ] Comparison operators (=, <>, <, <=, >, >=)
- [ ] Logical operators (AND, OR, NOT)
- [ ] Bitwise operators (&, |, ~, <<, >>)
- [ ] String concatenation (||)
- [ ] CASE expressions
- [ ] Function calls
- [ ] CAST expressions
- [ ] Subquery expressions
- [ ] IN expressions
- [ ] BETWEEN expressions
- [ ] LIKE/GLOB expressions
