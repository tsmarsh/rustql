//! WHERE clause code generation
//!
//! This module generates VDBE bytecode from query plans produced by the
//! WHERE clause optimizer. Corresponds to wherecode.c.

use crate::error::{Error, ErrorCode, Result};
use crate::parser::ast::{BinaryOp, Expr};
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

use super::where_clause::{TermOp, WhereInfo, WhereLevelFlags, WherePlan, WhereTerm};

// ============================================================================
// Constants
// ============================================================================

/// Maximum number of IN values to inline
const MAX_IN_VALUES: usize = 100;

// ============================================================================
// Affinity
// ============================================================================

/// Type affinity for columns
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Affinity {
    Blob,
    Text,
    Numeric,
    Integer,
    Real,
    None,
}

impl Affinity {
    /// Get affinity from SQLite type name
    pub fn from_type_name(type_name: &str) -> Self {
        let upper = type_name.to_uppercase();

        if upper.contains("INT") {
            Affinity::Integer
        } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
            Affinity::Text
        } else if upper.contains("BLOB") || upper.is_empty() {
            Affinity::Blob
        } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
            Affinity::Real
        } else {
            Affinity::Numeric
        }
    }

    /// Convert to P4 representation
    pub fn to_p4(&self) -> P4 {
        let ch = match self {
            Affinity::Blob => 'A',
            Affinity::Text => 'B',
            Affinity::Numeric => 'C',
            Affinity::Integer => 'D',
            Affinity::Real => 'E',
            Affinity::None => '\0',
        };
        P4::Text(ch.to_string())
    }
}

// ============================================================================
// WhereCodeGen
// ============================================================================

/// WHERE clause code generator
pub struct WhereCodeGen {
    /// Generated VDBE operations
    ops: Vec<VdbeOp>,

    /// Next register to allocate
    next_reg: i32,

    /// Next cursor to allocate
    next_cursor: i32,

    /// Label counter
    next_label: i32,

    /// Labels pending resolution
    labels: std::collections::HashMap<i32, Option<i32>>,

    /// Loop continuation addresses (one per level)
    loop_cont_addrs: Vec<i32>,

    /// Loop exit addresses
    loop_exit_addrs: Vec<i32>,
}

impl WhereCodeGen {
    /// Create a new code generator
    pub fn new() -> Self {
        WhereCodeGen {
            ops: Vec::new(),
            next_reg: 1,
            next_cursor: 0,
            next_label: -1,
            labels: std::collections::HashMap::new(),
            loop_cont_addrs: Vec::new(),
            loop_exit_addrs: Vec::new(),
        }
    }

    /// Generate code for a WHERE clause
    pub fn generate(&mut self, info: &WhereInfo) -> Result<Vec<VdbeOp>> {
        // Generate code for each level
        for (i, level) in info.levels.iter().enumerate() {
            self.generate_level(i, level, &info.terms)?;
        }

        // Resolve all pending labels
        self.resolve_labels()?;

        Ok(std::mem::take(&mut self.ops))
    }

    /// Generate code for one loop level
    fn generate_level(
        &mut self,
        level_idx: usize,
        level: &super::where_clause::WhereLevel,
        terms: &[WhereTerm],
    ) -> Result<()> {
        // Allocate continuation and exit labels
        let cont_label = self.alloc_label();
        let exit_label = self.alloc_label();
        self.loop_cont_addrs.push(cont_label);
        self.loop_exit_addrs.push(exit_label);

        // Generate code based on plan
        match &level.plan {
            WherePlan::FullScan => {
                self.code_full_scan(level_idx, level)?;
            }
            WherePlan::IndexScan {
                index_name,
                eq_cols,
                covering,
                has_range,
                ..
            } => {
                self.code_index_scan(
                    level_idx, level, index_name, *eq_cols, *covering, *has_range,
                )?;
            }
            WherePlan::PrimaryKey { eq_cols } => {
                self.code_pk_lookup(level_idx, level, *eq_cols)?;
            }
            WherePlan::RowidEq => {
                self.code_rowid_eq(level_idx, level)?;
            }
            WherePlan::RowidRange { has_start, has_end } => {
                self.code_rowid_range(level_idx, level, *has_start, *has_end)?;
            }
            WherePlan::RowidIn { term_idx } => {
                // RowidIn is handled in select/mod.rs - this code path shouldn't be reached
                // for normal SELECT queries, but add handling for completeness
                self.code_full_scan(level_idx, level)?;
                let _ = term_idx; // Suppress unused warning
            }
        }

        // Generate code for remaining WHERE terms at this level
        for &term_idx in &level.used_terms {
            if let Some(term) = terms.get(term_idx as usize) {
                if !term
                    .flags
                    .contains(super::where_clause::WhereTermFlags::CODED)
                {
                    self.code_term_filter(term, exit_label)?;
                }
            }
        }

        Ok(())
    }

    /// Generate code for a full table scan
    fn code_full_scan(
        &mut self,
        _level_idx: usize,
        level: &super::where_clause::WhereLevel,
    ) -> Result<()> {
        let cursor = level.from_idx;
        let cont_label = *self.loop_cont_addrs.last().unwrap();

        // Open table
        self.emit(
            Opcode::OpenRead,
            cursor,
            0,
            0,
            P4::Text(level.table_name.clone()),
        );

        // Rewind to start
        self.emit(Opcode::Rewind, cursor, cont_label, 0, P4::Unused);

        Ok(())
    }

    /// Generate code for an index scan
    fn code_index_scan(
        &mut self,
        _level_idx: usize,
        level: &super::where_clause::WhereLevel,
        index_name: &str,
        eq_cols: i32,
        covering: bool,
        _has_range: bool,
    ) -> Result<()> {
        let cursor = level.from_idx;
        let index_cursor = cursor + 100;
        let cont_label = *self.loop_cont_addrs.last().unwrap();

        // Open table cursor (for column reads or non-covering fallback)
        self.emit(
            Opcode::OpenRead,
            cursor,
            0,
            0,
            P4::Text(level.table_name.clone()),
        );

        // Open index cursor
        self.emit(
            Opcode::OpenRead,
            index_cursor,
            0,
            0,
            P4::Text(index_name.to_string()),
        );

        if eq_cols > 0 {
            // Build key from equality constraints
            let key_reg = self.alloc_reg();

            // For now, emit placeholder for key construction
            // In a real implementation, we'd compile the equality expressions
            self.emit(Opcode::Null, 0, key_reg, 0, P4::Unused);

            // Seek to key
            self.emit(
                Opcode::SeekGE,
                index_cursor,
                cont_label,
                key_reg,
                P4::Int64(eq_cols as i64),
            );
        } else {
            // Rewind to start
            self.emit(Opcode::Rewind, index_cursor, cont_label, 0, P4::Unused);
        }

        // Set up deferred seek from index to table
        // For covering indexes, column reads will be redirected to the index
        // For non-covering, FinishSeek will complete the table lookup
        if covering {
            // When covering, we can potentially skip the table seek entirely
            // by reading columns directly from the index
            // P4 would contain the column mapping (alt_map) if we had schema info
            self.emit(
                Opcode::DeferredSeek,
                cursor,
                0,
                index_cursor,
                P4::Unused, // TODO: Add column mapping when schema info available
            );
        } else {
            // For non-covering index scans, we need to seek to the table
            // to read columns not in the index
            self.emit(Opcode::DeferredSeek, cursor, 0, index_cursor, P4::Unused);
        }

        Ok(())
    }

    /// Generate code for a primary key lookup
    fn code_pk_lookup(
        &mut self,
        _level_idx: usize,
        level: &super::where_clause::WhereLevel,
        eq_cols: i32,
    ) -> Result<()> {
        let cursor = level.from_idx;
        let cont_label = *self.loop_cont_addrs.last().unwrap();

        // Open table
        self.emit(
            Opcode::OpenRead,
            cursor,
            0,
            0,
            P4::Text(level.table_name.clone()),
        );

        // Build key
        let key_reg = self.alloc_reg();
        self.emit(Opcode::Null, 0, key_reg, 0, P4::Unused);

        // Seek to key
        self.emit(
            Opcode::SeekGE,
            cursor,
            cont_label,
            key_reg,
            P4::Int64(eq_cols as i64),
        );

        // Mark as unique scan if appropriate
        if level.flags.contains(WhereLevelFlags::UNIQUE) {
            self.emit(Opcode::NullRow, cursor, 0, 0, P4::Unused);
        }

        Ok(())
    }

    /// Generate code for rowid equality
    fn code_rowid_eq(
        &mut self,
        _level_idx: usize,
        level: &super::where_clause::WhereLevel,
    ) -> Result<()> {
        let cursor = level.from_idx;
        let cont_label = *self.loop_cont_addrs.last().unwrap();

        // Open table
        self.emit(
            Opcode::OpenRead,
            cursor,
            0,
            0,
            P4::Text(level.table_name.clone()),
        );

        // Rowid is in a register (to be filled by caller)
        let rowid_reg = self.alloc_reg();
        self.emit(Opcode::Null, 0, rowid_reg, 0, P4::Unused);

        // Seek directly to rowid
        self.emit(Opcode::SeekRowid, cursor, cont_label, rowid_reg, P4::Unused);

        Ok(())
    }

    /// Generate code for rowid range scan
    fn code_rowid_range(
        &mut self,
        _level_idx: usize,
        level: &super::where_clause::WhereLevel,
        has_start: bool,
        _has_end: bool,
    ) -> Result<()> {
        let cursor = level.from_idx;
        let cont_label = *self.loop_cont_addrs.last().unwrap();

        // Open table
        self.emit(
            Opcode::OpenRead,
            cursor,
            0,
            0,
            P4::Text(level.table_name.clone()),
        );

        if has_start {
            // Seek to start rowid
            let start_reg = self.alloc_reg();
            self.emit(Opcode::Null, 0, start_reg, 0, P4::Unused);
            self.emit(Opcode::SeekGE, cursor, cont_label, start_reg, P4::Unused);
        } else {
            // Rewind to start
            self.emit(Opcode::Rewind, cursor, cont_label, 0, P4::Unused);
        }

        Ok(())
    }

    /// Generate filter code for a WHERE term
    fn code_term_filter(&mut self, term: &WhereTerm, skip_label: i32) -> Result<()> {
        match &term.op {
            Some(TermOp::Eq) => {
                self.code_comparison_filter(term, Opcode::Ne, skip_label)?;
            }
            Some(TermOp::Is) => {
                self.code_comparison_filter(term, Opcode::Ne, skip_label)?;
            }
            Some(TermOp::Ne) => {
                self.code_comparison_filter(term, Opcode::Eq, skip_label)?;
            }
            Some(TermOp::Lt) => {
                self.code_comparison_filter(term, Opcode::Ge, skip_label)?;
            }
            Some(TermOp::Le) => {
                self.code_comparison_filter(term, Opcode::Gt, skip_label)?;
            }
            Some(TermOp::Gt) => {
                self.code_comparison_filter(term, Opcode::Le, skip_label)?;
            }
            Some(TermOp::Ge) => {
                self.code_comparison_filter(term, Opcode::Lt, skip_label)?;
            }
            Some(TermOp::Like | TermOp::Glob) => {
                // LIKE/GLOB handled by Function opcode
                let reg = self.alloc_reg();
                self.code_expression(&term.expr, reg)?;
                self.emit(Opcode::IfNot, reg, skip_label, 0, P4::Unused);
            }
            Some(TermOp::In) => {
                self.code_in_filter(term, skip_label)?;
            }
            Some(TermOp::IsNull) => {
                let reg = self.alloc_reg();
                self.code_expression(&term.expr, reg)?;
                self.emit(Opcode::NotNull, reg, skip_label, 0, P4::Unused);
            }
            Some(TermOp::IsNotNull) => {
                let reg = self.alloc_reg();
                self.code_expression(&term.expr, reg)?;
                self.emit(Opcode::IsNull, reg, skip_label, 0, P4::Unused);
            }
            Some(TermOp::Between) => {
                // BETWEEN handled as two comparisons
                let reg = self.alloc_reg();
                self.code_expression(&term.expr, reg)?;
                self.emit(Opcode::IfNot, reg, skip_label, 0, P4::Unused);
            }
            None => {
                // Generic expression - evaluate to boolean
                let reg = self.alloc_reg();
                self.code_expression(&term.expr, reg)?;
                self.emit(Opcode::IfNot, reg, skip_label, 0, P4::Unused);
            }
        }
        Ok(())
    }

    /// Generate code for a comparison filter
    fn code_comparison_filter(
        &mut self,
        term: &WhereTerm,
        skip_op: Opcode,
        skip_label: i32,
    ) -> Result<()> {
        // Extract left and right from binary expression
        if let Expr::Binary { left, right, .. } = term.expr.as_ref() {
            let left_reg = self.alloc_reg();
            let right_reg = self.alloc_reg();

            self.code_expression(left, left_reg)?;
            self.code_expression(right, right_reg)?;

            // Comparison opcodes: P1=right operand, P2=jump target, P3=left operand
            // Lt P1 P2 P3 means "jump to P2 if r[P3] < r[P1]"
            self.emit(skip_op, right_reg, skip_label, left_reg, P4::Unused);
        }
        Ok(())
    }

    /// Generate code for an IN filter
    fn code_in_filter(&mut self, term: &WhereTerm, skip_label: i32) -> Result<()> {
        if let Expr::In {
            expr,
            list,
            negated,
            ..
        } = term.expr.as_ref()
        {
            let val_reg = self.alloc_reg();
            self.code_expression(expr, val_reg)?;

            match list {
                crate::parser::ast::InList::Values(values) => {
                    if values.len() <= MAX_IN_VALUES {
                        // Inline comparison
                        let match_label = self.alloc_label();

                        for value in values {
                            let cmp_reg = self.alloc_reg();
                            self.code_expression(value, cmp_reg)?;

                            if *negated {
                                self.emit(Opcode::Eq, val_reg, skip_label, cmp_reg, P4::Unused);
                            } else {
                                self.emit(Opcode::Eq, val_reg, match_label, cmp_reg, P4::Unused);
                            }
                        }

                        if !*negated {
                            // No match found
                            self.emit(Opcode::Goto, 0, skip_label, 0, P4::Unused);
                            self.resolve_label(match_label, self.current_addr() as i32);
                        }
                    } else {
                        // Use ephemeral table for large IN lists
                        let eph_cursor = self.alloc_cursor();
                        self.emit(Opcode::OpenEphemeral, eph_cursor, 1, 0, P4::Unused);

                        // Populate ephemeral table
                        for value in values {
                            let ins_reg = self.alloc_reg();
                            self.code_expression(value, ins_reg)?;
                            self.emit(Opcode::MakeRecord, ins_reg, 1, ins_reg, P4::Unused);
                            self.emit(Opcode::IdxInsert, eph_cursor, ins_reg, 0, P4::Unused);
                        }

                        // Search using IdxGE to find if key exists
                        let key_reg = self.alloc_reg();
                        self.emit(Opcode::MakeRecord, val_reg, 1, key_reg, P4::Unused);

                        // Search for key in ephemeral table
                        // IdxGE jumps if current index >= key
                        if *negated {
                            // NOT IN: skip if key is found
                            self.emit(Opcode::IdxGE, eph_cursor, skip_label, key_reg, P4::Int64(1));
                        } else {
                            // IN: skip if key is not found
                            let found_label = self.alloc_label();
                            self.emit(
                                Opcode::IdxGE,
                                eph_cursor,
                                found_label,
                                key_reg,
                                P4::Int64(1),
                            );
                            self.emit(Opcode::Goto, 0, skip_label, 0, P4::Unused);
                            self.resolve_label(found_label, self.current_addr() as i32);
                        }
                    }
                }
                crate::parser::ast::InList::Subquery(_) | crate::parser::ast::InList::Table(_) => {
                    // Subquery/Table IN - more complex
                    // For now, evaluate subquery result
                    let result_reg = self.alloc_reg();
                    self.emit(Opcode::Null, 0, result_reg, 0, P4::Unused);

                    if *negated {
                        self.emit(Opcode::Eq, val_reg, skip_label, result_reg, P4::Unused);
                    } else {
                        self.emit(Opcode::Ne, val_reg, skip_label, result_reg, P4::Unused);
                    }
                }
            }
        }
        Ok(())
    }

    /// Generate code to evaluate an expression into a register
    fn code_expression(&mut self, expr: &Expr, dest_reg: i32) -> Result<()> {
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
                // Column access - would need cursor and column index from schema
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
                self.code_expression(left, left_reg)?;
                self.code_expression(right, right_reg)?;

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
                    _ => {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            format!("Unsupported binary op: {:?}", op),
                        ));
                    }
                };

                // Binary opcodes: P1=right operand, P2=left operand, P3=dest
                // Arithmetic: r[P2] op r[P1] stored in r[P3]
                // Comparison: jump to P2 if r[P3] op r[P1]
                self.emit(opcode, right_reg, left_reg, dest_reg, P4::Unused);
            }
            Expr::Unary { op, expr: inner } => {
                self.code_expression(inner, dest_reg)?;
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
                            self.code_expression(arg, reg)?;
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
                // Default: null
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
            }
        }
        Ok(())
    }

    /// Generate code for loop continuation
    pub fn code_loop_cont(&mut self, level_idx: usize) -> Result<()> {
        if level_idx < self.loop_cont_addrs.len() {
            let cont_label = self.loop_cont_addrs[level_idx];
            self.resolve_label(cont_label, self.current_addr() as i32);
        }
        Ok(())
    }

    /// Generate code for loop exit
    pub fn code_loop_exit(&mut self, level_idx: usize) -> Result<()> {
        if level_idx < self.loop_exit_addrs.len() {
            let exit_label = self.loop_exit_addrs[level_idx];
            self.resolve_label(exit_label, self.current_addr() as i32);
        }
        Ok(())
    }

    /// Generate Next opcode for a level
    pub fn code_next(
        &mut self,
        level: &super::where_clause::WhereLevel,
        loop_start: i32,
    ) -> Result<()> {
        let cursor = level.from_idx;
        let cont_label = self.loop_cont_addrs.last().copied().unwrap_or(0);

        match &level.plan {
            WherePlan::IndexScan { .. } => {
                self.emit(Opcode::Next, cursor + 100, loop_start, 0, P4::Unused);
            }
            _ => {
                self.emit(Opcode::Next, cursor, loop_start, 0, P4::Unused);
            }
        }

        self.resolve_label(cont_label, self.current_addr() as i32);
        Ok(())
    }

    /// Close cursors for a level
    pub fn code_close(&mut self, level: &super::where_clause::WhereLevel) -> Result<()> {
        let cursor = level.from_idx;

        if let WherePlan::IndexScan { .. } = &level.plan {
            self.emit(Opcode::Close, cursor + 100, 0, 0, P4::Unused);
        }

        self.emit(Opcode::Close, cursor, 0, 0, P4::Unused);
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

impl Default for WhereCodeGen {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Generate VDBE code for a WHERE clause
pub fn generate_where_code(info: &WhereInfo) -> Result<Vec<VdbeOp>> {
    let mut codegen = WhereCodeGen::new();
    codegen.generate(info)
}

/// Apply affinity to a value
pub fn apply_affinity(affinity: Affinity, reg: i32) -> VdbeOp {
    VdbeOp::with_p4(Opcode::Affinity, reg, 1, 0, affinity.to_p4())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_affinity_from_type_name() {
        assert_eq!(Affinity::from_type_name("INTEGER"), Affinity::Integer);
        assert_eq!(Affinity::from_type_name("INT"), Affinity::Integer);
        assert_eq!(Affinity::from_type_name("TINYINT"), Affinity::Integer);
        assert_eq!(Affinity::from_type_name("BIGINT"), Affinity::Integer);

        assert_eq!(Affinity::from_type_name("TEXT"), Affinity::Text);
        assert_eq!(Affinity::from_type_name("VARCHAR(100)"), Affinity::Text);
        assert_eq!(Affinity::from_type_name("CHAR(10)"), Affinity::Text);
        assert_eq!(Affinity::from_type_name("CLOB"), Affinity::Text);

        assert_eq!(Affinity::from_type_name("BLOB"), Affinity::Blob);
        assert_eq!(Affinity::from_type_name(""), Affinity::Blob);

        assert_eq!(Affinity::from_type_name("REAL"), Affinity::Real);
        assert_eq!(Affinity::from_type_name("FLOAT"), Affinity::Real);
        assert_eq!(Affinity::from_type_name("DOUBLE"), Affinity::Real);

        assert_eq!(Affinity::from_type_name("NUMERIC"), Affinity::Numeric);
        assert_eq!(Affinity::from_type_name("DECIMAL"), Affinity::Numeric);
    }

    #[test]
    fn test_where_codegen_new() {
        let codegen = WhereCodeGen::new();
        assert!(codegen.ops.is_empty());
        assert_eq!(codegen.next_reg, 1);
        assert_eq!(codegen.next_cursor, 0);
    }

    #[test]
    fn test_alloc_reg() {
        let mut codegen = WhereCodeGen::new();
        assert_eq!(codegen.alloc_reg(), 1);
        assert_eq!(codegen.alloc_reg(), 2);
        assert_eq!(codegen.alloc_reg(), 3);
    }

    #[test]
    fn test_alloc_label() {
        let mut codegen = WhereCodeGen::new();
        let label1 = codegen.alloc_label();
        let label2 = codegen.alloc_label();
        assert_ne!(label1, label2);
        assert!(label1 < 0);
        assert!(label2 < 0);
    }

    #[test]
    fn test_emit_and_resolve() {
        let mut codegen = WhereCodeGen::new();
        let label = codegen.alloc_label();

        codegen.emit(Opcode::Goto, 0, label, 0, P4::Unused);
        codegen.emit(Opcode::Noop, 0, 0, 0, P4::Unused);
        codegen.resolve_label(label, codegen.current_addr() as i32);
        codegen.emit(Opcode::Halt, 0, 0, 0, P4::Unused);

        codegen.resolve_labels().unwrap();

        assert_eq!(codegen.ops[0].p2, 2);
    }

    #[test]
    fn test_generate_simple_where() {
        let info = WhereInfo::new();
        let ops = generate_where_code(&info).unwrap();
        assert!(ops.is_empty());
    }
}
