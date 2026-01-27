//! Trigger Compilation and Execution
//!
//! This module handles:
//! - CREATE TRIGGER statement compilation
//! - DROP TRIGGER statement compilation
//! - Trigger firing infrastructure
//! - OLD/NEW row pseudo-table access

use std::sync::Arc;

use crate::error::{Error, ErrorCode, Result};
use crate::parser::ast::{CreateTriggerStmt, TriggerEvent as AstTriggerEvent, TriggerTime};
use crate::schema::{Schema, Trigger, TriggerEvent, TriggerTiming};
use crate::vdbe::ops::{Opcode, SubProgram, VdbeOp, P4};

// ============================================================================
// Trigger Compilation
// ============================================================================

/// Compile a CREATE TRIGGER statement to VDBE bytecode
pub fn compile_create_trigger(
    schema: &mut Schema,
    create: &CreateTriggerStmt,
    sql: &str,
) -> Result<Vec<VdbeOp>> {
    // Validate table exists
    let table_name = &create.table;
    let _table = schema.table(table_name).ok_or_else(|| {
        Error::with_message(ErrorCode::Error, format!("no such table: {}", table_name))
    })?;

    // Check for INSTEAD OF on regular table (table.is_virtual is false for views)
    // In SQLite, views are stored in sqlite_master with type='view', not as regular tables
    // For now, we'll skip this validation since we'd need to track views separately

    // Convert AST types to schema types
    let timing = match create.time {
        TriggerTime::Before => TriggerTiming::Before,
        TriggerTime::After => TriggerTiming::After,
        TriggerTime::InsteadOf => TriggerTiming::InsteadOf,
    };

    let (event, update_columns) = match &create.event {
        AstTriggerEvent::Delete => (TriggerEvent::Delete, None),
        AstTriggerEvent::Insert => (TriggerEvent::Insert, None),
        AstTriggerEvent::Update(cols) => (TriggerEvent::Update, cols.clone()),
    };

    // Create trigger definition
    // Note: We store the raw SQL and re-parse body when needed
    // This avoids complex type conversions between AST and schema types
    let trigger = Trigger {
        name: create.name.name.clone(),
        table: table_name.clone(),
        timing,
        event,
        for_each_row: create.for_each_row,
        update_columns,
        when_clause: None, // Parsed from SQL when needed
        body: Vec::new(),  // Parsed from SQL when needed
        sql: Some(sql.to_string()),
    };

    // Check for duplicate trigger
    let trigger_name_lower = trigger.name.to_lowercase();
    if schema.triggers.contains_key(&trigger_name_lower) {
        if create.if_not_exists {
            // Return success without doing anything
            let mut ops = Vec::new();
            ops.push(make_op(Opcode::Init, 0, 1, 0, P4::Unused));
            ops.push(make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
            return Ok(ops);
        }
        return Err(Error::with_message(
            ErrorCode::Error,
            format!("trigger {} already exists", trigger.name),
        ));
    }

    // Store in schema
    let trigger_name = trigger.name.clone();
    schema
        .triggers
        .insert(trigger_name_lower, Arc::new(trigger));

    // Generate bytecode to record in sqlite_master
    let mut ops = Vec::new();
    ops.push(make_op(Opcode::Init, 0, 1, 0, P4::Unused));

    // In a full implementation, we'd insert into sqlite_master here
    // For now, the trigger is stored in memory
    ops.push(make_op(
        Opcode::Noop,
        0,
        0,
        0,
        P4::Text(format!("CREATE TRIGGER {}", trigger_name)),
    ));

    ops.push(make_op(Opcode::Halt, 0, 0, 0, P4::Unused));

    Ok(ops)
}

/// Compile a DROP TRIGGER statement to VDBE bytecode
pub fn compile_drop_trigger(
    schema: &mut Schema,
    name: &str,
    if_exists: bool,
) -> Result<Vec<VdbeOp>> {
    let name_lower = name.to_lowercase();

    if !schema.triggers.contains_key(&name_lower) {
        if if_exists {
            let mut ops = Vec::new();
            ops.push(make_op(Opcode::Init, 0, 1, 0, P4::Unused));
            ops.push(make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
            return Ok(ops);
        }
        return Err(Error::with_message(
            ErrorCode::Error,
            format!("no such trigger: {}", name),
        ));
    }

    // Remove from schema
    schema.triggers.remove(&name_lower);

    let mut ops = Vec::new();
    ops.push(make_op(Opcode::Init, 0, 1, 0, P4::Unused));
    ops.push(make_op(
        Opcode::Noop,
        0,
        0,
        0,
        P4::Text(format!("DROP TRIGGER {}", name)),
    ));
    ops.push(make_op(Opcode::Halt, 0, 0, 0, P4::Unused));

    Ok(ops)
}

// ============================================================================
// Trigger Firing
// ============================================================================

/// Trigger timing/event bits for efficient matching
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TriggerMask(u8);

impl TriggerMask {
    /// Before timing
    pub const BEFORE: u8 = 0x01;
    /// After timing
    pub const AFTER: u8 = 0x02;
    /// Instead of timing
    pub const INSTEAD_OF: u8 = 0x04;
    /// Insert event
    pub const INSERT: u8 = 0x08;
    /// Delete event
    pub const DELETE: u8 = 0x10;
    /// Update event
    pub const UPDATE: u8 = 0x20;

    pub fn new(timing: TriggerTiming, event: TriggerEvent) -> Self {
        let mut bits = 0u8;

        bits |= match timing {
            TriggerTiming::Before => Self::BEFORE,
            TriggerTiming::After => Self::AFTER,
            TriggerTiming::InsteadOf => Self::INSTEAD_OF,
        };

        bits |= match event {
            TriggerEvent::Insert => Self::INSERT,
            TriggerEvent::Delete => Self::DELETE,
            TriggerEvent::Update => Self::UPDATE,
        };

        Self(bits)
    }

    pub fn matches(&self, other: &TriggerMask) -> bool {
        // Check timing matches
        let timing_match = (self.0 & 0x07) == (other.0 & 0x07);
        // Check event matches
        let event_match = (self.0 & 0x38) == (other.0 & 0x38);
        timing_match && event_match
    }

    pub fn bits(&self) -> u8 {
        self.0
    }
}

/// Find triggers that match a given operation
pub fn find_matching_triggers(
    schema: &Schema,
    table_name: &str,
    timing: TriggerTiming,
    event: TriggerEvent,
    update_columns: Option<&[String]>,
) -> Vec<Arc<Trigger>> {
    let target_mask = TriggerMask::new(timing, event);

    schema
        .triggers
        .values()
        .filter(|trigger| {
            // Check table matches
            if trigger.table.to_lowercase() != table_name.to_lowercase() {
                return false;
            }

            // Check timing/event matches
            let trigger_mask = TriggerMask::new(trigger.timing, trigger.event);
            if !trigger_mask.matches(&target_mask) {
                return false;
            }

            // For UPDATE triggers, check column list
            if event == TriggerEvent::Update {
                if let (Some(trigger_cols), Some(update_cols)) =
                    (&trigger.update_columns, update_columns)
                {
                    // Trigger only fires if at least one of its columns is being updated
                    let trigger_cols_lower: Vec<_> =
                        trigger_cols.iter().map(|c| c.to_lowercase()).collect();
                    let update_cols_lower: Vec<_> =
                        update_cols.iter().map(|c| c.to_lowercase()).collect();

                    let has_match = trigger_cols_lower
                        .iter()
                        .any(|c| update_cols_lower.contains(c));
                    if !has_match {
                        return false;
                    }
                }
            }

            true
        })
        .cloned()
        .collect()
}

/// Compiler for trigger body statements
///
/// This compiles trigger body SQL into VDBE bytecode that can be executed
/// as a subprogram. It handles OLD/NEW pseudo-table references by converting
/// them to Param opcodes.
pub struct TriggerBodyCompiler<'s> {
    /// Schema for name resolution
    schema: Option<&'s Schema>,
    /// Generated opcodes
    ops: Vec<VdbeOp>,
    /// Next register to allocate
    next_reg: i32,
    /// Next cursor to allocate (pub for caller to track cursor usage)
    pub(crate) next_cursor: i32,
    /// Next label
    next_label: i32,
    /// Table name this trigger is on (for column resolution)
    table_name: String,
    /// Column map for the trigger's table: column_name -> index
    column_map: std::collections::HashMap<String, usize>,
    /// Labels for resolution
    labels: std::collections::HashMap<i32, i32>,
    /// Number of columns in the table
    num_columns: usize,
}

impl<'s> TriggerBodyCompiler<'s> {
    /// Create a new trigger body compiler
    ///
    /// The cursor_offset parameter is used to avoid cursor number conflicts with the
    /// parent program. Cursors in the subprogram will be numbered starting from cursor_offset.
    pub fn new(schema: Option<&'s Schema>, table_name: &str, cursor_offset: i32) -> Self {
        let mut compiler = Self {
            schema,
            ops: Vec::new(),
            next_reg: 1,
            next_cursor: cursor_offset,
            next_label: -1,
            table_name: table_name.to_string(),
            column_map: std::collections::HashMap::new(),
            labels: std::collections::HashMap::new(),
            num_columns: 0,
        };

        // Build column map from schema
        if let Some(schema) = schema {
            let table_lower = table_name.to_lowercase();
            if let Some(table) = schema.tables.get(&table_lower) {
                compiler.num_columns = table.columns.len();
                for (idx, col) in table.columns.iter().enumerate() {
                    compiler.column_map.insert(col.name.to_lowercase(), idx);
                }
            }
        }

        compiler
    }

    /// Allocate a register
    fn alloc_reg(&mut self) -> i32 {
        let r = self.next_reg;
        self.next_reg += 1;
        r
    }

    /// Allocate multiple consecutive registers, returns the first
    fn alloc_regs(&mut self, count: i32) -> i32 {
        let r = self.next_reg;
        self.next_reg += count;
        r
    }

    /// Allocate a cursor
    fn alloc_cursor(&mut self) -> i32 {
        let c = self.next_cursor;
        self.next_cursor += 1;
        c
    }

    /// Allocate a label
    fn alloc_label(&mut self) -> i32 {
        let l = self.next_label;
        self.next_label -= 1;
        l
    }

    /// Current address (for label resolution)
    fn current_addr(&self) -> usize {
        self.ops.len()
    }

    /// Resolve a label to an address
    fn resolve_label(&mut self, label: i32, addr: i32) {
        self.labels.insert(label, addr);
    }

    /// Emit an opcode
    fn emit(&mut self, opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: P4) {
        self.ops.push(VdbeOp {
            opcode,
            p1,
            p2,
            p3,
            p4,
            p5: 0,
            comment: None,
        });
    }

    /// Compile a trigger body to a SubProgram
    ///
    /// Takes the parsed trigger body statements and compiles them to VDBE bytecode.
    /// OLD/NEW references are converted to Param opcodes.
    pub fn compile_body(&mut self, body: &[crate::parser::ast::Stmt]) -> Result<SubProgram> {
        // Compile each statement in the body
        for stmt in body {
            self.compile_stmt(stmt)?;
        }

        // Add Halt at the end
        self.emit(Opcode::Halt, 0, 0, 0, P4::Unused);

        // Resolve labels
        self.resolve_labels()?;

        Ok(SubProgram {
            ops: std::mem::take(&mut self.ops),
            n_mem: self.next_reg,
            n_cursor: self.next_cursor,
            trigger: None,
        })
    }

    /// Resolve all labels to actual addresses
    fn resolve_labels(&mut self) -> Result<()> {
        for op in &mut self.ops {
            // Check P2 for negative (label) values
            if op.p2 < 0 {
                if let Some(&addr) = self.labels.get(&op.p2) {
                    op.p2 = addr;
                }
            }
        }
        Ok(())
    }

    /// Compile a single statement
    fn compile_stmt(&mut self, stmt: &crate::parser::ast::Stmt) -> Result<()> {
        use crate::parser::ast::Stmt;

        match stmt {
            Stmt::Insert(insert) => self.compile_insert(insert),
            Stmt::Update(update) => self.compile_update(update),
            Stmt::Delete(delete) => self.compile_delete(delete),
            Stmt::Select(select) => self.compile_select(select),
            _ => {
                // Other statements not supported in triggers
                Err(Error::with_message(
                    ErrorCode::Error,
                    "unsupported statement in trigger body",
                ))
            }
        }
    }

    /// Compile an INSERT statement in a trigger body
    fn compile_insert(&mut self, insert: &crate::parser::ast::InsertStmt) -> Result<()> {
        use crate::parser::ast::InsertSource;

        // Open target table for writing
        let cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenWrite,
            cursor,
            0,
            0,
            P4::Text(insert.table.name.clone()),
        );

        // Handle INSERT ... VALUES
        if let InsertSource::Values(rows) = &insert.source {
            for row in rows {
                // Build the record from values
                let base_reg = self.alloc_reg();
                let num_cols = row.len();

                // Allocate registers for all columns
                for _ in 1..num_cols {
                    self.alloc_reg();
                }

                // Compile each value expression
                for (i, expr) in row.iter().enumerate() {
                    let dest_reg = base_reg + i as i32;
                    self.compile_expr(expr, dest_reg)?;
                }

                // Allocate rowid register
                let rowid_reg = self.alloc_reg();
                self.emit(Opcode::NewRowid, cursor, rowid_reg, 0, P4::Unused);

                // Make the record
                let record_reg = self.alloc_reg();
                self.emit(
                    Opcode::MakeRecord,
                    base_reg,
                    num_cols as i32,
                    record_reg,
                    P4::Unused,
                );

                // Insert
                self.emit(Opcode::Insert, cursor, record_reg, rowid_reg, P4::Unused);
            }
        }

        // Close cursor
        self.emit(Opcode::Close, cursor, 0, 0, P4::Unused);

        Ok(())
    }

    /// Compile an UPDATE statement in a trigger body
    fn compile_update(&mut self, update: &crate::parser::ast::UpdateStmt) -> Result<()> {
        // Get table info
        let table_name = &update.table.name;
        let num_cols = if let Some(schema) = self.schema {
            let table_lower = table_name.to_lowercase();
            if let Some(table) = schema.tables.get(&table_lower) {
                table.columns.len()
            } else {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("no such table: {}", table_name),
                ));
            }
        } else {
            self.num_columns
        };

        if num_cols == 0 {
            return Ok(());
        }

        // Phase 1: Collect rowids of rows to update into an ephemeral table
        // This avoids issues with modifying the table while iterating

        // Open table for reading (to collect rowids)
        let read_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenRead,
            read_cursor,
            0,
            0,
            P4::Text(table_name.clone()),
        );

        // Open ephemeral table to store rowids
        let eph_cursor = self.alloc_cursor();
        self.emit(Opcode::OpenEphemeral, eph_cursor, 1, 0, P4::Unused);

        // Labels for the collection loop
        let collect_start = self.alloc_label();
        let collect_end = self.alloc_label();
        let skip_collect = self.alloc_label();

        // Rewind to start of table
        self.emit(Opcode::Rewind, read_cursor, collect_end, 0, P4::Unused);

        // Collection loop start
        self.resolve_label(collect_start, self.current_addr() as i32);

        let rowid_reg = self.alloc_reg();
        self.emit(Opcode::Rowid, read_cursor, rowid_reg, 0, P4::Unused);

        // If WHERE clause, evaluate and skip if false
        if let Some(ref where_expr) = update.where_clause {
            let cond_reg = self.alloc_reg();
            self.compile_update_where(where_expr, read_cursor, cond_reg, table_name)?;
            self.emit(Opcode::IfNot, cond_reg, skip_collect, 1, P4::Unused);
        }

        // Store rowid in ephemeral table using Insert (not IdxInsert)
        // IdxInsert only adds to ephemeral_set, but Rewind/Next read from ephemeral_rows
        let record_reg = self.alloc_reg();
        let eph_rowid_reg = self.alloc_reg();
        self.emit(Opcode::MakeRecord, rowid_reg, 1, record_reg, P4::Unused);
        // Use rowid as the ephemeral table rowid too
        self.emit(Opcode::SCopy, rowid_reg, eph_rowid_reg, 0, P4::Unused);
        self.emit(
            Opcode::Insert,
            eph_cursor,
            record_reg,
            eph_rowid_reg,
            P4::Unused,
        );

        self.resolve_label(skip_collect, self.current_addr() as i32);

        // Move to next row
        self.emit(Opcode::Next, read_cursor, collect_start, 0, P4::Unused);

        // Collection loop end
        self.resolve_label(collect_end, self.current_addr() as i32);

        // Close read cursor
        self.emit(Opcode::Close, read_cursor, 0, 0, P4::Unused);

        // Phase 2: Update collected rows

        // Open table for writing
        let write_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenWrite,
            write_cursor,
            0,
            0,
            P4::Text(table_name.clone()),
        );

        // Labels for the update loop
        let update_start = self.alloc_label();
        let update_end = self.alloc_label();

        // Rewind ephemeral table
        self.emit(Opcode::Rewind, eph_cursor, update_end, 0, P4::Unused);

        // Update loop start
        self.resolve_label(update_start, self.current_addr() as i32);

        // Get rowid from ephemeral table
        let eph_rowid_reg = self.alloc_reg();
        self.emit(Opcode::Column, eph_cursor, 0, eph_rowid_reg, P4::Unused);

        // Seek to the row in the main table
        self.emit(
            Opcode::SeekRowid,
            write_cursor,
            update_end,
            eph_rowid_reg,
            P4::Unused,
        );

        // Allocate registers for the row data
        let base_reg = self.alloc_regs(num_cols as i32);

        // Load current row values into registers
        for i in 0..num_cols {
            self.emit(
                Opcode::Column,
                write_cursor,
                i as i32,
                base_reg + i as i32,
                P4::Unused,
            );
        }

        // Apply SET clause assignments
        for assignment in &update.assignments {
            for col_name in &assignment.columns {
                let col_lower = col_name.to_lowercase();

                let col_idx = if let Some(schema) = self.schema {
                    let table_lower = table_name.to_lowercase();
                    if let Some(table) = schema.tables.get(&table_lower) {
                        table
                            .columns
                            .iter()
                            .position(|c| c.name.to_lowercase() == col_lower)
                    } else {
                        None
                    }
                } else {
                    self.column_map.get(&col_lower).copied()
                };

                if let Some(idx) = col_idx {
                    let dest_reg = base_reg + idx as i32;
                    self.compile_update_expr(&assignment.expr, write_cursor, dest_reg, table_name)?;
                }
            }
        }

        // Make the updated record
        let new_record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            base_reg,
            num_cols as i32,
            new_record_reg,
            P4::Unused,
        );

        // Delete old row and insert new one with same rowid
        self.emit(Opcode::Delete, write_cursor, 0, 0, P4::Unused);
        self.emit(
            Opcode::Insert,
            write_cursor,
            new_record_reg,
            eph_rowid_reg,
            P4::Unused,
        );

        // Move to next ephemeral row
        self.emit(Opcode::Next, eph_cursor, update_start, 0, P4::Unused);

        // Update loop end
        self.resolve_label(update_end, self.current_addr() as i32);

        // Close cursors
        self.emit(Opcode::Close, write_cursor, 0, 0, P4::Unused);
        self.emit(Opcode::Close, eph_cursor, 0, 0, P4::Unused);

        Ok(())
    }

    /// Compile an expression for UPDATE SET clause, handling column references
    fn compile_update_expr(
        &mut self,
        expr: &crate::parser::ast::Expr,
        cursor: i32,
        dest_reg: i32,
        table_name: &str,
    ) -> Result<()> {
        use crate::parser::ast::{BinaryOp, Expr, Literal};

        match expr {
            Expr::Binary { op, left, right } => {
                let left_reg = self.alloc_reg();
                let right_reg = self.alloc_reg();

                self.compile_update_expr(left, cursor, left_reg, table_name)?;
                self.compile_update_expr(right, cursor, right_reg, table_name)?;

                match op {
                    BinaryOp::Add => {
                        self.emit(Opcode::Add, right_reg, left_reg, dest_reg, P4::Unused);
                    }
                    BinaryOp::Sub => {
                        self.emit(Opcode::Subtract, right_reg, left_reg, dest_reg, P4::Unused);
                    }
                    BinaryOp::Mul => {
                        self.emit(Opcode::Multiply, right_reg, left_reg, dest_reg, P4::Unused);
                    }
                    BinaryOp::Div => {
                        self.emit(Opcode::Divide, right_reg, left_reg, dest_reg, P4::Unused);
                    }
                    BinaryOp::Concat => {
                        self.emit(Opcode::Concat, left_reg, right_reg, dest_reg, P4::Unused);
                    }
                    _ => {
                        // For other ops, just use the left value
                        self.emit(Opcode::SCopy, left_reg, dest_reg, 0, P4::Unused);
                    }
                }
            }

            Expr::Column(col_ref) => {
                // Handle OLD/NEW references
                if let Some(ref table) = col_ref.table {
                    let table_upper = table.to_uppercase();
                    if table_upper == "OLD" || table_upper == "NEW" {
                        let p1 = if table_upper == "OLD" { 0 } else { 1 };
                        let col_lower = col_ref.column.to_lowercase();
                        let col_idx = self
                            .column_map
                            .get(&col_lower)
                            .map(|&idx| idx as i32)
                            .unwrap_or(-1);
                        self.emit(Opcode::Param, p1, col_idx, dest_reg, P4::Unused);
                        return Ok(());
                    }
                }

                // Regular column reference - read from cursor
                let col_lower = col_ref.column.to_lowercase();
                let col_idx = if let Some(schema) = self.schema {
                    let table_lower = table_name.to_lowercase();
                    if let Some(table) = schema.tables.get(&table_lower) {
                        table
                            .columns
                            .iter()
                            .position(|c| c.name.to_lowercase() == col_lower)
                            .map(|i| i as i32)
                            .unwrap_or(0)
                    } else {
                        0
                    }
                } else {
                    self.column_map
                        .get(&col_lower)
                        .map(|&i| i as i32)
                        .unwrap_or(0)
                };

                self.emit(Opcode::Column, cursor, col_idx, dest_reg, P4::Unused);
            }

            Expr::Literal(lit) => match lit {
                Literal::Null => {
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                }
                Literal::Integer(n) => {
                    self.emit(Opcode::Integer, *n as i32, dest_reg, 0, P4::Unused);
                }
                Literal::Float(f) => {
                    self.emit(Opcode::Real, 0, dest_reg, 0, P4::Real(*f));
                }
                Literal::String(s) => {
                    self.emit(Opcode::String8, 0, dest_reg, 0, P4::Text(s.clone()));
                }
                Literal::Blob(b) => {
                    self.emit(
                        Opcode::Blob,
                        b.len() as i32,
                        dest_reg,
                        0,
                        P4::Blob(b.clone()),
                    );
                }
                _ => {
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                }
            },

            Expr::Parens(inner) => {
                self.compile_update_expr(inner, cursor, dest_reg, table_name)?;
            }

            _ => {
                // For other expressions, use the generic expr compiler
                self.compile_expr(expr, dest_reg)?;
            }
        }

        Ok(())
    }

    /// Compile a WHERE clause for UPDATE, handling OLD/NEW references
    fn compile_update_where(
        &mut self,
        expr: &crate::parser::ast::Expr,
        cursor: i32,
        dest_reg: i32,
        table_name: &str,
    ) -> Result<()> {
        // Reuse the delete WHERE compiler - same logic applies
        self.compile_delete_where(expr, cursor, dest_reg, table_name)
    }

    /// Compile a DELETE statement in a trigger body
    fn compile_delete(&mut self, delete: &crate::parser::ast::DeleteStmt) -> Result<()> {
        // Open table for writing
        let cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenWrite,
            cursor,
            0,
            0,
            P4::Text(delete.table.name.clone()),
        );

        // Labels for the scan loop
        let loop_start = self.alloc_label();
        let loop_end = self.alloc_label();
        let skip_delete = self.alloc_label();

        // Rewind to start of table
        self.emit(Opcode::Rewind, cursor, loop_end, 0, P4::Unused);

        // Loop start
        self.resolve_label(loop_start, self.current_addr() as i32);

        // If WHERE clause, evaluate and skip if false
        if let Some(ref where_expr) = delete.where_clause {
            let cond_reg = self.alloc_reg();
            self.compile_delete_where(where_expr, cursor, cond_reg, &delete.table.name)?;
            // If condition is false (0), skip the delete
            self.emit(Opcode::IfNot, cond_reg, skip_delete, 1, P4::Unused);
        }

        // Delete the current row
        self.emit(Opcode::Delete, cursor, 0, 0, P4::Unused);

        // Skip delete label
        self.resolve_label(skip_delete, self.current_addr() as i32);

        // Move to next row
        self.emit(Opcode::Next, cursor, loop_start, 0, P4::Unused);

        // Loop end
        self.resolve_label(loop_end, self.current_addr() as i32);

        // Close cursor
        self.emit(Opcode::Close, cursor, 0, 0, P4::Unused);

        Ok(())
    }

    /// Compile a WHERE clause for DELETE, handling OLD/NEW references
    fn compile_delete_where(
        &mut self,
        expr: &crate::parser::ast::Expr,
        cursor: i32,
        dest_reg: i32,
        table_name: &str,
    ) -> Result<()> {
        use crate::parser::ast::{BinaryOp, Expr, Literal};

        match expr {
            Expr::Binary { op, left, right } => {
                let left_reg = self.alloc_reg();
                let right_reg = self.alloc_reg();

                self.compile_delete_where(left, cursor, left_reg, table_name)?;
                self.compile_delete_where(right, cursor, right_reg, table_name)?;

                match op {
                    BinaryOp::Eq => {
                        let set_true = self.alloc_label();
                        let end_label = self.alloc_label();
                        self.emit(Opcode::Integer, 0, dest_reg, 0, P4::Unused);
                        self.emit(Opcode::Eq, right_reg, set_true, left_reg, P4::Unused);
                        self.emit(Opcode::Goto, 0, end_label, 0, P4::Unused);
                        self.resolve_label(set_true, self.current_addr() as i32);
                        self.emit(Opcode::Integer, 1, dest_reg, 0, P4::Unused);
                        self.resolve_label(end_label, self.current_addr() as i32);
                    }
                    BinaryOp::Add => {
                        self.emit(Opcode::Add, right_reg, left_reg, dest_reg, P4::Unused);
                    }
                    BinaryOp::Sub => {
                        self.emit(Opcode::Subtract, right_reg, left_reg, dest_reg, P4::Unused);
                    }
                    _ => {
                        self.emit(Opcode::SCopy, left_reg, dest_reg, 0, P4::Unused);
                    }
                }
            }

            Expr::Column(col_ref) => {
                if let Some(ref table) = col_ref.table {
                    let table_upper = table.to_uppercase();
                    if table_upper == "OLD" || table_upper == "NEW" {
                        let p1 = if table_upper == "OLD" { 0 } else { 1 };
                        let col_lower = col_ref.column.to_lowercase();
                        let col_idx = self
                            .column_map
                            .get(&col_lower)
                            .map(|&idx| idx as i32)
                            .unwrap_or(-1);
                        self.emit(Opcode::Param, p1, col_idx, dest_reg, P4::Unused);
                        return Ok(());
                    }
                }

                let col_lower = col_ref.column.to_lowercase();
                let col_idx = if let Some(schema) = self.schema {
                    let table_lower = table_name.to_lowercase();
                    if let Some(table) = schema.tables.get(&table_lower) {
                        table
                            .columns
                            .iter()
                            .position(|c| c.name.to_lowercase() == col_lower)
                            .map(|i| i as i32)
                            .unwrap_or(0)
                    } else {
                        0
                    }
                } else {
                    self.column_map
                        .get(&col_lower)
                        .map(|&i| i as i32)
                        .unwrap_or(0)
                };

                self.emit(Opcode::Column, cursor, col_idx, dest_reg, P4::Unused);
            }

            Expr::Literal(lit) => match lit {
                Literal::Integer(n) => {
                    self.emit(Opcode::Integer, *n as i32, dest_reg, 0, P4::Unused);
                }
                _ => {
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                }
            },

            Expr::Parens(inner) => {
                self.compile_delete_where(inner, cursor, dest_reg, table_name)?;
            }

            _ => {
                self.compile_expr(expr, dest_reg)?;
            }
        }

        Ok(())
    }

    /// Compile a SELECT statement in a trigger body
    fn compile_select(&mut self, select: &crate::parser::ast::SelectStmt) -> Result<()> {
        use crate::parser::ast::{ResultColumn, SelectBody};

        // SELECT in trigger body is executed for side effects (like RAISE)
        // We need to evaluate each expression in the SELECT list
        if let SelectBody::Select(core) = &select.body {
            for col in &core.columns {
                if let ResultColumn::Expr { expr, .. } = col {
                    let dest_reg = self.alloc_reg();
                    self.compile_expr(expr, dest_reg)?;
                }
            }
        }
        Ok(())
    }

    /// Compile an expression, handling OLD/NEW references
    fn compile_expr(&mut self, expr: &crate::parser::ast::Expr, dest_reg: i32) -> Result<()> {
        use crate::parser::ast::{ColumnRef, Expr, Literal};

        match expr {
            Expr::Literal(lit) => {
                match lit {
                    Literal::Null => {
                        self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                    }
                    Literal::Integer(n) => {
                        self.emit(Opcode::Integer, *n as i32, dest_reg, 0, P4::Unused);
                    }
                    Literal::Float(f) => {
                        self.emit(Opcode::Real, 0, dest_reg, 0, P4::Real(*f));
                    }
                    Literal::String(s) => {
                        self.emit(Opcode::String8, 0, dest_reg, 0, P4::Text(s.clone()));
                    }
                    Literal::Blob(b) => {
                        self.emit(
                            Opcode::Blob,
                            b.len() as i32,
                            dest_reg,
                            0,
                            P4::Blob(b.clone()),
                        );
                    }
                    Literal::Bool(b) => {
                        self.emit(
                            Opcode::Integer,
                            if *b { 1 } else { 0 },
                            dest_reg,
                            0,
                            P4::Unused,
                        );
                    }
                    Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp => {
                        // Use NULL for now - time functions need special handling
                        self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                    }
                }
            }

            Expr::Column(col_ref) => {
                // Check if this is an OLD or NEW reference
                self.compile_column_ref(col_ref, dest_reg)?;
            }

            Expr::Binary { op, left, right } => {
                // Compile binary operation
                let left_reg = self.alloc_reg();
                let right_reg = self.alloc_reg();

                self.compile_expr(left, left_reg)?;
                self.compile_expr(right, right_reg)?;

                // Emit appropriate opcode based on operator
                use crate::parser::ast::BinaryOp;
                match op {
                    BinaryOp::Add => {
                        self.emit(Opcode::Add, right_reg, left_reg, dest_reg, P4::Unused)
                    }
                    BinaryOp::Sub => {
                        self.emit(Opcode::Subtract, right_reg, left_reg, dest_reg, P4::Unused)
                    }
                    BinaryOp::Mul => {
                        self.emit(Opcode::Multiply, right_reg, left_reg, dest_reg, P4::Unused)
                    }
                    BinaryOp::Div => {
                        self.emit(Opcode::Divide, right_reg, left_reg, dest_reg, P4::Unused)
                    }
                    BinaryOp::Concat => {
                        self.emit(Opcode::Concat, left_reg, right_reg, dest_reg, P4::Unused)
                    }
                    _ => {
                        // For other operators, just use left value for now
                        self.emit(Opcode::SCopy, left_reg, dest_reg, 0, P4::Unused);
                    }
                }
            }

            Expr::Parens(inner) => {
                // Parenthesized expression - just compile the inner expression
                self.compile_expr(inner, dest_reg)?;
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

            Expr::Raise { action, message } => {
                // RAISE function in triggers
                use crate::parser::ast::RaiseAction;

                // P1 = error code, P2 = on-conflict action
                // SQLITE_CONSTRAINT = 19
                let (p1, p2) = match action {
                    RaiseAction::Ignore => (0, 4),    // OE_IGNORE = 4
                    RaiseAction::Rollback => (19, 1), // SQLITE_CONSTRAINT, OE_ROLLBACK = 1
                    RaiseAction::Abort => (19, 2),    // SQLITE_CONSTRAINT, OE_ABORT = 2
                    RaiseAction::Fail => (19, 3),     // SQLITE_CONSTRAINT, OE_FAIL = 3
                };

                let p4 = match message {
                    Some(msg) => P4::Text(msg.clone()),
                    None => P4::Unused,
                };

                self.emit(Opcode::Halt, p1, p2, 0, p4);
            }

            _ => {
                // For complex expressions, use NULL for now
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
            }
        }

        Ok(())
    }

    /// Compile a column reference, handling OLD/NEW pseudo-tables
    fn compile_column_ref(
        &mut self,
        col_ref: &crate::parser::ast::ColumnRef,
        dest_reg: i32,
    ) -> Result<()> {
        // Check if table qualifier is OLD or NEW
        if let Some(ref table) = col_ref.table {
            let table_upper = table.to_uppercase();

            if table_upper == "OLD" || table_upper == "NEW" {
                // This is an OLD/NEW reference - use Param opcode
                let p1 = if table_upper == "OLD" { 0 } else { 1 };

                // Find column index
                let col_lower = col_ref.column.to_lowercase();
                let col_idx = self
                    .column_map
                    .get(&col_lower)
                    .map(|&idx| idx as i32)
                    .unwrap_or(-1); // -1 for rowid

                // Special case: check for rowid aliases
                if col_ref.column.eq_ignore_ascii_case("rowid")
                    || col_ref.column.eq_ignore_ascii_case("_rowid_")
                    || col_ref.column.eq_ignore_ascii_case("oid")
                {
                    self.emit(Opcode::Param, p1, -1, dest_reg, P4::Unused);
                } else {
                    self.emit(Opcode::Param, p1, col_idx, dest_reg, P4::Unused);
                }

                return Ok(());
            }
        }

        // Not an OLD/NEW reference - use NULL for now
        // A full implementation would resolve the column from the current context
        self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
        Ok(())
    }
}

/// Generate code to fire triggers for an operation
///
/// This compiles matching triggers and generates Program opcodes to execute them.
/// The caller is responsible for setting up OLD/NEW row values in the VDBE before
/// executing the generated code.
///
/// Parameters:
/// - triggers: List of triggers to fire
/// - schema: Schema for name resolution in trigger bodies
/// - table_name: Name of the table (for column resolution)
/// - old_base_reg: Base register containing OLD row values (or None for INSERT)
/// - new_base_reg: Base register containing NEW row values (or None for DELETE)
/// - num_columns: Number of columns in the table
/// - next_reg: Pointer to next register counter (updated)
/// - next_cursor: Pointer to next cursor counter (updated)
/// - return_label: Label to jump to after trigger execution
pub fn generate_trigger_code(
    triggers: &[Arc<Trigger>],
    schema: Option<&Schema>,
    table_name: &str,
    old_base_reg: Option<i32>,
    new_base_reg: Option<i32>,
    num_columns: i32,
    next_reg: &mut i32,
    next_cursor: &mut i32,
    return_label: i32,
) -> Result<Vec<VdbeOp>> {
    let mut ops = Vec::new();

    if triggers.is_empty() {
        return Ok(ops);
    }

    for trigger in triggers {
        // Parse the trigger SQL to get the body statements
        let body_stmts = if let Some(sql) = &trigger.sql {
            // Parse the SQL to get the trigger AST
            match crate::parser::grammar::parse(sql) {
                Ok(crate::parser::ast::Stmt::CreateTrigger(create)) => create.body,
                _ => continue, // Skip if can't parse
            }
        } else {
            continue; // No SQL stored
        };

        if body_stmts.is_empty() {
            continue;
        }

        // Compile the trigger body to a SubProgram
        // Pass the current cursor counter to avoid conflicts with parent's cursors
        let mut compiler = TriggerBodyCompiler::new(schema, table_name, *next_cursor);
        let subprogram = compiler.compile_body(&body_stmts)?;

        // Update the cursor counter to account for cursors used by the trigger
        *next_cursor = compiler.next_cursor;

        // Before calling the trigger, we need to set up OLD/NEW row values
        // This is done by emitting Copy opcodes to move values from the base registers
        // to the trigger's expected locations

        // For OLD values (DELETE/UPDATE triggers)
        if let Some(old_reg) = old_base_reg {
            // Emit opcodes to copy OLD row to trigger_old_row in VDBE
            // The SetTriggerRow opcode stores the row values
            let copy_reg = *next_reg;
            *next_reg += num_columns;

            for i in 0..num_columns {
                ops.push(make_op(
                    Opcode::SCopy,
                    old_reg + i,
                    copy_reg + i,
                    0,
                    P4::Unused,
                ));
            }

            // SetTriggerRow stores the row for Param opcode to read
            ops.push(make_op(
                Opcode::SetTriggerRow,
                0, // 0 = OLD row
                copy_reg,
                num_columns,
                P4::Unused,
            ));
        }

        // For NEW values (INSERT/UPDATE triggers)
        if let Some(new_reg) = new_base_reg {
            let copy_reg = *next_reg;
            *next_reg += num_columns;

            for i in 0..num_columns {
                ops.push(make_op(
                    Opcode::SCopy,
                    new_reg + i,
                    copy_reg + i,
                    0,
                    P4::Unused,
                ));
            }

            ops.push(make_op(
                Opcode::SetTriggerRow,
                1, // 1 = NEW row
                copy_reg,
                num_columns,
                P4::Unused,
            ));
        }

        // Emit Program opcode to execute the trigger
        ops.push(make_op(
            Opcode::Program,
            0,
            return_label,
            0,
            P4::Subprogram(Arc::new(subprogram)),
        ));
    }

    Ok(ops)
}

/// Context for trigger execution
#[derive(Debug, Clone)]
pub struct TriggerContext {
    /// OLD row values (for DELETE/UPDATE triggers)
    pub old_values: Option<Vec<crate::types::Value>>,
    /// NEW row values (for INSERT/UPDATE triggers)
    pub new_values: Option<Vec<crate::types::Value>>,
    /// Rowid of affected row
    pub rowid: i64,
    /// Trigger recursion depth (for detecting infinite loops)
    pub depth: u32,
    /// Maximum recursion depth (default 1000, like SQLite)
    pub max_depth: u32,
}

impl TriggerContext {
    pub fn new() -> Self {
        Self {
            old_values: None,
            new_values: None,
            rowid: 0,
            depth: 0,
            max_depth: 1000,
        }
    }

    /// Check if we've exceeded the recursion limit
    pub fn check_recursion(&self) -> Result<()> {
        if self.depth >= self.max_depth {
            return Err(Error::with_message(
                ErrorCode::Constraint,
                "too many levels of trigger recursion",
            ));
        }
        Ok(())
    }

    /// Enter a trigger (increment depth)
    pub fn enter(&mut self) -> Result<()> {
        self.check_recursion()?;
        self.depth += 1;
        Ok(())
    }

    /// Exit a trigger (decrement depth)
    pub fn exit(&mut self) {
        if self.depth > 0 {
            self.depth -= 1;
        }
    }
}

impl Default for TriggerContext {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn make_op(opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: P4) -> VdbeOp {
    VdbeOp {
        opcode,
        p1,
        p2,
        p3,
        p4,
        p5: 0,
        comment: None,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trigger_mask() {
        let before_insert = TriggerMask::new(TriggerTiming::Before, TriggerEvent::Insert);
        let after_insert = TriggerMask::new(TriggerTiming::After, TriggerEvent::Insert);
        let before_delete = TriggerMask::new(TriggerTiming::Before, TriggerEvent::Delete);

        // Same timing and event should match
        let same = TriggerMask::new(TriggerTiming::Before, TriggerEvent::Insert);
        assert!(before_insert.matches(&same));

        // Different timing should not match
        assert!(!before_insert.matches(&after_insert));

        // Different event should not match
        assert!(!before_insert.matches(&before_delete));
    }

    #[test]
    fn test_trigger_context() {
        let ctx = TriggerContext::new();
        assert!(ctx.old_values.is_none());
        assert!(ctx.new_values.is_none());
        assert_eq!(ctx.rowid, 0);
        assert_eq!(ctx.depth, 0);
        assert_eq!(ctx.max_depth, 1000);
    }

    #[test]
    fn test_trigger_context_recursion() {
        let mut ctx = TriggerContext::new();
        ctx.max_depth = 3;

        // Should succeed up to max_depth
        assert!(ctx.enter().is_ok());
        assert_eq!(ctx.depth, 1);
        assert!(ctx.enter().is_ok());
        assert_eq!(ctx.depth, 2);
        assert!(ctx.enter().is_ok());
        assert_eq!(ctx.depth, 3);

        // Should fail at max_depth
        assert!(ctx.enter().is_err());
        assert_eq!(ctx.depth, 3); // depth unchanged on error

        // Exit should work
        ctx.exit();
        assert_eq!(ctx.depth, 2);
    }

    #[test]
    fn test_find_matching_triggers() {
        // This requires a schema setup, so just test the empty case
        let schema = Schema::default();
        let triggers = find_matching_triggers(
            &schema,
            "test",
            TriggerTiming::Before,
            TriggerEvent::Insert,
            None,
        );
        assert!(triggers.is_empty());
    }
}
