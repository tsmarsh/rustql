//! VDBE Core Execution Engine
//!
//! The Virtual Database Engine (VDBE) is the bytecode interpreter that
//! executes all SQL statements. This module implements the main execution
//! loop and manages the virtual machine state.

use std::cmp::Ordering;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::error::{Error, ErrorCode, Result};
use crate::schema::Schema;
use crate::storage::btree::{BtCursor, Btree, BtreeCursorFlags, BtreeInsertFlags, BtreePayload};
use crate::types::{ColumnType, Pgno, Value};
use crate::vdbe::mem::Mem;
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

// ============================================================================
// Constants
// ============================================================================

/// Magic number for valid VDBE state
const VDBE_MAGIC_INIT: u32 = 0x26bceaa5;
const VDBE_MAGIC_RUN: u32 = 0xbdf20da3;
const VDBE_MAGIC_HALT: u32 = 0x519c2973;
const VDBE_MAGIC_DEAD: u32 = 0xb606c3c8;

/// Default number of memory cells
const DEFAULT_MEM_SIZE: usize = 128;

/// Default number of cursor slots
const DEFAULT_CURSOR_SLOTS: usize = 16;

// ============================================================================
// Execution Result
// ============================================================================

/// Result of a single execution step
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecResult {
    /// SQLITE_ROW - a result row is available
    Row,
    /// SQLITE_DONE - execution completed successfully
    Done,
    /// Execution should continue
    Continue,
}

/// EXPLAIN mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExplainMode {
    /// Normal execution
    #[default]
    None,
    /// EXPLAIN - show opcodes
    Explain,
    /// EXPLAIN QUERY PLAN
    QueryPlan,
}

// ============================================================================
// VDBE Cursor
// ============================================================================

/// Cursor state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorState {
    /// Cursor is not positioned
    Invalid,
    /// Cursor is at a valid row
    Valid,
    /// At end of table/index
    AtEnd,
    /// Cursor requires seek
    RequireSeek,
}

/// A cursor for iterating over table/index rows
pub struct VdbeCursor {
    /// Cursor ID
    pub id: i32,
    /// Root page number
    pub root_page: Pgno,
    /// Is this a writable cursor?
    pub writable: bool,
    /// Is this an index cursor?
    pub is_index: bool,
    /// Is this an ephemeral table?
    pub is_ephemeral: bool,
    /// Current state
    pub state: CursorState,
    /// Current key (for index cursors)
    pub key: Option<Vec<u8>>,
    /// Current rowid
    pub rowid: Option<i64>,
    /// Cached row data
    pub row_data: Option<Vec<u8>>,
    /// Number of columns
    pub n_field: i32,
    /// Null row flag (for outer joins)
    pub null_row: bool,
    /// Deferred seek key
    pub seek_key: Option<Vec<Mem>>,
    /// B-tree cursor for actual storage operations
    pub btree_cursor: Option<BtCursor>,
    /// Table name (for looking up column indices at runtime)
    pub table_name: Option<String>,
    /// Is this a sqlite_master virtual cursor?
    pub is_sqlite_master: bool,
    /// Current index for virtual cursors (sqlite_master iteration)
    pub virtual_index: usize,
    /// Cached schema entries for sqlite_master (type, name, tbl_name, rootpage, sql)
    pub schema_entries: Option<Vec<(String, String, String, u32, Option<String>)>>,
    /// Sorter data - rows to be sorted (each row is a serialized record)
    pub sorter_data: Vec<Vec<u8>>,
    /// Sorter index - current position in sorted data
    pub sorter_index: usize,
    /// Has the sorter been sorted?
    pub sorter_sorted: bool,
}

impl std::fmt::Debug for VdbeCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VdbeCursor")
            .field("id", &self.id)
            .field("root_page", &self.root_page)
            .field("writable", &self.writable)
            .field("state", &self.state)
            .field("n_field", &self.n_field)
            .field("has_btree_cursor", &self.btree_cursor.is_some())
            .finish()
    }
}

impl VdbeCursor {
    /// Create a new cursor
    pub fn new(id: i32, root_page: Pgno, writable: bool) -> Self {
        Self {
            id,
            root_page,
            writable,
            is_index: false,
            is_ephemeral: false,
            state: CursorState::Invalid,
            key: None,
            rowid: None,
            row_data: None,
            n_field: 0,
            null_row: false,
            seek_key: None,
            btree_cursor: None,
            table_name: None,
            is_sqlite_master: false,
            virtual_index: 0,
            schema_entries: None,
            sorter_data: Vec::new(),
            sorter_index: 0,
            sorter_sorted: false,
        }
    }

    /// Check if cursor is valid
    pub fn is_valid(&self) -> bool {
        self.state == CursorState::Valid
    }

    /// Set null row mode
    pub fn set_null_row(&mut self, null_row: bool) {
        self.null_row = null_row;
        if null_row {
            self.row_data = None;
        }
    }
}

// ============================================================================
// VDBE Frame (for subroutines)
// ============================================================================

/// Stack frame for subroutine calls
#[derive(Debug)]
pub struct VdbeFrame {
    /// Return address (PC to return to)
    pub return_pc: i32,
    /// Base memory register for this frame
    pub mem_base: i32,
    /// Number of memory cells in this frame
    pub n_mem: i32,
    /// Base cursor for this frame
    pub cursor_base: i32,
    /// Number of cursors in this frame
    pub n_cursor: i32,
}

// ============================================================================
// VDBE Virtual Machine
// ============================================================================

/// The VDBE virtual machine
pub struct Vdbe {
    /// Magic number for state validation
    magic: u32,

    /// Program counter
    pc: i32,

    /// Most recent result code
    rc: ErrorCode,

    /// Array of opcodes (the program)
    ops: Vec<VdbeOp>,

    /// Memory cells (registers)
    mem: Vec<Mem>,

    /// Cursors
    cursors: Vec<Option<VdbeCursor>>,

    /// Stack frames for subroutines
    frames: Vec<VdbeFrame>,

    /// EXPLAIN mode
    explain_mode: ExplainMode,

    /// Is execution complete?
    is_done: bool,

    /// Has a result row?
    has_result: bool,

    /// Error message
    error_msg: Option<String>,

    /// Number of rows modified
    n_change: i64,

    /// Start time (for timeout)
    start_time: Option<Instant>,

    /// Bound parameters
    vars: Vec<Mem>,

    /// Parameter names
    var_names: Vec<Option<String>>,

    /// Interrupt flag
    interrupted: bool,

    /// Instruction counter (for infinite loop detection)
    instruction_count: u64,

    /// Maximum instructions before aborting (0 = unlimited)
    max_instructions: u64,

    /// Result row start register
    result_start: i32,

    /// Result row count
    result_count: i32,

    /// Column names for result
    column_names: Vec<String>,

    /// B-tree for main database (for storage operations)
    btree: Option<Arc<Btree>>,

    /// Schema for main database (for DDL operations)
    schema: Option<Arc<RwLock<Schema>>>,

    /// Deferred foreign key violation counter
    deferred_fk_counter: i64,

    /// Foreign key enforcement enabled
    fk_enabled: bool,

    // ========================================================================
    // Trigger Context
    // ========================================================================
    /// OLD row values for DELETE/UPDATE triggers
    trigger_old_row: Option<Vec<Mem>>,

    /// NEW row values for INSERT/UPDATE triggers
    trigger_new_row: Option<Vec<Mem>>,

    /// Trigger recursion depth
    trigger_depth: u32,

    /// Maximum trigger recursion depth (default 1000)
    max_trigger_depth: u32,

    /// Saved program state for subprogram execution
    /// (parent ops, parent pc) - allows returning from trigger
    subprogram_stack: Vec<(Vec<VdbeOp>, i32, i32)>, // (ops, pc, mem_base)
}

impl Default for Vdbe {
    fn default() -> Self {
        Self::new()
    }
}

impl Vdbe {
    /// Create a new empty VDBE
    pub fn new() -> Self {
        Self {
            magic: VDBE_MAGIC_INIT,
            pc: 0,
            rc: ErrorCode::Ok,
            ops: Vec::new(),
            mem: vec![Mem::new(); DEFAULT_MEM_SIZE],
            cursors: (0..DEFAULT_CURSOR_SLOTS).map(|_| None).collect(),
            frames: Vec::new(),
            explain_mode: ExplainMode::None,
            is_done: false,
            has_result: false,
            error_msg: None,
            n_change: 0,
            start_time: None,
            vars: Vec::new(),
            var_names: Vec::new(),
            interrupted: false,
            instruction_count: 0,
            max_instructions: 100_000, // Default 100K instruction limit
            result_start: 0,
            result_count: 0,
            column_names: Vec::new(),
            btree: None,
            schema: None,
            deferred_fk_counter: 0,
            fk_enabled: true,
            trigger_old_row: None,
            trigger_new_row: None,
            trigger_depth: 0,
            max_trigger_depth: 1000,
            subprogram_stack: Vec::new(),
        }
    }

    /// Set foreign key enforcement
    pub fn set_fk_enabled(&mut self, enabled: bool) {
        self.fk_enabled = enabled;
    }

    /// Get deferred FK violation count
    pub fn deferred_fk_count(&self) -> i64 {
        self.deferred_fk_counter
    }

    // ========================================================================
    // Trigger Context Methods
    // ========================================================================

    /// Set OLD row for DELETE/UPDATE triggers
    pub fn set_trigger_old_row(&mut self, row: Vec<Mem>) {
        self.trigger_old_row = Some(row);
    }

    /// Set NEW row for INSERT/UPDATE triggers
    pub fn set_trigger_new_row(&mut self, row: Vec<Mem>) {
        self.trigger_new_row = Some(row);
    }

    /// Clear trigger context
    pub fn clear_trigger_context(&mut self) {
        self.trigger_old_row = None;
        self.trigger_new_row = None;
    }

    /// Get trigger recursion depth
    pub fn trigger_depth(&self) -> u32 {
        self.trigger_depth
    }

    /// Check if we're inside a trigger
    pub fn in_trigger(&self) -> bool {
        self.trigger_depth > 0
    }

    /// Set the database btree for storage operations
    pub fn set_btree(&mut self, btree: Arc<Btree>) {
        self.btree = Some(btree);
    }

    /// Set the schema for DDL operations
    pub fn set_schema(&mut self, schema: Arc<RwLock<Schema>>) {
        self.schema = Some(schema);
    }

    /// Create from a list of operations
    pub fn from_ops(ops: Vec<VdbeOp>) -> Self {
        let mut vdbe = Self::new();
        vdbe.ops = ops;
        vdbe
    }

    // ========================================================================
    // Program Management
    // ========================================================================

    /// Add an instruction to the program
    pub fn add_op(&mut self, op: VdbeOp) -> i32 {
        let addr = self.ops.len() as i32;
        self.ops.push(op);
        addr
    }

    /// Get instruction at address
    pub fn op_at(&self, addr: i32) -> Option<&VdbeOp> {
        self.ops.get(addr as usize)
    }

    /// Get mutable instruction at address
    pub fn op_at_mut(&mut self, addr: i32) -> Option<&mut VdbeOp> {
        self.ops.get_mut(addr as usize)
    }

    /// Get current program counter
    pub fn get_pc(&self) -> i32 {
        self.pc
    }

    /// Get number of operations
    pub fn op_count(&self) -> i32 {
        self.ops.len() as i32
    }

    /// Set column names for result
    pub fn set_column_names(&mut self, names: Vec<String>) {
        self.column_names = names;
    }

    // ========================================================================
    // Memory Management
    // ========================================================================

    /// Ensure we have enough memory cells
    pub fn ensure_mem(&mut self, n: i32) {
        let n = n as usize;
        if self.mem.len() < n {
            self.mem.resize_with(n, Mem::new);
        }
    }

    /// Get memory cell
    pub fn mem(&self, reg: i32) -> &Mem {
        &self.mem[reg as usize]
    }

    /// Get mutable memory cell
    pub fn mem_mut(&mut self, reg: i32) -> &mut Mem {
        &mut self.mem[reg as usize]
    }

    /// Set memory cell value
    pub fn set_mem(&mut self, reg: i32, value: Mem) {
        self.mem[reg as usize] = value;
    }

    // ========================================================================
    // Parameter Binding
    // ========================================================================

    /// Ensure we have enough parameter slots
    pub fn ensure_vars(&mut self, n: i32) {
        let n = n as usize;
        if self.vars.len() < n {
            self.vars.resize_with(n, Mem::new);
            self.var_names.resize(n, None);
        }
    }

    /// Bind NULL to parameter (1-indexed)
    pub fn bind_null(&mut self, idx: i32) -> Result<()> {
        self.check_param_index(idx)?;
        self.vars[(idx - 1) as usize].set_null();
        Ok(())
    }

    /// Bind integer to parameter (1-indexed)
    pub fn bind_int(&mut self, idx: i32, value: i64) -> Result<()> {
        self.check_param_index(idx)?;
        self.vars[(idx - 1) as usize].set_int(value);
        Ok(())
    }

    /// Bind real to parameter (1-indexed)
    pub fn bind_real(&mut self, idx: i32, value: f64) -> Result<()> {
        self.check_param_index(idx)?;
        self.vars[(idx - 1) as usize].set_real(value);
        Ok(())
    }

    /// Bind text to parameter (1-indexed)
    pub fn bind_text(&mut self, idx: i32, value: &str) -> Result<()> {
        self.check_param_index(idx)?;
        self.vars[(idx - 1) as usize].set_str(value);
        Ok(())
    }

    /// Bind blob to parameter (1-indexed)
    pub fn bind_blob(&mut self, idx: i32, value: &[u8]) -> Result<()> {
        self.check_param_index(idx)?;
        self.vars[(idx - 1) as usize].set_blob(value);
        Ok(())
    }

    /// Bind value to parameter (1-indexed)
    pub fn bind_value(&mut self, idx: i32, value: &Value) -> Result<()> {
        self.check_param_index(idx)?;
        self.vars[(idx - 1) as usize].set_value(value);
        Ok(())
    }

    /// Clear all bindings
    pub fn clear_bindings(&mut self) {
        for var in &mut self.vars {
            var.set_null();
        }
    }

    fn check_param_index(&self, idx: i32) -> Result<()> {
        if idx < 1 || idx > self.vars.len() as i32 {
            return Err(Error::with_message(
                ErrorCode::Range,
                format!("parameter index {} out of range", idx),
            ));
        }
        Ok(())
    }

    // ========================================================================
    // Cursor Management
    // ========================================================================

    /// Ensure we have enough cursor slots
    pub fn ensure_cursors(&mut self, n: i32) {
        let n = n as usize;
        if self.cursors.len() < n {
            self.cursors.resize_with(n, || None);
        }
    }

    /// Open a cursor
    pub fn open_cursor(&mut self, id: i32, root_page: Pgno, writable: bool) -> Result<()> {
        self.ensure_cursors(id + 1);
        self.cursors[id as usize] = Some(VdbeCursor::new(id, root_page, writable));
        Ok(())
    }

    /// Close a cursor
    pub fn close_cursor(&mut self, id: i32) -> Result<()> {
        if let Some(slot) = self.cursors.get_mut(id as usize) {
            *slot = None;
        }
        Ok(())
    }

    /// Get cursor
    pub fn cursor(&self, id: i32) -> Option<&VdbeCursor> {
        self.cursors.get(id as usize).and_then(|c| c.as_ref())
    }

    /// Get mutable cursor
    pub fn cursor_mut(&mut self, id: i32) -> Option<&mut VdbeCursor> {
        self.cursors.get_mut(id as usize).and_then(|c| c.as_mut())
    }

    // ========================================================================
    // Execution Control
    // ========================================================================

    /// Reset the VM for re-execution
    pub fn reset(&mut self) {
        self.pc = 0;
        self.rc = ErrorCode::Ok;
        self.is_done = false;
        self.has_result = false;
        self.error_msg = None;
        self.n_change = 0;
        self.start_time = None;
        self.interrupted = false;
        self.magic = VDBE_MAGIC_INIT;

        // Clear memory cells
        for mem in &mut self.mem {
            mem.set_null();
        }

        // Close all cursors
        for cursor in &mut self.cursors {
            *cursor = None;
        }

        // Clear frames
        self.frames.clear();
    }

    /// Interrupt execution
    pub fn interrupt(&mut self) {
        self.interrupted = true;
    }

    /// Check if interrupted
    pub fn is_interrupted(&self) -> bool {
        self.interrupted
    }

    /// Get result code
    pub fn result_code(&self) -> ErrorCode {
        self.rc
    }

    /// Get error message
    pub fn error_message(&self) -> Option<&str> {
        self.error_msg.as_deref()
    }

    /// Get number of changes
    pub fn changes(&self) -> i64 {
        self.n_change
    }

    /// Check if execution is done
    pub fn is_done(&self) -> bool {
        self.is_done
    }

    // ========================================================================
    // Result Row Access
    // ========================================================================

    /// Get number of columns in result
    pub fn column_count(&self) -> i32 {
        self.result_count
    }

    /// Get column name
    pub fn column_name(&self, idx: i32) -> &str {
        self.column_names
            .get(idx as usize)
            .map(|s| s.as_str())
            .unwrap_or("")
    }

    /// Get column type
    pub fn column_type(&self, idx: i32) -> ColumnType {
        let reg = self.result_start + idx;
        self.mem(reg).column_type()
    }

    /// Get column as integer
    pub fn column_int(&self, idx: i32) -> i64 {
        let reg = self.result_start + idx;
        self.mem(reg).to_int()
    }

    /// Get column as real
    pub fn column_real(&self, idx: i32) -> f64 {
        let reg = self.result_start + idx;
        self.mem(reg).to_real()
    }

    /// Get column as text
    pub fn column_text(&self, idx: i32) -> String {
        let reg = self.result_start + idx;
        self.mem(reg).to_str()
    }

    /// Get column as blob
    pub fn column_blob(&self, idx: i32) -> Vec<u8> {
        let reg = self.result_start + idx;
        self.mem(reg).to_blob()
    }

    /// Get column as Value
    pub fn column_value(&self, idx: i32) -> Value {
        let reg = self.result_start + idx;
        self.mem(reg).to_value()
    }

    // ========================================================================
    // Main Execution Loop
    // ========================================================================

    /// Execute one step of the program
    pub fn step(&mut self) -> Result<ExecResult> {
        if self.is_done {
            return Ok(ExecResult::Done);
        }

        if self.magic == VDBE_MAGIC_INIT {
            self.magic = VDBE_MAGIC_RUN;
            self.start_time = Some(Instant::now());
        }

        // Check for interrupt
        if self.interrupted {
            self.is_done = true;
            self.rc = ErrorCode::Interrupt;
            return Err(Error::new(ErrorCode::Interrupt));
        }

        // Execute instructions until we hit a stopping point
        loop {
            if self.pc < 0 || self.pc >= self.ops.len() as i32 {
                self.is_done = true;
                self.magic = VDBE_MAGIC_HALT;
                return Ok(ExecResult::Done);
            }

            // Check instruction limit to prevent infinite loops
            self.instruction_count += 1;
            if self.max_instructions > 0 && self.instruction_count > self.max_instructions {
                self.is_done = true;
                self.magic = VDBE_MAGIC_HALT;
                self.error_msg = Some(format!(
                    "Query aborted: exceeded {} instruction limit (possible infinite loop)",
                    self.max_instructions
                ));
                return Err(Error::new(ErrorCode::Abort));
            }

            let result = self.exec_op()?;

            match result {
                ExecResult::Row => {
                    self.has_result = true;
                    return Ok(ExecResult::Row);
                }
                ExecResult::Done => {
                    self.is_done = true;
                    self.magic = VDBE_MAGIC_HALT;
                    return Ok(ExecResult::Done);
                }
                ExecResult::Continue => {
                    // Continue to next instruction
                }
            }

            // Check for interrupt periodically
            if self.interrupted {
                self.is_done = true;
                self.rc = ErrorCode::Interrupt;
                return Err(Error::new(ErrorCode::Interrupt));
            }
        }
    }

    /// Execute a single opcode
    fn exec_op(&mut self) -> Result<ExecResult> {
        let pc = self.pc;
        let op = self.ops[pc as usize].clone();
        self.pc += 1;

        match op.opcode {
            // ================================================================
            // Control Flow
            // ================================================================
            Opcode::Init => {
                // Jump to start of program
                if op.p2 != 0 {
                    self.pc = op.p2;
                }
            }

            Opcode::Goto => {
                self.pc = op.p2;
            }

            Opcode::Halt => {
                self.rc = ErrorCode::from_i32(op.p1).unwrap_or(ErrorCode::Error);
                if let P4::Text(ref msg) = op.p4 {
                    self.error_msg = Some(msg.clone());
                }

                // Check if we're in a subprogram (trigger)
                if let Some((parent_ops, return_pc, _parent_pc)) = self.subprogram_stack.pop() {
                    // Return from subprogram to parent
                    self.ops = parent_ops;
                    self.pc = return_pc - 1; // -1 because it will be incremented
                    self.trigger_depth = self.trigger_depth.saturating_sub(1);

                    // If halt was due to error, propagate it
                    if self.rc != ErrorCode::Ok {
                        return Ok(ExecResult::Done);
                    }
                    // Otherwise continue execution in parent
                } else {
                    // Top-level halt - execution is done
                    return Ok(ExecResult::Done);
                }
            }

            Opcode::If => {
                let val = self.mem(op.p1);
                if val.is_truthy() {
                    self.pc = op.p2;
                }
            }

            Opcode::IfNot => {
                let val = self.mem(op.p1);
                if !val.is_truthy() {
                    self.pc = op.p2;
                }
            }

            Opcode::IsNull => {
                if self.mem(op.p1).is_null() {
                    self.pc = op.p2;
                }
            }

            Opcode::NotNull => {
                if !self.mem(op.p1).is_null() {
                    self.pc = op.p2;
                }
            }

            Opcode::Gosub => {
                // Store return address in P1, jump to P2
                let return_addr = self.pc;
                self.mem_mut(op.p1).set_int(return_addr as i64);
                self.pc = op.p2;
            }

            Opcode::Return => {
                // Return to address in P1
                let addr = self.mem(op.p1).to_int();
                self.pc = addr as i32;
            }

            Opcode::Yield => {
                // Save PC to P1, jump to address in P1
                let saved = self.mem(op.p1).to_int() as i32;
                let current_pc = self.pc;
                self.mem_mut(op.p1).set_int(current_pc as i64);
                self.pc = saved;
                if op.p2 != 0 && self.pc == 0 {
                    self.pc = op.p2;
                }
            }

            Opcode::Noop => {
                // Do nothing
            }

            // ================================================================
            // Data Movement
            // ================================================================
            Opcode::Null => {
                // Store NULL in P2
                let count = if op.p3 > 0 { op.p3 - op.p2 + 1 } else { 1 };
                for i in 0..count {
                    self.mem_mut(op.p2 + i).set_null();
                }
            }

            Opcode::Integer => {
                self.mem_mut(op.p2).set_int(op.p1 as i64);
            }

            Opcode::Int64 => {
                if let P4::Int64(v) = op.p4 {
                    self.mem_mut(op.p2).set_int(v);
                }
            }

            Opcode::Real => {
                if let P4::Real(v) = op.p4 {
                    self.mem_mut(op.p2).set_real(v);
                }
            }

            Opcode::String8 => {
                if let P4::Text(ref s) = op.p4 {
                    self.mem_mut(op.p2).set_str(s);
                }
            }

            Opcode::Blob => {
                if let P4::Blob(ref b) = op.p4 {
                    self.mem_mut(op.p2).set_blob(b);
                }
            }

            Opcode::Variable => {
                // Copy bound parameter P1 to register P2
                if op.p1 >= 1 && (op.p1 as usize) <= self.vars.len() {
                    let val = self.vars[(op.p1 - 1) as usize].clone();
                    self.set_mem(op.p2, val);
                } else {
                    self.mem_mut(op.p2).set_null();
                }
            }

            Opcode::Copy => {
                // Copy P1 to P2
                let val = self.mem(op.p1).clone();
                self.set_mem(op.p2, val);
            }

            Opcode::SCopy => {
                // Shallow copy (same as Copy for our implementation)
                let val = self.mem(op.p1).clone();
                self.set_mem(op.p2, val);
            }

            Opcode::Move => {
                // Move P1 to P2, leaving P1 as NULL
                let count = op.p3.max(1);
                for i in 0..count {
                    let val = self.mem(op.p1 + i).clone();
                    self.set_mem(op.p2 + i, val);
                    self.mem_mut(op.p1 + i).set_null();
                }
            }

            // ================================================================
            // Comparison
            // ================================================================
            Opcode::Eq => {
                let cmp = self.mem(op.p1).compare(self.mem(op.p3));
                if cmp == Ordering::Equal {
                    self.pc = op.p2;
                }
            }

            Opcode::Ne => {
                let cmp = self.mem(op.p1).compare(self.mem(op.p3));
                if cmp != Ordering::Equal {
                    self.pc = op.p2;
                }
            }

            Opcode::Lt => {
                let cmp = self.mem(op.p3).compare(self.mem(op.p1));
                if cmp == Ordering::Less {
                    self.pc = op.p2;
                }
            }

            Opcode::Le => {
                let cmp = self.mem(op.p3).compare(self.mem(op.p1));
                if cmp != Ordering::Greater {
                    self.pc = op.p2;
                }
            }

            Opcode::Gt => {
                let cmp = self.mem(op.p3).compare(self.mem(op.p1));
                if cmp == Ordering::Greater {
                    self.pc = op.p2;
                }
            }

            Opcode::Ge => {
                let cmp = self.mem(op.p3).compare(self.mem(op.p1));
                if cmp != Ordering::Less {
                    self.pc = op.p2;
                }
            }

            // ================================================================
            // Arithmetic
            // ================================================================
            Opcode::Add => {
                let mut result = self.mem(op.p2).clone();
                result.add(self.mem(op.p1))?;
                self.set_mem(op.p3, result);
            }

            Opcode::Subtract => {
                let mut result = self.mem(op.p2).clone();
                result.subtract(self.mem(op.p1))?;
                self.set_mem(op.p3, result);
            }

            Opcode::Multiply => {
                let mut result = self.mem(op.p2).clone();
                result.multiply(self.mem(op.p1))?;
                self.set_mem(op.p3, result);
            }

            Opcode::Divide => {
                let mut result = self.mem(op.p2).clone();
                result.divide(self.mem(op.p1))?;
                self.set_mem(op.p3, result);
            }

            Opcode::Remainder => {
                let mut result = self.mem(op.p2).clone();
                result.remainder(self.mem(op.p1))?;
                self.set_mem(op.p3, result);
            }

            Opcode::Concat => {
                let mut result = self.mem(op.p2).clone();
                result.concat(self.mem(op.p1))?;
                self.set_mem(op.p3, result);
            }

            Opcode::Negative => {
                let mut result = self.mem(op.p1).clone();
                result.negate()?;
                self.set_mem(op.p2, result);
            }

            Opcode::Not => {
                let mut result = self.mem(op.p1).clone();
                result.logical_not();
                self.set_mem(op.p2, result);
            }

            Opcode::BitNot => {
                let mut result = self.mem(op.p1).clone();
                result.bit_not()?;
                self.set_mem(op.p2, result);
            }

            Opcode::BitAnd => {
                let mut result = self.mem(op.p2).clone();
                result.bit_and(self.mem(op.p1))?;
                self.set_mem(op.p3, result);
            }

            Opcode::BitOr => {
                let mut result = self.mem(op.p2).clone();
                result.bit_or(self.mem(op.p1))?;
                self.set_mem(op.p3, result);
            }

            Opcode::ShiftLeft => {
                let mut result = self.mem(op.p2).clone();
                result.shift_left(self.mem(op.p1))?;
                self.set_mem(op.p3, result);
            }

            Opcode::ShiftRight => {
                let mut result = self.mem(op.p2).clone();
                result.shift_right(self.mem(op.p1))?;
                self.set_mem(op.p3, result);
            }

            // ================================================================
            // Result Row
            // ================================================================
            Opcode::ResultRow => {
                // P1 = start register, P2 = number of columns
                self.result_start = op.p1;
                self.result_count = op.p2;
                return Ok(ExecResult::Row);
            }

            // ================================================================
            // Cursor Operations
            // ================================================================
            Opcode::OpenRead => {
                // P1 = cursor, P2 = root page (or register if P5 has OPFLAG_P2ISREG)
                // P3 = num columns, P4 = table name
                let mut root_page = if op.p5 & 0x02 != 0 {
                    // P2 is a register containing the root page
                    self.mem(op.p2).to_int() as Pgno
                } else {
                    // P2 is the root page directly
                    op.p2 as Pgno
                };

                // Extract table name from P4 for column resolution
                let table_name = if let P4::Text(name) = &op.p4 {
                    Some(name.clone())
                } else {
                    None
                };

                // Check for sqlite_master virtual table
                let is_sqlite_master = table_name
                    .as_ref()
                    .map(|n| n.eq_ignore_ascii_case("sqlite_master"))
                    .unwrap_or(false);

                if is_sqlite_master {
                    // Populate schema entries from current schema BEFORE borrowing cursor
                    let mut entries = Vec::new();
                    if let Some(ref schema) = self.schema {
                        if let Ok(schema_guard) = schema.read() {
                            for (_, table) in schema_guard.tables.iter() {
                                entries.push((
                                    "table".to_string(),
                                    table.name.clone(),
                                    table.name.clone(),
                                    table.root_page,
                                    table.sql.clone(),
                                ));
                            }
                        }
                    }

                    // Create virtual cursor for sqlite_master
                    self.open_cursor(op.p1, 0, false)?;
                    if let Some(cursor) = self.cursor_mut(op.p1) {
                        cursor.n_field = 5; // type, name, tbl_name, rootpage, sql
                        cursor.table_name = table_name;
                        cursor.is_sqlite_master = true;
                        cursor.schema_entries = Some(entries);
                    }
                } else {
                    // If root_page is 0 and we have a table name in P4, look it up in schema
                    if root_page == 0 {
                        if let Some(ref tname) = table_name {
                            if let Some(ref schema) = self.schema {
                                if let Ok(schema_guard) = schema.read() {
                                    if let Some(table) = schema_guard.tables.get(tname) {
                                        root_page = table.root_page;
                                    }
                                }
                            }
                        }
                    }

                    // Clone btree Arc to avoid borrow issues
                    let btree = self.btree.clone();
                    self.open_cursor(op.p1, root_page, false)?;
                    if let Some(cursor) = self.cursor_mut(op.p1) {
                        cursor.n_field = op.p3;
                        cursor.table_name = table_name;
                        // Create a real BtCursor if we have a btree
                        if let Some(ref btree) = btree {
                            let flags = BtreeCursorFlags::empty();
                            match btree.cursor(root_page, flags, None) {
                                Ok(bt_cursor) => cursor.btree_cursor = Some(bt_cursor),
                                Err(_) => {} // Failed to create cursor, use placeholder
                            }
                        }
                    }
                }
            }

            Opcode::OpenWrite => {
                // P1 = cursor, P2 = root page, P3 = num columns
                let mut root_page = if op.p5 & 0x02 != 0 {
                    self.mem(op.p2).to_int() as Pgno
                } else {
                    op.p2 as Pgno
                };

                // If root_page is 0 and we have a table name in P4, look it up in schema
                if root_page == 0 {
                    if let P4::Text(table_name) = &op.p4 {
                        if let Some(ref schema) = self.schema {
                            if let Ok(schema_guard) = schema.read() {
                                if let Some(table) = schema_guard.tables.get(table_name) {
                                    root_page = table.root_page;
                                }
                            }
                        }
                    }
                }

                // Clone btree Arc to avoid borrow issues
                let btree = self.btree.clone();
                self.open_cursor(op.p1, root_page, true)?;
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    cursor.n_field = op.p3;
                    // Create a writable BtCursor if we have a btree
                    if let Some(ref btree) = btree {
                        let flags = BtreeCursorFlags::WRCSR;
                        match btree.cursor(root_page, flags, None) {
                            Ok(bt_cursor) => cursor.btree_cursor = Some(bt_cursor),
                            Err(_) => {} // Failed to create cursor
                        }
                    }
                }
            }

            Opcode::OpenEphemeral => {
                self.open_cursor(op.p1, 0, true)?;
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    cursor.is_ephemeral = true;
                    cursor.n_field = op.p2;
                }
            }

            Opcode::Close => {
                self.close_cursor(op.p1)?;
            }

            Opcode::Rewind => {
                // Move cursor to first row, jump to P2 if empty
                let mut is_empty = true;
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if cursor.is_sqlite_master {
                        // Virtual cursor for sqlite_master
                        cursor.virtual_index = 0;
                        if let Some(ref entries) = cursor.schema_entries {
                            is_empty = entries.is_empty();
                            if !is_empty {
                                cursor.state = CursorState::Valid;
                            } else {
                                cursor.state = CursorState::AtEnd;
                            }
                        }
                    } else if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                        match bt_cursor.first() {
                            Ok(empty) => {
                                is_empty = empty;
                                if !empty {
                                    cursor.state = CursorState::Valid;
                                } else {
                                    cursor.state = CursorState::AtEnd;
                                }
                            }
                            Err(_) => cursor.state = CursorState::AtEnd,
                        }
                    } else {
                        // No btree cursor - assume empty
                        cursor.state = CursorState::AtEnd;
                    }
                }
                // Jump if no rows
                if is_empty {
                    self.pc = op.p2;
                }
            }

            Opcode::Next => {
                // Move cursor to next row, jump to P2 if has more rows
                let mut has_more = false;
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if cursor.is_sqlite_master {
                        // Virtual cursor for sqlite_master
                        cursor.virtual_index += 1;
                        if let Some(ref entries) = cursor.schema_entries {
                            has_more = cursor.virtual_index < entries.len();
                            cursor.state = if has_more {
                                CursorState::Valid
                            } else {
                                CursorState::AtEnd
                            };
                        }
                    } else if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                        match bt_cursor.next(0) {
                            Ok(()) => {
                                // Check if cursor is still valid
                                has_more =
                                    bt_cursor.state == crate::storage::btree::CursorState::Valid;
                                cursor.state = if has_more {
                                    CursorState::Valid
                                } else {
                                    CursorState::AtEnd
                                };
                            }
                            Err(_) => cursor.state = CursorState::AtEnd,
                        }
                    } else {
                        cursor.state = CursorState::AtEnd;
                    }
                }
                // Jump to P2 if there are more rows
                if has_more {
                    self.pc = op.p2;
                }
            }

            Opcode::Prev => {
                // Move cursor to previous row, jump to P2 if has more rows
                let mut has_more = false;
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                        match bt_cursor.previous(0) {
                            Ok(()) => {
                                has_more =
                                    bt_cursor.state == crate::storage::btree::CursorState::Valid;
                                cursor.state = if has_more {
                                    CursorState::Valid
                                } else {
                                    CursorState::AtEnd
                                };
                            }
                            Err(_) => cursor.state = CursorState::AtEnd,
                        }
                    } else {
                        cursor.state = CursorState::AtEnd;
                    }
                }
                // Jump to P2 if there are more rows
                if has_more {
                    self.pc = op.p2;
                }
            }

            Opcode::Column => {
                // Read column P2 from cursor P1 into register P3
                // If P4 is a column name and P2 is 0, look up index by name
                let mut col_idx = op.p2 as usize;

                // Try to resolve column index by name if P4 contains a column name
                if let P4::Text(col_name) = &op.p4 {
                    if let Some(cursor) = self.cursor(op.p1) {
                        // Special handling for sqlite_master virtual table
                        if cursor.is_sqlite_master {
                            // sqlite_master columns: type, name, tbl_name, rootpage, sql
                            col_idx = match col_name.to_lowercase().as_str() {
                                "type" => 0,
                                "name" => 1,
                                "tbl_name" => 2,
                                "rootpage" => 3,
                                "sql" => 4,
                                _ => col_idx,
                            };
                        } else if let Some(ref table_name) = cursor.table_name {
                            if let Some(ref schema) = self.schema {
                                if let Ok(schema_guard) = schema.read() {
                                    if let Some(table) = schema_guard.tables.get(table_name) {
                                        for (i, col) in table.columns.iter().enumerate() {
                                            if col.name.eq_ignore_ascii_case(col_name) {
                                                col_idx = i;
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Handle sqlite_master virtual cursor separately to avoid borrow issues
                let sqlite_master_value: Option<Mem> = if let Some(cursor) = self.cursor(op.p1) {
                    if cursor.is_sqlite_master {
                        if let Some(ref entries) = cursor.schema_entries {
                            if cursor.virtual_index < entries.len() {
                                let entry = &entries[cursor.virtual_index];
                                let result = match col_idx {
                                    0 => Mem::from_str(&entry.0),       // type
                                    1 => Mem::from_str(&entry.1),       // name
                                    2 => Mem::from_str(&entry.2),       // tbl_name
                                    3 => Mem::from_int(entry.3 as i64), // rootpage
                                    4 => {
                                        if let Some(ref sql) = entry.4 {
                                            Mem::from_str(sql)
                                        } else {
                                            Mem::new() // null
                                        }
                                    }
                                    _ => Mem::new(), // null
                                };
                                Some(result)
                            } else {
                                Some(Mem::new()) // null
                            }
                        } else {
                            Some(Mem::new()) // null
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                if let Some(value) = sqlite_master_value {
                    *self.mem_mut(op.p3) = value;
                } else if let Some(cursor) = self.cursor(op.p1) {
                    if cursor.null_row {
                        self.mem_mut(op.p3).set_null();
                    } else if let Some(ref bt_cursor) = cursor.btree_cursor {
                        // Read payload from BtCursor
                        if let Some(ref payload) = bt_cursor.info.payload {
                            // Decode the record to get the column value
                            match crate::vdbe::auxdata::decode_record_header(payload) {
                                Ok((types, header_size)) => {
                                    if col_idx < types.len() {
                                        // Calculate offset to this column's data
                                        let mut data_offset = header_size;
                                        for i in 0..col_idx {
                                            data_offset += types[i].size();
                                        }
                                        // Deserialize the value
                                        let col_data = &payload[data_offset..];
                                        match crate::vdbe::auxdata::deserialize_value(
                                            col_data,
                                            &types[col_idx],
                                        ) {
                                            Ok(mem) => {
                                                *self.mem_mut(op.p3) = mem;
                                            }
                                            Err(_) => self.mem_mut(op.p3).set_null(),
                                        }
                                    } else {
                                        self.mem_mut(op.p3).set_null();
                                    }
                                }
                                Err(_) => self.mem_mut(op.p3).set_null(),
                            }
                        } else {
                            self.mem_mut(op.p3).set_null();
                        }
                    } else if let Some(ref row_data) = cursor.row_data {
                        // Fallback: read from cached row_data
                        match crate::vdbe::auxdata::decode_record_header(row_data) {
                            Ok((types, header_size)) => {
                                if col_idx < types.len() {
                                    let mut data_offset = header_size;
                                    for i in 0..col_idx {
                                        data_offset += types[i].size();
                                    }
                                    let col_data = &row_data[data_offset..];
                                    match crate::vdbe::auxdata::deserialize_value(
                                        col_data,
                                        &types[col_idx],
                                    ) {
                                        Ok(mem) => {
                                            *self.mem_mut(op.p3) = mem;
                                        }
                                        Err(_) => self.mem_mut(op.p3).set_null(),
                                    }
                                } else {
                                    self.mem_mut(op.p3).set_null();
                                }
                            }
                            Err(_) => self.mem_mut(op.p3).set_null(),
                        }
                    } else {
                        self.mem_mut(op.p3).set_null();
                    }
                } else {
                    self.mem_mut(op.p3).set_null();
                }
            }

            Opcode::Rowid => {
                // Get rowid from cursor P1 into register P2
                if let Some(cursor) = self.cursor(op.p1) {
                    if let Some(rowid) = cursor.rowid {
                        self.mem_mut(op.p2).set_int(rowid);
                    } else {
                        self.mem_mut(op.p2).set_null();
                    }
                } else {
                    self.mem_mut(op.p2).set_null();
                }
            }

            Opcode::NullRow => {
                // Set cursor to null row mode
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    cursor.set_null_row(true);
                }
            }

            // ================================================================
            // Record Operations
            // ================================================================
            Opcode::MakeRecord => {
                // Make record from P1..P1+P2-1, store in P3
                // Uses SQLite record format with varint header and serial types
                let record = crate::vdbe::auxdata::make_record(&self.mem, op.p1, op.p2);
                self.mem_mut(op.p3).set_blob(&record);
            }

            Opcode::DecodeRecord => {
                // Decode record in P1, store columns starting at P2, P3 columns total
                let record_data = self.mem(op.p1).to_blob();
                if !record_data.is_empty() {
                    match crate::vdbe::decode_record_header(&record_data) {
                        Ok((types, header_size)) => {
                            let num_cols = op.p3.min(types.len() as i32) as usize;
                            let mut data_offset = header_size;
                            for i in 0..num_cols {
                                if i < types.len() {
                                    let col_data = &record_data[data_offset..];
                                    match crate::vdbe::deserialize_value(col_data, &types[i]) {
                                        Ok(mem) => {
                                            *self.mem_mut(op.p2 + i as i32) = mem;
                                        }
                                        Err(_) => {
                                            self.mem_mut(op.p2 + i as i32).set_null();
                                        }
                                    }
                                    data_offset += types[i].size();
                                } else {
                                    self.mem_mut(op.p2 + i as i32).set_null();
                                }
                            }
                        }
                        Err(_) => {
                            // Failed to decode, set all columns to null
                            for i in 0..op.p3 {
                                self.mem_mut(op.p2 + i).set_null();
                            }
                        }
                    }
                }
            }

            // ================================================================
            // Transaction (placeholder)
            // ================================================================
            Opcode::Transaction => {
                // P1 = database, P2 = write flag
                // Placeholder: Would start transaction
            }

            Opcode::AutoCommit => {
                // P1 = 1 to commit, 0 to rollback
                // Placeholder: Would handle autocommit
            }

            // ================================================================
            // Aggregation (placeholder)
            // ================================================================
            Opcode::AggStep | Opcode::AggStep0 => {
                // Placeholder: Would call aggregate step function
            }

            Opcode::AggFinal | Opcode::AggValue => {
                // Placeholder: Would call aggregate final function
                self.mem_mut(op.p1).set_null();
            }

            // ================================================================
            // Function Call (placeholder)
            // ================================================================
            Opcode::Function | Opcode::Function0 => {
                // Placeholder: Would call the function
                if let P4::FuncDef(ref name) = op.p4 {
                    // Simple built-in function handling
                    match name.as_str() {
                        "current_time" => {
                            // Placeholder time
                            self.mem_mut(op.p2).set_str("12:00:00");
                        }
                        "current_date" => {
                            self.mem_mut(op.p2).set_str("2024-01-01");
                        }
                        "current_timestamp" => {
                            self.mem_mut(op.p2).set_str("2024-01-01 12:00:00");
                        }
                        _ => {
                            // Unknown function returns NULL
                            self.mem_mut(op.p2).set_null();
                        }
                    }
                } else {
                    self.mem_mut(op.p2).set_null();
                }
            }

            // ================================================================
            // Rowid and Insert Operations
            // ================================================================
            Opcode::NewRowid => {
                // NewRowid P1 P2 P3
                // Generate a new unique rowid for cursor P1, store in register P2
                // P3 is the previous rowid if updating
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    // Simple rowid generation - increment from last known rowid
                    let new_rowid = cursor.rowid.map_or(1, |r| r + 1);
                    cursor.rowid = Some(new_rowid);
                    self.mem_mut(op.p2).set_int(new_rowid);
                } else {
                    self.mem_mut(op.p2).set_int(1);
                }
            }

            Opcode::Insert | Opcode::InsertInt => {
                // Insert P1 P2 P3
                // Insert record P2 with rowid P3 into cursor P1
                // P4 = table name (for debug)
                // P5 = flags (conflict resolution)
                let record_data = self.mem(op.p2).to_blob();
                let rowid = self.mem(op.p3).to_int();

                // Get btree Arc before cursor borrow
                let btree_arc = self.btree.clone();

                if let Some(cursor) = self.cursor_mut(op.p1) {
                    cursor.rowid = Some(rowid);

                    // Actually insert into btree
                    if let Some(ref btree) = btree_arc {
                        if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                            // Create payload with record data
                            let payload = BtreePayload {
                                key: None, // Table insert, not index
                                n_key: rowid,
                                data: Some(record_data.clone()),
                                mem: Vec::new(),
                                n_data: record_data.len() as i32,
                                n_zero: 0,
                            };

                            // Insert flags from P5 (lower 8 bits)
                            let flags = BtreeInsertFlags::from_bits_truncate(op.p5 as u8);

                            // Perform the insert
                            if let Err(e) = btree.insert(bt_cursor, &payload, flags, 0) {
                                // Log error but continue for now
                                eprintln!("Insert failed: {:?}", e);
                            }
                        }
                    }
                }
            }

            // ================================================================
            // Other opcodes (placeholder implementations)
            // ================================================================
            Opcode::Last
            | Opcode::SeekRowid
            | Opcode::SeekGE
            | Opcode::SeekGT
            | Opcode::SeekLE
            | Opcode::SeekLT
            | Opcode::SeekNull
            | Opcode::NotExists
            | Opcode::Delete
            | Opcode::IdxGE
            | Opcode::IdxGT
            | Opcode::IdxLE
            | Opcode::IdxLT
            | Opcode::IdxRowid
            | Opcode::IdxInsert
            | Opcode::IdxDelete => {
                // Placeholder: These need btree integration
            }

            Opcode::OpenPseudo | Opcode::OpenAutoindex | Opcode::ResetSorter => {
                // Placeholder: Other sorter-related operations
            }

            Opcode::SorterInsert => {
                // SorterInsert P1 P2: Insert record from register P2 into sorter P1
                let record_data = self.mem(op.p2).to_blob();
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    cursor.sorter_data.push(record_data);
                }
            }

            Opcode::SorterSort => {
                // SorterSort P1 P2: Sort the sorter P1. Jump to P2 if empty.
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if cursor.sorter_data.is_empty() {
                        self.pc = op.p2;
                    } else {
                        // Sort the data
                        cursor.sorter_data.sort();
                        cursor.sorter_sorted = true;
                        cursor.sorter_index = 0;
                        cursor.state = CursorState::Valid;
                    }
                } else {
                    self.pc = op.p2;
                }
            }

            Opcode::SorterNext => {
                // SorterNext P1 P2: Advance to next sorted row. Jump to P2 if more rows.
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    cursor.sorter_index += 1;
                    if cursor.sorter_index < cursor.sorter_data.len() {
                        self.pc = op.p2; // Jump back to loop start
                    } else {
                        cursor.state = CursorState::AtEnd;
                    }
                }
            }

            Opcode::SorterData => {
                // SorterData P1 P2: Copy current sorter row data to register P2
                if let Some(cursor) = self.cursor(op.p1) {
                    if cursor.sorter_index < cursor.sorter_data.len() {
                        let data = cursor.sorter_data[cursor.sorter_index].clone();
                        self.mem_mut(op.p2).set_blob(&data);
                    }
                }
            }

            Opcode::SorterCompare => {
                // Placeholder: Sorter comparison
            }

            Opcode::ReadCookie | Opcode::SetCookie | Opcode::VerifyCookie => {
                // Placeholder: Schema cookie operations
            }

            Opcode::Savepoint | Opcode::Checkpoint => {
                // Placeholder: Savepoint operations
            }

            Opcode::Cast | Opcode::Affinity => {
                // Placeholder: Type conversion
            }

            Opcode::Compare | Opcode::Jump | Opcode::Once => {
                // Placeholder: Advanced comparison
            }

            Opcode::Between | Opcode::Like | Opcode::Glob | Opcode::Regexp => {
                // Placeholder: Pattern matching
            }

            Opcode::IfNullRow | Opcode::EndCoroutine => {
                // Placeholder: Advanced control flow
            }

            Opcode::And | Opcode::Or => {
                // These are typically handled inline by the compiler
            }

            Opcode::CreateBtree => {
                // CreateBtree P1 P2 P3
                // Create a new btree root page, store page number in register P2
                // P1 = database index (0 for main)
                // P3 = flags (BTREE_INTKEY for tables, 0 for indexes)
                let flags = op.p3 as u8;
                if let Some(ref btree) = self.btree {
                    let root_pgno = btree.create_table(flags)?;
                    self.mem_mut(op.p2).set_int(root_pgno as i64);
                }
            }

            Opcode::ParseSchema => {
                // ParseSchema P1 P2 P3 P4
                // Parse a CREATE TABLE/INDEX statement and add to schema
                // P2 = register containing root page number
                // P4 = SQL text of the CREATE statement
                let root_page = self.mem(op.p2).to_int() as u32;
                if let P4::Text(sql) = &op.p4 {
                    if let Some(ref schema) = self.schema {
                        if let Ok(mut schema_guard) = schema.write() {
                            // Check if IF NOT EXISTS was specified
                            let if_not_exists = sql.to_uppercase().contains("IF NOT EXISTS");

                            // Parse CREATE TABLE SQL and register the table
                            if let Some(table) = self.parse_create_table_sql(sql, root_page) {
                                let table_name_lower = table.name.to_lowercase();

                                // Check for reserved internal names (sqlite_*)
                                if table_name_lower.starts_with("sqlite_") {
                                    return Err(crate::error::Error::with_message(
                                        crate::error::ErrorCode::Error,
                                        format!(
                                            "object name reserved for internal use: {}",
                                            table.name
                                        ),
                                    ));
                                }

                                // Check for duplicate column names
                                let mut seen_columns = std::collections::HashSet::new();
                                for col in &table.columns {
                                    let col_lower = col.name.to_lowercase();
                                    if !seen_columns.insert(col_lower) {
                                        return Err(crate::error::Error::with_message(
                                            crate::error::ErrorCode::Error,
                                            format!("duplicate column name: {}", col.name),
                                        ));
                                    }
                                }

                                // Check if table already exists
                                if let std::collections::hash_map::Entry::Vacant(e) =
                                    schema_guard.tables.entry(table_name_lower)
                                {
                                    e.insert(std::sync::Arc::new(table));
                                } else {
                                    if !if_not_exists {
                                        // Return error: table already exists
                                        return Err(crate::error::Error::with_message(
                                            crate::error::ErrorCode::Error,
                                            format!("table {} already exists", table.name),
                                        ));
                                    }
                                    // IF NOT EXISTS was specified, silently succeed
                                }
                            }
                        }
                    }
                }
            }

            Opcode::DropSchema => {
                // DropSchema P1 P2 P3 P4
                // Remove table/index from schema
                // P4 = name of table/index to drop
                if let P4::Text(name) = &op.p4 {
                    if let Some(ref schema) = self.schema {
                        if let Ok(mut schema_guard) = schema.write() {
                            let name_lower = name.to_lowercase();
                            schema_guard.tables.remove(&name_lower);
                        }
                    }
                }
            }

            Opcode::Trace | Opcode::Explain | Opcode::SqlExec => {
                // Debug/explain operations
            }

            Opcode::FinishSeek | Opcode::SortKey | Opcode::Sequence | Opcode::Count => {
                // Placeholder: Misc operations
            }

            // ================================================================
            // Foreign Key Operations
            // ================================================================
            Opcode::FkCounter => {
                // FkCounter P1 P2
                // Add P1 to the deferred FK constraint counter
                // P2 = database index (unused for now)
                let delta = op.p1 as i64;
                self.deferred_fk_counter += delta;
            }

            Opcode::FkIfZero => {
                // FkIfZero P1 P2
                // Jump to P2 if deferred FK counter is zero
                // P1 = database index (unused for now)
                if self.deferred_fk_counter == 0 {
                    self.pc = op.p2 - 1; // -1 because we increment after
                }
            }

            Opcode::FkCheck => {
                // FkCheck P1 P2 P3 P4
                // Check immediate FK constraints
                // P1 = cursor for table being modified
                // P2 = register containing rowid
                // P3 = operation type (0=INSERT, 1=DELETE, 2=UPDATE)
                // P4 = table name
                if self.fk_enabled {
                    if let (Some(ref schema_lock), Some(ref btree)) = (&self.schema, &self.btree) {
                        if let Ok(schema) = schema_lock.read() {
                            // Get table name from P4
                            let table_name = match &op.p4 {
                                P4::Text(name) => name.as_str(),
                                _ => "",
                            };

                            if !table_name.is_empty() {
                                if let Some(table) = schema.table(table_name) {
                                    // Get values from the cursor's cached record
                                    if let Some(cursor) = self.cursor(op.p1) {
                                        if let Some(ref row_data) = cursor.row_data {
                                            // Decode the row values (simplified)
                                            let values = self.decode_record_values(
                                                row_data,
                                                cursor.n_field as usize,
                                            );

                                            // Perform FK check based on operation type
                                            let op_type = op.p3;
                                            let result = match op_type {
                                                0 => {
                                                    // INSERT
                                                    crate::executor::fkey::fk_check_insert(
                                                        &schema, btree, &table, &values, true,
                                                    )
                                                }
                                                1 => {
                                                    // DELETE
                                                    crate::executor::fkey::fk_check_delete(
                                                        &schema, btree, &table, &values, true,
                                                    )
                                                }
                                                _ => Ok(()),
                                            };

                                            result?
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // ================================================================
            // Trigger Operations
            // ================================================================
            Opcode::Program => {
                // Program P1 P2 P3 P4
                // Execute a trigger subprogram
                // P1 = subprogram context register (unused currently)
                // P2 = return address (jump here when subprogram finishes)
                // P3 = trigger mask/flags
                // P4 = SubProgram containing trigger bytecode

                // Check recursion depth
                if self.trigger_depth >= self.max_trigger_depth {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        format!(
                            "too many levels of trigger recursion (max {})",
                            self.max_trigger_depth
                        ),
                    ));
                }

                // Get the subprogram from P4
                if let P4::Subprogram(ref subprog) = op.p4 {
                    // Save current execution state
                    let return_pc = op.p2;
                    let current_ops = std::mem::take(&mut self.ops);
                    let current_pc = self.pc;

                    // Push state onto subprogram stack
                    self.subprogram_stack
                        .push((current_ops, return_pc, current_pc));

                    // Load subprogram
                    self.ops = subprog.ops.clone();
                    self.pc = -1; // Will be incremented to 0

                    // Increment trigger depth
                    self.trigger_depth += 1;
                } else {
                    // No subprogram - this is an error or no-op
                    // Just continue to next instruction
                }
            }

            Opcode::Param => {
                // Param P1 P2 P3
                // Access parameter from parent VDBE (for trigger body)
                // P1 = which parameter (0 = OLD row, 1 = NEW row)
                // P2 = column index (-1 for rowid)
                // P3 = destination register

                let row = if op.p1 == 0 {
                    &self.trigger_old_row
                } else {
                    &self.trigger_new_row
                };

                if let Some(ref row_data) = row {
                    let col_idx = op.p2;
                    if col_idx >= 0 && (col_idx as usize) < row_data.len() {
                        // Copy value from trigger row to destination register
                        let value = row_data[col_idx as usize].clone();
                        *self.mem_mut(op.p3) = value;
                    } else {
                        // Column index out of range - return NULL
                        self.mem_mut(op.p3).set_null();
                    }
                } else {
                    // No trigger context (not in a trigger) - return NULL
                    self.mem_mut(op.p3).set_null();
                }
            }

            Opcode::TriggerTest => {
                // TriggerTest P1 P2 P3
                // Test if trigger should fire
                // P1 = register containing rowid
                // P2 = trigger flags (timing/event bits)
                // P3 = jump destination if trigger should NOT fire
                //
                // For now, always skip (jump to P3) - triggers are disabled
                self.pc = op.p3 - 1;
            }

            Opcode::TriggerProlog => {
                // TriggerProlog
                // Marks end of trigger prolog (where OLD/NEW setup ends)
                // This is a no-op marker used for debugging/tracing
            }

            Opcode::MaxOpcode => {
                // Should never be executed
                return Err(Error::with_message(
                    ErrorCode::Internal,
                    "MaxOpcode should not be executed",
                ));
            }
        }

        Ok(ExecResult::Continue)
    }

    /// Parse CREATE TABLE SQL and build a Table struct
    fn parse_create_table_sql(&self, sql: &str, root_page: u32) -> Option<crate::schema::Table> {
        use crate::schema::{Affinity, Column, Table};

        // Simple parser for CREATE TABLE name (col1 type, col2 type, ...)
        let sql_upper = sql.to_uppercase();
        if sql_upper.starts_with("CREATE VIRTUAL TABLE") {
            let mut after_create = sql["CREATE VIRTUAL TABLE".len()..].trim();
            let after_upper = after_create.to_uppercase();
            if after_upper.starts_with("IF NOT EXISTS") {
                after_create = after_create[13..].trim();
            }

            let using_pos = after_create
                .to_uppercase()
                .find("USING")
                .unwrap_or(after_create.len());
            let table_name = after_create[..using_pos].trim().to_string();
            let mut columns = Vec::new();

            if using_pos < after_create.len() {
                let mut after_using = after_create[using_pos + 5..].trim();
                if let Some(paren_pos) = after_using.find('(') {
                    let args = after_using[paren_pos + 1..].trim();
                    let args = args.strip_suffix(')')?;
                    for arg in args.split(',') {
                        let name = arg.trim();
                        if name.is_empty() {
                            continue;
                        }
                        columns.push(Column {
                            name: name.to_string(),
                            type_name: None,
                            affinity: Affinity::Blob,
                            not_null: false,
                            not_null_conflict: None,
                            default_value: None,
                            collation: "BINARY".to_string(),
                            is_primary_key: false,
                            is_hidden: false,
                            generated: None,
                        });
                    }
                } else {
                    // No args: leave columns empty until module defines them.
                    after_using = after_using.trim();
                    let _module = after_using;
                }
            }

            return Some(Table {
                name: table_name,
                db_idx: 0,
                root_page,
                columns,
                primary_key: None,
                indexes: Vec::new(),
                foreign_keys: Vec::new(),
                checks: Vec::new(),
                without_rowid: false,
                strict: false,
                is_virtual: true,
                autoincrement: false,
                sql: Some(sql.to_string()),
                row_estimate: 0,
            });
        }

        if !sql_upper.starts_with("CREATE TABLE") {
            return None;
        }

        // Extract table name
        let after_create = sql[12..].trim(); // Skip "CREATE TABLE"
        let after_create = if after_create.to_uppercase().starts_with("IF NOT EXISTS") {
            after_create[13..].trim()
        } else {
            after_create
        };

        let paren_pos = after_create.find('(')?;
        let table_name = after_create[..paren_pos].trim().to_string();
        let columns_str = after_create[paren_pos + 1..].trim();
        let columns_str = columns_str.strip_suffix(')')?;

        // Parse columns
        let mut columns = Vec::new();
        for col_def in columns_str.split(',') {
            let col_def = col_def.trim();
            if col_def.is_empty() {
                continue;
            }

            let parts: Vec<&str> = col_def.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }

            let name = parts[0].to_string();
            let type_name = if parts.len() > 1 {
                Some(parts[1].to_string())
            } else {
                None
            };

            // Determine affinity from type name
            let affinity = if let Some(ref tn) = type_name {
                let tn_upper = tn.to_uppercase();
                if tn_upper.contains("INT") {
                    Affinity::Integer
                } else if tn_upper.contains("CHAR")
                    || tn_upper.contains("CLOB")
                    || tn_upper.contains("TEXT")
                {
                    Affinity::Text
                } else if tn_upper.contains("BLOB") || tn_upper.is_empty() {
                    Affinity::Blob
                } else if tn_upper.contains("REAL")
                    || tn_upper.contains("FLOA")
                    || tn_upper.contains("DOUB")
                {
                    Affinity::Real
                } else {
                    Affinity::Numeric
                }
            } else {
                Affinity::Blob
            };

            columns.push(Column {
                name,
                type_name,
                affinity,
                not_null: false,
                not_null_conflict: None,
                default_value: None,
                collation: "BINARY".to_string(),
                is_primary_key: false,
                is_hidden: false,
                generated: None,
            });
        }

        Some(Table {
            name: table_name,
            db_idx: 0, // main database
            root_page,
            columns,
            primary_key: None,
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            checks: Vec::new(),
            without_rowid: false,
            strict: false,
            is_virtual: false,
            autoincrement: false,
            sql: Some(sql.to_string()),
            row_estimate: 0,
        })
    }

    /// Decode a record's raw bytes into Value vector
    /// This is a simplified decoder for FK checking
    fn decode_record_values(&self, _data: &[u8], n_fields: usize) -> Vec<Value> {
        // For now, return placeholder values
        // TODO: Implement proper record format decoding when needed
        // The actual format is: header_size (varint), type_codes[], data[]
        vec![Value::Null; n_fields]
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vdbe_new() {
        let vdbe = Vdbe::new();
        assert_eq!(vdbe.magic, VDBE_MAGIC_INIT);
        assert_eq!(vdbe.pc, 0);
        assert!(!vdbe.is_done);
    }

    #[test]
    fn test_vdbe_simple_program() {
        // Program: Integer 42 -> r1, ResultRow r1 1, Halt
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        // First step should return Row
        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_count(), 1);
        assert_eq!(vdbe.column_int(0), 42);

        // Second step should return Done
        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Done);
    }

    #[test]
    fn test_vdbe_arithmetic() {
        // Program: 10 + 20 = 30
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Integer, 10, 1, 0),
            VdbeOp::new(Opcode::Integer, 20, 2, 0),
            VdbeOp::new(Opcode::Add, 2, 1, 3), // r3 = r1 + r2
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 30);
    }

    #[test]
    fn test_vdbe_goto() {
        // Program: Goto 2, Integer 1 (skipped), Integer 42, ResultRow, Halt
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Goto, 0, 2, 0),
            VdbeOp::new(Opcode::Integer, 1, 1, 0), // Skipped
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 42);
    }

    #[test]
    fn test_vdbe_conditional() {
        // Program: If r1 is truthy, jump to 3; otherwise result is 0
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Integer, 1, 1, 0), // r1 = 1 (truthy)
            VdbeOp::new(Opcode::If, 1, 4, 0),      // if r1, goto 4
            VdbeOp::new(Opcode::Integer, 0, 2, 0), // r2 = 0 (not taken)
            VdbeOp::new(Opcode::Goto, 0, 5, 0),
            VdbeOp::new(Opcode::Integer, 99, 2, 0), // r2 = 99 (taken)
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 99);
    }

    #[test]
    fn test_vdbe_parameter_binding() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Variable, 1, 1, 0), // r1 = ?1
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        vdbe.ensure_vars(1);
        vdbe.bind_int(1, 42).unwrap();

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 42);
    }

    #[test]
    fn test_vdbe_string_operations() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("hello".to_string())),
            VdbeOp::with_p4(Opcode::String8, 0, 2, 0, P4::Text(" world".to_string())),
            VdbeOp::new(Opcode::Concat, 2, 1, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_text(0), "hello world");
    }

    #[test]
    fn test_vdbe_null_handling() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Null, 0, 1, 0),
            VdbeOp::new(Opcode::Integer, 42, 2, 0),
            VdbeOp::new(Opcode::Add, 2, 1, 3), // NULL + 42 = NULL
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_type(0), ColumnType::Null);
    }

    #[test]
    fn test_vdbe_comparison() {
        // Test Lt: if 5 < 10, jump
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Integer, 10, 1, 0), // r1 = 10
            VdbeOp::new(Opcode::Integer, 5, 2, 0),  // r2 = 5
            VdbeOp::new(Opcode::Lt, 1, 5, 2),       // if r2 < r1, goto 5
            VdbeOp::new(Opcode::Integer, 0, 3, 0),  // r3 = 0 (not taken)
            VdbeOp::new(Opcode::Goto, 0, 6, 0),
            VdbeOp::new(Opcode::Integer, 1, 3, 0), // r3 = 1 (taken)
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 1); // Jump was taken
    }

    #[test]
    fn test_vdbe_gosub_return() {
        // Program with subroutine
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Gosub, 1, 3, 0), // Call sub at 3, return addr in r1
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
            // Subroutine at 3:
            VdbeOp::new(Opcode::Integer, 42, 2, 0),
            VdbeOp::new(Opcode::Return, 1, 0, 0), // Return to r1
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 42);
    }

    #[test]
    fn test_vdbe_reset() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        // First run
        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        vdbe.step().unwrap(); // Done

        // Reset and run again
        vdbe.reset();
        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 42);
    }

    #[test]
    fn test_vdbe_interrupt() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Goto, 0, 0, 0), // Infinite loop
        ]);

        vdbe.interrupt();
        let result = vdbe.step();
        assert!(result.is_err());
        assert_eq!(vdbe.result_code(), ErrorCode::Interrupt);
    }

    #[test]
    fn test_vdbe_multiple_results() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Integer, 1, 1, 0),
            VdbeOp::new(Opcode::Integer, 2, 2, 0),
            VdbeOp::new(Opcode::Integer, 3, 3, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 3, 0), // 3 columns
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_count(), 3);
        assert_eq!(vdbe.column_int(0), 1);
        assert_eq!(vdbe.column_int(1), 2);
        assert_eq!(vdbe.column_int(2), 3);
    }
}
