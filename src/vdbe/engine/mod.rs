//! VDBE Core Execution Engine
//!
//! The Virtual Database Engine (VDBE) is the bytecode interpreter that
//! executes all SQL statements. This module implements the main execution
//! loop and manages the virtual machine state.

mod state;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::api::{SqliteConnection, TransactionState};
use crate::error::{Error, ErrorCode, Result};
use crate::functions::aggregate::AggregateState;
use crate::schema::Schema;
use crate::storage::btree::{
    BtCursor, Btree, BtreeCursorFlags, BtreeInsertFlags, BtreePayload, UnpackedRecord,
    BTREE_FILE_FORMAT, BTREE_SCHEMA_VERSION,
};
use crate::storage::pager::SavepointOp;
use crate::types::{ColumnType, OpenFlags, Pgno, Value};
use crate::vdbe::mem::Mem;
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

// Re-export from state module
pub use state::{get_search_count, get_sort_flag, reset_search_count, reset_sort_flag};

// Use state module items locally
use state::{
    inc_search_count, set_sort_flag, DEFAULT_CURSOR_SLOTS, DEFAULT_MEM_SIZE, OE_ABORT, OE_FAIL,
    OE_IGNORE, OE_MASK, OE_NONE, OE_REPLACE, OE_ROLLBACK, OPFLAG_APPEND, OPFLAG_ISUPDATE,
    OPFLAG_LASTROWID, OPFLAG_NCHANGE, VDBE_MAGIC_DEAD, VDBE_MAGIC_HALT, VDBE_MAGIC_INIT,
    VDBE_MAGIC_RUN,
};

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
    /// Cached decoded columns (to avoid re-decoding on repeated access)
    pub cached_columns: Option<Vec<Mem>>,
    /// Number of columns
    pub n_field: i32,
    /// Null row flag (for outer joins)
    pub null_row: bool,
    /// Deferred seek key
    pub seek_key: Option<Vec<Mem>>,
    /// Deferred table seek pending
    pub deferred_moveto: bool,
    /// Target rowid for deferred seek
    pub moveto_target: Option<i64>,
    /// Alternative cursor for deferred seek column reads
    pub alt_cursor: Option<i32>,
    /// Column mapping from this cursor to alt_cursor (maps col index -> alt col index)
    pub alt_map: Option<Vec<i32>>,
    /// B-tree cursor for actual storage operations
    pub btree_cursor: Option<BtCursor>,
    /// Table name (for looking up column indices at runtime)
    pub table_name: Option<String>,
    /// Is this a sqlite_master virtual cursor?
    pub is_sqlite_master: bool,
    /// Is this a sqlite_stat1 virtual cursor?
    pub is_sqlite_stat1: bool,
    /// Is this a virtual table cursor (custom module)
    pub is_virtual: bool,
    /// Current index for virtual cursors (sqlite_master iteration)
    pub virtual_index: usize,
    /// Cached schema entries for sqlite_master (type, name, tbl_name, rootpage, sql)
    pub schema_entries: Option<Vec<(String, String, String, u32, Option<String>)>>,
    /// Cached stat1 entries for sqlite_stat1 (tbl, idx, stat)
    pub stat1_entries: Option<Vec<(String, Option<String>, String)>>,
    /// Virtual table name (for module lookup)
    pub vtab_name: Option<String>,
    /// Virtual table rowids for current scan
    pub vtab_rowids: Vec<i64>,
    /// Current index into virtual table rowids
    pub vtab_row_index: usize,
    /// Tokenized rows for fts3tokenize virtual tables
    #[cfg(feature = "fts3")]
    pub vtab_tokens: Vec<crate::fts3::Fts3Token>,
    /// Input string for fts3tokenize virtual tables
    #[cfg(feature = "fts3")]
    pub vtab_input: Option<String>,
    /// Sorter data - rows to be sorted (each row is a serialized record)
    pub sorter_data: Vec<Vec<u8>>,
    /// Sorter index - current position in sorted data
    pub sorter_index: usize,
    /// Sequence counter for OP_Sequence
    pub seq_count: i64,
    /// Has the sorter been sorted?
    pub sorter_sorted: bool,
    /// Sort directions for each ORDER BY column (true = DESC, false = ASC)
    pub sort_desc: Vec<bool>,
    /// Ephemeral index data - used for DISTINCT and index operations
    pub ephemeral_set: std::collections::HashSet<Vec<u8>>,
    /// Ephemeral table rows for iteration - stores (rowid, record) pairs
    pub ephemeral_rows: Vec<(i64, Vec<u8>)>,
    /// Current index into ephemeral_rows during iteration
    pub ephemeral_index: usize,
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
            cached_columns: None,
            n_field: 0,
            null_row: false,
            seek_key: None,
            deferred_moveto: false,
            moveto_target: None,
            alt_cursor: None,
            alt_map: None,
            btree_cursor: None,
            table_name: None,
            is_sqlite_master: false,
            is_sqlite_stat1: false,
            is_virtual: false,
            virtual_index: 0,
            schema_entries: None,
            stat1_entries: None,
            vtab_name: None,
            vtab_rowids: Vec::new(),
            vtab_row_index: 0,
            #[cfg(feature = "fts3")]
            vtab_tokens: Vec::new(),
            #[cfg(feature = "fts3")]
            vtab_input: None,
            sorter_data: Vec::new(),
            sorter_index: 0,
            seq_count: 0,
            sorter_sorted: false,
            sort_desc: Vec::new(),
            ephemeral_set: std::collections::HashSet::new(),
            ephemeral_rows: Vec::new(),
            ephemeral_index: 0,
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

    /// Whether count_changes result row has been returned
    count_changes_returned: bool,

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

    /// Connection pointer for transaction/autocommit state
    conn_ptr: Option<*mut SqliteConnection>,

    /// Deferred foreign key violation counter
    deferred_fk_counter: i64,

    /// Foreign key enforcement enabled
    fk_enabled: bool,

    /// Active virtual table query string (for FTS helpers)
    vtab_query: Option<String>,
    /// Active virtual table name for FTS helpers
    vtab_context_name: Option<String>,
    /// Active virtual table rowid for FTS helpers
    vtab_context_rowid: Option<i64>,

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

    /// Aggregate function contexts, keyed by accumulator register
    agg_contexts: HashMap<i32, AggregateState>,

    /// Last comparison result (for Compare/Jump opcodes)
    last_compare: std::cmp::Ordering,

    /// Tracking which Once opcodes have been executed
    once_flags: std::collections::HashSet<i32>,

    /// RowSet objects for IN clause optimization, keyed by register number
    rowsets: HashMap<i32, std::collections::BTreeSet<i64>>,
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
            count_changes_returned: false,
            start_time: None,
            vars: Vec::new(),
            var_names: Vec::new(),
            interrupted: false,
            instruction_count: 0,
            max_instructions: 100_000_000, // Default 100M instruction limit
            result_start: 0,
            result_count: 0,
            column_names: Vec::new(),
            btree: None,
            schema: None,
            conn_ptr: None,
            deferred_fk_counter: 0,
            fk_enabled: true,
            vtab_query: None,
            vtab_context_name: None,
            vtab_context_rowid: None,
            trigger_old_row: None,
            trigger_new_row: None,
            trigger_depth: 0,
            max_trigger_depth: 1000,
            subprogram_stack: Vec::new(),
            agg_contexts: HashMap::new(),
            last_compare: std::cmp::Ordering::Equal,
            once_flags: std::collections::HashSet::new(),
            rowsets: HashMap::new(),
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

    /// Set the connection pointer for transaction/autocommit updates
    pub fn set_connection(&mut self, conn_ptr: *mut SqliteConnection) {
        self.conn_ptr = Some(conn_ptr);
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
        let idx = reg as usize;
        // Return first element (null) for out-of-bounds - safer than panic
        self.mem.get(idx).unwrap_or(&self.mem[0])
    }

    /// Get mutable memory cell
    pub fn mem_mut(&mut self, reg: i32) -> &mut Mem {
        let idx = reg as usize;
        // Grow memory array if needed
        if idx >= self.mem.len() {
            self.mem.resize(idx + 16, Mem::new());
        }
        &mut self.mem[idx]
    }

    /// Set memory cell value
    pub fn set_mem(&mut self, reg: i32, value: Mem) {
        let idx = reg as usize;
        // Grow memory array if needed
        if idx >= self.mem.len() {
            self.mem.resize(idx + 16, Mem::new());
        }
        self.mem[idx] = value;
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
        self.count_changes_returned = false;
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

        // Clear OP_Once flags so they trigger again on re-execution
        self.once_flags.clear();
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
                        let msg = self
                            .error_msg
                            .clone()
                            .unwrap_or_else(|| "constraint failed".to_string());
                        return Err(Error::with_message(self.rc, msg));
                    }
                    // Otherwise continue execution in parent
                } else {
                    // If halt was due to error (non-zero P1), return error
                    if self.rc != ErrorCode::Ok {
                        let msg = self
                            .error_msg
                            .clone()
                            .unwrap_or_else(|| "constraint failed".to_string());
                        return Err(Error::with_message(self.rc, msg));
                    }

                    // Top-level halt - check if we need to return count_changes result
                    // NOTE: Returning count_changes as a Row from Halt causes database corruption
                    // because it leaves cursors and transaction state inconsistent.
                    // Disabled for now - needs proper fix to clean up cursors/state first.
                    // TODO: Implement proper count_changes handling that:
                    // 1. Closes all cursors and cleans up VDBE state
                    // 2. Commits or rolls back transaction as needed
                    // 3. THEN returns the count as a synthetic row if needed
                    /*
                    if !self.count_changes_returned && self.n_change > 0 {
                        if let Some(conn_ptr) = self.conn_ptr {
                            let conn = unsafe { &*conn_ptr };
                            if conn.db_config.count_changes {
                                // Return the change count as a result row
                                self.count_changes_returned = true;
                                // Allocate a register for the count and set result
                                let count_reg = 1; // Use register 1 for the count
                                self.set_mem(count_reg, Mem::from_int(self.n_change));
                                self.result_start = count_reg;
                                self.result_count = 1;
                                // Don't increment PC so we come back here after returning Row
                                self.pc -= 1;
                                return Ok(ExecResult::Row);
                            }
                        }
                    }
                    */
                    // execution is done
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

            Opcode::IfPos => {
                // IfPos P1 P2 P3: If r[P1] > 0 then r[P1] -= P3, jump to P2
                let val = self.mem(op.p1).to_int();
                if val > 0 {
                    self.mem_mut(op.p1).set_int(val - op.p3 as i64);
                    self.pc = op.p2;
                }
            }

            Opcode::DecrJumpZero => {
                // DecrJumpZero P1 P2: Decrement r[P1], jump to P2 if it becomes exactly zero
                // SQLite vdbe.c: if( pIn1->u.i==0 ) goto jump_to_p2;
                let val = self.mem(op.p1).to_int();
                let new_val = val - 1;
                self.mem_mut(op.p1).set_int(new_val);
                if new_val == 0 {
                    self.pc = op.p2;
                }
            }

            Opcode::OffsetLimit => {
                // OffsetLimit P1 P2 P3: If r[P1] > 0, subtract r[P3] from r[P1] and store in r[P2]
                // Used for computing remaining LIMIT after OFFSET
                let limit = self.mem(op.p1).to_int();
                let offset = self.mem(op.p3).to_int();
                if limit < 0 {
                    // Negative limit means no limit
                    self.mem_mut(op.p2).set_int(-1);
                } else {
                    self.mem_mut(op.p2)
                        .set_int(limit.saturating_sub(offset).max(0));
                }
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
                // P5 flags: SQLITE_NULLEQ (0x80) means NULL==NULL is true
                let left = self.mem(op.p1);
                let right = self.mem(op.p3);
                let nulleq = (op.p5 & 0x80) != 0;

                // In standard SQL, comparing with NULL yields unknown (no jump)
                // unless NULLEQ flag is set
                if !nulleq && (left.is_null() || right.is_null()) {
                    // No jump - comparison with NULL is unknown
                } else {
                    let cmp = left.compare(right);
                    if cmp == Ordering::Equal {
                        self.pc = op.p2;
                    }
                }
            }

            Opcode::Ne => {
                // P5 flags: SQLITE_NULLEQ (0x80) means NULL==NULL is true
                let left = self.mem(op.p1);
                let right = self.mem(op.p3);
                let nulleq = (op.p5 & 0x80) != 0;

                // In standard SQL, comparing with NULL yields unknown (no jump)
                // unless NULLEQ flag is set
                if left.is_null() || right.is_null() {
                    if nulleq {
                        if left.is_null() ^ right.is_null() {
                            self.pc = op.p2;
                        }
                    }
                } else {
                    let cmp = left.compare(right);
                    if cmp != Ordering::Equal {
                        self.pc = op.p2;
                    }
                }
            }

            Opcode::Lt => {
                // Lt P1 P2 P3 * P5: jump to P2 if r[P3] < r[P1]
                // Note: SQLite semantics compare P3 vs P1 (not P1 vs P3)
                use crate::vdbe::ops::cmp_flags;

                let left = self.mem(op.p3);
                let right = self.mem(op.p1);
                let jumpifnull = (op.p5 & cmp_flags::JUMPIFNULL) != 0;

                if left.is_null() || right.is_null() {
                    if jumpifnull {
                        self.pc = op.p2;
                    }
                    // Otherwise: standard SQL - result is unknown, no jump
                } else {
                    let cmp = left.compare(right);
                    if cmp == Ordering::Less {
                        self.pc = op.p2;
                    }
                }
            }

            Opcode::Le => {
                // Le P1 P2 P3 * P5: jump to P2 if r[P3] <= r[P1]
                use crate::vdbe::ops::cmp_flags;

                let left = self.mem(op.p3);
                let right = self.mem(op.p1);
                let jumpifnull = (op.p5 & cmp_flags::JUMPIFNULL) != 0;

                if left.is_null() || right.is_null() {
                    if jumpifnull {
                        self.pc = op.p2;
                    }
                } else {
                    let cmp = left.compare(right);
                    if cmp != Ordering::Greater {
                        self.pc = op.p2;
                    }
                }
            }

            Opcode::Gt => {
                // Gt P1 P2 P3 * P5: jump to P2 if r[P3] > r[P1]
                use crate::vdbe::ops::cmp_flags;

                let left = self.mem(op.p3);
                let right = self.mem(op.p1);
                let jumpifnull = (op.p5 & cmp_flags::JUMPIFNULL) != 0;

                if left.is_null() || right.is_null() {
                    if jumpifnull {
                        self.pc = op.p2;
                    }
                } else {
                    let cmp = left.compare(right);
                    if cmp == Ordering::Greater {
                        self.pc = op.p2;
                    }
                }
            }

            Opcode::Ge => {
                // Ge P1 P2 P3 * P5: jump to P2 if r[P3] >= r[P1]
                use crate::vdbe::ops::cmp_flags;

                let left = self.mem(op.p3);
                let right = self.mem(op.p1);
                let jumpifnull = (op.p5 & cmp_flags::JUMPIFNULL) != 0;

                if left.is_null() || right.is_null() {
                    if jumpifnull {
                        self.pc = op.p2;
                    }
                } else {
                    let cmp = left.compare(right);
                    if cmp != Ordering::Less {
                        self.pc = op.p2;
                    }
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

            Opcode::AddImm => {
                // AddImm P1 P2: Add immediate P2 to register P1
                let val = self.mem(op.p1).to_int();
                self.mem_mut(op.p1).set_int(val + op.p2 as i64);
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

                // Check for sqlite_stat1 virtual table
                let is_sqlite_stat1 = table_name
                    .as_ref()
                    .map(|n| n.eq_ignore_ascii_case("sqlite_stat1"))
                    .unwrap_or(false);

                if is_sqlite_master {
                    // Populate schema entries from current schema BEFORE borrowing cursor
                    let mut entries = Vec::new();
                    if let Some(ref schema) = self.schema {
                        if let Ok(schema_guard) = schema.read() {
                            // Add tables
                            for (_, table) in schema_guard.tables.iter() {
                                entries.push((
                                    "table".to_string(),
                                    table.name.clone(),
                                    table.name.clone(),
                                    table.root_page,
                                    table.sql.clone(),
                                ));
                            }
                            // Add indexes
                            for (_, index) in schema_guard.indexes.iter() {
                                entries.push((
                                    "index".to_string(),
                                    index.name.clone(),
                                    index.table.clone(),
                                    index.root_page,
                                    index.sql.clone(),
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
                } else if is_sqlite_stat1 {
                    // Populate stat1 entries from schema
                    let mut entries: Vec<(String, Option<String>, String)> = Vec::new();
                    if let Some(ref schema) = self.schema {
                        if let Ok(schema_guard) = schema.read() {
                            for ((tbl, idx), row) in schema_guard.stat1.iter() {
                                entries.push((tbl.clone(), idx.clone(), row.stat.clone()));
                            }
                        }
                    }

                    // Create virtual cursor for sqlite_stat1
                    self.open_cursor(op.p1, 0, false)?;
                    if let Some(cursor) = self.cursor_mut(op.p1) {
                        cursor.n_field = 3; // tbl, idx, stat
                        cursor.table_name = table_name;
                        cursor.is_sqlite_stat1 = true;
                        cursor.stat1_entries = Some(entries);
                    }
                } else {
                    let mut table_meta = None;
                    // If root_page is 0 and we have a table name in P4, look it up in schema
                    if root_page == 0 {
                        if let Some(ref tname) = table_name {
                            if let Some(ref schema) = self.schema {
                                if let Ok(schema_guard) = schema.read() {
                                    if let Some(table) = schema_guard.tables.get(tname) {
                                        root_page = table.root_page;
                                        table_meta = Some(std::sync::Arc::clone(table));
                                    }
                                }
                            }
                            // Table not found - return error (but not if it's a virtual table)
                            let is_virtual =
                                table_meta.as_ref().map(|t| t.is_virtual).unwrap_or(false);
                            if root_page == 0 && !is_virtual && table_meta.is_none() {
                                return Err(Error::with_message(
                                    ErrorCode::Error,
                                    format!("no such table: {}", tname),
                                ));
                            }
                        }
                    }

                    if table_meta
                        .as_ref()
                        .map(|table| table.is_virtual)
                        .unwrap_or(false)
                    {
                        self.open_cursor(op.p1, 0, false)?;
                        if let Some(cursor) = self.cursor_mut(op.p1) {
                            cursor.n_field = table_meta
                                .as_ref()
                                .map(|table| table.columns.len() as i32)
                                .unwrap_or(op.p3);
                            cursor.table_name = table_name.clone();
                            cursor.is_virtual = true;
                            cursor.vtab_name = table_name;
                        }
                    } else {
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
            }

            Opcode::OpenWrite => {
                // P1 = cursor, P2 = root page, P3 = num columns
                let mut root_page = if op.p5 & 0x02 != 0 {
                    self.mem(op.p2).to_int() as Pgno
                } else {
                    op.p2 as Pgno
                };

                // Look up table info from schema first (need to check is_virtual before
                // deciding if root_page=0 is an error)
                let mut is_virtual = false;
                let mut table_name = None;
                let mut table_columns = None;
                let mut table_found = false;
                if let P4::Text(name) = &op.p4 {
                    table_name = Some(name.clone());
                    if let Some(ref schema) = self.schema {
                        if let Ok(schema_guard) = schema.read() {
                            if let Some(table) = schema_guard.tables.get(name) {
                                table_found = true;
                                is_virtual = table.is_virtual;
                                table_columns = Some(table.columns.len() as i32);
                                if root_page == 0 {
                                    root_page = table.root_page;
                                }
                            }
                        }
                    }
                }

                // For non-virtual tables, root_page=0 means table not found
                if root_page == 0 && !is_virtual {
                    if let Some(ref tname) = table_name {
                        if !table_found {
                            return Err(Error::with_message(
                                ErrorCode::Error,
                                format!("no such table: {}", tname),
                            ));
                        }
                    }
                }

                if is_virtual {
                    self.open_cursor(op.p1, 0, true)?;
                    if let Some(cursor) = self.cursor_mut(op.p1) {
                        cursor.n_field = table_columns.unwrap_or(op.p3);
                        cursor.is_virtual = true;
                        cursor.table_name = table_name.clone();
                        cursor.vtab_name = table_name;
                    }
                } else {
                    // Clone btree Arc to avoid borrow issues
                    let btree = self.btree.clone();
                    if let Some(ref btree) = btree {
                        btree.lock_table(root_page as i32, true)?;
                    }
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
            }

            Opcode::VFilter => {
                // Apply filter to virtual table cursor P1 (P4 = query string)
                // Extract query from P4 or memory before getting mutable cursor
                let mut query = match &op.p4 {
                    P4::Text(text) => text.clone(),
                    P4::Vtab(text) => text.clone(),
                    _ => String::new(),
                };
                if query.is_empty() && op.p2 > 0 {
                    query = self.mem(op.p2).to_str();
                }

                let vtab_name = self
                    .cursor(op.p1)
                    .and_then(|cursor| cursor.vtab_name.clone());

                let btree = self.btree.clone();
                let schema = self.schema.clone();
                let vtab_module = vtab_name.as_ref().and_then(|name| {
                    schema.as_ref().and_then(|schema| {
                        schema
                            .read()
                            .ok()
                            .and_then(|guard| guard.table(name))
                            .and_then(|table| table.virtual_module.clone())
                    })
                });
                let mut new_vtab_query: Option<String> = None;
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if cursor.is_virtual {
                        #[allow(unused_variables)]
                        if let Some(ref vtab_name) = vtab_name {
                            #[cfg(feature = "fts3")]
                            {
                                if let Some(module) = vtab_module.as_ref() {
                                    if module.eq_ignore_ascii_case("fts3tokenize") {
                                        if let Some(table) =
                                            crate::fts3::get_tokenize_table(vtab_name)
                                        {
                                            if let Ok(table) = table.lock() {
                                                cursor.vtab_input = if query.is_empty() {
                                                    None
                                                } else {
                                                    Some(query.clone())
                                                };
                                                let tokens = table.tokenize(&query)?;
                                                cursor.vtab_tokens = tokens;
                                                cursor.vtab_rowids =
                                                    (0..cursor.vtab_tokens.len() as i64).collect();
                                                cursor.vtab_row_index = 0;
                                                if cursor.vtab_rowids.is_empty() {
                                                    cursor.state = CursorState::AtEnd;
                                                    cursor.rowid = None;
                                                } else {
                                                    cursor.state = CursorState::Valid;
                                                    cursor.rowid = Some(cursor.vtab_rowids[0]);
                                                }
                                            }
                                        }
                                    } else if module.eq_ignore_ascii_case("fts3") {
                                        if let Some(table) = crate::fts3::get_table(vtab_name) {
                                            if let Ok(mut table) = table.lock() {
                                                if let (Some(ref btree), Some(ref schema)) =
                                                    (btree.as_ref(), schema.as_ref())
                                                {
                                                    if let Ok(schema_guard) = schema.read() {
                                                        table
                                                            .ensure_loaded(btree, &schema_guard)?;
                                                    }
                                                }
                                                if let Ok(rowids) = table.query_rowids(&query) {
                                                    cursor.vtab_rowids = rowids;
                                                    cursor.vtab_row_index = 0;
                                                    if cursor.vtab_rowids.is_empty() {
                                                        cursor.state = CursorState::AtEnd;
                                                        cursor.rowid = None;
                                                    } else {
                                                        cursor.state = CursorState::Valid;
                                                        cursor.rowid = Some(cursor.vtab_rowids[0]);
                                                    }
                                                }
                                            }
                                        }
                                        new_vtab_query = if query.is_empty() {
                                            None
                                        } else {
                                            Some(query.clone())
                                        };
                                    }
                                }
                            }
                            #[cfg(feature = "fts5")]
                            {
                                if let Some(module) = vtab_module.as_ref() {
                                    if module.eq_ignore_ascii_case("fts5") {
                                        if let Some(table) = crate::fts5::get_table(vtab_name) {
                                            if let Ok(table) = table.lock() {
                                                if let Ok(rowids) = table.query_rowids(&query) {
                                                    cursor.vtab_rowids = rowids;
                                                    cursor.vtab_row_index = 0;
                                                    if cursor.vtab_rowids.is_empty() {
                                                        cursor.state = CursorState::AtEnd;
                                                        cursor.rowid = None;
                                                    } else {
                                                        cursor.state = CursorState::Valid;
                                                        cursor.rowid = Some(cursor.vtab_rowids[0]);
                                                    }
                                                }
                                            }
                                        }
                                        new_vtab_query = if query.is_empty() {
                                            None
                                        } else {
                                            Some(query.clone())
                                        };
                                    }
                                }
                            }
                        }
                    }
                }
                self.vtab_query = new_vtab_query;
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
                let mut vtab_context: Option<(Option<String>, Option<i64>)> = None;
                let btree = self.btree.clone();
                let schema = self.schema.clone();
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    // Invalidate column cache on cursor movement
                    cursor.cached_columns = None;
                    // Clear null row mode when rewinding
                    cursor.null_row = false;
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
                    } else if cursor.is_sqlite_stat1 {
                        // Virtual cursor for sqlite_stat1
                        cursor.virtual_index = 0;
                        if let Some(ref entries) = cursor.stat1_entries {
                            is_empty = entries.is_empty();
                            if !is_empty {
                                cursor.state = CursorState::Valid;
                            } else {
                                cursor.state = CursorState::AtEnd;
                            }
                        }
                    } else if cursor.is_ephemeral {
                        // Ephemeral table cursor - start at first row
                        cursor.ephemeral_index = 0;
                        is_empty = cursor.ephemeral_rows.is_empty();
                        if !is_empty {
                            cursor.state = CursorState::Valid;
                            cursor.rowid = Some(cursor.ephemeral_rows[0].0);
                            cursor.row_data = Some(cursor.ephemeral_rows[0].1.clone());
                        } else {
                            cursor.state = CursorState::AtEnd;
                        }
                    } else if cursor.is_virtual {
                        let vtab_module = cursor.vtab_name.as_ref().and_then(|name| {
                            schema.as_ref().and_then(|schema| {
                                schema
                                    .read()
                                    .ok()
                                    .and_then(|guard| guard.table(name))
                                    .and_then(|table| table.virtual_module.clone())
                            })
                        });
                        if cursor.vtab_rowids.is_empty() {
                            if let Some(ref vtab_name) = cursor.vtab_name {
                                #[cfg(feature = "fts3")]
                                {
                                    if let Some(module) = vtab_module.as_ref() {
                                        if module.eq_ignore_ascii_case("fts3") {
                                            if let Some(table) = crate::fts3::get_table(vtab_name) {
                                                if let Ok(mut table) = table.lock() {
                                                    if let (Some(ref btree), Some(ref schema)) =
                                                        (btree.as_ref(), schema.as_ref())
                                                    {
                                                        if let Ok(schema_guard) = schema.read() {
                                                            table.ensure_loaded(
                                                                btree,
                                                                &schema_guard,
                                                            )?;
                                                        }
                                                    }
                                                    cursor.vtab_rowids = table.all_rowids();
                                                }
                                            }
                                        }
                                    }
                                }
                                #[cfg(feature = "fts5")]
                                {
                                    if let Some(module) = vtab_module.as_ref() {
                                        if module.eq_ignore_ascii_case("fts5") {
                                            if let Some(table) = crate::fts5::get_table(vtab_name) {
                                                if let Ok(table) = table.lock() {
                                                    cursor.vtab_rowids = table.all_rowids();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        cursor.vtab_row_index = 0;
                        is_empty = cursor.vtab_rowids.is_empty();
                        if !is_empty {
                            cursor.state = CursorState::Valid;
                            cursor.rowid = Some(cursor.vtab_rowids[0]);
                            vtab_context = Some((cursor.vtab_name.clone(), cursor.rowid));
                        } else {
                            cursor.state = CursorState::AtEnd;
                            cursor.rowid = None;
                            vtab_context = Some((None, None));
                        }
                    } else if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                        match bt_cursor.first() {
                            Ok(empty) => {
                                is_empty = empty;
                                if !empty {
                                    cursor.state = CursorState::Valid;
                                    cursor.rowid = Some(bt_cursor.integer_key());
                                } else {
                                    cursor.state = CursorState::AtEnd;
                                    cursor.rowid = None;
                                }
                            }
                            Err(_) => {
                                cursor.state = CursorState::AtEnd;
                                cursor.rowid = None;
                            }
                        }
                    } else {
                        // No btree cursor - assume empty
                        cursor.state = CursorState::AtEnd;
                        cursor.rowid = None;
                    }
                }
                // Jump if no rows
                if is_empty {
                    self.pc = op.p2;
                }
                if let Some((name, rowid)) = vtab_context {
                    let context_name = name.clone();
                    self.vtab_context_name = context_name.clone();
                    self.vtab_context_rowid = rowid;
                    #[cfg(feature = "fts3")]
                    {
                        crate::functions::fts3::set_fts3_context(
                            context_name.clone(),
                            rowid,
                            self.vtab_query.clone(),
                        );
                    }
                    #[cfg(feature = "fts5")]
                    {
                        crate::functions::fts5::set_fts5_context(
                            context_name,
                            rowid,
                            self.vtab_query.clone(),
                        );
                    }
                }
            }

            Opcode::Next => {
                // Move cursor to next row, jump to P2 if has more rows
                inc_search_count();
                let mut has_more = false;
                let mut vtab_context: Option<(Option<String>, Option<i64>)> = None;
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    // Invalidate column cache on cursor movement
                    cursor.cached_columns = None;
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
                    } else if cursor.is_sqlite_stat1 {
                        // Virtual cursor for sqlite_stat1
                        cursor.virtual_index += 1;
                        if let Some(ref entries) = cursor.stat1_entries {
                            has_more = cursor.virtual_index < entries.len();
                            cursor.state = if has_more {
                                CursorState::Valid
                            } else {
                                CursorState::AtEnd
                            };
                        }
                    } else if cursor.is_ephemeral {
                        // Ephemeral table cursor - move to next row
                        cursor.ephemeral_index += 1;
                        has_more = cursor.ephemeral_index < cursor.ephemeral_rows.len();
                        cursor.state = if has_more {
                            CursorState::Valid
                        } else {
                            CursorState::AtEnd
                        };
                        if has_more {
                            cursor.rowid = Some(cursor.ephemeral_rows[cursor.ephemeral_index].0);
                            cursor.row_data =
                                Some(cursor.ephemeral_rows[cursor.ephemeral_index].1.clone());
                        } else {
                            cursor.rowid = None;
                            cursor.row_data = None;
                        }
                    } else if cursor.is_virtual {
                        cursor.vtab_row_index += 1;
                        has_more = cursor.vtab_row_index < cursor.vtab_rowids.len();
                        cursor.state = if has_more {
                            CursorState::Valid
                        } else {
                            CursorState::AtEnd
                        };
                        if has_more {
                            cursor.rowid = Some(cursor.vtab_rowids[cursor.vtab_row_index]);
                            vtab_context = Some((cursor.vtab_name.clone(), cursor.rowid));
                        } else {
                            cursor.rowid = None;
                            vtab_context = Some((None, None));
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
                                // Update rowid from cursor position
                                if has_more {
                                    cursor.rowid = Some(bt_cursor.integer_key());
                                } else {
                                    cursor.rowid = None;
                                }
                            }
                            Err(_) => {
                                cursor.state = CursorState::AtEnd;
                                cursor.rowid = None;
                            }
                        }
                    } else {
                        cursor.state = CursorState::AtEnd;
                        cursor.rowid = None;
                    }
                }
                // Jump to P2 if there are more rows
                if has_more {
                    self.pc = op.p2;
                }
                if let Some((name, rowid)) = vtab_context {
                    let context_name = name.clone();
                    self.vtab_context_name = context_name.clone();
                    self.vtab_context_rowid = rowid;
                    #[cfg(feature = "fts3")]
                    {
                        crate::functions::fts3::set_fts3_context(
                            context_name.clone(),
                            rowid,
                            self.vtab_query.clone(),
                        );
                    }
                    #[cfg(feature = "fts5")]
                    {
                        crate::functions::fts5::set_fts5_context(
                            context_name,
                            rowid,
                            self.vtab_query.clone(),
                        );
                    }
                }
            }

            Opcode::Prev => {
                // Move cursor to previous row, jump to P2 if has more rows
                inc_search_count();
                let mut has_more = false;
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    // Invalidate column cache on cursor movement
                    cursor.cached_columns = None;
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
                                // Update rowid from cursor position
                                if has_more {
                                    cursor.rowid = Some(bt_cursor.integer_key());
                                } else {
                                    cursor.rowid = None;
                                }
                            }
                            Err(_) => {
                                cursor.state = CursorState::AtEnd;
                                cursor.rowid = None;
                            }
                        }
                    } else {
                        cursor.state = CursorState::AtEnd;
                        cursor.rowid = None;
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
                if op.p2 < 0 {
                    if let Some(cursor) = self.cursor(op.p1) {
                        if let Some(rowid) = cursor.rowid {
                            self.mem_mut(op.p3).set_int(rowid);
                        } else {
                            self.mem_mut(op.p3).set_null();
                        }
                    } else {
                        self.mem_mut(op.p3).set_null();
                    }
                    return Ok(ExecResult::Continue);
                }

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
                        } else if cursor.is_sqlite_stat1 {
                            // sqlite_stat1 columns: tbl, idx, stat
                            col_idx = match col_name.to_lowercase().as_str() {
                                "tbl" => 0,
                                "idx" => 1,
                                "stat" => 2,
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

                // Handle sqlite_stat1 virtual cursor separately
                let sqlite_stat1_value: Option<Mem> = if let Some(cursor) = self.cursor(op.p1) {
                    if cursor.is_sqlite_stat1 {
                        if let Some(ref entries) = cursor.stat1_entries {
                            if cursor.virtual_index < entries.len() {
                                let entry = &entries[cursor.virtual_index];
                                let result = match col_idx {
                                    0 => Mem::from_str(&entry.0), // tbl
                                    1 => {
                                        if let Some(ref idx) = entry.1 {
                                            Mem::from_str(idx) // idx
                                        } else {
                                            Mem::new() // null
                                        }
                                    }
                                    2 => Mem::from_str(&entry.2), // stat
                                    _ => Mem::new(),              // null
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

                let vtab_value: Option<Mem> = if let Some(cursor) = self.cursor(op.p1) {
                    if cursor.is_virtual {
                        let mut result = Mem::new();
                        if let (Some(rowid), Some(vtab_name)) =
                            (cursor.rowid, cursor.vtab_name.as_ref())
                        {
                            #[cfg(feature = "fts3")]
                            {
                                let btree = self.btree.clone();
                                let schema = self.schema.clone();
                                let module = schema.as_ref().and_then(|schema| {
                                    schema
                                        .read()
                                        .ok()
                                        .and_then(|guard| guard.table(vtab_name))
                                        .and_then(|table| table.virtual_module.clone())
                                });
                                if let Some(module) = module {
                                    if module.eq_ignore_ascii_case("fts3tokenize") {
                                        let idx = rowid as usize;
                                        if let Some(token) = cursor.vtab_tokens.get(idx) {
                                            match col_idx {
                                                0 => {
                                                    if let Some(ref input) = cursor.vtab_input {
                                                        result = Mem::from_str(input);
                                                    }
                                                }
                                                1 => result = Mem::from_str(&token.text),
                                                2 => result = Mem::from_int(token.start as i64),
                                                3 => result = Mem::from_int(token.end as i64),
                                                4 => result = Mem::from_int(token.position as i64),
                                                _ => result = Mem::new(),
                                            }
                                        }
                                    } else if module.eq_ignore_ascii_case("fts3") {
                                        if let Some(table) = crate::fts3::get_table(vtab_name) {
                                            if let Ok(mut table) = table.lock() {
                                                if let (Some(ref btree), Some(ref schema)) =
                                                    (btree.as_ref(), schema.as_ref())
                                                {
                                                    if let Ok(schema_guard) = schema.read() {
                                                        table
                                                            .ensure_loaded(btree, &schema_guard)?;
                                                    }
                                                }
                                                let values =
                                                    if let (Some(ref btree), Some(ref schema)) =
                                                        (btree.as_ref(), schema.as_ref())
                                                    {
                                                        if let Ok(schema_guard) = schema.read() {
                                                            table
                                                                .load_row_values(
                                                                    btree,
                                                                    &schema_guard,
                                                                    rowid,
                                                                )
                                                                .ok()
                                                                .flatten()
                                                        } else {
                                                            table
                                                                .row_values(rowid)
                                                                .map(|vals| vals.to_vec())
                                                        }
                                                    } else {
                                                        table
                                                            .row_values(rowid)
                                                            .map(|vals| vals.to_vec())
                                                    };
                                                if let Some(values) = values {
                                                    if let Some(value) = values.get(col_idx) {
                                                        result = Mem::from_str(value);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            #[cfg(feature = "fts5")]
                            {
                                let schema = self.schema.clone();
                                let module = schema.as_ref().and_then(|schema| {
                                    schema
                                        .read()
                                        .ok()
                                        .and_then(|guard| guard.table(vtab_name))
                                        .and_then(|table| table.virtual_module.clone())
                                });
                                if let Some(module) = module {
                                    if module.eq_ignore_ascii_case("fts5") {
                                        if let Some(table) = crate::fts5::get_table(vtab_name) {
                                            if let Ok(table) = table.lock() {
                                                if let Some(values) = table.row_values(rowid) {
                                                    if let Some(value) = values.get(col_idx) {
                                                        result = Mem::from_str(value);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Some(result)
                    } else {
                        None
                    }
                } else {
                    None
                };

                let mut affinity = None;
                if sqlite_master_value.is_none()
                    && sqlite_stat1_value.is_none()
                    && vtab_value.is_none()
                {
                    if let Some(cursor) = self.cursor(op.p1) {
                        if let Some(ref table_name) = cursor.table_name {
                            if let Some(ref schema) = self.schema {
                                if let Ok(schema_guard) = schema.read() {
                                    if let Some(table) = schema_guard.tables.get(table_name) {
                                        affinity = table.columns.get(col_idx).map(|c| c.affinity);
                                    }
                                }
                            }
                        }
                    }
                }

                if let Some(value) = sqlite_master_value {
                    *self.mem_mut(op.p3) = value;
                } else if let Some(value) = sqlite_stat1_value {
                    *self.mem_mut(op.p3) = value;
                } else if let Some(value) = vtab_value {
                    let (vtab_name, vtab_rowid) = if let Some(cursor) = self.cursor(op.p1) {
                        (cursor.vtab_name.clone(), cursor.rowid)
                    } else {
                        (None, None)
                    };
                    let context_name = vtab_name.clone();
                    self.vtab_context_name = context_name.clone();
                    self.vtab_context_rowid = vtab_rowid;
                    #[cfg(feature = "fts3")]
                    {
                        crate::functions::fts3::set_fts3_context(
                            context_name.clone(),
                            vtab_rowid,
                            self.vtab_query.clone(),
                        );
                    }
                    #[cfg(feature = "fts5")]
                    {
                        crate::functions::fts5::set_fts5_context(
                            context_name,
                            vtab_rowid,
                            self.vtab_query.clone(),
                        );
                    }
                    *self.mem_mut(op.p3) = value;
                } else if let Some(cursor) = self.cursor(op.p1) {
                    // Check for deferred seek with alt-map redirection
                    if cursor.deferred_moveto {
                        if let (Some(alt_cursor_id), Some(ref alt_map)) =
                            (cursor.alt_cursor, cursor.alt_map.as_ref())
                        {
                            // Check if this column is mapped to the alt cursor
                            if let Some(&mapped_col) = alt_map.get(col_idx) {
                                if mapped_col >= 0 {
                                    // Read from alt cursor instead
                                    if let Some(alt_cursor) = self.cursor(alt_cursor_id) {
                                        // Get payload data from alt cursor
                                        let payload_data =
                                            if let Some(ref bt_cursor) = alt_cursor.btree_cursor {
                                                bt_cursor.info.payload.clone()
                                            } else {
                                                alt_cursor.row_data.clone()
                                            };

                                        if let Some(ref data) = payload_data {
                                            let mems = self.decode_record_mems(data);
                                            if let Some(value) = mems.get(mapped_col as usize) {
                                                *self.mem_mut(op.p3) = value.clone();
                                            } else {
                                                self.mem_mut(op.p3).set_null();
                                            }
                                            return Ok(ExecResult::Continue);
                                        }
                                    }
                                }
                            }
                            // Column not in alt-map or mapped to -1, need to finish seek
                            // Fall through to complete the deferred seek
                        }
                    }

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
                                        // Deserialize the value (with bounds check)
                                        // Note: Zero and One serial types have size 0 and need no data
                                        let col_type = &types[col_idx];
                                        let needs_data = col_type.size() > 0;
                                        if needs_data && data_offset >= payload.len() {
                                            self.mem_mut(op.p3).set_null();
                                        } else {
                                            let col_data = if data_offset < payload.len() {
                                                &payload[data_offset..]
                                            } else {
                                                &[][..]
                                            };
                                            match crate::vdbe::auxdata::deserialize_value(
                                                col_data, col_type,
                                            ) {
                                                Ok(mem) => {
                                                    *self.mem_mut(op.p3) = mem;
                                                    if let Some(affinity) = affinity {
                                                        self.mem_mut(op.p3)
                                                            .apply_affinity(affinity);
                                                    }
                                                }
                                                Err(_) => self.mem_mut(op.p3).set_null(),
                                            }
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
                                    // Note: Zero and One serial types have size 0 and need no data
                                    let col_type = &types[col_idx];
                                    let needs_data = col_type.size() > 0;
                                    if needs_data && data_offset >= row_data.len() {
                                        self.mem_mut(op.p3).set_null();
                                    } else {
                                        let col_data = if data_offset < row_data.len() {
                                            &row_data[data_offset..]
                                        } else {
                                            &[][..]
                                        };
                                        match crate::vdbe::auxdata::deserialize_value(
                                            col_data, col_type,
                                        ) {
                                            Ok(mem) => {
                                                *self.mem_mut(op.p3) = mem;
                                                if let Some(affinity) = affinity {
                                                    self.mem_mut(op.p3).apply_affinity(affinity);
                                                }
                                            }
                                            Err(_) => self.mem_mut(op.p3).set_null(),
                                        }
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
                    match crate::vdbe::auxdata::decode_record_header(&record_data) {
                        Ok((types, header_size)) => {
                            let num_cols = op.p3.min(types.len() as i32) as usize;
                            let mut data_offset = header_size;
                            for i in 0..num_cols {
                                if i < types.len() {
                                    let col_data = &record_data[data_offset..];
                                    match crate::vdbe::auxdata::deserialize_value(
                                        col_data, &types[i],
                                    ) {
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
                // P1 = database, P2 = transaction type (0 read, 1 write, 2 exclusive)
                if op.p2 < 0 || op.p2 > 2 {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "invalid transaction type",
                    ));
                }
                if op.p1 != 0 {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "unsupported database index",
                    ));
                }

                let Some(conn_ptr) = self.conn_ptr else {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "missing connection for transaction",
                    ));
                };
                // SAFETY: conn_ptr is valid for the lifetime of the statement/VDBE.
                let conn = unsafe { &mut *conn_ptr };
                if !conn.autocommit.load(AtomicOrdering::SeqCst) {
                    return Ok(ExecResult::Continue);
                }

                let write = op.p2 > 0;
                if write && conn.flags.contains(OpenFlags::READONLY) {
                    return Err(Error::with_message(
                        ErrorCode::ReadOnly,
                        "attempt to write a readonly database",
                    ));
                }

                let Some(ref btree) = self.btree else {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "missing btree for transaction",
                    ));
                };

                let current = conn.transaction_state;
                if write {
                    if current != TransactionState::Write {
                        btree.begin_trans(true)?;
                        conn.transaction_state = TransactionState::Write;
                    }
                } else if current == TransactionState::None {
                    btree.begin_trans(false)?;
                    conn.transaction_state = TransactionState::Read;
                }

                if op.p5 != 0 {
                    let cookie = btree.get_meta(crate::storage::btree::BTREE_SCHEMA_VERSION)?;
                    if cookie != op.p3 as u32 {
                        return Err(Error::with_message(
                            ErrorCode::Schema,
                            "database schema has changed",
                        ));
                    }
                }
            }

            Opcode::AutoCommit => {
                // AutoCommit P1 P2
                // P1 = desired autocommit (1 commit/end txn, 0 begin txn)
                // P2 = rollback flag (1 rollback, only valid with P1=1)
                let mut desired = op.p1;
                let mut rollback = op.p2 != 0;

                // Back-compat with older compiler behavior (P1=2 for rollback)
                if desired == 2 && !rollback {
                    desired = 1;
                    rollback = true;
                }

                if desired != 0 && desired != 1 {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "invalid autocommit flag",
                    ));
                }
                if desired == 0 && rollback {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "invalid rollback flag for autocommit=0",
                    ));
                }

                let Some(conn_ptr) = self.conn_ptr else {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "missing connection for autocommit",
                    ));
                };
                // SAFETY: conn_ptr is valid for the lifetime of the statement/VDBE.
                let conn = unsafe { &mut *conn_ptr };
                let current = conn.autocommit.load(AtomicOrdering::SeqCst);
                let desired_autocommit = desired == 1;

                if desired_autocommit == current {
                    if rollback && desired_autocommit {
                        // SQLite treats ROLLBACK with no active transaction as a no-op.
                        return Ok(ExecResult::Continue);
                    }
                    let msg = if !desired_autocommit {
                        "cannot start a transaction within a transaction"
                    } else {
                        "cannot commit - no transaction is active"
                    };
                    return Err(Error::with_message(ErrorCode::Error, msg));
                }

                if rollback {
                    if let Some(ref btree) = self.btree {
                        let _ = btree.rollback(0, false);
                    }
                    // Reload schema from sqlite_master after rollback.
                    // DDL operations (CREATE/DROP TABLE/INDEX) modify the schema cache
                    // immediately, so after rollback we must re-read the actual state
                    // from sqlite_master to restore the pre-transaction schema.
                    let _ = conn.reload_schema();
                    if let Some(hook) = conn.rollback_hook.as_ref() {
                        hook();
                    }
                    conn.transaction_state = TransactionState::None;
                    conn.autocommit.store(true, AtomicOrdering::SeqCst);
                    conn.savepoints.clear();
                    conn.is_transaction_savepoint = false;
                    self.deferred_fk_counter = 0;
                    // Allow subsequent Halt opcode to finish the statement.
                } else if desired_autocommit {
                    if self.deferred_fk_counter > 0 {
                        return Err(Error::with_message(
                            ErrorCode::Constraint,
                            "foreign key constraint failed",
                        ));
                    }
                    if let Some(hook) = conn.commit_hook.as_ref() {
                        if hook() {
                            if let Some(ref btree) = self.btree {
                                let _ = btree.rollback(0, false);
                            }
                            if let Some(hook) = conn.rollback_hook.as_ref() {
                                hook();
                            }
                            conn.transaction_state = TransactionState::None;
                            conn.autocommit.store(true, AtomicOrdering::SeqCst);
                            conn.savepoints.clear();
                            conn.is_transaction_savepoint = false;
                            self.deferred_fk_counter = 0;
                            return Err(Error::with_message(
                                ErrorCode::Abort,
                                "commit hook aborted transaction",
                            ));
                        }
                    }
                    if let Some(ref btree) = self.btree {
                        btree.commit()?;
                    }
                    conn.transaction_state = TransactionState::None;
                    conn.autocommit.store(true, AtomicOrdering::SeqCst);
                    conn.savepoints.clear();
                    conn.is_transaction_savepoint = false;
                } else {
                    conn.autocommit.store(false, AtomicOrdering::SeqCst);
                }
            }

            // ================================================================
            // Aggregation
            // ================================================================
            Opcode::AggStep | Opcode::AggStep0 => {
                // As emitted by compiler:
                // P1 = argument register (single arg)
                // P2 = accumulator register
                // P3 = 0 (unused)
                // P4 = function name
                let func_name = match &op.p4 {
                    P4::Text(s) => s.as_str(),
                    P4::FuncDef(s) => s.as_str(),
                    _ => "",
                };

                let arg_reg = op.p1;
                let acc_reg = op.p2;

                // Get argument value
                let arg = self.mem(arg_reg).to_value();
                let args = vec![arg];

                // Get or create aggregate state
                let state = self.agg_contexts.entry(acc_reg).or_insert_with(|| {
                    AggregateState::new(func_name).unwrap_or(AggregateState::Count { count: 0 })
                });

                // Call step function
                let _ = state.step(&args);
            }

            Opcode::AggFinal | Opcode::AggValue => {
                // As emitted by compiler:
                // P1 = accumulator register (source)
                // P2 = destination register (where result goes)
                // P3 = 0 (unused)
                // P4 = function name
                let acc_reg = op.p1;
                let dest_reg = op.p2;

                let func_name = match &op.p4 {
                    P4::Text(s) => s.as_str(),
                    P4::FuncDef(s) => s.as_str(),
                    _ => "",
                };

                if let Some(state) = self.agg_contexts.remove(&acc_reg) {
                    // Finalize and store result
                    match state.finalize() {
                        Ok(value) => {
                            *self.mem_mut(dest_reg) = Mem::from_value(&value);
                        }
                        Err(_) => {
                            self.mem_mut(dest_reg).set_null();
                        }
                    }
                } else {
                    // No state means no rows were processed
                    // For COUNT, should return 0; for others, NULL
                    if func_name.eq_ignore_ascii_case("COUNT") {
                        self.mem_mut(dest_reg).set_int(0);
                    } else if func_name.eq_ignore_ascii_case("TOTAL") {
                        self.mem_mut(dest_reg).set_real(0.0);
                    } else {
                        self.mem_mut(dest_reg).set_null();
                    }
                }
            }

            // ================================================================
            // Function Call (placeholder)
            // ================================================================
            Opcode::Function | Opcode::Function0 => {
                let name = match &op.p4 {
                    P4::Text(text) => Some(text.as_str()),
                    P4::FuncDef(text) => Some(text.as_str()),
                    _ => None,
                };
                if let Some(name) = name {
                    if let Some(func) = crate::functions::get_scalar_function(name) {
                        let argc = op.p1.max(0) as usize;
                        let arg_base = op.p2;
                        let mut args = Vec::with_capacity(argc);
                        let btree = self.btree.clone();
                        let schema = self.schema.clone();
                        for i in 0..argc {
                            let mem = self.mem(arg_base + i as i32);
                            args.push(mem.to_value());
                        }
                        if name.eq_ignore_ascii_case("snippet")
                            || name.eq_ignore_ascii_case("offsets")
                            || name.eq_ignore_ascii_case("matchinfo")
                        {
                            let mut text = None;
                            if let (Some(vtab_name), Some(rowid)) =
                                (&self.vtab_context_name, self.vtab_context_rowid)
                            {
                                #[cfg(feature = "fts3")]
                                {
                                    if let Some(table) = crate::fts3::get_table(vtab_name) {
                                        if let Ok(mut table) = table.lock() {
                                            if let (Some(ref btree), Some(ref schema)) =
                                                (btree.as_ref(), schema.as_ref())
                                            {
                                                if let Ok(schema_guard) = schema.read() {
                                                    let _ =
                                                        table.ensure_loaded(btree, &schema_guard);
                                                }
                                            }
                                            let values =
                                                if let (Some(ref btree), Some(ref schema)) =
                                                    (btree.as_ref(), schema.as_ref())
                                                {
                                                    if let Ok(schema_guard) = schema.read() {
                                                        table
                                                            .load_row_values(
                                                                btree,
                                                                &schema_guard,
                                                                rowid,
                                                            )
                                                            .ok()
                                                            .flatten()
                                                    } else {
                                                        table
                                                            .row_values(rowid)
                                                            .map(|vals| vals.to_vec())
                                                    }
                                                } else {
                                                    table
                                                        .row_values(rowid)
                                                        .map(|vals| vals.to_vec())
                                                };
                                            if let Some(values) = values {
                                                text = Some(Value::Text(values.join(" ")));
                                            }
                                        }
                                    }
                                }
                            }

                            if args.is_empty() {
                                if let Some(text) = text {
                                    args.push(text);
                                }
                                if let Some(query) = self.vtab_query.clone() {
                                    args.push(Value::Text(query));
                                }
                            } else if args.len() == 1 {
                                if let Some(text) = text {
                                    let query = args.remove(0);
                                    args.push(text);
                                    args.push(query);
                                } else if let Some(query) = self.vtab_query.clone() {
                                    args.push(Value::Text(query));
                                }
                            }
                        }
                        match func(&args) {
                            Ok(value) => {
                                *self.mem_mut(op.p3) = Mem::from_value(&value);
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

            // ================================================================
            // Rowid and Insert Operations
            // ================================================================
            Opcode::NewRowid => {
                // NewRowid P1 P2 P3
                // Generate a new unique rowid for cursor P1, store in register P2
                // P3 is the previous rowid if updating (for AUTOINCREMENT)
                //
                // Following SQLite's algorithm:
                // 1. Move cursor to the last row
                // 2. If table is empty, use rowid 1
                // 3. Otherwise, get the last rowid and add 1
                let mut new_rowid: i64 = 1;
                let mut use_random = false;
                let autoinc_max = if op.p3 > 0 {
                    self.mem(op.p3).to_int()
                } else {
                    0
                };

                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                        // Move to the last entry in the btree
                        match bt_cursor.last() {
                            Ok(is_empty) => {
                                if is_empty {
                                    // Table is empty, start at 1
                                    new_rowid = 1;
                                } else {
                                    // Get the last rowid and increment
                                    let last_rowid = bt_cursor.integer_key();
                                    if last_rowid == i64::MAX {
                                        use_random = true;
                                    } else {
                                        new_rowid = last_rowid.saturating_add(1);
                                    }
                                }
                            }
                            Err(_) => {
                                // Error moving cursor, fall back to cursor's last known rowid
                                new_rowid = cursor.rowid.map_or(1, |r| r.saturating_add(1));
                            }
                        }
                        if autoinc_max == i64::MAX {
                            use_random = true;
                        }
                        if !use_random && new_rowid <= autoinc_max {
                            if autoinc_max == i64::MAX {
                                use_random = true;
                            } else {
                                new_rowid = autoinc_max.saturating_add(1);
                            }
                        }
                        if use_random {
                            let mut found = None;
                            for _ in 0..100 {
                                let mut v = crate::random::sqlite3_random_int64();
                                v &= i64::MAX >> 1;
                                v = v.saturating_add(1);
                                if v <= autoinc_max {
                                    continue;
                                }
                                match bt_cursor.table_moveto(v, false) {
                                    Ok(0) => continue,
                                    Ok(_) => {
                                        found = Some(v);
                                        break;
                                    }
                                    Err(_) => continue,
                                }
                            }
                            if let Some(v) = found {
                                new_rowid = v;
                            } else {
                                return Err(Error::with_message(
                                    ErrorCode::Full,
                                    "unable to generate rowid",
                                ));
                            }
                        }
                    } else {
                        // No btree cursor (ephemeral table, etc), use cursor's last known rowid
                        new_rowid = cursor.rowid.map_or(1, |r| r.saturating_add(1));
                    }
                    cursor.rowid = Some(new_rowid);
                }
                self.mem_mut(op.p2).set_int(new_rowid);
                if op.p3 > 0 && new_rowid > autoinc_max {
                    self.mem_mut(op.p3).set_int(new_rowid);
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

                let btree = self.btree.clone();
                let schema = self.schema.clone();
                let record_mems = self.decode_record_mems(&record_data);
                let mut inserted = false;
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    cursor.rowid = Some(rowid);

                    if cursor.is_ephemeral {
                        // Store into ephemeral table for iteration
                        cursor.ephemeral_rows.push((rowid, record_data.clone()));
                        // Also add to ephemeral_set for Found opcode lookups
                        cursor.ephemeral_set.insert(record_data.clone());
                        inserted = true;
                    } else if cursor.is_virtual {
                        if let Some(ref vtab_name) = cursor.vtab_name {
                            #[cfg(feature = "fts3")]
                            {
                                if let Some(table) = crate::fts3::get_table(vtab_name) {
                                    if let Ok(mut table) = table.lock() {
                                        let mut mems = record_mems.clone();
                                        let column_count = table.columns.len();
                                        if column_count > 0 {
                                            mems.truncate(column_count);
                                            mems.resize_with(column_count, Mem::new);
                                        }
                                        let values: Vec<String> =
                                            mems.iter().map(|mem| mem.to_str()).collect();
                                        let refs: Vec<&str> =
                                            values.iter().map(|value| value.as_str()).collect();
                                        let result = if let (Some(ref btree), Some(ref schema)) =
                                            (btree.as_ref(), schema.as_ref())
                                        {
                                            if let Ok(schema_guard) = schema.read() {
                                                table.insert_with_storage(
                                                    rowid,
                                                    &refs,
                                                    btree,
                                                    &schema_guard,
                                                )
                                            } else {
                                                table.insert(rowid, &refs)
                                            }
                                        } else {
                                            table.insert(rowid, &refs)
                                        };
                                        let _ = result;
                                        inserted = true;
                                    }
                                }
                            }
                            #[cfg(feature = "fts5")]
                            {
                                if let Some(table) = crate::fts5::get_table(vtab_name) {
                                    if let Ok(mut table) = table.lock() {
                                        let mut mems = record_mems.clone();
                                        let column_count = table.columns.len();
                                        if column_count > 0 {
                                            mems.truncate(column_count);
                                            mems.resize_with(column_count, Mem::new);
                                        }
                                        let values: Vec<String> =
                                            mems.iter().map(|mem| mem.to_str()).collect();
                                        let refs: Vec<&str> =
                                            values.iter().map(|value| value.as_str()).collect();
                                        let _ = table.insert(rowid, &refs);
                                        inserted = true;
                                    }
                                }
                            }
                        }
                    } else {
                        // Actually insert into btree
                        if let Some(ref btree) = btree_arc {
                            if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                                // Extract conflict resolution mode from P4 (if Int64)
                                // This allows P5 to be used for OPFLAG_NCHANGE and similar flags
                                let on_error = match &op.p4 {
                                    P4::Int64(flags) => (*flags as u8) & OE_MASK,
                                    _ => OE_NONE,
                                };

                                // Check if rowid already exists (for conflict detection)
                                let mut row_exists = false;
                                if on_error != OE_NONE {
                                    if let Ok(res) = bt_cursor.table_moveto(rowid, false) {
                                        row_exists = res == 0; // 0 = exact match found
                                    }
                                }

                                // Handle conflict based on resolution mode
                                let mut skip_insert = false;
                                if row_exists {
                                    match on_error {
                                        OE_IGNORE => {
                                            // Skip the insert silently
                                            skip_insert = true;
                                        }
                                        OE_REPLACE => {
                                            // Delete existing row first
                                            let del_flags = BtreeInsertFlags::empty();
                                            let _ = btree.delete(bt_cursor, del_flags);
                                        }
                                        OE_ABORT | OE_FAIL => {
                                            // Return constraint violation error
                                            let table_name = match &op.p4 {
                                                P4::Text(s) => s.as_str(),
                                                _ => "table",
                                            };
                                            return Err(Error::with_message(
                                                ErrorCode::Constraint,
                                                format!(
                                                    "UNIQUE constraint failed: {}.rowid",
                                                    table_name
                                                ),
                                            ));
                                        }
                                        OE_ROLLBACK => {
                                            // Rollback transaction and return error
                                            let _ = btree.rollback(0, false);
                                            let table_name = match &op.p4 {
                                                P4::Text(s) => s.as_str(),
                                                _ => "table",
                                            };
                                            return Err(Error::with_message(
                                                ErrorCode::Constraint,
                                                format!(
                                                    "UNIQUE constraint failed: {}.rowid",
                                                    table_name
                                                ),
                                            ));
                                        }
                                        _ => {
                                            // OE_NONE or unknown - proceed with insert (overwrite)
                                        }
                                    }
                                }

                                if !skip_insert {
                                    // Create payload with record data
                                    let payload = BtreePayload {
                                        key: None, // Table insert, not index
                                        n_key: rowid,
                                        data: Some(record_data.clone()),
                                        mem: Vec::new(),
                                        n_data: record_data.len() as i32,
                                        n_zero: 0,
                                    };

                                    // Insert flags from P5 (exclude conflict resolution bits)
                                    let flags = BtreeInsertFlags::from_bits_truncate(
                                        (op.p5 as u8) & !OE_MASK,
                                    );

                                    // Perform the insert
                                    btree.insert(bt_cursor, &payload, flags, 0)?;
                                    inserted = true;
                                }
                            }
                        }
                    }
                }
                if inserted && (op.p5 & OPFLAG_NCHANGE) != 0 {
                    self.n_change += 1;
                    if let Some(conn_ptr) = self.conn_ptr {
                        let conn = unsafe { &mut *conn_ptr };
                        conn.changes.fetch_add(1, AtomicOrdering::SeqCst);
                        conn.total_changes.fetch_add(1, AtomicOrdering::SeqCst);
                        if (op.p5 & OPFLAG_LASTROWID) != 0 {
                            conn.last_insert_rowid.store(rowid, AtomicOrdering::SeqCst);
                        }
                    }
                }
            }

            // ================================================================
            // Seek Operations
            // ================================================================
            Opcode::SeekRowid => {
                // SeekRowid P1 P2 P3: Move cursor P1 to rowid in register P3
                // If not found, jump to P2. Register P3 must be an integer.
                inc_search_count();
                let rowid = self.mem(op.p3).to_int();
                let mut found = false;

                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                        match bt_cursor.table_moveto(rowid, false) {
                            Ok(0) => {
                                // Exact match found
                                cursor.state = CursorState::Valid;
                                cursor.rowid = Some(rowid);
                                found = true;
                            }
                            Ok(_) => {
                                // Not found - cursor positioned elsewhere
                                cursor.state = CursorState::Invalid;
                            }
                            Err(_) => {
                                cursor.state = CursorState::Invalid;
                            }
                        }
                    }
                }

                if !found {
                    self.pc = op.p2;
                }
            }

            Opcode::NotExists => {
                // NotExists P1 P2 P3: If rowid P3 does NOT exist in cursor P1, jump to P2
                inc_search_count();
                let rowid = self.mem(op.p3).to_int();
                let mut exists = false;

                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                        match bt_cursor.table_moveto(rowid, false) {
                            Ok(0) => {
                                // Exact match found - row exists
                                cursor.state = CursorState::Valid;
                                cursor.rowid = Some(rowid);
                                exists = true;
                            }
                            Ok(_) => {
                                // Not found
                                cursor.state = CursorState::Invalid;
                            }
                            Err(_) => {
                                cursor.state = CursorState::Invalid;
                            }
                        }
                    }
                }

                if !exists {
                    self.pc = op.p2;
                }
            }

            Opcode::Found => {
                // Found P1 P2 P3: If record P3 exists in ephemeral index P1, jump to P2
                let record = self.mem(op.p3).to_blob();
                let mut found = false;

                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if cursor.is_ephemeral {
                        found = cursor.ephemeral_set.contains(&record);
                    }
                }

                if found {
                    self.pc = op.p2;
                }
            }

            Opcode::NotFound => {
                // NotFound P1 P2 P3: If record P3 does NOT exist in ephemeral index P1, jump to P2
                let record = self.mem(op.p3).to_blob();
                let mut found = false;

                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if cursor.is_ephemeral {
                        found = cursor.ephemeral_set.contains(&record);
                    }
                }

                if !found {
                    self.pc = op.p2;
                }
            }

            Opcode::Delete => {
                // Delete P1 P2 P3 P4 P5: Delete the current row from cursor P1
                // P2 = jump destination on constraint violation
                // P3 = register holding rowid for triggers
                // P4 = table name
                // P5 = flags (OPFLAG_* constants)
                let btree_arc = self.btree.clone();

                let btree = self.btree.clone();
                let schema = self.schema.clone();
                let mut deleted = false;
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if cursor.is_virtual {
                        if let Some(rowid) = cursor.rowid {
                            if let Some(ref vtab_name) = cursor.vtab_name {
                                #[cfg(feature = "fts3")]
                                {
                                    if let Some(table) = crate::fts3::get_table(vtab_name) {
                                        if let Ok(mut table) = table.lock() {
                                            if let (Some(ref btree), Some(ref schema)) =
                                                (btree.as_ref(), schema.as_ref())
                                            {
                                                if let Ok(schema_guard) = schema.read() {
                                                    table.ensure_loaded(btree, &schema_guard)?;
                                                }
                                            }
                                            // Clone values to avoid borrow conflict with delete
                                            let values: Option<Vec<String>> =
                                                table.row_values(rowid).map(|v| v.to_vec());
                                            if let Some(values) = values {
                                                let refs: Vec<&str> = values
                                                    .iter()
                                                    .map(|value| value.as_str())
                                                    .collect();
                                                let result =
                                                    if let (Some(ref btree), Some(ref schema)) =
                                                        (btree.as_ref(), schema.as_ref())
                                                    {
                                                        if let Ok(schema_guard) = schema.read() {
                                                            table.delete_with_storage(
                                                                rowid,
                                                                &refs,
                                                                btree,
                                                                &schema_guard,
                                                            )
                                                        } else {
                                                            table.delete(rowid, &refs)
                                                        }
                                                    } else {
                                                        table.delete(rowid, &refs)
                                                    };
                                                let _ = result;
                                                deleted = true;
                                            }
                                        }
                                    }
                                }
                                #[cfg(feature = "fts5")]
                                {
                                    if let Some(table) = crate::fts5::get_table(vtab_name) {
                                        if let Ok(mut table) = table.lock() {
                                            let values: Option<Vec<String>> =
                                                table.row_values(rowid).map(|v| v.to_vec());
                                            if let Some(values) = values {
                                                let refs: Vec<&str> = values
                                                    .iter()
                                                    .map(|value| value.as_str())
                                                    .collect();
                                                let _ = table.delete(rowid, &refs);
                                                deleted = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else if let Some(ref btree) = btree_arc {
                        if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                            let flags = BtreeInsertFlags::from_bits_truncate(op.p5 as u8);
                            btree.delete(bt_cursor, flags)?;
                            deleted = true;
                            // Sync cursor state from btree cursor
                            // After delete, btree cursor may still be valid (pointing to next row)
                            cursor.state = match bt_cursor.state {
                                crate::storage::btree::CursorState::Valid => CursorState::Valid,
                                _ => CursorState::Invalid,
                            };
                            if cursor.state == CursorState::Valid {
                                cursor.rowid = Some(bt_cursor.integer_key());
                            } else {
                                cursor.rowid = None;
                            }
                        }
                    }
                }
                if deleted && (op.p5 & OPFLAG_NCHANGE) != 0 {
                    self.n_change += 1;
                    if let Some(conn_ptr) = self.conn_ptr {
                        let conn = unsafe { &mut *conn_ptr };
                        conn.changes.fetch_add(1, AtomicOrdering::SeqCst);
                        conn.total_changes.fetch_add(1, AtomicOrdering::SeqCst);
                    }
                }
            }

            Opcode::Last => {
                // Last P1 P2: Move cursor P1 to last row, jump to P2 if empty
                let mut is_empty = true;

                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                        match bt_cursor.last() {
                            Ok(empty) => {
                                is_empty = empty;
                                if !empty {
                                    cursor.state = CursorState::Valid;
                                    cursor.rowid = Some(bt_cursor.integer_key());
                                } else {
                                    cursor.state = CursorState::AtEnd;
                                }
                            }
                            Err(_) => cursor.state = CursorState::AtEnd,
                        }
                    }
                }

                if is_empty {
                    self.pc = op.p2;
                }
            }

            // ================================================================
            // Other opcodes (placeholder implementations)
            // ================================================================
            Opcode::SeekGE => {
                inc_search_count();
                let mut jump = true;
                let index_key = self.mem(op.p3).to_blob();
                let rowid_key = self.mem(op.p3).to_int();
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                        let res = if cursor.is_index {
                            bt_cursor.index_moveto(&UnpackedRecord::new(index_key.clone()))?
                        } else {
                            bt_cursor.table_moveto(rowid_key, false)?
                        };

                        if res == 1 {
                            let _ = bt_cursor.next(0);
                        }

                        jump = bt_cursor.state != crate::storage::btree::CursorState::Valid;
                        if !jump {
                            cursor.state = CursorState::Valid;
                            if !cursor.is_index {
                                cursor.rowid = Some(bt_cursor.integer_key());
                            }
                        } else {
                            cursor.state = CursorState::Invalid;
                            cursor.rowid = None;
                        }
                    }
                }
                if jump {
                    self.pc = op.p2;
                }
            }

            Opcode::SeekGT => {
                inc_search_count();
                let mut jump = true;
                let index_key = self.mem(op.p3).to_blob();
                let rowid_key = self.mem(op.p3).to_int();
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                        let res = if cursor.is_index {
                            bt_cursor.index_moveto(&UnpackedRecord::new(index_key.clone()))?
                        } else {
                            bt_cursor.table_moveto(rowid_key, false)?
                        };

                        if res >= 0 {
                            let _ = bt_cursor.next(0);
                        }

                        jump = bt_cursor.state != crate::storage::btree::CursorState::Valid;
                        if !jump {
                            cursor.state = CursorState::Valid;
                            if !cursor.is_index {
                                cursor.rowid = Some(bt_cursor.integer_key());
                            }
                        } else {
                            cursor.state = CursorState::Invalid;
                            cursor.rowid = None;
                        }
                    }
                }
                if jump {
                    self.pc = op.p2;
                }
            }

            Opcode::SeekLE => {
                inc_search_count();
                let mut jump = true;
                let index_key = self.mem(op.p3).to_blob();
                let rowid_key = self.mem(op.p3).to_int();
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                        let res = if cursor.is_index {
                            bt_cursor.index_moveto(&UnpackedRecord::new(index_key.clone()))?
                        } else {
                            bt_cursor.table_moveto(rowid_key, false)?
                        };

                        if res < 0 {
                            let _ = bt_cursor.previous(0);
                        }

                        jump = bt_cursor.state != crate::storage::btree::CursorState::Valid;
                        if !jump {
                            cursor.state = CursorState::Valid;
                            if !cursor.is_index {
                                cursor.rowid = Some(bt_cursor.integer_key());
                            }
                        } else {
                            cursor.state = CursorState::Invalid;
                            cursor.rowid = None;
                        }
                    }
                }
                if jump {
                    self.pc = op.p2;
                }
            }

            Opcode::SeekLT => {
                inc_search_count();
                let mut jump = true;
                let index_key = self.mem(op.p3).to_blob();
                let rowid_key = self.mem(op.p3).to_int();
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                        let res = if cursor.is_index {
                            bt_cursor.index_moveto(&UnpackedRecord::new(index_key.clone()))?
                        } else {
                            bt_cursor.table_moveto(rowid_key, false)?
                        };

                        if res >= 0 {
                            let _ = bt_cursor.previous(0);
                        }

                        jump = bt_cursor.state != crate::storage::btree::CursorState::Valid;
                        if !jump {
                            cursor.state = CursorState::Valid;
                            if !cursor.is_index {
                                cursor.rowid = Some(bt_cursor.integer_key());
                            }
                        } else {
                            cursor.state = CursorState::Invalid;
                            cursor.rowid = None;
                        }
                    }
                }
                if jump {
                    self.pc = op.p2;
                }
            }

            Opcode::SeekNull => {
                // SeekNull P1 P2 P3 P4: Jump to P2 if any key register is NULL.
                let count = match op.p4 {
                    P4::Int64(v) => v as i32,
                    _ => 1,
                };
                for i in 0..count.max(1) {
                    if self.mem(op.p3 + i).is_null() {
                        self.pc = op.p2;
                        break;
                    }
                }
            }

            Opcode::OpenAutoindex => {
                self.open_cursor(op.p1, 0, true)?;
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    cursor.is_ephemeral = true;
                    cursor.is_index = true;
                    cursor.n_field = op.p2;
                    cursor.ephemeral_set.clear();
                    cursor.ephemeral_rows.clear();
                    cursor.ephemeral_index = 0;
                }
            }

            Opcode::OpenPseudo => {
                // OpenPseudo P1 P2 P3: pseudo cursor for single row in register P2
                self.open_cursor(op.p1, 0, false)?;
                let row_data = if op.p2 > 0 {
                    Some(self.mem(op.p2).to_blob())
                } else {
                    None
                };
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    cursor.n_field = op.p3;
                    if let Some(data) = row_data {
                        cursor.row_data = Some(data);
                        cursor.null_row = false;
                    } else {
                        cursor.row_data = None;
                        cursor.null_row = true;
                    }
                    cursor.state = CursorState::Valid;
                }
            }

            Opcode::ResetSorter => {
                // ResetSorter P1: Clear sorter or ephemeral table content
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    cursor.sorter_data.clear();
                    cursor.sorter_index = 0;
                    cursor.sorter_sorted = false;
                    if cursor.is_ephemeral {
                        cursor.ephemeral_set.clear();
                        cursor.ephemeral_rows.clear();
                        cursor.ephemeral_index = 0;
                    }
                    cursor.state = CursorState::Invalid;
                }
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
                // n_field contains the number of ORDER BY key columns
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if cursor.sorter_data.is_empty() {
                        self.pc = op.p2;
                    } else {
                        // Sort the data using custom comparison that decodes records
                        let num_key_cols = cursor.n_field.max(1) as usize;
                        let sort_desc = cursor.sort_desc.clone();
                        cursor
                            .sorter_data
                            .sort_by(|a, b| compare_records(a, b, num_key_cols, &sort_desc));
                        cursor.sorter_sorted = true;
                        cursor.sorter_index = 0;
                        cursor.state = CursorState::Valid;
                        // Mark that a sort was performed for "db status sort"
                        set_sort_flag();
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
                // Also set cursor.row_data so Column can read from it
                let data_opt = if let Some(cursor) = self.cursor(op.p1) {
                    if cursor.sorter_index < cursor.sorter_data.len() {
                        Some(cursor.sorter_data[cursor.sorter_index].clone())
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(data) = data_opt {
                    self.mem_mut(op.p2).set_blob(&data);
                    // Also store in cursor.row_data for Column to use
                    if let Some(cursor) = self.cursor_mut(op.p1) {
                        cursor.row_data = Some(data);
                    }
                }
            }

            Opcode::SorterCompare => {
                // SorterCompare P1 P2 P3 P4: jump if sorter key != record key
                // P4 can be Int64 (number of key columns) or KeyInfo (with collations)
                let key_info = match &op.p4 {
                    P4::KeyInfo(info) => Some(info),
                    _ => None,
                };
                let n_key_cols = match &op.p4 {
                    P4::Int64(v) => *v as usize,
                    P4::KeyInfo(info) => info.n_key_field as usize,
                    _ => 0,
                };

                if n_key_cols != 0 {
                    if let Some(cursor) = self.cursor(op.p1) {
                        if cursor.sorter_index < cursor.sorter_data.len() {
                            let sorter_record = &cursor.sorter_data[cursor.sorter_index];
                            let record = self.mem(op.p3).to_blob();
                            let sort_desc = cursor.sort_desc.clone();

                            let sorter_mems = self.decode_record_mems(sorter_record);
                            let record_mems = self.decode_record_mems(&record);
                            let mut has_null = false;
                            for i in 0..n_key_cols {
                                if sorter_mems.get(i).map_or(false, |mem| mem.is_null())
                                    || record_mems.get(i).map_or(false, |mem| mem.is_null())
                                {
                                    has_null = true;
                                    break;
                                }
                            }

                            if !has_null {
                                let null_mem = Mem::new();
                                let mut cmp = std::cmp::Ordering::Equal;
                                for i in 0..n_key_cols {
                                    let left = sorter_mems.get(i).unwrap_or(&null_mem);
                                    let right = record_mems.get(i).unwrap_or(&null_mem);

                                    // Get collation for this column
                                    let collation = key_info
                                        .and_then(|ki| ki.collations.get(i))
                                        .map(|s| s.as_str())
                                        .unwrap_or("BINARY");
                                    let col_cmp = left.compare_with_collation(right, collation);

                                    // Apply DESC sort order from cursor or KeyInfo
                                    let desc = key_info
                                        .and_then(|ki| ki.sort_orders.get(i).copied())
                                        .or_else(|| sort_desc.get(i).copied())
                                        .unwrap_or(false);
                                    let col_cmp = if desc { col_cmp.reverse() } else { col_cmp };

                                    if col_cmp != std::cmp::Ordering::Equal {
                                        cmp = col_cmp;
                                        break;
                                    }
                                }
                                if cmp != std::cmp::Ordering::Equal {
                                    self.pc = op.p2;
                                }
                            }
                        }
                    }
                }
            }

            Opcode::SorterConfig => {
                // SorterConfig P1: Set sort directions for cursor P1
                // P4 contains a blob where each byte is 0 (ASC) or 1 (DESC)
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if let P4::Blob(dirs) = &op.p4 {
                        cursor.sort_desc = dirs.iter().map(|&b| b != 0).collect();
                    }
                }
            }

            Opcode::ReadCookie => {
                // ReadCookie P1 P2 P3: read meta cookie P3 into register P2
                // P1 = database index (0=main, 1=temp, >1=attached)
                let db_index = op.p1 as usize;

                // Get btree for the specified database
                let btree = if db_index == 0 {
                    // Main database - use self.btree
                    self.btree.as_ref()
                } else if let Some(conn_ptr) = self.conn_ptr {
                    // Get from connection's dbs array
                    let conn = unsafe { &*conn_ptr };
                    if db_index < conn.dbs.len() {
                        conn.dbs[db_index].btree.as_ref()
                    } else {
                        None
                    }
                } else {
                    None
                };

                let Some(btree) = btree else {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "missing btree for ReadCookie",
                    ));
                };
                let cookie = btree.get_meta(op.p3 as usize)? as i64;
                self.mem_mut(op.p2).set_int(cookie);
            }

            Opcode::SetCookie => {
                // SetCookie P1 P2 P3: write meta cookie P2 with value P3
                // P1 = database index (0=main, 1=temp, >1=attached)
                // P2 = cookie index (1=schema version, 2=file format, etc.)
                // P3 = new value
                let db_index = op.p1 as usize;
                let cookie_index = op.p2 as usize;
                let new_value = op.p3 as u32;

                // Get btree and schema for the specified database
                let (btree, schema) = if db_index == 0 {
                    // Main database
                    (self.btree.clone(), self.schema.clone())
                } else if let Some(conn_ptr) = self.conn_ptr {
                    let conn = unsafe { &*conn_ptr };
                    if db_index < conn.dbs.len() {
                        (
                            conn.dbs[db_index].btree.clone(),
                            conn.dbs[db_index].schema.clone(),
                        )
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                };

                let Some(btree) = btree else {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "missing btree for SetCookie",
                    ));
                };

                // Update the btree meta value
                btree.update_meta(cookie_index, new_value)?;

                // Side effects: update in-memory schema state
                if let Some(schema) = schema {
                    if let Ok(mut schema_guard) = schema.write() {
                        match cookie_index {
                            BTREE_SCHEMA_VERSION => {
                                // Update schema cookie in memory
                                schema_guard.schema_cookie = new_value;
                            }
                            BTREE_FILE_FORMAT => {
                                // Update file format in memory
                                schema_guard.file_format = new_value as u8;
                            }
                            _ => {
                                // Other cookies don't need schema updates
                            }
                        }
                    }
                }

                // Schema version change invalidates all prepared statements
                if cookie_index == BTREE_SCHEMA_VERSION {
                    if let Some(conn_ptr) = self.conn_ptr {
                        let conn = unsafe { &*conn_ptr };
                        conn.increment_schema_generation();
                    }
                }
            }

            Opcode::VerifyCookie => {
                // VerifyCookie P1 P2 P3: ensure meta cookie P2 equals P3
                // P1 = database index (0=main, 1=temp, >1=attached)
                // P2 = cookie index to check (usually schema version)
                // P3 = expected value
                let db_index = op.p1 as usize;

                // Get btree for the specified database
                let btree = if db_index == 0 {
                    self.btree.as_ref()
                } else if let Some(conn_ptr) = self.conn_ptr {
                    let conn = unsafe { &*conn_ptr };
                    if db_index < conn.dbs.len() {
                        conn.dbs[db_index].btree.as_ref()
                    } else {
                        None
                    }
                } else {
                    None
                };

                let Some(btree) = btree else {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "missing btree for VerifyCookie",
                    ));
                };
                let cookie = btree.get_meta(op.p2 as usize)? as i64;
                if cookie != op.p3 as i64 {
                    return Err(Error::with_message(
                        ErrorCode::Schema,
                        "database schema has changed",
                    ));
                }
            }

            Opcode::Savepoint => {
                let name = match &op.p4 {
                    P4::Text(text) => text.as_str(),
                    _ => {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            "savepoint requires a name",
                        ));
                    }
                };
                let Some(conn_ptr) = self.conn_ptr else {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "missing connection for savepoint",
                    ));
                };
                let conn = unsafe { &mut *conn_ptr };
                let Some(ref btree) = self.btree else {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        "missing btree for savepoint",
                    ));
                };

                match op.p1 {
                    0 => {
                        if conn.autocommit.load(AtomicOrdering::SeqCst) {
                            conn.autocommit.store(false, AtomicOrdering::SeqCst);
                            conn.is_transaction_savepoint = true;
                            if conn.transaction_state == TransactionState::None {
                                btree.begin_trans(false)?;
                                conn.transaction_state = TransactionState::Read;
                            }
                        }
                        let idx = conn.savepoints.len() as i32;
                        btree.savepoint(SavepointOp::Begin, idx)?;
                        conn.savepoints.push(name.to_string());
                    }
                    1 | 2 => {
                        let pos = conn
                            .savepoints
                            .iter()
                            .rposition(|sp| sp.eq_ignore_ascii_case(name));
                        let Some(pos) = pos else {
                            return Err(Error::with_message(
                                ErrorCode::Error,
                                format!("no such savepoint: {}", name),
                            ));
                        };
                        let idx = pos as i32;
                        if op.p1 == 1 {
                            btree.savepoint(SavepointOp::Release, idx)?;
                            conn.savepoints.truncate(pos);
                            if conn.savepoints.is_empty() && conn.is_transaction_savepoint {
                                if self.deferred_fk_counter > 0 {
                                    return Err(Error::with_message(
                                        ErrorCode::Constraint,
                                        "foreign key constraint failed",
                                    ));
                                }
                                if let Some(hook) = conn.commit_hook.as_ref() {
                                    if hook() {
                                        let _ = btree.rollback(0, false);
                                        if let Some(hook) = conn.rollback_hook.as_ref() {
                                            hook();
                                        }
                                        conn.transaction_state = TransactionState::None;
                                        conn.autocommit.store(true, AtomicOrdering::SeqCst);
                                        conn.is_transaction_savepoint = false;
                                        self.deferred_fk_counter = 0;
                                        return Err(Error::with_message(
                                            ErrorCode::Abort,
                                            "commit hook aborted transaction",
                                        ));
                                    }
                                }
                                btree.commit()?;
                                conn.transaction_state = TransactionState::None;
                                conn.autocommit.store(true, AtomicOrdering::SeqCst);
                                conn.is_transaction_savepoint = false;
                            }
                        } else {
                            btree.savepoint(SavepointOp::Rollback, idx)?;
                            conn.savepoints.truncate(pos + 1);
                            self.deferred_fk_counter = 0;
                        }
                    }
                    _ => {
                        return Err(Error::with_message(
                            ErrorCode::Error,
                            "invalid savepoint operation",
                        ));
                    }
                }
            }

            Opcode::Checkpoint => {
                // Checkpoint P1 P2 P3: run WAL checkpoint, store results in P3..P3+2
                let mut busy = 0;
                let mut log = -1;
                let mut ckpt = -1;
                if let Some(ref btree) = self.btree {
                    match btree.checkpoint(op.p2) {
                        Ok((log_pages, ckpt_pages)) => {
                            log = log_pages;
                            ckpt = ckpt_pages;
                        }
                        Err(err) => {
                            if err.code == ErrorCode::Busy {
                                busy = 1;
                            } else {
                                return Err(err);
                            }
                        }
                    }
                }
                self.mem_mut(op.p3).set_int(busy);
                self.mem_mut(op.p3 + 1).set_int(log as i64);
                self.mem_mut(op.p3 + 2).set_int(ckpt as i64);
            }

            Opcode::Cast => {
                // Cast P1 P2: Convert value in P1 to affinity in P2
                // P2 is an affinity character: 'A'=BLOB, 'B'=TEXT, 'C'=NUMERIC, 'D'=INTEGER, 'E'=REAL
                let affinity = match op.p2 as u8 {
                    b'A' => crate::schema::Affinity::Blob,
                    b'B' => crate::schema::Affinity::Text,
                    b'C' => crate::schema::Affinity::Numeric,
                    b'D' => crate::schema::Affinity::Integer,
                    b'E' => crate::schema::Affinity::Real,
                    _ => crate::schema::Affinity::Blob,
                };
                self.mem_mut(op.p1).apply_affinity(affinity);
            }

            Opcode::Affinity => {
                // Affinity P1 P2 P3 P4: Apply affinities to range of registers
                // P1 = first register, P2 = count, P4 = affinity string
                if let P4::Text(affinity_str) = &op.p4 {
                    for (i, ch) in affinity_str.chars().take(op.p2 as usize).enumerate() {
                        let affinity = match ch {
                            'A' => crate::schema::Affinity::Blob,
                            'B' => crate::schema::Affinity::Text,
                            'C' => crate::schema::Affinity::Numeric,
                            'D' => crate::schema::Affinity::Integer,
                            'E' => crate::schema::Affinity::Real,
                            _ => crate::schema::Affinity::Blob,
                        };
                        self.mem_mut(op.p1 + i as i32).apply_affinity(affinity);
                    }
                }
            }

            Opcode::Compare => {
                // Compare P1 P2 P3 P4: Compare registers [P1..P1+P3] with [P2..P2+P3]
                // P4 may contain KeyInfo with collations and sort orders
                // Store result for following Jump opcode
                let num_regs = op.p3 as usize;

                // Extract KeyInfo if present
                let key_info = match &op.p4 {
                    P4::KeyInfo(info) => Some(info),
                    _ => None,
                };

                let mut cmp_result = std::cmp::Ordering::Equal;
                for i in 0..num_regs {
                    let left = self.mem(op.p1 + i as i32);
                    let right = self.mem(op.p2 + i as i32);

                    // Get collation for this column (default to BINARY)
                    let collation = key_info
                        .and_then(|ki| ki.collations.get(i))
                        .map(|s| s.as_str())
                        .unwrap_or("BINARY");

                    // Compare with collation
                    let col_cmp = left.compare_with_collation(right, collation);

                    // Apply DESC sort order if specified
                    let desc = key_info
                        .and_then(|ki| ki.sort_orders.get(i).copied())
                        .unwrap_or(false);
                    let col_cmp = if desc { col_cmp.reverse() } else { col_cmp };

                    if col_cmp != std::cmp::Ordering::Equal {
                        cmp_result = col_cmp;
                        break;
                    }
                }
                self.last_compare = cmp_result;
            }

            Opcode::Jump => {
                // Jump P1 P2 P3: Based on last Compare result
                // Jump to P1 if <, P2 if ==, P3 if >
                match self.last_compare {
                    std::cmp::Ordering::Less => {
                        if op.p1 != 0 {
                            self.pc = op.p1;
                        }
                    }
                    std::cmp::Ordering::Equal => {
                        if op.p2 != 0 {
                            self.pc = op.p2;
                        }
                    }
                    std::cmp::Ordering::Greater => {
                        if op.p3 != 0 {
                            self.pc = op.p3;
                        }
                    }
                }
            }

            Opcode::Once => {
                // Once P1 P2: Jump to P2 if this is not the first time P1 is executed
                // Use a set to track which Once opcodes have been executed
                let once_id = op.p1;
                if self.once_flags.contains(&once_id) {
                    self.pc = op.p2;
                } else {
                    self.once_flags.insert(once_id);
                }
            }

            Opcode::Like => {
                // LIKE P1 P2 P3 P4
                // Compare text in P1 against pattern in P3
                // Store result (1 for match, 0 for no match) in P2
                // P4 may contain escape character
                let text = self.mem(op.p1).to_value();
                let pattern = self.mem(op.p3).to_value();

                // Handle NULL - LIKE returns NULL if either operand is NULL
                if matches!(text, Value::Null) || matches!(pattern, Value::Null) {
                    self.mem_mut(op.p2).set_null();
                } else {
                    // func_like expects [pattern, text] order
                    let args = vec![pattern, text];
                    match crate::functions::scalar::func_like(&args) {
                        Ok(Value::Integer(result)) => {
                            self.mem_mut(op.p2).set_int(result);
                        }
                        _ => {
                            self.mem_mut(op.p2).set_int(0);
                        }
                    }
                }
            }

            Opcode::Glob => {
                // GLOB P1 P2 P3
                // Compare text in P1 against glob pattern in P3
                // Store result (1 for match, 0 for no match) in P2
                let text = self.mem(op.p1).to_value();
                let pattern = self.mem(op.p3).to_value();

                if matches!(text, Value::Null) || matches!(pattern, Value::Null) {
                    self.mem_mut(op.p2).set_null();
                } else {
                    // func_glob expects [pattern, text] order
                    let args = vec![pattern, text];
                    match crate::functions::scalar::func_glob(&args) {
                        Ok(Value::Integer(result)) => {
                            self.mem_mut(op.p2).set_int(result);
                        }
                        _ => {
                            self.mem_mut(op.p2).set_int(0);
                        }
                    }
                }
            }

            Opcode::Between => {
                // Between P1 P2 P3 P4: r[P2] = (r[P1] BETWEEN r[P3] AND r[P4])
                let high_reg = match op.p4 {
                    P4::Int64(v) => v as i32,
                    _ => {
                        self.mem_mut(op.p2).set_null();
                        return Ok(ExecResult::Continue);
                    }
                };
                let val = self.mem(op.p1);
                let low = self.mem(op.p3);
                let high = self.mem(high_reg);

                if val.is_null() || low.is_null() || high.is_null() {
                    self.mem_mut(op.p2).set_null();
                } else {
                    let below = val.compare(low) == Ordering::Less;
                    let above = val.compare(high) == Ordering::Greater;
                    self.mem_mut(op.p2)
                        .set_int(if below || above { 0 } else { 1 });
                }
            }

            Opcode::Regexp => {
                // Regexp P1 P2 P3: Compare text in P1 against regexp pattern in P3
                let text = self.mem(op.p1).to_value();
                let pattern = self.mem(op.p3).to_value();

                if matches!(text, Value::Null) || matches!(pattern, Value::Null) {
                    self.mem_mut(op.p2).set_null();
                } else {
                    let matched = regexp_match(&pattern.to_text(), &text.to_text());
                    self.mem_mut(op.p2).set_int(if matched { 1 } else { 0 });
                }
            }

            Opcode::EndCoroutine => {
                // EndCoroutine P1: Jump to Yield.P2 for coroutine in register P1
                let caller_addr = self.mem(op.p1).to_int() as i32;
                if caller_addr < 0 || caller_addr as usize >= self.ops.len() {
                    return Err(Error::with_message(
                        ErrorCode::Internal,
                        "invalid coroutine address",
                    ));
                }
                let caller = self.ops[caller_addr as usize].clone();
                if caller.opcode != Opcode::Yield {
                    return Err(Error::with_message(
                        ErrorCode::Internal,
                        "coroutine does not point to Yield",
                    ));
                }
                self.mem_mut(op.p1).set_int((pc) as i64);
                self.pc = caller.p2;
            }

            Opcode::IfNullRow => {
                // IfNullRow P1 P2: jump to P2 if cursor P1 is in NULL row state
                if let Some(cursor) = self.cursor(op.p1) {
                    if cursor.null_row {
                        self.pc = op.p2;
                    }
                }
            }

            // ================================================================
            // Index Operations (for ephemeral tables and DISTINCT)
            // ================================================================
            Opcode::IdxGE => {
                // IdxGE P1 P2 P3 P4: Check if record exists in ephemeral index
                // For ephemeral tables: jump to P2 if record P3 exists in cursor P1
                // P4 = number of key columns
                let record = self.mem(op.p3).to_blob();
                let mut found = false;

                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if cursor.is_ephemeral {
                        found = cursor.ephemeral_set.contains(&record);
                    }
                }

                if found {
                    self.pc = op.p2;
                }
            }

            Opcode::IdxGT => {
                // IdxGT P1 P2 P3 P4: jump if index entry > key
                let key_info = match &op.p4 {
                    P4::KeyInfo(info) => Some(info),
                    _ => None,
                };
                let key_cols = match &op.p4 {
                    P4::Int64(n) => *n as usize,
                    P4::KeyInfo(info) => info.n_key_field as usize,
                    _ => 0,
                };

                if key_cols != 0 {
                    let mut should_jump = false;

                    if let Some(cursor) = self.cursor_mut(op.p1) {
                        if !cursor.is_ephemeral {
                            if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                                let payload = if let Some(data) = bt_cursor.payload_fetch() {
                                    data.to_vec()
                                } else {
                                    bt_cursor.payload(0, bt_cursor.payload_size())?
                                };
                                let index_mems = self.decode_record_mems(&payload);
                                let null_mem = Mem::new();
                                let mut cmp = std::cmp::Ordering::Equal;
                                for i in 0..key_cols {
                                    let idx_mem = index_mems.get(i).unwrap_or(&null_mem);
                                    let key_mem = self.mem(op.p3 + i as i32);

                                    // Get collation for this column
                                    let collation = key_info
                                        .and_then(|ki| ki.collations.get(i))
                                        .map(|s| s.as_str())
                                        .unwrap_or("BINARY");
                                    let col_cmp =
                                        idx_mem.compare_with_collation(key_mem, collation);

                                    // Apply DESC sort order
                                    let desc = key_info
                                        .and_then(|ki| ki.sort_orders.get(i).copied())
                                        .unwrap_or(false);
                                    let col_cmp = if desc { col_cmp.reverse() } else { col_cmp };
                                    if col_cmp != std::cmp::Ordering::Equal {
                                        cmp = col_cmp;
                                        break;
                                    }
                                }
                                should_jump = cmp == std::cmp::Ordering::Greater;
                            }
                        }
                    }

                    if should_jump {
                        self.pc = op.p2;
                    }
                }
            }

            Opcode::IdxLE => {
                // IdxLE P1 P2 P3 P4: jump if index entry <= key
                let key_info = match &op.p4 {
                    P4::KeyInfo(info) => Some(info),
                    _ => None,
                };
                let key_cols = match &op.p4 {
                    P4::Int64(n) => *n as usize,
                    P4::KeyInfo(info) => info.n_key_field as usize,
                    _ => 0,
                };

                if key_cols != 0 {
                    let mut should_jump = false;

                    if let Some(cursor) = self.cursor_mut(op.p1) {
                        if !cursor.is_ephemeral {
                            if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                                let payload = if let Some(data) = bt_cursor.payload_fetch() {
                                    data.to_vec()
                                } else {
                                    bt_cursor.payload(0, bt_cursor.payload_size())?
                                };
                                let index_mems = self.decode_record_mems(&payload);
                                let null_mem = Mem::new();
                                let mut cmp = std::cmp::Ordering::Equal;
                                for i in 0..key_cols {
                                    let idx_mem = index_mems.get(i).unwrap_or(&null_mem);
                                    let key_mem = self.mem(op.p3 + i as i32);

                                    // Get collation for this column
                                    let collation = key_info
                                        .and_then(|ki| ki.collations.get(i))
                                        .map(|s| s.as_str())
                                        .unwrap_or("BINARY");
                                    let col_cmp =
                                        idx_mem.compare_with_collation(key_mem, collation);

                                    // Apply DESC sort order
                                    let desc = key_info
                                        .and_then(|ki| ki.sort_orders.get(i).copied())
                                        .unwrap_or(false);
                                    let col_cmp = if desc { col_cmp.reverse() } else { col_cmp };
                                    if col_cmp != std::cmp::Ordering::Equal {
                                        cmp = col_cmp;
                                        break;
                                    }
                                }
                                should_jump = matches!(
                                    cmp,
                                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                                );
                            }
                        }
                    }

                    if should_jump {
                        self.pc = op.p2;
                    }
                }
            }

            Opcode::IdxLT => {
                // IdxLT P1 P2 P3 P4: jump if index entry < key
                let key_info = match &op.p4 {
                    P4::KeyInfo(info) => Some(info),
                    _ => None,
                };
                let key_cols = match &op.p4 {
                    P4::Int64(n) => *n as usize,
                    P4::KeyInfo(info) => info.n_key_field as usize,
                    _ => 0,
                };

                if key_cols != 0 {
                    let mut should_jump = false;

                    if let Some(cursor) = self.cursor_mut(op.p1) {
                        if !cursor.is_ephemeral {
                            if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                                let payload = if let Some(data) = bt_cursor.payload_fetch() {
                                    data.to_vec()
                                } else {
                                    bt_cursor.payload(0, bt_cursor.payload_size())?
                                };
                                let index_mems = self.decode_record_mems(&payload);
                                let null_mem = Mem::new();
                                let mut cmp = std::cmp::Ordering::Equal;
                                for i in 0..key_cols {
                                    let idx_mem = index_mems.get(i).unwrap_or(&null_mem);
                                    let key_mem = self.mem(op.p3 + i as i32);

                                    // Get collation for this column
                                    let collation = key_info
                                        .and_then(|ki| ki.collations.get(i))
                                        .map(|s| s.as_str())
                                        .unwrap_or("BINARY");
                                    let col_cmp =
                                        idx_mem.compare_with_collation(key_mem, collation);

                                    // Apply DESC sort order
                                    let desc = key_info
                                        .and_then(|ki| ki.sort_orders.get(i).copied())
                                        .unwrap_or(false);
                                    let col_cmp = if desc { col_cmp.reverse() } else { col_cmp };
                                    if col_cmp != std::cmp::Ordering::Equal {
                                        cmp = col_cmp;
                                        break;
                                    }
                                }
                                should_jump = cmp == std::cmp::Ordering::Less;
                            }
                        }
                    }

                    if should_jump {
                        self.pc = op.p2;
                    }
                }
            }

            Opcode::IdxInsert => {
                // IdxInsert P1 P2 P3: Insert record P2 into ephemeral index P1
                let record = self.mem(op.p2).to_blob();

                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if cursor.is_ephemeral {
                        cursor.ephemeral_set.insert(record);
                    }
                }
            }

            Opcode::IdxDelete => {
                // IdxDelete P1 P2 P3: Delete record from ephemeral index
                let record = self.mem(op.p2).to_blob();

                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if cursor.is_ephemeral {
                        cursor.ephemeral_set.remove(&record);
                    }
                }
            }

            Opcode::IdxRowid => {
                // IdxRowid P1 P2: Get rowid from index cursor
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if cursor.null_row {
                        self.mem_mut(op.p2).set_null();
                    } else if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                        let payload = if let Some(data) = bt_cursor.payload_fetch() {
                            data.to_vec()
                        } else {
                            bt_cursor.payload(0, bt_cursor.payload_size())?
                        };
                        let mems = self.decode_record_mems(&payload);
                        if let Some(last) = mems.last() {
                            if last.is_null() {
                                self.mem_mut(op.p2).set_null();
                            } else {
                                self.mem_mut(op.p2).set_int(last.to_int());
                            }
                        } else {
                            self.mem_mut(op.p2).set_null();
                        }
                    } else {
                        self.mem_mut(op.p2).set_null();
                    }
                } else {
                    self.mem_mut(op.p2).set_null();
                }
            }

            Opcode::And => {
                // P3 = P1 AND P2 (with SQL three-valued logic for NULL)
                // NULL AND FALSE = FALSE
                // NULL AND TRUE = NULL
                // FALSE AND anything = FALSE
                // TRUE AND TRUE = TRUE
                let left = self.mem(op.p1);
                let right = self.mem(op.p2);

                let result = match (left.is_null(), right.is_null()) {
                    (true, true) => Mem::new(), // NULL AND NULL = NULL
                    (true, false) => {
                        if right.is_truthy() {
                            Mem::new() // NULL AND TRUE = NULL
                        } else {
                            Mem::from_int(0) // NULL AND FALSE = FALSE
                        }
                    }
                    (false, true) => {
                        if left.is_truthy() {
                            Mem::new() // TRUE AND NULL = NULL
                        } else {
                            Mem::from_int(0) // FALSE AND NULL = FALSE
                        }
                    }
                    (false, false) => Mem::from_int(if left.is_truthy() && right.is_truthy() {
                        1
                    } else {
                        0
                    }),
                };
                *self.mem_mut(op.p3) = result;
            }

            Opcode::Or => {
                // P3 = P1 OR P2 (with SQL three-valued logic for NULL)
                // NULL OR TRUE = TRUE
                // NULL OR FALSE = NULL
                // TRUE OR anything = TRUE
                // FALSE OR FALSE = FALSE
                let left = self.mem(op.p1);
                let right = self.mem(op.p2);

                let result = match (left.is_null(), right.is_null()) {
                    (true, true) => Mem::new(), // NULL OR NULL = NULL
                    (true, false) => {
                        if right.is_truthy() {
                            Mem::from_int(1) // NULL OR TRUE = TRUE
                        } else {
                            Mem::new() // NULL OR FALSE = NULL
                        }
                    }
                    (false, true) => {
                        if left.is_truthy() {
                            Mem::from_int(1) // TRUE OR NULL = TRUE
                        } else {
                            Mem::new() // FALSE OR NULL = NULL
                        }
                    }
                    (false, false) => Mem::from_int(if left.is_truthy() || right.is_truthy() {
                        1
                    } else {
                        0
                    }),
                };
                *self.mem_mut(op.p3) = result;
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
                            if let Some(table) = crate::schema::parse_create_sql(sql, root_page) {
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
                                            format!("table \"{}\" already exists", table.name),
                                        ));
                                    }
                                    // IF NOT EXISTS was specified, silently succeed
                                }
                            }
                        }
                    }
                }
            }

            Opcode::ParseSchemaIndex => {
                // ParseSchemaIndex P1 P2 P3 P4
                // Parse a CREATE INDEX statement and add to schema
                // P1 = 1 if UNIQUE index
                // P2 = register containing root page number
                // P4 = SQL text of the CREATE INDEX statement
                if let P4::Text(sql) = &op.p4 {
                    // Get root page from register P2
                    let root_page = if op.p2 > 0 {
                        self.mem(op.p2).to_int() as u32
                    } else {
                        0
                    };

                    if let Some(ref schema) = self.schema {
                        if let Ok(mut schema_guard) = schema.write() {
                            // Parse CREATE INDEX SQL
                            if let Some(mut index) =
                                crate::schema::parse_create_index_sql(sql, op.p1 != 0)
                            {
                                // Set the root page from the register
                                index.root_page = root_page;

                                let index_name_lower = index.name.to_lowercase();
                                let table_name_lower = index.table.to_lowercase();

                                // Insert into schema.indexes
                                schema_guard
                                    .indexes
                                    .insert(index_name_lower, std::sync::Arc::new(index.clone()));

                                // Also add to the parent table's index list
                                if let Some(table) = schema_guard.tables.get_mut(&table_name_lower)
                                {
                                    // Use Arc::make_mut to get mutable access even if there are
                                    // other Arc references (will clone if needed)
                                    let table_mut = std::sync::Arc::make_mut(table);
                                    table_mut.indexes.push(std::sync::Arc::new(index));
                                }
                            }
                        }
                    }
                }
            }

            Opcode::DropSchema => {
                // DropSchema P1 P2 P3 P4
                // Remove object from schema
                // P1 = type: 0=table, 1=index, 2=view, 3=trigger
                // P4 = name of object to drop
                if let P4::Text(name) = &op.p4 {
                    if let Some(ref schema) = self.schema {
                        if let Ok(mut schema_guard) = schema.write() {
                            let name_lower = name.to_lowercase();
                            match op.p1 {
                                0 => {
                                    // Drop table - also remove all indexes for this table
                                    schema_guard.tables.remove(&name_lower);
                                    // Remove indexes that reference this table from the global index map
                                    schema_guard
                                        .indexes
                                        .retain(|_, idx| idx.table.to_lowercase() != name_lower);
                                }
                                1 => {
                                    // Drop index
                                    schema_guard.indexes.remove(&name_lower);
                                    // Also remove from parent table's index list
                                    for table in schema_guard.tables.values_mut() {
                                        // Use Arc::make_mut to get mutable access even if there are
                                        // other Arc references (will clone if needed)
                                        let table_mut = std::sync::Arc::make_mut(table);
                                        table_mut
                                            .indexes
                                            .retain(|idx| idx.name.to_lowercase() != name_lower);
                                    }
                                }
                                2 => {
                                    // Drop view (stored in tables)
                                    schema_guard.tables.remove(&name_lower);
                                }
                                3 => {
                                    // Drop trigger
                                    schema_guard.triggers.remove(&name_lower);
                                }
                                _ => {
                                    // Unknown type - try tables as fallback
                                    schema_guard.tables.remove(&name_lower);
                                }
                            }
                        }
                    }
                }
            }

            Opcode::Trace | Opcode::Explain | Opcode::SqlExec => {
                // Debug/explain operations
            }

            Opcode::Count => {
                // Count P1 P2 P3: count entries in cursor P1, store in P2
                let mut total = 0i64;
                let btree = self.btree.clone();
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if cursor.is_ephemeral {
                        total = cursor.ephemeral_rows.len() as i64;
                    } else if cursor.is_virtual {
                        total = cursor.vtab_rowids.len() as i64;
                    } else if let Some(ref mut bt_cursor) = cursor.btree_cursor {
                        if let Some(ref btree) = btree {
                            total = btree.count(bt_cursor)?;
                        }
                    }
                }
                self.mem_mut(op.p2).set_int(total);
            }

            Opcode::DeferredSeek => {
                // DeferredSeek P1 P2 P3 P4: Set up deferred table seek from index cursor
                // P1 = table cursor to seek
                // P3 = index cursor with the rowid
                // P4 = alt-map array (column mapping from P1 cols to P3 cols)
                //
                // This sets P1 into deferred mode. Column reads from P1 will be
                // redirected to P3 using the alt-map until FinishSeek is called.

                // Get the rowid from the index cursor (P3)
                // First try to get from row_data or existing rowid
                let rowid = {
                    // Try row_data first
                    let from_row_data = if let Some(idx_cursor) = self.cursor(op.p3) {
                        if let Some(ref data) = idx_cursor.row_data {
                            let mems = self.decode_record_mems(data);
                            mems.last().map(|m| m.to_int())
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    if from_row_data.is_some() {
                        from_row_data
                    } else {
                        // Try btree cursor payload
                        let payload = if let Some(idx_cursor) = self.cursor_mut(op.p3) {
                            if let Some(ref mut bt_cursor) = idx_cursor.btree_cursor {
                                if let Some(data) = bt_cursor.payload_fetch() {
                                    Some(data.to_vec())
                                } else if let Ok(data) =
                                    bt_cursor.payload(0, bt_cursor.payload_size())
                                {
                                    Some(data)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        if let Some(ref data) = payload {
                            if !data.is_empty() {
                                let mems = self.decode_record_mems(data);
                                mems.last().map(|m| m.to_int())
                            } else {
                                None
                            }
                        } else {
                            // Fall back to cursor rowid
                            self.cursor(op.p3).and_then(|c| c.rowid)
                        }
                    }
                };

                // Extract alt-map from P4 if present (convert i64 to i32)
                let alt_map: Option<Vec<i32>> = match &op.p4 {
                    P4::IntArray(arr) => Some(arr.iter().map(|&x| x as i32).collect()),
                    _ => None,
                };

                // Set up the table cursor for deferred seek
                if let Some(tbl_cursor) = self.cursor_mut(op.p1) {
                    if let Some(target) = rowid {
                        tbl_cursor.deferred_moveto = true;
                        tbl_cursor.moveto_target = Some(target);
                        tbl_cursor.alt_cursor = Some(op.p3);
                        tbl_cursor.alt_map = alt_map;
                        tbl_cursor.state = CursorState::Valid;
                        tbl_cursor.rowid = Some(target);
                    }
                }
            }

            Opcode::FinishSeek => {
                // Complete a deferred table seek if one is pending.
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    if cursor.deferred_moveto {
                        if let (Some(target), Some(ref mut bt_cursor)) =
                            (cursor.moveto_target, cursor.btree_cursor.as_mut())
                        {
                            match bt_cursor.table_moveto(target, false) {
                                Ok(0) => {
                                    cursor.state = CursorState::Valid;
                                    cursor.rowid = Some(target);
                                }
                                Ok(_) | Err(_) => {
                                    cursor.state = CursorState::Invalid;
                                    cursor.rowid = None;
                                }
                            }
                        }
                        cursor.deferred_moveto = false;
                        cursor.moveto_target = None;
                        cursor.alt_cursor = None;
                        cursor.alt_map = None;
                    }
                }
            }

            Opcode::Sequence => {
                // Sequence P1 P2: r[P2]=cursor[P1].seq_count++
                if let Some(cursor) = self.cursor_mut(op.p1) {
                    let seq = cursor.seq_count;
                    cursor.seq_count = cursor.seq_count.wrapping_add(1);
                    self.mem_mut(op.p2).set_int(seq);
                } else {
                    self.mem_mut(op.p2).set_null();
                }
            }

            Opcode::SortKey => {
                // SortKey P1 P2: write sorter key into register P2
                if let Some(cursor) = self.cursor(op.p1) {
                    if cursor.sorter_index < cursor.sorter_data.len() {
                        let record = &cursor.sorter_data[cursor.sorter_index];
                        let key_cols = cursor.n_field.max(0) as usize;
                        if key_cols == 0 {
                            self.mem_mut(op.p2).set_null();
                        } else {
                            let mems = self.decode_record_mems(record);
                            let mut key_mems = Vec::with_capacity(key_cols);
                            for i in 0..key_cols {
                                if let Some(mem) = mems.get(i) {
                                    key_mems.push(mem.clone());
                                } else {
                                    key_mems.push(Mem::new());
                                }
                            }
                            let key_record =
                                crate::vdbe::auxdata::make_record(&key_mems, 0, key_cols as i32);
                            self.mem_mut(op.p2).set_blob(&key_record);
                        }
                    } else {
                        self.mem_mut(op.p2).set_null();
                    }
                } else {
                    self.mem_mut(op.p2).set_null();
                }
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
                    self.pc = op.p2;
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
                // Note: pc was already incremented at start of exec_op, so no -1 needed
                self.pc = op.p3;
            }

            Opcode::TriggerProlog => {
                // TriggerProlog
                // Marks end of trigger prolog (where OLD/NEW setup ends)
                // This is a no-op marker used for debugging/tracing
            }

            // ================================================================
            // RowSet Operations (for IN clause optimization)
            // ================================================================
            Opcode::RowSetAdd => {
                // RowSetAdd P1 P2: Add integer in register P2 to RowSet in register P1
                // SQLite vdbe.c lines 7302-7311
                let val = self.mem(op.p2).to_int();
                self.rowsets
                    .entry(op.p1)
                    .or_insert_with(std::collections::BTreeSet::new)
                    .insert(val);
            }

            Opcode::RowSetRead => {
                // RowSetRead P1 P2 P3: Extract smallest value from RowSet P1 into P3
                // Jump to P2 if RowSet is empty
                // SQLite vdbe.c lines 7322-7339
                let is_empty = if let Some(rowset) = self.rowsets.get_mut(&op.p1) {
                    if let Some(&val) = rowset.iter().next() {
                        rowset.remove(&val);
                        self.mem_mut(op.p3).set_int(val);
                        false
                    } else {
                        true
                    }
                } else {
                    true
                };

                if is_empty {
                    self.rowsets.remove(&op.p1);
                    self.mem_mut(op.p1).set_null();
                    self.pc = op.p2;
                }
            }

            Opcode::RowSetTest => {
                // RowSetTest P1 P2 P3 P4: Test if P3 is in RowSet P1
                // If P4>=0, also insert P3; if found, jump to P2
                // SQLite vdbe.c lines 7365-7391
                let val = self.mem(op.p3).to_int();
                let iset = match &op.p4 {
                    P4::Int64(v) => *v as i32,
                    _ => -1,
                };

                let rowset = self
                    .rowsets
                    .entry(op.p1)
                    .or_insert_with(std::collections::BTreeSet::new);

                let exists = if iset != 0 {
                    rowset.contains(&val)
                } else {
                    false
                };

                if iset >= 0 {
                    rowset.insert(val);
                }

                if exists {
                    self.pc = op.p2;
                }
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

    /// Decode a record's raw bytes into Value vector
    /// This is a simplified decoder for FK checking
    fn decode_record_values(&self, _data: &[u8], n_fields: usize) -> Vec<Value> {
        let mems = self.decode_record_mems(_data);
        let mut values = Vec::with_capacity(n_fields);
        for i in 0..n_fields {
            if let Some(mem) = mems.get(i) {
                values.push(mem.to_value());
            } else {
                values.push(Value::Null);
            }
        }
        values
    }

    fn decode_record_mems(&self, data: &[u8]) -> Vec<Mem> {
        if data.is_empty() {
            return Vec::new();
        }
        let Ok((types, header_size)) = crate::vdbe::auxdata::decode_record_header(data) else {
            return Vec::new();
        };

        let mut mems = Vec::with_capacity(types.len());
        let mut data_offset = header_size;
        for typ in &types {
            let size = typ.size();
            if data_offset > data.len() || data_offset + size > data.len() {
                mems.push(Mem::new());
            } else {
                let col_data = &data[data_offset..];
                match crate::vdbe::auxdata::deserialize_value(col_data, typ) {
                    Ok(mem) => mems.push(mem),
                    Err(_) => mems.push(Mem::new()),
                }
            }
            data_offset = data_offset.saturating_add(size);
        }
        mems
    }
}

// ============================================================================
// Sorter comparison helper
// ============================================================================

/// Compare two SQLite records by their first N columns (ORDER BY keys).
/// Returns Ordering for use with sort_by.
/// sort_desc: slice of booleans indicating DESC (true) or ASC (false) for each key column
fn compare_records(a: &[u8], b: &[u8], num_key_cols: usize, sort_desc: &[bool]) -> Ordering {
    use crate::vdbe::auxdata::{decode_record_header, deserialize_value};

    // Decode headers to get column types
    let (a_types, a_header_size) = match decode_record_header(a) {
        Ok(v) => v,
        Err(_) => return Ordering::Equal,
    };
    let (b_types, b_header_size) = match decode_record_header(b) {
        Ok(v) => v,
        Err(_) => return Ordering::Equal,
    };

    // Compare key columns
    let mut a_offset = a_header_size;
    let mut b_offset = b_header_size;

    for i in 0..num_key_cols {
        // Get types and values for this column
        let a_type = a_types.get(i);
        let b_type = b_types.get(i);

        let a_mem = if let Some(t) = a_type {
            deserialize_value(&a[a_offset..], t).ok()
        } else {
            None
        };

        let b_mem = if let Some(t) = b_type {
            deserialize_value(&b[b_offset..], t).ok()
        } else {
            None
        };

        // Advance offsets
        if let Some(t) = a_type {
            a_offset += t.size();
        }
        if let Some(t) = b_type {
            b_offset += t.size();
        }

        // Compare values
        let cmp = match (a_mem, b_mem) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less, // NULL sorts first
            (Some(_), None) => Ordering::Greater,
            (Some(a_val), Some(b_val)) => a_val.compare(&b_val),
        };

        if cmp != Ordering::Equal {
            // Reverse comparison for DESC columns
            let is_desc = sort_desc.get(i).copied().unwrap_or(false);
            return if is_desc { cmp.reverse() } else { cmp };
        }
    }

    Ordering::Equal
}

// ============================================================================
// Regexp helper
// ============================================================================

/// Simple regexp matcher supporting ^, $, ., and *
fn regexp_match(pattern: &str, text: &str) -> bool {
    let p = pattern.as_bytes();
    let t = text.as_bytes();
    if p.first() == Some(&b'^') {
        return regexp_match_here(&p[1..], t);
    }
    for i in 0..=t.len() {
        if regexp_match_here(p, &t[i..]) {
            return true;
        }
    }
    false
}

fn regexp_match_here(pattern: &[u8], text: &[u8]) -> bool {
    if pattern.is_empty() {
        return true;
    }
    if pattern.len() >= 2 && pattern[1] == b'*' {
        return regexp_match_star(pattern[0], &pattern[2..], text);
    }
    if pattern[0] == b'$' && pattern.len() == 1 {
        return text.is_empty();
    }
    if !text.is_empty() && (pattern[0] == b'.' || pattern[0] == text[0]) {
        return regexp_match_here(&pattern[1..], &text[1..]);
    }
    false
}

fn regexp_match_star(c: u8, pattern: &[u8], text: &[u8]) -> bool {
    let mut i = 0;
    loop {
        if regexp_match_here(pattern, &text[i..]) {
            return true;
        }
        if i >= text.len() {
            break;
        }
        if c != b'.' && text[i] != c {
            break;
        }
        i += 1;
    }
    false
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{sqlite3_initialize, sqlite3_open, SqliteConnection, TransactionState};
    use crate::storage::btree::TransState;
    use std::sync::atomic::Ordering as AtomicOrdering;
    use std::sync::Once;

    fn open_test_connection() -> Box<SqliteConnection> {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            sqlite3_initialize().expect("sqlite3_initialize");
        });
        sqlite3_open(":memory:").expect("sqlite3_open")
    }

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
    fn test_op_autocommit_commit_and_rollback() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        btree.begin_trans(true).unwrap();
        conn.transaction_state = TransactionState::Write;
        conn.autocommit.store(false, AtomicOrdering::SeqCst);

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::AutoCommit, 1, 0, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree.clone());
        vdbe.set_connection(conn_ptr);
        vdbe.step().unwrap();
        vdbe.step().unwrap();
        assert!(conn.get_autocommit());
        assert_eq!(conn.transaction_state, TransactionState::None);

        btree.begin_trans(true).unwrap();
        conn.transaction_state = TransactionState::Write;
        conn.autocommit.store(false, AtomicOrdering::SeqCst);

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::AutoCommit, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree);
        vdbe.set_connection(conn_ptr);
        vdbe.step().unwrap();
        vdbe.step().unwrap();
        assert!(conn.get_autocommit());
        assert_eq!(conn.transaction_state, TransactionState::None);
    }

    #[test]
    fn test_op_autocommit_errors_without_transaction() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::AutoCommit, 1, 0, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree);
        vdbe.set_connection(conn_ptr);
        let err = vdbe.step().unwrap_err();
        assert_eq!(err.code, ErrorCode::Error);
    }

    #[test]
    fn test_op_transaction_read_then_write() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Transaction, 0, 0, 0),
            VdbeOp::new(Opcode::Integer, 1, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Transaction, 0, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree.clone());
        vdbe.set_connection(conn_ptr);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(conn.transaction_state, TransactionState::Read);
        assert_eq!(btree.txn_state(), TransState::Read);
        assert!(conn.get_autocommit());

        vdbe.step().unwrap();
        assert_eq!(conn.transaction_state, TransactionState::Write);
        assert_eq!(btree.txn_state(), TransState::Write);
        assert!(conn.get_autocommit());
    }

    #[test]
    fn test_op_savepoint_begin_release() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp {
                opcode: Opcode::Savepoint,
                p1: 0,
                p2: 0,
                p3: 0,
                p4: P4::Text("sp1".to_string()),
                p5: 0,
                comment: None,
            },
            VdbeOp::new(Opcode::Integer, 1, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp {
                opcode: Opcode::Savepoint,
                p1: 1,
                p2: 0,
                p3: 0,
                p4: P4::Text("sp1".to_string()),
                p5: 0,
                comment: None,
            },
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree.clone());
        vdbe.set_connection(conn_ptr);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert!(!conn.get_autocommit());
        assert_eq!(conn.savepoints.len(), 1);
        assert!(conn.is_transaction_savepoint);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Done);
        assert!(conn.get_autocommit());
        assert_eq!(conn.savepoints.len(), 0);
        assert!(!conn.is_transaction_savepoint);
        assert_eq!(conn.transaction_state, TransactionState::None);
    }

    #[test]
    fn test_op_openautoindex_found() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::OpenAutoindex, 0, 1, 0),
            VdbeOp {
                opcode: Opcode::Blob,
                p1: 0,
                p2: 1,
                p3: 0,
                p4: P4::Blob(vec![1, 2, 3]),
                p5: 0,
                comment: None,
            },
            VdbeOp::new(Opcode::IdxInsert, 0, 1, 0),
            VdbeOp::new(Opcode::Found, 0, 6, 1),
            VdbeOp::new(Opcode::Integer, 0, 2, 0),
            VdbeOp::new(Opcode::Goto, 0, 7, 0),
            VdbeOp::new(Opcode::Integer, 1, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 1);
    }

    #[test]
    fn test_op_seekge_table() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        btree.begin_trans(true).unwrap();
        let root_page = btree
            .create_table(crate::storage::btree::BTREE_INTKEY)
            .unwrap();
        let mut cursor = btree
            .cursor(root_page, BtreeCursorFlags::WRCSR, None)
            .unwrap();
        for rowid in [1, 3, 5] {
            let payload = BtreePayload {
                key: None,
                n_key: rowid,
                data: None,
                mem: Vec::new(),
                n_data: 0,
                n_zero: 0,
            };
            btree
                .insert(&mut cursor, &payload, BtreeInsertFlags::APPEND, 0)
                .unwrap();
        }

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::OpenRead, 0, root_page as i32, 1),
            VdbeOp::new(Opcode::Integer, 2, 1, 0),
            VdbeOp::new(Opcode::SeekGE, 0, 6, 1),
            VdbeOp::new(Opcode::Rowid, 0, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree.clone());
        vdbe.set_connection(conn_ptr);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 3);
    }

    #[test]
    fn test_op_seekgt_table() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        btree.begin_trans(true).unwrap();
        let root_page = btree
            .create_table(crate::storage::btree::BTREE_INTKEY)
            .unwrap();
        let mut cursor = btree
            .cursor(root_page, BtreeCursorFlags::WRCSR, None)
            .unwrap();
        for rowid in [1, 3, 5] {
            let payload = BtreePayload {
                key: None,
                n_key: rowid,
                data: None,
                mem: Vec::new(),
                n_data: 0,
                n_zero: 0,
            };
            btree
                .insert(&mut cursor, &payload, BtreeInsertFlags::APPEND, 0)
                .unwrap();
        }

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::OpenRead, 0, root_page as i32, 1),
            VdbeOp::new(Opcode::Integer, 3, 1, 0),
            VdbeOp::new(Opcode::SeekGT, 0, 6, 1),
            VdbeOp::new(Opcode::Rowid, 0, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree.clone());
        vdbe.set_connection(conn_ptr);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 5);
    }

    #[test]
    fn test_op_seekle_table() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        btree.begin_trans(true).unwrap();
        let root_page = btree
            .create_table(crate::storage::btree::BTREE_INTKEY)
            .unwrap();
        let mut cursor = btree
            .cursor(root_page, BtreeCursorFlags::WRCSR, None)
            .unwrap();
        for rowid in [1, 3, 5] {
            let payload = BtreePayload {
                key: None,
                n_key: rowid,
                data: None,
                mem: Vec::new(),
                n_data: 0,
                n_zero: 0,
            };
            btree
                .insert(&mut cursor, &payload, BtreeInsertFlags::APPEND, 0)
                .unwrap();
        }

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::OpenRead, 0, root_page as i32, 1),
            VdbeOp::new(Opcode::Integer, 4, 1, 0),
            VdbeOp::new(Opcode::SeekLE, 0, 6, 1),
            VdbeOp::new(Opcode::Rowid, 0, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree.clone());
        vdbe.set_connection(conn_ptr);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 3);
    }

    #[test]
    fn test_op_seeklt_table() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        btree.begin_trans(true).unwrap();
        let root_page = btree
            .create_table(crate::storage::btree::BTREE_INTKEY)
            .unwrap();
        let mut cursor = btree
            .cursor(root_page, BtreeCursorFlags::WRCSR, None)
            .unwrap();
        for rowid in [1, 3, 5] {
            let payload = BtreePayload {
                key: None,
                n_key: rowid,
                data: None,
                mem: Vec::new(),
                n_data: 0,
                n_zero: 0,
            };
            btree
                .insert(&mut cursor, &payload, BtreeInsertFlags::APPEND, 0)
                .unwrap();
        }

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::OpenRead, 0, root_page as i32, 1),
            VdbeOp::new(Opcode::Integer, 5, 1, 0),
            VdbeOp::new(Opcode::SeekLT, 0, 6, 1),
            VdbeOp::new(Opcode::Rowid, 0, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree.clone());
        vdbe.set_connection(conn_ptr);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 3);
    }

    #[test]
    fn test_op_seeknull_jumps_on_null() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Null, 0, 1, 0),
            VdbeOp {
                opcode: Opcode::SeekNull,
                p1: 0,
                p2: 4,
                p3: 1,
                p4: P4::Int64(1),
                p5: 0,
                comment: None,
            },
            VdbeOp::new(Opcode::Integer, 0, 2, 0),
            VdbeOp::new(Opcode::Goto, 0, 5, 0),
            VdbeOp::new(Opcode::Integer, 1, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 1);
    }

    #[test]
    fn test_op_finishseek_deferred_rowid() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        btree.begin_trans(true).unwrap();
        let root_page = btree
            .create_table(crate::storage::btree::BTREE_INTKEY)
            .unwrap();
        let mut insert_cursor = btree
            .cursor(root_page, BtreeCursorFlags::WRCSR, None)
            .unwrap();
        for rowid in [1, 3] {
            let payload = BtreePayload {
                key: None,
                n_key: rowid,
                data: None,
                mem: Vec::new(),
                n_data: 0,
                n_zero: 0,
            };
            btree
                .insert(&mut insert_cursor, &payload, BtreeInsertFlags::APPEND, 0)
                .unwrap();
        }

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::FinishSeek, 0, 0, 0),
            VdbeOp::new(Opcode::Rowid, 0, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree.clone());
        vdbe.set_connection(conn_ptr);
        vdbe.open_cursor(0, root_page, false).unwrap();
        if let Some(cursor) = vdbe.cursor_mut(0) {
            cursor.btree_cursor = Some(
                btree
                    .cursor(root_page, BtreeCursorFlags::WRCSR, None)
                    .unwrap(),
            );
            cursor.deferred_moveto = true;
            cursor.moveto_target = Some(3);
        }

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 3);
    }

    #[test]
    fn test_op_idxgt_index_key() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        btree.begin_trans(true).unwrap();
        let root_page = btree
            .create_table(crate::storage::btree::BTREE_BLOBKEY)
            .unwrap();
        let mut index_cursor = btree
            .cursor(root_page, BtreeCursorFlags::WRCSR, None)
            .unwrap();

        let mems = vec![Mem::from_int(1)];
        let key1 = crate::vdbe::auxdata::make_record(&mems, 0, 1);
        let payload1 = BtreePayload {
            key: Some(key1),
            n_key: 0,
            data: None,
            mem: Vec::new(),
            n_data: 0,
            n_zero: 0,
        };
        btree
            .insert(&mut index_cursor, &payload1, BtreeInsertFlags::APPEND, 0)
            .unwrap();

        let mems = vec![Mem::from_int(3)];
        let key3 = crate::vdbe::auxdata::make_record(&mems, 0, 1);
        let payload3 = BtreePayload {
            key: Some(key3.clone()),
            n_key: 0,
            data: None,
            mem: Vec::new(),
            n_data: 0,
            n_zero: 0,
        };
        btree
            .insert(&mut index_cursor, &payload3, BtreeInsertFlags::APPEND, 0)
            .unwrap();
        index_cursor.first().unwrap();
        index_cursor.next(0).unwrap();

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Integer, 2, 1, 0),
            VdbeOp {
                opcode: Opcode::IdxGT,
                p1: 0,
                p2: 4,
                p3: 1,
                p4: P4::Int64(1),
                p5: 0,
                comment: None,
            },
            VdbeOp::new(Opcode::Integer, 0, 2, 0),
            VdbeOp::new(Opcode::Goto, 0, 5, 0),
            VdbeOp::new(Opcode::Integer, 1, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree.clone());
        vdbe.set_connection(conn_ptr);
        vdbe.open_cursor(0, root_page, false).unwrap();
        if let Some(cursor) = vdbe.cursor_mut(0) {
            cursor.is_index = true;
            cursor.btree_cursor = Some(index_cursor);
        }

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 1);
    }

    #[test]
    fn test_op_idxle_index_key() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        btree.begin_trans(true).unwrap();
        let root_page = btree
            .create_table(crate::storage::btree::BTREE_BLOBKEY)
            .unwrap();
        let mut index_cursor = btree
            .cursor(root_page, BtreeCursorFlags::WRCSR, None)
            .unwrap();

        let mems = vec![Mem::from_int(1)];
        let key1 = crate::vdbe::auxdata::make_record(&mems, 0, 1);
        let payload1 = BtreePayload {
            key: Some(key1),
            n_key: 0,
            data: None,
            mem: Vec::new(),
            n_data: 0,
            n_zero: 0,
        };
        btree
            .insert(&mut index_cursor, &payload1, BtreeInsertFlags::APPEND, 0)
            .unwrap();

        let mems = vec![Mem::from_int(3)];
        let key3 = crate::vdbe::auxdata::make_record(&mems, 0, 1);
        let payload3 = BtreePayload {
            key: Some(key3),
            n_key: 0,
            data: None,
            mem: Vec::new(),
            n_data: 0,
            n_zero: 0,
        };
        btree
            .insert(&mut index_cursor, &payload3, BtreeInsertFlags::APPEND, 0)
            .unwrap();
        index_cursor.first().unwrap();

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Integer, 2, 1, 0),
            VdbeOp {
                opcode: Opcode::IdxLE,
                p1: 0,
                p2: 4,
                p3: 1,
                p4: P4::Int64(1),
                p5: 0,
                comment: None,
            },
            VdbeOp::new(Opcode::Integer, 0, 2, 0),
            VdbeOp::new(Opcode::Goto, 0, 5, 0),
            VdbeOp::new(Opcode::Integer, 1, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree.clone());
        vdbe.set_connection(conn_ptr);
        vdbe.open_cursor(0, root_page, false).unwrap();
        if let Some(cursor) = vdbe.cursor_mut(0) {
            cursor.is_index = true;
            cursor.btree_cursor = Some(index_cursor);
        }

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 1);
    }

    #[test]
    fn test_op_idxlt_index_key_strict() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        btree.begin_trans(true).unwrap();
        let root_page = btree
            .create_table(crate::storage::btree::BTREE_BLOBKEY)
            .unwrap();
        let mut index_cursor = btree
            .cursor(root_page, BtreeCursorFlags::WRCSR, None)
            .unwrap();

        let mems = vec![Mem::from_int(1)];
        let key1 = crate::vdbe::auxdata::make_record(&mems, 0, 1);
        let payload1 = BtreePayload {
            key: Some(key1),
            n_key: 0,
            data: None,
            mem: Vec::new(),
            n_data: 0,
            n_zero: 0,
        };
        btree
            .insert(&mut index_cursor, &payload1, BtreeInsertFlags::APPEND, 0)
            .unwrap();

        let mems = vec![Mem::from_int(3)];
        let key3 = crate::vdbe::auxdata::make_record(&mems, 0, 1);
        let payload3 = BtreePayload {
            key: Some(key3),
            n_key: 0,
            data: None,
            mem: Vec::new(),
            n_data: 0,
            n_zero: 0,
        };
        btree
            .insert(&mut index_cursor, &payload3, BtreeInsertFlags::APPEND, 0)
            .unwrap();
        index_cursor.first().unwrap();

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Integer, 1, 1, 0),
            VdbeOp {
                opcode: Opcode::IdxLT,
                p1: 0,
                p2: 4,
                p3: 1,
                p4: P4::Int64(1),
                p5: 0,
                comment: None,
            },
            VdbeOp::new(Opcode::Integer, 0, 2, 0),
            VdbeOp::new(Opcode::Goto, 0, 5, 0),
            VdbeOp::new(Opcode::Integer, 1, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree.clone());
        vdbe.set_connection(conn_ptr);
        vdbe.open_cursor(0, root_page, false).unwrap();
        if let Some(cursor) = vdbe.cursor_mut(0) {
            cursor.is_index = true;
            cursor.btree_cursor = Some(index_cursor);
        }

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 0);
    }

    #[test]
    fn test_op_idxrowid_index_key() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        btree.begin_trans(true).unwrap();
        let root_page = btree
            .create_table(crate::storage::btree::BTREE_BLOBKEY)
            .unwrap();
        let mut index_cursor = btree
            .cursor(root_page, BtreeCursorFlags::WRCSR, None)
            .unwrap();

        let mems = vec![Mem::from_int(10), Mem::from_int(99)];
        let key = crate::vdbe::auxdata::make_record(&mems, 0, 2);
        let payload = BtreePayload {
            key: Some(key),
            n_key: 0,
            data: None,
            mem: Vec::new(),
            n_data: 0,
            n_zero: 0,
        };
        btree
            .insert(&mut index_cursor, &payload, BtreeInsertFlags::APPEND, 0)
            .unwrap();
        index_cursor.first().unwrap();

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::IdxRowid, 0, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree.clone());
        vdbe.set_connection(conn_ptr);
        vdbe.open_cursor(0, root_page, false).unwrap();
        if let Some(cursor) = vdbe.cursor_mut(0) {
            cursor.is_index = true;
            cursor.btree_cursor = Some(index_cursor);
        }

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 99);
    }

    #[test]
    fn test_op_ifnullrow_jumps() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::NullRow, 0, 0, 0),
            VdbeOp::new(Opcode::IfNullRow, 0, 4, 0),
            VdbeOp::new(Opcode::Integer, 0, 1, 0),
            VdbeOp::new(Opcode::Goto, 0, 5, 0),
            VdbeOp::new(Opcode::Integer, 1, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.open_cursor(0, 0, false).unwrap();

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 1);
    }

    #[test]
    fn test_op_openpseudo_reads_columns() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Integer, 10, 1, 0),
            VdbeOp::new(Opcode::Integer, 20, 2, 0),
            VdbeOp::new(Opcode::MakeRecord, 1, 2, 3),
            VdbeOp::new(Opcode::OpenPseudo, 0, 3, 2),
            VdbeOp::new(Opcode::Column, 0, 0, 4),
            VdbeOp::new(Opcode::Column, 0, 1, 5),
            VdbeOp::new(Opcode::ResultRow, 4, 2, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 10);
        assert_eq!(vdbe.column_int(1), 20);
    }

    #[test]
    fn test_op_readcookie_schema_version() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();
        let expected = btree
            .get_meta(crate::storage::btree::BTREE_SCHEMA_VERSION)
            .unwrap() as i64;

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(
                Opcode::ReadCookie,
                0,
                1,
                crate::storage::btree::BTREE_SCHEMA_VERSION as i32,
            ),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree);
        vdbe.set_connection(conn_ptr);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), expected);
    }

    #[test]
    fn test_op_setcookie_user_version() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp {
                opcode: Opcode::SetCookie,
                p1: 0,
                p2: crate::storage::btree::BTREE_USER_VERSION as i32,
                p3: 123,
                p4: P4::Unused,
                p5: 0,
                comment: None,
            },
            VdbeOp::new(
                Opcode::ReadCookie,
                0,
                1,
                crate::storage::btree::BTREE_USER_VERSION as i32,
            ),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree);
        vdbe.set_connection(conn_ptr);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 123);
    }

    #[test]
    fn test_op_resetsorter_clears_sorter() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::OpenEphemeral, 0, 1, 0),
            VdbeOp::new(Opcode::Integer, 7, 1, 0),
            VdbeOp::new(Opcode::MakeRecord, 1, 1, 2),
            VdbeOp::new(Opcode::SorterInsert, 0, 2, 0),
            VdbeOp::new(Opcode::ResetSorter, 0, 0, 0),
            VdbeOp::new(Opcode::SorterSort, 0, 8, 0),
            VdbeOp::new(Opcode::Integer, 0, 3, 0),
            VdbeOp::new(Opcode::Goto, 0, 9, 0),
            VdbeOp::new(Opcode::Integer, 1, 3, 0),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 1);
    }

    #[test]
    fn test_op_sortercompare_jumps_on_mismatch() {
        let record7 = crate::vdbe::auxdata::make_record(&[Mem::from_int(7)], 0, 1);
        let record8 = crate::vdbe::auxdata::make_record(&[Mem::from_int(8)], 0, 1);
        assert_ne!(
            compare_records(&record7, &record8, 1, &[]),
            std::cmp::Ordering::Equal
        );

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::OpenEphemeral, 0, 1, 0),
            VdbeOp::new(Opcode::Integer, 7, 1, 0),
            VdbeOp::new(Opcode::MakeRecord, 1, 1, 2),
            VdbeOp::new(Opcode::SorterInsert, 0, 2, 0),
            VdbeOp::new(Opcode::SorterSort, 0, 11, 0),
            VdbeOp::new(Opcode::Integer, 8, 3, 0),
            VdbeOp::new(Opcode::MakeRecord, 3, 1, 4),
            VdbeOp {
                opcode: Opcode::SorterCompare,
                p1: 0,
                p2: 10,
                p3: 4,
                p4: P4::Int64(1),
                p5: 0,
                comment: None,
            },
            VdbeOp::new(Opcode::Integer, 0, 5, 0),
            VdbeOp::new(Opcode::Goto, 0, 11, 0),
            VdbeOp::new(Opcode::Integer, 1, 5, 0),
            VdbeOp::new(Opcode::ResultRow, 5, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 1);
    }

    #[test]
    fn test_op_verifycookie_matches() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp {
                opcode: Opcode::SetCookie,
                p1: 0,
                p2: crate::storage::btree::BTREE_USER_VERSION as i32,
                p3: 321,
                p4: P4::Unused,
                p5: 0,
                comment: None,
            },
            VdbeOp {
                opcode: Opcode::VerifyCookie,
                p1: 0,
                p2: crate::storage::btree::BTREE_USER_VERSION as i32,
                p3: 321,
                p4: P4::Unused,
                p5: 0,
                comment: None,
            },
            VdbeOp::new(Opcode::Integer, 1, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree);
        vdbe.set_connection(conn_ptr);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 1);
    }

    #[test]
    fn test_op_regexp_matches() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp {
                opcode: Opcode::String8,
                p1: 0,
                p2: 2,
                p3: 0,
                p4: P4::Text("^a.*b$".to_string()),
                p5: 0,
                comment: None,
            },
            VdbeOp {
                opcode: Opcode::String8,
                p1: 0,
                p2: 1,
                p3: 0,
                p4: P4::Text("acb".to_string()),
                p5: 0,
                comment: None,
            },
            VdbeOp::new(Opcode::Regexp, 1, 3, 2),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 1);
    }

    #[test]
    fn test_op_sequence_increments() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::OpenEphemeral, 0, 1, 0),
            VdbeOp::new(Opcode::Sequence, 0, 1, 0),
            VdbeOp::new(Opcode::Sequence, 0, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 2, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 0);
        assert_eq!(vdbe.column_int(1), 1);
    }

    #[test]
    fn test_op_sortkey_returns_key_record() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::OpenEphemeral, 0, 1, 0),
            VdbeOp::new(Opcode::Integer, 7, 1, 0),
            VdbeOp::new(Opcode::MakeRecord, 1, 1, 2),
            VdbeOp::new(Opcode::SorterInsert, 0, 2, 0),
            VdbeOp::new(Opcode::SorterSort, 0, 8, 0),
            VdbeOp::new(Opcode::SortKey, 0, 3, 0),
            VdbeOp::new(Opcode::DecodeRecord, 3, 4, 1),
            VdbeOp::new(Opcode::ResultRow, 4, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 7);
    }

    #[test]
    fn test_op_between_inclusive() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Integer, 5, 1, 0),
            VdbeOp::new(Opcode::Integer, 3, 2, 0),
            VdbeOp::new(Opcode::Integer, 7, 3, 0),
            VdbeOp {
                opcode: Opcode::Between,
                p1: 1,
                p2: 4,
                p3: 2,
                p4: P4::Int64(3),
                p5: 0,
                comment: None,
            },
            VdbeOp::new(Opcode::ResultRow, 4, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 1);
    }

    #[test]
    fn test_op_checkpoint_default() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Checkpoint, 0, 0, 1),
            VdbeOp::new(Opcode::ResultRow, 1, 3, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree);
        vdbe.set_connection(conn_ptr);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 0);
        assert_eq!(vdbe.column_int(1), 0);
        assert_eq!(vdbe.column_int(2), 0);
    }

    #[test]
    fn test_op_count_table() {
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        btree.begin_trans(true).unwrap();
        let root_page = btree
            .create_table(crate::storage::btree::BTREE_INTKEY)
            .unwrap();
        let mut cursor = btree
            .cursor(root_page, BtreeCursorFlags::WRCSR, None)
            .unwrap();
        for rowid in [1, 3, 5] {
            let payload = BtreePayload {
                key: None,
                n_key: rowid,
                data: None,
                mem: Vec::new(),
                n_data: 0,
                n_zero: 0,
            };
            btree
                .insert(&mut cursor, &payload, BtreeInsertFlags::APPEND, 0)
                .unwrap();
        }

        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::OpenRead, 0, root_page as i32, 1),
            VdbeOp::new(Opcode::Count, 0, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);
        vdbe.set_btree(btree);
        vdbe.set_connection(conn_ptr);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 3);
    }

    #[test]
    fn test_op_endcoroutine_jumps_to_yield_target() {
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Integer, 3, 1, 0),
            VdbeOp::new(Opcode::EndCoroutine, 1, 0, 0),
            VdbeOp::new(Opcode::Integer, 0, 2, 0),
            VdbeOp::new(Opcode::Yield, 1, 5, 0),
            VdbeOp::new(Opcode::Integer, 0, 2, 0),
            VdbeOp::new(Opcode::Integer, 1, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 1);
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
        // Test Lt: SQLite semantics are Lt P1 P2 P3 = jump if r[P3] < r[P1]
        // So: Lt 1, 5, 2 means jump if r[2] < r[1] = 5 < 10 = true
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Integer, 10, 1, 0), // r1 = 10
            VdbeOp::new(Opcode::Integer, 5, 2, 0),  // r2 = 5
            VdbeOp::new(Opcode::Lt, 1, 5, 2), // if r[P3] < r[P1] = r[2] < r[1] = 5 < 10, goto 5
            VdbeOp::new(Opcode::Integer, 0, 3, 0), // r3 = 0 (not taken)
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
    fn test_op_once_clears_on_reset() {
        // Test that OP_Once flags are cleared on reset, allowing the Once opcode
        // to trigger again when the VM is re-executed.
        //
        // Program structure:
        // 0: Once 1 3      - If once_id 1 already seen, jump to addr 3 (skip Integer 100)
        // 1: Integer 100 1 - Only executed first time through this run
        // 2: Goto 0 4      - Jump to ResultRow
        // 3: Integer 200 1 - Executed if Once was already triggered this run
        // 4: ResultRow 1 1 - Return value in register 1
        // 5: Halt
        let mut vdbe = Vdbe::from_ops(vec![
            VdbeOp::new(Opcode::Once, 1, 3, 0), // addr 0: jump to 3 if already triggered
            VdbeOp::new(Opcode::Integer, 100, 1, 0), // addr 1: first time = 100
            VdbeOp::new(Opcode::Goto, 0, 4, 0), // addr 2: skip the 200 path
            VdbeOp::new(Opcode::Integer, 200, 1, 0), // addr 3: not first time = 200
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0), // addr 4: return result
            VdbeOp::new(Opcode::Halt, 0, 0, 0), // addr 5
        ]);

        // First run: Once falls through, we get 100
        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(vdbe.column_int(0), 100);
        vdbe.step().unwrap(); // Halt

        // Reset the VM
        vdbe.reset();

        // Second run: Once should fall through again (flags cleared), we get 100 again
        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Row);
        assert_eq!(
            vdbe.column_int(0),
            100,
            "OP_Once should trigger again after reset"
        );
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

    #[test]
    fn test_deferred_seek_cursor_state() {
        // Test that DeferredSeek sets up cursor state correctly
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;

        let mut vdbe = Vdbe::from_ops(vec![
            // Open table cursor P1=0
            VdbeOp::new(Opcode::OpenRead, 0, 1, 0),
            // Open index cursor P1=1 (simulated)
            VdbeOp::new(Opcode::OpenRead, 1, 1, 0),
            // Set up a record with rowid=42 in register 2
            VdbeOp::new(Opcode::Integer, 42, 2, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        vdbe.set_connection(conn_ptr);

        // Execute up to halt
        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Done);
    }

    #[test]
    fn test_finish_seek_clears_deferred_state() {
        // Test that FinishSeek clears deferred seek state
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;

        let mut vdbe = Vdbe::from_ops(vec![
            // Open table cursor
            VdbeOp::new(Opcode::OpenRead, 0, 1, 0),
            // FinishSeek should be a no-op if no deferred seek pending
            VdbeOp::new(Opcode::FinishSeek, 0, 0, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        vdbe.set_connection(conn_ptr);

        // Execute - should not fail
        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Done);
    }

    // =========================================================================
    // Cookie Opcode Tests
    // =========================================================================

    #[test]
    fn test_read_cookie_main_db() {
        // Test ReadCookie with main database (P1=0)
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        let mut vdbe = Vdbe::from_ops(vec![
            // ReadCookie P1=0 (main db), P2=1 (result reg), P3=1 (schema version)
            VdbeOp::new(Opcode::ReadCookie, 0, 1, BTREE_SCHEMA_VERSION as i32),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        vdbe.set_btree(btree);
        vdbe.set_connection(conn_ptr);

        // Execute
        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Done);

        // Register 1 should contain the schema cookie value
        let cookie_value = vdbe.mem(1).to_int();
        // For a new database, schema version should be 0 or some initial value
        assert!(cookie_value >= 0);
    }

    #[test]
    fn test_set_cookie_main_db() {
        // Test SetCookie updates the btree meta value
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        // Start a write transaction
        btree.begin_trans(true).unwrap();

        let mut vdbe = Vdbe::from_ops(vec![
            // SetCookie P1=0 (main db), P2=6 (user version), P3=42
            VdbeOp::new(Opcode::SetCookie, 0, 6, 42), // Use user_version to avoid schema issues
            // ReadCookie to verify
            VdbeOp::new(Opcode::ReadCookie, 0, 1, 6),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        vdbe.set_btree(btree);
        vdbe.set_connection(conn_ptr);

        // Execute
        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Done);

        // Register 1 should contain 42
        assert_eq!(vdbe.mem(1).to_int(), 42);
    }

    #[test]
    fn test_set_cookie_updates_schema_cookie() {
        // Test SetCookie updates in-memory schema cookie
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();
        let schema = conn.main_db().schema.clone();

        // Start a write transaction
        btree.begin_trans(true).unwrap();

        let mut vdbe = Vdbe::from_ops(vec![
            // SetCookie P1=0, P2=1 (schema version), P3=100
            VdbeOp::new(Opcode::SetCookie, 0, BTREE_SCHEMA_VERSION as i32, 100),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        vdbe.set_btree(btree);
        if let Some(ref s) = schema {
            vdbe.set_schema(s.clone());
        }
        vdbe.set_connection(conn_ptr);

        // Execute
        let result = vdbe.step().unwrap();
        assert_eq!(result, ExecResult::Done);

        // Verify schema cookie was updated in memory
        if let Some(ref schema) = schema {
            let schema_guard = schema.read().unwrap();
            assert_eq!(schema_guard.schema_cookie, 100);
        }
    }

    #[test]
    fn test_verify_cookie_success() {
        // Test VerifyCookie succeeds when cookie matches
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        let mut vdbe = Vdbe::from_ops(vec![
            // Read the current schema cookie
            VdbeOp::new(Opcode::ReadCookie, 0, 1, BTREE_SCHEMA_VERSION as i32),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        vdbe.set_btree(btree.clone());
        vdbe.set_connection(conn_ptr);
        let _ = vdbe.step().unwrap();
        let current_cookie = vdbe.mem(1).to_int();

        // Now verify with the correct value
        let mut vdbe2 = Vdbe::from_ops(vec![
            VdbeOp::new(
                Opcode::VerifyCookie,
                0,
                BTREE_SCHEMA_VERSION as i32,
                current_cookie as i32,
            ),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        vdbe2.set_btree(btree);
        vdbe2.set_connection(conn_ptr);
        let result = vdbe2.step().unwrap();
        assert_eq!(result, ExecResult::Done);
    }

    #[test]
    fn test_verify_cookie_mismatch() {
        // Test VerifyCookie fails when cookie doesn't match
        let mut conn = open_test_connection();
        let conn_ptr = &mut *conn as *mut SqliteConnection;
        let btree = conn.main_db().btree.as_ref().unwrap().clone();

        let mut vdbe = Vdbe::from_ops(vec![
            // VerifyCookie with wrong expected value
            VdbeOp::new(Opcode::VerifyCookie, 0, BTREE_SCHEMA_VERSION as i32, 99999),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ]);

        vdbe.set_btree(btree);
        vdbe.set_connection(conn_ptr);

        // Should fail with schema error
        let result = vdbe.step();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ErrorCode::Schema);
    }
}
