//! Opcode handlers for the VDBE engine
//!
//! Each opcode (or group of related opcodes) is implemented as a handler that
//! implements the `OpcodeHandler` trait. This makes the code more modular,
//! testable, and easier to verify against SQLite's behavior.
//!
//! ## Design Goals
//!
//! 1. **Testability**: Each handler can be unit tested in isolation
//! 2. **SQLite Parity**: Each handler should match SQLite's vdbe.c behavior exactly
//! 3. **Modularity**: Related opcodes are grouped together in modules
//!
//! ## Module Organization
//!
//! - `control_flow`: Init, Goto, Halt, If, IfNot, Gosub, Return, etc.
//! - `data_movement`: Null, Integer, Copy, Move, SCopy, etc.
//! - `comparison`: Eq, Ne, Lt, Le, Gt, Ge, etc.
//! - `arithmetic`: Add, Subtract, Multiply, Divide, etc.
//! - `cursor`: OpenRead, OpenWrite, Close, Rewind, Next, etc.
//! - `record`: MakeRecord, Column, etc.
//! - `seek`: SeekGE, SeekGT, SeekLE, SeekLT, SeekRowid, etc.
//! - `index`: IdxGE, IdxGT, IdxInsert, IdxDelete, etc.
//! - `aggregate`: AggStep, AggFinal, etc.
//! - `trigger`: Program, Param

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use crate::error::Result;
use crate::schema::Schema;
use crate::storage::btree::Btree;
use crate::vdbe::ops::{Opcode, VdbeOp};

use super::VdbeCursor;

pub mod arithmetic;
pub mod comparison;
pub mod control_flow;
pub mod data_movement;
pub mod logic;
pub mod misc;

/// Result of executing an opcode
#[derive(Debug, Clone, PartialEq)]
pub enum OpcodeResult {
    /// Continue to next instruction (pc += 1)
    Continue,
    /// Jump to specific instruction
    Jump(i32),
    /// Yield a row (for SELECT)
    Row,
    /// Halt execution
    Done,
}

/// Trait for opcode handlers
///
/// Each handler implements the execution logic for one or more opcodes.
/// The handler receives a mutable reference to the engine context and
/// the current operation, and returns the result of execution.
///
/// ## Example
///
/// ```ignore
/// pub struct GotoHandler;
///
/// impl OpcodeHandler for GotoHandler {
///     fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
///         // Goto P2: Jump unconditionally to instruction P2
///         Ok(OpcodeResult::Jump(op.p2))
///     }
/// }
/// ```
pub trait OpcodeHandler: Send + Sync {
    /// Execute the opcode
    ///
    /// # Arguments
    /// * `ctx` - Mutable reference to the opcode execution context
    /// * `op` - The operation to execute
    ///
    /// # Returns
    /// * `Ok(OpcodeResult)` - The result of execution
    /// * `Err(Error)` - If execution failed
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult>;

    /// Get the opcodes this handler supports
    fn opcodes(&self) -> &'static [Opcode];
}

/// Context for opcode execution
///
/// This provides access to the VDBE engine state needed by handlers,
/// without exposing the entire VdbeEngine struct.
pub struct OpcodeContext<'a> {
    /// Memory cells (registers)
    pub mem: &'a mut [crate::vdbe::engine::Mem],
    /// Program counter
    pub pc: &'a mut i32,
    /// Current database connection
    pub db: Option<&'a crate::SqliteConnection>,
    /// Cursors for table/index access
    pub cursors: &'a mut [Option<VdbeCursor>],
    /// B-tree for storage operations
    pub btree: Option<&'a Arc<Btree>>,
    /// Schema for lookups
    pub schema: Option<&'a Arc<RwLock<Schema>>>,
    /// Once flags (for OP_Once)
    pub once_flags: &'a mut HashSet<i32>,
    /// Row change counter
    pub n_change: &'a mut i64,
}

impl<'a> OpcodeContext<'a> {
    /// Read a memory cell
    pub fn mem(&self, reg: i32) -> &crate::vdbe::engine::Mem {
        &self.mem[reg as usize]
    }

    /// Write to a memory cell
    pub fn mem_mut(&mut self, reg: i32) -> &mut crate::vdbe::engine::Mem {
        &mut self.mem[reg as usize]
    }

    /// Get a cursor by ID
    pub fn cursor(&self, id: i32) -> Option<&VdbeCursor> {
        self.cursors.get(id as usize).and_then(|c| c.as_ref())
    }

    /// Get a mutable cursor by ID
    pub fn cursor_mut(&mut self, id: i32) -> Option<&mut VdbeCursor> {
        self.cursors.get_mut(id as usize).and_then(|c| c.as_mut())
    }

    /// Ensure cursor slot exists
    pub fn ensure_cursor(&mut self, id: i32) {
        // Note: This is a no-op if cursors is a slice
        // The actual resizing must happen in the engine before creating the context
        let _ = id;
    }

    /// Open a new cursor
    pub fn open_cursor(&mut self, id: i32, cursor: VdbeCursor) {
        if let Some(slot) = self.cursors.get_mut(id as usize) {
            *slot = Some(cursor);
        }
    }

    /// Close a cursor
    pub fn close_cursor(&mut self, id: i32) {
        if let Some(slot) = self.cursors.get_mut(id as usize) {
            *slot = None;
        }
    }
}

/// Registry of opcode handlers
pub struct OpcodeRegistry {
    handlers: std::collections::HashMap<Opcode, Arc<dyn OpcodeHandler>>,
}

impl OpcodeRegistry {
    /// Create a new registry with all handlers registered
    pub fn new() -> Self {
        let mut registry = Self {
            handlers: std::collections::HashMap::new(),
        };

        // Register control flow handlers
        registry.register(Arc::new(control_flow::InitHandler));
        registry.register(Arc::new(control_flow::GotoHandler));
        registry.register(Arc::new(control_flow::HaltHandler));
        registry.register(Arc::new(control_flow::NoopHandler));
        registry.register(Arc::new(control_flow::IfHandler));
        registry.register(Arc::new(control_flow::IfNotHandler));
        registry.register(Arc::new(control_flow::IsNullHandler));
        registry.register(Arc::new(control_flow::NotNullHandler));
        registry.register(Arc::new(control_flow::IfPosHandler));
        registry.register(Arc::new(control_flow::DecrJumpZeroHandler));
        registry.register(Arc::new(control_flow::GosubHandler));
        registry.register(Arc::new(control_flow::ReturnHandler));
        registry.register(Arc::new(control_flow::YieldHandler));
        registry.register(Arc::new(control_flow::OffsetLimitHandler));
        registry.register(Arc::new(control_flow::IfNotZeroHandler));
        registry.register(Arc::new(control_flow::IfNotOpenHandler));

        // Register data movement handlers
        registry.register(Arc::new(data_movement::NullHandler));
        registry.register(Arc::new(data_movement::IntegerHandler));
        registry.register(Arc::new(data_movement::Int64Handler));
        registry.register(Arc::new(data_movement::RealHandler));
        registry.register(Arc::new(data_movement::String8Handler));
        registry.register(Arc::new(data_movement::CopyHandler));
        registry.register(Arc::new(data_movement::SCopyHandler));
        registry.register(Arc::new(data_movement::MoveHandler));
        registry.register(Arc::new(data_movement::IntCopyHandler));
        registry.register(Arc::new(data_movement::SoftNullHandler));
        registry.register(Arc::new(data_movement::ZeroOrNullHandler));
        registry.register(Arc::new(data_movement::MemMaxHandler));

        // Register arithmetic handlers
        registry.register(Arc::new(arithmetic::AddHandler));
        registry.register(Arc::new(arithmetic::AddImmHandler));
        registry.register(Arc::new(arithmetic::SubtractHandler));
        registry.register(Arc::new(arithmetic::MultiplyHandler));
        registry.register(Arc::new(arithmetic::DivideHandler));
        registry.register(Arc::new(arithmetic::RemainderHandler));

        // Register comparison handlers
        registry.register(Arc::new(comparison::EqHandler));
        registry.register(Arc::new(comparison::NeHandler));
        registry.register(Arc::new(comparison::LtHandler));
        registry.register(Arc::new(comparison::LeHandler));
        registry.register(Arc::new(comparison::GtHandler));
        registry.register(Arc::new(comparison::GeHandler));

        // Register logic handlers
        registry.register(Arc::new(logic::NotHandler));
        registry.register(Arc::new(logic::BitNotHandler));
        registry.register(Arc::new(logic::BitAndHandler));
        registry.register(Arc::new(logic::BitOrHandler));
        registry.register(Arc::new(logic::ShiftLeftHandler));
        registry.register(Arc::new(logic::ShiftRightHandler));
        registry.register(Arc::new(logic::ConcatHandler));

        // Register misc handlers
        registry.register(Arc::new(misc::OnceHandler));
        registry.register(Arc::new(misc::IfNullRowHandler));
        registry.register(Arc::new(misc::NullRowHandler));
        registry.register(Arc::new(misc::MakeRecordHandler));
        registry.register(Arc::new(misc::AffinityHandler));
        registry.register(Arc::new(misc::CastHandler));
        registry.register(Arc::new(misc::BlobHandler));
        registry.register(Arc::new(misc::MustBeIntHandler));
        registry.register(Arc::new(misc::RealAffinityHandler));
        registry.register(Arc::new(misc::VariableHandler));

        registry
    }

    /// Register a handler for its opcodes
    fn register(&mut self, handler: Arc<dyn OpcodeHandler>) {
        for opcode in handler.opcodes() {
            self.handlers.insert(*opcode, Arc::clone(&handler));
        }
    }

    /// Get the handler for an opcode
    pub fn get(&self, opcode: Opcode) -> Option<Arc<dyn OpcodeHandler>> {
        self.handlers.get(&opcode).cloned()
    }

    /// Check if an opcode has a handler registered
    pub fn has_handler(&self, opcode: Opcode) -> bool {
        self.handlers.contains_key(&opcode)
    }
}

impl Default for OpcodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Test utilities for handler tests
#[cfg(test)]
pub mod test_utils {
    use super::*;
    use crate::vdbe::engine::Mem;
    use std::collections::HashSet;

    /// Create an OpcodeContext for testing with minimal setup
    pub struct TestContext {
        pub mem: Vec<Mem>,
        pub pc: i32,
        pub cursors: Vec<Option<VdbeCursor>>,
        pub once_flags: HashSet<i32>,
        pub n_change: i64,
    }

    impl TestContext {
        pub fn new(mem_size: usize) -> Self {
            Self {
                mem: vec![Mem::default(); mem_size],
                pc: 0,
                cursors: vec![],
                once_flags: HashSet::new(),
                n_change: 0,
            }
        }

        pub fn with_cursors(mut self, n: usize) -> Self {
            self.cursors = (0..n).map(|_| None).collect();
            self
        }

        pub fn as_context(&mut self) -> OpcodeContext {
            OpcodeContext {
                mem: &mut self.mem,
                pc: &mut self.pc,
                db: None,
                cursors: &mut self.cursors,
                btree: None,
                schema: None,
                once_flags: &mut self.once_flags,
                n_change: &mut self.n_change,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_creation() {
        let registry = OpcodeRegistry::new();
        // Control flow
        assert!(registry.has_handler(Opcode::Init));
        assert!(registry.has_handler(Opcode::Goto));
        assert!(registry.has_handler(Opcode::Halt));
        assert!(registry.has_handler(Opcode::Noop));
        assert!(registry.has_handler(Opcode::If));
        assert!(registry.has_handler(Opcode::IfNot));
        assert!(registry.has_handler(Opcode::IsNull));
        assert!(registry.has_handler(Opcode::NotNull));
        assert!(registry.has_handler(Opcode::IfPos));
        assert!(registry.has_handler(Opcode::DecrJumpZero));
        assert!(registry.has_handler(Opcode::Gosub));
        assert!(registry.has_handler(Opcode::Return));
        assert!(registry.has_handler(Opcode::Yield));
        assert!(registry.has_handler(Opcode::OffsetLimit));
        assert!(registry.has_handler(Opcode::IfNotZero));
        assert!(registry.has_handler(Opcode::IfNotOpen));

        // Data movement
        assert!(registry.has_handler(Opcode::Null));
        assert!(registry.has_handler(Opcode::Integer));
        assert!(registry.has_handler(Opcode::Int64));
        assert!(registry.has_handler(Opcode::Real));
        assert!(registry.has_handler(Opcode::String8));
        assert!(registry.has_handler(Opcode::Copy));
        assert!(registry.has_handler(Opcode::SCopy));
        assert!(registry.has_handler(Opcode::Move));
        assert!(registry.has_handler(Opcode::IntCopy));
        assert!(registry.has_handler(Opcode::SoftNull));
        assert!(registry.has_handler(Opcode::ZeroOrNull));
        assert!(registry.has_handler(Opcode::MemMax));

        // Arithmetic
        assert!(registry.has_handler(Opcode::Add));
        assert!(registry.has_handler(Opcode::AddImm));
        assert!(registry.has_handler(Opcode::Subtract));
        assert!(registry.has_handler(Opcode::Multiply));
        assert!(registry.has_handler(Opcode::Divide));
        assert!(registry.has_handler(Opcode::Remainder));

        // Comparison
        assert!(registry.has_handler(Opcode::Eq));
        assert!(registry.has_handler(Opcode::Ne));
        assert!(registry.has_handler(Opcode::Lt));
        assert!(registry.has_handler(Opcode::Le));
        assert!(registry.has_handler(Opcode::Gt));
        assert!(registry.has_handler(Opcode::Ge));

        // Logic
        assert!(registry.has_handler(Opcode::Not));
        assert!(registry.has_handler(Opcode::BitNot));
        assert!(registry.has_handler(Opcode::BitAnd));
        assert!(registry.has_handler(Opcode::BitOr));
        assert!(registry.has_handler(Opcode::ShiftLeft));
        assert!(registry.has_handler(Opcode::ShiftRight));
        assert!(registry.has_handler(Opcode::Concat));

        // Misc
        assert!(registry.has_handler(Opcode::Once));
        assert!(registry.has_handler(Opcode::IfNullRow));
        assert!(registry.has_handler(Opcode::NullRow));
        assert!(registry.has_handler(Opcode::MakeRecord));
        assert!(registry.has_handler(Opcode::Affinity));
        assert!(registry.has_handler(Opcode::Cast));
        assert!(registry.has_handler(Opcode::Blob));
        assert!(registry.has_handler(Opcode::MustBeInt));
        assert!(registry.has_handler(Opcode::RealAffinity));
        assert!(registry.has_handler(Opcode::Variable));
    }

    #[test]
    fn test_registry_unregistered_opcode() {
        let registry = OpcodeRegistry::new();
        // Most opcodes aren't registered yet
        assert!(!registry.has_handler(Opcode::OpenRead));
        assert!(!registry.has_handler(Opcode::SeekGE));
    }
}
