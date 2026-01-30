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

use std::sync::Arc;

use crate::error::Result;
use crate::vdbe::ops::{Opcode, VdbeOp};

pub mod control_flow;

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
    // Add more fields as needed during migration
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_creation() {
        let registry = OpcodeRegistry::new();
        assert!(registry.has_handler(Opcode::Init));
        assert!(registry.has_handler(Opcode::Goto));
        assert!(registry.has_handler(Opcode::Halt));
        assert!(registry.has_handler(Opcode::Noop));
    }

    #[test]
    fn test_registry_unregistered_opcode() {
        let registry = OpcodeRegistry::new();
        // Most opcodes aren't registered yet
        assert!(!registry.has_handler(Opcode::Add));
        assert!(!registry.has_handler(Opcode::OpenRead));
    }
}
