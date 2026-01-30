//! Control flow opcode handlers
//!
//! Handles: Init, Goto, Halt, If, IfNot, Gosub, Return, Yield, Noop, etc.
//!
//! ## SQLite Reference
//!
//! These opcodes control program execution flow. See SQLite's vdbe.c for
//! the canonical implementation.

use crate::error::Result;
use crate::vdbe::ops::{Opcode, VdbeOp};

use super::{OpcodeContext, OpcodeHandler, OpcodeResult};

/// Init: Initialize program and optionally jump
///
/// SQLite: `case OP_Init:`
///
/// - P2 = jump destination (0 = continue to next instruction)
///
/// This is typically the first instruction in every program.
#[derive(Clone)]
pub struct InitHandler;

impl OpcodeHandler for InitHandler {
    fn execute(&self, _ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        // If P2 is non-zero, jump to that instruction
        if op.p2 != 0 {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Init]
    }
}

/// Goto: Unconditional jump
///
/// SQLite: `case OP_Goto:`
///
/// - P2 = jump destination
///
/// Jump immediately to instruction P2.
#[derive(Clone)]
pub struct GotoHandler;

impl OpcodeHandler for GotoHandler {
    fn execute(&self, _ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        Ok(OpcodeResult::Jump(op.p2))
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Goto]
    }
}

/// Halt: Stop execution
///
/// SQLite: `case OP_Halt:`
///
/// - P1 = result code (SQLITE_OK, SQLITE_CONSTRAINT, etc.)
/// - P2 = not used
/// - P4 = error message (optional)
///
/// Exit immediately. If in a subprogram (trigger), return to parent.
#[derive(Clone)]
pub struct HaltHandler;

impl OpcodeHandler for HaltHandler {
    fn execute(&self, _ctx: &mut OpcodeContext, _op: &VdbeOp) -> Result<OpcodeResult> {
        // For now, just signal done
        // Full implementation needs to handle:
        // - Error codes in P1
        // - Error messages in P4
        // - Subprogram return
        Ok(OpcodeResult::Done)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Halt]
    }
}

/// Noop: Do nothing
///
/// SQLite: `case OP_Noop:`
///
/// Placeholder instruction that does nothing.
#[derive(Clone)]
pub struct NoopHandler;

impl OpcodeHandler for NoopHandler {
    fn execute(&self, _ctx: &mut OpcodeContext, _op: &VdbeOp) -> Result<OpcodeResult> {
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Noop]
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vdbe::ops::P4;

    fn make_op(opcode: Opcode, p1: i32, p2: i32, p3: i32) -> VdbeOp {
        VdbeOp {
            opcode,
            p1,
            p2,
            p3,
            p4: P4::Unused,
            p5: 0,
            comment: None,
        }
    }

    #[test]
    fn test_init_no_jump() {
        let handler = InitHandler;
        let op = make_op(Opcode::Init, 0, 0, 0);
        let mut mem = vec![];
        let mut pc = 0;
        let mut ctx = OpcodeContext {
            mem: &mut mem,
            pc: &mut pc,
            db: None,
        };

        let result = handler.execute(&mut ctx, &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    #[test]
    fn test_init_with_jump() {
        let handler = InitHandler;
        let op = make_op(Opcode::Init, 0, 42, 0);
        let mut mem = vec![];
        let mut pc = 0;
        let mut ctx = OpcodeContext {
            mem: &mut mem,
            pc: &mut pc,
            db: None,
        };

        let result = handler.execute(&mut ctx, &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(42));
    }

    #[test]
    fn test_goto() {
        let handler = GotoHandler;
        let op = make_op(Opcode::Goto, 0, 100, 0);
        let mut mem = vec![];
        let mut pc = 0;
        let mut ctx = OpcodeContext {
            mem: &mut mem,
            pc: &mut pc,
            db: None,
        };

        let result = handler.execute(&mut ctx, &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_halt() {
        let handler = HaltHandler;
        let op = make_op(Opcode::Halt, 0, 0, 0);
        let mut mem = vec![];
        let mut pc = 0;
        let mut ctx = OpcodeContext {
            mem: &mut mem,
            pc: &mut pc,
            db: None,
        };

        let result = handler.execute(&mut ctx, &op).unwrap();
        assert_eq!(result, OpcodeResult::Done);
    }

    #[test]
    fn test_noop() {
        let handler = NoopHandler;
        let op = make_op(Opcode::Noop, 0, 0, 0);
        let mut mem = vec![];
        let mut pc = 0;
        let mut ctx = OpcodeContext {
            mem: &mut mem,
            pc: &mut pc,
            db: None,
        };

        let result = handler.execute(&mut ctx, &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }
}
