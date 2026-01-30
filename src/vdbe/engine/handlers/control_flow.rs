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

/// If: Conditional jump if truthy
///
/// SQLite: `case OP_If:`
///
/// - P1 = register to test
/// - P2 = jump destination if truthy
///
/// Jump to P2 if r[P1] is non-zero/non-null/truthy.
#[derive(Clone)]
pub struct IfHandler;

impl OpcodeHandler for IfHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let val = ctx.mem(op.p1);
        if val.is_truthy() {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::If]
    }
}

/// IfNot: Conditional jump if not truthy
///
/// SQLite: `case OP_IfNot:`
///
/// - P1 = register to test
/// - P2 = jump destination if not truthy
///
/// Jump to P2 if r[P1] is zero/null/falsy.
#[derive(Clone)]
pub struct IfNotHandler;

impl OpcodeHandler for IfNotHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let val = ctx.mem(op.p1);
        if !val.is_truthy() {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::IfNot]
    }
}

/// IsNull: Jump if NULL
///
/// SQLite: `case OP_IsNull:`
///
/// - P1 = register to test
/// - P2 = jump destination if NULL
///
/// Jump to P2 if r[P1] is NULL.
#[derive(Clone)]
pub struct IsNullHandler;

impl OpcodeHandler for IsNullHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        if ctx.mem(op.p1).is_null() {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::IsNull]
    }
}

/// NotNull: Jump if not NULL
///
/// SQLite: `case OP_NotNull:`
///
/// - P1 = register to test
/// - P2 = jump destination if not NULL
///
/// Jump to P2 if r[P1] is not NULL.
#[derive(Clone)]
pub struct NotNullHandler;

impl OpcodeHandler for NotNullHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        if !ctx.mem(op.p1).is_null() {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::NotNull]
    }
}

/// IfPos: Jump and decrement if positive
///
/// SQLite: `case OP_IfPos:`
///
/// - P1 = register to test and modify
/// - P2 = jump destination if positive
/// - P3 = amount to subtract
///
/// If r[P1] > 0, then r[P1] -= P3 and jump to P2.
#[derive(Clone)]
pub struct IfPosHandler;

impl OpcodeHandler for IfPosHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let val = ctx.mem(op.p1).to_int();
        if val > 0 {
            ctx.mem_mut(op.p1).set_int(val - op.p3 as i64);
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::IfPos]
    }
}

/// DecrJumpZero: Decrement and jump if zero
///
/// SQLite: `case OP_DecrJumpZero:`
///
/// - P1 = register to decrement
/// - P2 = jump destination if becomes zero
///
/// Decrement r[P1] (unless already at i64::MIN to prevent underflow),
/// then jump to P2 if r[P1] == 0.
#[derive(Clone)]
pub struct DecrJumpZeroHandler;

impl OpcodeHandler for DecrJumpZeroHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let val = ctx.mem(op.p1).to_int();
        // SQLite: if( pIn1->u.i>SMALLEST_INT64 ) pIn1->u.i--;
        // Guard against underflow
        let new_val = if val > i64::MIN { val - 1 } else { val };
        ctx.mem_mut(op.p1).set_int(new_val);
        if new_val == 0 {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::DecrJumpZero]
    }
}

/// Gosub: Call subroutine
///
/// SQLite: `case OP_Gosub:`
///
/// - P1 = register to store return address
/// - P2 = subroutine address
///
/// Store the address of the next instruction (return address) in r[P1],
/// then jump to P2.
#[derive(Clone)]
pub struct GosubHandler;

impl OpcodeHandler for GosubHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        // ctx.pc is already pointing to the next instruction (return address)
        let return_addr = *ctx.pc;
        ctx.mem_mut(op.p1).set_int(return_addr as i64);
        Ok(OpcodeResult::Jump(op.p2))
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Gosub]
    }
}

/// Return: Return from subroutine
///
/// SQLite: `case OP_Return:`
///
/// - P1 = register containing return address
/// - P3 = if non-zero, allow fall-through when P1 is not an integer
///
/// Jump to the address stored in r[P1] if it's an integer.
/// If P1 is not an integer and P3 is non-zero, fall through.
/// This is used with OP_BeginSubrtn which may store a non-integer sentinel.
#[derive(Clone)]
pub struct ReturnHandler;

impl OpcodeHandler for ReturnHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let mem = ctx.mem(op.p1);
        // SQLite: Only jump if P1 contains an integer
        if mem.is_int() {
            let addr = mem.to_int() as i32;
            Ok(OpcodeResult::Jump(addr))
        } else if op.p3 != 0 {
            // P3 non-zero: fall through if P1 is not an integer
            // This is used with BeginSubrtn
            Ok(OpcodeResult::Continue)
        } else {
            // P3 is zero but P1 is not an integer - this shouldn't happen
            // in well-formed bytecode, but fall through to be safe
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Return]
    }
}

/// Yield: Coroutine yield/resume
///
/// SQLite: `case OP_Yield:`
///
/// - P1 = register containing/storing coroutine PC
/// - P2 = fallback jump if saved PC is 0
///
/// Swap the current PC with r[P1]. If the new PC is 0 and P2 != 0, jump to P2.
/// This implements coroutine switching.
#[derive(Clone)]
pub struct YieldHandler;

impl OpcodeHandler for YieldHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        // Read saved PC from register
        let saved_pc = ctx.mem(op.p1).to_int() as i32;
        // Store current PC (next instruction) in register
        let current_pc = *ctx.pc;
        ctx.mem_mut(op.p1).set_int(current_pc as i64);

        // Jump to saved PC, or fallback to P2 if saved was 0
        if saved_pc == 0 && op.p2 != 0 {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Jump(saved_pc))
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Yield]
    }
}

/// OffsetLimit: Compute total rows to scan (limit + offset)
///
/// SQLite: `case OP_OffsetLimit:`
///
/// - P1 = register containing limit
/// - P2 = destination register
/// - P3 = register containing offset
///
/// Compute total rows to scan:
/// - If limit <= 0, store -1 (infinite/loop forever)
/// - If limit + offset overflows, store -1
/// - Otherwise store limit + offset
#[derive(Clone)]
pub struct OffsetLimitHandler;

impl OpcodeHandler for OffsetLimitHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let limit = ctx.mem(op.p1).to_int();
        let offset = ctx.mem(op.p3).to_int();

        if limit <= 0 {
            // Zero or negative limit means infinite (loop forever)
            ctx.mem_mut(op.p2).set_int(-1);
        } else {
            // Add offset (only if positive) to limit
            let offset_to_add = if offset > 0 { offset } else { 0 };
            match limit.checked_add(offset_to_add) {
                Some(result) => ctx.mem_mut(op.p2).set_int(result),
                None => ctx.mem_mut(op.p2).set_int(-1), // Overflow = infinite
            }
        }

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::OffsetLimit]
    }
}

/// IfNotZero: Jump if not zero, decrement if positive
///
/// SQLite: `case OP_IfNotZero:`
///
/// - P1 = register to test and possibly decrement
/// - P2 = jump destination if not zero
///
/// If r[P1] != 0, decrement r[P1] only if positive, then jump to P2.
/// If r[P1] == 0, fall through without decrementing.
#[derive(Clone)]
pub struct IfNotZeroHandler;

impl OpcodeHandler for IfNotZeroHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let val = ctx.mem(op.p1).to_int();
        if val != 0 {
            // Only decrement if positive (SQLite behavior)
            if val > 0 {
                ctx.mem_mut(op.p1).set_int(val - 1);
            }
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::IfNotZero]
    }
}

/// IfNotOpen: Jump if cursor is not open
///
/// SQLite: `case OP_IfNotOpen:`
///
/// - P1 = cursor number
/// - P2 = jump destination if cursor not open
///
/// Jump to P2 if cursor P1 is not open.
#[derive(Clone)]
pub struct IfNotOpenHandler;

impl OpcodeHandler for IfNotOpenHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let is_open = ctx.cursor(op.p1).is_some();
        if !is_open {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::IfNotOpen]
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vdbe::engine::handlers::test_utils::TestContext;
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
        let mut tc = TestContext::new(0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    #[test]
    fn test_init_with_jump() {
        let handler = InitHandler;
        let op = make_op(Opcode::Init, 0, 42, 0);
        let mut tc = TestContext::new(0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(42));
    }

    #[test]
    fn test_goto() {
        let handler = GotoHandler;
        let op = make_op(Opcode::Goto, 0, 100, 0);
        let mut tc = TestContext::new(0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_halt() {
        let handler = HaltHandler;
        let op = make_op(Opcode::Halt, 0, 0, 0);
        let mut tc = TestContext::new(0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Done);
    }

    #[test]
    fn test_noop() {
        let handler = NoopHandler;
        let op = make_op(Opcode::Noop, 0, 0, 0);
        let mut tc = TestContext::new(0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    // ========================================================================
    // If tests
    // ========================================================================

    #[test]
    fn test_if_truthy_integer() {
        let handler = IfHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(42);
        let op = make_op(Opcode::If, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_if_falsy_zero() {
        let handler = IfHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(0);
        let op = make_op(Opcode::If, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    #[test]
    fn test_if_null() {
        let handler = IfHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_null();
        let op = make_op(Opcode::If, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    // ========================================================================
    // IfNot tests
    // ========================================================================

    #[test]
    fn test_ifnot_falsy() {
        let handler = IfNotHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(0);
        let op = make_op(Opcode::IfNot, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_ifnot_truthy() {
        let handler = IfNotHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(1);
        let op = make_op(Opcode::IfNot, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    // ========================================================================
    // IsNull tests
    // ========================================================================

    #[test]
    fn test_isnull_null() {
        let handler = IsNullHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_null();
        let op = make_op(Opcode::IsNull, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_isnull_not_null() {
        let handler = IsNullHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(42);
        let op = make_op(Opcode::IsNull, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    // ========================================================================
    // NotNull tests
    // ========================================================================

    #[test]
    fn test_notnull_not_null() {
        let handler = NotNullHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(42);
        let op = make_op(Opcode::NotNull, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_notnull_null() {
        let handler = NotNullHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_null();
        let op = make_op(Opcode::NotNull, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    // ========================================================================
    // IfPos tests
    // ========================================================================

    #[test]
    fn test_ifpos_positive() {
        let handler = IfPosHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(5);
        let op = make_op(Opcode::IfPos, 1, 100, 2);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
        assert_eq!(tc.mem[1].to_int(), 3);
    }

    #[test]
    fn test_ifpos_zero() {
        let handler = IfPosHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(0);
        let op = make_op(Opcode::IfPos, 1, 100, 1);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert_eq!(tc.mem[1].to_int(), 0);
    }

    #[test]
    fn test_ifpos_negative() {
        let handler = IfPosHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(-5);
        let op = make_op(Opcode::IfPos, 1, 100, 1);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert_eq!(tc.mem[1].to_int(), -5);
    }

    // ========================================================================
    // DecrJumpZero tests
    // ========================================================================

    #[test]
    fn test_decrjumpzero_becomes_zero() {
        let handler = DecrJumpZeroHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(1);
        let op = make_op(Opcode::DecrJumpZero, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
        assert_eq!(tc.mem[1].to_int(), 0);
    }

    #[test]
    fn test_decrjumpzero_not_zero() {
        let handler = DecrJumpZeroHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(5);
        let op = make_op(Opcode::DecrJumpZero, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert_eq!(tc.mem[1].to_int(), 4);
    }

    #[test]
    fn test_decrjumpzero_negative() {
        let handler = DecrJumpZeroHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(0);
        let op = make_op(Opcode::DecrJumpZero, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert_eq!(tc.mem[1].to_int(), -1);
    }

    // ========================================================================
    // Gosub tests
    // ========================================================================

    #[test]
    fn test_gosub() {
        let handler = GosubHandler;
        let mut tc = TestContext::new(5);
        tc.pc = 10; // Simulate "next instruction" is 10
        let op = make_op(Opcode::Gosub, 1, 50, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(50));
        assert_eq!(tc.mem[1].to_int(), 10); // Return address saved
    }

    // ========================================================================
    // Return tests
    // ========================================================================

    #[test]
    fn test_return() {
        let handler = ReturnHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(25); // Saved return address
        let op = make_op(Opcode::Return, 1, 0, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(25));
    }

    #[test]
    fn test_return_non_int_with_p3() {
        // SQLite: When P1 is not an integer and P3 is non-zero, fall through
        // This is used with OP_BeginSubrtn
        let handler = ReturnHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_null(); // Not an integer
        let op = make_op(Opcode::Return, 1, 0, 1); // P3=1 means fall through allowed
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    #[test]
    fn test_return_non_int_without_p3() {
        // When P1 is not an integer and P3 is zero, still fall through (safe fallback)
        let handler = ReturnHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_str("not an int"); // Not an integer
        let op = make_op(Opcode::Return, 1, 0, 0); // P3=0
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    // ========================================================================
    // Yield tests
    // ========================================================================

    #[test]
    fn test_yield_swap() {
        let handler = YieldHandler;
        let mut tc = TestContext::new(5);
        tc.pc = 20; // Current PC (next instruction)
        tc.mem[1].set_int(30); // Saved coroutine PC
        let op = make_op(Opcode::Yield, 1, 0, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(30));
        assert_eq!(tc.mem[1].to_int(), 20); // Current PC saved
    }

    #[test]
    fn test_yield_fallback_to_p2() {
        let handler = YieldHandler;
        let mut tc = TestContext::new(5);
        tc.pc = 20;
        tc.mem[1].set_int(0); // Saved PC is 0
        let op = make_op(Opcode::Yield, 1, 100, 0); // P2 = 100
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100)); // Fallback to P2
        assert_eq!(tc.mem[1].to_int(), 20);
    }

    #[test]
    fn test_yield_no_fallback() {
        let handler = YieldHandler;
        let mut tc = TestContext::new(5);
        tc.pc = 20;
        tc.mem[1].set_int(0); // Saved PC is 0
        let op = make_op(Opcode::Yield, 1, 0, 0); // P2 = 0 (no fallback)
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(0)); // Jump to 0
    }

    // ========================================================================
    // OffsetLimit tests (SQLite: computes limit + offset for total rows to scan)
    // ========================================================================

    #[test]
    fn test_offsetlimit_basic() {
        let handler = OffsetLimitHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(100); // limit
        tc.mem[3].set_int(20); // offset
        let op = make_op(Opcode::OffsetLimit, 1, 2, 3);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert_eq!(tc.mem[2].to_int(), 120); // 100 + 20 = total rows to scan
    }

    #[test]
    fn test_offsetlimit_negative_limit() {
        let handler = OffsetLimitHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(-1); // negative limit = infinite
        tc.mem[3].set_int(20);
        let op = make_op(Opcode::OffsetLimit, 1, 2, 3);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert_eq!(tc.mem[2].to_int(), -1); // infinite
    }

    #[test]
    fn test_offsetlimit_zero_limit() {
        let handler = OffsetLimitHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(0); // zero limit = infinite (SQLite behavior)
        tc.mem[3].set_int(20);
        let op = make_op(Opcode::OffsetLimit, 1, 2, 3);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert_eq!(tc.mem[2].to_int(), -1); // infinite
    }

    #[test]
    fn test_offsetlimit_negative_offset_ignored() {
        let handler = OffsetLimitHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(100); // limit
        tc.mem[3].set_int(-50); // negative offset treated as 0
        let op = make_op(Opcode::OffsetLimit, 1, 2, 3);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert_eq!(tc.mem[2].to_int(), 100); // 100 + 0 = 100
    }

    #[test]
    fn test_offsetlimit_overflow() {
        let handler = OffsetLimitHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(i64::MAX); // limit
        tc.mem[3].set_int(100); // offset causes overflow
        let op = make_op(Opcode::OffsetLimit, 1, 2, 3);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert_eq!(tc.mem[2].to_int(), -1); // overflow = infinite
    }

    // ========================================================================
    // IfNotZero tests (SQLite: only decrements if positive, jumps if non-zero)
    // ========================================================================

    #[test]
    fn test_ifnotzero_positive() {
        let handler = IfNotZeroHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(5);
        let op = make_op(Opcode::IfNotZero, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
        assert_eq!(tc.mem[1].to_int(), 4); // Decremented because positive
    }

    #[test]
    fn test_ifnotzero_one_becomes_zero() {
        let handler = IfNotZeroHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(1);
        let op = make_op(Opcode::IfNotZero, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        // Value was 1 (non-zero), so we jump. Decrement to 0.
        assert_eq!(result, OpcodeResult::Jump(100));
        assert_eq!(tc.mem[1].to_int(), 0);
    }

    #[test]
    fn test_ifnotzero_zero_falls_through() {
        let handler = IfNotZeroHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(0);
        let op = make_op(Opcode::IfNotZero, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        // Value is 0, so we fall through without decrementing
        assert_eq!(result, OpcodeResult::Continue);
        assert_eq!(tc.mem[1].to_int(), 0); // Unchanged
    }

    #[test]
    fn test_ifnotzero_negative_jumps_no_decrement() {
        let handler = IfNotZeroHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(-5);
        let op = make_op(Opcode::IfNotZero, 1, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        // Value is non-zero (negative), so we jump but DON'T decrement
        assert_eq!(result, OpcodeResult::Jump(100));
        assert_eq!(tc.mem[1].to_int(), -5); // Unchanged (only decrement if positive)
    }

    // ========================================================================
    // IfNotOpen tests
    // ========================================================================

    #[test]
    fn test_ifnotopen_not_open() {
        let handler = IfNotOpenHandler;
        let mut tc = TestContext::new(5).with_cursors(1);
        let op = make_op(Opcode::IfNotOpen, 0, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_ifnotopen_open() {
        let handler = IfNotOpenHandler;
        let mut tc = TestContext::new(5).with_cursors(1);
        use crate::vdbe::engine::VdbeCursor;
        tc.cursors[0] = Some(VdbeCursor::new(0, 0, false));
        let op = make_op(Opcode::IfNotOpen, 0, 100, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }
}
