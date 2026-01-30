//! Arithmetic opcode handlers
//!
//! Handles: Add, Subtract, Multiply, Divide, Remainder
//!
//! ## SQLite Reference
//!
//! See SQLite's vdbe.c `case OP_Add:` for the canonical implementation.
//!
//! ## SQLite Semantics
//!
//! - If both operands are integers, perform integer arithmetic
//! - If either operand is NULL, result is NULL
//! - If integer overflow occurs, fall back to floating point
//! - Division by zero returns NULL
//! - Operands: P1 and P2 are source registers, P3 is destination
//! - Note: Result is P2 OP P1 (reversed order from what you might expect)

use crate::error::Result;
use crate::vdbe::ops::{Opcode, VdbeOp};

use super::{OpcodeContext, OpcodeHandler, OpcodeResult};

/// Add: P3 = P2 + P1
///
/// SQLite: `case OP_Add:`
///
/// - P1 = first operand register
/// - P2 = second operand register
/// - P3 = destination register
///
/// SQLite computes: P3 = P2 + P1 (note the order!)
#[derive(Clone)]
pub struct AddHandler;

impl OpcodeHandler for AddHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let a = ctx.mem(op.p1);
        let b = ctx.mem(op.p2);

        // SQLite: if either is NULL, result is NULL
        if a.is_null() || b.is_null() {
            ctx.mem_mut(op.p3).set_null();
            return Ok(OpcodeResult::Continue);
        }

        // Try integer arithmetic first
        if a.is_int() && b.is_int() {
            let ia = a.to_int();
            let ib = b.to_int();

            // Check for overflow
            match ib.checked_add(ia) {
                Some(result) => {
                    ctx.mem_mut(op.p3).set_int(result);
                    return Ok(OpcodeResult::Continue);
                }
                None => {
                    // Fall through to float
                }
            }
        }

        // Float arithmetic
        let ra = a.to_real();
        let rb = b.to_real();
        ctx.mem_mut(op.p3).set_real(rb + ra);

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Add]
    }
}

/// AddImm: Add immediate value to register
///
/// SQLite: `case OP_AddImm:`
///
/// - P1 = register to modify
/// - P2 = immediate integer to add
///
/// SQLite computes: r[P1] += P2 using wrapping arithmetic.
/// The value is first converted to integer if not already.
#[derive(Clone)]
pub struct AddImmHandler;

impl OpcodeHandler for AddImmHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        // SQLite: sqlite3VdbeMemIntegerify(pIn1) - convert to int first
        let val = ctx.mem(op.p1).to_int();
        // SQLite uses unsigned wrapping: *(u64*)&pIn1->u.i += (u64)pOp->p2
        let result = (val as u64).wrapping_add(op.p2 as i64 as u64) as i64;
        ctx.mem_mut(op.p1).set_int(result);
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::AddImm]
    }
}

/// Subtract: P3 = P2 - P1
///
/// SQLite: `case OP_Subtract:`
///
/// SQLite computes: P3 = P2 - P1
#[derive(Clone)]
pub struct SubtractHandler;

impl OpcodeHandler for SubtractHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let a = ctx.mem(op.p1);
        let b = ctx.mem(op.p2);

        if a.is_null() || b.is_null() {
            ctx.mem_mut(op.p3).set_null();
            return Ok(OpcodeResult::Continue);
        }

        if a.is_int() && b.is_int() {
            let ia = a.to_int();
            let ib = b.to_int();

            match ib.checked_sub(ia) {
                Some(result) => {
                    ctx.mem_mut(op.p3).set_int(result);
                    return Ok(OpcodeResult::Continue);
                }
                None => {}
            }
        }

        let ra = a.to_real();
        let rb = b.to_real();
        ctx.mem_mut(op.p3).set_real(rb - ra);

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Subtract]
    }
}

/// Multiply: P3 = P2 * P1
///
/// SQLite: `case OP_Multiply:`
///
/// SQLite computes: P3 = P2 * P1
#[derive(Clone)]
pub struct MultiplyHandler;

impl OpcodeHandler for MultiplyHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let a = ctx.mem(op.p1);
        let b = ctx.mem(op.p2);

        if a.is_null() || b.is_null() {
            ctx.mem_mut(op.p3).set_null();
            return Ok(OpcodeResult::Continue);
        }

        if a.is_int() && b.is_int() {
            let ia = a.to_int();
            let ib = b.to_int();

            match ib.checked_mul(ia) {
                Some(result) => {
                    ctx.mem_mut(op.p3).set_int(result);
                    return Ok(OpcodeResult::Continue);
                }
                None => {}
            }
        }

        let ra = a.to_real();
        let rb = b.to_real();
        ctx.mem_mut(op.p3).set_real(rb * ra);

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Multiply]
    }
}

/// Divide: P3 = P2 / P1
///
/// SQLite: `case OP_Divide:`
///
/// SQLite computes: P3 = P2 / P1
/// Division by zero returns NULL.
#[derive(Clone)]
pub struct DivideHandler;

impl OpcodeHandler for DivideHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let a = ctx.mem(op.p1);
        let b = ctx.mem(op.p2);

        if a.is_null() || b.is_null() {
            ctx.mem_mut(op.p3).set_null();
            return Ok(OpcodeResult::Continue);
        }

        if a.is_int() && b.is_int() {
            let ia = a.to_int();
            let ib = b.to_int();

            // SQLite: division by zero returns NULL
            if ia == 0 {
                ctx.mem_mut(op.p3).set_null();
                return Ok(OpcodeResult::Continue);
            }

            // SQLite: -1 / MIN_INT would overflow, use float
            if ia == -1 && ib == i64::MIN {
                // Fall through to float
            } else {
                ctx.mem_mut(op.p3).set_int(ib / ia);
                return Ok(OpcodeResult::Continue);
            }
        }

        let ra = a.to_real();
        let rb = b.to_real();

        if ra == 0.0 {
            ctx.mem_mut(op.p3).set_null();
            return Ok(OpcodeResult::Continue);
        }

        ctx.mem_mut(op.p3).set_real(rb / ra);

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Divide]
    }
}

/// Remainder: P3 = P2 % P1
///
/// SQLite: `case OP_Remainder:`
///
/// SQLite computes: P3 = P2 % P1
/// Modulo by zero returns NULL.
#[derive(Clone)]
pub struct RemainderHandler;

impl OpcodeHandler for RemainderHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let a = ctx.mem(op.p1);
        let b = ctx.mem(op.p2);

        if a.is_null() || b.is_null() {
            ctx.mem_mut(op.p3).set_null();
            return Ok(OpcodeResult::Continue);
        }

        let ia = a.to_int();
        let ib = b.to_int();

        // SQLite: modulo by zero returns NULL
        if ia == 0 {
            ctx.mem_mut(op.p3).set_null();
            return Ok(OpcodeResult::Continue);
        }

        // SQLite: if ia == -1, use ia = 1 to avoid overflow
        let divisor = if ia == -1 { 1 } else { ia };
        ctx.mem_mut(op.p3).set_int(ib % divisor);

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Remainder]
    }
}

// ============================================================================
// Tests - Verify SQLite-equivalent behavior
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
    fn test_add_integers() {
        let handler = AddHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(5);
        tc.mem[2].set_int(3);
        let op = make_op(Opcode::Add, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), 8);
    }

    #[test]
    fn test_add_negative() {
        let handler = AddHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(-5);
        tc.mem[2].set_int(3);
        let op = make_op(Opcode::Add, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), -2);
    }

    #[test]
    fn test_add_null_operand() {
        let handler = AddHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_null();
        tc.mem[2].set_int(5);
        let op = make_op(Opcode::Add, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!(tc.mem[3].is_null());
    }

    #[test]
    fn test_add_overflow_to_float() {
        let handler = AddHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(i64::MAX);
        tc.mem[2].set_int(i64::MAX);
        let op = make_op(Opcode::Add, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        let result = tc.mem[3].to_real();
        let expected = 2.0 * (i64::MAX as f64);
        assert!((result - expected).abs() < 1e10);
    }

    #[test]
    fn test_add_floats() {
        let handler = AddHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_real(1.5);
        tc.mem[2].set_real(2.5);
        let op = make_op(Opcode::Add, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!((tc.mem[3].to_real() - 4.0).abs() < 1e-10);
    }

    // AddImm tests
    #[test]
    fn test_addimm_positive() {
        let handler = AddImmHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(10);
        let op = make_op(Opcode::AddImm, 1, 5, 0);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[1].to_int(), 15);
    }

    #[test]
    fn test_addimm_negative() {
        let handler = AddImmHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(10);
        let op = make_op(Opcode::AddImm, 1, -3, 0);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[1].to_int(), 7);
    }

    #[test]
    fn test_subtract_integers() {
        let handler = SubtractHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(3);
        tc.mem[2].set_int(10);
        let op = make_op(Opcode::Subtract, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), 7);
    }

    #[test]
    fn test_subtract_null() {
        let handler = SubtractHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(5);
        tc.mem[2].set_null();
        let op = make_op(Opcode::Subtract, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!(tc.mem[3].is_null());
    }

    #[test]
    fn test_multiply_integers() {
        let handler = MultiplyHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(4);
        tc.mem[2].set_int(5);
        let op = make_op(Opcode::Multiply, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), 20);
    }

    #[test]
    fn test_multiply_by_zero() {
        let handler = MultiplyHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(0);
        tc.mem[2].set_int(999);
        let op = make_op(Opcode::Multiply, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), 0);
    }

    #[test]
    fn test_divide_integers() {
        let handler = DivideHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(3);
        tc.mem[2].set_int(10);
        let op = make_op(Opcode::Divide, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), 3);
    }

    #[test]
    fn test_divide_by_zero_returns_null() {
        let handler = DivideHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(0);
        tc.mem[2].set_int(10);
        let op = make_op(Opcode::Divide, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!(tc.mem[3].is_null());
    }

    #[test]
    fn test_divide_floats() {
        let handler = DivideHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_real(2.0);
        tc.mem[2].set_real(5.0);
        let op = make_op(Opcode::Divide, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!((tc.mem[3].to_real() - 2.5).abs() < 1e-10);
    }

    #[test]
    fn test_remainder() {
        let handler = RemainderHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(3);
        tc.mem[2].set_int(10);
        let op = make_op(Opcode::Remainder, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), 1);
    }

    #[test]
    fn test_remainder_by_zero_returns_null() {
        let handler = RemainderHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(0);
        tc.mem[2].set_int(10);
        let op = make_op(Opcode::Remainder, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!(tc.mem[3].is_null());
    }

    #[test]
    fn test_remainder_negative() {
        let handler = RemainderHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(3);
        tc.mem[2].set_int(-10);
        let op = make_op(Opcode::Remainder, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), -1);
    }
}
