//! Logic and bitwise opcode handlers
//!
//! Handles: Not, BitNot, BitAnd, BitOr, ShiftLeft, ShiftRight, Concat
//!
//! ## SQLite Reference
//!
//! See SQLite's vdbe.c for the canonical implementation.
//!
//! ## SQLite Semantics
//!
//! - Not: Logical NOT, P2 = NOT P1
//! - BitNot: Bitwise NOT, P2 = ~P1
//! - BitAnd: P3 = P2 & P1
//! - BitOr: P3 = P2 | P1
//! - ShiftLeft: P3 = P2 << P1
//! - ShiftRight: P3 = P2 >> P1
//! - Concat: P3 = P2 || P1 (string concatenation)

use crate::error::Result;
use crate::vdbe::ops::{Opcode, VdbeOp};

use super::{OpcodeContext, OpcodeHandler, OpcodeResult};

/// Not: Logical NOT
///
/// SQLite: `case OP_Not:`
///
/// - P1 = source register
/// - P2 = destination register
///
/// P2 = NOT P1 (logical negation)
/// NULL remains NULL, 0 becomes 1, non-zero becomes 0.
#[derive(Clone)]
pub struct NotHandler;

impl OpcodeHandler for NotHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let src = ctx.mem(op.p1);

        if src.is_null() {
            ctx.mem_mut(op.p2).set_null();
        } else if src.is_truthy() {
            ctx.mem_mut(op.p2).set_int(0);
        } else {
            ctx.mem_mut(op.p2).set_int(1);
        }

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Not]
    }
}

/// BitNot: Bitwise NOT
///
/// SQLite: `case OP_BitNot:`
///
/// - P1 = source register
/// - P2 = destination register
///
/// P2 = ~P1 (bitwise complement)
#[derive(Clone)]
pub struct BitNotHandler;

impl OpcodeHandler for BitNotHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let src = ctx.mem(op.p1);

        if src.is_null() {
            ctx.mem_mut(op.p2).set_null();
        } else {
            let val = src.to_int();
            ctx.mem_mut(op.p2).set_int(!val);
        }

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::BitNot]
    }
}

/// BitAnd: Bitwise AND
///
/// SQLite: `case OP_BitAnd:`
///
/// - P1 = right operand register
/// - P2 = left operand register
/// - P3 = destination register
///
/// P3 = P2 & P1
#[derive(Clone)]
pub struct BitAndHandler;

impl OpcodeHandler for BitAndHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let a = ctx.mem(op.p1);
        let b = ctx.mem(op.p2);

        if a.is_null() || b.is_null() {
            ctx.mem_mut(op.p3).set_null();
        } else {
            let result = b.to_int() & a.to_int();
            ctx.mem_mut(op.p3).set_int(result);
        }

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::BitAnd]
    }
}

/// BitOr: Bitwise OR
///
/// SQLite: `case OP_BitOr:`
///
/// - P1 = right operand register
/// - P2 = left operand register
/// - P3 = destination register
///
/// P3 = P2 | P1
#[derive(Clone)]
pub struct BitOrHandler;

impl OpcodeHandler for BitOrHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let a = ctx.mem(op.p1);
        let b = ctx.mem(op.p2);

        if a.is_null() || b.is_null() {
            ctx.mem_mut(op.p3).set_null();
        } else {
            let result = b.to_int() | a.to_int();
            ctx.mem_mut(op.p3).set_int(result);
        }

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::BitOr]
    }
}

/// ShiftLeft: Left shift
///
/// SQLite: `case OP_ShiftLeft:`
///
/// - P1 = shift amount register
/// - P2 = value register
/// - P3 = destination register
///
/// P3 = P2 << P1
#[derive(Clone)]
pub struct ShiftLeftHandler;

impl OpcodeHandler for ShiftLeftHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let shift = ctx.mem(op.p1);
        let val = ctx.mem(op.p2);

        if shift.is_null() || val.is_null() {
            ctx.mem_mut(op.p3).set_null();
        } else {
            let shift_amt = shift.to_int();
            let value = val.to_int();

            // SQLite: negative shift = right shift, shift >= 64 = 0
            let result = if shift_amt < 0 {
                if shift_amt < -63 {
                    if value < 0 {
                        -1
                    } else {
                        0
                    }
                } else {
                    value >> (-shift_amt as u32)
                }
            } else if shift_amt > 63 {
                0
            } else {
                value << (shift_amt as u32)
            };

            ctx.mem_mut(op.p3).set_int(result);
        }

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::ShiftLeft]
    }
}

/// ShiftRight: Right shift
///
/// SQLite: `case OP_ShiftRight:`
///
/// - P1 = shift amount register
/// - P2 = value register
/// - P3 = destination register
///
/// P3 = P2 >> P1
#[derive(Clone)]
pub struct ShiftRightHandler;

impl OpcodeHandler for ShiftRightHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let shift = ctx.mem(op.p1);
        let val = ctx.mem(op.p2);

        if shift.is_null() || val.is_null() {
            ctx.mem_mut(op.p3).set_null();
        } else {
            let shift_amt = shift.to_int();
            let value = val.to_int();

            // SQLite: negative shift = left shift, shift >= 64 = sign extension
            let result = if shift_amt < 0 {
                if shift_amt < -63 {
                    0
                } else {
                    value << (-shift_amt as u32)
                }
            } else if shift_amt > 63 {
                if value < 0 {
                    -1
                } else {
                    0
                }
            } else {
                value >> (shift_amt as u32)
            };

            ctx.mem_mut(op.p3).set_int(result);
        }

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::ShiftRight]
    }
}

/// Concat: String concatenation
///
/// SQLite: `case OP_Concat:`
///
/// - P1 = right operand register
/// - P2 = left operand register
/// - P3 = destination register
///
/// P3 = P2 || P1 (string concatenation)
/// If either is NULL, result is NULL.
#[derive(Clone)]
pub struct ConcatHandler;

impl OpcodeHandler for ConcatHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let a = ctx.mem(op.p1);
        let b = ctx.mem(op.p2);

        if a.is_null() || b.is_null() {
            ctx.mem_mut(op.p3).set_null();
        } else {
            let left = b.to_string();
            let right = a.to_string();
            let result = format!("{}{}", left, right);
            ctx.mem_mut(op.p3).set_str(&result);
        }

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Concat]
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

    // Not tests
    #[test]
    fn test_not_true_to_false() {
        let handler = NotHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(42);
        let op = make_op(Opcode::Not, 1, 2, 0);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[2].to_int(), 0);
    }

    #[test]
    fn test_not_false_to_true() {
        let handler = NotHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(0);
        let op = make_op(Opcode::Not, 1, 2, 0);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[2].to_int(), 1);
    }

    #[test]
    fn test_not_null() {
        let handler = NotHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_null();
        let op = make_op(Opcode::Not, 1, 2, 0);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!(tc.mem[2].is_null());
    }

    // BitNot tests
    #[test]
    fn test_bitnot() {
        let handler = BitNotHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(0);
        let op = make_op(Opcode::BitNot, 1, 2, 0);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[2].to_int(), -1);
    }

    #[test]
    fn test_bitnot_value() {
        let handler = BitNotHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(0b1010);
        let op = make_op(Opcode::BitNot, 1, 2, 0);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[2].to_int(), !10i64);
    }

    #[test]
    fn test_bitnot_null() {
        let handler = BitNotHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_null();
        let op = make_op(Opcode::BitNot, 1, 2, 0);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!(tc.mem[2].is_null());
    }

    // BitAnd tests
    #[test]
    fn test_bitand() {
        let handler = BitAndHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(0b1010);
        tc.mem[2].set_int(0b1100);
        let op = make_op(Opcode::BitAnd, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), 0b1000);
    }

    #[test]
    fn test_bitand_null() {
        let handler = BitAndHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_null();
        tc.mem[2].set_int(0b1100);
        let op = make_op(Opcode::BitAnd, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!(tc.mem[3].is_null());
    }

    // BitOr tests
    #[test]
    fn test_bitor() {
        let handler = BitOrHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(0b1010);
        tc.mem[2].set_int(0b1100);
        let op = make_op(Opcode::BitOr, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), 0b1110);
    }

    // ShiftLeft tests
    #[test]
    fn test_shiftleft() {
        let handler = ShiftLeftHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(2);
        tc.mem[2].set_int(5);
        let op = make_op(Opcode::ShiftLeft, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), 20);
    }

    #[test]
    fn test_shiftleft_negative() {
        let handler = ShiftLeftHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(-2);
        tc.mem[2].set_int(20);
        let op = make_op(Opcode::ShiftLeft, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), 5);
    }

    #[test]
    fn test_shiftleft_overflow() {
        let handler = ShiftLeftHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(100);
        tc.mem[2].set_int(5);
        let op = make_op(Opcode::ShiftLeft, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), 0);
    }

    // ShiftRight tests
    #[test]
    fn test_shiftright() {
        let handler = ShiftRightHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(2);
        tc.mem[2].set_int(20);
        let op = make_op(Opcode::ShiftRight, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), 5);
    }

    #[test]
    fn test_shiftright_negative_value() {
        let handler = ShiftRightHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(2);
        tc.mem[2].set_int(-16);
        let op = make_op(Opcode::ShiftRight, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), -4);
    }

    // Concat tests
    #[test]
    fn test_concat() {
        let handler = ConcatHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_str("world");
        tc.mem[2].set_str("hello");
        let op = make_op(Opcode::Concat, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_string(), "helloworld");
    }

    #[test]
    fn test_concat_with_numbers() {
        let handler = ConcatHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_int(42);
        tc.mem[2].set_str("value:");
        let op = make_op(Opcode::Concat, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_string(), "value:42");
    }

    #[test]
    fn test_concat_null() {
        let handler = ConcatHandler;
        let mut tc = TestContext::new(5);
        tc.mem[1].set_null();
        tc.mem[2].set_str("hello");
        let op = make_op(Opcode::Concat, 1, 2, 3);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!(tc.mem[3].is_null());
    }
}
