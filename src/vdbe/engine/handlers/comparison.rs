//! Comparison opcode handlers
//!
//! Handles: Eq, Ne, Lt, Le, Gt, Ge
//!
//! ## SQLite Reference
//!
//! See SQLite's vdbe.c comparison opcodes. These opcodes compare values
//! and conditionally jump based on the result.
//!
//! ## SQLite Semantics
//!
//! - P1 = right operand register
//! - P2 = jump destination if condition is true
//! - P3 = left operand register
//! - P5 = flags (NULLEQ, JUMPIFNULL, affinity)
//!
//! Comparison is: r[P3] <op> r[P1] (note the order!)
//!
//! NULL handling:
//! - By default, any comparison with NULL yields "unknown" (no jump)
//! - NULLEQ (0x80): Makes NULL == NULL true (for Eq/Ne only)
//! - JUMPIFNULL (0x10): Jump if either operand is NULL

use std::cmp::Ordering;

use crate::error::Result;
use crate::vdbe::ops::{cmp_flags, Opcode, VdbeOp};

use super::{OpcodeContext, OpcodeHandler, OpcodeResult};

/// Eq: Jump if r[P3] == r[P1]
///
/// SQLite: `case OP_Eq:`
///
/// - P1 = right operand register
/// - P2 = jump destination
/// - P3 = left operand register
/// - P5 = comparison flags (NULLEQ, affinity)
#[derive(Clone)]
pub struct EqHandler;

impl OpcodeHandler for EqHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let left = ctx.mem(op.p3);
        let right = ctx.mem(op.p1);
        let nulleq = (op.p5 & cmp_flags::NULLEQ as u16) != 0;
        let affinity = op.p5 & cmp_flags::AFFINITY_MASK as u16;

        // SQLite: comparing with NULL yields unknown (no jump) unless NULLEQ
        if !nulleq && (left.is_null() || right.is_null()) {
            return Ok(OpcodeResult::Continue);
        }

        let cmp = left.compare_with_affinity(right, affinity);
        if cmp == Ordering::Equal {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Eq]
    }
}

/// Ne: Jump if r[P3] != r[P1]
///
/// SQLite: `case OP_Ne:`
///
/// - P1 = right operand register
/// - P2 = jump destination
/// - P3 = left operand register
/// - P5 = comparison flags (NULLEQ, JUMPIFNULL, affinity)
#[derive(Clone)]
pub struct NeHandler;

impl OpcodeHandler for NeHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let left = ctx.mem(op.p3);
        let right = ctx.mem(op.p1);
        let nulleq = (op.p5 & cmp_flags::NULLEQ as u16) != 0;
        let jumpifnull = (op.p5 & cmp_flags::JUMPIFNULL as u16) != 0;
        let affinity = op.p5 & cmp_flags::AFFINITY_MASK as u16;

        // Handle NULL cases
        if left.is_null() || right.is_null() {
            if jumpifnull {
                // JUMPIFNULL: jump when either operand is NULL
                return Ok(OpcodeResult::Jump(op.p2));
            } else if nulleq {
                // NULLEQ: NULL != non-NULL, NULL == NULL
                if left.is_null() ^ right.is_null() {
                    return Ok(OpcodeResult::Jump(op.p2));
                }
            }
            // Standard SQL: comparison with NULL is unknown, no jump
            return Ok(OpcodeResult::Continue);
        }

        let cmp = left.compare_with_affinity(right, affinity);
        if cmp != Ordering::Equal {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Ne]
    }
}

/// Lt: Jump if r[P3] < r[P1]
///
/// SQLite: `case OP_Lt:`
///
/// - P1 = right operand register
/// - P2 = jump destination
/// - P3 = left operand register
/// - P5 = comparison flags (JUMPIFNULL, affinity)
#[derive(Clone)]
pub struct LtHandler;

impl OpcodeHandler for LtHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let left = ctx.mem(op.p3);
        let right = ctx.mem(op.p1);
        let jumpifnull = (op.p5 & cmp_flags::JUMPIFNULL as u16) != 0;
        let affinity = op.p5 & cmp_flags::AFFINITY_MASK as u16;

        if left.is_null() || right.is_null() {
            if jumpifnull {
                return Ok(OpcodeResult::Jump(op.p2));
            }
            return Ok(OpcodeResult::Continue);
        }

        let cmp = left.compare_with_affinity(right, affinity);
        if cmp == Ordering::Less {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Lt]
    }
}

/// Le: Jump if r[P3] <= r[P1]
///
/// SQLite: `case OP_Le:`
///
/// - P1 = right operand register
/// - P2 = jump destination
/// - P3 = left operand register
/// - P5 = comparison flags (JUMPIFNULL, affinity)
#[derive(Clone)]
pub struct LeHandler;

impl OpcodeHandler for LeHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let left = ctx.mem(op.p3);
        let right = ctx.mem(op.p1);
        let jumpifnull = (op.p5 & cmp_flags::JUMPIFNULL as u16) != 0;
        let affinity = op.p5 & cmp_flags::AFFINITY_MASK as u16;

        if left.is_null() || right.is_null() {
            if jumpifnull {
                return Ok(OpcodeResult::Jump(op.p2));
            }
            return Ok(OpcodeResult::Continue);
        }

        let cmp = left.compare_with_affinity(right, affinity);
        if cmp != Ordering::Greater {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Le]
    }
}

/// Gt: Jump if r[P3] > r[P1]
///
/// SQLite: `case OP_Gt:`
///
/// - P1 = right operand register
/// - P2 = jump destination
/// - P3 = left operand register
/// - P5 = comparison flags (JUMPIFNULL, affinity)
#[derive(Clone)]
pub struct GtHandler;

impl OpcodeHandler for GtHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let left = ctx.mem(op.p3);
        let right = ctx.mem(op.p1);
        let jumpifnull = (op.p5 & cmp_flags::JUMPIFNULL as u16) != 0;
        let affinity = op.p5 & cmp_flags::AFFINITY_MASK as u16;

        if left.is_null() || right.is_null() {
            if jumpifnull {
                return Ok(OpcodeResult::Jump(op.p2));
            }
            return Ok(OpcodeResult::Continue);
        }

        let cmp = left.compare_with_affinity(right, affinity);
        if cmp == Ordering::Greater {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Gt]
    }
}

/// Ge: Jump if r[P3] >= r[P1]
///
/// SQLite: `case OP_Ge:`
///
/// - P1 = right operand register
/// - P2 = jump destination
/// - P3 = left operand register
/// - P5 = comparison flags (JUMPIFNULL, affinity)
#[derive(Clone)]
pub struct GeHandler;

impl OpcodeHandler for GeHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let left = ctx.mem(op.p3);
        let right = ctx.mem(op.p1);
        let jumpifnull = (op.p5 & cmp_flags::JUMPIFNULL as u16) != 0;
        let affinity = op.p5 & cmp_flags::AFFINITY_MASK as u16;

        if left.is_null() || right.is_null() {
            if jumpifnull {
                return Ok(OpcodeResult::Jump(op.p2));
            }
            return Ok(OpcodeResult::Continue);
        }

        let cmp = left.compare_with_affinity(right, affinity);
        if cmp != Ordering::Less {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Ge]
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

    fn make_op(opcode: Opcode, p1: i32, p2: i32, p3: i32, p5: u16) -> VdbeOp {
        VdbeOp {
            opcode,
            p1,
            p2,
            p3,
            p4: P4::Unused,
            p5,
            comment: None,
        }
    }

    // Eq tests
    #[test]
    fn test_eq_integers_equal() {
        let handler = EqHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(42);
        tc.mem[3].set_int(42);
        let op = make_op(Opcode::Eq, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_eq_integers_not_equal() {
        let handler = EqHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(42);
        tc.mem[3].set_int(99);
        let op = make_op(Opcode::Eq, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    #[test]
    fn test_eq_null_no_jump_by_default() {
        let handler = EqHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_null();
        tc.mem[3].set_int(42);
        let op = make_op(Opcode::Eq, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    #[test]
    fn test_eq_null_with_nulleq_flag() {
        let handler = EqHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_null();
        tc.mem[3].set_null();
        let op = make_op(Opcode::Eq, 1, 100, 3, cmp_flags::NULLEQ as u16);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_eq_strings() {
        let handler = EqHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_str("hello");
        tc.mem[3].set_str("hello");
        let op = make_op(Opcode::Eq, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    // Ne tests
    #[test]
    fn test_ne_integers_not_equal() {
        let handler = NeHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(42);
        tc.mem[3].set_int(99);
        let op = make_op(Opcode::Ne, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_ne_integers_equal() {
        let handler = NeHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(42);
        tc.mem[3].set_int(42);
        let op = make_op(Opcode::Ne, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    #[test]
    fn test_ne_null_jumpifnull() {
        let handler = NeHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_null();
        tc.mem[3].set_int(42);
        let op = make_op(Opcode::Ne, 1, 100, 3, cmp_flags::JUMPIFNULL as u16);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_ne_null_nulleq() {
        let handler = NeHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_null();
        tc.mem[3].set_int(42);
        let op = make_op(Opcode::Ne, 1, 100, 3, cmp_flags::NULLEQ as u16);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_ne_both_null_nulleq() {
        let handler = NeHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_null();
        tc.mem[3].set_null();
        let op = make_op(Opcode::Ne, 1, 100, 3, cmp_flags::NULLEQ as u16);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    // Lt tests
    #[test]
    fn test_lt_true() {
        let handler = LtHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(10);
        tc.mem[3].set_int(5);
        let op = make_op(Opcode::Lt, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_lt_false() {
        let handler = LtHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(5);
        tc.mem[3].set_int(10);
        let op = make_op(Opcode::Lt, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    #[test]
    fn test_lt_equal_no_jump() {
        let handler = LtHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(5);
        tc.mem[3].set_int(5);
        let op = make_op(Opcode::Lt, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    #[test]
    fn test_lt_null_jumpifnull() {
        let handler = LtHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_null();
        tc.mem[3].set_int(5);
        let op = make_op(Opcode::Lt, 1, 100, 3, cmp_flags::JUMPIFNULL as u16);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    // Le tests
    #[test]
    fn test_le_less() {
        let handler = LeHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(10);
        tc.mem[3].set_int(5);
        let op = make_op(Opcode::Le, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_le_equal() {
        let handler = LeHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(5);
        tc.mem[3].set_int(5);
        let op = make_op(Opcode::Le, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_le_greater() {
        let handler = LeHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(5);
        tc.mem[3].set_int(10);
        let op = make_op(Opcode::Le, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    // Gt tests
    #[test]
    fn test_gt_true() {
        let handler = GtHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(5);
        tc.mem[3].set_int(10);
        let op = make_op(Opcode::Gt, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_gt_false() {
        let handler = GtHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(10);
        tc.mem[3].set_int(5);
        let op = make_op(Opcode::Gt, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    #[test]
    fn test_gt_equal_no_jump() {
        let handler = GtHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(5);
        tc.mem[3].set_int(5);
        let op = make_op(Opcode::Gt, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    // Ge tests
    #[test]
    fn test_ge_greater() {
        let handler = GeHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(5);
        tc.mem[3].set_int(10);
        let op = make_op(Opcode::Ge, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_ge_equal() {
        let handler = GeHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(5);
        tc.mem[3].set_int(5);
        let op = make_op(Opcode::Ge, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_ge_less() {
        let handler = GeHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(10);
        tc.mem[3].set_int(5);
        let op = make_op(Opcode::Ge, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    #[test]
    fn test_ge_null_no_jumpifnull() {
        let handler = GeHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_null();
        tc.mem[3].set_int(5);
        let op = make_op(Opcode::Ge, 1, 100, 3, 0);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }
}
