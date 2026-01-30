//! Data movement opcode handlers
//!
//! Handles: Null, Integer, Int64, Real, String8, Blob, Copy, SCopy, Move
//!
//! ## SQLite Reference
//!
//! These opcodes move data into registers. See SQLite's vdbe.c for
//! the canonical implementation.
//!
//! ## SQLite Semantics
//!
//! - Null: Sets registers P2 through P2+P3 to NULL
//!   - P1 controls "cleared" flag (used for UPDATE OR REPLACE)
//! - Integer: Write P1 (32-bit signed) to register P2
//! - Int64: Write P4 (64-bit signed) to register P2
//! - Real: Write P4 (64-bit float) to register P2
//! - String8: Write P4 (string) to register P2
//! - Copy: Deep copy P3+1 registers from P1 to P2
//! - SCopy: Shallow copy register P1 to P2

use crate::error::Result;
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

use super::{OpcodeContext, OpcodeHandler, OpcodeResult};

/// Null: Set register(s) to NULL
///
/// SQLite: `case OP_Null:`
///
/// - P1 = 1 means set MEM_Cleared flag (for UPDATE OR REPLACE)
/// - P2 = first register to set
/// - P3 = last register to set (P2..P3 inclusive)
///
/// Sets registers P2 through P3 to NULL.
#[derive(Clone)]
pub struct NullHandler;

impl OpcodeHandler for NullHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        // SQLite: cnt = pOp->p3-pOp->p2;
        // Sets P2 through P3 inclusive
        let start = op.p2 as usize;
        let end = if op.p3 > op.p2 {
            op.p3 as usize
        } else {
            op.p2 as usize
        };

        for i in start..=end {
            if i < ctx.mem.len() {
                ctx.mem[i].set_null();
                // P1=1 means "cleared" null (for UPDATE OR REPLACE semantics)
                // We track this in the Mem type if needed
            }
        }

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Null]
    }
}

/// Integer: Write 32-bit integer to register
///
/// SQLite: `case OP_Integer:`
///
/// - P1 = integer value (32-bit signed)
/// - P2 = destination register
///
/// Write P1 into register P2.
#[derive(Clone)]
pub struct IntegerHandler;

impl OpcodeHandler for IntegerHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        // SQLite: pOut->u.i = pOp->p1;
        ctx.mem_mut(op.p2).set_int(op.p1 as i64);
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Integer]
    }
}

/// Int64: Write 64-bit integer to register
///
/// SQLite: `case OP_Int64:`
///
/// - P2 = destination register
/// - P4 = 64-bit integer value
///
/// Write P4 into register P2.
#[derive(Clone)]
pub struct Int64Handler;

impl OpcodeHandler for Int64Handler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        // SQLite: pOut->u.i = *pOp->p4.pI64;
        let value = match &op.p4 {
            P4::Int64(v) => *v,
            _ => 0,
        };
        ctx.mem_mut(op.p2).set_int(value);
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Int64]
    }
}

/// Real: Write 64-bit float to register
///
/// SQLite: `case OP_Real:`
///
/// - P2 = destination register
/// - P4 = 64-bit float value
///
/// Write P4 into register P2.
#[derive(Clone)]
pub struct RealHandler;

impl OpcodeHandler for RealHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        // SQLite: pOut->u.r = *pOp->p4.pReal;
        let value = match &op.p4 {
            P4::Real(v) => *v,
            _ => 0.0,
        };
        ctx.mem_mut(op.p2).set_real(value);
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Real]
    }
}

/// String8: Write string to register
///
/// SQLite: `case OP_String8:`
///
/// - P2 = destination register
/// - P4 = string value
///
/// Write P4 into register P2.
#[derive(Clone)]
pub struct String8Handler;

impl OpcodeHandler for String8Handler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let value = match &op.p4 {
            P4::Text(s) => s.clone(),
            _ => String::new(),
        };
        ctx.mem_mut(op.p2).set_str(&value);
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::String8]
    }
}

/// Copy: Deep copy registers
///
/// SQLite: `case OP_Copy:`
///
/// - P1 = source register
/// - P2 = destination register
/// - P3 = number of additional registers to copy (copies P3+1 total)
///
/// Make a deep copy of registers P1..P1+P3 into P2..P2+P3.
#[derive(Clone)]
pub struct CopyHandler;

impl OpcodeHandler for CopyHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        // SQLite: n = pOp->p3; copies n+1 registers
        let count = (op.p3 + 1) as usize;
        let src_start = op.p1 as usize;
        let dst_start = op.p2 as usize;

        // Clone values first to avoid borrow issues
        let values: Vec<_> = (0..count).map(|i| ctx.mem[src_start + i].clone()).collect();

        for (i, value) in values.into_iter().enumerate() {
            ctx.mem[dst_start + i] = value;
        }

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Copy]
    }
}

/// SCopy: Shallow copy register
///
/// SQLite: `case OP_SCopy:`
///
/// - P1 = source register
/// - P2 = destination register
///
/// Make a shallow copy of register P1 into P2.
/// For our Rust implementation, we clone (same as Copy for single register).
#[derive(Clone)]
pub struct SCopyHandler;

impl OpcodeHandler for SCopyHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let value = ctx.mem(op.p1).clone();
        *ctx.mem_mut(op.p2) = value;
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::SCopy]
    }
}

/// Move: Move register contents
///
/// SQLite: `case OP_Move:`
///
/// - P1 = source register
/// - P2 = destination register
/// - P3 = number of registers to move
///
/// Move P3 registers from P1 to P2. Source registers are cleared to NULL.
#[derive(Clone)]
pub struct MoveHandler;

impl OpcodeHandler for MoveHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let count = op.p3 as usize;
        let src_start = op.p1 as usize;
        let dst_start = op.p2 as usize;

        // Move values
        for i in 0..count {
            let value = std::mem::take(&mut ctx.mem[src_start + i]);
            ctx.mem[dst_start + i] = value;
            // Source is now default (NULL)
        }

        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Move]
    }
}

/// IntCopy: Copy integer value between registers
///
/// SQLite: `case OP_IntCopy:`
///
/// - P1 = source register
/// - P2 = destination register
///
/// Copy the integer value from r[P1] to r[P2].
#[derive(Clone)]
pub struct IntCopyHandler;

impl OpcodeHandler for IntCopyHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let val = ctx.mem(op.p1).to_int();
        ctx.mem_mut(op.p2).set_int(val);
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::IntCopy]
    }
}

/// SoftNull: Set register to soft null
///
/// SQLite: `case OP_SoftNull:`
///
/// - P1 = register to set
///
/// Set r[P1] to NULL. Same as regular null for our purposes.
#[derive(Clone)]
pub struct SoftNullHandler;

impl OpcodeHandler for SoftNullHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        ctx.mem_mut(op.p1).set_null();
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::SoftNull]
    }
}

/// ZeroOrNull: Check for nulls in range
///
/// SQLite: `case OP_ZeroOrNull:`
///
/// - P1 = start register
/// - P2 = count of registers
/// - P3 = destination register
///
/// If any register P1..P1+P2-1 is NULL, set P3 to NULL, else set to 0.
#[derive(Clone)]
pub struct ZeroOrNullHandler;

impl OpcodeHandler for ZeroOrNullHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let mut any_null = false;
        for i in 0..op.p2 {
            if ctx.mem(op.p1 + i).is_null() {
                any_null = true;
                break;
            }
        }
        if any_null {
            ctx.mem_mut(op.p3).set_null();
        } else {
            ctx.mem_mut(op.p3).set_int(0);
        }
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::ZeroOrNull]
    }
}

/// MemMax: Store maximum of two registers
///
/// SQLite: `case OP_MemMax:`
///
/// - P1 = first value register
/// - P2 = destination register (also second value)
///
/// Store max(r[P1], r[P2]) as integer into r[P2].
#[derive(Clone)]
pub struct MemMaxHandler;

impl OpcodeHandler for MemMaxHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let val1 = ctx.mem(op.p1).to_int();
        let val2 = ctx.mem(op.p2).to_int();
        ctx.mem_mut(op.p2).set_int(val1.max(val2));
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::MemMax]
    }
}

// ============================================================================
// Tests - Verify SQLite-equivalent behavior
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vdbe::engine::handlers::test_utils::TestContext;

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

    #[test]
    fn test_null_single_register() {
        let handler = NullHandler;
        let mut tc = TestContext::new(10);
        tc.mem[5].set_int(42);
        let op = make_op(Opcode::Null, 0, 5, 5, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert!(tc.mem[5].is_null());
    }

    #[test]
    fn test_null_multiple_registers() {
        let handler = NullHandler;
        let mut tc = TestContext::new(10);
        tc.mem[2].set_int(1);
        tc.mem[3].set_int(2);
        tc.mem[4].set_int(3);
        tc.mem[5].set_int(4);
        let op = make_op(Opcode::Null, 0, 2, 5, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!(tc.mem[2].is_null() && tc.mem[3].is_null());
        assert!(tc.mem[4].is_null() && tc.mem[5].is_null());
    }

    #[test]
    fn test_integer_positive() {
        let handler = IntegerHandler;
        let mut tc = TestContext::new(10);
        let op = make_op(Opcode::Integer, 42, 3, 0, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), 42);
    }

    #[test]
    fn test_integer_negative() {
        let handler = IntegerHandler;
        let mut tc = TestContext::new(10);
        let op = make_op(Opcode::Integer, -100, 3, 0, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), -100);
    }

    #[test]
    fn test_integer_zero() {
        let handler = IntegerHandler;
        let mut tc = TestContext::new(10);
        tc.mem[3].set_int(999);
        let op = make_op(Opcode::Integer, 0, 3, 0, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_int(), 0);
    }

    #[test]
    fn test_int64_large_value() {
        let handler = Int64Handler;
        let mut tc = TestContext::new(10);
        let large_value: i64 = 9_223_372_036_854_775_807;
        let op = make_op(Opcode::Int64, 0, 5, 0, P4::Int64(large_value));
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[5].to_int(), large_value);
    }

    #[test]
    fn test_int64_negative() {
        let handler = Int64Handler;
        let mut tc = TestContext::new(10);
        let value: i64 = -9_223_372_036_854_775_808;
        let op = make_op(Opcode::Int64, 0, 5, 0, P4::Int64(value));
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[5].to_int(), value);
    }

    #[test]
    fn test_real_positive() {
        let handler = RealHandler;
        let mut tc = TestContext::new(10);
        let op = make_op(Opcode::Real, 0, 3, 0, P4::Real(3.14159));
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!((tc.mem[3].to_real() - 3.14159).abs() < 1e-10);
    }

    #[test]
    fn test_real_negative() {
        let handler = RealHandler;
        let mut tc = TestContext::new(10);
        let op = make_op(Opcode::Real, 0, 3, 0, P4::Real(-2.718));
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!((tc.mem[3].to_real() - (-2.718)).abs() < 1e-10);
    }

    #[test]
    fn test_string8() {
        let handler = String8Handler;
        let mut tc = TestContext::new(10);
        let op = make_op(Opcode::String8, 0, 3, 0, P4::Text("hello".to_string()));
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_string(), "hello");
    }

    #[test]
    fn test_string8_empty() {
        let handler = String8Handler;
        let mut tc = TestContext::new(10);
        let op = make_op(Opcode::String8, 0, 3, 0, P4::Text("".to_string()));
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[3].to_string(), "");
    }

    #[test]
    fn test_copy_single() {
        let handler = CopyHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(42);
        let op = make_op(Opcode::Copy, 1, 5, 0, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[1].to_int(), 42);
        assert_eq!(tc.mem[5].to_int(), 42);
    }

    #[test]
    fn test_copy_multiple() {
        let handler = CopyHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(10);
        tc.mem[2].set_int(20);
        tc.mem[3].set_int(30);
        let op = make_op(Opcode::Copy, 1, 5, 2, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[5].to_int(), 10);
        assert_eq!(tc.mem[6].to_int(), 20);
        assert_eq!(tc.mem[7].to_int(), 30);
    }

    #[test]
    fn test_scopy() {
        let handler = SCopyHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_str("test");
        let op = make_op(Opcode::SCopy, 1, 5, 0, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[1].to_string(), "test");
        assert_eq!(tc.mem[5].to_string(), "test");
    }

    #[test]
    fn test_move_single() {
        let handler = MoveHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(42);
        let op = make_op(Opcode::Move, 1, 5, 1, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!(tc.mem[1].is_null() || tc.mem[1].to_int() == 0);
        assert_eq!(tc.mem[5].to_int(), 42);
    }

    #[test]
    fn test_move_multiple() {
        let handler = MoveHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(10);
        tc.mem[2].set_int(20);
        let op = make_op(Opcode::Move, 1, 5, 2, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!(tc.mem[1].is_null() || tc.mem[1].to_int() == 0);
        assert!(tc.mem[2].is_null() || tc.mem[2].to_int() == 0);
        assert_eq!(tc.mem[5].to_int(), 10);
        assert_eq!(tc.mem[6].to_int(), 20);
    }

    // IntCopy tests
    #[test]
    fn test_intcopy() {
        let handler = IntCopyHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(42);
        let op = make_op(Opcode::IntCopy, 1, 5, 0, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[5].to_int(), 42);
    }

    #[test]
    fn test_intcopy_from_real() {
        let handler = IntCopyHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_real(3.7);
        let op = make_op(Opcode::IntCopy, 1, 5, 0, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[5].to_int(), 3);
    }

    // SoftNull tests
    #[test]
    fn test_softnull() {
        let handler = SoftNullHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(42);
        let op = make_op(Opcode::SoftNull, 1, 0, 0, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!(tc.mem[1].is_null());
    }

    // ZeroOrNull tests
    #[test]
    fn test_zeroornull_no_null() {
        let handler = ZeroOrNullHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(1);
        tc.mem[2].set_int(2);
        tc.mem[3].set_int(3);
        let op = make_op(Opcode::ZeroOrNull, 1, 3, 5, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[5].to_int(), 0);
    }

    #[test]
    fn test_zeroornull_with_null() {
        let handler = ZeroOrNullHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(1);
        tc.mem[2].set_null();
        tc.mem[3].set_int(3);
        let op = make_op(Opcode::ZeroOrNull, 1, 3, 5, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert!(tc.mem[5].is_null());
    }

    // MemMax tests
    #[test]
    fn test_memmax_first_larger() {
        let handler = MemMaxHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(50);
        tc.mem[2].set_int(30);
        let op = make_op(Opcode::MemMax, 1, 2, 0, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[2].to_int(), 50);
    }

    #[test]
    fn test_memmax_second_larger() {
        let handler = MemMaxHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(10);
        tc.mem[2].set_int(30);
        let op = make_op(Opcode::MemMax, 1, 2, 0, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[2].to_int(), 30);
    }

    #[test]
    fn test_memmax_negative() {
        let handler = MemMaxHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(-5);
        tc.mem[2].set_int(-10);
        let op = make_op(Opcode::MemMax, 1, 2, 0, P4::Unused);
        handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(tc.mem[2].to_int(), -5);
    }
}
