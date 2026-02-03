//! Miscellaneous opcode handlers
//!
//! Handles: Once, IfNullRow, NullRow, MakeRecord, Rowid, Affinity
//!
//! ## SQLite Reference
//!
//! These are various utility opcodes that don't fit neatly into other categories.

use crate::error::Result;
use crate::schema::Affinity;
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

use super::{OpcodeContext, OpcodeHandler, OpcodeResult};

/// Convert affinity character to Affinity enum
///
/// SQLite uses single-character codes for affinities:
/// - '@' (0x40) = NONE
/// - 'A' (0x41) = BLOB
/// - 'B' (0x42) = TEXT
/// - 'C' (0x43) = NUMERIC
/// - 'D' (0x44) = INTEGER
/// - 'E' (0x45) = REAL
/// - 'F' (0x46) = FLEXNUM
fn affinity_from_char(ch: char) -> Affinity {
    match ch {
        '@' => Affinity::None,
        'A' => Affinity::Blob,
        'B' => Affinity::Text,
        'C' => Affinity::Numeric,
        'D' => Affinity::Integer,
        'E' => Affinity::Real,
        'F' => Affinity::Flexnum,
        _ => Affinity::Blob, // Default to Blob for unknown affinities
    }
}

/// Once: Execute block only once per statement execution
///
/// SQLite: `case OP_Once:`
///
/// - P1 = unique identifier for this Once block
/// - P2 = jump destination if already executed
///
/// On first execution, continue. On subsequent executions, jump to P2.
/// Used for one-time initializations in loops.
#[derive(Clone)]
pub struct OnceHandler;

impl OpcodeHandler for OnceHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let once_id = op.p1;
        if ctx.once_flags.contains(&once_id) {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            ctx.once_flags.insert(once_id);
            Ok(OpcodeResult::Continue)
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Once]
    }
}

/// IfNullRow: Jump if cursor is in null row state
///
/// SQLite: `case OP_IfNullRow:`
///
/// - P1 = cursor number
/// - P2 = jump destination
///
/// Jump to P2 if cursor P1 is pointing to a NULL row (e.g., for LEFT JOINs).
#[derive(Clone)]
pub struct IfNullRowHandler;

impl OpcodeHandler for IfNullRowHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        if let Some(cursor) = ctx.cursor(op.p1) {
            if cursor.null_row {
                return Ok(OpcodeResult::Jump(op.p2));
            }
        }
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::IfNullRow]
    }
}

/// NullRow: Set cursor to null row mode
///
/// SQLite: `case OP_NullRow:`
///
/// - P1 = cursor number
///
/// Mark cursor P1 as pointing to a NULL row. All Column operations
/// on this cursor will return NULL until the cursor is moved.
#[derive(Clone)]
pub struct NullRowHandler;

impl OpcodeHandler for NullRowHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        if let Some(cursor) = ctx.cursor_mut(op.p1) {
            cursor.set_null_row(true);
        }
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::NullRow]
    }
}

/// MakeRecord: Build a record from consecutive registers
///
/// SQLite: `case OP_MakeRecord:`
///
/// - P1 = first source register
/// - P2 = number of registers to include
/// - P3 = destination register
///
/// Constructs a database record from registers P1 through P1+P2-1
/// using SQLite's record format (varint header + serial types).
#[derive(Clone)]
pub struct MakeRecordHandler;

impl OpcodeHandler for MakeRecordHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let record = crate::vdbe::auxdata::make_record(ctx.mem, op.p1, op.p2);
        ctx.mem_mut(op.p3).set_blob(&record);
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::MakeRecord]
    }
}

/// Affinity: Apply type affinity to a range of registers
///
/// SQLite: `case OP_Affinity:`
///
/// - P1 = first register
/// - P2 = number of registers
/// - P4 = affinity string (one char per register)
///
/// Apply the affinity characters in P4 to the corresponding registers.
#[derive(Clone)]
pub struct AffinityHandler;

impl OpcodeHandler for AffinityHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        if let P4::Text(affinity_str) = &op.p4 {
            for (i, ch) in affinity_str.chars().take(op.p2 as usize).enumerate() {
                let affinity = affinity_from_char(ch);
                ctx.mem_mut(op.p1 + i as i32).apply_affinity(affinity);
            }
        }
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Affinity]
    }
}

/// Cast: Apply type affinity to a single register
///
/// SQLite: `case OP_Cast:`
///
/// - P1 = register to cast
/// - P2 = affinity character (as integer)
///
/// Force the value in register P1 to the type given by P2.
#[derive(Clone)]
pub struct CastHandler;

impl OpcodeHandler for CastHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        // CAST is more aggressive than affinity for INTEGER:
        // CAST('0x1234' AS INTEGER) → 0 (extracts leading digits)
        if op.p2 as u8 == b'D' {
            ctx.mem_mut(op.p1).cast_to_integer();
        } else {
            let affinity = affinity_from_char((op.p2 as u8) as char);
            ctx.mem_mut(op.p1).apply_affinity(affinity);
        }
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Cast]
    }
}

/// Blob: Store a blob value in a register
///
/// SQLite: `case OP_Blob:`
///
/// - P1 = length of blob
/// - P2 = destination register
/// - P4 = blob data
///
/// Store P4 (blob of P1 bytes) into register P2.
#[derive(Clone)]
pub struct BlobHandler;

impl OpcodeHandler for BlobHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let blob = match &op.p4 {
            P4::Blob(data) => data.clone(),
            _ => Vec::new(),
        };
        ctx.mem_mut(op.p2).set_blob(&blob);
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Blob]
    }
}

/// MustBeInt: Convert value to integer
///
/// SQLite: `case OP_MustBeInt:`
///
/// - P1 = register to convert
/// - P2 = jump destination if conversion fails (0 = error)
///
/// Force value to integer using NUMERIC affinity. If the result is not
/// an integer (including NULL), jump to P2 or raise error if P2==0.
#[derive(Clone)]
pub struct MustBeIntHandler;

impl OpcodeHandler for MustBeIntHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        let mem = ctx.mem(op.p1);

        // SQLite: First check if already an integer
        if mem.is_int() {
            return Ok(OpcodeResult::Continue);
        }

        // Real values can only convert if they are whole numbers
        if mem.is_real() {
            let val = mem.to_real();
            if val.fract() == 0.0 && val >= i64::MIN as f64 && val <= i64::MAX as f64 {
                ctx.mem_mut(op.p1).set_int(val as i64);
                return Ok(OpcodeResult::Continue);
            }
            // Non-whole real - error
            if op.p2 != 0 {
                return Ok(OpcodeResult::Jump(op.p2));
            } else {
                return Err(crate::error::Error::with_message(
                    crate::error::ErrorCode::Mismatch,
                    "datatype mismatch",
                ));
            }
        }

        // Strings can convert only if they look like integers
        if mem.is_str() {
            let s = mem.to_string();
            // Try to parse as integer
            if let Ok(val) = s.trim().parse::<i64>() {
                ctx.mem_mut(op.p1).set_int(val);
                return Ok(OpcodeResult::Continue);
            }
            // String is not a valid integer - error
            if op.p2 != 0 {
                return Ok(OpcodeResult::Jump(op.p2));
            } else {
                return Err(crate::error::Error::with_message(
                    crate::error::ErrorCode::Mismatch,
                    "datatype mismatch",
                ));
            }
        }

        // NULL and other types cannot become integers - jump or error
        // SQLite: NULL cannot become MEM_Int, so it triggers the jump/error path
        if op.p2 != 0 {
            Ok(OpcodeResult::Jump(op.p2))
        } else {
            Err(crate::error::Error::with_message(
                crate::error::ErrorCode::Mismatch,
                "datatype mismatch",
            ))
        }
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::MustBeInt]
    }
}

/// RealAffinity: Apply REAL affinity to register
///
/// SQLite: `case OP_RealAffinity:`
///
/// - P1 = register to convert
///
/// If r[P1] is an integer, convert it to a real number.
#[derive(Clone)]
pub struct RealAffinityHandler;

impl OpcodeHandler for RealAffinityHandler {
    fn execute(&self, ctx: &mut OpcodeContext, op: &VdbeOp) -> Result<OpcodeResult> {
        if ctx.mem(op.p1).is_int() {
            let val = ctx.mem(op.p1).to_int();
            ctx.mem_mut(op.p1).set_real(val as f64);
        }
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::RealAffinity]
    }
}

/// Variable: Load bound parameter into register
///
/// SQLite: `case OP_Variable:`
///
/// - P1 = parameter index (1-based)
/// - P2 = destination register
///
/// Transfer the value of bound parameter P1 into register P2.
/// Note: This handler returns Continue but actual binding must be done by engine.
#[derive(Clone)]
pub struct VariableHandler;

impl OpcodeHandler for VariableHandler {
    fn execute(&self, _ctx: &mut OpcodeContext, _op: &VdbeOp) -> Result<OpcodeResult> {
        // Variable binding is handled by the engine's bind infrastructure
        // This handler just signals continuation; actual binding happens elsewhere
        Ok(OpcodeResult::Continue)
    }

    fn opcodes(&self) -> &'static [Opcode] {
        &[Opcode::Variable]
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

    // Once tests
    #[test]
    fn test_once_first_time() {
        let handler = OnceHandler;
        let mut tc = TestContext::new(5);
        let op = make_op(Opcode::Once, 1, 100, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert!(tc.once_flags.contains(&1));
    }

    #[test]
    fn test_once_second_time() {
        let handler = OnceHandler;
        let mut tc = TestContext::new(5);
        tc.once_flags.insert(1);
        let op = make_op(Opcode::Once, 1, 100, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_once_different_ids() {
        let handler = OnceHandler;
        let mut tc = TestContext::new(5);
        tc.once_flags.insert(1);

        // Different ID should continue
        let op = make_op(Opcode::Once, 2, 100, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert!(tc.once_flags.contains(&2));
    }

    // IfNullRow tests
    #[test]
    fn test_ifnullrow_with_null_row() {
        let handler = IfNullRowHandler;
        let mut tc = TestContext::new(5).with_cursors(1);

        // Set up cursor with null_row = true
        use crate::vdbe::engine::VdbeCursor;
        tc.cursors[0] = Some(VdbeCursor::new(0, 0, false));
        if let Some(ref mut cursor) = tc.cursors[0] {
            cursor.null_row = true;
        }

        let op = make_op(Opcode::IfNullRow, 0, 100, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_ifnullrow_without_null_row() {
        let handler = IfNullRowHandler;
        let mut tc = TestContext::new(5).with_cursors(1);

        use crate::vdbe::engine::VdbeCursor;
        tc.cursors[0] = Some(VdbeCursor::new(0, 0, false));

        let op = make_op(Opcode::IfNullRow, 0, 100, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    #[test]
    fn test_ifnullrow_no_cursor() {
        let handler = IfNullRowHandler;
        let mut tc = TestContext::new(5).with_cursors(1);

        let op = make_op(Opcode::IfNullRow, 0, 100, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    // NullRow tests
    #[test]
    fn test_nullrow_sets_null_row() {
        let handler = NullRowHandler;
        let mut tc = TestContext::new(5).with_cursors(1);

        use crate::vdbe::engine::VdbeCursor;
        tc.cursors[0] = Some(VdbeCursor::new(0, 0, false));

        let op = make_op(Opcode::NullRow, 0, 0, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);

        if let Some(ref cursor) = tc.cursors[0] {
            assert!(cursor.null_row);
        }
    }

    // MakeRecord tests
    #[test]
    fn test_makerecord_single_int() {
        let handler = MakeRecordHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(42);

        let op = make_op(Opcode::MakeRecord, 1, 1, 5, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);

        // Result should be a blob
        assert!(tc.mem[5].is_blob());
    }

    #[test]
    fn test_makerecord_multiple_values() {
        let handler = MakeRecordHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(1);
        tc.mem[2].set_str("hello");
        tc.mem[3].set_real(3.14);

        let op = make_op(Opcode::MakeRecord, 1, 3, 5, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert!(tc.mem[5].is_blob());
    }

    // Affinity tests
    #[test]
    fn test_affinity_numeric() {
        let handler = AffinityHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_str("42");

        // 'D' = NUMERIC affinity
        let op = make_op(Opcode::Affinity, 1, 1, 0, P4::Text("D".to_string()));
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        // After numeric affinity, "42" should become integer 42
        assert_eq!(tc.mem[1].to_int(), 42);
    }

    #[test]
    fn test_affinity_multiple() {
        let handler = AffinityHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_str("42");
        tc.mem[2].set_str("3.14");

        // 'D' = NUMERIC for both
        let op = make_op(Opcode::Affinity, 1, 2, 0, P4::Text("DD".to_string()));
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    // Cast tests
    #[test]
    fn test_cast_to_integer() {
        let handler = CastHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_str("42");

        // 'D' = NUMERIC affinity (0x44 = 68)
        let op = make_op(Opcode::Cast, 1, 68, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
    }

    // Blob tests
    #[test]
    fn test_blob() {
        let handler = BlobHandler;
        let mut tc = TestContext::new(10);

        let data = vec![1, 2, 3, 4, 5];
        let op = make_op(Opcode::Blob, 5, 1, 0, P4::Blob(data.clone()));
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert!(tc.mem[1].is_blob());
    }

    // MustBeInt tests
    #[test]
    fn test_mustbeint_from_int() {
        let handler = MustBeIntHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(42);
        let op = make_op(Opcode::MustBeInt, 1, 100, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert_eq!(tc.mem[1].to_int(), 42);
    }

    #[test]
    fn test_mustbeint_from_real_whole() {
        // Whole number real (3.0) should convert to integer
        let handler = MustBeIntHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_real(3.0);
        let op = make_op(Opcode::MustBeInt, 1, 100, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert_eq!(tc.mem[1].to_int(), 3);
    }

    #[test]
    fn test_mustbeint_from_real_nonwhole() {
        // Non-whole real (3.7) should jump to P2
        let handler = MustBeIntHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_real(3.7);
        let op = make_op(Opcode::MustBeInt, 1, 100, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_mustbeint_from_string() {
        let handler = MustBeIntHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_str("42");
        let op = make_op(Opcode::MustBeInt, 1, 100, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert_eq!(tc.mem[1].to_int(), 42);
    }

    #[test]
    fn test_mustbeint_null_jumps() {
        // SQLite: NULL cannot become integer, so it jumps to P2
        let handler = MustBeIntHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_null();
        let op = make_op(Opcode::MustBeInt, 1, 100, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Jump(100));
    }

    #[test]
    fn test_mustbeint_null_errors_if_p2_zero() {
        // SQLite: NULL cannot become integer, and P2==0 means error
        let handler = MustBeIntHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_null();
        let op = make_op(Opcode::MustBeInt, 1, 0, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op);
        assert!(result.is_err());
    }

    // RealAffinity tests
    #[test]
    fn test_realaffinity_from_int() {
        let handler = RealAffinityHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_int(42);
        let op = make_op(Opcode::RealAffinity, 1, 0, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert!((tc.mem[1].to_real() - 42.0).abs() < 1e-10);
    }

    #[test]
    fn test_realaffinity_from_real() {
        let handler = RealAffinityHandler;
        let mut tc = TestContext::new(10);
        tc.mem[1].set_real(3.14);
        let op = make_op(Opcode::RealAffinity, 1, 0, 0, P4::Unused);
        let result = handler.execute(&mut tc.as_context(), &op).unwrap();
        assert_eq!(result, OpcodeResult::Continue);
        assert!((tc.mem[1].to_real() - 3.14).abs() < 1e-10);
    }
}
