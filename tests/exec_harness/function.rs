//! Function and type-coercion opcode tests
//!
//! Note: Function/PureFunc opcodes require P4::FuncDef which cannot easily
//! be marshaled across FFI. These tests cover type coercion, register
//! manipulation, and utility opcodes instead.
//!
//! All programs use linear layout (no Init/Goto envelope).

use super::*;

// ============================================================================
// Cast: type affinity conversion
// ============================================================================

#[test]
fn test_cast_integer_to_text() {
    // Cast P1 P2: convert r[P1] to affinity P2
    // P2=66 ('B') = TEXT affinity
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::Cast, 1, 66, 0), // 'B' = TEXT
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_cast_text_to_integer() {
    // P2=68 ('D') = INTEGER affinity
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("123".into())),
            VdbeOp::new(Opcode::Cast, 1, 68, 0), // 'D' = INTEGER
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_cast_integer_to_real() {
    // P2=69 ('E') = REAL affinity
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::Cast, 1, 69, 0), // 'E' = REAL
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_cast_real_to_integer() {
    // P2=68 ('D') = INTEGER affinity
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::Real, 0, 1, 0, P4::Real(3.7)),
            VdbeOp::new(Opcode::Cast, 1, 68, 0), // 'D' = INTEGER
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_cast_text_to_real() {
    // P2=69 ('E') = REAL affinity
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("3.125".into())),
            VdbeOp::new(Opcode::Cast, 1, 69, 0), // 'E' = REAL
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_cast_integer_to_blob() {
    // P2=65 ('A') = BLOB affinity
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::Cast, 1, 65, 0), // 'A' = BLOB
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_cast_null_to_integer() {
    // Casting NULL should remain NULL
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Null, 0, 1, 0),
            VdbeOp::new(Opcode::Cast, 1, 68, 0), // 'D' = INTEGER
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_cast_text_to_numeric() {
    // P2=67 ('C') = NUMERIC affinity
    // "456" should become integer 456 under NUMERIC affinity
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("456".into())),
            VdbeOp::new(Opcode::Cast, 1, 67, 0), // 'C' = NUMERIC
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

// ============================================================================
// MustBeInt: integer coercion with error jump
// ============================================================================

#[test]
fn test_mustbeint_with_integer() {
    // r[1]=5 is already integer, so MustBeInt should NOT jump
    // P2=error target (unused since 5 is integer)
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 5, 1, 0),   // 0: r[1] = 5
            VdbeOp::new(Opcode::MustBeInt, 1, 4, 0), // 1: must be int, else jump to 4
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0), // 2: output r[1]
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 3: done
            VdbeOp::new(Opcode::Null, 0, 1, 0),      // 4: error path (not taken)
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0), // 5
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 6
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_mustbeint_with_exact_real() {
    // r[1]=5.0 (exact real), MustBeInt should convert to integer 5
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::Real, 0, 1, 0, P4::Real(5.0)), // 0: r[1] = 5.0
            VdbeOp::new(Opcode::MustBeInt, 1, 4, 0),               // 1: must be int
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),               // 2: output r[1]
            VdbeOp::new(Opcode::Halt, 0, 0, 0),                    // 3: done
            VdbeOp::new(Opcode::Null, 0, 1, 0),                    // 4: error path
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),               // 5
            VdbeOp::new(Opcode::Halt, 0, 0, 0),                    // 6
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_mustbeint_with_fractional_real() {
    // r[1]=3.7 (fractional), MustBeInt should jump to error target
    // Output NULL from error path to distinguish from success path
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::Real, 0, 1, 0, P4::Real(3.7)), // 0: r[1] = 3.7
            VdbeOp::new(Opcode::MustBeInt, 1, 4, 0),               // 1: must be int, jump 4
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),               // 2: success path
            VdbeOp::new(Opcode::Halt, 0, 0, 0),                    // 3
            VdbeOp::new(Opcode::Null, 0, 2, 0),                    // 4: error path
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),               // 5
            VdbeOp::new(Opcode::Halt, 0, 0, 0),                    // 6
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

// ============================================================================
// RealAffinity: convert integer to real
// ============================================================================

#[test]
fn test_realaffinity_integer() {
    // r[1]=42 (integer) -> RealAffinity -> should become 42.0 (real)
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::RealAffinity, 1, 0, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_realaffinity_already_real() {
    // r[1]=3.125 (already real) -> RealAffinity -> no change
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::Real, 0, 1, 0, P4::Real(3.125)),
            VdbeOp::new(Opcode::RealAffinity, 1, 0, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_realaffinity_null() {
    // r[1]=NULL -> RealAffinity -> should remain NULL
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Null, 0, 1, 0),
            VdbeOp::new(Opcode::RealAffinity, 1, 0, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_realaffinity_zero() {
    // r[1]=0 (integer) -> RealAffinity -> should become 0.0
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 0, 1, 0),
            VdbeOp::new(Opcode::RealAffinity, 1, 0, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_realaffinity_negative() {
    // r[1]=-7 (integer) -> RealAffinity -> should become -7.0
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, -7, 1, 0),
            VdbeOp::new(Opcode::RealAffinity, 1, 0, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

// ============================================================================
// Move: move registers (zeroing source)
// ============================================================================

#[test]
fn test_move_single() {
    // Move P1=1 P2=2 P3=1: move r[1] -> r[2], r[1] becomes NULL
    // Output both r[1] (should be NULL) and r[2] (should be 10)
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 10, 1, 0),  // 0: r[1] = 10
            VdbeOp::new(Opcode::Move, 1, 2, 1),      // 1: move r[1] -> r[2]
            VdbeOp::new(Opcode::ResultRow, 1, 2, 0), // 2: output r[1], r[2]
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 3
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 2,
        ..Default::default()
    });
}

#[test]
fn test_move_multiple() {
    // Move P1=1 P2=3 P3=2: move r[1..2] -> r[3..4]
    // r[1]=10, r[2]=20 -> after move: r[1]=NULL, r[2]=NULL, r[3]=10, r[4]=20
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 10, 1, 0),  // 0: r[1] = 10
            VdbeOp::new(Opcode::Integer, 20, 2, 0),  // 1: r[2] = 20
            VdbeOp::new(Opcode::Move, 1, 3, 2),      // 2: move r[1..2] -> r[3..4]
            VdbeOp::new(Opcode::ResultRow, 3, 2, 0), // 3: output r[3], r[4]
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 4
        ],
        n_mem: 4,
        n_cursor: 0,
        n_col: 2,
        ..Default::default()
    });
}

#[test]
fn test_move_source_becomes_null() {
    // Verify that the source register is zeroed after Move
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 77, 1, 0),  // 0: r[1] = 77
            VdbeOp::new(Opcode::Move, 1, 2, 1),      // 1: move r[1] -> r[2]
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0), // 2: output r[1] (should be NULL)
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 3
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

// ============================================================================
// Copy: copy registers (source preserved)
// ============================================================================

#[test]
fn test_copy_single() {
    // Copy P1=1 P2=2 P3=0: copy 1 register from r[1] to r[2]
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 99, 1, 0),  // 0: r[1] = 99
            VdbeOp::new(Opcode::Copy, 1, 2, 0),      // 1: copy r[1] -> r[2]
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0), // 2: output r[2]
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 3
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_copy_preserves_source() {
    // Copy should NOT zero out the source register
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 55, 1, 0),  // 0: r[1] = 55
            VdbeOp::new(Opcode::Copy, 1, 2, 0),      // 1: copy r[1] -> r[2]
            VdbeOp::new(Opcode::ResultRow, 1, 2, 0), // 2: output r[1], r[2]
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 3
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 2,
        ..Default::default()
    });
}

// ============================================================================
// IntCopy: copy integer value only
// ============================================================================

#[test]
fn test_intcopy() {
    // IntCopy P1=1 P2=2: copy integer from r[1] to r[2]
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 7, 1, 0),   // 0: r[1] = 7
            VdbeOp::new(Opcode::IntCopy, 1, 2, 0),   // 1: intcopy r[1] -> r[2]
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0), // 2: output r[2]
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 3
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_intcopy_negative() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, -42, 1, 0),
            VdbeOp::new(Opcode::IntCopy, 1, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_intcopy_zero() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 0, 1, 0),
            VdbeOp::new(Opcode::IntCopy, 1, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

// ============================================================================
// SoftNull: set register to NULL
// ============================================================================

#[test]
fn test_softnull() {
    // SoftNull r[1] -> output should be NULL
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::SoftNull, 1, 0, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_softnull_overwrites_value() {
    // Set r[1]=42 then SoftNull -> should become NULL
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::SoftNull, 1, 0, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

// ============================================================================
// IfPos: jump if register is positive
// ============================================================================

#[test]
fn test_ifpos_positive() {
    // IfPos P1=1 P2=3 P3=0: if r[1]>0 then r[1]-=P3, jump to P2
    // r[1]=3 (positive) -> should jump to addr 3 -> output 1
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 3, 1, 0),   // 0: r[1] = 3
            VdbeOp::new(Opcode::IfPos, 1, 4, 0),     // 1: if r[1]>0, jump to 4
            VdbeOp::new(Opcode::Integer, 0, 2, 0),   // 2: not jumped: r[2] = 0
            VdbeOp::new(Opcode::Goto, 0, 5, 0),      // 3: skip
            VdbeOp::new(Opcode::Integer, 1, 2, 0),   // 4: jumped: r[2] = 1
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0), // 5: output r[2]
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 6
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_ifpos_zero() {
    // r[1]=0 (not positive) -> should NOT jump -> output 0
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 0, 1, 0),   // 0: r[1] = 0
            VdbeOp::new(Opcode::IfPos, 1, 4, 0),     // 1: if r[1]>0, jump to 4
            VdbeOp::new(Opcode::Integer, 0, 2, 0),   // 2: not jumped: r[2] = 0
            VdbeOp::new(Opcode::Goto, 0, 5, 0),      // 3: skip
            VdbeOp::new(Opcode::Integer, 1, 2, 0),   // 4: jumped: r[2] = 1
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0), // 5: output r[2]
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 6
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_ifpos_negative() {
    // r[1]=-5 (negative) -> should NOT jump -> output 0
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, -5, 1, 0),  // 0: r[1] = -5
            VdbeOp::new(Opcode::IfPos, 1, 4, 0),     // 1: if r[1]>0, jump to 4
            VdbeOp::new(Opcode::Integer, 0, 2, 0),   // 2: not jumped: r[2] = 0
            VdbeOp::new(Opcode::Goto, 0, 5, 0),      // 3: skip
            VdbeOp::new(Opcode::Integer, 1, 2, 0),   // 4: jumped: r[2] = 1
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0), // 5: output r[2]
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 6
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_ifpos_with_decrement() {
    // IfPos P1=1 P2=4 P3=1: if r[1]>0 then r[1]-=1, jump
    // r[1]=3 -> after IfPos: r[1]=2, jumped
    // Output r[1] to verify decrement
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 3, 1, 0),   // 0: r[1] = 3
            VdbeOp::new(Opcode::IfPos, 1, 3, 1),     // 1: if r[1]>0, r[1]-=1, jump to 3
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 2: not reached
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0), // 3: output r[1] (should be 2)
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 4
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

// ============================================================================
// DecrJumpZero: decrement and jump if zero
// ============================================================================

#[test]
fn test_decrjumpzero_hits_zero() {
    // r[1]=1 -> decrement -> 0 -> should jump
    // Output 1 at jump target to confirm jump was taken
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 1, 1, 0),      // 0: r[1] = 1
            VdbeOp::new(Opcode::DecrJumpZero, 1, 4, 0), // 1: r[1]-=1, jump to 4 if zero
            VdbeOp::new(Opcode::Integer, 0, 2, 0),      // 2: not jumped: r[2] = 0
            VdbeOp::new(Opcode::Goto, 0, 5, 0),         // 3: skip
            VdbeOp::new(Opcode::Integer, 1, 2, 0),      // 4: jumped: r[2] = 1
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),    // 5: output
            VdbeOp::new(Opcode::Halt, 0, 0, 0),         // 6
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_decrjumpzero_not_zero() {
    // r[1]=5 -> decrement -> 4 -> should NOT jump
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 5, 1, 0),      // 0: r[1] = 5
            VdbeOp::new(Opcode::DecrJumpZero, 1, 4, 0), // 1: r[1]-=1, jump to 4 if zero
            VdbeOp::new(Opcode::Integer, 0, 2, 0),      // 2: not jumped: r[2] = 0
            VdbeOp::new(Opcode::Goto, 0, 5, 0),         // 3: skip
            VdbeOp::new(Opcode::Integer, 1, 2, 0),      // 4: jumped: r[2] = 1
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),    // 5: output
            VdbeOp::new(Opcode::Halt, 0, 0, 0),         // 6
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_decrjumpzero_countdown_loop() {
    // Count down from 3 to 0, outputting a row each iteration
    // This also tests that the register value is correctly decremented
    // 0: r[1]=3
    // 1: r[2]=r[1] (copy current value for output)
    // 2: ResultRow r[2]
    // 3: DecrJumpZero r[1] 5 (jump to halt when zero)
    // 4: Goto 0 1 0 (loop back)
    // 5: Halt
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 3, 1, 0),      // 0: r[1] = 3
            VdbeOp::new(Opcode::SCopy, 1, 2, 0),        // 1: r[2] = r[1]
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),    // 2: output r[2]
            VdbeOp::new(Opcode::DecrJumpZero, 1, 5, 0), // 3: r[1]--, jump 5 if zero
            VdbeOp::new(Opcode::Goto, 0, 1, 0),         // 4: loop
            VdbeOp::new(Opcode::Halt, 0, 0, 0),         // 5: done
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

// ============================================================================
// Once: fall through on first execution, jump on subsequent
// ============================================================================

#[test]
fn test_once_first_execution() {
    // Once P1=0 P2=3: first time -> fall through -> output 1
    // (Since program only runs once, should always fall through)
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Once, 0, 3, 0), // 0: first time, fall through
            VdbeOp::new(Opcode::Integer, 1, 1, 0), // 1: r[1] = 1
            VdbeOp::new(Opcode::Goto, 0, 4, 0), // 2: skip
            VdbeOp::new(Opcode::Integer, 0, 1, 0), // 3: r[1] = 0 (if jumped)
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0), // 4: output
            VdbeOp::new(Opcode::Halt, 0, 0, 0), // 5
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_once_second_execution() {
    // Run Once twice via a loop to verify it jumps on second encounter.
    // Note: backward Goto must NOT target address 0 because the C framework
    // inserts Init at addr 0 and only adjusts P2 values > 0.
    // We use a Noop at addr 0 so the loop target (addr 1) gets adjusted.
    //
    // 0: Noop              (padding so loop target is > 0)
    // 1: Once 0 4          (first time: fall through; second time: jump to 4)
    // 2: Integer 1 -> r[1] (first pass marker)
    // 3: Goto 0 1          (loop back to Once)
    // 4: ResultRow r[1]    (output after Once jumps here on second encounter)
    // 5: Halt
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Noop, 0, 0, 0),      // 0: padding
            VdbeOp::new(Opcode::Once, 0, 4, 0),      // 1: once
            VdbeOp::new(Opcode::Integer, 1, 1, 0),   // 2: r[1] = 1 (first pass)
            VdbeOp::new(Opcode::Goto, 0, 1, 0),      // 3: loop back to Once
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0), // 4: output r[1]
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 5
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

// ============================================================================
// Combined: register operations chained together
// ============================================================================

#[test]
fn test_move_then_copy() {
    // r[1]=100 -> Move r[1]->r[2] (r[1]=NULL) -> Copy r[2]->r[3]
    // Output all three: r[1]=NULL, r[2]=100, r[3]=100
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 100, 1, 0), // 0: r[1] = 100
            VdbeOp::new(Opcode::Move, 1, 2, 1),      // 1: move r[1]->r[2]
            VdbeOp::new(Opcode::Copy, 2, 3, 0),      // 2: copy r[2]->r[3]
            VdbeOp::new(Opcode::ResultRow, 1, 3, 0), // 3: output r[1..3]
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 4
        ],
        n_mem: 3,
        n_cursor: 0,
        n_col: 3,
        ..Default::default()
    });
}

#[test]
fn test_cast_chain() {
    // Integer 42 -> Cast to TEXT -> Cast to REAL -> output
    // 42 -> "42" -> 42.0
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),  // 0: r[1] = 42
            VdbeOp::new(Opcode::Cast, 1, 66, 0),     // 1: cast to TEXT ('B')
            VdbeOp::new(Opcode::Cast, 1, 69, 0),     // 2: cast to REAL ('E')
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0), // 3: output
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 4
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_intcopy_after_addimm() {
    // r[1]=10 -> AddImm r[1]+=5 -> IntCopy r[1]->r[2] -> output r[2]
    // Expected: 15
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 10, 1, 0),  // 0: r[1] = 10
            VdbeOp::new(Opcode::AddImm, 1, 5, 0),    // 1: r[1] += 5
            VdbeOp::new(Opcode::IntCopy, 1, 2, 0),   // 2: intcopy r[1]->r[2]
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0), // 3: output r[2]
            VdbeOp::new(Opcode::Halt, 0, 0, 0),      // 4
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_realaffinity_then_add() {
    // r[1]=10, RealAffinity -> 10.0, r[2]=3, Add -> 13.0
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 10, 1, 0),     // 0: r[1] = 10
            VdbeOp::new(Opcode::RealAffinity, 1, 0, 0), // 1: r[1] -> 10.0
            VdbeOp::new(Opcode::Integer, 3, 2, 0),      // 2: r[2] = 3
            VdbeOp::new(Opcode::Add, 2, 1, 3),          // 3: r[3] = r[1] + r[2]
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),    // 4: output r[3]
            VdbeOp::new(Opcode::Halt, 0, 0, 0),         // 5
        ],
        n_mem: 3,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_cast_non_numeric_text_to_integer() {
    // Cast "hello" to INTEGER -> should become 0
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("hello".into())),
            VdbeOp::new(Opcode::Cast, 1, 68, 0), // 'D' = INTEGER
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_cast_non_numeric_text_to_real() {
    // Cast "hello" to REAL -> should become 0.0
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("hello".into())),
            VdbeOp::new(Opcode::Cast, 1, 69, 0), // 'E' = REAL
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}
