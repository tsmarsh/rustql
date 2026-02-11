//! Basic VDBE execution tests: literals, arithmetic, string ops, NULL handling
//!
//! These tests construct opcode programs manually and verify both
//! C SQLite3 and RustQL VDBE produce identical results.
//!
//! Programs use linear layout (no Init/Goto envelope) for simplicity.

use super::*;

// ============================================================================
// Integer literals
// ============================================================================

#[test]
fn test_integer_literal_42() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
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
fn test_integer_literal_zero() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 0, 1, 0),
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
fn test_integer_literal_negative() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, -99, 1, 0),
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
fn test_int64_literal() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::Int64, 0, 1, 0, P4::Int64(9_999_999_999i64)),
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
// NULL
// ============================================================================

#[test]
fn test_null_literal() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Null, 0, 1, 0),
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
// Real (float) literals
// ============================================================================

#[test]
fn test_real_literal() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::Real, 0, 1, 0, P4::Real(3.125)),
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
// String literals
// ============================================================================

#[test]
fn test_string_literal() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("hello".into())),
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
fn test_string_empty() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("".into())),
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
// Arithmetic: Add, Subtract, Multiply, Divide, Remainder
// ============================================================================

#[test]
fn test_add_integers() {
    // r[1]=10, r[2]=3, r[3]=r[1]+r[2]=13
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 10, 1, 0),
            VdbeOp::new(Opcode::Integer, 3, 2, 0),
            VdbeOp::new(Opcode::Add, 2, 1, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_subtract_integers() {
    // Subtract P1 P2 P3: r[P3]=r[P2]-r[P1]
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 3, 1, 0),
            VdbeOp::new(Opcode::Integer, 10, 2, 0),
            VdbeOp::new(Opcode::Subtract, 1, 2, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_multiply_integers() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 7, 1, 0),
            VdbeOp::new(Opcode::Integer, 6, 2, 0),
            VdbeOp::new(Opcode::Multiply, 2, 1, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_divide_integers() {
    // Divide P1 P2 P3: r[P3]=r[P2]/r[P1]
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 3, 1, 0),
            VdbeOp::new(Opcode::Integer, 10, 2, 0),
            VdbeOp::new(Opcode::Divide, 1, 2, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_remainder_integers() {
    // Remainder P1 P2 P3: r[P3]=r[P2]%r[P1]
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 3, 1, 0),
            VdbeOp::new(Opcode::Integer, 10, 2, 0),
            VdbeOp::new(Opcode::Remainder, 1, 2, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

// ============================================================================
// NULL arithmetic (NULL propagation)
// ============================================================================

#[test]
fn test_null_plus_integer() {
    // r[1]=NULL, r[2]=5, r[3]=r[1]+r[2] should be NULL
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Null, 0, 1, 0),
            VdbeOp::new(Opcode::Integer, 5, 2, 0),
            VdbeOp::new(Opcode::Add, 2, 1, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_divide_by_zero() {
    // r[1]=0, r[2]=10, r[3]=r[2]/r[1] should be NULL
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 0, 1, 0),
            VdbeOp::new(Opcode::Integer, 10, 2, 0),
            VdbeOp::new(Opcode::Divide, 1, 2, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

// ============================================================================
// Concat
// ============================================================================

#[test]
fn test_concat_strings() {
    // Concat P1 P2 P3: r[P3]=r[P2]||r[P1]
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("hello".into())),
            VdbeOp::with_p4(Opcode::String8, 0, 2, 0, P4::Text(" world".into())),
            VdbeOp::new(Opcode::Concat, 2, 1, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

// ============================================================================
// Multiple columns in result row
// ============================================================================

#[test]
fn test_multi_column_result() {
    // r[1]=42, r[2]="hello", r[3]=NULL — output all three
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::with_p4(Opcode::String8, 0, 2, 0, P4::Text("hello".into())),
            VdbeOp::new(Opcode::Null, 0, 3, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 3, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 0,
        n_col: 3,
        ..Default::default()
    });
}

// ============================================================================
// Bitwise operations
// ============================================================================

#[test]
fn test_bitand() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 0xFF, 1, 0),
            VdbeOp::new(Opcode::Integer, 0x0F, 2, 0),
            VdbeOp::new(Opcode::BitAnd, 2, 1, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_bitor() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 0xF0, 1, 0),
            VdbeOp::new(Opcode::Integer, 0x0F, 2, 0),
            VdbeOp::new(Opcode::BitOr, 2, 1, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_bitnot() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 0, 1, 0),
            VdbeOp::new(Opcode::BitNot, 1, 2, 0),
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
// Logic: Not
// ============================================================================

#[test]
fn test_not_true() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 1, 1, 0),
            VdbeOp::new(Opcode::Not, 1, 2, 0),
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
fn test_not_false() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 0, 1, 0),
            VdbeOp::new(Opcode::Not, 1, 2, 0),
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
// Register operations: SCopy, AddImm
// ============================================================================

#[test]
fn test_scopy() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::SCopy, 1, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 2, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 2,
        ..Default::default()
    });
}

#[test]
fn test_addimm() {
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 10, 1, 0),
            VdbeOp::new(Opcode::AddImm, 1, 5, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}
