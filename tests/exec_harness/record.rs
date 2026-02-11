//! Record and type-related opcode tests: MakeRecord, Column, Cast, MustBeInt,
//! RealAffinity, Copy, Move.
//!
//! All programs use linear layout (no Init/Goto envelope).

use super::*;

// ============================================================================
// MakeRecord — encode registers into a record blob
// ============================================================================

#[test]
fn test_makerecord_single_integer() {
    // r[1]=42, MakeRecord r[1]..r[1] -> r[2], output r[2] as blob
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::MakeRecord, 1, 1, 2),
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
fn test_makerecord_multiple_types() {
    // r[1]=42 (int), r[2]="hello" (text), r[3]=NULL, MakeRecord r[1]..r[3] -> r[4]
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::with_p4(Opcode::String8, 0, 2, 0, P4::Text("hello".into())),
            VdbeOp::new(Opcode::Null, 0, 3, 0),
            VdbeOp::new(Opcode::MakeRecord, 1, 3, 4),
            VdbeOp::new(Opcode::ResultRow, 4, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 4,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_makerecord_real_value() {
    // r[1]=3.125 (real), MakeRecord r[1]..r[1] -> r[2]
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::Real, 0, 1, 0, P4::Real(3.125)),
            VdbeOp::new(Opcode::MakeRecord, 1, 1, 2),
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
fn test_makerecord_all_nulls() {
    // r[1]=NULL, r[2]=NULL, MakeRecord r[1]..r[2] -> r[3]
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Null, 0, 1, 2),
            VdbeOp::new(Opcode::MakeRecord, 1, 2, 3),
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
// MakeRecord + OpenPseudo + Column round-trip
// ============================================================================

#[test]
fn test_makerecord_column_roundtrip_integer() {
    // r[1]=42, MakeRecord r[1]..r[1] -> r[2]
    // OpenPseudo cursor 0 on r[2] with 1 column
    // Column cursor 0, col 0 -> r[3]
    // ResultRow r[3]
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::MakeRecord, 1, 1, 2),
            VdbeOp::new(Opcode::OpenPseudo, 0, 2, 1),
            VdbeOp::new(Opcode::Column, 0, 0, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 1,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_makerecord_column_roundtrip_string() {
    // r[1]="hello", MakeRecord -> r[2], OpenPseudo, Column -> r[3], output
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("hello".into())),
            VdbeOp::new(Opcode::MakeRecord, 1, 1, 2),
            VdbeOp::new(Opcode::OpenPseudo, 0, 2, 1),
            VdbeOp::new(Opcode::Column, 0, 0, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 1,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_makerecord_column_roundtrip_null() {
    // r[1]=NULL, MakeRecord -> r[2], OpenPseudo, Column -> r[3], output
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Null, 0, 1, 0),
            VdbeOp::new(Opcode::MakeRecord, 1, 1, 2),
            VdbeOp::new(Opcode::OpenPseudo, 0, 2, 1),
            VdbeOp::new(Opcode::Column, 0, 0, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 1,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_makerecord_column_roundtrip_real() {
    // r[1]=3.125, MakeRecord -> r[2], OpenPseudo, Column -> r[3], output
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::Real, 0, 1, 0, P4::Real(3.125)),
            VdbeOp::new(Opcode::MakeRecord, 1, 1, 2),
            VdbeOp::new(Opcode::OpenPseudo, 0, 2, 1),
            VdbeOp::new(Opcode::Column, 0, 0, 3),
            VdbeOp::new(Opcode::ResultRow, 3, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 1,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_makerecord_column_roundtrip_multi_column() {
    // r[1]=42, r[2]="world", r[3]=NULL
    // MakeRecord r[1]..r[3] -> r[4]
    // OpenPseudo cursor 0 on r[4] with 3 columns
    // Column cursor 0, col 0 -> r[5]  (integer)
    // Column cursor 0, col 1 -> r[6]  (text)
    // Column cursor 0, col 2 -> r[7]  (null)
    // ResultRow r[5], 3
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::with_p4(Opcode::String8, 0, 2, 0, P4::Text("world".into())),
            VdbeOp::new(Opcode::Null, 0, 3, 0),
            VdbeOp::new(Opcode::MakeRecord, 1, 3, 4),
            VdbeOp::new(Opcode::OpenPseudo, 0, 4, 3),
            VdbeOp::new(Opcode::Column, 0, 0, 5),
            VdbeOp::new(Opcode::Column, 0, 1, 6),
            VdbeOp::new(Opcode::Column, 0, 2, 7),
            VdbeOp::new(Opcode::ResultRow, 5, 3, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 7,
        n_cursor: 1,
        n_col: 3,
        ..Default::default()
    });
}

// ============================================================================
// Cast opcode — P1=register, P2=affinity character
// Affinity chars: 'A'=65 (blob), 'B'=66 (text), 'C'=67 (numeric),
//                 'D'=68 (integer), 'E'=69 (real)
// ============================================================================

#[test]
fn test_cast_integer_to_text() {
    // r[1]=42, Cast r[1] to text (affinity 'B'=66), output
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::Cast, 1, 66, 0),
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
    // r[1]="123", Cast r[1] to integer (affinity 'D'=68), output
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("123".into())),
            VdbeOp::new(Opcode::Cast, 1, 68, 0),
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
    // r[1]=42, Cast r[1] to real (affinity 'E'=69), output
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::Cast, 1, 69, 0),
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
    // r[1]=3.125, Cast r[1] to integer (affinity 'D'=68), output -> 3
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::Real, 0, 1, 0, P4::Real(3.125)),
            VdbeOp::new(Opcode::Cast, 1, 68, 0),
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
    // r[1]="3.125", Cast r[1] to real (affinity 'E'=69), output
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("3.125".into())),
            VdbeOp::new(Opcode::Cast, 1, 69, 0),
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
fn test_cast_text_to_blob() {
    // r[1]="hello", Cast r[1] to blob (affinity 'A'=65), output
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("hello".into())),
            VdbeOp::new(Opcode::Cast, 1, 65, 0),
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
    // r[1]=NULL, Cast r[1] to integer (affinity 'D'=68) -> stays NULL
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Null, 0, 1, 0),
            VdbeOp::new(Opcode::Cast, 1, 68, 0),
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
fn test_cast_numeric_string_to_numeric() {
    // r[1]="456", Cast to numeric (affinity 'C'=67) -> integer 456
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("456".into())),
            VdbeOp::new(Opcode::Cast, 1, 67, 0),
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
fn test_cast_real_string_to_numeric() {
    // r[1]="2.5", Cast to numeric (affinity 'C'=67) -> real 2.5
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("2.5".into())),
            VdbeOp::new(Opcode::Cast, 1, 67, 0),
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
// MustBeInt — P1=register, P2=jump target if not convertible to integer
// ============================================================================

#[test]
fn test_mustbeint_with_integer() {
    // r[1]=42, MustBeInt r[1] (jump to 4 on fail)
    // Falls through on success, output r[1]
    // 0: Integer 42 -> r[1]
    // 1: MustBeInt r[1], jump=4 on fail
    // 2: ResultRow r[1], 1
    // 3: Halt
    // 4: Integer -1 -> r[1]  (failure path)
    // 5: ResultRow r[1], 1
    // 6: Halt
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::MustBeInt, 1, 4, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
            VdbeOp::new(Opcode::Integer, -1, 1, 0),
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
fn test_mustbeint_with_real_integer() {
    // r[1]=5.0 (exact integer as real), MustBeInt should convert to 5
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::Real, 0, 1, 0, P4::Real(5.0)),
            VdbeOp::new(Opcode::MustBeInt, 1, 4, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
            VdbeOp::new(Opcode::Integer, -1, 1, 0),
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
fn test_mustbeint_with_non_integer_real() {
    // r[1]=3.125 (not an exact integer), MustBeInt should jump to failure path
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::Real, 0, 1, 0, P4::Real(3.125)),
            VdbeOp::new(Opcode::MustBeInt, 1, 4, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
            VdbeOp::new(Opcode::Integer, -1, 1, 0),
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
fn test_mustbeint_with_text_integer() {
    // r[1]="100" (text that looks like integer), MustBeInt should convert to 100
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("100".into())),
            VdbeOp::new(Opcode::MustBeInt, 1, 4, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
            VdbeOp::new(Opcode::Integer, -1, 1, 0),
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
// RealAffinity — P1=register. If integer, convert to real.
// ============================================================================

#[test]
fn test_realaffinity_integer_to_real() {
    // r[1]=42 (integer), RealAffinity -> 42.0 (real)
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
    // r[1]=3.125 (already real), RealAffinity -> still 3.125
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
fn test_realaffinity_null_unchanged() {
    // r[1]=NULL, RealAffinity -> stays NULL
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
    // r[1]=0 (integer), RealAffinity -> 0.0 (real)
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
fn test_realaffinity_negative_integer() {
    // r[1]=-7 (integer), RealAffinity -> -7.0 (real)
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
// Copy — P1=src, P2=dest, P3=additional count (copy P3+1 registers)
// ============================================================================

#[test]
fn test_copy_single_register() {
    // r[1]=42, Copy r[1] -> r[2] (P3=0 means copy 1 register)
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::Copy, 1, 2, 0),
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
fn test_copy_multiple_registers() {
    // r[1]=10, r[2]=20, r[3]=30
    // Copy r[1]..r[3] -> r[4]..r[6] (P3=2 means copy 3 registers)
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 10, 1, 0),
            VdbeOp::new(Opcode::Integer, 20, 2, 0),
            VdbeOp::new(Opcode::Integer, 30, 3, 0),
            VdbeOp::new(Opcode::Copy, 1, 4, 2),
            VdbeOp::new(Opcode::ResultRow, 4, 3, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 6,
        n_cursor: 0,
        n_col: 3,
        ..Default::default()
    });
}

#[test]
fn test_copy_string_value() {
    // r[1]="hello", Copy r[1] -> r[2]
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("hello".into())),
            VdbeOp::new(Opcode::Copy, 1, 2, 0),
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
fn test_copy_null_value() {
    // r[1]=NULL, Copy r[1] -> r[2]
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Null, 0, 1, 0),
            VdbeOp::new(Opcode::Copy, 1, 2, 0),
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
fn test_copy_is_deep() {
    // Verify Copy is a deep copy: modifying source after copy doesn't affect dest
    // r[1]=42, Copy r[1] -> r[2], then change r[1] to 99, output both
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::Copy, 1, 2, 0),
            VdbeOp::new(Opcode::Integer, 99, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 2, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 2,
        ..Default::default()
    });
}

// ============================================================================
// Move — P1=src, P2=dest, P3=count. Move P3 registers; source becomes NULL.
// ============================================================================

#[test]
fn test_move_single_register() {
    // r[1]=42, Move r[1] -> r[2] (P3=1), output r[1] and r[2]
    // After move: r[1]=NULL, r[2]=42
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::Move, 1, 2, 1),
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
fn test_move_multiple_registers() {
    // r[1]=10, r[2]=20, r[3]=30
    // Move r[1]..r[3] -> r[4]..r[6] (P3=3)
    // After: r[1..3]=NULL, r[4..6]=10,20,30
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 10, 1, 0),
            VdbeOp::new(Opcode::Integer, 20, 2, 0),
            VdbeOp::new(Opcode::Integer, 30, 3, 0),
            VdbeOp::new(Opcode::Move, 1, 4, 3),
            // Output dest registers (should have values)
            VdbeOp::new(Opcode::ResultRow, 4, 3, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 6,
        n_cursor: 0,
        n_col: 3,
        ..Default::default()
    });
}

#[test]
fn test_move_source_becomes_null() {
    // r[1]=42, Move r[1] -> r[2], then output r[1] (should be NULL)
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::Move, 1, 2, 1),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}

#[test]
fn test_move_string_value() {
    // r[1]="hello", Move r[1] -> r[2], output r[2]
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::String8, 0, 1, 0, P4::Text("hello".into())),
            VdbeOp::new(Opcode::Move, 1, 2, 1),
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
fn test_move_real_value() {
    // r[1]=2.875, Move r[1] -> r[2], output r[2]
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::with_p4(Opcode::Real, 0, 1, 0, P4::Real(2.875)),
            VdbeOp::new(Opcode::Move, 1, 2, 1),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}
