//! Comparison opcode tests: Eq, Ne, Lt, Le, Gt, Ge, IsNull, NotNull
//!
//! Comparison opcodes conditionally jump. To test, we use:
//! - Set values → compare (jump to 5 if true) → result=0, goto 6 → result=1 → ResultRow → Halt

use super::*;

// Helper: build a comparison test program (linear, no Init/Goto)
fn cmp_program(cmp_opcode: Opcode, p1_val: i32, p3_val: i32) -> VdbeProgram {
    // 0: Integer p1_val -> r[1]
    // 1: Integer p3_val -> r[3]
    // 2: <cmp> r[1] 5 r[3]   -- jump to 5 if condition true
    // 3: Integer 0 -> r[2]    -- not jumped: result=0
    // 4: Goto 0 6 0           -- skip to result
    // 5: Integer 1 -> r[2]    -- jumped: result=1
    // 6: ResultRow r[2], 1
    // 7: Halt
    VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, p1_val, 1, 0),
            VdbeOp::new(Opcode::Integer, p3_val, 3, 0),
            VdbeOp::new(cmp_opcode, 1, 5, 3),
            VdbeOp::new(Opcode::Integer, 0, 2, 0),
            VdbeOp::new(Opcode::Goto, 0, 6, 0),
            VdbeOp::new(Opcode::Integer, 1, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 3,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    }
}

#[test]
fn test_eq_true() {
    assert_vdbe_match!(cmp_program(Opcode::Eq, 5, 5));
}

#[test]
fn test_eq_false() {
    assert_vdbe_match!(cmp_program(Opcode::Eq, 5, 3));
}

#[test]
fn test_ne_true() {
    assert_vdbe_match!(cmp_program(Opcode::Ne, 5, 3));
}

#[test]
fn test_ne_false() {
    assert_vdbe_match!(cmp_program(Opcode::Ne, 5, 5));
}

#[test]
fn test_lt_true() {
    assert_vdbe_match!(cmp_program(Opcode::Lt, 5, 3));
}

#[test]
fn test_lt_false_equal() {
    assert_vdbe_match!(cmp_program(Opcode::Lt, 5, 5));
}

#[test]
fn test_lt_false_greater() {
    assert_vdbe_match!(cmp_program(Opcode::Lt, 3, 5));
}

#[test]
fn test_le_true_less() {
    assert_vdbe_match!(cmp_program(Opcode::Le, 5, 3));
}

#[test]
fn test_le_true_equal() {
    assert_vdbe_match!(cmp_program(Opcode::Le, 5, 5));
}

#[test]
fn test_le_false() {
    assert_vdbe_match!(cmp_program(Opcode::Le, 3, 5));
}

#[test]
fn test_gt_true() {
    assert_vdbe_match!(cmp_program(Opcode::Gt, 3, 5));
}

#[test]
fn test_gt_false_equal() {
    assert_vdbe_match!(cmp_program(Opcode::Gt, 5, 5));
}

#[test]
fn test_gt_false_less() {
    assert_vdbe_match!(cmp_program(Opcode::Gt, 5, 3));
}

#[test]
fn test_ge_true_greater() {
    assert_vdbe_match!(cmp_program(Opcode::Ge, 3, 5));
}

#[test]
fn test_ge_true_equal() {
    assert_vdbe_match!(cmp_program(Opcode::Ge, 5, 5));
}

#[test]
fn test_ge_false() {
    assert_vdbe_match!(cmp_program(Opcode::Ge, 5, 3));
}

// IsNull / NotNull helper
fn null_check_program(check_opcode: Opcode, use_null: bool) -> VdbeProgram {
    // 0: Null/Integer -> r[1]
    // 1: <check> r[1] 4    -- jump to 4 if condition true
    // 2: Integer 0 -> r[2]
    // 3: Goto 0 5 0
    // 4: Integer 1 -> r[2]
    // 5: ResultRow r[2], 1
    // 6: Halt
    let first_op = if use_null {
        VdbeOp::new(Opcode::Null, 0, 1, 0)
    } else {
        VdbeOp::new(Opcode::Integer, 42, 1, 0)
    };
    VdbeProgram {
        ops: vec![
            first_op,
            VdbeOp::new(check_opcode, 1, 4, 0),
            VdbeOp::new(Opcode::Integer, 0, 2, 0),
            VdbeOp::new(Opcode::Goto, 0, 5, 0),
            VdbeOp::new(Opcode::Integer, 1, 2, 0),
            VdbeOp::new(Opcode::ResultRow, 2, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 2,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    }
}

#[test]
fn test_isnull_true() {
    assert_vdbe_match!(null_check_program(Opcode::IsNull, true));
}

#[test]
fn test_isnull_false() {
    assert_vdbe_match!(null_check_program(Opcode::IsNull, false));
}

#[test]
fn test_notnull_true() {
    assert_vdbe_match!(null_check_program(Opcode::NotNull, false));
}

#[test]
fn test_notnull_false() {
    assert_vdbe_match!(null_check_program(Opcode::NotNull, true));
}
