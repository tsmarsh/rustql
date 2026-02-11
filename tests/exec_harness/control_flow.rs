//! Control flow opcode tests: Goto, If, IfNot, IfPos
//! All programs use linear layout (no Init/Goto envelope).

use super::*;

// Helper for branch tests: set r[1], branch, output 0 or 1
fn branch_program(branch_opcode: Opcode, reg_val_is_truthy: bool) -> VdbeProgram {
    let val = if reg_val_is_truthy { 1 } else { 0 };
    // 0: Integer val -> r[1]
    // 1: <branch> r[1] 4  -- jump to 4 if condition
    // 2: Integer 0 -> r[2]
    // 3: Goto 0 5 0
    // 4: Integer 1 -> r[2]
    // 5: ResultRow r[2], 1
    // 6: Halt
    VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, val, 1, 0),
            VdbeOp::new(branch_opcode, 1, 4, 0),
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
fn test_if_true() {
    assert_vdbe_match!(branch_program(Opcode::If, true));
}

#[test]
fn test_if_false() {
    assert_vdbe_match!(branch_program(Opcode::If, false));
}

#[test]
fn test_ifnot_true() {
    // IfNot jumps when value is false (0)
    assert_vdbe_match!(branch_program(Opcode::IfNot, false));
}

#[test]
fn test_ifnot_false() {
    // IfNot does NOT jump when value is true (1)
    assert_vdbe_match!(branch_program(Opcode::IfNot, true));
}

// ============================================================================
// Multiple result rows (loop)
// ============================================================================

#[test]
fn test_multiple_result_rows() {
    // Output three rows: 1, 2, 3
    assert_vdbe_match!(VdbeProgram {
        ops: vec![
            VdbeOp::new(Opcode::Integer, 1, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Integer, 2, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Integer, 3, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ],
        n_mem: 1,
        n_cursor: 0,
        n_col: 1,
        ..Default::default()
    });
}
