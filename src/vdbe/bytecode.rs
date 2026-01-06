//! Bytecode Virtual Table
//!
//! This module implements a virtual table that exposes VDBE bytecode for
//! debugging and introspection. It corresponds to SQLite's vdbevtab.c.
//!
//! Usage:
//! ```sql
//! SELECT * FROM bytecode('SELECT * FROM users WHERE id = 5');
//! ```

use crate::vdbe::ops::{VdbeOp, Opcode, P4};

// ============================================================================
// Bytecode Row
// ============================================================================

/// A single row from the bytecode virtual table
#[derive(Debug, Clone)]
pub struct BytecodeRow {
    /// Instruction address (0-based)
    pub addr: i32,
    /// Opcode name
    pub opcode: String,
    /// First operand
    pub p1: i32,
    /// Second operand
    pub p2: i32,
    /// Third operand
    pub p3: i32,
    /// Fourth operand (formatted as text)
    pub p4: String,
    /// Fifth operand
    pub p5: u16,
    /// Instruction comment (if available)
    pub comment: Option<String>,
    /// Subprogram name (for triggers)
    pub subprog: Option<String>,
}

impl BytecodeRow {
    /// Create a bytecode row from a VDBE operation
    pub fn from_op(addr: i32, op: &VdbeOp) -> Self {
        Self {
            addr,
            opcode: format!("{:?}", op.opcode),
            p1: op.p1,
            p2: op.p2,
            p3: op.p3,
            p4: format_p4(&op.p4),
            p5: op.p5,
            comment: op.comment.clone(),
            subprog: None,
        }
    }
}

/// Format P4 operand as text
fn format_p4(p4: &P4) -> String {
    match p4 {
        P4::Unused => String::new(),
        P4::Int64(i) => i.to_string(),
        P4::Real(r) => r.to_string(),
        P4::Text(s) => format!("'{}'", s.replace('\'', "''")),
        P4::Blob(b) => format!("blob({})", b.len()),
        P4::Collation(c) => format!("collseq({})", c),
        P4::FuncDef(f) => format!("func({})", f),
        P4::KeyInfo(k) => format!("k({})", k.n_key_field),
        P4::Mem(m) => format!("r[{}]", m),
        P4::Vtab(v) => format!("vtab({})", v),
        P4::Subprogram(_) => "subprog".to_string(),
        P4::Table(t) => format!("table({})", t),
        P4::IntArray(arr) => {
            let vals: Vec<String> = arr.iter().map(|i| i.to_string()).collect();
            format!("[{}]", vals.join(","))
        }
    }
}

// ============================================================================
// Bytecode Iterator
// ============================================================================

/// Iterator over bytecode operations
pub struct BytecodeIterator {
    /// The operations
    ops: Vec<BytecodeRow>,
    /// Current position
    pos: usize,
}

impl BytecodeIterator {
    /// Create a new bytecode iterator from a list of operations
    pub fn new(ops: &[VdbeOp]) -> Self {
        let rows: Vec<BytecodeRow> = ops
            .iter()
            .enumerate()
            .map(|(i, op)| BytecodeRow::from_op(i as i32, op))
            .collect();

        Self { ops: rows, pos: 0 }
    }

    /// Create an empty iterator
    pub fn empty() -> Self {
        Self {
            ops: Vec::new(),
            pos: 0,
        }
    }

    /// Check if at end
    pub fn eof(&self) -> bool {
        self.pos >= self.ops.len()
    }

    /// Get current row
    pub fn current(&self) -> Option<&BytecodeRow> {
        self.ops.get(self.pos)
    }

    /// Advance to next row
    pub fn next(&mut self) {
        if self.pos < self.ops.len() {
            self.pos += 1;
        }
    }

    /// Reset to beginning
    pub fn rewind(&mut self) {
        self.pos = 0;
    }

    /// Get total count
    pub fn count(&self) -> usize {
        self.ops.len()
    }
}

// ============================================================================
// EXPLAIN Output
// ============================================================================

/// Format bytecode as EXPLAIN output
pub fn explain_bytecode(ops: &[VdbeOp]) -> String {
    let mut output = String::new();

    output.push_str("addr  opcode         p1    p2    p3    p4             p5  comment\n");
    output.push_str("----  -------------  ----  ----  ----  -------------  --  -------\n");

    for (i, op) in ops.iter().enumerate() {
        let row = BytecodeRow::from_op(i as i32, op);
        output.push_str(&format!(
            "{:<4}  {:<13}  {:<4}  {:<4}  {:<4}  {:<13}  {:<2}  {}\n",
            row.addr,
            row.opcode,
            row.p1,
            row.p2,
            row.p3,
            row.p4,
            row.p5,
            row.comment.as_deref().unwrap_or("")
        ));
    }

    output
}

/// Format bytecode as EXPLAIN QUERY PLAN output
pub fn explain_query_plan(ops: &[VdbeOp]) -> Vec<(i32, i32, i32, String)> {
    // EXPLAIN QUERY PLAN returns rows with:
    // - id: The node ID
    // - parent: The parent node ID (0 for root)
    // - notused: Always 0
    // - detail: Description of the operation

    let mut result = Vec::new();
    let mut id = 0;

    for op in ops {
        match op.opcode {
            Opcode::OpenRead | Opcode::OpenWrite => {
                let detail = format!("SCAN TABLE {} USING INDEX", op.p2);
                result.push((id, 0, 0, detail));
                id += 1;
            }
            Opcode::SeekGE | Opcode::SeekGT | Opcode::SeekLE | Opcode::SeekLT => {
                let detail = "SEARCH TABLE USING INDEX".to_string();
                result.push((id, 0, 0, detail));
                id += 1;
            }
            Opcode::SorterSort | Opcode::SorterData => {
                // Sorter operations indicate ORDER BY
                let detail = "USE TEMP B-TREE FOR ORDER BY".to_string();
                result.push((id, 0, 0, detail));
                id += 1;
            }
            Opcode::OpenEphemeral => {
                let detail = "USE TEMP B-TREE FOR DISTINCT".to_string();
                result.push((id, 0, 0, detail));
                id += 1;
            }
            _ => {}
        }
    }

    result
}

// ============================================================================
// Schema
// ============================================================================

/// Get the schema for the bytecode virtual table
pub fn bytecode_schema() -> &'static str {
    "CREATE TABLE bytecode(
        addr INTEGER,
        opcode TEXT,
        p1 INTEGER,
        p2 INTEGER,
        p3 INTEGER,
        p4 TEXT,
        p5 INTEGER,
        comment TEXT,
        subprog TEXT
    )"
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::vdbe::ops::KeyInfo;

    #[test]
    fn test_bytecode_row_from_op() {
        let op = VdbeOp {
            opcode: Opcode::Integer,
            p1: 42,
            p2: 1,
            p3: 0,
            p4: P4::Unused,
            p5: 0,
            comment: Some("load constant".to_string()),
        };

        let row = BytecodeRow::from_op(0, &op);
        assert_eq!(row.addr, 0);
        assert_eq!(row.opcode, "Integer");
        assert_eq!(row.p1, 42);
        assert_eq!(row.p2, 1);
        assert_eq!(row.comment, Some("load constant".to_string()));
    }

    #[test]
    fn test_format_p4() {
        assert_eq!(format_p4(&P4::Unused), "");
        assert_eq!(format_p4(&P4::Int64(42)), "42");
        assert_eq!(format_p4(&P4::Text("hello".to_string())), "'hello'");
        assert_eq!(format_p4(&P4::Collation("BINARY".to_string())), "collseq(BINARY)");

        let key_info = Arc::new(KeyInfo::new(3));
        assert_eq!(format_p4(&P4::KeyInfo(key_info)), "k(3)");
    }

    #[test]
    fn test_bytecode_iterator() {
        let ops = vec![
            VdbeOp::new(Opcode::Init, 0, 5, 0),
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ];

        let mut iter = BytecodeIterator::new(&ops);

        assert!(!iter.eof());
        assert_eq!(iter.current().unwrap().opcode, "Init");

        iter.next();
        assert_eq!(iter.current().unwrap().opcode, "Integer");

        iter.next();
        iter.next();
        iter.next();
        assert!(iter.eof());
    }

    #[test]
    fn test_bytecode_iterator_rewind() {
        let ops = vec![
            VdbeOp::new(Opcode::Init, 0, 5, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ];

        let mut iter = BytecodeIterator::new(&ops);

        iter.next();
        iter.next();
        assert!(iter.eof());

        iter.rewind();
        assert!(!iter.eof());
        assert_eq!(iter.current().unwrap().opcode, "Init");
    }

    #[test]
    fn test_explain_bytecode() {
        let ops = vec![
            VdbeOp::new(Opcode::Init, 0, 3, 0),
            VdbeOp::new(Opcode::Integer, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ];

        let output = explain_bytecode(&ops);
        assert!(output.contains("Init"));
        assert!(output.contains("Integer"));
        assert!(output.contains("Halt"));
    }

    #[test]
    fn test_bytecode_schema() {
        let schema = bytecode_schema();
        assert!(schema.contains("CREATE TABLE bytecode"));
        assert!(schema.contains("addr INTEGER"));
        assert!(schema.contains("opcode TEXT"));
    }
}
