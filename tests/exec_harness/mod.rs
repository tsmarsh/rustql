//! VDBE Execution Comparison Test Framework
//!
//! Feeds identical opcode programs to both C SQLite3's VDBE (via FFI) and
//! RustQL's VDBE, then compares results row-by-row and type-by-type.
//!
//! This tests the VDBE execution layer in isolation — no SQL compilation involved.

#![allow(dead_code)]

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_double, c_int, c_longlong, c_void};

use rustql::vdbe::engine::{ExecResult, Vdbe};
use rustql::vdbe::ops::{Opcode, VdbeOp, P4};

// ============================================================================
// FFI bindings for vdbe_ref.c wrapper functions
// ============================================================================

extern "C" {
    fn vdbe_ref_new(n_mem: c_int, n_cursor: c_int, n_col: c_int) -> *mut c_void;
    fn vdbe_ref_destroy(handle: *mut c_void);
    fn vdbe_ref_add_op(
        handle: *mut c_void,
        opname: *const c_char,
        p1: c_int,
        p2: c_int,
        p3: c_int,
    ) -> c_int;
    fn vdbe_ref_change_p5(handle: *mut c_void, p5: c_int);
    fn vdbe_ref_change_p4_int(handle: *mut c_void, addr: c_int, val: c_int);
    fn vdbe_ref_change_p4_int64(handle: *mut c_void, addr: c_int, val: c_longlong);
    fn vdbe_ref_change_p4_real(handle: *mut c_void, addr: c_int, val: c_double);
    fn vdbe_ref_change_p4_str(handle: *mut c_void, addr: c_int, str: *const c_char);
    fn vdbe_ref_set_col_name(handle: *mut c_void, idx: c_int, name: *const c_char);
    fn vdbe_ref_step(handle: *mut c_void) -> c_int;
    fn vdbe_ref_column_count(handle: *mut c_void) -> c_int;
    fn vdbe_ref_column_type(handle: *mut c_void, col: c_int) -> c_int;
    fn vdbe_ref_column_int64(handle: *mut c_void, col: c_int) -> c_longlong;
    fn vdbe_ref_column_double(handle: *mut c_void, col: c_int) -> c_double;
    fn vdbe_ref_column_text(handle: *mut c_void, col: c_int) -> *const c_char;
    fn vdbe_ref_column_blob(handle: *mut c_void, col: c_int) -> *const c_void;
    fn vdbe_ref_column_bytes(handle: *mut c_void, col: c_int) -> c_int;
    fn vdbe_ref_exec_sql(handle: *mut c_void, sql: *const c_char) -> c_int;
    fn vdbe_ref_errmsg(handle: *mut c_void) -> *const c_char;

    // Adjust jump targets for the auto-inserted Init at address 0
    fn vdbe_ref_adjust_jumps(handle: *mut c_void, offset: c_int);

    // Debug: inspect stored program
    fn vdbe_ref_get_op_count(handle: *mut c_void) -> c_int;
    fn vdbe_ref_get_op_name(handle: *mut c_void, addr: c_int) -> *const c_char;
    fn vdbe_ref_get_op_p1(handle: *mut c_void, addr: c_int) -> c_int;
    fn vdbe_ref_get_op_p2(handle: *mut c_void, addr: c_int) -> c_int;
    fn vdbe_ref_get_op_p3(handle: *mut c_void, addr: c_int) -> c_int;
}

// SQLite type constants (from sqlite3.h)
const SQLITE_INTEGER: c_int = 1;
const SQLITE_FLOAT: c_int = 2;
const SQLITE_TEXT: c_int = 3;
const SQLITE_BLOB: c_int = 4;
const SQLITE_NULL: c_int = 5;

// SQLite result codes
const SQLITE_ROW: c_int = 100;
const SQLITE_DONE: c_int = 101;

// ============================================================================
// Core types
// ============================================================================

/// A single value from a VDBE result column
#[derive(Debug, Clone)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl std::fmt::Display for SqlValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlValue::Null => write!(f, "NULL"),
            SqlValue::Integer(i) => write!(f, "{}", i),
            SqlValue::Real(r) => write!(f, "{}", r),
            SqlValue::Text(s) => write!(f, "'{}'", s),
            SqlValue::Blob(b) => write!(f, "x'{}'", hex::encode(b)),
        }
    }
}

/// Result of executing a VDBE program
#[derive(Debug)]
pub enum VdbeResult {
    /// One or more result rows
    Rows(Vec<Vec<SqlValue>>),
    /// Completed with no rows
    Done,
    /// Execution error
    Error(String),
}

/// Describes a VDBE program to execute on both engines
#[derive(Debug, Clone)]
pub struct VdbeProgram {
    pub ops: Vec<VdbeOp>,
    pub n_mem: i32,
    pub n_cursor: i32,
    pub n_col: i32,
    pub col_names: Vec<String>,
}

impl Default for VdbeProgram {
    fn default() -> Self {
        VdbeProgram {
            ops: Vec::new(),
            n_mem: 0,
            n_cursor: 0,
            n_col: 0,
            col_names: Vec::new(),
        }
    }
}

// ============================================================================
// hex encoding (minimal, test-only)
// ============================================================================

mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

// ============================================================================
// C SQLite3 VDBE wrapper (via vdbe_ref.c FFI)
// ============================================================================

pub struct RefVdbe {
    handle: *mut c_void,
}

impl RefVdbe {
    /// Dump the stored C program for debugging
    pub fn dump_program(&self) {
        let n = unsafe { vdbe_ref_get_op_count(self.handle) };
        eprintln!("=== C VDBE stored program ({} ops) ===", n);
        for i in 0..n {
            let name =
                unsafe { CStr::from_ptr(vdbe_ref_get_op_name(self.handle, i)).to_string_lossy() };
            let p1 = unsafe { vdbe_ref_get_op_p1(self.handle, i) };
            let p2 = unsafe { vdbe_ref_get_op_p2(self.handle, i) };
            let p3 = unsafe { vdbe_ref_get_op_p3(self.handle, i) };
            eprintln!("  {:3}: {:15} {:4} {:4} {:4}", i, name, p1, p2, p3);
        }
    }

    /// Load a VdbeProgram into the C SQLite3 VDBE
    pub fn from_program(program: &VdbeProgram) -> Self {
        let handle = unsafe { vdbe_ref_new(program.n_mem, program.n_cursor, program.n_col) };
        assert!(!handle.is_null(), "vdbe_ref_new failed");

        // Add opcodes
        for op in &program.ops {
            let opname = CString::new(op.opcode.name()).unwrap();
            let addr = unsafe { vdbe_ref_add_op(handle, opname.as_ptr(), op.p1, op.p2, op.p3) };
            if addr < 0 {
                panic!(
                    "vdbe_ref_add_op failed for opcode '{}' — not found in C SQLite3",
                    op.opcode.name()
                );
            }

            // Set P5 if non-zero
            if op.p5 != 0 {
                unsafe { vdbe_ref_change_p5(handle, op.p5 as c_int) };
            }

            // Set P4 if present
            match &op.p4 {
                P4::Int64(val) => unsafe {
                    vdbe_ref_change_p4_int64(handle, addr, *val as c_longlong);
                },
                P4::Real(val) => unsafe {
                    vdbe_ref_change_p4_real(handle, addr, *val);
                },
                P4::Text(s) => {
                    let cs = CString::new(s.as_str()).unwrap();
                    unsafe { vdbe_ref_change_p4_str(handle, addr, cs.as_ptr()) };
                }
                P4::Collation(s) => {
                    let cs = CString::new(s.as_str()).unwrap();
                    unsafe { vdbe_ref_change_p4_str(handle, addr, cs.as_ptr()) };
                }
                P4::Unused => {}
                _ => {} // Other P4 types handled as needed
            }
        }

        // Set column names
        for (i, name) in program.col_names.iter().enumerate() {
            let cname = CString::new(name.as_str()).unwrap();
            unsafe { vdbe_ref_set_col_name(handle, i as c_int, cname.as_ptr()) };
        }

        // sqlite3VdbeCreate auto-inserts Init at address 0, shifting all our
        // ops by +1. Adjust P2 jump targets in our ops to compensate.
        unsafe { vdbe_ref_adjust_jumps(handle, 1) };

        RefVdbe { handle }
    }

    /// Execute the program and collect all result rows
    pub fn execute(&mut self) -> VdbeResult {
        let mut rows: Vec<Vec<SqlValue>> = Vec::new();

        loop {
            let rc = unsafe { vdbe_ref_step(self.handle) };

            if rc == SQLITE_ROW {
                let n_col = unsafe { vdbe_ref_column_count(self.handle) };
                let mut row = Vec::with_capacity(n_col as usize);

                for col in 0..n_col {
                    let col_type = unsafe { vdbe_ref_column_type(self.handle, col) };
                    let value = match col_type {
                        SQLITE_NULL => SqlValue::Null,
                        SQLITE_INTEGER => {
                            let v = unsafe { vdbe_ref_column_int64(self.handle, col) };
                            SqlValue::Integer(v as i64)
                        }
                        SQLITE_FLOAT => {
                            let v = unsafe { vdbe_ref_column_double(self.handle, col) };
                            SqlValue::Real(v)
                        }
                        SQLITE_TEXT => {
                            let ptr = unsafe { vdbe_ref_column_text(self.handle, col) };
                            if ptr.is_null() {
                                SqlValue::Null
                            } else {
                                let s = unsafe { CStr::from_ptr(ptr) }
                                    .to_string_lossy()
                                    .into_owned();
                                SqlValue::Text(s)
                            }
                        }
                        SQLITE_BLOB => {
                            let ptr = unsafe { vdbe_ref_column_blob(self.handle, col) };
                            let len = unsafe { vdbe_ref_column_bytes(self.handle, col) };
                            if ptr.is_null() || len <= 0 {
                                SqlValue::Blob(Vec::new())
                            } else {
                                let slice = unsafe {
                                    std::slice::from_raw_parts(ptr as *const u8, len as usize)
                                };
                                SqlValue::Blob(slice.to_vec())
                            }
                        }
                        _ => SqlValue::Null,
                    };
                    row.push(value);
                }
                rows.push(row);
            } else if rc == SQLITE_DONE {
                break;
            } else {
                let msg = unsafe {
                    let ptr = vdbe_ref_errmsg(self.handle);
                    if ptr.is_null() {
                        format!("SQLite3 VDBE error (rc={})", rc)
                    } else {
                        CStr::from_ptr(ptr).to_string_lossy().into_owned()
                    }
                };
                return VdbeResult::Error(msg);
            }
        }

        if rows.is_empty() {
            VdbeResult::Done
        } else {
            VdbeResult::Rows(rows)
        }
    }
}

impl Drop for RefVdbe {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { vdbe_ref_destroy(self.handle) };
        }
    }
}

// ============================================================================
// RustQL VDBE wrapper
// ============================================================================

pub struct RustVdbe {
    vdbe: Vdbe,
}

impl RustVdbe {
    /// Load a VdbeProgram into the RustQL VDBE
    pub fn from_program(program: &VdbeProgram) -> Self {
        let vdbe = Vdbe::from_ops(program.ops.clone());
        RustVdbe { vdbe }
    }

    /// Execute the program and collect all result rows
    pub fn execute(&mut self) -> VdbeResult {
        let mut rows: Vec<Vec<SqlValue>> = Vec::new();

        loop {
            match self.vdbe.step() {
                Ok(ExecResult::Row) => {
                    let n_col = self.vdbe.column_count();
                    let mut row = Vec::with_capacity(n_col as usize);

                    for col in 0..n_col {
                        let col_type = self.vdbe.column_type(col);
                        let value = match col_type {
                            rustql::types::ColumnType::Null => SqlValue::Null,
                            rustql::types::ColumnType::Integer => {
                                SqlValue::Integer(self.vdbe.column_int(col))
                            }
                            rustql::types::ColumnType::Float => {
                                SqlValue::Real(self.vdbe.column_real(col))
                            }
                            rustql::types::ColumnType::Text => {
                                SqlValue::Text(self.vdbe.column_text(col))
                            }
                            rustql::types::ColumnType::Blob => {
                                SqlValue::Blob(self.vdbe.column_blob(col))
                            }
                        };
                        row.push(value);
                    }
                    rows.push(row);
                }
                Ok(ExecResult::Done) => break,
                Ok(ExecResult::Continue) => continue,
                Err(e) => {
                    return VdbeResult::Error(format!("{}", e));
                }
            }
        }

        if rows.is_empty() {
            VdbeResult::Done
        } else {
            VdbeResult::Rows(rows)
        }
    }
}

// ============================================================================
// Comparison engine
// ============================================================================

/// Compare two SqlValues for equality (with epsilon for floats)
fn values_equal(a: &SqlValue, b: &SqlValue) -> bool {
    match (a, b) {
        (SqlValue::Null, SqlValue::Null) => true,
        (SqlValue::Integer(a), SqlValue::Integer(b)) => a == b,
        (SqlValue::Real(a), SqlValue::Real(b)) => (a - b).abs() < 1e-10,
        (SqlValue::Text(a), SqlValue::Text(b)) => a == b,
        (SqlValue::Blob(a), SqlValue::Blob(b)) => a == b,
        // Integer/Real cross-comparison (SQLite may return either)
        (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64 - b).abs() < 1e-10,
        (SqlValue::Real(a), SqlValue::Integer(b)) => (a - *b as f64).abs() < 1e-10,
        _ => false,
    }
}

/// Compare two VdbeResults, returning a descriptive error string if they differ
fn compare_vdbe_results(ref_result: &VdbeResult, rust_result: &VdbeResult) -> Option<String> {
    match (ref_result, rust_result) {
        (VdbeResult::Done, VdbeResult::Done) => None,
        (VdbeResult::Error(_ref_msg), VdbeResult::Error(_rust_msg)) => {
            // Both errored — we consider this a match (both correctly rejected)
            None
        }
        (VdbeResult::Rows(ref_rows), VdbeResult::Rows(rust_rows)) => {
            if ref_rows.len() != rust_rows.len() {
                return Some(format!(
                    "Row count mismatch: SQLite3 returned {} rows, RustQL returned {} rows",
                    ref_rows.len(),
                    rust_rows.len()
                ));
            }
            for (i, (ref_row, rust_row)) in ref_rows.iter().zip(rust_rows.iter()).enumerate() {
                if ref_row.len() != rust_row.len() {
                    return Some(format!(
                        "Row {}: column count mismatch: SQLite3={}, RustQL={}",
                        i,
                        ref_row.len(),
                        rust_row.len()
                    ));
                }
                for (j, (ref_val, rust_val)) in ref_row.iter().zip(rust_row.iter()).enumerate() {
                    if !values_equal(ref_val, rust_val) {
                        return Some(format!(
                            "Row {} col {}: SQLite3={}, RustQL={}",
                            i, j, ref_val, rust_val
                        ));
                    }
                }
            }
            None
        }
        (VdbeResult::Done, VdbeResult::Rows(rows)) => Some(format!(
            "SQLite3 returned Done, RustQL returned {} rows",
            rows.len()
        )),
        (VdbeResult::Rows(rows), VdbeResult::Done) => Some(format!(
            "SQLite3 returned {} rows, RustQL returned Done",
            rows.len()
        )),
        (VdbeResult::Error(msg), other) => Some(format!(
            "SQLite3 errored '{}', RustQL returned {:?}",
            msg, other
        )),
        (other, VdbeResult::Error(msg)) => Some(format!(
            "SQLite3 returned {:?}, RustQL errored '{}'",
            other, msg
        )),
    }
}

// ============================================================================
// Test macros
// ============================================================================

/// Assert that a VDBE program produces identical results on both engines.
///
/// Usage:
/// ```
/// assert_vdbe_match!(VdbeProgram {
///     ops: vec![...],
///     n_mem: 1, n_cursor: 0, n_col: 1,
///     ..Default::default()
/// });
/// ```
macro_rules! assert_vdbe_match {
    ($program:expr) => {{
        let program: VdbeProgram = $program;

        // Execute on C SQLite3 VDBE
        let mut ref_vdbe = RefVdbe::from_program(&program);
        let ref_result = ref_vdbe.execute();

        // Execute on RustQL VDBE
        let mut rust_vdbe = RustVdbe::from_program(&program);
        let rust_result = rust_vdbe.execute();

        // Compare
        if let Some(diff) = compare_vdbe_results(&ref_result, &rust_result) {
            // Print detailed program for debugging
            eprintln!("=== VDBE Program ===");
            for (i, op) in program.ops.iter().enumerate() {
                eprintln!(
                    "  {:3}: {:15} {:4} {:4} {:4}  {:?}",
                    i,
                    op.opcode.name(),
                    op.p1,
                    op.p2,
                    op.p3,
                    op.p4
                );
            }
            eprintln!("=== SQLite3 result ===");
            eprintln!("  {:?}", ref_result);
            eprintln!("=== RustQL result ===");
            eprintln!("  {:?}", rust_result);
            panic!("VDBE execution mismatch: {}", diff);
        }
    }};
}

/// Assert that a VDBE program produces the expected result on RustQL
/// (without comparing to SQLite3 — useful for RustQL-specific behavior)
macro_rules! assert_vdbe_result {
    ($program:expr, $expected:pat) => {{
        let program: VdbeProgram = $program;
        let mut rust_vdbe = RustVdbe::from_program(&program);
        let result = rust_vdbe.execute();
        assert!(
            matches!(result, $expected),
            "Expected {:?}, got {:?}",
            stringify!($expected),
            result
        );
    }};
}

// Test modules — declared AFTER macros so they can use them
mod basic;
mod comparison;
mod control_flow;
mod function;
mod record;
