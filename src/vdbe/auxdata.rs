//! VDBE Auxiliary Functions (vdbeaux.c translation)
//!
//! Helper functions for building VDBE programs, managing resources,
//! encoding/decoding records, and supporting EXPLAIN output.

use std::fmt::Write;

use crate::error::{Error, ErrorCode, Result};
use crate::vdbe::mem::Mem;
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

// ============================================================================
// Serial Types
// ============================================================================

/// Serial type codes for SQLite record format
///
/// The serial type is a variable-length integer stored in record headers
/// that determines the type and size of each column value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerialType {
    /// NULL value (serial type 0)
    Null,
    /// 8-bit signed integer (serial type 1)
    Int8,
    /// 16-bit big-endian integer (serial type 2)
    Int16,
    /// 24-bit big-endian integer (serial type 3)
    Int24,
    /// 32-bit big-endian integer (serial type 4)
    Int32,
    /// 48-bit big-endian integer (serial type 5)
    Int48,
    /// 64-bit big-endian integer (serial type 6)
    Int64,
    /// 64-bit IEEE float (serial type 7)
    Float64,
    /// Integer constant 0 (serial type 8)
    Zero,
    /// Integer constant 1 (serial type 9)
    One,
    /// Reserved (serial types 10-11)
    Reserved(u32),
    /// Blob of (N-12)/2 bytes (serial type >= 12 and even)
    Blob(u32),
    /// Text of (N-13)/2 bytes (serial type >= 13 and odd)
    Text(u32),
}

impl SerialType {
    /// Create a SerialType from its integer code
    pub fn from_code(code: u32) -> Self {
        match code {
            0 => SerialType::Null,
            1 => SerialType::Int8,
            2 => SerialType::Int16,
            3 => SerialType::Int24,
            4 => SerialType::Int32,
            5 => SerialType::Int48,
            6 => SerialType::Int64,
            7 => SerialType::Float64,
            8 => SerialType::Zero,
            9 => SerialType::One,
            10 | 11 => SerialType::Reserved(code),
            n if n >= 12 && n % 2 == 0 => SerialType::Blob((n - 12) / 2),
            n if n >= 13 && n % 2 == 1 => SerialType::Text((n - 13) / 2),
            _ => SerialType::Null,
        }
    }

    /// Get the integer code for this serial type
    pub fn code(&self) -> u32 {
        match self {
            SerialType::Null => 0,
            SerialType::Int8 => 1,
            SerialType::Int16 => 2,
            SerialType::Int24 => 3,
            SerialType::Int32 => 4,
            SerialType::Int48 => 5,
            SerialType::Int64 => 6,
            SerialType::Float64 => 7,
            SerialType::Zero => 8,
            SerialType::One => 9,
            SerialType::Reserved(n) => *n,
            SerialType::Blob(n) => 12 + n * 2,
            SerialType::Text(n) => 13 + n * 2,
        }
    }

    /// Get the size in bytes of the data for this serial type
    pub fn size(&self) -> usize {
        match self {
            SerialType::Null => 0,
            SerialType::Int8 => 1,
            SerialType::Int16 => 2,
            SerialType::Int24 => 3,
            SerialType::Int32 => 4,
            SerialType::Int48 => 6,
            SerialType::Int64 => 8,
            SerialType::Float64 => 8,
            SerialType::Zero | SerialType::One => 0,
            SerialType::Reserved(_) => 0,
            SerialType::Blob(n) | SerialType::Text(n) => *n as usize,
        }
    }

    /// Determine the serial type for a memory cell
    pub fn for_mem(mem: &Mem) -> Self {
        if mem.is_null() {
            SerialType::Null
        } else if mem.is_int() {
            let v = mem.to_int();
            if v == 0 {
                SerialType::Zero
            } else if v == 1 {
                SerialType::One
            } else if v >= -128 && v <= 127 {
                SerialType::Int8
            } else if v >= -32768 && v <= 32767 {
                SerialType::Int16
            } else if v >= -8388608 && v <= 8388607 {
                SerialType::Int24
            } else if v >= -2147483648 && v <= 2147483647 {
                SerialType::Int32
            } else if v >= -140737488355328 && v <= 140737488355327 {
                SerialType::Int48
            } else {
                SerialType::Int64
            }
        } else if mem.is_real() {
            SerialType::Float64
        } else if mem.is_str() {
            SerialType::Text(mem.len() as u32)
        } else if mem.is_blob() {
            SerialType::Blob(mem.len() as u32)
        } else {
            SerialType::Null
        }
    }
}

// ============================================================================
// Varint Encoding/Decoding
// ============================================================================

/// Read a variable-length integer from a byte slice
///
/// Returns the value and the number of bytes consumed.
/// SQLite varints use 1-9 bytes, with 7 bits of data per byte.
pub fn get_varint(data: &[u8]) -> (u64, usize) {
    if data.is_empty() {
        return (0, 0);
    }

    let mut value = 0u64;
    let mut i = 0;

    while i < data.len() && i < 9 {
        let byte = data[i] as u64;
        if i < 8 {
            value = (value << 7) | (byte & 0x7F);
            if byte & 0x80 == 0 {
                return (value, i + 1);
            }
        } else {
            // 9th byte uses all 8 bits
            value = (value << 8) | byte;
            return (value, 9);
        }
        i += 1;
    }

    (value, i)
}

/// Write a variable-length integer to a buffer
///
/// Returns the number of bytes written.
pub fn put_varint(buf: &mut Vec<u8>, value: u64) -> usize {
    if value <= 0x7F {
        buf.push(value as u8);
        1
    } else if value <= 0x3FFF {
        buf.push(((value >> 7) | 0x80) as u8);
        buf.push((value & 0x7F) as u8);
        2
    } else if value <= 0x1FFFFF {
        buf.push(((value >> 14) | 0x80) as u8);
        buf.push(((value >> 7) | 0x80) as u8);
        buf.push((value & 0x7F) as u8);
        3
    } else if value <= 0x0FFFFFFF {
        buf.push(((value >> 21) | 0x80) as u8);
        buf.push(((value >> 14) | 0x80) as u8);
        buf.push(((value >> 7) | 0x80) as u8);
        buf.push((value & 0x7F) as u8);
        4
    } else {
        // For larger values, use full 9-byte encoding
        let len = varint_len(value);
        let start = buf.len();
        buf.resize(start + len, 0);
        put_varint_at(&mut buf[start..], value);
        len
    }
}

/// Calculate the length of a varint
pub fn varint_len(value: u64) -> usize {
    if value <= 0x7F {
        1
    } else if value <= 0x3FFF {
        2
    } else if value <= 0x1FFFFF {
        3
    } else if value <= 0x0FFFFFFF {
        4
    } else if value <= 0x07FFFFFFFF {
        5
    } else if value <= 0x03FFFFFFFFFF {
        6
    } else if value <= 0x01FFFFFFFFFFFF {
        7
    } else if value <= 0x00FFFFFFFFFFFFFF {
        8
    } else {
        9
    }
}

/// Write a varint at a specific location
fn put_varint_at(buf: &mut [u8], value: u64) -> usize {
    let len = varint_len(value);
    if len == 9 {
        buf[0] = 0xFF;
        for i in 1..9 {
            buf[i] = ((value >> ((8 - i) * 8)) & 0xFF) as u8;
        }
    } else {
        let mut v = value;
        for i in (0..len).rev() {
            if i == len - 1 {
                buf[i] = (v & 0x7F) as u8;
            } else {
                buf[i] = ((v & 0x7F) | 0x80) as u8;
            }
            v >>= 7;
        }
    }
    len
}

// ============================================================================
// Record Encoding/Decoding
// ============================================================================

/// Decode a record header
///
/// Returns the serial types and the offset where data begins.
pub fn decode_record_header(data: &[u8]) -> Result<(Vec<SerialType>, usize)> {
    if data.is_empty() {
        return Ok((Vec::new(), 0));
    }

    let (header_size, header_size_len) = get_varint(data);
    let header_size = header_size as usize;

    if header_size > data.len() {
        return Err(Error::with_message(
            ErrorCode::Corrupt,
            "record header size exceeds data length",
        ));
    }

    let mut types = Vec::new();
    let mut offset = header_size_len;

    while offset < header_size {
        let (type_code, consumed) = get_varint(&data[offset..]);
        types.push(SerialType::from_code(type_code as u32));
        offset += consumed;
    }

    Ok((types, header_size))
}

/// Build a record from memory cells
///
/// Creates a byte array in SQLite record format from registers start..start+count-1.
pub fn make_record(mems: &[Mem], start: i32, count: i32) -> Vec<u8> {
    let start = start as usize;
    let count = count as usize;

    // First pass: determine serial types and sizes
    let mut serial_types = Vec::with_capacity(count);
    let mut data_size = 0usize;

    for i in 0..count {
        if let Some(mem) = mems.get(start + i) {
            let serial_type = SerialType::for_mem(mem);
            data_size += serial_type.size();
            serial_types.push(serial_type);
        } else {
            serial_types.push(SerialType::Null);
        }
    }

    // Calculate header size
    let mut header = Vec::new();
    for serial_type in &serial_types {
        put_varint(&mut header, serial_type.code() as u64);
    }

    // Header includes its own size varint
    let header_size = varint_len((header.len() + 1) as u64) + header.len();

    // Build the record
    let mut record = Vec::with_capacity(header_size + data_size);

    // Write header size
    put_varint(&mut record, header_size as u64);

    // Write serial types
    record.append(&mut header);

    // Write data
    for i in 0..count {
        if let Some(mem) = mems.get(start + i) {
            serialize_mem(mem, &serial_types[i], &mut record);
        }
    }

    record
}

/// Serialize a memory cell according to its serial type
fn serialize_mem(mem: &Mem, serial_type: &SerialType, buf: &mut Vec<u8>) {
    match serial_type {
        SerialType::Null | SerialType::Zero | SerialType::One | SerialType::Reserved(_) => {
            // No data to write
        }
        SerialType::Int8 => {
            buf.push(mem.to_int() as u8);
        }
        SerialType::Int16 => {
            let v = mem.to_int() as i16;
            buf.extend_from_slice(&v.to_be_bytes());
        }
        SerialType::Int24 => {
            let v = mem.to_int() as i32;
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        SerialType::Int32 => {
            let v = mem.to_int() as i32;
            buf.extend_from_slice(&v.to_be_bytes());
        }
        SerialType::Int48 => {
            let v = mem.to_int();
            buf.push((v >> 40) as u8);
            buf.push((v >> 32) as u8);
            buf.push((v >> 24) as u8);
            buf.push((v >> 16) as u8);
            buf.push((v >> 8) as u8);
            buf.push(v as u8);
        }
        SerialType::Int64 => {
            buf.extend_from_slice(&mem.to_int().to_be_bytes());
        }
        SerialType::Float64 => {
            buf.extend_from_slice(&mem.to_real().to_be_bytes());
        }
        SerialType::Blob(_) | SerialType::Text(_) => {
            buf.extend_from_slice(mem.as_bytes());
        }
    }
}

/// Deserialize a value from record data
pub fn deserialize_value(data: &[u8], serial_type: &SerialType) -> Result<Mem> {
    let mut mem = Mem::new();

    match serial_type {
        SerialType::Null => {
            mem.set_null();
        }
        SerialType::Zero => {
            mem.set_int(0);
        }
        SerialType::One => {
            mem.set_int(1);
        }
        SerialType::Int8 => {
            if data.is_empty() {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            mem.set_int(data[0] as i8 as i64);
        }
        SerialType::Int16 => {
            if data.len() < 2 {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let v = i16::from_be_bytes([data[0], data[1]]);
            mem.set_int(v as i64);
        }
        SerialType::Int24 => {
            if data.len() < 3 {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let v = ((data[0] as i32) << 16) | ((data[1] as i32) << 8) | (data[2] as i32);
            // Sign extend from 24-bit
            let v = if v & 0x800000 != 0 { v | !0xFFFFFF } else { v };
            mem.set_int(v as i64);
        }
        SerialType::Int32 => {
            if data.len() < 4 {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let v = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            mem.set_int(v as i64);
        }
        SerialType::Int48 => {
            if data.len() < 6 {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let v = ((data[0] as i64) << 40)
                | ((data[1] as i64) << 32)
                | ((data[2] as i64) << 24)
                | ((data[3] as i64) << 16)
                | ((data[4] as i64) << 8)
                | (data[5] as i64);
            // Sign extend from 48-bit
            let v = if v & 0x800000000000 != 0 {
                v | !0xFFFFFFFFFFFF
            } else {
                v
            };
            mem.set_int(v);
        }
        SerialType::Int64 => {
            if data.len() < 8 {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let v = i64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            mem.set_int(v);
        }
        SerialType::Float64 => {
            if data.len() < 8 {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let v = f64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            mem.set_real(v);
        }
        SerialType::Blob(n) => {
            let n = *n as usize;
            if data.len() < n {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            mem.set_blob(&data[..n]);
        }
        SerialType::Text(n) => {
            let n = *n as usize;
            if data.len() < n {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let s = String::from_utf8_lossy(&data[..n]);
            mem.set_str(&s);
        }
        SerialType::Reserved(_) => {
            mem.set_null();
        }
    }

    Ok(mem)
}

// ============================================================================
// EXPLAIN Output
// ============================================================================

/// Generate EXPLAIN output for a VDBE program
pub fn explain_program(ops: &[VdbeOp]) -> String {
    let mut output = String::new();

    // Header
    let _ = writeln!(
        output,
        "{:>4}  {:<15}  {:>4}  {:>4}  {:>4}  {:<10}  {}",
        "addr", "opcode", "p1", "p2", "p3", "p4", "comment"
    );
    let _ = writeln!(
        output,
        "----  ---------------  ----  ----  ----  ----------  -------"
    );

    for (i, op) in ops.iter().enumerate() {
        let p4_str = format_p4(&op.p4);
        let comment = op.comment.as_deref().unwrap_or("");

        let _ = writeln!(
            output,
            "{:>4}  {:<15}  {:>4}  {:>4}  {:>4}  {:<10}  {}",
            i,
            op.opcode.name(),
            op.p1,
            op.p2,
            op.p3,
            p4_str,
            comment
        );
    }

    output
}

/// Format P4 operand for display
fn format_p4(p4: &P4) -> String {
    match p4 {
        P4::Unused => String::new(),
        P4::Int64(i) => i.to_string(),
        P4::Real(r) => format!("{:.6}", r),
        P4::Text(s) => {
            if s.len() > 20 {
                format!("'{:.17}...'", s)
            } else {
                format!("'{}'", s)
            }
        }
        P4::Blob(b) => {
            if b.len() > 10 {
                format!("x'{}'...", hex_encode(&b[..10]))
            } else {
                format!("x'{}'", hex_encode(b))
            }
        }
        P4::Collation(c) => format!("collseq({})", c),
        P4::FuncDef(f) => format!("func({})", f),
        P4::KeyInfo(k) => format!("k({})", k.n_key_field),
        P4::Mem(m) => format!("r[{}]", m),
        P4::Vtab(v) => format!("vtab({})", v),
        P4::Subprogram(s) => format!("prog({})", s.ops.len()),
        P4::Table(t) => format!("table({})", t),
        P4::IntArray(a) => format!("[{} ints]", a.len()),
    }
}

/// Hex encode bytes
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

// ============================================================================
// Program Builder Helpers
// ============================================================================

/// Label for forward jumps (resolved later)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Label(i32);

impl Label {
    /// Create an unresolved label
    pub fn new(id: i32) -> Self {
        Label(-id - 1)
    }

    /// Get the internal value (negative for unresolved)
    pub fn value(&self) -> i32 {
        self.0
    }

    /// Check if this label is resolved
    pub fn is_resolved(&self) -> bool {
        self.0 >= 0
    }
}

/// Helper for building VDBE programs
pub struct VdbeBuilder {
    /// Accumulated instructions
    ops: Vec<VdbeOp>,
    /// Next label ID
    next_label: i32,
    /// Labels awaiting resolution
    pending_labels: Vec<(i32, usize)>, // (label_id, instruction_index)
    /// Number of memory cells needed
    n_mem: i32,
    /// Number of cursors needed
    n_cursor: i32,
}

impl VdbeBuilder {
    /// Create a new program builder
    pub fn new() -> Self {
        Self {
            ops: Vec::new(),
            next_label: 0,
            pending_labels: Vec::new(),
            n_mem: 0,
            n_cursor: 0,
        }
    }

    /// Get current address (instruction count)
    pub fn current_addr(&self) -> i32 {
        self.ops.len() as i32
    }

    /// Allocate a new label for forward jumps
    pub fn alloc_label(&mut self) -> Label {
        let label = Label::new(self.next_label);
        self.next_label += 1;
        label
    }

    /// Add an instruction
    pub fn add_op(&mut self, opcode: Opcode, p1: i32, p2: i32, p3: i32) -> i32 {
        let addr = self.ops.len() as i32;
        self.ops.push(VdbeOp::new(opcode, p1, p2, p3));
        addr
    }

    /// Add an instruction with P4
    pub fn add_op4(&mut self, opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: P4) -> i32 {
        let addr = self.ops.len() as i32;
        self.ops.push(VdbeOp::with_p4(opcode, p1, p2, p3, p4));
        addr
    }

    /// Add a jump instruction with a label target
    pub fn add_op_label(&mut self, opcode: Opcode, p1: i32, label: Label, p3: i32) -> i32 {
        let addr = self.ops.len() as i32;
        self.ops.push(VdbeOp::new(opcode, p1, label.value(), p3));
        if !label.is_resolved() {
            self.pending_labels
                .push((-label.value() - 1, addr as usize));
        }
        addr
    }

    /// Resolve a label to the current address
    pub fn resolve_label(&mut self, label: Label) {
        let target = self.current_addr();
        let label_id = -label.value() - 1;

        // Update all instructions that reference this label
        for &(id, idx) in &self.pending_labels {
            if id == label_id {
                self.ops[idx].p2 = target;
            }
        }

        // Remove resolved labels
        self.pending_labels.retain(|&(id, _)| id != label_id);
    }

    /// Change P2 of an instruction
    pub fn change_p2(&mut self, addr: i32, p2: i32) {
        if addr >= 0 && (addr as usize) < self.ops.len() {
            self.ops[addr as usize].p2 = p2;
        }
    }

    /// Change P3 of an instruction
    pub fn change_p3(&mut self, addr: i32, p3: i32) {
        if addr >= 0 && (addr as usize) < self.ops.len() {
            self.ops[addr as usize].p3 = p3;
        }
    }

    /// Set comment on an instruction
    pub fn set_comment(&mut self, addr: i32, comment: impl Into<String>) {
        if addr >= 0 && (addr as usize) < self.ops.len() {
            self.ops[addr as usize].comment = Some(comment.into());
        }
    }

    /// Allocate memory registers
    pub fn alloc_reg(&mut self, n: i32) -> i32 {
        let start = self.n_mem;
        self.n_mem += n;
        start + 1 // 1-indexed
    }

    /// Allocate a cursor slot
    pub fn alloc_cursor(&mut self) -> i32 {
        let slot = self.n_cursor;
        self.n_cursor += 1;
        slot
    }

    /// Get number of registers needed
    pub fn n_mem(&self) -> i32 {
        self.n_mem
    }

    /// Get number of cursors needed
    pub fn n_cursor(&self) -> i32 {
        self.n_cursor
    }

    /// Build and return the instruction list
    pub fn build(self) -> Vec<VdbeOp> {
        self.ops
    }

    /// Get instruction at address
    pub fn op_at(&self, addr: i32) -> Option<&VdbeOp> {
        self.ops.get(addr as usize)
    }

    /// Get mutable instruction at address
    pub fn op_at_mut(&mut self, addr: i32) -> Option<&mut VdbeOp> {
        self.ops.get_mut(addr as usize)
    }
}

impl Default for VdbeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serial_type_from_code() {
        assert_eq!(SerialType::from_code(0), SerialType::Null);
        assert_eq!(SerialType::from_code(1), SerialType::Int8);
        assert_eq!(SerialType::from_code(6), SerialType::Int64);
        assert_eq!(SerialType::from_code(7), SerialType::Float64);
        assert_eq!(SerialType::from_code(8), SerialType::Zero);
        assert_eq!(SerialType::from_code(9), SerialType::One);
        assert_eq!(SerialType::from_code(12), SerialType::Blob(0));
        assert_eq!(SerialType::from_code(14), SerialType::Blob(1));
        assert_eq!(SerialType::from_code(13), SerialType::Text(0));
        assert_eq!(SerialType::from_code(15), SerialType::Text(1));
    }

    #[test]
    fn test_serial_type_code() {
        assert_eq!(SerialType::Null.code(), 0);
        assert_eq!(SerialType::Int64.code(), 6);
        assert_eq!(SerialType::Float64.code(), 7);
        assert_eq!(SerialType::Blob(5).code(), 22);
        assert_eq!(SerialType::Text(5).code(), 23);
    }

    #[test]
    fn test_serial_type_size() {
        assert_eq!(SerialType::Null.size(), 0);
        assert_eq!(SerialType::Int8.size(), 1);
        assert_eq!(SerialType::Int16.size(), 2);
        assert_eq!(SerialType::Int64.size(), 8);
        assert_eq!(SerialType::Float64.size(), 8);
        assert_eq!(SerialType::Zero.size(), 0);
        assert_eq!(SerialType::Blob(10).size(), 10);
        assert_eq!(SerialType::Text(5).size(), 5);
    }

    #[test]
    fn test_serial_type_for_mem() {
        let null_mem = Mem::new();
        assert_eq!(SerialType::for_mem(&null_mem), SerialType::Null);

        let zero_mem = Mem::from_int(0);
        assert_eq!(SerialType::for_mem(&zero_mem), SerialType::Zero);

        let one_mem = Mem::from_int(1);
        assert_eq!(SerialType::for_mem(&one_mem), SerialType::One);

        let small_int = Mem::from_int(42);
        assert_eq!(SerialType::for_mem(&small_int), SerialType::Int8);

        let real_mem = Mem::from_real(3.14);
        assert_eq!(SerialType::for_mem(&real_mem), SerialType::Float64);

        let text_mem = Mem::from_str("hello");
        assert_eq!(SerialType::for_mem(&text_mem), SerialType::Text(5));
    }

    #[test]
    fn test_varint_roundtrip() {
        let test_values = [
            0u64,
            1,
            127,
            128,
            0x3FFF,
            0x4000,
            0x1FFFFF,
            0x200000,
            0xFFFFFFFF,
            u64::MAX,
        ];

        for &value in &test_values {
            let mut buf = Vec::new();
            let written = put_varint(&mut buf, value);
            let (decoded, consumed) = get_varint(&buf);

            assert_eq!(decoded, value, "failed for value {}", value);
            assert_eq!(written, consumed, "length mismatch for value {}", value);
        }
    }

    #[test]
    fn test_varint_len() {
        assert_eq!(varint_len(0), 1);
        assert_eq!(varint_len(127), 1);
        assert_eq!(varint_len(128), 2);
        assert_eq!(varint_len(0x3FFF), 2);
        assert_eq!(varint_len(0x4000), 3);
    }

    #[test]
    fn test_make_record_simple() {
        let mems = vec![
            Mem::from_int(42),
            Mem::from_str("hello"),
            Mem::new(), // NULL
        ];

        let record = make_record(&mems, 0, 3);

        // Verify we can decode the header
        let (types, header_size) = decode_record_header(&record).unwrap();
        assert_eq!(types.len(), 3);
        assert_eq!(types[0], SerialType::Int8);
        assert_eq!(types[1], SerialType::Text(5));
        assert_eq!(types[2], SerialType::Null);
        assert!(header_size > 0);
    }

    #[test]
    fn test_deserialize_values() {
        // Int8
        let mem = deserialize_value(&[42], &SerialType::Int8).unwrap();
        assert_eq!(mem.to_int(), 42);

        // Int64
        let bytes = 12345678i64.to_be_bytes();
        let mem = deserialize_value(&bytes, &SerialType::Int64).unwrap();
        assert_eq!(mem.to_int(), 12345678);

        // Float64
        let bytes = 3.14f64.to_be_bytes();
        let mem = deserialize_value(&bytes, &SerialType::Float64).unwrap();
        assert!((mem.to_real() - 3.14).abs() < f64::EPSILON);

        // Text
        let mem = deserialize_value(b"hello", &SerialType::Text(5)).unwrap();
        assert_eq!(mem.to_str(), "hello");

        // Blob
        let mem = deserialize_value(&[1, 2, 3], &SerialType::Blob(3)).unwrap();
        assert_eq!(mem.to_blob(), vec![1, 2, 3]);
    }

    #[test]
    fn test_explain_program() {
        let ops = vec![
            VdbeOp::new(Opcode::Init, 0, 3, 0),
            VdbeOp::new(Opcode::Integer, 42, 1, 0),
            VdbeOp::new(Opcode::ResultRow, 1, 1, 0),
            VdbeOp::new(Opcode::Halt, 0, 0, 0),
        ];

        let output = explain_program(&ops);
        assert!(output.contains("Init"));
        assert!(output.contains("Integer"));
        assert!(output.contains("ResultRow"));
        assert!(output.contains("Halt"));
        assert!(output.contains("42"));
    }

    #[test]
    fn test_vdbe_builder_basic() {
        let mut builder = VdbeBuilder::new();

        builder.add_op(Opcode::Init, 0, 1, 0);
        builder.add_op(Opcode::Integer, 42, 1, 0);
        builder.add_op(Opcode::Halt, 0, 0, 0);

        let ops = builder.build();
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[0].opcode, Opcode::Init);
        assert_eq!(ops[1].p1, 42);
    }

    #[test]
    fn test_vdbe_builder_labels() {
        let mut builder = VdbeBuilder::new();

        let done = builder.alloc_label();
        builder.add_op(Opcode::Init, 0, 0, 0);
        builder.add_op_label(Opcode::Goto, 0, done, 0); // Jump to 'done'
        builder.add_op(Opcode::Integer, 1, 1, 0); // This will be skipped
        builder.resolve_label(done);
        builder.add_op(Opcode::Halt, 0, 0, 0);

        let ops = builder.build();
        assert_eq!(ops.len(), 4);
        // The Goto at index 1 should point to index 3 (Halt)
        assert_eq!(ops[1].p2, 3);
    }

    #[test]
    fn test_vdbe_builder_alloc() {
        let mut builder = VdbeBuilder::new();

        let reg1 = builder.alloc_reg(3);
        let reg2 = builder.alloc_reg(2);
        assert_eq!(reg1, 1); // 1-indexed
        assert_eq!(reg2, 4); // After the first 3

        let cursor1 = builder.alloc_cursor();
        let cursor2 = builder.alloc_cursor();
        assert_eq!(cursor1, 0);
        assert_eq!(cursor2, 1);

        assert_eq!(builder.n_mem(), 5);
        assert_eq!(builder.n_cursor(), 2);
    }

    #[test]
    fn test_format_p4() {
        assert_eq!(format_p4(&P4::Unused), "");
        assert_eq!(format_p4(&P4::Int64(42)), "42");
        assert_eq!(format_p4(&P4::Text("hi".to_string())), "'hi'");
        assert_eq!(format_p4(&P4::FuncDef("abs".to_string())), "func(abs)");
    }
}
