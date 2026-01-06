//! VDBE Memory Cells
//!
//! Memory cells (Mem) are the registers of the VDBE virtual machine.
//! Each cell can hold any SQLite value type and tracks its current type.

use std::cmp::Ordering;
use std::fmt;

use crate::error::{Error, ErrorCode, Result};
use crate::schema::Affinity;
use crate::types::{ColumnType, Value};

// ============================================================================
// Memory Cell Flags
// ============================================================================

bitflags::bitflags! {
    /// Flags for memory cell state
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct MemFlags: u16 {
        /// Value is NULL
        const NULL = 0x0001;
        /// Value is a string
        const STR = 0x0002;
        /// Value is an integer
        const INT = 0x0004;
        /// Value is a real number
        const REAL = 0x0008;
        /// Value is a BLOB
        const BLOB = 0x0010;
        /// Value is a rowid (special integer)
        const ROWID = 0x0020;
        /// String is zero-terminated
        const TERM = 0x0200;
        /// String is static (don't free)
        const STATIC = 0x0400;
        /// String is ephemeral
        const EPHEM = 0x0800;
        /// Value came from bind
        const SUBTYPE = 0x1000;
        /// Cleared by OP_Null
        const CLEARED = 0x2000;
    }
}

// ============================================================================
// Memory Cell
// ============================================================================

/// A VDBE memory cell (register)
///
/// This corresponds to SQLite's `Mem` structure. Each cell can hold
/// any SQLite value type and tracks metadata about the value.
#[derive(Clone)]
pub struct Mem {
    /// Flags indicating the type and state
    pub flags: MemFlags,
    /// Integer value (when flags contains INT)
    i: i64,
    /// Real value (when flags contains REAL)
    r: f64,
    /// String or blob data
    data: Vec<u8>,
    /// Collation sequence name
    pub collation: String,
}

impl Default for Mem {
    fn default() -> Self {
        Self::new()
    }
}

impl Mem {
    /// Create a new NULL memory cell
    pub fn new() -> Self {
        Self {
            flags: MemFlags::NULL,
            i: 0,
            r: 0.0,
            data: Vec::new(),
            collation: "BINARY".to_string(),
        }
    }

    /// Create a memory cell with an integer value
    pub fn from_int(value: i64) -> Self {
        let mut mem = Self::new();
        mem.set_int(value);
        mem
    }

    /// Create a memory cell with a real value
    pub fn from_real(value: f64) -> Self {
        let mut mem = Self::new();
        mem.set_real(value);
        mem
    }

    /// Create a memory cell with a string value
    pub fn from_str(value: &str) -> Self {
        let mut mem = Self::new();
        mem.set_str(value);
        mem
    }

    /// Create a memory cell with a blob value
    pub fn from_blob(value: &[u8]) -> Self {
        let mut mem = Self::new();
        mem.set_blob(value);
        mem
    }

    /// Create from a Value enum
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => Self::new(),
            Value::Integer(i) => Self::from_int(*i),
            Value::Real(r) => Self::from_real(*r),
            Value::Text(s) => Self::from_str(s),
            Value::Blob(b) => Self::from_blob(b),
        }
    }

    // ========================================================================
    // Type Checking
    // ========================================================================

    /// Check if the value is NULL
    pub fn is_null(&self) -> bool {
        self.flags.contains(MemFlags::NULL)
    }

    /// Check if the value is an integer
    pub fn is_int(&self) -> bool {
        self.flags.contains(MemFlags::INT)
    }

    /// Check if the value is a real number
    pub fn is_real(&self) -> bool {
        self.flags.contains(MemFlags::REAL)
    }

    /// Check if the value is a string
    pub fn is_str(&self) -> bool {
        self.flags.contains(MemFlags::STR)
    }

    /// Check if the value is a blob
    pub fn is_blob(&self) -> bool {
        self.flags.contains(MemFlags::BLOB)
    }

    /// Get the SQLite column type
    pub fn column_type(&self) -> ColumnType {
        if self.is_null() {
            ColumnType::Null
        } else if self.is_int() {
            ColumnType::Integer
        } else if self.is_real() {
            ColumnType::Float
        } else if self.is_str() {
            ColumnType::Text
        } else if self.is_blob() {
            ColumnType::Blob
        } else {
            ColumnType::Null
        }
    }

    // ========================================================================
    // Setters
    // ========================================================================

    /// Set to NULL
    pub fn set_null(&mut self) {
        self.flags = MemFlags::NULL;
        self.i = 0;
        self.r = 0.0;
        self.data.clear();
    }

    /// Set to an integer value
    pub fn set_int(&mut self, value: i64) {
        self.flags = MemFlags::INT;
        self.i = value;
        self.r = 0.0;
        self.data.clear();
    }

    /// Set to a real value
    pub fn set_real(&mut self, value: f64) {
        self.flags = MemFlags::REAL;
        self.i = 0;
        self.r = value;
        self.data.clear();
    }

    /// Set to a string value
    pub fn set_str(&mut self, value: &str) {
        self.flags = MemFlags::STR | MemFlags::TERM;
        self.i = 0;
        self.r = 0.0;
        self.data = value.as_bytes().to_vec();
    }

    /// Set to a blob value
    pub fn set_blob(&mut self, value: &[u8]) {
        self.flags = MemFlags::BLOB;
        self.i = 0;
        self.r = 0.0;
        self.data = value.to_vec();
    }

    /// Set from a Value enum
    pub fn set_value(&mut self, value: &Value) {
        match value {
            Value::Null => self.set_null(),
            Value::Integer(i) => self.set_int(*i),
            Value::Real(r) => self.set_real(*r),
            Value::Text(s) => self.set_str(s),
            Value::Blob(b) => self.set_blob(b),
        }
    }

    // ========================================================================
    // Getters
    // ========================================================================

    /// Get as integer (with coercion)
    pub fn to_int(&self) -> i64 {
        if self.is_int() {
            self.i
        } else if self.is_real() {
            self.r as i64
        } else if self.is_str() {
            // Parse string as integer
            let s = String::from_utf8_lossy(&self.data);
            s.trim().parse().unwrap_or(0)
        } else {
            0
        }
    }

    /// Get as real (with coercion)
    pub fn to_real(&self) -> f64 {
        if self.is_real() {
            self.r
        } else if self.is_int() {
            self.i as f64
        } else if self.is_str() {
            let s = String::from_utf8_lossy(&self.data);
            s.trim().parse().unwrap_or(0.0)
        } else {
            0.0
        }
    }

    /// Get as string (with coercion)
    pub fn to_str(&self) -> String {
        if self.is_str() {
            String::from_utf8_lossy(&self.data).into_owned()
        } else if self.is_int() {
            self.i.to_string()
        } else if self.is_real() {
            self.r.to_string()
        } else if self.is_blob() {
            String::from_utf8_lossy(&self.data).into_owned()
        } else {
            String::new()
        }
    }

    /// Get as blob (with coercion)
    pub fn to_blob(&self) -> Vec<u8> {
        if self.is_blob() || self.is_str() {
            self.data.clone()
        } else if self.is_int() {
            self.i.to_string().into_bytes()
        } else if self.is_real() {
            self.r.to_string().into_bytes()
        } else {
            Vec::new()
        }
    }

    /// Get raw bytes reference
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Get byte length
    pub fn len(&self) -> usize {
        if self.is_str() || self.is_blob() {
            self.data.len()
        } else if self.is_int() {
            8
        } else if self.is_real() {
            8
        } else {
            0
        }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.is_null() || (self.is_str() && self.data.is_empty())
    }

    /// Convert to Value enum
    pub fn to_value(&self) -> Value {
        if self.is_null() {
            Value::Null
        } else if self.is_int() {
            Value::Integer(self.i)
        } else if self.is_real() {
            Value::Real(self.r)
        } else if self.is_str() {
            Value::Text(self.to_str())
        } else if self.is_blob() {
            Value::Blob(self.data.clone())
        } else {
            Value::Null
        }
    }

    // ========================================================================
    // Operations
    // ========================================================================

    /// Copy value from another memory cell
    pub fn copy_from(&mut self, other: &Mem) {
        self.flags = other.flags;
        self.i = other.i;
        self.r = other.r;
        self.data = other.data.clone();
        self.collation = other.collation.clone();
    }

    /// Move value from another memory cell (source becomes NULL)
    pub fn move_from(&mut self, other: &mut Mem) {
        self.flags = other.flags;
        self.i = other.i;
        self.r = other.r;
        self.data = std::mem::take(&mut other.data);
        self.collation = std::mem::take(&mut other.collation);
        other.set_null();
    }

    /// Apply type affinity
    pub fn apply_affinity(&mut self, affinity: Affinity) {
        match affinity {
            Affinity::Integer => {
                if self.is_real() {
                    // Check if the real can be represented exactly as int
                    let i = self.r as i64;
                    if (i as f64) == self.r {
                        self.set_int(i);
                    }
                } else if self.is_str() {
                    // Try to parse as integer
                    if let Ok(i) = self.to_str().trim().parse::<i64>() {
                        self.set_int(i);
                    }
                }
            }
            Affinity::Real => {
                if self.is_int() {
                    self.set_real(self.i as f64);
                } else if self.is_str() {
                    if let Ok(r) = self.to_str().trim().parse::<f64>() {
                        self.set_real(r);
                    }
                }
            }
            Affinity::Numeric => {
                if self.is_str() {
                    let s = self.to_str();
                    let trimmed = s.trim();
                    // Try integer first
                    if let Ok(i) = trimmed.parse::<i64>() {
                        self.set_int(i);
                    } else if let Ok(r) = trimmed.parse::<f64>() {
                        self.set_real(r);
                    }
                }
            }
            Affinity::Text => {
                if !self.is_str() && !self.is_null() {
                    let s = self.to_str();
                    self.set_str(&s);
                }
            }
            Affinity::Blob => {
                // No conversion needed for BLOB affinity
            }
        }
    }

    /// Negate the value
    pub fn negate(&mut self) -> Result<()> {
        if self.is_int() {
            self.i = self.i.wrapping_neg();
        } else if self.is_real() {
            self.r = -self.r;
        } else if self.is_null() {
            // NULL stays NULL
        } else {
            return Err(Error::with_message(
                ErrorCode::Mismatch,
                "cannot negate non-numeric value",
            ));
        }
        Ok(())
    }

    /// Bitwise NOT
    pub fn bit_not(&mut self) -> Result<()> {
        let i = self.to_int();
        self.set_int(!i);
        Ok(())
    }

    /// Logical NOT
    pub fn logical_not(&mut self) {
        if self.is_null() {
            // NULL stays NULL
        } else {
            let truthy = self.is_truthy();
            self.set_int(if truthy { 0 } else { 1 });
        }
    }

    /// Check if value is truthy (non-zero, non-empty)
    pub fn is_truthy(&self) -> bool {
        if self.is_null() {
            false
        } else if self.is_int() {
            self.i != 0
        } else if self.is_real() {
            self.r != 0.0
        } else if self.is_str() {
            !self.data.is_empty()
        } else if self.is_blob() {
            !self.data.is_empty()
        } else {
            false
        }
    }

    // ========================================================================
    // Comparison
    // ========================================================================

    /// Compare with another memory cell
    pub fn compare(&self, other: &Mem) -> Ordering {
        // NULL handling
        if self.is_null() && other.is_null() {
            return Ordering::Equal;
        }
        if self.is_null() {
            return Ordering::Less;
        }
        if other.is_null() {
            return Ordering::Greater;
        }

        // Type-based comparison (SQLite sort order: NULL < INT/REAL < TEXT < BLOB)
        match (self.column_type(), other.column_type()) {
            // Both numeric
            (ColumnType::Integer, ColumnType::Integer) => self.i.cmp(&other.i),
            (ColumnType::Float, ColumnType::Float) => {
                self.r.partial_cmp(&other.r).unwrap_or(Ordering::Equal)
            }
            (ColumnType::Integer, ColumnType::Float) => {
                (self.i as f64).partial_cmp(&other.r).unwrap_or(Ordering::Equal)
            }
            (ColumnType::Float, ColumnType::Integer) => {
                self.r.partial_cmp(&(other.i as f64)).unwrap_or(Ordering::Equal)
            }

            // Both text
            (ColumnType::Text, ColumnType::Text) => self.data.cmp(&other.data),

            // Both blob
            (ColumnType::Blob, ColumnType::Blob) => self.data.cmp(&other.data),

            // Mixed types - use SQLite type ordering
            (ColumnType::Integer | ColumnType::Float, ColumnType::Text | ColumnType::Blob) => {
                Ordering::Less
            }
            (ColumnType::Text | ColumnType::Blob, ColumnType::Integer | ColumnType::Float) => {
                Ordering::Greater
            }
            (ColumnType::Text, ColumnType::Blob) => Ordering::Less,
            (ColumnType::Blob, ColumnType::Text) => Ordering::Greater,

            // NULL cases handled above
            _ => Ordering::Equal,
        }
    }
}

impl fmt::Debug for Mem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_null() {
            write!(f, "NULL")
        } else if self.is_int() {
            write!(f, "INT({})", self.i)
        } else if self.is_real() {
            write!(f, "REAL({})", self.r)
        } else if self.is_str() {
            write!(f, "TEXT({:?})", self.to_str())
        } else if self.is_blob() {
            write!(f, "BLOB({} bytes)", self.data.len())
        } else {
            write!(f, "UNKNOWN")
        }
    }
}

impl fmt::Display for Mem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_null() {
            write!(f, "NULL")
        } else if self.is_int() {
            write!(f, "{}", self.i)
        } else if self.is_real() {
            write!(f, "{}", self.r)
        } else if self.is_str() {
            write!(f, "{}", self.to_str())
        } else if self.is_blob() {
            write!(f, "<{} bytes>", self.data.len())
        } else {
            write!(f, "???")
        }
    }
}

// ============================================================================
// Arithmetic Operations
// ============================================================================

impl Mem {
    /// Add two memory cells, storing result in self
    pub fn add(&mut self, other: &Mem) -> Result<()> {
        if self.is_null() || other.is_null() {
            self.set_null();
            return Ok(());
        }

        if self.is_int() && other.is_int() {
            self.set_int(self.i.wrapping_add(other.i));
        } else {
            self.set_real(self.to_real() + other.to_real());
        }
        Ok(())
    }

    /// Subtract other from self
    pub fn subtract(&mut self, other: &Mem) -> Result<()> {
        if self.is_null() || other.is_null() {
            self.set_null();
            return Ok(());
        }

        if self.is_int() && other.is_int() {
            self.set_int(self.i.wrapping_sub(other.i));
        } else {
            self.set_real(self.to_real() - other.to_real());
        }
        Ok(())
    }

    /// Multiply self by other
    pub fn multiply(&mut self, other: &Mem) -> Result<()> {
        if self.is_null() || other.is_null() {
            self.set_null();
            return Ok(());
        }

        if self.is_int() && other.is_int() {
            self.set_int(self.i.wrapping_mul(other.i));
        } else {
            self.set_real(self.to_real() * other.to_real());
        }
        Ok(())
    }

    /// Divide self by other
    pub fn divide(&mut self, other: &Mem) -> Result<()> {
        if self.is_null() || other.is_null() {
            self.set_null();
            return Ok(());
        }

        let divisor = other.to_real();
        if divisor == 0.0 {
            self.set_null();
            return Ok(());
        }

        self.set_real(self.to_real() / divisor);
        Ok(())
    }

    /// Remainder of self divided by other
    pub fn remainder(&mut self, other: &Mem) -> Result<()> {
        if self.is_null() || other.is_null() {
            self.set_null();
            return Ok(());
        }

        let a = self.to_int();
        let b = other.to_int();
        if b == 0 {
            self.set_null();
        } else {
            self.set_int(a % b);
        }
        Ok(())
    }

    /// Concatenate strings
    pub fn concat(&mut self, other: &Mem) -> Result<()> {
        if self.is_null() || other.is_null() {
            self.set_null();
            return Ok(());
        }

        let mut result = self.to_str();
        result.push_str(&other.to_str());
        self.set_str(&result);
        Ok(())
    }

    /// Bitwise AND
    pub fn bit_and(&mut self, other: &Mem) -> Result<()> {
        if self.is_null() || other.is_null() {
            self.set_null();
            return Ok(());
        }
        self.set_int(self.to_int() & other.to_int());
        Ok(())
    }

    /// Bitwise OR
    pub fn bit_or(&mut self, other: &Mem) -> Result<()> {
        if self.is_null() || other.is_null() {
            self.set_null();
            return Ok(());
        }
        self.set_int(self.to_int() | other.to_int());
        Ok(())
    }

    /// Left shift
    pub fn shift_left(&mut self, other: &Mem) -> Result<()> {
        if self.is_null() || other.is_null() {
            self.set_null();
            return Ok(());
        }
        let shift = other.to_int();
        if shift < 0 {
            self.set_int(self.to_int() >> (-shift as u32));
        } else {
            self.set_int(self.to_int() << (shift as u32));
        }
        Ok(())
    }

    /// Right shift
    pub fn shift_right(&mut self, other: &Mem) -> Result<()> {
        if self.is_null() || other.is_null() {
            self.set_null();
            return Ok(());
        }
        let shift = other.to_int();
        if shift < 0 {
            self.set_int(self.to_int() << (-shift as u32));
        } else {
            self.set_int(self.to_int() >> (shift as u32));
        }
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_null() {
        let mem = Mem::new();
        assert!(mem.is_null());
        assert!(!mem.is_int());
        assert_eq!(mem.column_type(), ColumnType::Null);
    }

    #[test]
    fn test_mem_int() {
        let mem = Mem::from_int(42);
        assert!(!mem.is_null());
        assert!(mem.is_int());
        assert_eq!(mem.to_int(), 42);
        assert_eq!(mem.column_type(), ColumnType::Integer);
    }

    #[test]
    fn test_mem_real() {
        let mem = Mem::from_real(3.14);
        assert!(mem.is_real());
        assert!((mem.to_real() - 3.14).abs() < f64::EPSILON);
        assert_eq!(mem.column_type(), ColumnType::Float);
    }

    #[test]
    fn test_mem_str() {
        let mem = Mem::from_str("hello");
        assert!(mem.is_str());
        assert_eq!(mem.to_str(), "hello");
        assert_eq!(mem.column_type(), ColumnType::Text);
    }

    #[test]
    fn test_mem_blob() {
        let mem = Mem::from_blob(&[1, 2, 3]);
        assert!(mem.is_blob());
        assert_eq!(mem.to_blob(), vec![1, 2, 3]);
        assert_eq!(mem.column_type(), ColumnType::Blob);
    }

    #[test]
    fn test_mem_coercion() {
        // Int to real
        let mem = Mem::from_int(42);
        assert_eq!(mem.to_real(), 42.0);

        // Real to int
        let mem = Mem::from_real(3.7);
        assert_eq!(mem.to_int(), 3);

        // String to int
        let mem = Mem::from_str("123");
        assert_eq!(mem.to_int(), 123);

        // Int to string
        let mem = Mem::from_int(42);
        assert_eq!(mem.to_str(), "42");
    }

    #[test]
    fn test_mem_copy() {
        let src = Mem::from_int(42);
        let mut dst = Mem::new();
        dst.copy_from(&src);
        assert_eq!(dst.to_int(), 42);
    }

    #[test]
    fn test_mem_move() {
        let mut src = Mem::from_int(42);
        let mut dst = Mem::new();
        dst.move_from(&mut src);
        assert_eq!(dst.to_int(), 42);
        assert!(src.is_null());
    }

    #[test]
    fn test_mem_arithmetic() {
        let mut a = Mem::from_int(10);
        let b = Mem::from_int(3);

        let mut result = a.clone();
        result.add(&b).unwrap();
        assert_eq!(result.to_int(), 13);

        result = a.clone();
        result.subtract(&b).unwrap();
        assert_eq!(result.to_int(), 7);

        result = a.clone();
        result.multiply(&b).unwrap();
        assert_eq!(result.to_int(), 30);

        result = a.clone();
        result.remainder(&b).unwrap();
        assert_eq!(result.to_int(), 1);
    }

    #[test]
    fn test_mem_division() {
        let mut a = Mem::from_int(10);
        let b = Mem::from_int(4);
        a.divide(&b).unwrap();
        assert_eq!(a.to_real(), 2.5);

        // Division by zero
        let mut a = Mem::from_int(10);
        let b = Mem::from_int(0);
        a.divide(&b).unwrap();
        assert!(a.is_null());
    }

    #[test]
    fn test_mem_concat() {
        let mut a = Mem::from_str("hello ");
        let b = Mem::from_str("world");
        a.concat(&b).unwrap();
        assert_eq!(a.to_str(), "hello world");
    }

    #[test]
    fn test_mem_bitwise() {
        let mut a = Mem::from_int(0b1100);
        let b = Mem::from_int(0b1010);

        let mut result = a.clone();
        result.bit_and(&b).unwrap();
        assert_eq!(result.to_int(), 0b1000);

        result = a.clone();
        result.bit_or(&b).unwrap();
        assert_eq!(result.to_int(), 0b1110);

        result = a.clone();
        result.bit_not().unwrap();
        assert_eq!(result.to_int(), !0b1100i64);
    }

    #[test]
    fn test_mem_shift() {
        let mut a = Mem::from_int(8);
        let b = Mem::from_int(2);

        let mut result = a.clone();
        result.shift_left(&b).unwrap();
        assert_eq!(result.to_int(), 32);

        result = a.clone();
        result.shift_right(&b).unwrap();
        assert_eq!(result.to_int(), 2);
    }

    #[test]
    fn test_mem_comparison() {
        let null = Mem::new();
        let int1 = Mem::from_int(10);
        let int2 = Mem::from_int(20);
        let str1 = Mem::from_str("abc");
        let str2 = Mem::from_str("def");

        // NULL comparisons
        assert_eq!(null.compare(&null), Ordering::Equal);
        assert_eq!(null.compare(&int1), Ordering::Less);

        // Integer comparisons
        assert_eq!(int1.compare(&int2), Ordering::Less);
        assert_eq!(int2.compare(&int1), Ordering::Greater);

        // String comparisons
        assert_eq!(str1.compare(&str2), Ordering::Less);

        // Type ordering (numbers < text)
        assert_eq!(int1.compare(&str1), Ordering::Less);
    }

    #[test]
    fn test_mem_affinity() {
        // String to integer affinity
        let mut mem = Mem::from_str("42");
        mem.apply_affinity(Affinity::Integer);
        assert!(mem.is_int());
        assert_eq!(mem.to_int(), 42);

        // Integer to real affinity
        let mut mem = Mem::from_int(42);
        mem.apply_affinity(Affinity::Real);
        assert!(mem.is_real());
        assert_eq!(mem.to_real(), 42.0);

        // Numeric affinity - integer preferred
        let mut mem = Mem::from_str("42");
        mem.apply_affinity(Affinity::Numeric);
        assert!(mem.is_int());

        // Numeric affinity - real if not integer
        let mut mem = Mem::from_str("3.14");
        mem.apply_affinity(Affinity::Numeric);
        assert!(mem.is_real());
    }

    #[test]
    fn test_mem_truthy() {
        assert!(!Mem::new().is_truthy()); // NULL
        assert!(!Mem::from_int(0).is_truthy());
        assert!(Mem::from_int(1).is_truthy());
        assert!(Mem::from_int(-1).is_truthy());
        assert!(!Mem::from_real(0.0).is_truthy());
        assert!(Mem::from_real(0.1).is_truthy());
        assert!(!Mem::from_str("").is_truthy());
        assert!(Mem::from_str("x").is_truthy());
    }

    #[test]
    fn test_mem_logical_not() {
        let mut mem = Mem::from_int(1);
        mem.logical_not();
        assert_eq!(mem.to_int(), 0);

        let mut mem = Mem::from_int(0);
        mem.logical_not();
        assert_eq!(mem.to_int(), 1);

        let mut mem = Mem::new(); // NULL
        mem.logical_not();
        assert!(mem.is_null()); // NULL NOT -> NULL
    }

    #[test]
    fn test_mem_null_arithmetic() {
        let mut a = Mem::new(); // NULL
        let b = Mem::from_int(42);
        a.add(&b).unwrap();
        assert!(a.is_null()); // NULL + x = NULL
    }

    #[test]
    fn test_mem_to_value() {
        assert_eq!(Mem::new().to_value(), Value::Null);
        assert_eq!(Mem::from_int(42).to_value(), Value::Integer(42));
        assert_eq!(Mem::from_real(3.14).to_value(), Value::Real(3.14));
        assert_eq!(
            Mem::from_str("test").to_value(),
            Value::Text("test".to_string())
        );
        assert_eq!(
            Mem::from_blob(&[1, 2, 3]).to_value(),
            Value::Blob(vec![1, 2, 3])
        );
    }

    #[test]
    fn test_mem_from_value() {
        let mem = Mem::from_value(&Value::Integer(42));
        assert_eq!(mem.to_int(), 42);

        let mem = Mem::from_value(&Value::Text("hello".to_string()));
        assert_eq!(mem.to_str(), "hello");
    }
}
