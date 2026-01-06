//! VDBE Internal Types (vdbeInt.h translation)
//!
//! Internal types and constants used by the VDBE implementation.
//! These correspond to the definitions in SQLite's vdbeInt.h.

use std::cmp::Ordering;
use std::sync::Arc;

// ============================================================================
// VDBE Magic Numbers
// ============================================================================

/// Magic number indicating VDBE is initialized but not yet run
pub const VDBE_MAGIC_INIT: u32 = 0x16bceaa5;

/// Magic number indicating VDBE is currently running
pub const VDBE_MAGIC_RUN: u32 = 0x2df20da3;

/// Magic number indicating VDBE execution has halted
pub const VDBE_MAGIC_HALT: u32 = 0x319c2973;

/// Magic number indicating VDBE has been reset
pub const VDBE_MAGIC_RESET: u32 = 0x48fa9f76;

/// Magic number indicating VDBE is dead (finalized)
pub const VDBE_MAGIC_DEAD: u32 = 0x5606c3c8;

// ============================================================================
// Limits
// ============================================================================

/// Maximum number of bound parameters
pub const SQLITE_MAX_VARIABLE_NUMBER: i32 = 32766;

/// Default number of memory cells
pub const SQLITE_DEFAULT_VDBE_MEM: i32 = 128;

/// Default number of cursor slots
pub const SQLITE_DEFAULT_VDBE_CURSORS: i32 = 16;

/// Maximum recursion depth for triggers
pub const SQLITE_MAX_TRIGGER_DEPTH: i32 = 1000;

/// Maximum compound select statements
pub const SQLITE_MAX_COMPOUND_SELECT: i32 = 500;

// ============================================================================
// Cursor Types
// ============================================================================

/// Type of VDBE cursor
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CursorType {
    /// Normal B-tree cursor (table or index)
    #[default]
    BTree,
    /// Sorter cursor for ORDER BY
    Sorter,
    /// Pseudo-cursor (reads from registers)
    Pseudo,
    /// Virtual table cursor
    VTab,
}

// ============================================================================
// Collation Sequence
// ============================================================================

/// Comparison function type for collation sequences
pub type CollationCmp = Arc<dyn Fn(&str, &str) -> Ordering + Send + Sync>;

/// Collation sequence for string comparison
#[derive(Clone)]
pub struct CollSeq {
    /// Collation name (e.g., "BINARY", "NOCASE", "RTRIM")
    pub name: String,
    /// Text encoding this collation handles
    pub encoding: Encoding,
    /// Comparison function
    pub cmp: CollationCmp,
}

impl CollSeq {
    /// Create the default BINARY collation
    pub fn binary() -> Self {
        Self {
            name: "BINARY".to_string(),
            encoding: Encoding::Utf8,
            cmp: Arc::new(|a: &str, b: &str| a.cmp(b)),
        }
    }

    /// Create the NOCASE collation (case-insensitive for ASCII)
    pub fn nocase() -> Self {
        Self {
            name: "NOCASE".to_string(),
            encoding: Encoding::Utf8,
            cmp: Arc::new(|a: &str, b: &str| {
                a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase())
            }),
        }
    }

    /// Create the RTRIM collation (ignores trailing spaces)
    pub fn rtrim() -> Self {
        Self {
            name: "RTRIM".to_string(),
            encoding: Encoding::Utf8,
            cmp: Arc::new(|a: &str, b: &str| a.trim_end().cmp(b.trim_end())),
        }
    }

    /// Compare two strings using this collation
    pub fn compare(&self, a: &str, b: &str) -> Ordering {
        (self.cmp)(a, b)
    }
}

impl std::fmt::Debug for CollSeq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CollSeq")
            .field("name", &self.name)
            .field("encoding", &self.encoding)
            .finish()
    }
}

impl PartialEq for CollSeq {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.encoding == other.encoding
    }
}

// ============================================================================
// Encoding
// ============================================================================

/// Text encoding
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Encoding {
    /// UTF-8 encoding (default)
    #[default]
    Utf8,
    /// UTF-16 Little Endian
    Utf16Le,
    /// UTF-16 Big Endian
    Utf16Be,
}

impl Encoding {
    /// Get encoding code for database header
    pub fn code(&self) -> u32 {
        match self {
            Encoding::Utf8 => 1,
            Encoding::Utf16Le => 2,
            Encoding::Utf16Be => 3,
        }
    }

    /// Create encoding from database header code
    pub fn from_code(code: u32) -> Option<Self> {
        match code {
            1 => Some(Encoding::Utf8),
            2 => Some(Encoding::Utf16Le),
            3 => Some(Encoding::Utf16Be),
            _ => None,
        }
    }
}

// ============================================================================
// Affinity
// ============================================================================

/// Column type affinity (determines type conversion behavior)
///
/// Character codes match SQLite's internal representation:
/// - 'A' (0x41) = BLOB
/// - 'B' (0x42) = TEXT
/// - 'C' (0x43) = NUMERIC
/// - 'D' (0x44) = INTEGER
/// - 'E' (0x45) = REAL
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Affinity {
    /// BLOB affinity - no type preference
    #[default]
    Blob = 0x41,
    /// TEXT affinity - prefers string
    Text = 0x42,
    /// NUMERIC affinity - prefers integer/real
    Numeric = 0x43,
    /// INTEGER affinity - prefers integer
    Integer = 0x44,
    /// REAL affinity - prefers real
    Real = 0x45,
}

impl Affinity {
    /// Get the character code for this affinity
    pub fn code(&self) -> u8 {
        *self as u8
    }

    /// Create affinity from character code
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0x41 | b'A' => Some(Affinity::Blob),
            0x42 | b'B' => Some(Affinity::Text),
            0x43 | b'C' => Some(Affinity::Numeric),
            0x44 | b'D' => Some(Affinity::Integer),
            0x45 | b'E' => Some(Affinity::Real),
            _ => None,
        }
    }

    /// Get affinity from SQL type name
    pub fn from_type_name(type_name: &str) -> Self {
        let upper = type_name.to_uppercase();

        // INTEGER affinity
        if upper.contains("INT") {
            return Affinity::Integer;
        }

        // TEXT affinity
        if upper.contains("CHAR")
            || upper.contains("CLOB")
            || upper.contains("TEXT")
        {
            return Affinity::Text;
        }

        // BLOB affinity
        if upper.contains("BLOB") || type_name.is_empty() {
            return Affinity::Blob;
        }

        // REAL affinity
        if upper.contains("REAL")
            || upper.contains("FLOA")
            || upper.contains("DOUB")
        {
            return Affinity::Real;
        }

        // Default to NUMERIC
        Affinity::Numeric
    }
}

// ============================================================================
// Type Classes
// ============================================================================

/// Type class for comparison ordering
///
/// When comparing values of different types, SQLite orders by type class:
/// NULL < numbers < text < blob
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
pub enum TypeClass {
    /// NULL values
    Null = 0,
    /// Numbers (INTEGER and REAL)
    Numeric = 1,
    /// Text strings
    Text = 2,
    /// Binary blobs
    Blob = 3,
}

// ============================================================================
// P5 Flags
// ============================================================================

/// Flags for comparison operations (stored in P5)
pub mod compare_flags {
    /// NULL values compare equal
    pub const NULLEQ: u16 = 0x80;
    /// Jump if either operand is NULL
    pub const JUMPIFNULL: u16 = 0x10;
    /// Store result in P2 register
    pub const STOREP2: u16 = 0x20;
    /// Affinity mask (low 4 bits)
    pub const AFFINITY_MASK: u16 = 0x0F;
    /// Permutation index for OP_Compare
    pub const PERMUTATION: u16 = 0x01;
}

/// Flags for seek operations (stored in P5)
pub mod seek_flags {
    /// Seek to last occurrence
    pub const SEEK_LAST: u16 = 0x01;
    /// Unique constraint
    pub const SEEK_UNIQUE: u16 = 0x02;
    /// Scan in reverse
    pub const REVERSE: u16 = 0x04;
}

/// Flags for insert operations (stored in P5)
pub mod insert_flags {
    /// Table is WITHOUT ROWID
    pub const NO_CONFLICT: u16 = 0x01;
    /// Overwrite existing entry
    pub const OVERWRITE: u16 = 0x02;
    /// Append to end
    pub const APPEND: u16 = 0x04;
    /// Use rowid from stack
    pub const USE_SEEK_RESULT: u16 = 0x08;
    /// Isvalid flag
    pub const ISUPDATE: u16 = 0x10;
    /// No constraint errors
    pub const IGNORE: u16 = 0x20;
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_affinity_from_code() {
        assert_eq!(Affinity::from_code(0x41), Some(Affinity::Blob));
        assert_eq!(Affinity::from_code(0x44), Some(Affinity::Integer));
        assert_eq!(Affinity::from_code(b'B'), Some(Affinity::Text));
        assert_eq!(Affinity::from_code(0xFF), None);
    }

    #[test]
    fn test_affinity_from_type_name() {
        assert_eq!(Affinity::from_type_name("INTEGER"), Affinity::Integer);
        assert_eq!(Affinity::from_type_name("INT"), Affinity::Integer);
        assert_eq!(Affinity::from_type_name("BIGINT"), Affinity::Integer);
        assert_eq!(Affinity::from_type_name("TEXT"), Affinity::Text);
        assert_eq!(Affinity::from_type_name("VARCHAR(100)"), Affinity::Text);
        assert_eq!(Affinity::from_type_name("BLOB"), Affinity::Blob);
        assert_eq!(Affinity::from_type_name(""), Affinity::Blob);
        assert_eq!(Affinity::from_type_name("REAL"), Affinity::Real);
        assert_eq!(Affinity::from_type_name("FLOAT"), Affinity::Real);
        assert_eq!(Affinity::from_type_name("DOUBLE"), Affinity::Real);
        assert_eq!(Affinity::from_type_name("DECIMAL"), Affinity::Numeric);
    }

    #[test]
    fn test_collseq_binary() {
        let coll = CollSeq::binary();
        assert_eq!(coll.compare("abc", "abd"), Ordering::Less);
        assert_eq!(coll.compare("abc", "abc"), Ordering::Equal);
        assert_eq!(coll.compare("ABC", "abc"), Ordering::Less); // ASCII order
    }

    #[test]
    fn test_collseq_nocase() {
        let coll = CollSeq::nocase();
        assert_eq!(coll.compare("ABC", "abc"), Ordering::Equal);
        assert_eq!(coll.compare("ABC", "abd"), Ordering::Less);
    }

    #[test]
    fn test_collseq_rtrim() {
        let coll = CollSeq::rtrim();
        assert_eq!(coll.compare("abc", "abc   "), Ordering::Equal);
        assert_eq!(coll.compare("abc  ", "abc"), Ordering::Equal);
    }

    #[test]
    fn test_encoding() {
        assert_eq!(Encoding::Utf8.code(), 1);
        assert_eq!(Encoding::Utf16Le.code(), 2);
        assert_eq!(Encoding::Utf16Be.code(), 3);

        assert_eq!(Encoding::from_code(1), Some(Encoding::Utf8));
        assert_eq!(Encoding::from_code(2), Some(Encoding::Utf16Le));
        assert_eq!(Encoding::from_code(3), Some(Encoding::Utf16Be));
        assert_eq!(Encoding::from_code(0), None);
    }

    #[test]
    fn test_cursor_type() {
        assert_eq!(CursorType::default(), CursorType::BTree);
    }

    #[test]
    fn test_type_class_ordering() {
        assert!(TypeClass::Null < TypeClass::Numeric);
        assert!(TypeClass::Numeric < TypeClass::Text);
        assert!(TypeClass::Text < TypeClass::Blob);
    }

    #[test]
    fn test_magic_constants() {
        // Just ensure they're different
        assert_ne!(VDBE_MAGIC_INIT, VDBE_MAGIC_RUN);
        assert_ne!(VDBE_MAGIC_RUN, VDBE_MAGIC_HALT);
        assert_ne!(VDBE_MAGIC_HALT, VDBE_MAGIC_DEAD);
    }
}
