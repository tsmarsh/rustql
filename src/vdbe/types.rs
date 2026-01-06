//! VDBE Internal Types (vdbeInt.h translation)
//!
//! Internal types and constants used by the VDBE implementation.
//! These correspond to the definitions in SQLite's vdbeInt.h.

use std::any::Any;
use std::cmp::Ordering;
use std::ffi::c_void;
use std::sync::{Arc, Weak};

use crate::api::connection::SqliteConnection;
use crate::storage::btree::BtCursor;
use crate::types::Pgno;
use crate::vdbe::engine::Vdbe;

/// sqlite3 connection handle type alias.
pub type Connection = SqliteConnection;

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

/// Default page cache size
pub const SQLITE_DEFAULT_CACHE_SIZE: i32 = -2000;

/// Default number of memory cells
pub const SQLITE_DEFAULT_VDBE_MEM: i32 = 128;

/// Default number of cursor slots
pub const SQLITE_DEFAULT_VDBE_CURSORS: i32 = 16;

/// Maximum recursion depth for triggers
pub const SQLITE_MAX_TRIGGER_DEPTH: i32 = 1000;

/// Maximum compound select statements
pub const SQLITE_MAX_COMPOUND_SELECT: i32 = 500;

// ============================================================================
// Memory Cell
// ============================================================================

bitflags::bitflags! {
    /// Memory cell flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct MemFlags: u16 {
        const NULL     = 0x0001;
        const STR      = 0x0002;
        const INT      = 0x0004;
        const REAL     = 0x0008;
        const BLOB     = 0x0010;
        const INTREAL  = 0x0020;
        const ZERO     = 0x0040;
        const SUBTYPE  = 0x0080;
        const TERM     = 0x0200;
        const DYN      = 0x0400;
        const STATIC   = 0x0800;
        const EPHEM    = 0x1000;
        const FRAME    = 0x2000;
    }
}

/// Value storage for a Mem cell.
#[derive(Debug, Clone)]
pub enum MemValue {
    Null,
    Int(i64),
    Real(f64),
    Str { data: String, nul: bool },
    Blob(Vec<u8>),
    ZeroBlob(i32),
    Ptr { ptr: *mut c_void, type_name: String },
}

/// VDBE memory cell (sqlite3_value).
#[derive(Debug, Clone)]
pub struct Mem {
    /// Union of value types.
    pub value: MemValue,
    /// Type flags.
    pub flags: MemFlags,
    /// Text encoding.
    pub enc: Encoding,
    /// Number of bytes in string/blob.
    pub n: i32,
    /// Associated database connection for memory accounting.
    pub db: Option<Weak<Connection>>,
}

impl Mem {
    /// Create a new NULL value.
    pub fn new() -> Self {
        Self {
            value: MemValue::Null,
            flags: MemFlags::NULL,
            enc: Encoding::Utf8,
            n: 0,
            db: None,
        }
    }

    /// Classify value for comparisons (NULL < numeric < text < blob).
    pub fn type_class(&self) -> TypeClass {
        match &self.value {
            MemValue::Null => TypeClass::Null,
            MemValue::Int(_) | MemValue::Real(_) => TypeClass::Numeric,
            MemValue::Str { .. } => TypeClass::Text,
            MemValue::Blob(_) | MemValue::ZeroBlob(_) => TypeClass::Blob,
            MemValue::Ptr { .. } => TypeClass::Null,
        }
    }

    /// Compare two Mem values using optional collation.
    pub fn compare(&self, other: &Mem, coll: Option<&CollSeq>) -> Ordering {
        if matches!(self.value, MemValue::Null) {
            return if matches!(other.value, MemValue::Null) {
                Ordering::Equal
            } else {
                Ordering::Less
            };
        }
        if matches!(other.value, MemValue::Null) {
            return Ordering::Greater;
        }

        let tc1 = self.type_class();
        let tc2 = other.type_class();
        if tc1 != tc2 {
            return tc1.cmp(&tc2);
        }

        match (&self.value, &other.value) {
            (MemValue::Int(a), MemValue::Int(b)) => a.cmp(b),
            (MemValue::Real(a), MemValue::Real(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (MemValue::Int(a), MemValue::Real(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (MemValue::Real(a), MemValue::Int(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            (MemValue::Str { data: a, .. }, MemValue::Str { data: b, .. }) => {
                if let Some(c) = coll {
                    c.compare(a, b)
                } else {
                    a.cmp(b)
                }
            }
            (MemValue::Blob(a), MemValue::Blob(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}

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
// Key Info and Cursors
// ============================================================================

/// Key comparison information for indexes.
pub struct KeyInfo {
    pub db: Weak<Connection>,
    pub enc: Encoding,
    pub n_key_field: u16,
    pub n_all_field: u16,
    pub sort_order: Vec<bool>,
    pub collations: Vec<Arc<CollSeq>>,
}

/// Opaque sorter object placeholder.
pub struct VdbeSorter;

/// Cursor wrapper for table/index/sorter/pseudo-table access.
pub struct VdbeCursor {
    pub cursor_type: CursorType,
    pub idx: i32,
    pub root: Pgno,
    pub writable: bool,
    pub btree_cursor: Option<BtCursor>,
    pub pseudo_data: Option<Vec<u8>>,
    pub sorter: Option<VdbeSorter>,
    pub cached_columns: Vec<Option<Mem>>,
    pub payload: Option<Vec<u8>>,
    pub key_info: Option<Arc<KeyInfo>>,
    pub null_row: bool,
    pub seek_result: i32,
}

/// Stack frame for subroutines and triggers.
pub struct VdbeFrame {
    pub v: *mut Vdbe,
    pub parent: Option<Box<VdbeFrame>>,
    pub mem: Vec<Mem>,
    pub cursors: Vec<Option<VdbeCursor>>,
    pub pc: i32,
    pub n_op: i32,
    pub n_mem: i32,
    pub n_cursor: i32,
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
/// - 'F' (0x46) = FLEXNUM
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
    /// FLEXNUM affinity - numeric with flexible storage
    Flexnum = 0x46,
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
            0x46 | b'F' => Some(Affinity::Flexnum),
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
// Opcodes
// ============================================================================

/// VDBE opcode values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Goto = 0,
    Gosub = 1,
    Return = 2,
    InitCoroutine = 3,
    EndCoroutine = 4,
    Yield = 5,
    HaltIfNull = 6,
    Halt = 7,
    Integer = 8,
    Int64 = 9,
    Real = 10,
    String8 = 11,
    String = 12,
    BeginSubrtn = 13,
    Null = 14,
    SoftNull = 15,
    Blob = 16,
    Variable = 17,
    Move = 18,
    Copy = 19,
    SCopy = 20,
    IntCopy = 21,
    FkCheck = 22,
    ResultRow = 23,
    Concat = 24,
    Add = 25,
    Subtract = 26,
    Multiply = 27,
    Divide = 28,
    Remainder = 29,
    CollSeq = 30,
    BitAnd = 31,
    BitOr = 32,
    ShiftLeft = 33,
    ShiftRight = 34,
    AddImm = 35,
    MustBeInt = 36,
    RealAffinity = 37,
    Cast = 38,
    Eq = 39,
    Ne = 40,
    Lt = 41,
    Le = 42,
    Gt = 43,
    Ge = 44,
    ElseEq = 45,
    Permutation = 46,
    Compare = 47,
    Jump = 48,
    And = 49,
    Or = 50,
    IsTrue = 51,
    Not = 52,
    BitNot = 53,
    Once = 54,
    If = 55,
    IfNot = 56,
    IsNull = 57,
    IsType = 58,
    ZeroOrNull = 59,
    NotNull = 60,
    IfNullRow = 61,
    Offset = 62,
    Column = 63,
    TypeCheck = 64,
    Affinity = 65,
    MakeRecord = 66,
    Count = 67,
    Savepoint = 68,
    AutoCommit = 69,
    Transaction = 70,
    ReadCookie = 71,
    SetCookie = 72,
    ReopenIdx = 73,
    OpenRead = 74,
    OpenWrite = 75,
    OpenDup = 76,
    OpenAutoindex = 77,
    OpenEphemeral = 78,
    SorterOpen = 79,
    SequenceTest = 80,
    OpenPseudo = 81,
    Close = 82,
    ColumnsUsed = 83,
    SeekLT = 84,
    SeekLE = 85,
    SeekGE = 86,
    SeekGT = 87,
    SeekScan = 88,
    SeekHit = 89,
    IfNotOpen = 90,
    IfNoHope = 91,
    NoConflict = 92,
    NotFound = 93,
    Found = 94,
    SeekRowid = 95,
    NotExists = 96,
    Sequence = 97,
    NewRowid = 98,
    Insert = 99,
    RowCell = 100,
    Delete = 101,
    ResetCount = 102,
    SorterCompare = 103,
    SorterData = 104,
    RowData = 105,
    Rowid = 106,
    NullRow = 107,
    SeekEnd = 108,
    Last = 109,
    IfSizeBetween = 110,
    SorterSort = 111,
    Sort = 112,
    Rewind = 113,
    IfEmpty = 114,
    SorterNext = 115,
    Prev = 116,
    Next = 117,
    IdxInsert = 118,
    SorterInsert = 119,
    IdxDelete = 120,
    DeferredSeek = 121,
    IdxRowid = 122,
    FinishSeek = 123,
    IdxLE = 124,
    IdxGT = 125,
    IdxLT = 126,
    IdxGE = 127,
    Destroy = 128,
    Clear = 129,
    ResetSorter = 130,
    CreateBtree = 131,
    SqlExec = 132,
    ParseSchema = 133,
    LoadAnalysis = 134,
    DropTable = 135,
    DropIndex = 136,
    DropTrigger = 137,
    IntegrityCk = 138,
    RowSetAdd = 139,
    RowSetRead = 140,
    RowSetTest = 141,
    Program = 142,
    Param = 143,
    FkCounter = 144,
    FkIfZero = 145,
    MemMax = 146,
    IfPos = 147,
    OffsetLimit = 148,
    IfNotZero = 149,
    DecrJumpZero = 150,
    AggInverse = 151,
    AggStep = 152,
    AggStep1 = 153,
    AggValue = 154,
    AggFinal = 155,
    Checkpoint = 156,
    JournalMode = 157,
    Vacuum = 158,
    IncrVacuum = 159,
    Expire = 160,
    CursorLock = 161,
    CursorUnlock = 162,
    TableLock = 163,
    VBegin = 164,
    VCreate = 165,
    VDestroy = 166,
    VOpen = 167,
    VCheck = 168,
    VInitIn = 169,
    VFilter = 170,
    VColumn = 171,
    VNext = 172,
    VRename = 173,
    VUpdate = 174,
    Pagecount = 175,
    MaxPgcnt = 176,
    PureFunc = 177,
    Function = 178,
    ClrSubtype = 179,
    GetSubtype = 180,
    SetSubtype = 181,
    FilterAdd = 182,
    Filter = 183,
    Trace = 184,
    Init = 185,
    CursorHint = 186,
    ReleaseReg = 187,
    Noop = 188,
    Explain = 189,
    Abortable = 190,
    MaxOpcode = 190,
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
