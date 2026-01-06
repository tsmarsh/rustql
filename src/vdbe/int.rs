//! VDBE internals translated from SQLite's vdbeInt.h.

use std::any::Any;
use std::cmp::Ordering;
use std::ffi::c_void;
use std::sync::{Arc, Weak};

use crate::api::connection::SqliteConnection;
use crate::schema::Encoding;
use crate::storage::btree::BtCursor;
use crate::types::Pgno;
use crate::vdbe::engine::Vdbe;

/// sqlite3 connection handle type alias.
pub type Connection = SqliteConnection;

// ============================================================================
// Constants
// ============================================================================

/// Maximum number of variables in a SQL statement.
pub const SQLITE_MAX_VARIABLE_NUMBER: i32 = 32766;

/// Default page cache size.
pub const SQLITE_DEFAULT_CACHE_SIZE: i32 = -2000;

/// VDBE magic numbers.
pub const VDBE_MAGIC_INIT: u32 = 0x16bceaa5;
pub const VDBE_MAGIC_RUN: u32 = 0x2df20da3;
pub const VDBE_MAGIC_HALT: u32 = 0x319c2973;
pub const VDBE_MAGIC_RESET: u32 = 0x48fa9f76;
pub const VDBE_MAGIC_DEAD: u32 = 0x5606c3c8;

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
    pub fn type_class(&self) -> i32 {
        match &self.value {
            MemValue::Null => 0,
            MemValue::Int(_) | MemValue::Real(_) => 1,
            MemValue::Str { .. } => 2,
            MemValue::Blob(_) | MemValue::ZeroBlob(_) => 3,
            MemValue::Ptr { .. } => 0,
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
            (MemValue::Str { data: a, .. }, MemValue::Str { data: b, .. }) => {
                if let Some(c) = coll {
                    (c.cmp)(a, b)
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
// Collation and Key Info
// ============================================================================

/// Collation sequence for string comparisons.
pub struct CollSeq {
    pub name: String,
    pub enc: Encoding,
    pub cmp: fn(&str, &str) -> Ordering,
    pub user_data: Option<Box<dyn Any>>,
}

/// Key comparison information for indexes.
pub struct KeyInfo {
    pub db: Weak<Connection>,
    pub enc: Encoding,
    pub n_key_field: u16,
    pub n_all_field: u16,
    pub sort_order: Vec<bool>,
    pub collations: Vec<Arc<CollSeq>>,
}

// ============================================================================
// Cursor Types
// ============================================================================

/// Cursor type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorType {
    BTree,
    Sorter,
    Pseudo,
    VTab,
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

// ============================================================================
// VDBE Frames
// ============================================================================

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
// Affinity
// ============================================================================

/// Affinity types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Affinity {
    Blob = b'A',
    Text = b'B',
    Numeric = b'C',
    Integer = b'D',
    Real = b'E',
    Flexnum = b'F',
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
