# Translate vdbeInt.h - VDBE Internals

## Overview
Translate the VDBE internal header defining private structures, opcodes, and constants used by the virtual machine implementation.

## Source Reference
- `sqlite3/src/vdbeInt.h` - 750 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Mem (sqlite3_value)
Memory cell holding a single value:
```rust
#[derive(Debug, Clone)]
pub struct Mem {
    /// Union of value types
    value: MemValue,

    /// Type flags
    flags: MemFlags,

    /// Encoding for strings (UTF-8, UTF-16LE, UTF-16BE)
    enc: Encoding,

    /// Number of bytes in string/blob (not including nul terminator)
    n: i32,

    /// Associated database connection (for memory accounting)
    db: Option<Weak<Connection>>,
}

#[derive(Debug, Clone)]
pub enum MemValue {
    Null,
    Int(i64),
    Real(f64),
    /// String with optional nul terminator
    Str { data: String, nul: bool },
    /// Binary blob
    Blob(Vec<u8>),
    /// Zero-blob (n bytes of zeros, lazily materialized)
    ZeroBlob(i32),
    /// Pointer to external data
    Ptr { ptr: *mut c_void, type_name: String },
}

bitflags! {
    pub struct MemFlags: u16 {
        const NULL     = 0x0001;  // Value is NULL
        const STR      = 0x0002;  // Value is a string
        const INT      = 0x0004;  // Value is an integer
        const REAL     = 0x0008;  // Value is a real number
        const BLOB     = 0x0010;  // Value is a BLOB
        const INTREAL  = 0x0020;  // Int rep of REAL value
        const ZERO     = 0x0040;  // Zero-filled blob
        const SUBTYPE  = 0x0080;  // Has subtype
        const TERM     = 0x0200;  // String has nul terminator
        const DYN      = 0x0400;  // Need to free string/blob
        const STATIC   = 0x0800;  // Static string/blob
        const EPHEM    = 0x1000;  // Ephemeral string/blob
        const FRAME    = 0x2000;  // Mem in VdbeFrame
    }
}
```

### VdbeCursor
Cursor for traversing database tables/indexes:
```rust
pub struct VdbeCursor {
    /// Cursor type
    cursor_type: CursorType,

    /// Index into Vdbe.cursors array
    idx: i32,

    /// Root page number
    root: Pgno,

    /// True if this is a writable cursor
    writable: bool,

    /// B-tree cursor (for table/index)
    btree_cursor: Option<BtCursor>,

    /// Pseudo-table cursor (for subqueries)
    pseudo_data: Option<Vec<u8>>,

    /// Sorter cursor
    sorter: Option<VdbeSorter>,

    /// Column cache
    cached_columns: Vec<Option<Mem>>,

    /// Current row data
    payload: Option<Vec<u8>>,

    /// Key info for index cursors
    key_info: Option<Arc<KeyInfo>>,

    /// NULL row flag
    null_row: bool,

    /// Seek result (for deferred seek)
    seek_result: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorType {
    BTree,      // Normal B-tree cursor
    Sorter,     // Sorter for ORDER BY
    Pseudo,     // Pseudo-table (single row)
    VTab,       // Virtual table
}
```

### VdbeFrame
Stack frame for subroutines (triggers, coroutines):
```rust
pub struct VdbeFrame {
    /// Parent VM
    v: *mut Vdbe,

    /// Parent frame (or None for top level)
    parent: Option<Box<VdbeFrame>>,

    /// Saved memory cells
    mem: Vec<Mem>,

    /// Saved cursors
    cursors: Vec<Option<VdbeCursor>>,

    /// Saved program counter
    pc: i32,

    /// Number of ops in parent
    n_op: i32,

    /// Number of memory cells in parent
    n_mem: i32,

    /// Number of cursors in parent
    n_cursor: i32,
}
```

### KeyInfo
Key comparison information for indexes:
```rust
pub struct KeyInfo {
    /// Database connection
    db: Weak<Connection>,

    /// Encoding
    enc: Encoding,

    /// Number of key columns
    n_key_field: u16,

    /// Total fields including rowid
    n_all_field: u16,

    /// Sort orders (true = DESC)
    sort_order: Vec<bool>,

    /// Collation sequences per column
    collations: Vec<Arc<CollSeq>>,
}
```

### CollSeq
Collation sequence for string comparisons:
```rust
pub struct CollSeq {
    /// Collation name (e.g., "BINARY", "NOCASE")
    name: String,

    /// Encoding this collation handles
    enc: Encoding,

    /// Comparison function
    cmp: fn(&str, &str) -> Ordering,

    /// User data for custom collations
    user_data: Option<Box<dyn Any>>,
}
```

## Opcodes Enum

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    // Initialization
    Init = 0,
    Goto = 1,
    Gosub = 2,
    Return = 3,
    Yield = 4,

    // Halt and transaction
    Halt = 5,
    HaltIfNull = 6,
    Transaction = 7,
    Savepoint = 8,
    AutoCommit = 9,

    // ... continue for all ~180 opcodes

    // Maximum opcode value
    MaxOpcode = 186,
}
```

## Constants

```rust
/// Maximum number of memory cells
pub const SQLITE_MAX_VARIABLE_NUMBER: i32 = 32766;

/// Default cache size
pub const SQLITE_DEFAULT_CACHE_SIZE: i32 = -2000;

/// VDBE magic numbers
pub const VDBE_MAGIC_INIT: u32 = 0x16bceaa5;
pub const VDBE_MAGIC_RUN: u32 = 0x2df20da3;
pub const VDBE_MAGIC_HALT: u32 = 0x319c2973;
pub const VDBE_MAGIC_RESET: u32 = 0x48fa9f76;
pub const VDBE_MAGIC_DEAD: u32 = 0x5606c3c8;
```

## Affinity Types

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Affinity {
    Blob = 0x41,      // 'A' - No preference
    Text = 0x42,      // 'B' - String affinity
    Numeric = 0x43,   // 'C' - Numeric affinity
    Integer = 0x44,   // 'D' - Integer affinity
    Real = 0x45,      // 'E' - Real affinity
    Flexnum = 0x46,   // 'F' - Flexible numeric
}
```

## Comparison Helpers

```rust
impl Mem {
    /// Get type for comparison purposes
    pub fn type_class(&self) -> i32 {
        match &self.value {
            MemValue::Null => 0,
            MemValue::Int(_) => 1,
            MemValue::Real(_) => 1,
            MemValue::Str { .. } => 2,
            MemValue::Blob(_) => 3,
            MemValue::ZeroBlob(_) => 3,
            MemValue::Ptr { .. } => 0,
        }
    }

    /// Compare two Mem values
    pub fn compare(&self, other: &Mem, coll: Option<&CollSeq>) -> Ordering {
        // NULL is less than everything
        if self.is_null() {
            return if other.is_null() {
                Ordering::Equal
            } else {
                Ordering::Less
            };
        }
        if other.is_null() {
            return Ordering::Greater;
        }

        // Compare by type class
        let tc1 = self.type_class();
        let tc2 = other.type_class();
        if tc1 != tc2 {
            return tc1.cmp(&tc2);
        }

        // Same type - compare values
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
```

## Acceptance Criteria
- [ ] Mem struct with MemValue enum
- [ ] MemFlags bitflags defined
- [ ] VdbeCursor struct with CursorType
- [ ] VdbeFrame for subroutine calls
- [ ] KeyInfo and CollSeq structures
- [ ] All opcodes in Opcode enum
- [ ] Affinity enum with SQLite values
- [ ] Mem comparison logic
- [ ] VDBE magic constants
