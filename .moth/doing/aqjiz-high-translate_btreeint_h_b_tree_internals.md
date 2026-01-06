# Translate btreeInt.h - B-tree Internals

## Overview
Translate the internal B-tree header defining private structures, constants, and macros used by the B-tree implementation.

## Source Reference
- `sqlite3/src/btreeInt.h` - 740 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Constants

### Page Types
```rust
/// Page type flags (stored in page header byte 0)
pub const PTF_INTKEY: u8 = 0x01;      // Integer keys (tables)
pub const PTF_ZERODATA: u8 = 0x02;    // Zero-length data
pub const PTF_LEAFDATA: u8 = 0x04;    // Data only on leaves
pub const PTF_LEAF: u8 = 0x08;        // Leaf page

/// Derived page types
pub const PTF_TABLE_LEAF: u8 = PTF_INTKEY | PTF_LEAFDATA | PTF_LEAF;  // 0x0D
pub const PTF_TABLE_INTERIOR: u8 = PTF_INTKEY | PTF_LEAFDATA;          // 0x05
pub const PTF_INDEX_LEAF: u8 = PTF_LEAF;                                // 0x0A
pub const PTF_INDEX_INTERIOR: u8 = 0x02;                                // 0x02
```

### Page Layout Constants
```rust
/// Page header sizes
pub const PAGE_HEADER_SIZE_LEAF: usize = 8;
pub const PAGE_HEADER_SIZE_INTERIOR: usize = 12;

/// Maximum embedded payload fraction (255 = 100%)
pub const MAX_EMBEDDED: u8 = 64;      // ~25% for overflow threshold
pub const MIN_EMBEDDED: u8 = 32;      // ~12.5% minimum on page

/// Cell pointer size
pub const CELL_PTR_SIZE: usize = 2;

/// Maximum page size
pub const MAX_PAGE_SIZE: u32 = 65536;
pub const MIN_PAGE_SIZE: u32 = 512;
pub const DEFAULT_PAGE_SIZE: u32 = 4096;
```

### Cursor States
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorState {
    Invalid = 0,    // Cursor not pointing to valid entry
    Valid = 1,      // Cursor points to a valid entry
    RequireSeek = 2, // Need to seek before reading
    Fault = 3,      // Cursor encountered an error
}
```

### Lock Types
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtLock {
    Read = 1,
    Write = 2,
}
```

## Key Structures

### CellInfo
Information about a single cell on a page:
```rust
#[derive(Debug, Clone, Default)]
pub struct CellInfo {
    pub key: i64,           // Integer key (for intkey tables)
    pub payload: u32,       // Total payload size
    pub header: u32,        // Size of cell header
    pub local: u32,         // Payload bytes on this page
    pub overflow: Pgno,     // First overflow page (0 if none)
    pub size: u16,          // Total cell size on page
}
```

### IntegrityCk
Structure for integrity check operations:
```rust
pub struct IntegrityCk {
    pub db: Arc<Connection>,
    pub btree: Arc<BtShared>,
    pub page_refs: BitVec,      // Pages referenced
    pub page_counts: Vec<u32>,  // Reference counts
    pub max_err: i32,           // Max errors to report
    pub n_err: i32,             // Errors found
    pub errors: Vec<String>,    // Error messages
}
```

### BtreePayload
Describes payload for insert operations:
```rust
pub struct BtreePayload {
    pub key: Option<Vec<u8>>,   // Key for index entries
    pub key_int: i64,           // Integer key for tables
    pub data: Option<Vec<u8>>,  // Data payload
    pub n_data: u32,            // Data size
    pub n_zero: u32,            // Zero-padding bytes
}
```

## Key Macros (as functions)

### Cell Access
```rust
impl MemPage {
    /// Get offset to cell i from cell pointer array
    pub fn cell_offset(&self, i: u16) -> usize {
        let ptr_offset = self.cell_ptr_offset + (i as usize * 2);
        u16::from_be_bytes([self.data[ptr_offset], self.data[ptr_offset + 1]]) as usize
    }

    /// Get reference to cell content
    pub fn cell(&self, i: u16) -> &[u8] {
        let offset = self.cell_offset(i);
        &self.data[offset..]
    }

    /// Parse cell info
    pub fn parse_cell(&self, i: u16) -> CellInfo { ... }
}
```

### Varint Encoding
```rust
/// Read a varint from buffer, return (value, bytes_consumed)
pub fn get_varint(buf: &[u8]) -> (u64, usize) { ... }

/// Write a varint to buffer, return bytes written
pub fn put_varint(buf: &mut [u8], value: u64) -> usize { ... }

/// Get size of varint encoding
pub fn varint_len(value: u64) -> usize { ... }
```

### Page Calculations
```rust
impl BtShared {
    /// Max local payload for leaf cells
    pub fn max_local(&self, is_leaf: bool) -> u32 {
        if is_leaf {
            (self.usable_size - 35) * 64 / 255 - 23
        } else {
            (self.usable_size - 12) * 64 / 255 - 23
        }
    }

    /// Min local payload
    pub fn min_local(&self, is_leaf: bool) -> u32 {
        (self.usable_size - 12) * 32 / 255 - 23
    }

    /// Calculate overflow threshold
    pub fn overflow_threshold(&self, is_leaf: bool) -> u32 { ... }
}
```

## Internal Functions

### Page Initialization
```rust
/// Initialize a page after reading from disk
pub fn btree_init_page(page: &mut MemPage) -> Result<()>;

/// Zero a page (for new allocations)
pub fn zero_page(page: &mut MemPage, flags: u8);
```

### Defragmentation
```rust
/// Defragment a page to consolidate free space
pub fn defragment_page(page: &mut MemPage) -> Result<()>;

/// Allocate space on a page
pub fn allocate_space(page: &mut MemPage, size: u16) -> Result<u16>;

/// Free space on a page
pub fn free_space(page: &mut MemPage, offset: u16, size: u16) -> Result<()>;
```

## Acceptance Criteria
- [ ] All page type constants defined
- [ ] CursorState and BtLock enums defined
- [ ] CellInfo, IntegrityCk, BtreePayload structs defined
- [ ] Varint encoding/decoding functions working
- [ ] Cell access methods on MemPage
- [ ] Page calculation methods on BtShared
- [ ] All constants match SQLite values exactly
