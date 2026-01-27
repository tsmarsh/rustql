//! B-tree type definitions, constants, and bitflags

use std::sync::Arc;

use bitflags::bitflags;

use crate::error::{Error, ErrorCode, Result};
use crate::types::Pgno;

use super::encoding::{read_u16, read_u32};

// Page flags
pub const BTREE_PAGEFLAG_INTKEY: u8 = 0x01;
pub const BTREE_PAGEFLAG_ZERODATA: u8 = 0x02;
pub const BTREE_PAGEFLAG_LEAFDATA: u8 = 0x04;
pub const BTREE_PAGEFLAG_LEAF: u8 = 0x08;

pub const PTF_INTKEY: u8 = BTREE_PAGEFLAG_INTKEY;
pub const PTF_ZERODATA: u8 = BTREE_PAGEFLAG_ZERODATA;
pub const PTF_LEAFDATA: u8 = BTREE_PAGEFLAG_LEAFDATA;
pub const PTF_LEAF: u8 = BTREE_PAGEFLAG_LEAF;
pub const PTF_TABLE_LEAF: u8 = PTF_INTKEY | PTF_LEAFDATA | PTF_LEAF;
pub const PTF_TABLE_INTERIOR: u8 = PTF_INTKEY | PTF_LEAFDATA;
pub const PTF_INDEX_LEAF: u8 = PTF_LEAF | PTF_ZERODATA;
pub const PTF_INDEX_INTERIOR: u8 = PTF_ZERODATA;

// Page sizes
pub const PAGE_HEADER_SIZE_LEAF: usize = 8;
pub const PAGE_HEADER_SIZE_INTERIOR: usize = 12;
pub const MAX_EMBEDDED: u8 = 64;
pub const MIN_EMBEDDED: u8 = 32;
pub const CELL_PTR_SIZE: usize = 2;
pub const MAX_PAGE_SIZE: u32 = 65536;
pub const MIN_PAGE_SIZE: u32 = 512;
pub const DEFAULT_PAGE_SIZE: u32 = 4096;

// Auto-vacuum modes
pub const BTREE_AUTOVACUUM_NONE: u8 = 0;
pub const BTREE_AUTOVACUUM_FULL: u8 = 1;
pub const BTREE_AUTOVACUUM_INCR: u8 = 2;

// B-tree key types
pub const BTREE_INTKEY: u8 = 1;
pub const BTREE_BLOBKEY: u8 = 2;
pub const BTREE_HINT_RANGE: u8 = 0;

// B-tree metadata indices
pub const BTREE_FREE_PAGE_COUNT: usize = 0;
pub const BTREE_SCHEMA_VERSION: usize = 1;
pub const BTREE_FILE_FORMAT: usize = 2;
pub const BTREE_DEFAULT_CACHE_SIZE: usize = 3;
pub const BTREE_LARGEST_ROOT_PAGE: usize = 4;
pub const BTREE_TEXT_ENCODING: usize = 5;
pub const BTREE_USER_VERSION: usize = 6;
pub const BTREE_INCR_VACUUM: usize = 7;
pub const BTREE_APPLICATION_ID: usize = 8;
pub const BTREE_DATA_VERSION: usize = 15;
pub const SQLITE_N_BTREE_META: usize = 16;
pub const BTCURSOR_MAX_DEPTH: usize = 20;
pub const BT_MAX_LOCAL: u16 = 65501;

// Pointer map types
pub const PTRMAP_ROOTPAGE: u8 = 1;
pub const PTRMAP_FREEPAGE: u8 = 2;
pub const PTRMAP_OVERFLOW1: u8 = 3;
pub const PTRMAP_OVERFLOW2: u8 = 4;
pub const PTRMAP_BTREE: u8 = 5;

pub const SQLITE_FILE_HEADER: &[u8; 16] = b"SQLite format 3\0";

/// Sort order flags for KeyInfo columns
pub const KEYINFO_ORDER_DESC: u8 = 0x01;
pub const KEYINFO_ORDER_NULLS_FIRST: u8 = 0x02;

bitflags! {
    #[derive(Clone, Copy)]
    pub struct BtreeOpenFlags: u8 {
        const OMIT_JOURNAL = 0x01;
        const MEMORY = 0x02;
        const SINGLE = 0x04;
        const UNORDERED = 0x08;
    }
}

bitflags! {
    pub struct BtsFlags: u16 {
        const READ_ONLY = 0x0001;
        const PAGESIZE_FIXED = 0x0002;
        const SECURE_DELETE = 0x0004;
        const OVERWRITE = 0x0008;
        const FAST_SECURE = 0x000c;
        const INITIALLY_EMPTY = 0x0010;
        const NO_WAL = 0x0020;
        const EXCLUSIVE = 0x0040;
        const PENDING = 0x0080;
    }
}

bitflags! {
    pub struct BtreeCursorFlags: u32 {
        const BULKLOAD = 0x0000_0001;
        const SEEK_EQ = 0x0000_0002;
        const WRCSR = 0x0000_0004;
        const FORDELETE = 0x0000_0008;
    }
}

bitflags! {
    pub struct BtreeInsertFlags: u8 {
        const SAVEPOSITION = 0x02;
        const AUXDELETE = 0x04;
        const APPEND = 0x08;
        /// Use seek_result parameter to skip internal seek - cursor is already positioned
        const USESEEKRESULT = 0x10;
        const PREFORMAT = 0x80;
    }
}

bitflags! {
    pub struct BtCursorFlags: u8 {
        const WRITE = 0x01;
        const VALID_NKEY = 0x02;
        const VALID_OVFL = 0x04;
        const AT_LAST = 0x08;
        const INCRBLOB = 0x10;
        const MULTIPLE = 0x20;
        const PINNED = 0x40;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum TransState {
    None = 0,
    Read = 1,
    Write = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum BtLock {
    Read = 1,
    Write = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum CursorState {
    Valid = 0,
    Invalid = 1,
    SkipNext = 2,
    RequireSeek = 3,
    Fault = 4,
}

bitflags! {
    pub struct CursorHints: u8 {
        const NONE = 0;
        const BULKLOAD = 0x01;
        const SEEK_EQ = 0x02;
    }
}

/// Collation sequence type for string comparison
#[derive(Clone)]
pub enum CollSeq {
    /// Binary comparison (memcmp, default)
    Binary,
    /// Case-insensitive comparison for ASCII
    NoCase,
    /// Ignore trailing spaces
    RTrim,
    /// Custom collation with name and comparison function
    Custom {
        name: String,
        cmp: Arc<dyn Fn(&str, &str) -> std::cmp::Ordering + Send + Sync>,
    },
}

impl CollSeq {
    /// Compare two strings using this collation
    pub fn compare(&self, a: &str, b: &str) -> std::cmp::Ordering {
        match self {
            CollSeq::Binary => a.cmp(b),
            CollSeq::NoCase => a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase()),
            CollSeq::RTrim => a.trim_end().cmp(b.trim_end()),
            CollSeq::Custom { cmp, .. } => cmp(a, b),
        }
    }

    /// Get the name of this collation
    pub fn name(&self) -> &str {
        match self {
            CollSeq::Binary => "BINARY",
            CollSeq::NoCase => "NOCASE",
            CollSeq::RTrim => "RTRIM",
            CollSeq::Custom { name, .. } => name,
        }
    }
}

impl std::fmt::Debug for CollSeq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CollSeq({})", self.name())
    }
}

impl Default for CollSeq {
    fn default() -> Self {
        CollSeq::Binary
    }
}

/// Represents a parsed field value from a SQLite record
#[derive(Clone, Debug, PartialEq)]
pub enum RecordField {
    Null,
    Int(i64),
    Float(f64),
    Blob(Vec<u8>),
    Text(String),
}

/// Page limits and offset calculations
#[derive(Clone, Copy, Debug)]
pub struct PageLimits {
    pub page_size: u32,
    pub usable_size: u32,
    pub header_offset: usize,
}

impl PageLimits {
    pub fn new(page_size: u32, usable_size: u32) -> Self {
        Self {
            page_size,
            usable_size,
            header_offset: 0,
        }
    }

    pub fn for_page1(page_size: u32, usable_size: u32) -> Self {
        Self {
            page_size,
            usable_size,
            header_offset: 100,
        }
    }

    pub fn header_start(&self) -> usize {
        self.header_offset
    }

    pub fn usable_end(&self) -> usize {
        self.usable_size as usize
    }

    pub fn max_local(&self, is_leaf: bool) -> u32 {
        let usable = self.usable_size;
        if is_leaf {
            (usable
                .saturating_sub(35)
                .saturating_mul(MAX_EMBEDDED as u32)
                / 255)
                .saturating_sub(23)
        } else {
            (usable
                .saturating_sub(12)
                .saturating_mul(MAX_EMBEDDED as u32)
                / 255)
                .saturating_sub(23)
        }
    }

    pub fn min_local(&self) -> u32 {
        (self
            .usable_size
            .saturating_sub(12)
            .saturating_mul(MIN_EMBEDDED as u32)
            / 255)
            .saturating_sub(23)
    }
}

/// B-tree table lock entry for shared cache mode
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BtTableLockEntry {
    pub(crate) table: i32,
    pub(crate) btree_id: usize,
    pub(crate) lock_type: BtLock,
}

/// Database header structure
pub struct DbHeader {
    pub page_size: u32,
    pub reserve: u8,
    pub file_format: u8,
    pub schema_cookie: u32,
    pub auto_vacuum: u8,
    pub incr_vacuum: u8,
    pub first_trunk_page: Pgno,
    pub free_page_count: u32,
}

impl DbHeader {
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < 100 {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        let mut page_size = read_u16(data, 16).ok_or(Error::new(ErrorCode::Corrupt))? as u32;
        if page_size == 1 {
            page_size = 65536;
        }
        if !(512..=65536).contains(&page_size) || !page_size.is_power_of_two() {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        let reserve = data[20];
        let file_format = data[18];
        // Offset 32-35: First freelist trunk page
        let first_trunk_page = read_u32(data, 32).unwrap_or(0);
        // Offset 36-39: Total number of freelist pages
        let free_page_count = read_u32(data, 36).unwrap_or(0);
        let schema_cookie = read_u32(data, 40).ok_or(Error::new(ErrorCode::Corrupt))?;
        let auto_vacuum = if read_u32(data, 52).unwrap_or(0) != 0 {
            1
        } else {
            0
        };
        let incr_vacuum = if read_u32(data, 64).unwrap_or(0) != 0 {
            1
        } else {
            0
        };
        Ok(Self {
            page_size,
            reserve,
            file_format,
            schema_cookie,
            auto_vacuum,
            incr_vacuum,
            first_trunk_page,
            free_page_count,
        })
    }
}
