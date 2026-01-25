//! Global state and constants for the VDBE engine

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};

// ============================================================================
// Magic numbers for VDBE state validation
// ============================================================================

/// Magic number for valid VDBE state
pub const VDBE_MAGIC_INIT: u32 = 0x26bceaa5;
pub const VDBE_MAGIC_RUN: u32 = 0xbdf20da3;
pub const VDBE_MAGIC_HALT: u32 = 0x519c2973;
pub const VDBE_MAGIC_DEAD: u32 = 0xb606c3c8;

/// Default number of memory cells
pub const DEFAULT_MEM_SIZE: usize = 128;

/// Default number of cursor slots
pub const DEFAULT_CURSOR_SLOTS: usize = 16;

// OPFLAG constants (from SQLite's vdbe.h)
pub const OPFLAG_NCHANGE: u16 = 0x01;
pub const OPFLAG_LASTROWID: u16 = 0x20;
pub const OPFLAG_ISUPDATE: u16 = 0x04;
pub const OPFLAG_APPEND: u16 = 0x08;

// Conflict resolution modes (from SQLite's sqlite.h - OE_* constants)
// These are encoded in bits 0-4 of P5 for Insert/Update/Delete
pub const OE_NONE: u8 = 0;
pub const OE_ROLLBACK: u8 = 1;
pub const OE_ABORT: u8 = 2; // Default
pub const OE_FAIL: u8 = 3;
pub const OE_IGNORE: u8 = 4;
pub const OE_REPLACE: u8 = 5;

// Mask to extract conflict resolution from P5
pub const OE_MASK: u8 = 0x1F;

// ============================================================================
// Global Search Counter (for sqlite_search_count compatibility)
// ============================================================================

/// Global counter for tracking VDBE search operations (SeekGE, SeekGT, etc.)
/// This is used by sqlite_search_count() for test compatibility.
static SEARCH_COUNT: AtomicU64 = AtomicU64::new(0);

/// Get the current search count (for sqlite_search_count() function)
pub fn get_search_count() -> u64 {
    SEARCH_COUNT.load(AtomicOrdering::Relaxed)
}

/// Reset the search count to zero
pub fn reset_search_count() {
    SEARCH_COUNT.store(0, AtomicOrdering::Relaxed);
}

/// Increment the search count
pub(crate) fn inc_search_count() {
    SEARCH_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
}

// ============================================================================
// Global Sort Counter (for sqlite_sort_count compatibility)
// ============================================================================

/// Global counter for tracking sort operations (Sort, SorterSort opcodes).
/// This is used by sqlite_sort_count() for test compatibility.
static SORT_COUNT: AtomicU64 = AtomicU64::new(0);

/// Get the current sort count (for sqlite_sort_count variable)
pub fn get_sort_count() -> u64 {
    SORT_COUNT.load(AtomicOrdering::Relaxed)
}

/// Reset the sort count to zero
pub fn reset_sort_count() {
    SORT_COUNT.store(0, AtomicOrdering::Relaxed);
}

/// Increment the sort count (called when Sort/SorterSort executes)
pub(crate) fn inc_sort_count() {
    SORT_COUNT.fetch_add(1, AtomicOrdering::Relaxed);
}

/// Get whether a sort was performed (for db status sort compatibility)
pub fn get_sort_flag() -> bool {
    SORT_COUNT.load(AtomicOrdering::Relaxed) > 0
}
