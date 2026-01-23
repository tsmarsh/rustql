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
// Global Sort Flag (for db status sort compatibility)
// ============================================================================

/// Global flag for tracking whether a sort operation was performed.
/// This is used by TCL's "db status sort" for test compatibility.
static SORT_FLAG: AtomicBool = AtomicBool::new(false);

/// Get whether a sort was performed in the most recent query
pub fn get_sort_flag() -> bool {
    SORT_FLAG.load(AtomicOrdering::Relaxed)
}

/// Reset the sort flag to false (call before executing a query)
pub fn reset_sort_flag() {
    SORT_FLAG.store(false, AtomicOrdering::Relaxed);
}

/// Set the sort flag to true (called when SorterSort executes)
pub(crate) fn set_sort_flag() {
    SORT_FLAG.store(true, AtomicOrdering::Relaxed);
}
