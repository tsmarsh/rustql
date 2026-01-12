//! Memory Allocation Subsystem
//!
//! This module provides SQLite's memory allocation interface, including:
//! - MemMethods trait for pluggable allocators
//! - System allocator wrapper (mem1)
//! - Debug allocator with guards (mem2)
//! - Pool allocator (mem5)
//! - Memory statistics tracking

// Allow public functions that dereference raw pointers without being marked unsafe.
// This is appropriate for memory allocation APIs that need to mimic the SQLite C API.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

pub mod alloc;
pub mod debug;
pub mod pool;
pub mod status;

pub use alloc::{MemMethods, SystemAllocator};
pub use debug::DebugAllocator;
pub use pool::PoolAllocator;
pub use status::MemStatus;

use crate::error::Result;
use std::sync::{Mutex, OnceLock};

// ============================================================================
// Global State
// ============================================================================

/// Global memory status tracker
static MEM_STATUS: OnceLock<MemStatus> = OnceLock::new();

/// Global allocator (defaults to system allocator)
static ALLOCATOR: OnceLock<Mutex<Box<dyn MemMethods>>> = OnceLock::new();

/// Soft heap limit (0 = disabled)
static SOFT_HEAP_LIMIT: OnceLock<Mutex<i64>> = OnceLock::new();

fn get_status() -> &'static MemStatus {
    MEM_STATUS.get_or_init(MemStatus::new)
}

fn get_allocator() -> &'static Mutex<Box<dyn MemMethods>> {
    ALLOCATOR.get_or_init(|| Mutex::new(Box::new(SystemAllocator::new())))
}

fn get_soft_heap_limit() -> &'static Mutex<i64> {
    SOFT_HEAP_LIMIT.get_or_init(|| Mutex::new(0))
}

// ============================================================================
// Public API - SQLite3 Compatible Functions
// ============================================================================

/// Allocate memory
///
/// Returns a pointer to at least `size` bytes of memory, or null on failure.
/// This is the Rust equivalent of sqlite3_malloc64().
pub fn sqlite3_malloc(size: usize) -> *mut u8 {
    if size == 0 {
        return std::ptr::null_mut();
    }

    // Check soft heap limit
    let limit = *get_soft_heap_limit().lock().unwrap();
    if limit > 0 {
        let current = get_status().current();
        if current + size as i64 > limit {
            // Try to release memory via sqlite3_release_memory (not implemented yet)
            // For now, just proceed and let allocation possibly fail
        }
    }

    let alloc = get_allocator().lock().unwrap();
    let ptr = alloc.malloc(size);

    if !ptr.is_null() {
        let actual_size = alloc.size(ptr);
        get_status().record_alloc(actual_size);
    }

    ptr
}

/// Free memory allocated by sqlite3_malloc or sqlite3_realloc
pub fn sqlite3_free(ptr: *mut u8) {
    if ptr.is_null() {
        return;
    }

    let alloc = get_allocator().lock().unwrap();
    let size = alloc.size(ptr);
    get_status().record_free(size);
    alloc.free(ptr);
}

/// Change the size of a memory allocation
///
/// If ptr is null, this is equivalent to sqlite3_malloc(size).
/// If size is 0, this is equivalent to sqlite3_free(ptr).
pub fn sqlite3_realloc(ptr: *mut u8, size: usize) -> *mut u8 {
    if ptr.is_null() {
        return sqlite3_malloc(size);
    }
    if size == 0 {
        sqlite3_free(ptr);
        return std::ptr::null_mut();
    }

    let alloc = get_allocator().lock().unwrap();
    let old_size = alloc.size(ptr);
    let new_ptr = alloc.realloc(ptr, size);

    if !new_ptr.is_null() {
        let new_size = alloc.size(new_ptr);
        get_status().record_free(old_size);
        get_status().record_alloc(new_size);
    }

    new_ptr
}

/// Return the size of a memory allocation
pub fn sqlite3_msize(ptr: *mut u8) -> usize {
    if ptr.is_null() {
        return 0;
    }
    let alloc = get_allocator().lock().unwrap();
    alloc.size(ptr)
}

/// Return current amount of memory in use
pub fn sqlite3_memory_used() -> i64 {
    get_status().current()
}

/// Return the maximum amount of memory that has been used
///
/// If `reset` is true, the high-water mark is reset to the current usage.
pub fn sqlite3_memory_highwater(reset: bool) -> i64 {
    get_status().highwater(reset)
}

/// Set or query the soft heap limit
///
/// The soft heap limit is advisory. When memory usage exceeds this limit,
/// SQLite will attempt to release memory before allocating more.
///
/// Returns the previous soft heap limit value.
pub fn sqlite3_soft_heap_limit64(n: i64) -> i64 {
    let mut limit = get_soft_heap_limit().lock().unwrap();
    let old = *limit;
    if n >= 0 {
        *limit = n;
    }
    old
}

/// Query status information about the memory subsystem
pub fn sqlite3_status(
    op: StatusOp,
    current: &mut i64,
    highwater: &mut i64,
    reset: bool,
) -> Result<()> {
    match op {
        StatusOp::MemoryUsed => {
            *current = get_status().current();
            *highwater = get_status().highwater(reset);
        }
        StatusOp::MallocSize => {
            *current = get_status().largest();
            *highwater = get_status().largest();
        }
        StatusOp::MallocCount => {
            *current = get_status().current_count();
            *highwater = get_status().alloc_count();
        }
        StatusOp::PagecacheUsed
        | StatusOp::PagecacheOverflow
        | StatusOp::ScratchUsed
        | StatusOp::ScratchOverflow
        | StatusOp::ParserStack
        | StatusOp::PagecacheSize
        | StatusOp::ScratchSize => {
            // These would be implemented when those subsystems exist
            *current = 0;
            *highwater = 0;
        }
    }
    Ok(())
}

/// Query status information about the memory subsystem (64-bit variant)
pub fn sqlite3_status64(
    op: StatusOp,
    current: &mut i64,
    highwater: &mut i64,
    reset: bool,
) -> Result<()> {
    sqlite3_status(op, current, highwater, reset)
}

/// Status operation codes for sqlite3_status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusOp {
    /// Total memory currently in use
    MemoryUsed = 0,
    /// Memory used by page cache
    PagecacheUsed = 1,
    /// Page cache overflow allocations
    PagecacheOverflow = 2,
    /// Memory used by scratch buffers
    ScratchUsed = 3,
    /// Scratch overflow allocations
    ScratchOverflow = 4,
    /// Largest allocation size
    MallocSize = 5,
    /// Parser stack depth
    ParserStack = 6,
    /// Page cache size
    PagecacheSize = 7,
    /// Scratch buffer size
    ScratchSize = 8,
    /// Number of outstanding allocations
    MallocCount = 9,
}

/// Configure the memory allocator
///
/// Must be called before any other memory functions.
pub fn sqlite3_config_mem(methods: Box<dyn MemMethods>) -> Result<()> {
    // Note: In a real implementation, we'd need to ensure this is only
    // called before any allocations are made
    let allocator = get_allocator();
    let mut alloc = allocator.lock().unwrap();
    *alloc = methods;
    Ok(())
}

// ============================================================================
// Safe Rust Wrappers
// ============================================================================

/// Allocate a Vec-like buffer using SQLite's allocator
///
/// This is useful for allocations that need to be tracked by SQLite's
/// memory subsystem.
pub fn alloc_vec(size: usize) -> Option<AllocVec> {
    let ptr = sqlite3_malloc(size);
    if ptr.is_null() {
        None
    } else {
        Some(AllocVec {
            ptr,
            len: 0,
            capacity: size,
        })
    }
}

/// A Vec-like buffer allocated via sqlite3_malloc
pub struct AllocVec {
    ptr: *mut u8,
    len: usize,
    capacity: usize,
}

impl AllocVec {
    /// Get the current length
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get the capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get a slice of the contents
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Get a mutable slice of the contents
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    /// Push a byte
    pub fn push(&mut self, byte: u8) -> bool {
        if self.len >= self.capacity && !self.grow() {
            return false;
        }
        unsafe {
            *self.ptr.add(self.len) = byte;
        }
        self.len += 1;
        true
    }

    /// Grow the buffer
    fn grow(&mut self) -> bool {
        let new_capacity = if self.capacity == 0 {
            8
        } else {
            self.capacity * 2
        };

        let new_ptr = sqlite3_realloc(self.ptr, new_capacity);
        if new_ptr.is_null() {
            return false;
        }

        self.ptr = new_ptr;
        self.capacity = new_capacity;
        true
    }

    /// Clear the contents
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Extend from a slice
    pub fn extend_from_slice(&mut self, data: &[u8]) -> bool {
        for &byte in data {
            if !self.push(byte) {
                return false;
            }
        }
        true
    }
}

impl Drop for AllocVec {
    fn drop(&mut self) {
        sqlite3_free(self.ptr);
    }
}

// AllocVec cannot be sent between threads safely due to raw pointer
// In a more complete implementation, we'd use proper synchronization

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_malloc_free() {
        let ptr = sqlite3_malloc(100);
        assert!(!ptr.is_null());

        // Write to the memory to verify it's accessible
        unsafe {
            std::ptr::write_bytes(ptr, 0x55, 100);
        }

        sqlite3_free(ptr);
    }

    #[test]
    fn test_malloc_zero() {
        let ptr = sqlite3_malloc(0);
        assert!(ptr.is_null());
    }

    #[test]
    fn test_realloc() {
        let ptr = sqlite3_malloc(100);
        assert!(!ptr.is_null());

        // Write a pattern
        unsafe {
            std::ptr::write_bytes(ptr, 0xAA, 100);
        }

        // Grow
        let ptr = sqlite3_realloc(ptr, 200);
        assert!(!ptr.is_null());

        // Verify original data preserved
        unsafe {
            let slice = std::slice::from_raw_parts(ptr, 100);
            assert!(slice.iter().all(|&b| b == 0xAA));
        }

        sqlite3_free(ptr);
    }

    #[test]
    fn test_realloc_null() {
        // realloc(null, n) should behave like malloc(n)
        let ptr = sqlite3_realloc(std::ptr::null_mut(), 100);
        assert!(!ptr.is_null());
        sqlite3_free(ptr);
    }

    #[test]
    fn test_realloc_zero() {
        // realloc(ptr, 0) should behave like free(ptr)
        let ptr = sqlite3_malloc(100);
        assert!(!ptr.is_null());
        let result = sqlite3_realloc(ptr, 0);
        assert!(result.is_null());
        // ptr has been freed, no need to free again
    }

    #[test]
    fn test_free_null() {
        // free(null) should be a no-op
        sqlite3_free(std::ptr::null_mut());
    }

    #[test]
    fn test_memory_tracking() {
        let initial = sqlite3_memory_used();

        let ptr = sqlite3_malloc(1024);
        assert!(!ptr.is_null());

        // Memory usage should have increased
        let after_alloc = sqlite3_memory_used();
        assert!(after_alloc >= initial + 1024);

        sqlite3_free(ptr);

        // Memory usage should return to approximately initial
        let after_free = sqlite3_memory_used();
        assert!(after_free <= after_alloc);
    }

    #[test]
    fn test_highwater() {
        let _initial_hw = sqlite3_memory_highwater(true); // Reset

        let ptr1 = sqlite3_malloc(1000);
        let ptr2 = sqlite3_malloc(2000);

        let hw1 = sqlite3_memory_highwater(false);
        assert!(hw1 >= 3000);

        sqlite3_free(ptr1);
        sqlite3_free(ptr2);

        // High water should still be >= 3000 even after freeing
        let hw2 = sqlite3_memory_highwater(false);
        assert!(hw2 >= 3000);

        // Reset should return current value
        let hw3 = sqlite3_memory_highwater(true);
        assert!(hw3 >= 3000);
    }

    #[test]
    fn test_soft_heap_limit() {
        let old = sqlite3_soft_heap_limit64(10000);

        // Query current limit
        let current = sqlite3_soft_heap_limit64(-1);
        assert_eq!(current, 10000);

        // Restore old limit
        sqlite3_soft_heap_limit64(old);
    }

    #[test]
    fn test_alloc_vec() {
        let mut vec = alloc_vec(10).unwrap();
        assert_eq!(vec.len(), 0);
        assert!(vec.capacity() >= 10);

        vec.push(1);
        vec.push(2);
        vec.push(3);

        assert_eq!(vec.len(), 3);
        assert_eq!(vec.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_alloc_vec_grow() {
        let mut vec = alloc_vec(4).unwrap();

        for i in 0..100 {
            assert!(vec.push(i as u8));
        }

        assert_eq!(vec.len(), 100);
        assert!(vec.capacity() >= 100);
    }
}
