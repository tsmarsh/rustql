//! Debug Memory Allocator (mem2.c equivalent)
//!
//! This allocator adds guard bytes around allocations to detect buffer
//! overflows and tracks all allocations for leak detection.

#![allow(clippy::not_unsafe_ptr_arg_deref)]

use super::alloc::MemMethods;
use std::collections::HashMap;
use std::sync::Mutex;

/// Guard byte size (before and after user data)
const GUARD_SIZE: usize = 8;

/// Guard byte pattern (easily recognizable in hex dumps)
const GUARD_BYTE: u8 = 0xBE;

/// Uninitialized memory pattern
const UNINIT_BYTE: u8 = 0x55;

/// Freed memory pattern
const FREED_BYTE: u8 = 0xDD;

/// Information about a single allocation
struct AllocationInfo {
    /// Requested size
    size: usize,
    /// Base pointer (including guard bytes)
    base_ptr: *mut u8,
}

// SAFETY: AllocationInfo contains raw pointers, but we only access them
// while holding the allocations mutex
unsafe impl Send for AllocationInfo {}

/// Debug allocator with guard bytes and tracking
///
/// This allocator:
/// - Adds guard bytes before and after each allocation
/// - Fills new allocations with a pattern to detect uninitialized reads
/// - Fills freed memory with a pattern to detect use-after-free
/// - Tracks all allocations for leak detection
pub struct DebugAllocator {
    /// Track all allocations (user_ptr -> info)
    allocations: Mutex<HashMap<usize, AllocationInfo>>,
    /// Alignment (8 bytes)
    alignment: usize,
}

impl DebugAllocator {
    /// Create a new debug allocator
    pub fn new() -> Self {
        Self {
            allocations: Mutex::new(HashMap::new()),
            alignment: 8,
        }
    }

    /// Verify guard bytes are intact
    fn verify_guards(&self, ptr: *mut u8, size: usize) -> bool {
        unsafe {
            // Check leading guard
            let lead_guard = std::slice::from_raw_parts(ptr.sub(GUARD_SIZE), GUARD_SIZE);
            for &b in lead_guard {
                if b != GUARD_BYTE {
                    return false;
                }
            }

            // Check trailing guard
            let trail_guard = std::slice::from_raw_parts(ptr.add(size), GUARD_SIZE);
            for &b in trail_guard {
                if b != GUARD_BYTE {
                    return false;
                }
            }
        }
        true
    }

    /// Get the number of outstanding allocations
    pub fn outstanding_allocations(&self) -> usize {
        self.allocations.lock().unwrap().len()
    }

    /// Check for memory leaks and report them
    pub fn check_leaks(&self) -> Vec<LeakInfo> {
        let allocs = self.allocations.lock().unwrap();
        allocs
            .iter()
            .map(|(&addr, info)| LeakInfo {
                address: addr,
                size: info.size,
            })
            .collect()
    }

    /// Create layout for allocation including guards
    fn total_size(&self, size: usize) -> usize {
        GUARD_SIZE + size + GUARD_SIZE
    }
}

impl Default for DebugAllocator {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a leaked allocation
#[derive(Debug, Clone)]
pub struct LeakInfo {
    /// Address of the allocation
    pub address: usize,
    /// Size of the allocation
    pub size: usize,
}

impl MemMethods for DebugAllocator {
    fn malloc(&self, size: usize) -> *mut u8 {
        if size == 0 {
            return std::ptr::null_mut();
        }

        let total_size = self.total_size(size);

        unsafe {
            // Allocate with space for size header and guards
            let header_size = std::mem::size_of::<usize>();
            let layout =
                std::alloc::Layout::from_size_align(header_size + total_size, self.alignment)
                    .unwrap();

            let base = std::alloc::alloc(layout);
            if base.is_null() {
                return std::ptr::null_mut();
            }

            // Store total size in header
            *(base as *mut usize) = header_size + total_size;

            // Pointer to start of guards
            let guard_start = base.add(header_size);

            // Write leading guard bytes
            std::ptr::write_bytes(guard_start, GUARD_BYTE, GUARD_SIZE);

            // User pointer
            let user_ptr = guard_start.add(GUARD_SIZE);

            // Initialize user memory with pattern (to detect uninitialized reads)
            std::ptr::write_bytes(user_ptr, UNINIT_BYTE, size);

            // Write trailing guard bytes
            std::ptr::write_bytes(user_ptr.add(size), GUARD_BYTE, GUARD_SIZE);

            // Track allocation
            let mut allocs = self.allocations.lock().unwrap();
            allocs.insert(
                user_ptr as usize,
                AllocationInfo {
                    size,
                    base_ptr: base,
                },
            );

            user_ptr
        }
    }

    fn free(&self, ptr: *mut u8) {
        if ptr.is_null() {
            return;
        }

        let mut allocs = self.allocations.lock().unwrap();

        // Look up the allocation
        let info = match allocs.remove(&(ptr as usize)) {
            Some(info) => info,
            None => {
                // Freeing unallocated memory
                panic!("DebugAllocator: freeing unallocated memory at {:p}", ptr);
            }
        };

        // Verify guard bytes
        if !self.verify_guards(ptr, info.size) {
            panic!(
                "DebugAllocator: buffer overflow detected at {:p} (size {})",
                ptr, info.size
            );
        }

        unsafe {
            // Poison freed memory
            std::ptr::write_bytes(ptr, FREED_BYTE, info.size);

            // Free the entire allocation
            let total_size = *(info.base_ptr as *const usize);
            let layout = std::alloc::Layout::from_size_align(total_size, self.alignment).unwrap();
            std::alloc::dealloc(info.base_ptr, layout);
        }
    }

    fn realloc(&self, ptr: *mut u8, new_size: usize) -> *mut u8 {
        if ptr.is_null() {
            return self.malloc(new_size);
        }

        if new_size == 0 {
            self.free(ptr);
            return std::ptr::null_mut();
        }

        // Get old size
        let old_size = self.size(ptr);
        if old_size == 0 {
            // Unknown allocation - treat as malloc
            return self.malloc(new_size);
        }

        // Allocate new block
        let new_ptr = self.malloc(new_size);
        if new_ptr.is_null() {
            return std::ptr::null_mut();
        }

        // Copy data
        let copy_size = old_size.min(new_size);
        unsafe {
            std::ptr::copy_nonoverlapping(ptr, new_ptr, copy_size);
        }

        // Free old block
        self.free(ptr);

        new_ptr
    }

    fn size(&self, ptr: *mut u8) -> usize {
        if ptr.is_null() {
            return 0;
        }

        let allocs = self.allocations.lock().unwrap();
        allocs
            .get(&(ptr as usize))
            .map(|info| info.size)
            .unwrap_or(0)
    }

    fn roundup(&self, size: usize) -> usize {
        // Round to 8-byte boundary
        (size + 7) & !7
    }

    fn init(&self) -> crate::error::Result<()> {
        Ok(())
    }

    fn shutdown(&self) {
        let allocs = self.allocations.lock().unwrap();
        if !allocs.is_empty() {
            eprintln!("DebugAllocator: {} memory leak(s) detected", allocs.len());
            for (addr, info) in allocs.iter() {
                eprintln!("  Leak at 0x{:x}, size {} bytes", *addr, info.size);
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug_allocator_basic() {
        let alloc = DebugAllocator::new();

        let ptr = alloc.malloc(100);
        assert!(!ptr.is_null());
        assert_eq!(alloc.size(ptr), 100);
        assert_eq!(alloc.outstanding_allocations(), 1);

        // Verify memory is initialized with pattern
        unsafe {
            let slice = std::slice::from_raw_parts(ptr, 100);
            assert!(slice.iter().all(|&b| b == UNINIT_BYTE));
        }

        alloc.free(ptr);
        assert_eq!(alloc.outstanding_allocations(), 0);
    }

    #[test]
    fn test_debug_allocator_multiple() {
        let alloc = DebugAllocator::new();

        let ptr1 = alloc.malloc(100);
        let ptr2 = alloc.malloc(200);
        let ptr3 = alloc.malloc(50);

        assert_eq!(alloc.outstanding_allocations(), 3);

        alloc.free(ptr2);
        assert_eq!(alloc.outstanding_allocations(), 2);

        alloc.free(ptr1);
        alloc.free(ptr3);
        assert_eq!(alloc.outstanding_allocations(), 0);
    }

    #[test]
    fn test_debug_allocator_realloc() {
        let alloc = DebugAllocator::new();

        let ptr = alloc.malloc(100);
        unsafe {
            std::ptr::write_bytes(ptr, 0xAA, 100);
        }

        // Grow
        let ptr2 = alloc.realloc(ptr, 200);
        assert!(!ptr2.is_null());
        assert_eq!(alloc.size(ptr2), 200);

        // Verify data preserved
        unsafe {
            let slice = std::slice::from_raw_parts(ptr2, 100);
            assert!(slice.iter().all(|&b| b == 0xAA));
        }

        // Shrink
        let ptr3 = alloc.realloc(ptr2, 50);
        assert!(!ptr3.is_null());
        assert_eq!(alloc.size(ptr3), 50);

        unsafe {
            let slice = std::slice::from_raw_parts(ptr3, 50);
            assert!(slice.iter().all(|&b| b == 0xAA));
        }

        alloc.free(ptr3);
    }

    #[test]
    fn test_debug_allocator_check_leaks() {
        let alloc = DebugAllocator::new();

        let _ptr1 = alloc.malloc(100);
        let _ptr2 = alloc.malloc(200);

        let leaks = alloc.check_leaks();
        assert_eq!(leaks.len(), 2);

        // Sizes should match
        let sizes: Vec<_> = leaks.iter().map(|l| l.size).collect();
        assert!(sizes.contains(&100));
        assert!(sizes.contains(&200));
    }

    #[test]
    fn test_debug_allocator_zero() {
        let alloc = DebugAllocator::new();
        let ptr = alloc.malloc(0);
        assert!(ptr.is_null());
    }

    #[test]
    fn test_debug_allocator_free_null() {
        let alloc = DebugAllocator::new();
        alloc.free(std::ptr::null_mut()); // Should not panic
    }

    #[test]
    fn test_debug_allocator_guard_intact() {
        let alloc = DebugAllocator::new();

        let ptr = alloc.malloc(100);

        // Write within bounds - this should be fine
        unsafe {
            std::ptr::write_bytes(ptr, 0x42, 100);
        }

        // Free should succeed (guards intact)
        alloc.free(ptr);
    }

    #[test]
    #[should_panic(expected = "buffer overflow")]
    fn test_debug_allocator_overflow_detected() {
        let alloc = DebugAllocator::new();

        let ptr = alloc.malloc(100);

        // Write past the end (corrupt trailing guard)
        unsafe {
            std::ptr::write_bytes(ptr.add(100), 0x00, 1);
        }

        // Free should detect the corruption
        alloc.free(ptr);
    }

    #[test]
    #[should_panic(expected = "buffer overflow")]
    fn test_debug_allocator_underflow_detected() {
        let alloc = DebugAllocator::new();

        let ptr = alloc.malloc(100);

        // Write before the start (corrupt leading guard)
        unsafe {
            std::ptr::write_bytes(ptr.sub(1), 0x00, 1);
        }

        // Free should detect the corruption
        alloc.free(ptr);
    }

    #[test]
    #[should_panic(expected = "freeing unallocated")]
    fn test_debug_allocator_double_free() {
        let alloc = DebugAllocator::new();

        let ptr = alloc.malloc(100);
        alloc.free(ptr);
        alloc.free(ptr); // Should panic
    }

    #[test]
    fn test_debug_allocator_realloc_null() {
        let alloc = DebugAllocator::new();

        let ptr = alloc.realloc(std::ptr::null_mut(), 100);
        assert!(!ptr.is_null());
        assert_eq!(alloc.size(ptr), 100);

        alloc.free(ptr);
    }

    #[test]
    fn test_debug_allocator_realloc_zero() {
        let alloc = DebugAllocator::new();

        let ptr = alloc.malloc(100);
        let result = alloc.realloc(ptr, 0);
        assert!(result.is_null());
        assert_eq!(alloc.outstanding_allocations(), 0);
    }
}
