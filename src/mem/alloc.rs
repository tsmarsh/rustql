//! Memory Allocator Interface and System Allocator
//!
//! This module provides:
//! - MemMethods trait for pluggable memory allocators
//! - SystemAllocator - wrapper around Rust's global allocator (equivalent to mem1.c)

#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::alloc::{GlobalAlloc, Layout, System};

// ============================================================================
// Memory Allocator Trait
// ============================================================================

/// Memory allocator interface (sqlite3_mem_methods equivalent)
///
/// This trait allows SQLite to use different memory allocation strategies.
/// All methods must be thread-safe.
pub trait MemMethods: Send + Sync {
    /// Allocate at least `size` bytes of memory
    ///
    /// Returns a pointer to the allocated memory, or null on failure.
    /// The pointer must be aligned to at least 8 bytes.
    fn malloc(&self, size: usize) -> *mut u8;

    /// Free memory previously allocated by malloc or realloc
    ///
    /// If ptr is null, this is a no-op.
    fn free(&self, ptr: *mut u8);

    /// Change the size of a memory allocation
    ///
    /// Returns a pointer to the reallocated memory, or null on failure.
    /// If null is returned, the original memory is not freed.
    fn realloc(&self, ptr: *mut u8, new_size: usize) -> *mut u8;

    /// Get the size of an allocation
    ///
    /// Returns the size of the memory block pointed to by ptr.
    /// If ptr is null, returns 0.
    fn size(&self, ptr: *mut u8) -> usize;

    /// Round up a size to the allocator's granularity
    ///
    /// This is used to predict the actual size of an allocation.
    fn roundup(&self, size: usize) -> usize;

    /// Initialize the allocator
    ///
    /// Called before the first allocation.
    fn init(&self) -> crate::error::Result<()>;

    /// Shutdown the allocator
    ///
    /// Called when the memory subsystem is shut down.
    fn shutdown(&self);
}

// ============================================================================
// System Allocator (mem1.c equivalent)
// ============================================================================

/// System allocator - wrapper around Rust's global allocator
///
/// This is equivalent to SQLite's mem1.c, which uses the system's
/// malloc/free/realloc functions.
pub struct SystemAllocator {
    /// Allocation alignment (8 bytes for compatibility)
    alignment: usize,
}

impl SystemAllocator {
    /// Create a new system allocator
    pub fn new() -> Self {
        Self { alignment: 8 }
    }

    /// Create a layout for a given size
    fn layout(&self, size: usize) -> Layout {
        // We need to track the size with each allocation since Rust's
        // dealloc requires the layout. We store the size in a header.
        let header_size = std::mem::size_of::<usize>();
        let total_size = header_size + size;
        Layout::from_size_align(total_size, self.alignment).unwrap()
    }

    /// Get pointer to user data from base pointer
    unsafe fn user_ptr(&self, base: *mut u8) -> *mut u8 {
        base.add(std::mem::size_of::<usize>())
    }

    /// Get base pointer from user pointer
    unsafe fn base_ptr(&self, user: *mut u8) -> *mut u8 {
        user.sub(std::mem::size_of::<usize>())
    }
}

impl Default for SystemAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl MemMethods for SystemAllocator {
    fn malloc(&self, size: usize) -> *mut u8 {
        if size == 0 {
            return std::ptr::null_mut();
        }

        let layout = self.layout(size);

        unsafe {
            let base = System.alloc(layout);
            if base.is_null() {
                return std::ptr::null_mut();
            }

            // Store size in header
            *(base as *mut usize) = size;

            self.user_ptr(base)
        }
    }

    fn free(&self, ptr: *mut u8) {
        if ptr.is_null() {
            return;
        }

        unsafe {
            let base = self.base_ptr(ptr);
            let size = *(base as *const usize);
            let layout = self.layout(size);
            System.dealloc(base, layout);
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

        unsafe {
            let base = self.base_ptr(ptr);
            let old_size = *(base as *const usize);
            let old_layout = self.layout(old_size);
            let new_layout = self.layout(new_size);

            let new_base = System.realloc(base, old_layout, new_layout.size());
            if new_base.is_null() {
                return std::ptr::null_mut();
            }

            // Update size in header
            *(new_base as *mut usize) = new_size;

            self.user_ptr(new_base)
        }
    }

    fn size(&self, ptr: *mut u8) -> usize {
        if ptr.is_null() {
            return 0;
        }

        unsafe {
            let base = self.base_ptr(ptr);
            *(base as *const usize)
        }
    }

    fn roundup(&self, size: usize) -> usize {
        // Round to 8-byte boundary
        (size + 7) & !7
    }

    fn init(&self) -> crate::error::Result<()> {
        // Nothing to initialize for system allocator
        Ok(())
    }

    fn shutdown(&self) {
        // Nothing to clean up for system allocator
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_allocator_basic() {
        let alloc = SystemAllocator::new();

        let ptr = alloc.malloc(100);
        assert!(!ptr.is_null());

        // Verify we can write to the memory
        unsafe {
            std::ptr::write_bytes(ptr, 0x55, 100);
        }

        assert_eq!(alloc.size(ptr), 100);

        alloc.free(ptr);
    }

    #[test]
    fn test_system_allocator_zero() {
        let alloc = SystemAllocator::new();
        let ptr = alloc.malloc(0);
        assert!(ptr.is_null());
    }

    #[test]
    fn test_system_allocator_realloc() {
        let alloc = SystemAllocator::new();

        let ptr = alloc.malloc(100);
        assert!(!ptr.is_null());

        // Write pattern
        unsafe {
            std::ptr::write_bytes(ptr, 0xAA, 100);
        }

        // Grow
        let ptr2 = alloc.realloc(ptr, 200);
        assert!(!ptr2.is_null());
        assert_eq!(alloc.size(ptr2), 200);

        // Verify pattern preserved
        unsafe {
            let slice = std::slice::from_raw_parts(ptr2, 100);
            assert!(slice.iter().all(|&b| b == 0xAA));
        }

        // Shrink
        let ptr3 = alloc.realloc(ptr2, 50);
        assert!(!ptr3.is_null());
        assert_eq!(alloc.size(ptr3), 50);

        // Verify pattern preserved
        unsafe {
            let slice = std::slice::from_raw_parts(ptr3, 50);
            assert!(slice.iter().all(|&b| b == 0xAA));
        }

        alloc.free(ptr3);
    }

    #[test]
    fn test_system_allocator_realloc_null() {
        let alloc = SystemAllocator::new();

        // realloc(null, n) should work like malloc
        let ptr = alloc.realloc(std::ptr::null_mut(), 100);
        assert!(!ptr.is_null());
        assert_eq!(alloc.size(ptr), 100);

        alloc.free(ptr);
    }

    #[test]
    fn test_system_allocator_realloc_zero() {
        let alloc = SystemAllocator::new();

        let ptr = alloc.malloc(100);
        assert!(!ptr.is_null());

        // realloc(ptr, 0) should free
        let result = alloc.realloc(ptr, 0);
        assert!(result.is_null());
    }

    #[test]
    fn test_system_allocator_free_null() {
        let alloc = SystemAllocator::new();
        alloc.free(std::ptr::null_mut()); // Should not panic
    }

    #[test]
    fn test_system_allocator_size_null() {
        let alloc = SystemAllocator::new();
        assert_eq!(alloc.size(std::ptr::null_mut()), 0);
    }

    #[test]
    fn test_system_allocator_roundup() {
        let alloc = SystemAllocator::new();

        assert_eq!(alloc.roundup(1), 8);
        assert_eq!(alloc.roundup(8), 8);
        assert_eq!(alloc.roundup(9), 16);
        assert_eq!(alloc.roundup(15), 16);
        assert_eq!(alloc.roundup(16), 16);
    }

    #[test]
    fn test_system_allocator_alignment() {
        let alloc = SystemAllocator::new();

        for _ in 0..100 {
            let ptr = alloc.malloc(17); // Odd size
            assert!(!ptr.is_null());

            // Check alignment (8-byte aligned)
            assert_eq!((ptr as usize) % 8, 0);

            alloc.free(ptr);
        }
    }

    #[test]
    fn test_system_allocator_large() {
        let alloc = SystemAllocator::new();

        // Allocate 1MB
        let ptr = alloc.malloc(1024 * 1024);
        assert!(!ptr.is_null());

        // Write to verify
        unsafe {
            std::ptr::write_bytes(ptr, 0x42, 1024 * 1024);
        }

        alloc.free(ptr);
    }
}
