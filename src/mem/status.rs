//! Memory Status Tracking
//!
//! Provides atomic counters for tracking memory usage, allocation counts,
//! high-water marks, and other memory statistics.

use std::sync::atomic::{AtomicI64, Ordering};

/// Memory subsystem status
///
/// All fields are atomic for thread-safe access without locks.
pub struct MemStatus {
    /// Current memory in use (bytes)
    current: AtomicI64,
    /// High-water mark (bytes)
    high_water: AtomicI64,
    /// Total number of allocations ever made
    alloc_count: AtomicI64,
    /// Current number of outstanding allocations
    current_count: AtomicI64,
    /// Largest single allocation ever made (bytes)
    largest: AtomicI64,
}

impl MemStatus {
    /// Create a new memory status tracker
    pub const fn new() -> Self {
        Self {
            current: AtomicI64::new(0),
            high_water: AtomicI64::new(0),
            alloc_count: AtomicI64::new(0),
            current_count: AtomicI64::new(0),
            largest: AtomicI64::new(0),
        }
    }

    /// Record a new allocation
    pub fn record_alloc(&self, size: usize) {
        let size = size as i64;

        // Update current usage
        let new_current = self.current.fetch_add(size, Ordering::SeqCst) + size;

        // Increment allocation counts
        self.alloc_count.fetch_add(1, Ordering::SeqCst);
        self.current_count.fetch_add(1, Ordering::SeqCst);

        // Update high-water mark using CAS loop
        loop {
            let high = self.high_water.load(Ordering::SeqCst);
            if new_current <= high {
                break;
            }
            match self.high_water.compare_exchange(
                high,
                new_current,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(_) => continue,
            }
        }

        // Update largest allocation using CAS loop
        loop {
            let largest = self.largest.load(Ordering::SeqCst);
            if size <= largest {
                break;
            }
            match self
                .largest
                .compare_exchange(largest, size, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(_) => continue,
            }
        }
    }

    /// Record a freed allocation
    pub fn record_free(&self, size: usize) {
        self.current.fetch_sub(size as i64, Ordering::SeqCst);
        self.current_count.fetch_sub(1, Ordering::SeqCst);
    }

    /// Get current memory usage
    pub fn current(&self) -> i64 {
        self.current.load(Ordering::SeqCst)
    }

    /// Get high-water mark, optionally resetting it
    pub fn highwater(&self, reset: bool) -> i64 {
        if reset {
            let current = self.current.load(Ordering::SeqCst);
            self.high_water.swap(current, Ordering::SeqCst)
        } else {
            self.high_water.load(Ordering::SeqCst)
        }
    }

    /// Get total number of allocations ever made
    pub fn alloc_count(&self) -> i64 {
        self.alloc_count.load(Ordering::SeqCst)
    }

    /// Get current number of outstanding allocations
    pub fn current_count(&self) -> i64 {
        self.current_count.load(Ordering::SeqCst)
    }

    /// Get largest single allocation
    pub fn largest(&self) -> i64 {
        self.largest.load(Ordering::SeqCst)
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.current.store(0, Ordering::SeqCst);
        self.high_water.store(0, Ordering::SeqCst);
        self.alloc_count.store(0, Ordering::SeqCst);
        self.current_count.store(0, Ordering::SeqCst);
        self.largest.store(0, Ordering::SeqCst);
    }
}

impl Default for MemStatus {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_status_new() {
        let status = MemStatus::new();
        assert_eq!(status.current(), 0);
        assert_eq!(status.highwater(false), 0);
        assert_eq!(status.alloc_count(), 0);
        assert_eq!(status.current_count(), 0);
        assert_eq!(status.largest(), 0);
    }

    #[test]
    fn test_mem_status_alloc_free() {
        let status = MemStatus::new();

        status.record_alloc(100);
        assert_eq!(status.current(), 100);
        assert_eq!(status.alloc_count(), 1);
        assert_eq!(status.current_count(), 1);

        status.record_alloc(200);
        assert_eq!(status.current(), 300);
        assert_eq!(status.alloc_count(), 2);
        assert_eq!(status.current_count(), 2);

        status.record_free(100);
        assert_eq!(status.current(), 200);
        assert_eq!(status.alloc_count(), 2); // Total stays same
        assert_eq!(status.current_count(), 1);
    }

    #[test]
    fn test_mem_status_highwater() {
        let status = MemStatus::new();

        status.record_alloc(100);
        status.record_alloc(200);
        assert_eq!(status.highwater(false), 300);

        status.record_free(200);
        // High water should still be 300
        assert_eq!(status.highwater(false), 300);

        // Reset highwater
        let old_hw = status.highwater(true);
        assert_eq!(old_hw, 300);

        // New highwater should be current usage
        assert_eq!(status.highwater(false), 100);
    }

    #[test]
    fn test_mem_status_largest() {
        let status = MemStatus::new();

        status.record_alloc(50);
        assert_eq!(status.largest(), 50);

        status.record_alloc(100);
        assert_eq!(status.largest(), 100);

        status.record_alloc(75);
        assert_eq!(status.largest(), 100); // Stays at max

        status.record_free(100);
        assert_eq!(status.largest(), 100); // Still 100
    }

    #[test]
    fn test_mem_status_reset() {
        let status = MemStatus::new();

        status.record_alloc(100);
        status.record_alloc(200);
        status.record_free(100);

        status.reset();

        assert_eq!(status.current(), 0);
        assert_eq!(status.highwater(false), 0);
        assert_eq!(status.alloc_count(), 0);
        assert_eq!(status.current_count(), 0);
        assert_eq!(status.largest(), 0);
    }

    #[test]
    fn test_mem_status_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let status = Arc::new(MemStatus::new());
        let mut handles = vec![];

        // Spawn threads that allocate
        for _ in 0..10 {
            let status = Arc::clone(&status);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    status.record_alloc(10);
                }
            }));
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Should have 10 * 100 = 1000 allocations
        assert_eq!(status.alloc_count(), 1000);
        assert_eq!(status.current_count(), 1000);
        assert_eq!(status.current(), 10000);

        // Now free some in parallel
        let mut handles = vec![];
        for _ in 0..10 {
            let status = Arc::clone(&status);
            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    status.record_free(10);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have freed 500
        assert_eq!(status.current_count(), 500);
        assert_eq!(status.current(), 5000);
    }
}
