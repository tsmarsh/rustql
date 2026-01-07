# Translate mem*.c - Memory Allocators

## Overview
Translate memory allocation subsystem including different allocation strategies and memory tracking.

## Source Reference
- `sqlite3/src/malloc.c` - Main memory interface (~800 lines)
- `sqlite3/src/mem1.c` - System malloc wrapper (~150 lines)
- `sqlite3/src/mem2.c` - Debug allocator with guards (~400 lines)
- `sqlite3/src/mem3.c` - Pool allocator from static buffer (~500 lines)
- `sqlite3/src/mem5.c` - Memsys5 power-of-two allocator (~600 lines)

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Memory Methods Trait
```rust
/// Memory allocator interface
pub trait MemMethods: Send + Sync {
    /// Allocate memory
    fn malloc(&self, size: usize) -> *mut u8;

    /// Free memory
    fn free(&self, ptr: *mut u8);

    /// Reallocate memory
    fn realloc(&self, ptr: *mut u8, new_size: usize) -> *mut u8;

    /// Get size of allocation
    fn size(&self, ptr: *mut u8) -> usize;

    /// Round up to allocation granularity
    fn roundup(&self, size: usize) -> usize;

    /// Initialize allocator
    fn init(&mut self) -> Result<()>;

    /// Shutdown allocator
    fn shutdown(&mut self);
}
```

### Memory Status
```rust
/// Memory subsystem status
pub struct MemStatus {
    /// Current memory in use
    pub current: AtomicI64,
    /// High-water mark
    pub high_water: AtomicI64,
    /// Total allocations
    pub alloc_count: AtomicI64,
    /// Current allocation count
    pub current_count: AtomicI64,
    /// Largest single allocation
    pub largest: AtomicI64,
}

impl MemStatus {
    pub fn new() -> Self {
        Self {
            current: AtomicI64::new(0),
            high_water: AtomicI64::new(0),
            alloc_count: AtomicI64::new(0),
            current_count: AtomicI64::new(0),
            largest: AtomicI64::new(0),
        }
    }

    pub fn record_alloc(&self, size: usize) {
        let size = size as i64;
        let current = self.current.fetch_add(size, Ordering::SeqCst) + size;
        self.alloc_count.fetch_add(1, Ordering::SeqCst);
        self.current_count.fetch_add(1, Ordering::SeqCst);

        // Update high water mark
        loop {
            let high = self.high_water.load(Ordering::SeqCst);
            if current <= high {
                break;
            }
            if self.high_water.compare_exchange(
                high, current, Ordering::SeqCst, Ordering::SeqCst
            ).is_ok() {
                break;
            }
        }

        // Update largest
        loop {
            let largest = self.largest.load(Ordering::SeqCst);
            if size <= largest {
                break;
            }
            if self.largest.compare_exchange(
                largest, size, Ordering::SeqCst, Ordering::SeqCst
            ).is_ok() {
                break;
            }
        }
    }

    pub fn record_free(&self, size: usize) {
        self.current.fetch_sub(size as i64, Ordering::SeqCst);
        self.current_count.fetch_sub(1, Ordering::SeqCst);
    }
}
```

## System Allocator (mem1)

```rust
/// Wrapper around system allocator
pub struct SystemAllocator;

impl MemMethods for SystemAllocator {
    fn malloc(&self, size: usize) -> *mut u8 {
        if size == 0 {
            return std::ptr::null_mut();
        }

        let layout = std::alloc::Layout::from_size_align(size, 8).unwrap();
        unsafe { std::alloc::alloc(layout) }
    }

    fn free(&self, ptr: *mut u8) {
        if ptr.is_null() {
            return;
        }

        // We need to know the size to deallocate
        // Store size in header or use sized deallocation
        unsafe {
            let size = self.size(ptr);
            let layout = std::alloc::Layout::from_size_align(size, 8).unwrap();
            std::alloc::dealloc(ptr, layout);
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

        let old_size = self.size(ptr);
        let old_layout = std::alloc::Layout::from_size_align(old_size, 8).unwrap();
        unsafe { std::alloc::realloc(ptr, old_layout, new_size) }
    }

    fn size(&self, ptr: *mut u8) -> usize {
        if ptr.is_null() {
            return 0;
        }
        // Use platform-specific size query
        #[cfg(unix)]
        unsafe {
            libc::malloc_usable_size(ptr as *mut libc::c_void)
        }
        #[cfg(windows)]
        unsafe {
            _msize(ptr as *mut libc::c_void)
        }
    }

    fn roundup(&self, size: usize) -> usize {
        (size + 7) & !7  // Round to 8-byte boundary
    }

    fn init(&mut self) -> Result<()> { Ok(()) }
    fn shutdown(&mut self) {}
}
```

## Debug Allocator (mem2)

```rust
/// Debug allocator with guard bytes and tracking
pub struct DebugAllocator {
    inner: SystemAllocator,
    /// Guard byte pattern
    guard_pattern: u8,
    /// Allocation map for leak detection
    allocations: Mutex<HashMap<usize, AllocationInfo>>,
}

struct AllocationInfo {
    size: usize,
    #[cfg(feature = "backtrace")]
    backtrace: backtrace::Backtrace,
}

const GUARD_SIZE: usize = 8;
const GUARD_BYTE: u8 = 0xBE;

impl DebugAllocator {
    pub fn new() -> Self {
        Self {
            inner: SystemAllocator,
            guard_pattern: GUARD_BYTE,
            allocations: Mutex::new(HashMap::new()),
        }
    }

    fn verify_guards(&self, ptr: *mut u8, size: usize) -> bool {
        unsafe {
            // Check leading guard
            let lead = std::slice::from_raw_parts(ptr.sub(GUARD_SIZE), GUARD_SIZE);
            for &b in lead {
                if b != self.guard_pattern {
                    return false;
                }
            }

            // Check trailing guard
            let trail = std::slice::from_raw_parts(ptr.add(size), GUARD_SIZE);
            for &b in trail {
                if b != self.guard_pattern {
                    return false;
                }
            }
        }
        true
    }
}

impl MemMethods for DebugAllocator {
    fn malloc(&self, size: usize) -> *mut u8 {
        if size == 0 {
            return std::ptr::null_mut();
        }

        // Allocate with space for guards
        let total = GUARD_SIZE + size + GUARD_SIZE;
        let base = self.inner.malloc(total);
        if base.is_null() {
            return std::ptr::null_mut();
        }

        unsafe {
            // Write leading guard
            std::ptr::write_bytes(base, self.guard_pattern, GUARD_SIZE);

            // User pointer
            let user_ptr = base.add(GUARD_SIZE);

            // Initialize user memory to 0x55 (detect uninitialized reads)
            std::ptr::write_bytes(user_ptr, 0x55, size);

            // Write trailing guard
            std::ptr::write_bytes(user_ptr.add(size), self.guard_pattern, GUARD_SIZE);

            // Track allocation
            let mut allocs = self.allocations.lock().unwrap();
            allocs.insert(user_ptr as usize, AllocationInfo {
                size,
                #[cfg(feature = "backtrace")]
                backtrace: backtrace::Backtrace::new(),
            });

            user_ptr
        }
    }

    fn free(&self, ptr: *mut u8) {
        if ptr.is_null() {
            return;
        }

        let mut allocs = self.allocations.lock().unwrap();
        let info = allocs.remove(&(ptr as usize))
            .expect("freeing unallocated memory");

        // Verify guards
        if !self.verify_guards(ptr, info.size) {
            panic!("memory corruption detected: guard bytes overwritten");
        }

        unsafe {
            // Poison freed memory
            std::ptr::write_bytes(ptr, 0xDD, info.size);

            // Free including guards
            let base = ptr.sub(GUARD_SIZE);
            self.inner.free(base);
        }
    }

    fn realloc(&self, ptr: *mut u8, new_size: usize) -> *mut u8 {
        if ptr.is_null() {
            return self.malloc(new_size);
        }

        let old_size = self.size(ptr);
        let new_ptr = self.malloc(new_size);

        if !new_ptr.is_null() {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    ptr,
                    new_ptr,
                    std::cmp::min(old_size, new_size)
                );
            }
            self.free(ptr);
        }

        new_ptr
    }

    fn size(&self, ptr: *mut u8) -> usize {
        if ptr.is_null() {
            return 0;
        }
        let allocs = self.allocations.lock().unwrap();
        allocs.get(&(ptr as usize)).map(|i| i.size).unwrap_or(0)
    }

    fn roundup(&self, size: usize) -> usize {
        self.inner.roundup(size)
    }

    fn init(&mut self) -> Result<()> { Ok(()) }

    fn shutdown(&mut self) {
        let allocs = self.allocations.lock().unwrap();
        if !allocs.is_empty() {
            eprintln!("Memory leak detected: {} allocations not freed", allocs.len());
            for (addr, info) in allocs.iter() {
                eprintln!("  Leak at {:p}, size {}", *addr as *const u8, info.size);
            }
        }
    }
}
```

## Pool Allocator (mem5)

```rust
/// Power-of-two pool allocator
pub struct PoolAllocator {
    /// Memory buffer
    buffer: Vec<u8>,
    /// Free lists by size class (power of 2)
    free_lists: [AtomicPtr<FreeBlock>; 32],
    /// Mutex for allocation
    mutex: Mutex<()>,
    /// Minimum allocation size
    min_size: usize,
}

struct FreeBlock {
    next: *mut FreeBlock,
}

impl PoolAllocator {
    pub fn new(buffer_size: usize, min_alloc: usize) -> Self {
        let buffer = vec![0u8; buffer_size];

        let mut alloc = Self {
            buffer,
            free_lists: Default::default(),
            mutex: Mutex::new(()),
            min_size: min_alloc.max(8),
        };

        // Initialize entire buffer as single free block
        alloc.init_free_list();
        alloc
    }

    fn size_class(&self, size: usize) -> usize {
        let size = size.max(self.min_size);
        (size.next_power_of_two().trailing_zeros() as usize)
            .saturating_sub(self.min_size.trailing_zeros() as usize)
    }

    fn class_size(&self, class: usize) -> usize {
        self.min_size << class
    }
}

impl MemMethods for PoolAllocator {
    fn malloc(&self, size: usize) -> *mut u8 {
        if size == 0 {
            return std::ptr::null_mut();
        }

        let _lock = self.mutex.lock().unwrap();
        let class = self.size_class(size);

        // Try to find block in this class or larger
        for c in class..self.free_lists.len() {
            let head = self.free_lists[c].load(Ordering::SeqCst);
            if !head.is_null() {
                unsafe {
                    let block = &*head;
                    self.free_lists[c].store(block.next, Ordering::SeqCst);

                    // Split larger blocks if needed
                    let block_size = self.class_size(c);
                    let needed_size = self.class_size(class);

                    let mut remaining = block_size;
                    let mut ptr = head as *mut u8;
                    ptr = ptr.add(needed_size);
                    remaining -= needed_size;

                    // Return excess to smaller free lists
                    while remaining >= self.min_size {
                        let excess_class = self.size_class(remaining);
                        let excess_size = self.class_size(excess_class);

                        let excess_block = ptr as *mut FreeBlock;
                        (*excess_block).next = self.free_lists[excess_class].load(Ordering::SeqCst);
                        self.free_lists[excess_class].store(excess_block, Ordering::SeqCst);

                        ptr = ptr.add(excess_size);
                        remaining -= excess_size;
                    }

                    return head as *mut u8;
                }
            }
        }

        std::ptr::null_mut()  // Out of memory
    }

    fn free(&self, ptr: *mut u8) {
        if ptr.is_null() {
            return;
        }

        let _lock = self.mutex.lock().unwrap();
        let size = self.size(ptr);
        let class = self.size_class(size);

        unsafe {
            let block = ptr as *mut FreeBlock;
            (*block).next = self.free_lists[class].load(Ordering::SeqCst);
            self.free_lists[class].store(block, Ordering::SeqCst);
        }
    }

    fn realloc(&self, ptr: *mut u8, new_size: usize) -> *mut u8 {
        let new_ptr = self.malloc(new_size);
        if !new_ptr.is_null() && !ptr.is_null() {
            let old_size = self.size(ptr);
            unsafe {
                std::ptr::copy_nonoverlapping(ptr, new_ptr, old_size.min(new_size));
            }
            self.free(ptr);
        }
        new_ptr
    }

    fn size(&self, _ptr: *mut u8) -> usize {
        // Track size separately or encode in block header
        0
    }

    fn roundup(&self, size: usize) -> usize {
        self.class_size(self.size_class(size))
    }

    fn init(&mut self) -> Result<()> { Ok(()) }
    fn shutdown(&mut self) {}
}
```

## Global Memory Functions

```rust
lazy_static! {
    static ref MEM_STATUS: MemStatus = MemStatus::new();
    static ref ALLOCATOR: Mutex<Box<dyn MemMethods>> =
        Mutex::new(Box::new(SystemAllocator));
}

pub fn sqlite3_malloc(size: usize) -> *mut u8 {
    let alloc = ALLOCATOR.lock().unwrap();
    let ptr = alloc.malloc(size);
    if !ptr.is_null() {
        MEM_STATUS.record_alloc(size);
    }
    ptr
}

pub fn sqlite3_free(ptr: *mut u8) {
    if ptr.is_null() {
        return;
    }
    let alloc = ALLOCATOR.lock().unwrap();
    let size = alloc.size(ptr);
    MEM_STATUS.record_free(size);
    alloc.free(ptr);
}

pub fn sqlite3_realloc(ptr: *mut u8, size: usize) -> *mut u8 {
    let alloc = ALLOCATOR.lock().unwrap();
    let old_size = if ptr.is_null() { 0 } else { alloc.size(ptr) };
    let new_ptr = alloc.realloc(ptr, size);

    if !new_ptr.is_null() {
        MEM_STATUS.record_free(old_size);
        MEM_STATUS.record_alloc(size);
    }

    new_ptr
}

pub fn sqlite3_memory_used() -> i64 {
    MEM_STATUS.current.load(Ordering::SeqCst)
}

pub fn sqlite3_memory_highwater(reset: bool) -> i64 {
    if reset {
        MEM_STATUS.high_water.swap(
            MEM_STATUS.current.load(Ordering::SeqCst),
            Ordering::SeqCst
        )
    } else {
        MEM_STATUS.high_water.load(Ordering::SeqCst)
    }
}
```

## Acceptance Criteria
- [ ] MemMethods trait interface
- [ ] System allocator wrapper (mem1)
- [ ] Debug allocator with guards (mem2)
- [ ] Pool allocator (mem5)
- [ ] Memory status tracking
- [ ] High-water mark
- [ ] Allocation counting
- [ ] sqlite3_malloc/free/realloc
- [ ] sqlite3_memory_used
- [ ] sqlite3_memory_highwater
- [ ] Soft heap limit support
- [ ] Thread-safe allocation
- [ ] Memory leak detection in debug mode

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `malloc.test` - Basic memory allocation
- `malloc3.test` - Memory allocation stress tests
- `malloc4.test` - Additional malloc tests
- `malloc5.test` - Malloc with soft heap limit
- `malloc6.test` - Memory subsystem tests
- `malloc7.test` - Additional malloc scenarios
- `malloc8.test` - Malloc failure recovery
- `malloc9.test` - Memory allocation edge cases
- `mallocA.test` - Malloc fault injection
- `mallocB.test` - More malloc fault tests
- `mallocC.test` - Malloc and prepared statements
- `mallocD.test` - Malloc with various operations
- `mallocE.test` - Malloc in expressions
- `mallocF.test` - Malloc with foreign keys
- `mallocG.test` - Malloc with triggers
- `mallocH.test` - Malloc with indices
- `mallocI.test` - Malloc with views
- `mallocJ.test` - Malloc with virtual tables
- `mallocK.test` - Malloc stress test
- `mem5.test` - Memsys5 pool allocator tests
