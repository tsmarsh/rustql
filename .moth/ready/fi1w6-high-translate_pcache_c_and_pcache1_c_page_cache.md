# Translate pcache.c and pcache1.c - Page Cache

## Overview
Translate the pluggable page cache system. pcache.c defines the interface and pcache1.c provides the default LRU implementation.

## Source Reference
- `sqlite3/src/pcache.c` - 667 lines (interface)
- `sqlite3/src/pcache1.c` - 1,233 lines (default implementation)
- `sqlite3/src/pcache.h` - 190 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### PCache (from pcache.c)
Page cache for a single pager:
```rust
pub struct PCache {
    dirty_head: Option<*mut PgHdr>,  // List of dirty pages
    dirty_tail: Option<*mut PgHdr>,  // End of dirty list
    synced: Option<*mut PgHdr>,      // Last synced page
    n_ref: i32,                       // Total page references
    cache_size: i32,                  // Suggested cache size
    spill_size: i32,                  // Size before spilling
    cache: Box<dyn PcacheImpl>,       // Underlying cache impl
    create_flag: bool,                // Create pages on miss
}
```

### PgHdr (Page Header)
Header prepended to each cached page:
```rust
pub struct PgHdr {
    pub page: *mut u8,           // Page data pointer
    pub extra: *mut c_void,      // Extra space for B-tree
    pub dirty: *mut PgHdr,       // Next in dirty list
    pub pager: *mut Pager,       // Owning pager
    pub pgno: Pgno,              // Page number
    pub flags: u16,              // PGHDR_* flags
    pub n_ref: i16,              // Reference count
    pub cache: *mut PCache,      // Cache that owns this page
    pub dirty_next: *mut PgHdr,  // Next dirty page
    pub dirty_prev: *mut PgHdr,  // Previous dirty page
}
```

### PcacheImpl Trait
Interface for cache implementations:
```rust
pub trait PcacheImpl: Send {
    /// Create a new cache instance
    fn create(page_size: i32, extra_size: i32, purgeable: bool) -> Self
    where Self: Sized;

    /// Set suggested cache size
    fn set_cache_size(&mut self, n_cache_size: i32);

    /// Get current page count
    fn page_count(&self) -> i32;

    /// Fetch a page (create if create_flag and not present)
    fn fetch(&mut self, pgno: Pgno, create_flag: bool) -> Option<*mut PgHdr>;

    /// Release a page reference
    fn unpin(&mut self, page: *mut PgHdr, discard: bool);

    /// Mark page as clean (remove from dirty list)
    fn make_clean(&mut self, page: *mut PgHdr);

    /// Mark page as dirty
    fn make_dirty(&mut self, page: *mut PgHdr);

    /// Truncate cache (remove pages >= pgno)
    fn truncate(&mut self, pgno: Pgno);

    /// Destroy cache
    fn destroy(&mut self);

    /// Shrink cache to target size
    fn shrink(&mut self);
}
```

### PCache1 (Default Implementation)
```rust
pub struct PCache1 {
    // Group this cache belongs to
    group: Arc<PGroup>,

    // Configuration
    page_size: i32,              // Page data size
    extra_size: i32,             // Extra bytes per page
    purgeable: bool,             // Can pages be recycled?

    // Limits
    n_min: u32,                  // Minimum pages to keep
    n_max: u32,                  // Maximum cache size
    n90pct: u32,                 // 90% of max for recycling

    // Current state
    n_page: u32,                 // Current page count
    n_hash: u32,                 // Hash table size
    hash: Vec<Option<*mut PgHdr1>>, // Hash table

    // LRU list
    lru_head: Option<*mut PgHdr1>,
    lru_tail: Option<*mut PgHdr1>,
}
```

### PGroup
Shared state for cache memory management:
```rust
pub struct PGroup {
    mutex: Mutex<()>,            // Group-wide mutex
    n_max_page: u32,             // Total max pages for group
    n_min_page: u32,             // Total min pages
    mxPinned: u32,               // Max pinned pages
    n_purgeable: u32,            // Current purgeable pages
    lru: Option<*mut PgHdr1>,    // Global LRU list
}
```

## Key Functions

### PCache Interface (pcache.c)
- `sqlite3PcacheOpen()` - Create cache for a pager
- `sqlite3PcacheClose()` - Destroy cache
- `sqlite3PcacheFetch()` - Get page, load from disk if needed
- `sqlite3PcacheFetchStress()` - Fetch with memory pressure
- `sqlite3PcacheRelease()` - Release page reference
- `sqlite3PcacheMakeDirty()` - Mark page dirty
- `sqlite3PcacheMakeClean()` - Mark page clean
- `sqlite3PcacheDirtyList()` - Get list of dirty pages
- `sqlite3PcacheCleanAll()` - Mark all pages clean
- `sqlite3PcacheTruncate()` - Remove pages >= pgno

### PCache1 Implementation (pcache1.c)
- `pcache1Create()` - Create new cache
- `pcache1Destroy()` - Destroy cache
- `pcache1Fetch()` - Fetch/create page
- `pcache1Unpin()` - Unpin page
- `pcache1Rekey()` - Change page number
- `pcache1Truncate()` - Remove pages by number
- `pcache1EnforceMaxPage()` - Evict to meet limit
- `pcache1ResizeHash()` - Resize hash table

### Memory Management
- `pcache1Alloc()` - Allocate page buffer
- `pcache1Free()` - Free page buffer
- `pcache1UnderMemoryPressure()` - Check memory state
- `pcache1TryRecycleAndFetch()` - Recycle LRU page

## Hash Table

Page lookup uses open-addressing hash:
```rust
impl PCache1 {
    fn hash_key(&self, pgno: Pgno) -> u32 {
        pgno % self.n_hash
    }

    fn lookup(&self, pgno: Pgno) -> Option<&PgHdr1> {
        let h = self.hash_key(pgno);
        let mut p = self.hash[h as usize];
        while let Some(page) = p {
            if page.pgno == pgno {
                return Some(page);
            }
            p = page.hash_next;
        }
        None
    }
}
```

## LRU Eviction

Pages are evicted LRU when cache is full:
```rust
impl PCache1 {
    fn evict_lru(&mut self) -> Option<*mut PgHdr1> {
        // Find unpinned page from LRU tail
        let mut p = self.lru_tail;
        while let Some(page) = p {
            if page.n_ref == 0 {
                self.remove_from_lru(page);
                return Some(page);
            }
            p = page.lru_prev;
        }
        None
    }
}
```

## Rust Translation Considerations

### Memory Safety
- Raw pointers needed for intrusive lists
- Consider `Box<>` with careful lifetime management
- Use `NonNull<>` where appropriate

### Thread Safety
- PGroup requires synchronization
- Individual caches may need per-cache locks
- Consider lock-free data structures

### Custom Allocator
- SQLite allows custom page allocators
- Use global allocator by default
- Support lookaside allocator option

## Page States

```
[Not in cache]
     |
     v (Fetch)
[Clean, Unpinned] <--+
     |               |
     v (Reference)   | (Unpin)
[Clean, Pinned] -----+
     |
     v (MakeDirty)
[Dirty, Pinned]
     |
     v (Unpin)
[Dirty, Unpinned]
     |
     v (MakeClean or Flush)
[Clean, Unpinned]
```

## Acceptance Criteria
- [ ] PCache struct with dirty list management
- [ ] PcacheImpl trait defined
- [ ] PCache1 with hash table and LRU list
- [ ] Page fetch with create option
- [ ] Pin/unpin with reference counting
- [ ] Dirty/clean state management
- [ ] LRU eviction when at capacity
- [ ] Truncate operation
- [ ] Memory pressure handling
