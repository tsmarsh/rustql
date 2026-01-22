# Enforce cache_size limits and cache spill semantics in pcache

## Problem
`PCache1` does not enforce the configured cache size. When `n_page >= n_max`, it attempts one LRU eviction but still allocates a new page even if no pages are evictable (all pinned). This allows unbounded cache growth and ignores SQLite’s `cache_spill` behavior.

Code refs:
- `src/storage/pcache.rs:267` (evict_lru)
- `src/storage/pcache.rs:309` (fetch allocates even if eviction fails)

## SQLite Behavior
SQLite’s pcache respects the page-cache limit. If the cache is full and no pages can be evicted, the pager will attempt to spill dirty pages; if it cannot, the operation fails with `SQLITE_FULL`/`SQLITE_NOMEM` depending on context.

## Expected Fix
- Enforce cache size limits: if eviction fails and spill is disabled/unavailable, return an error instead of allocating.
- Wire pager’s `cache_spill` setting to pcache behavior.
- Add a spill hook or surface a signal to pager to flush dirty pages.

## Concrete Test (Rust)
Add a unit test in `src/storage/pcache.rs`:

```rust
#[test]
fn test_cache_size_limit_enforced() {
    let mut cache = PCache::open(1024, 0, true);
    cache.set_cache_size(1);

    // Pin two pages without releasing the first
    let p1 = cache.fetch(1, true).unwrap();
    let p2 = cache.fetch(2, true);

    // Expect failure or None when cache is full and no eviction possible
    assert!(p2.is_none(), "pcache should not grow beyond cache_size");

    cache.release(p1);
}
```

If a spill mechanism is added, update the test to assert that a spill occurs instead of unbounded growth.

## Success Criteria
- `cache_size` limits are respected.
- Allocations fail or spill when the cache is full and no evictions are possible.
