//! Page cache implementation (pcache.c/pcache1.c translation).

use std::collections::{HashMap, VecDeque};
use std::ptr::NonNull;

use crate::storage::pager::PgFlags;
use crate::types::Pgno;

/// Page header used by the page cache.
pub struct PgHdr {
    pub pgno: Pgno,
    pub data: Vec<u8>,
    pub extra: Vec<u8>,
    pub flags: PgFlags,
    pub n_ref: i32,
    pub cache_index: usize,
    pub dirty_next: Option<NonNull<PgHdr>>,
    pub dirty_prev: Option<NonNull<PgHdr>>,
}

impl PgHdr {
    fn new(pgno: Pgno, page_size: usize, extra_size: usize, cache_index: usize) -> Self {
        Self {
            pgno,
            data: vec![0u8; page_size],
            extra: vec![0u8; extra_size],
            flags: PgFlags::CLEAN,
            n_ref: 0,
            cache_index,
            dirty_next: None,
            dirty_prev: None,
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.flags.contains(PgFlags::DIRTY)
    }
}

/// Interface for page cache implementations.
pub trait PcacheImpl {
    fn set_cache_size(&mut self, n_cache_size: i32);
    fn page_count(&self) -> i32;
    fn fetch(&mut self, pgno: Pgno, create: bool) -> Option<NonNull<PgHdr>>;
    fn unpin(&mut self, page: NonNull<PgHdr>, discard: bool);
    fn make_clean(&mut self, _page: NonNull<PgHdr>) {}
    fn make_dirty(&mut self, _page: NonNull<PgHdr>) {}
    fn truncate(&mut self, pgno: Pgno);
    fn destroy(&mut self);
    fn shrink(&mut self);
    /// Check if cache is at or near capacity (may need spilling)
    fn needs_spill(&self) -> bool {
        false
    }
    /// Get the maximum cache size
    fn max_size(&self) -> i32 {
        0
    }
}

/// Page cache wrapper for a pager.
pub struct PCache {
    dirty_head: Option<NonNull<PgHdr>>,
    dirty_tail: Option<NonNull<PgHdr>>,
    synced: Option<NonNull<PgHdr>>,
    n_ref_sum: i64,
    cache_size: i32,
    spill_size: i32,
    page_size: usize,
    extra_size: usize,
    purgeable: bool,
    create_flag: bool,
    cache: Box<dyn PcacheImpl>,
}

impl PCache {
    pub fn open(page_size: usize, extra_size: usize, purgeable: bool) -> Self {
        let cache = Box::new(PCache1::new(page_size, extra_size, purgeable));
        Self {
            dirty_head: None,
            dirty_tail: None,
            synced: None,
            n_ref_sum: 0,
            cache_size: 0,
            spill_size: 0,
            page_size,
            extra_size,
            purgeable,
            create_flag: false,
            cache,
        }
    }

    pub fn close(&mut self) {
        self.cache.destroy();
        self.dirty_head = None;
        self.dirty_tail = None;
        self.synced = None;
        self.n_ref_sum = 0;
    }

    pub fn set_cache_size(&mut self, n_cache_size: i32) {
        self.cache_size = n_cache_size;
        self.cache.set_cache_size(n_cache_size);
    }

    pub fn set_spill_size(&mut self, n_spill: i32) {
        self.spill_size = n_spill;
    }

    pub fn fetch(&mut self, pgno: Pgno, create: bool) -> Option<NonNull<PgHdr>> {
        let mut page = self.cache.fetch(pgno, create || self.create_flag)?;
        unsafe {
            let page_ref = page.as_mut();
            page_ref.n_ref += 1;
        }
        self.n_ref_sum += 1;
        Some(page)
    }

    pub fn release(&mut self, mut page: NonNull<PgHdr>) {
        unsafe {
            let page_ref = page.as_mut();
            if page_ref.n_ref > 0 {
                page_ref.n_ref -= 1;
                self.n_ref_sum -= 1;
            }
        }
        self.cache.unpin(page, false);
    }

    pub fn make_dirty(&mut self, mut page: NonNull<PgHdr>) {
        unsafe {
            let page_ref = page.as_mut();
            if page_ref.is_dirty() {
                return;
            }
            page_ref.flags.insert(PgFlags::DIRTY);
        }
        self.manage_dirty_list(page, DirtyListOp::Add);
        self.cache.make_dirty(page);
    }

    pub fn make_clean(&mut self, mut page: NonNull<PgHdr>) {
        unsafe {
            let page_ref = page.as_mut();
            if !page_ref.is_dirty() {
                return;
            }
            page_ref.flags.remove(PgFlags::DIRTY);
        }
        self.manage_dirty_list(page, DirtyListOp::Remove);
        self.cache.make_clean(page);
    }

    pub fn dirty_list(&self) -> Option<NonNull<PgHdr>> {
        self.dirty_head
    }

    pub fn clean_all(&mut self) {
        let mut current = self.dirty_head;
        while let Some(page) = current {
            unsafe {
                current = page.as_ref().dirty_next;
                self.make_clean(page);
            }
        }
    }

    pub fn truncate(&mut self, pgno: Pgno) {
        self.cache.truncate(pgno);
    }

    pub fn shrink(&mut self) {
        self.cache.shrink();
    }

    /// Get the total reference count sum across all pages
    pub fn ref_count(&self) -> i64 {
        self.n_ref_sum
    }

    /// Get the number of pages in the cache
    pub fn page_count(&self) -> i32 {
        self.cache.page_count()
    }

    /// Check if cache needs spilling (at or near capacity)
    pub fn needs_spill(&self) -> bool {
        self.cache.needs_spill()
    }

    /// Get the dirty page count
    pub fn dirty_count(&self) -> i32 {
        let mut count = 0;
        let mut current = self.dirty_head;
        while let Some(page) = current {
            count += 1;
            unsafe {
                current = page.as_ref().dirty_next;
            }
        }
        count
    }

    fn manage_dirty_list(&mut self, page: NonNull<PgHdr>, op: DirtyListOp) {
        match op {
            DirtyListOp::Remove => self.remove_dirty(page),
            DirtyListOp::Add => self.add_dirty(page),
            DirtyListOp::Front => {
                self.remove_dirty(page);
                self.add_dirty(page);
            }
        }
    }

    fn add_dirty(&mut self, mut page: NonNull<PgHdr>) {
        unsafe {
            let page_ref = page.as_mut();
            page_ref.dirty_prev = None;
            page_ref.dirty_next = self.dirty_head;
            if let Some(mut head) = self.dirty_head {
                head.as_mut().dirty_prev = Some(page);
            } else {
                self.dirty_tail = Some(page);
            }
            self.dirty_head = Some(page);
            if self.synced.is_none() {
                self.synced = self.dirty_tail;
            }
        }
    }

    fn remove_dirty(&mut self, mut page: NonNull<PgHdr>) {
        unsafe {
            let page_ref = page.as_mut();
            if let Some(mut next) = page_ref.dirty_next {
                next.as_mut().dirty_prev = page_ref.dirty_prev;
            } else {
                self.dirty_tail = page_ref.dirty_prev;
            }
            if let Some(mut prev) = page_ref.dirty_prev {
                prev.as_mut().dirty_next = page_ref.dirty_next;
            } else {
                self.dirty_head = page_ref.dirty_next;
            }
            if self.synced == Some(page) {
                self.synced = page_ref.dirty_prev;
            }
            page_ref.dirty_next = None;
            page_ref.dirty_prev = None;
        }
    }
}

enum DirtyListOp {
    Remove,
    Add,
    Front,
}

/// Default cache implementation (pcache1).
pub struct PCache1 {
    page_size: usize,
    extra_size: usize,
    purgeable: bool,
    n_min: u32,
    n_max: u32,
    n90pct: u32,
    n_page: u32,
    pages: Vec<Option<Box<PgHdr>>>,
    map: HashMap<Pgno, usize>,
    lru: VecDeque<usize>,
}

impl PCache1 {
    pub fn new(page_size: usize, extra_size: usize, purgeable: bool) -> Self {
        Self {
            page_size,
            extra_size,
            purgeable,
            n_min: 0,
            n_max: 2000,
            n90pct: 1800,
            n_page: 0,
            pages: Vec::new(),
            map: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    fn remove_from_lru(&mut self, idx: usize) {
        if let Some(pos) = self.lru.iter().position(|&v| v == idx) {
            self.lru.remove(pos);
        }
    }

    fn evict_lru(&mut self) -> Option<usize> {
        while let Some(idx) = self.lru.pop_front() {
            if let Some(page) = self.pages.get(idx).and_then(|p| p.as_ref()) {
                if page.n_ref == 0 {
                    let pgno = page.pgno;
                    self.map.remove(&pgno);
                    self.pages[idx] = None;
                    self.n_page = self.n_page.saturating_sub(1);
                    return Some(idx);
                }
            }
        }
        None
    }

    fn allocate_page(&mut self, pgno: Pgno) -> NonNull<PgHdr> {
        let idx = self.pages.len();
        let page = Box::new(PgHdr::new(pgno, self.page_size, self.extra_size, idx));
        let ptr = NonNull::from(page.as_ref());
        self.pages.push(Some(page));
        self.map.insert(pgno, idx);
        self.n_page += 1;
        ptr
    }
}

impl PcacheImpl for PCache1 {
    fn set_cache_size(&mut self, n_cache_size: i32) {
        if n_cache_size <= 0 {
            return;
        }
        self.n_max = n_cache_size as u32;
        self.n90pct = (self.n_max * 9) / 10;
        if self.n90pct < self.n_min {
            self.n90pct = self.n_min;
        }
    }

    fn page_count(&self) -> i32 {
        self.n_page as i32
    }

    fn fetch(&mut self, pgno: Pgno, create: bool) -> Option<NonNull<PgHdr>> {
        if let Some(&idx) = self.map.get(&pgno) {
            let mut remove_lru = false;
            let page_ptr = {
                let page = self.pages.get_mut(idx)?.as_mut()?;
                if page.n_ref == 0 {
                    remove_lru = true;
                }
                NonNull::from(page.as_mut())
            };
            if remove_lru {
                self.remove_from_lru(idx);
            }
            return Some(page_ptr);
        }

        if !create {
            return None;
        }

        // Enforce cache size limits: if cache is full, try to evict
        if self.purgeable && self.n_page >= self.n_max {
            if self.evict_lru().is_none() {
                // Eviction failed (all pages pinned) - respect cache limit
                // Return None to signal cache is full
                return None;
            }
        }

        Some(self.allocate_page(pgno))
    }

    fn unpin(&mut self, page: NonNull<PgHdr>, discard: bool) {
        unsafe {
            let page_ref = page.as_ref();
            if discard {
                let idx = page_ref.cache_index;
                self.map.remove(&page_ref.pgno);
                self.pages[idx] = None;
                self.n_page = self.n_page.saturating_sub(1);
                self.remove_from_lru(idx);
                return;
            }
            if page_ref.n_ref == 0 {
                self.lru.push_back(page_ref.cache_index);
            }
        }
    }

    fn truncate(&mut self, pgno: Pgno) {
        let mut to_remove = Vec::new();
        for (&key, &idx) in &self.map {
            if key >= pgno {
                to_remove.push((key, idx));
            }
        }
        for (key, idx) in to_remove {
            self.map.remove(&key);
            self.pages[idx] = None;
            self.remove_from_lru(idx);
            self.n_page = self.n_page.saturating_sub(1);
        }
    }

    fn destroy(&mut self) {
        self.pages.clear();
        self.map.clear();
        self.lru.clear();
        self.n_page = 0;
    }

    fn shrink(&mut self) {
        while self.n_page > self.n90pct {
            if self.evict_lru().is_none() {
                break;
            }
        }
    }

    fn needs_spill(&self) -> bool {
        // Cache needs spilling when we're at 90% capacity or more
        self.purgeable && self.n_page >= self.n90pct
    }

    fn max_size(&self) -> i32 {
        self.n_max as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_size_limit_enforced() {
        let mut cache = PCache::open(1024, 0, true);
        cache.set_cache_size(2);

        // Fetch first page - should succeed
        let p1 = cache.fetch(1, true);
        assert!(p1.is_some(), "First page should be allocated");

        // Fetch second page - should succeed (cache size is 2)
        let p2 = cache.fetch(2, true);
        assert!(p2.is_some(), "Second page should be allocated");

        // Both pages are pinned (n_ref > 0), so third page should fail
        // when cache is full and no eviction possible
        let p3 = cache.fetch(3, true);
        assert!(
            p3.is_none(),
            "pcache should not grow beyond cache_size when all pages pinned"
        );

        // Release first page
        if let Some(page) = p1 {
            cache.release(page);
        }

        // Now third page should succeed (p1 can be evicted)
        let p3 = cache.fetch(3, true);
        assert!(
            p3.is_some(),
            "Third page should succeed after releasing first"
        );

        // Cleanup
        if let Some(page) = p2 {
            cache.release(page);
        }
        if let Some(page) = p3 {
            cache.release(page);
        }
    }

    #[test]
    fn test_cache_eviction_with_unpinned_pages() {
        let mut cache = PCache::open(1024, 0, true);
        cache.set_cache_size(2);

        // Fetch and immediately release a page (so it's unpinned)
        let p1 = cache.fetch(1, true).unwrap();
        cache.release(p1);

        // Fetch second page
        let p2 = cache.fetch(2, true).unwrap();
        cache.release(p2);

        // Cache is full but both pages are unpinned
        // Third page should succeed via eviction
        let p3 = cache.fetch(3, true);
        assert!(p3.is_some(), "Third page should succeed via LRU eviction");

        if let Some(page) = p3 {
            cache.release(page);
        }
    }

    #[test]
    fn test_needs_spill_at_90_percent() {
        let mut cache = PCache::open(1024, 0, true);
        cache.set_cache_size(10);

        // Fill cache to 90% (9 pages)
        for i in 1..=8 {
            let page = cache.fetch(i, true).unwrap();
            cache.release(page);
        }
        assert!(!cache.needs_spill(), "Should not need spill at 80%");

        let page = cache.fetch(9, true).unwrap();
        cache.release(page);
        assert!(cache.needs_spill(), "Should need spill at 90%");
    }

    #[test]
    fn test_page_count() {
        let mut cache = PCache::open(1024, 0, true);
        cache.set_cache_size(10);

        assert_eq!(cache.page_count(), 0);

        let p1 = cache.fetch(1, true).unwrap();
        assert_eq!(cache.page_count(), 1);

        let p2 = cache.fetch(2, true).unwrap();
        assert_eq!(cache.page_count(), 2);

        cache.release(p1);
        cache.release(p2);
        // Pages still in cache (unpinned), count should be same
        assert_eq!(cache.page_count(), 2);
    }

    #[test]
    fn test_dirty_count() {
        let mut cache = PCache::open(1024, 0, true);
        cache.set_cache_size(10);

        assert_eq!(cache.dirty_count(), 0);

        let p1 = cache.fetch(1, true).unwrap();
        cache.make_dirty(p1);
        assert_eq!(cache.dirty_count(), 1);

        let p2 = cache.fetch(2, true).unwrap();
        cache.make_dirty(p2);
        assert_eq!(cache.dirty_count(), 2);

        cache.make_clean(p1);
        assert_eq!(cache.dirty_count(), 1);

        cache.release(p1);
        cache.release(p2);
    }
}
