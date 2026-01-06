# Translate btmutex.c - B-tree Mutexes

## Overview
Translate the B-tree mutex routines that handle locking for B-tree operations. These ensure thread-safety when multiple threads access the same B-tree connection.

## Source Reference
- `sqlite3/src/btmutex.c` - 240 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Functions

### Lock Entry/Exit
```rust
impl Btree {
    /// Enter the mutex on this B-tree
    /// Must be called before any B-tree operation
    pub fn enter(&self) {
        // If shared cache mode, need to lock BtShared
        if let Some(shared) = &self.shared {
            shared.mutex.lock();
        }
    }

    /// Leave the mutex on this B-tree
    pub fn leave(&self) {
        if let Some(shared) = &self.shared {
            shared.mutex.unlock();
        }
    }

    /// Enter mutex and track for cursor operations
    pub fn enter_cursor(&self, cursor: &BtCursor) {
        self.enter();
        // Additional cursor-specific setup
    }
}
```

### Lock All B-trees
```rust
impl Connection {
    /// Lock all B-trees for this connection
    /// Used during schema changes, VACUUM, etc.
    pub fn enter_all(&self) {
        for db in &self.databases {
            if let Some(btree) = &db.btree {
                btree.enter();
            }
        }
    }

    /// Unlock all B-trees
    pub fn leave_all(&self) {
        for db in self.databases.iter().rev() {
            if let Some(btree) = &db.btree {
                btree.leave();
            }
        }
    }
}
```

### Shared Cache Locking
```rust
impl BtShared {
    /// Check if any cursor in use (for shared cache)
    pub fn has_cursors(&self) -> bool {
        !self.cursors.is_empty()
    }

    /// Get lock on table for read or write
    pub fn lock_table(&mut self, table: Pgno, write: bool) -> Result<()> {
        // In shared cache mode, track table-level locks
        // to prevent conflicts between connections
        if write {
            // Check no other connection has read or write lock
            if self.has_read_lock(table) || self.has_write_lock(table) {
                return Err(Error::new(ErrorCode::Locked));
            }
            self.write_locks.insert(table);
        } else {
            // Check no other connection has write lock
            if self.has_write_lock(table) {
                return Err(Error::new(ErrorCode::Locked));
            }
            self.read_locks.entry(table).or_insert(0) += 1;
        }
        Ok(())
    }

    /// Release table lock
    pub fn unlock_table(&mut self, table: Pgno, write: bool) {
        if write {
            self.write_locks.remove(&table);
        } else {
            if let Some(count) = self.read_locks.get_mut(&table) {
                *count -= 1;
                if *count == 0 {
                    self.read_locks.remove(&table);
                }
            }
        }
    }
}
```

### Guard Types
```rust
/// RAII guard for B-tree mutex
pub struct BtreeGuard<'a> {
    btree: &'a Btree,
}

impl<'a> BtreeGuard<'a> {
    pub fn new(btree: &'a Btree) -> Self {
        btree.enter();
        BtreeGuard { btree }
    }
}

impl<'a> Drop for BtreeGuard<'a> {
    fn drop(&mut self) {
        self.btree.leave();
    }
}

/// Guard for all B-trees on a connection
pub struct AllBtreesGuard<'a> {
    conn: &'a Connection,
}

impl<'a> AllBtreesGuard<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        conn.enter_all();
        AllBtreesGuard { conn }
    }
}

impl<'a> Drop for AllBtreesGuard<'a> {
    fn drop(&mut self) {
        self.conn.leave_all();
    }
}
```

### Debug Assertions
```rust
impl Btree {
    /// Assert that we hold the mutex (debug only)
    #[cfg(debug_assertions)]
    pub fn assert_held(&self) {
        if let Some(shared) = &self.shared {
            assert!(shared.mutex.is_locked_by_current_thread());
        }
    }

    /// Assert that we don't hold the mutex
    #[cfg(debug_assertions)]
    pub fn assert_not_held(&self) {
        if let Some(shared) = &self.shared {
            assert!(!shared.mutex.is_locked_by_current_thread());
        }
    }
}
```

## Shared Cache Mode

When shared cache is enabled, multiple connections can share the same BtShared:

```rust
/// Global registry of shared B-trees
static SHARED_CACHE: Lazy<Mutex<HashMap<String, Weak<BtShared>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

impl Btree {
    /// Open with shared cache support
    pub fn open_shared(path: &str) -> Result<Arc<BtShared>> {
        let mut cache = SHARED_CACHE.lock().unwrap();

        // Check for existing shared cache
        if let Some(weak) = cache.get(path) {
            if let Some(shared) = weak.upgrade() {
                return Ok(shared);
            }
        }

        // Create new shared cache
        let shared = Arc::new(BtShared::new(path)?);
        cache.insert(path.to_string(), Arc::downgrade(&shared));
        Ok(shared)
    }
}
```

## Rust Translation Considerations

### Mutex Choice
- Use `std::sync::Mutex` for simplicity
- Consider `parking_lot::Mutex` for performance
- Need recursive mutex support for SQLite compatibility

### Lock Ordering
- Always lock B-trees in consistent order to avoid deadlock
- Main database before attached databases
- Lock all before any cursor operation

### RAII
- Use guard types for automatic unlock
- Prevents forgetting to release locks
- Exception-safe (panic-safe in Rust)

## Acceptance Criteria
- [ ] enter/leave mutex functions
- [ ] enter_all/leave_all for connection
- [ ] BtreeGuard RAII type
- [ ] AllBtreesGuard RAII type
- [ ] Table-level locking for shared cache
- [ ] Debug assertions for lock state
- [ ] Shared cache registry (if supporting shared cache mode)
