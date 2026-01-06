# Translate status.c and global.c - Status and Global State

## Overview
Translate status reporting functions and global state management.

## Source Reference
- `sqlite3/src/status.c` - ~200 lines
- `sqlite3/src/global.c` - ~300 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Status Counters
```rust
/// Status counter identifiers
#[derive(Debug, Clone, Copy)]
pub enum StatusOp {
    /// Memory used by SQLite
    MemoryUsed = 0,
    /// Number of page cache entries
    PageCacheUsed = 1,
    /// Page cache size
    PageCacheSize = 2,
    /// Bytes of page cache overflow
    PageCacheOverflow = 3,
    /// Bytes of malloc'd scratch memory
    ScratchUsed = 4,
    /// Number of scratch allocations
    ScratchSize = 5,
    /// Bytes of scratch overflow
    ScratchOverflow = 6,
    /// Malloc count
    MallocCount = 7,
    /// Largest single allocation
    MallocSize = 8,
    /// Parser stack depth
    ParserStack = 9,
    /// Page cache hit count
    PageCacheHit = 10,
    /// Page cache miss count
    PageCacheMiss = 11,
    /// Page cache writes
    PageCacheWrite = 12,
    /// Page cache spills
    PageCacheSpill = 13,
}

/// Database connection status counters
#[derive(Debug, Clone, Copy)]
pub enum DbStatusOp {
    /// Lookaside memory used
    LookasideUsed = 0,
    /// Pager cache used
    CacheUsed = 1,
    /// Schema memory used
    SchemaUsed = 2,
    /// Statement memory used
    StmtUsed = 3,
    /// Lookaside hit count
    LookasideHit = 4,
    /// Lookaside miss (size)
    LookasideMissSize = 5,
    /// Lookaside miss (full)
    LookasideMissFull = 6,
    /// Cache hits
    CacheHit = 7,
    /// Cache misses
    CacheMiss = 8,
    /// Cache writes
    CacheWrite = 9,
    /// Deferred foreign key violations
    DeferredFks = 10,
    /// Cache spills
    CacheSpill = 11,
    /// Cache used (shared)
    CacheUsedShared = 12,
}
```

### Status Storage
```rust
/// Global status counters
pub struct GlobalStatus {
    /// Counter values
    values: [AtomicI64; 14],
    /// High water marks
    high_water: [AtomicI64; 14],
}

impl GlobalStatus {
    pub const fn new() -> Self {
        Self {
            values: [
                AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0),
                AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0),
                AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0),
                AtomicI64::new(0), AtomicI64::new(0),
            ],
            high_water: [
                AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0),
                AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0),
                AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0),
                AtomicI64::new(0), AtomicI64::new(0),
            ],
        }
    }
}

static GLOBAL_STATUS: GlobalStatus = GlobalStatus::new();
```

## Status API

```rust
/// Get current and high-water value for a status counter
pub fn sqlite3_status(
    op: StatusOp,
    current: &mut i64,
    high_water: &mut i64,
    reset_flag: bool,
) -> Result<()> {
    let idx = op as usize;

    if idx >= GLOBAL_STATUS.values.len() {
        return Err(Error::with_code(ErrorCode::Misuse));
    }

    *current = GLOBAL_STATUS.values[idx].load(Ordering::SeqCst);
    *high_water = GLOBAL_STATUS.high_water[idx].load(Ordering::SeqCst);

    if reset_flag {
        GLOBAL_STATUS.high_water[idx].store(*current, Ordering::SeqCst);
    }

    Ok(())
}

/// Get 64-bit status value
pub fn sqlite3_status64(
    op: StatusOp,
    current: &mut i64,
    high_water: &mut i64,
    reset_flag: bool,
) -> Result<()> {
    sqlite3_status(op, current, high_water, reset_flag)
}

/// Update a status counter
pub fn status_add(op: StatusOp, delta: i64) {
    let idx = op as usize;
    if idx < GLOBAL_STATUS.values.len() {
        let new_val = GLOBAL_STATUS.values[idx].fetch_add(delta, Ordering::SeqCst) + delta;

        // Update high water mark
        loop {
            let high = GLOBAL_STATUS.high_water[idx].load(Ordering::SeqCst);
            if new_val <= high {
                break;
            }
            if GLOBAL_STATUS.high_water[idx]
                .compare_exchange(high, new_val, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }
}

/// Set a status counter
pub fn status_set(op: StatusOp, value: i64) {
    let idx = op as usize;
    if idx < GLOBAL_STATUS.values.len() {
        GLOBAL_STATUS.values[idx].store(value, Ordering::SeqCst);
    }
}
```

## Database Status

```rust
impl Connection {
    /// Get database connection status
    pub fn db_status(
        &self,
        op: DbStatusOp,
        current: &mut i64,
        high_water: &mut i64,
        reset_flag: bool,
    ) -> Result<()> {
        match op {
            DbStatusOp::LookasideUsed => {
                *current = self.lookaside.as_ref()
                    .map(|l| l.used() as i64)
                    .unwrap_or(0);
                *high_water = self.lookaside.as_ref()
                    .map(|l| l.high_water() as i64)
                    .unwrap_or(0);
            }
            DbStatusOp::CacheUsed => {
                *current = self.dbs.iter()
                    .filter_map(|db| db.btree.as_ref())
                    .map(|bt| bt.pager().cache_size() as i64)
                    .sum();
                *high_water = *current;  // No high water for this
            }
            DbStatusOp::SchemaUsed => {
                // Estimate schema memory usage
                *current = self.estimate_schema_memory() as i64;
                *high_water = *current;
            }
            DbStatusOp::StmtUsed => {
                *current = self.estimate_statement_memory() as i64;
                *high_water = *current;
            }
            DbStatusOp::CacheHit => {
                *current = self.cache_stats.hits.load(Ordering::SeqCst);
                *high_water = *current;
            }
            DbStatusOp::CacheMiss => {
                *current = self.cache_stats.misses.load(Ordering::SeqCst);
                *high_water = *current;
            }
            DbStatusOp::DeferredFks => {
                *current = self.deferred_fk_count;
                *high_water = *current;
            }
            _ => {
                *current = 0;
                *high_water = 0;
            }
        }

        if reset_flag {
            // Reset where applicable
        }

        Ok(())
    }
}
```

## Global State

```rust
/// Global SQLite configuration
pub struct SqliteGlobal {
    /// Is library initialized
    pub is_init: AtomicBool,
    /// Initialization in progress
    pub init_mutex: Mutex<()>,
    /// Current malloc implementation
    pub mem: Option<Box<dyn MemMethods>>,
    /// Current mutex implementation
    pub mutex: Option<Box<dyn MutexMethods>>,
    /// Default VFS
    pub default_vfs: Option<Arc<dyn Vfs>>,
    /// VFS list
    pub vfs_list: Vec<Arc<dyn Vfs>>,
    /// Soft heap limit
    pub soft_heap_limit: AtomicI64,
    /// Hard heap limit
    pub hard_heap_limit: AtomicI64,
    /// URI enabled by default
    pub uri: AtomicBool,
    /// Memory mapping enabled
    pub mmap_size: AtomicI64,
}

lazy_static! {
    pub static ref SQLITE_GLOBAL: RwLock<SqliteGlobal> = RwLock::new(SqliteGlobal {
        is_init: AtomicBool::new(false),
        init_mutex: Mutex::new(()),
        mem: None,
        mutex: None,
        default_vfs: None,
        vfs_list: Vec::new(),
        soft_heap_limit: AtomicI64::new(0),
        hard_heap_limit: AtomicI64::new(0),
        uri: AtomicBool::new(true),
        mmap_size: AtomicI64::new(0),
    });
}
```

## Configuration

```rust
/// Configuration options
#[derive(Debug, Clone, Copy)]
pub enum ConfigOp {
    SingleThread = 1,
    MultiThread = 2,
    Serialized = 3,
    Malloc = 4,
    GetMalloc = 5,
    Mutex = 9,
    GetMutex = 10,
    Lookaside = 13,
    MemStatus = 16,
    PageCache = 7,
    PCache = 14,
    Uri = 17,
    MmapSize = 22,
    SqlLog = 28,
    SoftHeapLimit = 25,
}

/// Configure SQLite (must be called before init)
pub fn sqlite3_config(op: ConfigOp, args: &[i64]) -> Result<()> {
    let global = SQLITE_GLOBAL.write().map_err(|_| Error::with_code(ErrorCode::Misuse))?;

    if global.is_init.load(Ordering::SeqCst) {
        return Err(Error::with_message(
            ErrorCode::Misuse,
            "sqlite3_config must be called before initialization"
        ));
    }

    match op {
        ConfigOp::SingleThread => {
            // Configure for single-threaded use
        }
        ConfigOp::MultiThread => {
            // Configure for multi-threaded use (serialized access)
        }
        ConfigOp::Serialized => {
            // Configure for serialized (thread-safe) use
        }
        ConfigOp::Uri => {
            let enable = args.get(0).copied().unwrap_or(1) != 0;
            global.uri.store(enable, Ordering::SeqCst);
        }
        ConfigOp::MmapSize => {
            let default = args.get(0).copied().unwrap_or(0);
            let max = args.get(1).copied().unwrap_or(default);
            global.mmap_size.store(max, Ordering::SeqCst);
        }
        ConfigOp::SoftHeapLimit => {
            let limit = args.get(0).copied().unwrap_or(0);
            global.soft_heap_limit.store(limit, Ordering::SeqCst);
        }
        _ => {}
    }

    Ok(())
}

/// Configure database connection
pub fn sqlite3_db_config(conn: &mut Connection, op: i32, args: &[i64]) -> Result<()> {
    match op {
        1000 => {  // SQLITE_DBCONFIG_MAINDBNAME
            // Set main database name
        }
        1001 => {  // SQLITE_DBCONFIG_LOOKASIDE
            let buf_size = args.get(0).copied().unwrap_or(0);
            let count = args.get(1).copied().unwrap_or(0);
            conn.configure_lookaside(buf_size as usize, count as usize)?;
        }
        1002 => {  // SQLITE_DBCONFIG_ENABLE_FKEY
            let enable = args.get(0).copied().unwrap_or(1) != 0;
            conn.foreign_keys = enable;
        }
        1003 => {  // SQLITE_DBCONFIG_ENABLE_TRIGGER
            let enable = args.get(0).copied().unwrap_or(1) != 0;
            conn.triggers_enabled = enable;
        }
        _ => {}
    }

    Ok(())
}
```

## Version Information

```rust
/// SQLite version string
pub const SQLITE_VERSION: &str = "3.46.0";

/// SQLite version number (3046000)
pub const SQLITE_VERSION_NUMBER: i32 = 3046000;

/// Source ID (for reproducibility)
pub const SQLITE_SOURCE_ID: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " ",
    env!("GIT_HASH", "unknown")
);

/// Get version string
pub fn sqlite3_libversion() -> &'static str {
    SQLITE_VERSION
}

/// Get version number
pub fn sqlite3_libversion_number() -> i32 {
    SQLITE_VERSION_NUMBER
}

/// Get source ID
pub fn sqlite3_sourceid() -> &'static str {
    SQLITE_SOURCE_ID
}

/// Check if thread-safe
pub fn sqlite3_threadsafe() -> i32 {
    // 0 = single-thread, 1 = multi-thread, 2 = serialized
    2
}
```

## Acceptance Criteria
- [ ] Global status counters (StatusOp)
- [ ] Database status counters (DbStatusOp)
- [ ] sqlite3_status/status64
- [ ] sqlite3_db_status
- [ ] High water mark tracking
- [ ] Global configuration (sqlite3_config)
- [ ] Database configuration (sqlite3_db_config)
- [ ] Version information functions
- [ ] Thread safety mode reporting
- [ ] Memory status tracking
- [ ] Cache hit/miss statistics
- [ ] Atomic counter updates
