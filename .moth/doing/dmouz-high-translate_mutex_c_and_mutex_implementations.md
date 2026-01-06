# Translate mutex.c - Mutex Implementations

## Overview
Translate mutex abstraction layer and platform-specific implementations for thread safety.

## Source Reference
- `sqlite3/src/mutex.c` - Main mutex interface (~200 lines)
- `sqlite3/src/mutex_unix.c` - POSIX pthread mutexes (~300 lines)
- `sqlite3/src/mutex_w32.c` - Windows critical sections (~300 lines)
- `sqlite3/src/mutex_noop.c` - No-op for single-threaded (~100 lines)

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Mutex Types
```rust
/// Mutex type identifiers
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MutexType {
    /// Fast mutex (non-recursive)
    Fast = 0,
    /// Recursive mutex
    Recursive = 1,
    /// Static mutex for global structures
    Static(StaticMutexId),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StaticMutexId {
    Main = 2,
    Mem = 3,
    Mem2 = 4,
    Open = 5,
    Prng = 6,
    Lru = 7,
    Lru2 = 8,
    Pmem = 9,
    App1 = 10,
    App2 = 11,
    App3 = 12,
    Vfs1 = 13,
    Vfs2 = 14,
    Vfs3 = 15,
}
```

### Mutex Trait
```rust
/// Abstract mutex interface
pub trait Mutex: Send + Sync {
    /// Enter the mutex (blocking)
    fn enter(&self);

    /// Try to enter mutex (non-blocking)
    fn try_enter(&self) -> bool;

    /// Leave the mutex
    fn leave(&self);

    /// Check if mutex is held by current thread (debug only)
    #[cfg(debug_assertions)]
    fn held(&self) -> bool;
}

/// Mutex factory
pub trait MutexMethods {
    /// Create a new mutex
    fn new_mutex(&self, mutex_type: MutexType) -> Box<dyn Mutex>;

    /// Get a static mutex
    fn static_mutex(&self, id: StaticMutexId) -> &dyn Mutex;
}
```

## POSIX Implementation

```rust
#[cfg(unix)]
pub struct PthreadMutex {
    /// Native pthread mutex
    inner: UnsafeCell<libc::pthread_mutex_t>,
    /// Thread that holds the mutex (for recursive/debug)
    #[cfg(debug_assertions)]
    owner: AtomicI64,
    /// Recursion count
    count: AtomicI32,
    /// Is this a recursive mutex?
    recursive: bool,
}

#[cfg(unix)]
impl PthreadMutex {
    pub fn new(recursive: bool) -> Self {
        let mut mutex = unsafe { std::mem::zeroed::<libc::pthread_mutex_t>() };

        unsafe {
            if recursive {
                let mut attr = std::mem::zeroed::<libc::pthread_mutexattr_t>();
                libc::pthread_mutexattr_init(&mut attr);
                libc::pthread_mutexattr_settype(&mut attr, libc::PTHREAD_MUTEX_RECURSIVE);
                libc::pthread_mutex_init(&mut mutex, &attr);
                libc::pthread_mutexattr_destroy(&mut attr);
            } else {
                libc::pthread_mutex_init(&mut mutex, std::ptr::null());
            }
        }

        Self {
            inner: UnsafeCell::new(mutex),
            #[cfg(debug_assertions)]
            owner: AtomicI64::new(0),
            count: AtomicI32::new(0),
            recursive,
        }
    }
}

#[cfg(unix)]
impl Mutex for PthreadMutex {
    fn enter(&self) {
        unsafe {
            libc::pthread_mutex_lock(self.inner.get());
        }

        #[cfg(debug_assertions)]
        {
            let tid = unsafe { libc::pthread_self() } as i64;
            self.owner.store(tid, Ordering::SeqCst);
        }

        self.count.fetch_add(1, Ordering::SeqCst);
    }

    fn try_enter(&self) -> bool {
        let rc = unsafe {
            libc::pthread_mutex_trylock(self.inner.get())
        };

        if rc == 0 {
            #[cfg(debug_assertions)]
            {
                let tid = unsafe { libc::pthread_self() } as i64;
                self.owner.store(tid, Ordering::SeqCst);
            }
            self.count.fetch_add(1, Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    fn leave(&self) {
        let cnt = self.count.fetch_sub(1, Ordering::SeqCst);
        assert!(cnt > 0, "mutex underflow");

        #[cfg(debug_assertions)]
        if cnt == 1 {
            self.owner.store(0, Ordering::SeqCst);
        }

        unsafe {
            libc::pthread_mutex_unlock(self.inner.get());
        }
    }

    #[cfg(debug_assertions)]
    fn held(&self) -> bool {
        let tid = unsafe { libc::pthread_self() } as i64;
        self.owner.load(Ordering::SeqCst) == tid && self.count.load(Ordering::SeqCst) > 0
    }
}

#[cfg(unix)]
impl Drop for PthreadMutex {
    fn drop(&mut self) {
        unsafe {
            libc::pthread_mutex_destroy(self.inner.get());
        }
    }
}

// Safety: mutex operations are thread-safe
#[cfg(unix)]
unsafe impl Send for PthreadMutex {}
#[cfg(unix)]
unsafe impl Sync for PthreadMutex {}
```

## Windows Implementation

```rust
#[cfg(windows)]
pub struct Win32Mutex {
    /// Critical section
    inner: UnsafeCell<CRITICAL_SECTION>,
    /// Recursion count
    count: AtomicI32,
}

#[cfg(windows)]
impl Win32Mutex {
    pub fn new() -> Self {
        let mut cs = unsafe { std::mem::zeroed::<CRITICAL_SECTION>() };
        unsafe {
            InitializeCriticalSection(&mut cs);
        }

        Self {
            inner: UnsafeCell::new(cs),
            count: AtomicI32::new(0),
        }
    }
}

#[cfg(windows)]
impl Mutex for Win32Mutex {
    fn enter(&self) {
        unsafe {
            EnterCriticalSection(self.inner.get());
        }
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    fn try_enter(&self) -> bool {
        let rc = unsafe { TryEnterCriticalSection(self.inner.get()) };
        if rc != 0 {
            self.count.fetch_add(1, Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    fn leave(&self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
        unsafe {
            LeaveCriticalSection(self.inner.get());
        }
    }

    #[cfg(debug_assertions)]
    fn held(&self) -> bool {
        // Windows CRITICAL_SECTION tracks owner internally
        self.count.load(Ordering::SeqCst) > 0
    }
}

#[cfg(windows)]
impl Drop for Win32Mutex {
    fn drop(&mut self) {
        unsafe {
            DeleteCriticalSection(self.inner.get());
        }
    }
}

#[cfg(windows)]
unsafe impl Send for Win32Mutex {}
#[cfg(windows)]
unsafe impl Sync for Win32Mutex {}
```

## No-Op Implementation

```rust
/// No-op mutex for single-threaded mode
pub struct NoopMutex;

impl Mutex for NoopMutex {
    fn enter(&self) {}
    fn try_enter(&self) -> bool { true }
    fn leave(&self) {}

    #[cfg(debug_assertions)]
    fn held(&self) -> bool { true }
}
```

## Rust-Native Implementation

```rust
/// Rust-native mutex using std::sync
pub struct RustMutex {
    inner: parking_lot::RawMutex,
    count: AtomicI32,
}

impl RustMutex {
    pub fn new() -> Self {
        Self {
            inner: parking_lot::RawMutex::INIT,
            count: AtomicI32::new(0),
        }
    }
}

impl Mutex for RustMutex {
    fn enter(&self) {
        self.inner.lock();
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    fn try_enter(&self) -> bool {
        if self.inner.try_lock() {
            self.count.fetch_add(1, Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    fn leave(&self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
        unsafe { self.inner.unlock() };
    }

    #[cfg(debug_assertions)]
    fn held(&self) -> bool {
        self.count.load(Ordering::SeqCst) > 0
    }
}
```

## Static Mutex Management

```rust
/// Static mutex pool
pub struct StaticMutexPool {
    mutexes: [parking_lot::Mutex<()>; 16],
}

lazy_static! {
    static ref STATIC_MUTEXES: StaticMutexPool = StaticMutexPool::new();
}

impl StaticMutexPool {
    fn new() -> Self {
        Self {
            mutexes: Default::default(),
        }
    }

    pub fn get(&self, id: StaticMutexId) -> &parking_lot::Mutex<()> {
        &self.mutexes[id as usize]
    }
}

pub fn mutex_alloc(mutex_type: MutexType) -> Box<dyn Mutex> {
    match mutex_type {
        MutexType::Fast => Box::new(RustMutex::new()),
        MutexType::Recursive => Box::new(RustRecursiveMutex::new()),
        MutexType::Static(id) => {
            // Return wrapper around static mutex
            Box::new(StaticMutexWrapper(id))
        }
    }
}

pub fn mutex_enter(mtx: &dyn Mutex) {
    mtx.enter();
}

pub fn mutex_try(mtx: &dyn Mutex) -> bool {
    mtx.try_enter()
}

pub fn mutex_leave(mtx: &dyn Mutex) {
    mtx.leave();
}

#[cfg(debug_assertions)]
pub fn mutex_held(mtx: &dyn Mutex) -> bool {
    mtx.held()
}
```

## Global Configuration

```rust
/// Mutex subsystem configuration
pub struct MutexConfig {
    /// Use no-op mutexes (single-threaded mode)
    pub single_threaded: bool,
    /// Use native platform mutexes
    pub use_native: bool,
}

impl Default for MutexConfig {
    fn default() -> Self {
        Self {
            single_threaded: false,
            use_native: false,
        }
    }
}

static MUTEX_CONFIG: once_cell::sync::OnceCell<MutexConfig> = once_cell::sync::OnceCell::new();

pub fn mutex_init(config: MutexConfig) {
    MUTEX_CONFIG.set(config).ok();
}
```

## Acceptance Criteria
- [ ] Mutex trait abstraction
- [ ] Static mutex identifiers (Main, Mem, Open, etc.)
- [ ] POSIX pthread_mutex implementation
- [ ] Windows CRITICAL_SECTION implementation
- [ ] No-op implementation for single-threaded
- [ ] Rust-native implementation using parking_lot
- [ ] Recursive mutex support
- [ ] Try-lock (non-blocking acquire)
- [ ] Debug-mode ownership tracking
- [ ] Static mutex pool for global resources
- [ ] Thread-safety guarantees (Send + Sync)
- [ ] Proper cleanup on drop
