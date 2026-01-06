# Translate os.c - OS Interface

## Overview
Translate the VFS (Virtual File System) abstraction layer that provides platform-independent file and OS operations.

## Source Reference
- `sqlite3/src/os.c` - 447 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### VFS (Virtual File System)
```rust
/// Virtual File System - platform abstraction
pub trait Vfs: Send + Sync {
    /// VFS name (e.g., "unix", "win32")
    fn name(&self) -> &str;

    /// Maximum pathname length
    fn max_pathname(&self) -> i32 { 1024 }

    /// Open a file
    fn open(
        &self,
        path: Option<&str>,
        flags: OpenFlags,
    ) -> Result<Box<dyn VfsFile>>;

    /// Delete a file
    fn delete(&self, path: &str, sync_dir: bool) -> Result<()>;

    /// Check if file exists
    fn access(&self, path: &str, flags: AccessFlags) -> Result<bool>;

    /// Get full pathname
    fn full_pathname(&self, path: &str) -> Result<String>;

    /// Dynamic library operations (optional)
    fn dlopen(&self, _path: &str) -> Result<*mut ()> {
        Err(Error::not_supported())
    }
    fn dlerror(&self) -> String { String::new() }
    fn dlsym(&self, _handle: *mut (), _symbol: &str) -> Option<*mut ()> { None }
    fn dlclose(&self, _handle: *mut ()) {}

    /// Random bytes
    fn randomness(&self, buf: &mut [u8]) -> i32;

    /// Sleep in microseconds
    fn sleep(&self, microseconds: i32) -> i32;

    /// Current time as Julian day
    fn current_time(&self) -> f64;

    /// Current time with higher precision
    fn current_time_i64(&self) -> i64;

    /// Last OS error
    fn get_last_error(&self) -> (i32, String);

    /// System call configuration
    fn set_system_call(&mut self, _name: &str, _ptr: *const ()) -> Result<()> {
        Err(Error::not_found())
    }
    fn get_system_call(&self, _name: &str) -> Option<*const ()> { None }
    fn next_system_call(&self, _name: &str) -> Option<&str> { None }
}

bitflags! {
    pub struct OpenFlags: u32 {
        const READONLY = 0x00000001;
        const READWRITE = 0x00000002;
        const CREATE = 0x00000004;
        const DELETEONCLOSE = 0x00000008;
        const EXCLUSIVE = 0x00000010;
        const AUTOPROXY = 0x00000020;
        const URI = 0x00000040;
        const MEMORY = 0x00000080;
        const MAIN_DB = 0x00000100;
        const TEMP_DB = 0x00000200;
        const TRANSIENT_DB = 0x00000400;
        const MAIN_JOURNAL = 0x00000800;
        const TEMP_JOURNAL = 0x00001000;
        const SUBJOURNAL = 0x00002000;
        const SUPER_JOURNAL = 0x00004000;
        const NOMUTEX = 0x00008000;
        const FULLMUTEX = 0x00010000;
        const SHAREDCACHE = 0x00020000;
        const PRIVATECACHE = 0x00040000;
        const WAL = 0x00080000;
        const NOFOLLOW = 0x01000000;
        const EXRESCODE = 0x02000000;
    }
}

bitflags! {
    pub struct AccessFlags: u32 {
        const EXISTS = 0;
        const READWRITE = 1;
        const READ = 2;
    }
}
```

### VFS File
```rust
/// File handle abstraction
pub trait VfsFile: Send + Sync {
    /// Read from file
    fn read(&self, buf: &mut [u8], offset: i64) -> Result<usize>;

    /// Write to file
    fn write(&self, buf: &[u8], offset: i64) -> Result<usize>;

    /// Truncate file
    fn truncate(&self, size: i64) -> Result<()>;

    /// Sync to disk
    fn sync(&self, flags: SyncFlags) -> Result<()>;

    /// Get file size
    fn file_size(&self) -> Result<i64>;

    /// Lock file
    fn lock(&self, lock_type: LockType) -> Result<()>;

    /// Unlock file
    fn unlock(&self, lock_type: LockType) -> Result<()>;

    /// Check if lock held
    fn check_reserved_lock(&self) -> Result<bool>;

    /// File control operations
    fn file_control(&mut self, op: FileControlOp) -> Result<()>;

    /// Sector size for this file
    fn sector_size(&self) -> i32 { 4096 }

    /// Device characteristics
    fn device_characteristics(&self) -> DeviceCharacteristics {
        DeviceCharacteristics::empty()
    }

    /// Shared memory operations (for WAL)
    fn shm_map(&self, region: i32, size: i32, extend: bool) -> Result<*mut u8>;
    fn shm_lock(&self, offset: i32, n: i32, flags: ShmLockFlags) -> Result<()>;
    fn shm_barrier(&self);
    fn shm_unmap(&self, delete: bool) -> Result<()>;

    /// Memory-mapped I/O
    fn fetch(&self, offset: i64, amount: i32) -> Result<Option<&[u8]>>;
    fn unfetch(&self, offset: i64, data: &[u8]) -> Result<()>;
}

#[derive(Debug, Clone, Copy)]
pub enum LockType {
    None = 0,
    Shared = 1,
    Reserved = 2,
    Pending = 3,
    Exclusive = 4,
}

bitflags! {
    pub struct SyncFlags: u32 {
        const NORMAL = 0x00002;
        const FULL = 0x00003;
        const DATAONLY = 0x00010;
    }
}

bitflags! {
    pub struct DeviceCharacteristics: u32 {
        const ATOMIC = 0x00000001;
        const ATOMIC512 = 0x00000002;
        const ATOMIC1K = 0x00000004;
        const ATOMIC2K = 0x00000008;
        const ATOMIC4K = 0x00000010;
        const ATOMIC8K = 0x00000020;
        const ATOMIC16K = 0x00000040;
        const ATOMIC32K = 0x00000080;
        const ATOMIC64K = 0x00000100;
        const SAFE_APPEND = 0x00000200;
        const SEQUENTIAL = 0x00000400;
        const UNDELETABLE_WHEN_OPEN = 0x00000800;
        const POWERSAFE_OVERWRITE = 0x00001000;
        const IMMUTABLE = 0x00002000;
        const BATCH_ATOMIC = 0x00004000;
    }
}

bitflags! {
    pub struct ShmLockFlags: u32 {
        const UNLOCK = 1;
        const LOCK = 2;
        const SHARED = 4;
        const EXCLUSIVE = 8;
    }
}
```

### File Control Operations
```rust
pub enum FileControlOp {
    LockState,
    GetLockProxyFile,
    SetLockProxyFile(String),
    LastErrno,
    SizeHint(i64),
    ChunkSize(i32),
    FilePointer,
    SyncOmitted,
    WinAvl,
    PersistWal(bool),
    OverWrite,
    VfsPointer,
    TempFilename,
    MmapSize(i64),
    TraceFile(bool),
    HasMoved,
    Sync,
    CommitPhaseTwoBegin,
    CommitPhaseTwo,
    LockTimeout(i32),
    DataVersion,
    SizeLimit(i64),
    CkptDone,
    ReserveByte(i32),
    GetLastError,
    Custom(i32, *mut ()),
}
```

## VFS Registry

```rust
/// Global VFS registry
pub struct VfsRegistry {
    vfs_list: Vec<Arc<dyn Vfs>>,
    default_vfs: Option<Arc<dyn Vfs>>,
}

impl VfsRegistry {
    pub fn new() -> Self {
        Self {
            vfs_list: Vec::new(),
            default_vfs: None,
        }
    }

    /// Register a VFS
    pub fn register(&mut self, vfs: Arc<dyn Vfs>, make_default: bool) -> Result<()> {
        // Remove existing with same name
        self.vfs_list.retain(|v| v.name() != vfs.name());

        if make_default || self.default_vfs.is_none() {
            self.default_vfs = Some(vfs.clone());
        }

        self.vfs_list.push(vfs);
        Ok(())
    }

    /// Unregister a VFS
    pub fn unregister(&mut self, name: &str) -> Result<()> {
        self.vfs_list.retain(|v| v.name() != name);
        if self.default_vfs.as_ref().map(|v| v.name()) == Some(name) {
            self.default_vfs = self.vfs_list.first().cloned();
        }
        Ok(())
    }

    /// Find VFS by name
    pub fn find(&self, name: Option<&str>) -> Option<Arc<dyn Vfs>> {
        match name {
            None => self.default_vfs.clone(),
            Some(name) => self.vfs_list.iter()
                .find(|v| v.name() == name)
                .cloned(),
        }
    }
}

lazy_static! {
    static ref VFS_REGISTRY: Mutex<VfsRegistry> = Mutex::new(VfsRegistry::new());
}

pub fn vfs_find(name: Option<&str>) -> Option<Arc<dyn Vfs>> {
    VFS_REGISTRY.lock().unwrap().find(name)
}

pub fn vfs_register(vfs: Arc<dyn Vfs>, make_default: bool) -> Result<()> {
    VFS_REGISTRY.lock().unwrap().register(vfs, make_default)
}
```

## OS Layer Functions

```rust
/// Initialize OS layer
pub fn os_init() -> Result<()> {
    // Platform-specific initialization
    #[cfg(unix)]
    {
        let unix_vfs = Arc::new(UnixVfs::new());
        vfs_register(unix_vfs, true)?;
    }

    #[cfg(windows)]
    {
        let win_vfs = Arc::new(WinVfs::new());
        vfs_register(win_vfs, true)?;
    }

    Ok(())
}

/// Clean up OS layer
pub fn os_end() -> Result<()> {
    // Cleanup
    Ok(())
}

/// Open a file
pub fn os_open(
    vfs: &dyn Vfs,
    path: Option<&str>,
    flags: OpenFlags,
) -> Result<Box<dyn VfsFile>> {
    vfs.open(path, flags)
}

/// Delete a file
pub fn os_delete(vfs: &dyn Vfs, path: &str, sync_dir: bool) -> Result<()> {
    vfs.delete(path, sync_dir)
}

/// Check file access
pub fn os_access(vfs: &dyn Vfs, path: &str, flags: AccessFlags) -> Result<bool> {
    vfs.access(path, flags)
}

/// Get full pathname
pub fn os_full_pathname(vfs: &dyn Vfs, path: &str) -> Result<String> {
    vfs.full_pathname(path)
}

/// Sleep
pub fn os_sleep(vfs: &dyn Vfs, microseconds: i32) -> i32 {
    vfs.sleep(microseconds)
}

/// Current time
pub fn os_current_time(vfs: &dyn Vfs) -> f64 {
    vfs.current_time()
}
```

## Acceptance Criteria
- [ ] VFS trait definition
- [ ] VfsFile trait definition
- [ ] VFS registry (find, register, unregister)
- [ ] OpenFlags complete set
- [ ] Lock types (None, Shared, Reserved, Pending, Exclusive)
- [ ] File control operations
- [ ] Device characteristics flags
- [ ] Shared memory operations for WAL
- [ ] Platform initialization hooks
- [ ] Time functions (current_time, current_time_i64)
- [ ] Random number generation
- [ ] Sleep function
