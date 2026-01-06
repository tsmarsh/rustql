//! Virtual File System trait and types
//!
//! This module defines the VFS abstraction layer that provides platform-independent
//! file and OS operations, mirroring SQLite's os.c interface.

use crate::error::{Error, ErrorCode, Result};
use bitflags::bitflags;
use std::sync::{Arc, Mutex};

// ============================================================================
// Flags and Enums
// ============================================================================

bitflags! {
    /// Flags for opening files
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct OpenFlags: u32 {
        const READONLY         = 0x00000001;
        const READWRITE        = 0x00000002;
        const CREATE           = 0x00000004;
        const DELETEONCLOSE    = 0x00000008;
        const EXCLUSIVE        = 0x00000010;
        const AUTOPROXY        = 0x00000020;
        const URI              = 0x00000040;
        const MEMORY           = 0x00000080;
        const MAIN_DB          = 0x00000100;
        const TEMP_DB          = 0x00000200;
        const TRANSIENT_DB     = 0x00000400;
        const MAIN_JOURNAL     = 0x00000800;
        const TEMP_JOURNAL     = 0x00001000;
        const SUBJOURNAL       = 0x00002000;
        const SUPER_JOURNAL    = 0x00004000;
        const NOMUTEX          = 0x00008000;
        const FULLMUTEX        = 0x00010000;
        const SHAREDCACHE      = 0x00020000;
        const PRIVATECACHE     = 0x00040000;
        const WAL              = 0x00080000;
        const NOFOLLOW         = 0x01000000;
        const EXRESCODE        = 0x02000000;
    }
}

bitflags! {
    /// Flags for checking file access
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct AccessFlags: u32 {
        /// Check if file exists
        const EXISTS = 0;
        /// Check if file is readable and writable
        const READWRITE = 1;
        /// Check if file is readable
        const READ = 2;
    }
}

bitflags! {
    /// Flags for file sync operations
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct SyncFlags: u32 {
        const NORMAL   = 0x00002;
        const FULL     = 0x00003;
        const DATAONLY = 0x00010;
    }
}

bitflags! {
    /// Device characteristics flags
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct DeviceCharacteristics: u32 {
        const ATOMIC                  = 0x00000001;
        const ATOMIC512               = 0x00000002;
        const ATOMIC1K                = 0x00000004;
        const ATOMIC2K                = 0x00000008;
        const ATOMIC4K                = 0x00000010;
        const ATOMIC8K                = 0x00000020;
        const ATOMIC16K               = 0x00000040;
        const ATOMIC32K               = 0x00000080;
        const ATOMIC64K               = 0x00000100;
        const SAFE_APPEND             = 0x00000200;
        const SEQUENTIAL              = 0x00000400;
        const UNDELETABLE_WHEN_OPEN   = 0x00000800;
        const POWERSAFE_OVERWRITE     = 0x00001000;
        const IMMUTABLE               = 0x00002000;
        const BATCH_ATOMIC            = 0x00004000;
    }
}

bitflags! {
    /// Flags for shared memory lock operations
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct ShmLockFlags: u32 {
        const UNLOCK    = 1;
        const LOCK      = 2;
        const SHARED    = 4;
        const EXCLUSIVE = 8;
    }
}

/// File lock types (from SQLite's lock state machine)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(i32)]
pub enum LockType {
    /// No lock held
    #[default]
    None = 0,
    /// Shared lock (multiple readers)
    Shared = 1,
    /// Reserved lock (intend to write)
    Reserved = 2,
    /// Pending lock (waiting for exclusive)
    Pending = 3,
    /// Exclusive lock (single writer)
    Exclusive = 4,
}

/// File control operations
#[derive(Debug)]
pub enum FileControlOp {
    /// Get current lock state
    LockState,
    /// Get lock proxy file path
    GetLockProxyFile,
    /// Set lock proxy file path
    SetLockProxyFile(String),
    /// Get last OS error number
    LastErrno,
    /// Hint about expected file size
    SizeHint(i64),
    /// Set chunk size for incremental vacuum
    ChunkSize(i32),
    /// Get underlying file pointer
    FilePointer,
    /// Sync was omitted
    SyncOmitted,
    /// Windows AVL hint
    WinAvl,
    /// Persist WAL file after close
    PersistWal(bool),
    /// File was overwritten
    OverWrite,
    /// Get VFS pointer
    VfsPointer,
    /// Get temp filename
    TempFilename,
    /// Set memory-mapped I/O size
    MmapSize(i64),
    /// Enable/disable file tracing
    TraceFile(bool),
    /// Check if file has moved
    HasMoved,
    /// Force sync
    Sync,
    /// Begin phase two of commit
    CommitPhaseTwoBegin,
    /// Complete phase two of commit
    CommitPhaseTwo,
    /// Set lock timeout in milliseconds
    LockTimeout(i32),
    /// Get data version
    DataVersion,
    /// Set size limit
    SizeLimit(i64),
    /// Checkpoint done
    CkptDone,
    /// Reserve byte
    ReserveByte(i32),
    /// Get last error details
    GetLastError,
    /// Custom file control operation
    Custom(i32, *mut ()),
}

// ============================================================================
// VFS File Trait
// ============================================================================

/// File handle abstraction
///
/// This trait defines the interface for file operations that SQLite performs.
/// Each VFS implementation provides a concrete type implementing this trait.
pub trait VfsFile: Send + Sync {
    /// Read from file at the given offset
    fn read(&self, buf: &mut [u8], offset: i64) -> Result<usize>;

    /// Write to file at the given offset
    fn write(&self, buf: &[u8], offset: i64) -> Result<usize>;

    /// Truncate file to the given size
    fn truncate(&self, size: i64) -> Result<()>;

    /// Sync file to disk
    fn sync(&self, flags: SyncFlags) -> Result<()>;

    /// Get file size
    fn file_size(&self) -> Result<i64>;

    /// Acquire a file lock
    fn lock(&self, lock_type: LockType) -> Result<()>;

    /// Release a file lock
    fn unlock(&self, lock_type: LockType) -> Result<()>;

    /// Check if a reserved lock is held by another connection
    fn check_reserved_lock(&self) -> Result<bool>;

    /// File control operations
    fn file_control(&mut self, op: FileControlOp) -> Result<()>;

    /// Get sector size for this file
    fn sector_size(&self) -> i32 {
        4096
    }

    /// Get device characteristics
    fn device_characteristics(&self) -> DeviceCharacteristics {
        DeviceCharacteristics::empty()
    }

    /// Map shared memory region (for WAL)
    fn shm_map(&self, _region: i32, _size: i32, _extend: bool) -> Result<*mut u8> {
        Err(Error::new(ErrorCode::IoErr))
    }

    /// Lock shared memory region
    fn shm_lock(&self, _offset: i32, _n: i32, _flags: ShmLockFlags) -> Result<()> {
        Err(Error::new(ErrorCode::IoErr))
    }

    /// Shared memory barrier
    fn shm_barrier(&self) {}

    /// Unmap shared memory
    fn shm_unmap(&self, _delete: bool) -> Result<()> {
        Ok(())
    }

    /// Fetch memory-mapped region
    fn fetch(&self, _offset: i64, _amount: i32) -> Result<Option<*const u8>> {
        Ok(None)
    }

    /// Release memory-mapped region
    fn unfetch(&self, _offset: i64, _data: *const u8) -> Result<()> {
        Ok(())
    }
}

// ============================================================================
// VFS Trait
// ============================================================================

/// Virtual File System - platform abstraction
///
/// This trait defines the interface for platform-specific file system operations.
/// SQLite uses this abstraction to remain portable across different operating systems.
pub trait Vfs: Send + Sync {
    /// VFS name (e.g., "unix", "win32")
    fn name(&self) -> &str;

    /// Maximum pathname length supported
    fn max_pathname(&self) -> i32 {
        1024
    }

    /// Open a file
    fn open(&self, path: Option<&str>, flags: OpenFlags) -> Result<Box<dyn VfsFile>>;

    /// Delete a file
    fn delete(&self, path: &str, sync_dir: bool) -> Result<()>;

    /// Check if file exists/is accessible
    fn access(&self, path: &str, flags: AccessFlags) -> Result<bool>;

    /// Get full pathname from relative path
    fn full_pathname(&self, path: &str) -> Result<String>;

    /// Open a dynamic library
    fn dlopen(&self, _path: &str) -> Result<*mut ()> {
        Err(Error::with_message(
            ErrorCode::Error,
            "Dynamic loading not supported",
        ))
    }

    /// Get last dynamic library error
    fn dlerror(&self) -> String {
        String::new()
    }

    /// Get symbol from dynamic library
    fn dlsym(&self, _handle: *mut (), _symbol: &str) -> Option<*mut ()> {
        None
    }

    /// Close dynamic library
    fn dlclose(&self, _handle: *mut ()) {}

    /// Fill buffer with random bytes
    fn randomness(&self, buf: &mut [u8]) -> i32;

    /// Sleep for specified microseconds, returns actual sleep time
    fn sleep(&self, microseconds: i32) -> i32;

    /// Get current time as Julian day number
    fn current_time(&self) -> f64;

    /// Get current time with higher precision (milliseconds since Unix epoch * 86400000)
    fn current_time_i64(&self) -> i64;

    /// Get last OS error
    fn get_last_error(&self) -> (i32, String);

    /// Set a system call replacement
    fn set_system_call(&mut self, _name: &str, _ptr: *const ()) -> Result<()> {
        Err(Error::new(ErrorCode::NotFound))
    }

    /// Get current system call pointer
    fn get_system_call(&self, _name: &str) -> Option<*const ()> {
        None
    }

    /// Get next system call name in iteration
    fn next_system_call(&self, _name: &str) -> Option<&str> {
        None
    }
}

// ============================================================================
// VFS Registry
// ============================================================================

/// Global VFS registry
pub struct VfsRegistry {
    /// List of registered VFS implementations
    vfs_list: Vec<Arc<dyn Vfs>>,
    /// Default VFS to use when none specified
    default_vfs: Option<Arc<dyn Vfs>>,
}

impl VfsRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            vfs_list: Vec::new(),
            default_vfs: None,
        }
    }

    /// Register a VFS implementation
    pub fn register(&mut self, vfs: Arc<dyn Vfs>, make_default: bool) -> Result<()> {
        let name = vfs.name().to_string();

        // Remove existing VFS with same name
        self.vfs_list.retain(|v| v.name() != name);

        // Update default if requested or if this is the first VFS
        if make_default || self.default_vfs.is_none() {
            self.default_vfs = Some(vfs.clone());
        }

        self.vfs_list.push(vfs);
        Ok(())
    }

    /// Unregister a VFS by name
    pub fn unregister(&mut self, name: &str) -> Result<()> {
        let was_default = self
            .default_vfs
            .as_ref()
            .map(|v| v.name() == name)
            .unwrap_or(false);

        self.vfs_list.retain(|v| v.name() != name);

        // If we removed the default, pick a new one
        if was_default {
            self.default_vfs = self.vfs_list.first().cloned();
        }

        Ok(())
    }

    /// Find a VFS by name, or return default if name is None
    pub fn find(&self, name: Option<&str>) -> Option<Arc<dyn Vfs>> {
        match name {
            None => self.default_vfs.clone(),
            Some(name) => self.vfs_list.iter().find(|v| v.name() == name).cloned(),
        }
    }
}

impl Default for VfsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// Global VFS registry instance
lazy_static::lazy_static! {
    static ref VFS_REGISTRY: Mutex<VfsRegistry> = Mutex::new(VfsRegistry::new());
}

// ============================================================================
// Public API Functions
// ============================================================================

/// Find a VFS by name (or return default)
pub fn vfs_find(name: Option<&str>) -> Option<Arc<dyn Vfs>> {
    VFS_REGISTRY.lock().unwrap().find(name)
}

/// Register a VFS
pub fn vfs_register(vfs: Arc<dyn Vfs>, make_default: bool) -> Result<()> {
    VFS_REGISTRY.lock().unwrap().register(vfs, make_default)
}

/// Unregister a VFS by name
pub fn vfs_unregister(name: &str) -> Result<()> {
    VFS_REGISTRY.lock().unwrap().unregister(name)
}

// ============================================================================
// OS Layer Functions
// ============================================================================

/// Initialize the OS layer
pub fn os_init() -> Result<()> {
    // Platform-specific initialization happens in the platform modules
    #[cfg(unix)]
    {
        crate::os::unix::register_unix_vfs()?;
    }

    #[cfg(windows)]
    {
        crate::os::windows::register_windows_vfs()?;
    }

    Ok(())
}

/// Clean up the OS layer
pub fn os_end() -> Result<()> {
    // Currently nothing to clean up
    Ok(())
}

/// Open a file using a VFS
pub fn os_open(vfs: &dyn Vfs, path: Option<&str>, flags: OpenFlags) -> Result<Box<dyn VfsFile>> {
    vfs.open(path, flags)
}

/// Delete a file using a VFS
pub fn os_delete(vfs: &dyn Vfs, path: &str, sync_dir: bool) -> Result<()> {
    vfs.delete(path, sync_dir)
}

/// Check file access using a VFS
pub fn os_access(vfs: &dyn Vfs, path: &str, flags: AccessFlags) -> Result<bool> {
    vfs.access(path, flags)
}

/// Get full pathname using a VFS
pub fn os_full_pathname(vfs: &dyn Vfs, path: &str) -> Result<String> {
    vfs.full_pathname(path)
}

/// Sleep using a VFS
pub fn os_sleep(vfs: &dyn Vfs, microseconds: i32) -> i32 {
    vfs.sleep(microseconds)
}

/// Get current time using a VFS
pub fn os_current_time(vfs: &dyn Vfs) -> f64 {
    vfs.current_time()
}

/// Get current time with high precision using a VFS
pub fn os_current_time_i64(vfs: &dyn Vfs) -> i64 {
    vfs.current_time_i64()
}

/// Get random bytes using a VFS
pub fn os_randomness(vfs: &dyn Vfs, buf: &mut [u8]) -> i32 {
    vfs.randomness(buf)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_flags() {
        let flags = OpenFlags::READWRITE | OpenFlags::CREATE;
        assert!(flags.contains(OpenFlags::READWRITE));
        assert!(flags.contains(OpenFlags::CREATE));
        assert!(!flags.contains(OpenFlags::READONLY));
    }

    #[test]
    fn test_lock_type_ordering() {
        assert!(LockType::None < LockType::Shared);
        assert!(LockType::Shared < LockType::Reserved);
        assert!(LockType::Reserved < LockType::Pending);
        assert!(LockType::Pending < LockType::Exclusive);
    }

    #[test]
    fn test_sync_flags() {
        let flags = SyncFlags::FULL | SyncFlags::DATAONLY;
        assert!(flags.contains(SyncFlags::FULL));
        assert!(flags.contains(SyncFlags::DATAONLY));
    }

    #[test]
    fn test_device_characteristics() {
        let chars = DeviceCharacteristics::ATOMIC4K | DeviceCharacteristics::SAFE_APPEND;
        assert!(chars.contains(DeviceCharacteristics::ATOMIC4K));
        assert!(chars.contains(DeviceCharacteristics::SAFE_APPEND));
        assert!(!chars.contains(DeviceCharacteristics::IMMUTABLE));
    }

    #[test]
    fn test_vfs_registry() {
        // Create a mock registry for testing (not using global)
        let registry = VfsRegistry::new();
        assert!(registry.find(None).is_none());
    }
}
