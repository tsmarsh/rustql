//! OS abstraction layer: VFS and platform implementations
//!
//! This module provides the Virtual File System abstraction that allows
//! SQLite to work across different operating systems.

pub mod mutex;
pub mod vfs;

#[cfg(unix)]
pub mod unix;

#[cfg(windows)]
pub mod windows;

// Re-export main VFS types and functions
pub use vfs::{
    os_access, os_current_time, os_current_time_i64, os_delete, os_end, os_full_pathname, os_init,
    os_open, os_randomness, os_sleep, vfs_find, vfs_register, vfs_unregister, AccessFlags,
    DeviceCharacteristics, FileControlOp, LockType, OpenFlags, ShmLockFlags, SyncFlags, Vfs,
    VfsFile,
};
