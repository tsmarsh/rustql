//! OS abstraction layer: VFS and platform implementations

pub mod vfs;

#[cfg(unix)]
pub mod unix;

#[cfg(windows)]
pub mod windows;
