//! Windows VFS implementation
//!
//! This module provides the Windows-specific VFS implementation.
//! The full implementation is tracked in a separate moth issue.

use crate::error::Result;

/// Register the Windows VFS with the global registry
///
/// This is called during os_init() to register the default Windows VFS.
/// The full implementation will be added in the os_win.c translation moth.
pub fn register_windows_vfs() -> Result<()> {
    // Stub: Full implementation will be added in the Windows VFS moth
    // For now, we don't register anything - this allows the code to compile
    // and tests to run without a concrete VFS implementation.
    Ok(())
}
