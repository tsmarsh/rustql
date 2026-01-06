//! Global configuration (sqlite3_config, sqlite3_db_config)
//!
//! This module handles library-wide and per-connection configuration settings.

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::RwLock;

use crate::error::{Error, ErrorCode, Result};
use crate::os::mutex;

// ============================================================================
// Threading Mode
// ============================================================================

/// Threading mode (SQLITE_CONFIG_SINGLETHREAD, etc.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum ThreadingMode {
    /// Single-threaded mode - no mutex support
    SingleThread = 1,
    /// Multi-threaded mode - each connection used by single thread
    MultiThread = 2,
    /// Serialized mode - full mutex support (default)
    #[default]
    Serialized = 3,
}

// ============================================================================
// Global Configuration
// ============================================================================

/// Global SQLite configuration state
pub struct GlobalConfig {
    /// Library has been initialized
    pub is_init: AtomicBool,
    /// Initialization in progress
    pub in_progress: AtomicBool,
    /// Threading mode
    pub threading_mode: RwLock<ThreadingMode>,
    /// Enable URI filenames
    pub uri: AtomicBool,
    /// Default page cache size (-2000 = 2000 pages)
    pub page_cache_size: AtomicI32,
    /// Default page size
    pub page_size: AtomicI32,
    /// Memory status tracking enabled
    pub mem_status: AtomicBool,
    /// Lookaside slot size
    pub lookaside_size: AtomicI32,
    /// Lookaside slot count
    pub lookaside_count: AtomicI32,
    /// Soft heap limit (bytes)
    pub soft_heap_limit: AtomicI64,
    /// Enable covering index scan
    pub covering_index_scan: AtomicBool,
    /// Small malloc threshold
    pub small_malloc: AtomicI32,
    /// Statement journal spill threshold
    pub stmtjrnl_spill: AtomicI32,
    /// Sorter reference size
    pub sorter_ref_size: AtomicI32,
    /// Default mmap size
    pub mmap_size: AtomicI64,
    /// Maximum mmap size
    pub max_mmap_size: AtomicI64,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            is_init: AtomicBool::new(false),
            in_progress: AtomicBool::new(false),
            threading_mode: RwLock::new(ThreadingMode::Serialized),
            uri: AtomicBool::new(true),
            page_cache_size: AtomicI32::new(-2000),
            page_size: AtomicI32::new(4096),
            mem_status: AtomicBool::new(true),
            lookaside_size: AtomicI32::new(1200),
            lookaside_count: AtomicI32::new(100),
            soft_heap_limit: AtomicI64::new(0),
            covering_index_scan: AtomicBool::new(true),
            small_malloc: AtomicI32::new(128),
            stmtjrnl_spill: AtomicI32::new(64 * 1024),
            sorter_ref_size: AtomicI32::new(0x7fffffff),
            mmap_size: AtomicI64::new(0),
            max_mmap_size: AtomicI64::new(0x7fff0000),
        }
    }
}

/// Global configuration singleton
static GLOBAL_CONFIG: std::sync::OnceLock<GlobalConfig> = std::sync::OnceLock::new();

/// Get the global configuration
pub fn global_config() -> &'static GlobalConfig {
    GLOBAL_CONFIG.get_or_init(GlobalConfig::default)
}

// ============================================================================
// Configuration Options
// ============================================================================

/// sqlite3_config option codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ConfigOption {
    /// SQLITE_CONFIG_SINGLETHREAD
    SingleThread = 1,
    /// SQLITE_CONFIG_MULTITHREAD
    MultiThread = 2,
    /// SQLITE_CONFIG_SERIALIZED
    Serialized = 3,
    /// SQLITE_CONFIG_MEMSTATUS
    MemStatus = 9,
    /// SQLITE_CONFIG_LOOKASIDE
    Lookaside = 13,
    /// SQLITE_CONFIG_URI
    Uri = 17,
    /// SQLITE_CONFIG_COVERING_INDEX_SCAN
    CoveringIndexScan = 20,
    /// SQLITE_CONFIG_MMAP_SIZE
    MmapSize = 22,
    /// SQLITE_CONFIG_STMTJRNL_SPILL
    StmtJrnlSpill = 26,
    /// SQLITE_CONFIG_SMALL_MALLOC
    SmallMalloc = 27,
    /// SQLITE_CONFIG_SORTERREF_SIZE
    SorterRefSize = 28,
}

/// Database configuration option codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum DbConfigOption {
    /// SQLITE_DBCONFIG_MAINDBNAME
    MainDbName = 1000,
    /// SQLITE_DBCONFIG_LOOKASIDE
    Lookaside = 1001,
    /// SQLITE_DBCONFIG_ENABLE_FKEY
    EnableFKey = 1002,
    /// SQLITE_DBCONFIG_ENABLE_TRIGGER
    EnableTrigger = 1003,
    /// SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER
    EnableFts3Tokenizer = 1004,
    /// SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION
    EnableLoadExtension = 1005,
    /// SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE
    NoCkptOnClose = 1006,
    /// SQLITE_DBCONFIG_ENABLE_QPSG
    EnableQpsg = 1007,
    /// SQLITE_DBCONFIG_TRIGGER_EQP
    TriggerEqp = 1008,
    /// SQLITE_DBCONFIG_RESET_DATABASE
    ResetDatabase = 1009,
    /// SQLITE_DBCONFIG_DEFENSIVE
    Defensive = 1010,
    /// SQLITE_DBCONFIG_WRITABLE_SCHEMA
    WritableSchema = 1011,
    /// SQLITE_DBCONFIG_LEGACY_ALTER_TABLE
    LegacyAlterTable = 1012,
    /// SQLITE_DBCONFIG_DQS_DML
    DqsDml = 1013,
    /// SQLITE_DBCONFIG_DQS_DDL
    DqsDdl = 1014,
    /// SQLITE_DBCONFIG_ENABLE_VIEW
    EnableView = 1015,
    /// SQLITE_DBCONFIG_LEGACY_FILE_FORMAT
    LegacyFileFormat = 1016,
    /// SQLITE_DBCONFIG_TRUSTED_SCHEMA
    TrustedSchema = 1017,
    /// SQLITE_DBCONFIG_STMT_SCANSTATUS
    StmtScanStatus = 1018,
    /// SQLITE_DBCONFIG_REVERSE_SCANORDER
    ReverseScanOrder = 1019,
}

// ============================================================================
// API Functions
// ============================================================================

/// sqlite3_config - Configure the library before initialization
///
/// Must be called before sqlite3_initialize() or after sqlite3_shutdown().
pub fn sqlite3_config(option: ConfigOption, value: i64) -> Result<()> {
    let config = global_config();

    // Cannot configure after initialization
    if config.is_init.load(Ordering::SeqCst) {
        return Err(Error::new(ErrorCode::Misuse));
    }

    match option {
        ConfigOption::SingleThread => {
            *config.threading_mode.write().unwrap() = ThreadingMode::SingleThread;
        }
        ConfigOption::MultiThread => {
            *config.threading_mode.write().unwrap() = ThreadingMode::MultiThread;
        }
        ConfigOption::Serialized => {
            *config.threading_mode.write().unwrap() = ThreadingMode::Serialized;
        }
        ConfigOption::MemStatus => {
            config.mem_status.store(value != 0, Ordering::SeqCst);
        }
        ConfigOption::Uri => {
            config.uri.store(value != 0, Ordering::SeqCst);
        }
        ConfigOption::CoveringIndexScan => {
            config
                .covering_index_scan
                .store(value != 0, Ordering::SeqCst);
        }
        ConfigOption::MmapSize => {
            config.mmap_size.store(value, Ordering::SeqCst);
        }
        ConfigOption::StmtJrnlSpill => {
            config.stmtjrnl_spill.store(value as i32, Ordering::SeqCst);
        }
        ConfigOption::SmallMalloc => {
            config.small_malloc.store(value as i32, Ordering::SeqCst);
        }
        ConfigOption::SorterRefSize => {
            config.sorter_ref_size.store(value as i32, Ordering::SeqCst);
        }
        ConfigOption::Lookaside => {
            // Takes two values but we only have one parameter here
            // In real implementation, we'd need a different signature
            config.lookaside_size.store(value as i32, Ordering::SeqCst);
        }
    }

    Ok(())
}

/// sqlite3_initialize - Initialize the SQLite library
///
/// This must be called before any other SQLite function, but is usually
/// called automatically by sqlite3_open().
pub fn sqlite3_initialize() -> Result<()> {
    let config = global_config();

    // Already initialized
    if config.is_init.load(Ordering::SeqCst) {
        return Ok(());
    }

    // Check for recursive initialization
    if config
        .in_progress
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Err(Error::new(ErrorCode::Misuse));
    }

    // Initialize subsystems
    // In a full implementation, this would initialize:
    // - OS interface (VFS)
    // - Memory allocator
    // - Mutex system
    // - Page cache
    // - etc.
    mutex::mutex_init();

    config.is_init.store(true, Ordering::SeqCst);
    config.in_progress.store(false, Ordering::SeqCst);

    Ok(())
}

/// sqlite3_shutdown - Shutdown the SQLite library
///
/// Deallocates resources allocated by sqlite3_initialize().
/// Should be called when done using SQLite.
pub fn sqlite3_shutdown() -> Result<()> {
    let config = global_config();

    if !config.is_init.load(Ordering::SeqCst) {
        return Ok(());
    }

    // Shutdown subsystems
    // In a full implementation, this would cleanup:
    // - Registered VFS implementations
    // - Memory allocator
    // - Mutex system
    // - etc.
    mutex::mutex_end();

    config.is_init.store(false, Ordering::SeqCst);

    Ok(())
}

// ============================================================================
// Version Information
// ============================================================================

/// SQLite version string
pub const SQLITE_VERSION: &str = "3.46.0";

/// SQLite version number (major * 1000000 + minor * 1000 + patch)
pub const SQLITE_VERSION_NUMBER: i32 = 3046000;

/// sqlite3_libversion - Get version string
pub fn sqlite3_libversion() -> &'static str {
    SQLITE_VERSION
}

/// sqlite3_libversion_number - Get version number
pub fn sqlite3_libversion_number() -> i32 {
    SQLITE_VERSION_NUMBER
}

/// sqlite3_threadsafe - Get threading mode
///
/// Returns:
/// - 0: Single-threaded
/// - 1: Multi-threaded
/// - 2: Serialized
pub fn sqlite3_threadsafe() -> i32 {
    let config = global_config();
    match *config.threading_mode.read().unwrap() {
        ThreadingMode::SingleThread => 0,
        ThreadingMode::MultiThread => 1,
        ThreadingMode::Serialized => 2,
    }
}

/// sqlite3_sourceid - Source control version identifier
pub fn sqlite3_sourceid() -> &'static str {
    // In a real implementation, this would be set during build
    concat!(env!("CARGO_PKG_VERSION"), " (rustql)")
}

/// sqlite3_compileoption_used - Check if compile option was used
pub fn sqlite3_compileoption_used(option: &str) -> bool {
    // RustQL-specific compile options
    matches!(
        option,
        "THREADSAFE" | "ENABLE_FTS5" | "ENABLE_JSON1" | "ENABLE_RTREE"
    )
}

/// sqlite3_compileoption_get - Get compile option by index
pub fn sqlite3_compileoption_get(idx: i32) -> Option<&'static str> {
    match idx {
        0 => Some("THREADSAFE=2"),
        1 => Some("ENABLE_FTS5"),
        2 => Some("ENABLE_JSON1"),
        3 => Some("ENABLE_RTREE"),
        _ => None,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert_eq!(sqlite3_libversion(), "3.46.0");
        assert_eq!(sqlite3_libversion_number(), 3046000);
    }

    #[test]
    fn test_threadsafe() {
        // Default is serialized (2)
        let ts = sqlite3_threadsafe();
        assert!(ts >= 0 && ts <= 2);
    }

    #[test]
    fn test_compileoption() {
        assert!(sqlite3_compileoption_used("THREADSAFE"));
        assert!(!sqlite3_compileoption_used("NONEXISTENT_OPTION"));
    }

    #[test]
    fn test_compileoption_get() {
        assert!(sqlite3_compileoption_get(0).is_some());
        assert!(sqlite3_compileoption_get(100).is_none());
    }
}
