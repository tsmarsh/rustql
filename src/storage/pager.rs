//! Page cache management
//!
//! The Pager module manages the page cache, handles database I/O, journaling,
//! and transaction durability. This is the layer between the B-tree and the
//! OS file system.

use bitflags::bitflags;
use std::ptr::NonNull;

use crate::error::{Error, ErrorCode, Result};
use crate::storage::pcache::PCache;
use crate::storage::wal::{CheckpointMode, Wal};
use crate::types::{LockLevel, OpenFlags, Pgno, SyncFlags, Vfs, VfsFile};

// ============================================================================
// Constants
// ============================================================================

/// Default maximum size for persistent journal files (-1 = no limit)
pub const DEFAULT_JOURNAL_SIZE_LIMIT: i64 = -1;

/// Journal header magic number
pub const JOURNAL_MAGIC: [u8; 8] = [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7];

/// Size of the journal header in bytes
pub const JOURNAL_HEADER_SIZE: usize = 28;

/// Default page size
pub const DEFAULT_PAGE_SIZE: u32 = 4096;

/// Minimum page size
pub const MIN_PAGE_SIZE: u32 = 512;

/// Maximum page size
pub const MAX_PAGE_SIZE: u32 = 65536;

// ============================================================================
// Pager Flags
// ============================================================================

bitflags! {
    /// Flags for sqlite3PagerOpen()
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PagerOpenFlags: u32 {
        /// Do not use a rollback journal
        const OMIT_JOURNAL = 0x0001;
        /// In-memory database
        const MEMORY = 0x0002;
    }

    /// Flags for sqlite3PagerGet()
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PagerGetFlags: u8 {
        /// Do not load data from disk
        const NOCONTENT = 0x01;
        /// Read-only page is acceptable
        const READONLY = 0x02;
    }

    /// Flags for sqlite3PagerSetFlags() - synchronous mode
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PagerFlags: u32 {
        /// PRAGMA synchronous=OFF
        const SYNCHRONOUS_OFF = 0x01;
        /// PRAGMA synchronous=NORMAL
        const SYNCHRONOUS_NORMAL = 0x02;
        /// PRAGMA synchronous=FULL
        const SYNCHRONOUS_FULL = 0x03;
        /// PRAGMA synchronous=EXTRA
        const SYNCHRONOUS_EXTRA = 0x04;
        /// Mask for synchronous values
        const SYNCHRONOUS_MASK = 0x07;
        /// PRAGMA fullfsync=ON
        const FULLFSYNC = 0x08;
        /// PRAGMA checkpoint_fullfsync=ON
        const CKPT_FULLFSYNC = 0x10;
        /// PRAGMA cache_spill=ON
        const CACHESPILL = 0x20;
    }

    /// Page state flags
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PgFlags: u8 {
        /// Page is clean (unmodified)
        const CLEAN = 0x00;
        /// Page has been modified
        const DIRTY = 0x01;
        /// Do not write this page
        const DONT_WRITE = 0x02;
        /// Page needs sync before commit
        const NEED_SYNC = 0x04;
        /// Page is writeable
        const WRITEABLE = 0x08;
    }
}

// ============================================================================
// Enums
// ============================================================================

/// Pager state machine states
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
pub enum PagerState {
    /// No lock held, pager is open
    Open = 0,
    /// Shared lock held, can read
    Reader = 1,
    /// Reserved lock held, writing to journal
    Writer = 2,
    /// Exclusive lock held, committing
    WriterLocked = 3,
    /// Commit complete, releasing locks
    WriterFinished = 4,
    /// Error occurred, pager is in error state
    Error = 5,
}

/// Journal mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum JournalMode {
    /// Commit by deleting journal file
    Delete = 0,
    /// Commit by zeroing journal header
    Persist = 1,
    /// Journal omitted (unsafe)
    Off = 2,
    /// Commit by truncating journal to zero
    Truncate = 3,
    /// In-memory journal file
    Memory = 4,
    /// Use write-ahead logging
    Wal = 5,
}

impl JournalMode {
    /// Check if this is WAL mode
    pub fn is_wal(&self) -> bool {
        matches!(self, JournalMode::Wal)
    }
}

/// Locking mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum LockingMode {
    /// Normal locking - release locks after transaction
    Normal = 0,
    /// Exclusive locking - hold exclusive lock
    Exclusive = 1,
}

/// Savepoint operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SavepointOp {
    /// Begin a new savepoint
    Begin,
    /// Release (commit) savepoint
    Release,
    /// Rollback to savepoint
    Rollback,
}

// ============================================================================
// Page Header
// ============================================================================

/// Page header for cached pages (DbPage in SQLite)
pub struct PgHdr {
    /// Page number (1-indexed, 0 means "not a page")
    pub pgno: Pgno,
    /// Page content data
    pub data: Vec<u8>,
    /// Page state flags
    pub flags: PgFlags,
    /// Reference count
    pub n_ref: u32,
    /// Link to pager that owns this page
    pub pager: Option<*mut Pager>,
    /// Hash collision chain for page cache
    pub hash_next: Option<Box<PgHdr>>,
    /// Dirty list links
    pub dirty_next: Option<Box<PgHdr>>,
    pub dirty_prev: Option<*mut PgHdr>,
}

impl PgHdr {
    /// Create a new page header
    pub fn new(pgno: Pgno, page_size: u32) -> Self {
        PgHdr {
            pgno,
            data: vec![0u8; page_size as usize],
            flags: PgFlags::CLEAN,
            n_ref: 0,
            pager: None,
            hash_next: None,
            dirty_next: None,
            dirty_prev: None,
        }
    }

    /// Check if page is dirty
    pub fn is_dirty(&self) -> bool {
        self.flags.contains(PgFlags::DIRTY)
    }

    /// Check if page is writeable
    pub fn is_writeable(&self) -> bool {
        self.flags.contains(PgFlags::WRITEABLE)
    }

    /// Get page data
    pub fn get_data(&self) -> &[u8] {
        &self.data
    }

    /// Get mutable page data
    pub fn get_data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl Drop for PgHdr {
    fn drop(&mut self) {
        // If this page is dirty and has a pager reference, write back to cache
        if self.is_dirty() {
            if let Some(pager_ptr) = self.pager {
                // SAFETY: pager_ptr is valid as long as the connection is open
                unsafe {
                    let pager = &mut *pager_ptr;
                    if let Some(mut cache_page) = pager.pcache.fetch(self.pgno, true) {
                        let cache_ref = cache_page.as_mut();
                        cache_ref.data[..self.data.len()].copy_from_slice(&self.data);
                    }
                }
            }
        }
    }
}

// ============================================================================
// Savepoint
// ============================================================================

/// Savepoint state
pub struct Savepoint {
    /// Offset in the journal
    pub offset: i64,
    /// Sub-journal offset
    pub sub_rec: u32,
    /// Original database size at savepoint
    pub orig_db_size: Pgno,
    /// Number of pages in savepoint
    pub n_orig: Pgno,
    /// Savepoint name hash
    pub name_hash: u32,
}

impl Savepoint {
    /// Create a new savepoint
    pub fn new(offset: i64, db_size: Pgno) -> Self {
        Savepoint {
            offset,
            sub_rec: 0,
            orig_db_size: db_size,
            n_orig: db_size,
            name_hash: 0,
        }
    }
}

// ============================================================================
// Journal Header
// ============================================================================

/// Rollback journal header (28 bytes)
#[derive(Debug, Clone)]
pub struct JournalHeader {
    /// Magic number (8 bytes)
    pub magic: [u8; 8],
    /// Page count in this segment
    pub page_count: u32,
    /// Random nonce for checksum
    pub nonce: u32,
    /// Initial database page count
    pub initial_pages: u32,
    /// Disk sector size
    pub sector_size: u32,
    /// Page size
    pub page_size: u32,
}

impl JournalHeader {
    /// Create a new journal header
    pub fn new(page_count: u32, initial_pages: u32, sector_size: u32, page_size: u32) -> Self {
        JournalHeader {
            magic: JOURNAL_MAGIC,
            page_count,
            nonce: rand_nonce(),
            initial_pages,
            sector_size,
            page_size,
        }
    }

    /// Parse a journal header from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < JOURNAL_HEADER_SIZE {
            return Err(Error::new(ErrorCode::Corrupt));
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&data[0..8]);

        if magic != JOURNAL_MAGIC {
            return Err(Error::new(ErrorCode::Corrupt));
        }

        Ok(JournalHeader {
            magic,
            page_count: u32::from_be_bytes([data[8], data[9], data[10], data[11]]),
            nonce: u32::from_be_bytes([data[12], data[13], data[14], data[15]]),
            initial_pages: u32::from_be_bytes([data[16], data[17], data[18], data[19]]),
            sector_size: u32::from_be_bytes([data[20], data[21], data[22], data[23]]),
            page_size: u32::from_be_bytes([data[24], data[25], data[26], data[27]]),
        })
    }

    /// Serialize journal header to bytes
    pub fn to_bytes(&self) -> [u8; JOURNAL_HEADER_SIZE] {
        let mut buf = [0u8; JOURNAL_HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.magic);
        buf[8..12].copy_from_slice(&self.page_count.to_be_bytes());
        buf[12..16].copy_from_slice(&self.nonce.to_be_bytes());
        buf[16..20].copy_from_slice(&self.initial_pages.to_be_bytes());
        buf[20..24].copy_from_slice(&self.sector_size.to_be_bytes());
        buf[24..28].copy_from_slice(&self.page_size.to_be_bytes());
        buf
    }
}

/// Generate a random nonce for journal checksum
fn rand_nonce() -> u32 {
    // Simple PRNG for now - in production use proper randomness
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (duration.as_nanos() & 0xFFFFFFFF) as u32
}

// ============================================================================
// VFS Operations
// ============================================================================

/// Callback type for opening files
pub type VfsOpenFn = Box<dyn Fn(&str, OpenFlags) -> Result<Box<dyn VfsFile>> + Send + Sync>;

/// Callback type for deleting files
pub type VfsDeleteFn = Box<dyn Fn(&str) -> Result<()> + Send + Sync>;

/// Callback type for checking file existence
pub type VfsAccessFn = Box<dyn Fn(&str) -> Result<bool> + Send + Sync>;

// ============================================================================
// Pager
// ============================================================================

/// Main pager object managing a database file
pub struct Pager {
    // File handles
    /// Database file handle
    pub fd: Option<Box<dyn VfsFile>>,
    /// Journal file handle
    pub jfd: Option<Box<dyn VfsFile>>,
    /// Sub-journal file handle
    pub sjfd: Option<Box<dyn VfsFile>>,

    // VFS operations
    /// Callback to open a file
    vfs_open: Option<VfsOpenFn>,
    /// Callback to delete a file
    vfs_delete: Option<VfsDeleteFn>,
    /// Callback to check file existence
    vfs_access: Option<VfsAccessFn>,

    // Paths
    /// Database file path
    pub db_path: String,
    /// Journal file path
    pub journal_path: String,

    // State
    /// Current pager state
    pub state: PagerState,
    /// Current lock level
    pub lock: LockLevel,
    /// Journal mode
    pub journal_mode: JournalMode,
    /// Locking mode
    pub locking_mode: LockingMode,
    /// Error code if in error state
    pub err_code: ErrorCode,

    // Page management
    /// Database page size
    pub page_size: u32,
    /// Usable bytes per page (page_size - reserved)
    pub usable_size: u32,
    /// Database size in pages
    pub db_size: Pgno,
    /// Original database size (at transaction start)
    pub db_orig_size: Pgno,
    /// Actual file size in pages
    pub db_file_size: Pgno,
    /// Maximum allowed page count
    pub max_page_count: Pgno,

    // Cache
    /// Page cache for in-memory page storage
    pub pcache: PCache,
    /// Page cache size (in pages)
    pub cache_size: i32,
    /// Spill size threshold
    pub spill_size: i32,
    /// Memory-mapped I/O limit
    pub mmap_limit: i64,

    // Journal state
    /// Current position in journal
    pub journal_offset: i64,
    /// Start of current journal header
    pub journal_header: i64,
    /// Records in current journal segment
    pub n_rec: u32,
    /// Journal size limit
    pub journal_size_limit: i64,
    /// Checksum nonce for this journal
    pub checksum_nonce: u32,

    // Stats
    /// Pages read from disk
    pub n_read: u32,
    /// Pages written to disk
    pub n_write: u32,
    /// Cache hits
    pub n_hit: u32,
    /// Cache misses
    pub n_miss: u32,

    // Options
    /// Pager flags (sync mode, etc.)
    pub flags: PagerFlags,
    /// Is this a temp database
    pub temp_file: bool,
    /// Is this an in-memory database
    pub mem_db: bool,
    /// Read-only database
    pub read_only: bool,
    /// Disable syncs (unsafe)
    pub no_sync: bool,

    // Savepoints
    /// Active savepoints
    pub savepoints: Vec<Savepoint>,

    // Sub-journal tracking
    /// Pages written to sub-journal
    pub sub_journal_pages: Vec<Pgno>,

    // In-memory journal for memory databases
    /// Journal records stored in memory (page number, original data)
    pub mem_journal: Vec<(Pgno, Vec<u8>)>,

    // WAL mode
    /// Write-ahead log (when in WAL mode)
    pub wal: Option<Wal>,

    // Temporary space
    /// Temporary buffer for page operations
    pub tmp_space: Vec<u8>,
}

impl Pager {
    // ========================================================================
    // Initialization
    // ========================================================================

    /// Open a pager on a database file (sqlite3PagerOpen)
    pub fn open<V: Vfs + Clone + 'static>(
        vfs: &V,
        path: &str,
        flags: PagerOpenFlags,
        vfs_flags: OpenFlags,
    ) -> Result<Self>
    where
        V::File: 'static,
    {
        let journal_path = format!("{}-journal", path);

        // Determine if this is an in-memory database
        let mem_db =
            flags.contains(PagerOpenFlags::MEMORY) || path.is_empty() || path == ":memory:";

        // Create page cache with default page size and no extra bytes
        let pcache = PCache::open(DEFAULT_PAGE_SIZE as usize, 0, true);

        // Try to open the database file (unless memory mode)
        let fd: Option<Box<dyn VfsFile>> = if !mem_db && !path.is_empty() {
            match vfs.open(path, vfs_flags) {
                Ok(file) => Some(Box::new(file)),
                Err(e) => {
                    // If file doesn't exist and we're not creating, that's okay
                    if !vfs_flags.contains(OpenFlags::CREATE) {
                        None
                    } else {
                        return Err(e);
                    }
                }
            }
        } else {
            None
        };

        // Get initial file size
        let (db_size, db_file_size) = if let Some(ref f) = fd {
            let size = f.file_size().unwrap_or(0);
            let pages = (size / DEFAULT_PAGE_SIZE as i64) as Pgno;
            (pages, pages)
        } else {
            (0, 0)
        };

        // Determine read-only status
        let read_only = vfs_flags.contains(OpenFlags::READONLY);

        // Create VFS callbacks for journal operations (unless memory-only mode)
        let (vfs_open, vfs_delete, vfs_access): (
            Option<VfsOpenFn>,
            Option<VfsDeleteFn>,
            Option<VfsAccessFn>,
        ) = if !mem_db {
            let vfs_for_open = vfs.clone();
            let vfs_for_delete = vfs.clone();
            let vfs_for_access = vfs.clone();

            let open_fn: VfsOpenFn = Box::new(move |path: &str, flags: OpenFlags| {
                let file = vfs_for_open.open(path, flags)?;
                Ok(Box::new(file) as Box<dyn VfsFile>)
            });

            let delete_fn: VfsDeleteFn =
                Box::new(move |path: &str| vfs_for_delete.delete(path, false));

            let access_fn: VfsAccessFn = Box::new(move |path: &str| {
                vfs_for_access.access(path, crate::types::AccessFlags::EXISTS)
            });

            (Some(open_fn), Some(delete_fn), Some(access_fn))
        } else {
            (None, None, None)
        };

        Ok(Pager {
            fd,
            jfd: None,
            sjfd: None,
            vfs_open,
            vfs_delete,
            vfs_access,
            db_path: path.to_string(),
            journal_path,
            state: PagerState::Open,
            lock: LockLevel::None,
            journal_mode: if flags.contains(PagerOpenFlags::OMIT_JOURNAL) {
                JournalMode::Off
            } else {
                JournalMode::Delete
            },
            locking_mode: LockingMode::Normal,
            err_code: ErrorCode::Ok,
            page_size: DEFAULT_PAGE_SIZE,
            usable_size: DEFAULT_PAGE_SIZE,
            db_size,
            db_orig_size: 0,
            db_file_size,
            max_page_count: 0xFFFFFFFF,
            pcache,
            cache_size: 2000,
            spill_size: 1,
            mmap_limit: 0,
            journal_offset: 0,
            journal_header: 0,
            n_rec: 0,
            journal_size_limit: DEFAULT_JOURNAL_SIZE_LIMIT,
            checksum_nonce: rand_nonce(),
            n_read: 0,
            n_write: 0,
            n_hit: 0,
            n_miss: 0,
            flags: PagerFlags::SYNCHRONOUS_FULL,
            temp_file: false,
            mem_db,
            read_only,
            no_sync: false,
            savepoints: Vec::new(),
            sub_journal_pages: Vec::new(),
            mem_journal: Vec::new(),
            wal: None,
            tmp_space: vec![0u8; DEFAULT_PAGE_SIZE as usize],
        })
    }

    /// Set VFS callbacks for file operations (call after open for non-memory databases)
    pub fn set_vfs_callbacks(
        &mut self,
        open_fn: VfsOpenFn,
        delete_fn: VfsDeleteFn,
        access_fn: VfsAccessFn,
    ) {
        self.vfs_open = Some(open_fn);
        self.vfs_delete = Some(delete_fn);
        self.vfs_access = Some(access_fn);
    }

    /// Close the pager and release resources (sqlite3PagerClose)
    pub fn close(&mut self) -> Result<()> {
        // Rollback any active transaction
        if self.state >= PagerState::Writer {
            let _ = self.rollback();
        }

        // Release locks
        self.unlock(LockLevel::None)?;

        // Close files
        self.fd = None;
        self.jfd = None;
        self.sjfd = None;

        self.state = PagerState::Open;
        Ok(())
    }

    /// Read the database file header (sqlite3PagerReadFileheader)
    pub fn read_file_header(&mut self, buf: &mut [u8]) -> Result<()> {
        if let Some(ref mut fd) = self.fd {
            let n = fd.read(buf, 0)?;
            // Zero out any unread portion
            if n < buf.len() {
                buf[n..].fill(0);
            }
            Ok(())
        } else {
            buf.fill(0);
            Ok(())
        }
    }

    // ========================================================================
    // Configuration
    // ========================================================================

    /// Set the page size (sqlite3PagerSetPagesize)
    pub fn set_page_size(&mut self, page_size: u32, reserve: i32) -> Result<()> {
        if !(MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&page_size) {
            return Err(Error::new(ErrorCode::Misuse));
        }
        if !page_size.is_power_of_two() {
            return Err(Error::new(ErrorCode::Misuse));
        }
        if self.state != PagerState::Open {
            return Err(Error::new(ErrorCode::Misuse));
        }

        self.page_size = page_size;
        let reserve = reserve.max(0) as u32;
        self.usable_size = page_size - reserve.min(page_size - 480);
        self.tmp_space = vec![0u8; page_size as usize];

        Ok(())
    }

    /// Get the current page size
    pub fn get_page_size(&self) -> u32 {
        self.page_size
    }

    /// Set maximum page count (sqlite3PagerMaxPageCount)
    pub fn set_max_page_count(&mut self, max: Pgno) -> Pgno {
        if max > 0 {
            self.max_page_count = max;
        }
        self.max_page_count
    }

    /// Set cache size (sqlite3PagerSetCachesize)
    pub fn set_cache_size(&mut self, size: i32) {
        self.cache_size = size;
    }

    /// Set spill size (sqlite3PagerSetSpillsize)
    pub fn set_spill_size(&mut self, size: i32) -> i32 {
        let old = self.spill_size;
        if size >= 0 {
            self.spill_size = size;
        }
        old
    }

    /// Set memory-mapped I/O limit (sqlite3PagerSetMmapLimit)
    pub fn set_mmap_limit(&mut self, limit: i64) {
        self.mmap_limit = limit;
    }

    /// Set pager flags (sqlite3PagerSetFlags)
    pub fn set_flags(&mut self, flags: PagerFlags) {
        self.flags = flags;
    }

    /// Get/set locking mode (sqlite3PagerLockingMode)
    pub fn locking_mode(&mut self, mode: Option<LockingMode>) -> LockingMode {
        if let Some(m) = mode {
            self.locking_mode = m;
        }
        self.locking_mode
    }

    /// Set journal mode (sqlite3PagerSetJournalMode)
    pub fn set_journal_mode(&mut self, mode: JournalMode) -> Result<JournalMode> {
        // Can't change journal mode during a transaction
        if self.state >= PagerState::Writer {
            return Ok(self.journal_mode);
        }
        self.journal_mode = mode;
        Ok(mode)
    }

    /// Get journal mode (sqlite3PagerGetJournalMode)
    pub fn get_journal_mode(&self) -> JournalMode {
        self.journal_mode
    }

    /// Set journal size limit (sqlite3PagerJournalSizeLimit)
    pub fn set_journal_size_limit(&mut self, limit: i64) -> i64 {
        if limit >= -1 {
            self.journal_size_limit = limit;
        }
        self.journal_size_limit
    }

    // ========================================================================
    // Page Acquisition
    // ========================================================================

    /// Get a page, reading from disk if needed (sqlite3PagerGet)
    pub fn get(&mut self, pgno: Pgno, _flags: PagerGetFlags) -> Result<Box<PgHdr>> {
        if pgno == 0 {
            return Err(Error::new(ErrorCode::Corrupt));
        }

        // Check if we have a shared lock
        if self.state < PagerState::Reader {
            self.shared_lock()?;
        }

        // Try to fetch from page cache first (create=true to allocate if not present)
        if let Some(cache_page) = self.pcache.fetch(pgno, true) {
            // Page is in cache - copy data to returned PgHdr
            let mut page = Box::new(PgHdr::new(pgno, self.page_size));
            // Set pager pointer so Drop can write back dirty pages
            page.pager = Some(self as *mut Pager);
            unsafe {
                let cache_page_ref = cache_page.as_ref();
                // Check if page has been read from disk (non-zero data)
                let is_new_page =
                    cache_page_ref.n_ref == 1 && cache_page_ref.data.iter().all(|&b| b == 0);

                if is_new_page {
                    // New page - read from disk if file is open
                    if let Some(ref mut fd) = self.fd {
                        let offset = ((pgno - 1) as i64) * (self.page_size as i64);
                        let _ = fd.read(&mut page.data, offset);
                        self.n_read += 1;
                    }
                    // Update cache with disk data
                    self.update_cache_page(cache_page, &page.data);
                } else {
                    // Existing page - copy from cache
                    page.data.copy_from_slice(&cache_page_ref.data);
                    self.n_hit += 1;
                }
            }
            page.n_ref = 1;
            return Ok(page);
        }

        // Fallback: create page without cache (shouldn't happen with create=true)
        let mut page = Box::new(PgHdr::new(pgno, self.page_size));
        // Set pager pointer so Drop can write back dirty pages
        page.pager = Some(self as *mut Pager);

        // Read from disk if file is open and page exists
        if let Some(ref mut fd) = self.fd {
            let offset = ((pgno - 1) as i64) * (self.page_size as i64);
            let _ = fd.read(&mut page.data, offset);
            self.n_read += 1;
        }

        page.n_ref = 1;
        self.n_miss += 1;
        Ok(page)
    }

    /// Update cached page data (internal helper)
    fn update_cache_page(
        &mut self,
        mut cache_page: NonNull<crate::storage::pcache::PgHdr>,
        data: &[u8],
    ) {
        unsafe {
            let cache_ref = cache_page.as_mut();
            cache_ref.data[..data.len()].copy_from_slice(data);
        }
    }

    /// Write page data back to cache (call after modifying a page)
    pub fn write_page_to_cache(&mut self, page: &PgHdr) {
        if let Some(cache_page) = self.pcache.fetch(page.pgno, false) {
            self.update_cache_page(cache_page, &page.data);
        }
    }

    /// Get a page only if it's already cached (sqlite3PagerLookup)
    ///
    /// Note: Due to the current design where get() creates new PgHdr objects,
    /// this cannot return a reference. Returns None - callers should use get()
    /// which handles cache internally.
    pub fn lookup(&self, _pgno: Pgno) -> Option<&PgHdr> {
        // The pcache uses a different PgHdr type internally.
        // This function would need redesign to return a proper reference.
        // For now, callers should use get() which handles caching correctly.
        None
    }

    /// Check if a page is in the cache (without returning a reference)
    pub fn is_cached(&self, pgno: Pgno) -> bool {
        // Use pcache's fetch with create=false to check
        // Note: This is a const method that can't call mutable fetch
        // So we check via db_size which indicates loaded pages
        pgno > 0 && pgno <= self.db_size
    }

    /// Increment page reference count (sqlite3PagerRef)
    pub fn page_ref(page: &mut PgHdr) {
        page.n_ref += 1;
    }

    /// Decrement page reference count (sqlite3PagerUnref)
    pub fn page_unref(page: &mut PgHdr) {
        if page.n_ref > 0 {
            page.n_ref -= 1;
        }
    }

    /// Mark a page as writeable (sqlite3PagerWrite)
    pub fn write(&mut self, page: &mut PgHdr) -> Result<()> {
        // Must be in writer state
        if self.state < PagerState::Writer {
            self.begin(true)?;
        }

        // Journal the original page content before modification
        if !page.flags.contains(PgFlags::WRITEABLE) {
            self.journal_page(page)?;
            page.flags.insert(PgFlags::WRITEABLE);
            page.flags.insert(PgFlags::DIRTY);
        }

        if let Some(cache_page) = self.pcache.fetch(page.pgno, false) {
            self.pcache.make_dirty(cache_page);
        }

        Ok(())
    }

    /// Mark a page as "do not write" (sqlite3PagerDontWrite)
    pub fn dont_write(&mut self, page: &mut PgHdr) {
        page.flags.insert(PgFlags::DONT_WRITE);
    }

    // ========================================================================
    // Transaction Control
    // ========================================================================

    /// Acquire a shared lock (sqlite3PagerSharedLock)
    pub fn shared_lock(&mut self) -> Result<()> {
        if self.state >= PagerState::Reader {
            return Ok(());
        }

        // Acquire shared lock on database file
        self.lock(LockLevel::Shared)?;

        // Read database size from file
        if let Some(ref fd) = self.fd {
            let size = fd.file_size()?;
            self.db_size = (size / self.page_size as i64) as Pgno;
            self.db_file_size = self.db_size;
        }

        self.state = PagerState::Reader;
        Ok(())
    }

    /// Begin a write transaction (sqlite3PagerBegin)
    pub fn begin(&mut self, exclusive: bool) -> Result<()> {
        if self.state >= PagerState::Writer {
            return Ok(());
        }

        // Must have at least a shared lock
        if self.state < PagerState::Reader {
            self.shared_lock()?;
        }

        // Acquire reserved lock
        self.lock(LockLevel::Reserved)?;

        // Save original database size
        self.db_orig_size = self.db_size;

        // Open the journal file
        self.open_journal()?;

        self.state = PagerState::Writer;

        // If exclusive requested, upgrade to exclusive lock
        if exclusive {
            self.lock(LockLevel::Exclusive)?;
            self.state = PagerState::WriterLocked;
        }

        Ok(())
    }

    /// Commit phase one - sync journal (sqlite3PagerCommitPhaseOne)
    pub fn commit_phase_one(&mut self, _super_journal: Option<&str>) -> Result<()> {
        if self.state < PagerState::Writer {
            return Ok(());
        }

        // Sync the journal
        if let Some(ref mut jfd) = self.jfd {
            jfd.sync(SyncFlags::NORMAL)?;
        }

        // Acquire exclusive lock
        self.lock(LockLevel::Exclusive)?;
        self.state = PagerState::WriterLocked;

        Ok(())
    }

    /// Commit phase two - write pages and finalize (sqlite3PagerCommitPhaseTwo)
    pub fn commit_phase_two(&mut self) -> Result<()> {
        if self.state < PagerState::WriterLocked {
            return Ok(());
        }

        // Write all dirty pages from cache to database file
        if let Some(ref mut fd) = self.fd {
            let mut current = self.pcache.dirty_list();
            while let Some(page) = current {
                unsafe {
                    let page_ref = page.as_ref();
                    // Skip pages marked as "don't write"
                    if !page_ref.flags.contains(PgFlags::DONT_WRITE) {
                        let offset = ((page_ref.pgno - 1) as i64) * (self.page_size as i64);
                        fd.write(&page_ref.data, offset)?;
                        self.n_write += 1;
                    }
                    current = page_ref.dirty_next;
                }
            }
        }

        // Clear dirty list - all pages are now clean
        self.pcache.clean_all();

        // Sync database file to ensure durability
        if let Some(ref mut fd) = self.fd {
            fd.sync(SyncFlags::NORMAL)?;
        }

        // End journal (delete/truncate/zero based on mode)
        self.end_journal()?;

        // Truncate database file if needed (in case of vacuum shrinking)
        if self.db_size < self.db_file_size {
            if let Some(ref mut fd) = self.fd {
                let new_size = (self.db_size as i64) * (self.page_size as i64);
                fd.truncate(new_size)?;
            }
            self.db_file_size = self.db_size;
        }

        // Release locks (unless in exclusive mode)
        if self.locking_mode == LockingMode::Normal {
            self.unlock(LockLevel::Shared)?;
            self.state = PagerState::Reader;
        } else {
            self.state = PagerState::WriterFinished;
        }

        Ok(())
    }

    /// Rollback transaction (sqlite3PagerRollback)
    pub fn rollback(&mut self) -> Result<()> {
        if self.state < PagerState::Writer {
            return Ok(());
        }

        // Playback journal to restore database
        // For file-based databases, use file journal; for memory databases, use in-memory journal
        if self.jfd.is_some() || (self.mem_db && !self.mem_journal.is_empty()) {
            self.playback_journal()?;
        }

        // Clear the dirty list - all pages have been restored from journal
        // This is critical: playback_journal removes the DIRTY flag but doesn't
        // remove pages from the dirty list. We must clean the list here to prevent
        // cycles when pages are re-dirtied in subsequent transactions.
        self.pcache.clean_all();

        // Restore original database size
        self.db_size = self.db_orig_size;

        // End journal
        self.end_journal()?;

        // Release locks
        if self.locking_mode == LockingMode::Normal {
            self.unlock(LockLevel::Shared)?;
            self.state = PagerState::Reader;
        } else {
            self.state = PagerState::WriterFinished;
        }

        Ok(())
    }

    /// Sync pager to disk (sqlite3PagerSync)
    pub fn sync(&mut self) -> Result<()> {
        if let Some(ref mut fd) = self.fd {
            fd.sync(SyncFlags::NORMAL)?;
        }
        Ok(())
    }

    // ========================================================================
    // Savepoint Operations
    // ========================================================================

    /// Open a new savepoint (sqlite3PagerOpenSavepoint)
    pub fn open_savepoint(&mut self, n: i32) -> Result<()> {
        while self.savepoints.len() < n as usize {
            let savepoint = Savepoint::new(self.journal_offset, self.db_size);
            self.savepoints.push(savepoint);
        }
        Ok(())
    }

    /// Savepoint operation (sqlite3PagerSavepoint)
    pub fn savepoint(&mut self, op: SavepointOp, index: i32) -> Result<()> {
        let idx = index as usize;

        match op {
            SavepointOp::Release => {
                // Remove savepoints from index onwards
                if idx < self.savepoints.len() {
                    self.savepoints.truncate(idx);
                }
            }
            SavepointOp::Rollback => {
                // Rollback to savepoint
                if idx < self.savepoints.len() {
                    let savepoint = &self.savepoints[idx];
                    self.db_size = savepoint.orig_db_size;

                    // Playback sub-journal to restore pages modified after savepoint
                    // Pages recorded in sub_journal_pages after savepoint.sub_rec
                    // need to be restored from the main journal
                    let sub_rec_start = savepoint.sub_rec as usize;
                    let pages_to_restore: Vec<Pgno> =
                        self.sub_journal_pages[sub_rec_start..].to_vec();

                    for pgno in pages_to_restore {
                        // Invalidate cache entry - forces re-read from disk on next access
                        if let Some(cache_page) = self.pcache.fetch(pgno, false) {
                            self.pcache.make_clean(cache_page);
                        }
                    }

                    // Truncate sub-journal tracking
                    let sub_rec = savepoint.sub_rec as usize;
                    self.sub_journal_pages.truncate(sub_rec);

                    // Truncate savepoints to the rolled-back level
                    self.savepoints.truncate(idx);
                }
            }
            SavepointOp::Begin => {
                self.open_savepoint(index + 1)?;
            }
        }

        Ok(())
    }

    // ========================================================================
    // Lock Management
    // ========================================================================

    /// Acquire a lock (internal)
    fn lock(&mut self, level: LockLevel) -> Result<()> {
        if level <= self.lock {
            return Ok(());
        }

        if let Some(ref mut fd) = self.fd {
            fd.lock(level)?;
        }
        self.lock = level;
        Ok(())
    }

    /// Release a lock (internal)
    fn unlock(&mut self, level: LockLevel) -> Result<()> {
        if level >= self.lock {
            return Ok(());
        }

        if let Some(ref mut fd) = self.fd {
            fd.unlock(level)?;
        }
        self.lock = level;
        Ok(())
    }

    /// Acquire exclusive lock (sqlite3PagerExclusiveLock)
    pub fn exclusive_lock(&mut self) -> Result<()> {
        self.lock(LockLevel::Exclusive)
    }

    // ========================================================================
    // Journal Operations
    // ========================================================================

    /// Open the journal file
    fn open_journal(&mut self) -> Result<()> {
        if self.jfd.is_some() {
            return Ok(());
        }

        // In memory mode doesn't need a journal file
        if self.journal_mode == JournalMode::Off || self.mem_db {
            return Ok(());
        }

        // Open the journal file via VFS callback
        if let Some(ref open_fn) = self.vfs_open {
            let flags = OpenFlags::CREATE | OpenFlags::READWRITE;
            let jfd = open_fn(&self.journal_path, flags)?;
            self.jfd = Some(jfd);

            // Write journal header
            let header = JournalHeader::new(
                0, // page count will be updated as we write
                self.db_orig_size,
                4096, // sector size
                self.page_size,
            );
            self.checksum_nonce = header.nonce;

            if let Some(ref mut jfd) = self.jfd {
                jfd.write(&header.to_bytes(), 0)?;
            }

            self.journal_offset = JOURNAL_HEADER_SIZE as i64;
            self.journal_header = 0;
            self.n_rec = 0;
        } else {
            // No VFS callback set - operate in memory-only mode
            self.journal_offset = 0;
            self.journal_header = 0;
            self.n_rec = 0;
        }

        Ok(())
    }

    /// Write page to journal
    fn journal_page(&mut self, page: &PgHdr) -> Result<()> {
        if self.journal_mode == JournalMode::Off {
            return Ok(());
        }

        // Calculate checksum before borrowing jfd
        let checksum = Self::checksum_data(&page.data);

        if let Some(ref mut jfd) = self.jfd {
            // Write to file-based journal
            // Write page number (4 bytes)
            jfd.write(&page.pgno.to_be_bytes(), self.journal_offset)?;
            self.journal_offset += 4;

            // Write page content
            jfd.write(&page.data, self.journal_offset)?;
            self.journal_offset += page.data.len() as i64;

            // Write checksum (4 bytes)
            jfd.write(&checksum.to_be_bytes(), self.journal_offset)?;
            self.journal_offset += 4;

            self.n_rec += 1;
        } else if self.mem_db {
            // For memory databases, store journal records in memory
            self.mem_journal.push((page.pgno, page.data.clone()));
            self.n_rec += 1;
        }

        Ok(())
    }

    /// End journal (finalize transaction)
    fn end_journal(&mut self) -> Result<()> {
        match self.journal_mode {
            JournalMode::Delete => {
                // Close and delete the journal file
                self.jfd = None;
                if let Some(ref delete_fn) = self.vfs_delete {
                    // Ignore errors on delete - file might not exist
                    let _ = delete_fn(&self.journal_path);
                }
            }
            JournalMode::Truncate => {
                // Truncate journal to zero
                if let Some(ref mut jfd) = self.jfd {
                    jfd.truncate(0)?;
                    jfd.sync(SyncFlags::NORMAL)?;
                }
            }
            JournalMode::Persist => {
                // Zero the journal header to invalidate it
                if let Some(ref mut jfd) = self.jfd {
                    let zeros = [0u8; JOURNAL_HEADER_SIZE];
                    jfd.write(&zeros, 0)?;
                    jfd.sync(SyncFlags::NORMAL)?;
                }
            }
            JournalMode::Memory | JournalMode::Off => {
                // Nothing to do - no journal file
            }
            JournalMode::Wal => {
                // WAL mode handles commits via WAL, not journal
            }
        }

        self.journal_offset = 0;
        self.journal_header = 0;
        self.n_rec = 0;
        self.sub_journal_pages.clear();
        self.mem_journal.clear();

        Ok(())
    }

    /// Playback journal for recovery/rollback
    fn playback_journal(&mut self) -> Result<()> {
        // For memory databases, use the in-memory journal
        if self.mem_db && !self.mem_journal.is_empty() {
            let page_size = self.page_size as usize;

            // Process journal records in reverse order (LIFO) to restore original state
            // We iterate in reverse because a page might be journaled multiple times
            // and we want to restore the earliest (original) version
            let journal_records: Vec<_> = std::mem::take(&mut self.mem_journal);

            // Track which pages we've already restored to avoid double-restoring
            let mut restored_pages = std::collections::HashSet::new();

            for (pgno, original_data) in journal_records.into_iter().rev() {
                if restored_pages.contains(&pgno) {
                    continue; // Already restored this page
                }
                restored_pages.insert(pgno);

                // Restore the page in the cache
                // Note: We don't remove the DIRTY flag here - clean_all() in rollback()
                // will handle removing pages from the dirty list and clearing the flag.
                if let Some(mut cache_page) = self.pcache.fetch(pgno, false) {
                    unsafe {
                        let data_len = original_data.len().min(page_size);
                        cache_page.as_mut().data[..data_len]
                            .copy_from_slice(&original_data[..data_len]);
                        cache_page.as_mut().flags.remove(PgFlags::WRITEABLE);
                    }
                }
            }

            // Restore original database size
            self.db_size = self.db_orig_size;
            return Ok(());
        }

        // For file-based databases, use the file journal
        // Check if journal exists and has valid content
        let jfd = match self.jfd.as_mut() {
            Some(jfd) => jfd,
            None => {
                // No journal to playback
                self.db_size = self.db_orig_size;
                return Ok(());
            }
        };

        // Read and validate journal header
        let mut header_buf = [0u8; JOURNAL_HEADER_SIZE];
        let bytes_read = jfd.read(&mut header_buf, 0)?;
        if bytes_read < JOURNAL_HEADER_SIZE {
            // Incomplete journal header - treat as empty
            self.db_size = self.db_orig_size;
            return Ok(());
        }

        let header = match JournalHeader::from_bytes(&header_buf) {
            Ok(h) => h,
            Err(_) => {
                // Invalid journal - treat as empty
                self.db_size = self.db_orig_size;
                return Ok(());
            }
        };

        // Restore original database size
        self.db_size = header.initial_pages;

        // Read each journal record and restore pages
        let page_size = header.page_size as usize;
        let _record_size = 4 + page_size + 4; // pgno + data + checksum

        // Allocate buffer for reading pages
        let mut page_buf = vec![0u8; page_size];
        let mut pgno_buf = [0u8; 4];
        let mut checksum_buf = [0u8; 4];

        let mut offset = JOURNAL_HEADER_SIZE as i64;

        // Read and restore each page
        while offset < self.journal_offset {
            // Read page number
            let n = jfd.read(&mut pgno_buf, offset)?;
            if n < 4 {
                break; // Incomplete record
            }
            let pgno = u32::from_be_bytes(pgno_buf);
            offset += 4;

            // Read page data
            let n = jfd.read(&mut page_buf, offset)?;
            if n < page_size {
                break; // Incomplete record
            }
            offset += page_size as i64;

            // Read and verify checksum
            let n = jfd.read(&mut checksum_buf, offset)?;
            if n < 4 {
                break; // Incomplete record
            }
            let stored_checksum = u32::from_be_bytes(checksum_buf);
            let computed_checksum = Self::checksum_data(&page_buf);
            offset += 4;

            if stored_checksum != computed_checksum {
                // Checksum mismatch - corrupted record, stop playback
                break;
            }

            // Write original page data back to database file
            if let Some(ref mut fd) = self.fd {
                let db_offset = ((pgno - 1) as i64) * (self.page_size as i64);
                fd.write(&page_buf, db_offset)?;
            }

            // Also update the cache if the page is there
            // Note: We don't remove the DIRTY flag here - clean_all() in rollback()
            // will handle removing pages from the dirty list and clearing the flag.
            if let Some(mut cache_page) = self.pcache.fetch(pgno, false) {
                unsafe {
                    cache_page.as_mut().data[..page_size].copy_from_slice(&page_buf);
                }
            }
        }

        // Sync the database file after restoration
        if let Some(ref mut fd) = self.fd {
            fd.sync(SyncFlags::NORMAL)?;

            // Truncate database to original size
            let new_size = (self.db_size as i64) * (self.page_size as i64);
            fd.truncate(new_size)?;
        }

        self.db_file_size = self.db_size;

        Ok(())
    }

    /// Calculate checksum for journal (static method)
    fn checksum_data(data: &[u8]) -> u32 {
        let mut sum: u32 = 0;
        for (i, &byte) in data.iter().enumerate() {
            sum = sum.wrapping_add((byte as u32) << ((i & 3) * 8));
        }
        sum
    }

    // ========================================================================
    // Query Functions
    // ========================================================================

    /// Check if pager is read-only (sqlite3PagerIsreadonly)
    pub fn is_readonly(&self) -> bool {
        self.read_only
    }

    /// Check if this is an in-memory database (sqlite3PagerIsMemdb)
    pub fn is_memdb(&self) -> bool {
        self.mem_db
    }

    /// Get database filename (sqlite3PagerFilename)
    pub fn filename(&self) -> &str {
        &self.db_path
    }

    /// Get journal filename (sqlite3PagerJournalname)
    pub fn journal_name(&self) -> &str {
        &self.journal_path
    }

    /// Get page count (sqlite3PagerPagecount)
    pub fn page_count(&self) -> Pgno {
        self.db_size
    }

    /// Get temporary space buffer (sqlite3PagerTempSpace)
    pub fn temp_space(&mut self) -> &mut [u8] {
        &mut self.tmp_space
    }

    /// Get reference count (sqlite3PagerRefcount) - debug only
    pub fn refcount(&self) -> i32 {
        // Return the sum of references from the page cache
        self.pcache.ref_count() as i32
    }

    /// Get memory used by pager (sqlite3PagerMemUsed)
    pub fn mem_used(&self) -> i32 {
        // Approximate memory usage
        (self.page_size as i32) * self.cache_size
    }

    /// Truncate database image (sqlite3PagerTruncateImage)
    pub fn truncate_image(&mut self, pgno: Pgno) {
        if pgno < self.db_size {
            self.db_size = pgno;
        }
    }

    // ========================================================================
    // WAL Mode Support
    // ========================================================================

    /// Open WAL mode for this pager (sqlite3PagerOpenWal)
    pub fn open_wal(&mut self) -> Result<()> {
        if self.wal.is_some() {
            return Ok(()); // Already open
        }

        if self.mem_db {
            return Err(Error::with_message(
                ErrorCode::Misuse,
                "WAL mode not supported for in-memory databases".to_string(),
            ));
        }

        // Create WAL instance
        let wal = Wal::open(&self.db_path, self.page_size)?;
        self.wal = Some(wal);
        self.journal_mode = JournalMode::Wal;

        Ok(())
    }

    /// Close WAL mode and return to rollback journal (sqlite3PagerCloseWal)
    pub fn close_wal(&mut self) -> Result<()> {
        if self.wal.is_none() {
            return Ok(()); // Not in WAL mode
        }

        // Checkpoint and close WAL
        if let Some(ref mut fd) = self.fd {
            if let Some(ref mut wal) = self.wal {
                wal.checkpoint(fd.as_mut(), CheckpointMode::Truncate, None)?;
            }
        }

        self.wal = None;
        self.journal_mode = JournalMode::Delete;

        Ok(())
    }

    /// Perform WAL checkpoint (sqlite3PagerCheckpoint)
    pub fn checkpoint(&mut self, mode: CheckpointMode) -> Result<(i32, i32)> {
        if self.journal_mode != JournalMode::Wal {
            return Ok((0, 0)); // Not in WAL mode
        }

        let wal = match self.wal.as_mut() {
            Some(w) => w,
            None => return Ok((0, 0)),
        };

        let fd = match self.fd.as_mut() {
            Some(f) => f,
            None => return Ok((0, 0)),
        };

        wal.checkpoint(fd.as_mut(), mode, None)
    }

    /// Read a page, checking WAL first if in WAL mode
    fn read_page_with_wal(&mut self, pgno: Pgno, buf: &mut [u8]) -> Result<()> {
        // In WAL mode, check WAL first
        if let Some(ref mut wal) = self.wal {
            if let Ok(frame) = wal.find_frame(pgno) {
                if frame > 0 {
                    return wal.read_frame(frame, buf);
                }
            }
        }

        // Fall back to database file
        if let Some(ref mut fd) = self.fd {
            let offset = ((pgno - 1) as i64) * (self.page_size as i64);
            fd.read(buf, offset)?;
        }

        Ok(())
    }

    /// Check if WAL mode is active
    pub fn is_wal_mode(&self) -> bool {
        self.wal.is_some() && self.journal_mode == JournalMode::Wal
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_journal_mode() {
        assert!(JournalMode::Wal.is_wal());
        assert!(!JournalMode::Delete.is_wal());
        assert!(!JournalMode::Persist.is_wal());
    }

    #[test]
    fn test_pager_state_ordering() {
        assert!((PagerState::Open as i32) < (PagerState::Reader as i32));
        assert!((PagerState::Reader as i32) < (PagerState::Writer as i32));
        assert!((PagerState::Writer as i32) < (PagerState::WriterLocked as i32));
    }

    #[test]
    fn test_journal_header_roundtrip() {
        let header = JournalHeader::new(100, 50, 512, 4096);
        let bytes = header.to_bytes();
        let parsed = JournalHeader::from_bytes(&bytes).unwrap();

        assert_eq!(header.page_count, parsed.page_count);
        assert_eq!(header.initial_pages, parsed.initial_pages);
        assert_eq!(header.sector_size, parsed.sector_size);
        assert_eq!(header.page_size, parsed.page_size);
    }

    #[test]
    fn test_pg_flags() {
        let mut flags = PgFlags::CLEAN;
        assert!(!flags.contains(PgFlags::DIRTY));

        flags.insert(PgFlags::DIRTY);
        assert!(flags.contains(PgFlags::DIRTY));

        flags.insert(PgFlags::WRITEABLE);
        assert!(flags.contains(PgFlags::DIRTY));
        assert!(flags.contains(PgFlags::WRITEABLE));
    }

    #[test]
    fn test_pghdr_new() {
        let page = PgHdr::new(1, 4096);
        assert_eq!(page.pgno, 1);
        assert_eq!(page.data.len(), 4096);
        assert!(!page.is_dirty());
        assert!(!page.is_writeable());
    }
}
