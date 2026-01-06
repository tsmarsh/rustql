//! Page cache management
//!
//! The Pager module manages the page cache, handles database I/O, journaling,
//! and transaction durability. This is the layer between the B-tree and the
//! OS file system.

use bitflags::bitflags;

use crate::error::{Error, ErrorCode, Result};
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

    // Temporary space
    /// Temporary buffer for page operations
    pub tmp_space: Vec<u8>,
}

impl Pager {
    // ========================================================================
    // Initialization
    // ========================================================================

    /// Open a pager on a database file (sqlite3PagerOpen)
    pub fn open<V: Vfs>(
        _vfs: &V,
        path: &str,
        _flags: PagerOpenFlags,
        _vfs_flags: OpenFlags,
    ) -> Result<Self> {
        let journal_path = format!("{}-journal", path);

        Ok(Pager {
            fd: None,
            jfd: None,
            sjfd: None,
            db_path: path.to_string(),
            journal_path,
            state: PagerState::Open,
            lock: LockLevel::None,
            journal_mode: JournalMode::Delete,
            locking_mode: LockingMode::Normal,
            err_code: ErrorCode::Ok,
            page_size: DEFAULT_PAGE_SIZE,
            usable_size: DEFAULT_PAGE_SIZE,
            db_size: 0,
            db_orig_size: 0,
            db_file_size: 0,
            max_page_count: 0xFFFFFFFF,
            cache_size: 2000,
            spill_size: 1,
            mmap_limit: 0,
            journal_offset: 0,
            journal_header: 0,
            n_rec: 0,
            journal_size_limit: DEFAULT_JOURNAL_SIZE_LIMIT,
            n_read: 0,
            n_write: 0,
            n_hit: 0,
            n_miss: 0,
            flags: PagerFlags::SYNCHRONOUS_FULL,
            temp_file: false,
            mem_db: false,
            read_only: false,
            no_sync: false,
            savepoints: Vec::new(),
            tmp_space: vec![0u8; DEFAULT_PAGE_SIZE as usize],
        })
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
        if page_size < MIN_PAGE_SIZE || page_size > MAX_PAGE_SIZE {
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

        // Create new page
        let mut page = Box::new(PgHdr::new(pgno, self.page_size));

        // Read from disk if file is open and page exists
        if let Some(ref mut fd) = self.fd {
            let offset = ((pgno - 1) as i64) * (self.page_size as i64);
            let _ = fd.read(&mut page.data, offset);
            self.n_read += 1;
        }

        page.n_ref = 1;
        Ok(page)
    }

    /// Get a page only if it's already cached (sqlite3PagerLookup)
    pub fn lookup(&self, _pgno: Pgno) -> Option<&PgHdr> {
        // TODO: Implement actual page cache lookup
        None
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

        // Write all dirty pages to database
        // TODO: Implement dirty page writing

        // Sync database file
        if let Some(ref mut fd) = self.fd {
            fd.sync(SyncFlags::NORMAL)?;
        }

        // End journal
        self.end_journal()?;

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
        if self.jfd.is_some() {
            self.playback_journal()?;
        }

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
                    // TODO: Playback sub-journal
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

        // TODO: Actually open the journal file via VFS
        // For now, just set journal offset
        self.journal_offset = 0;
        self.journal_header = 0;
        self.n_rec = 0;

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
        }

        Ok(())
    }

    /// End journal (finalize transaction)
    fn end_journal(&mut self) -> Result<()> {
        match self.journal_mode {
            JournalMode::Delete => {
                // Delete the journal file
                self.jfd = None;
                // TODO: Actually delete file via VFS
            }
            JournalMode::Truncate => {
                // Truncate journal to zero
                if let Some(ref mut jfd) = self.jfd {
                    jfd.truncate(0)?;
                }
            }
            JournalMode::Persist => {
                // Zero the journal header
                if let Some(ref mut jfd) = self.jfd {
                    let zeros = [0u8; JOURNAL_HEADER_SIZE];
                    jfd.write(&zeros, 0)?;
                }
            }
            JournalMode::Memory | JournalMode::Off => {
                // Nothing to do
            }
            JournalMode::Wal => {
                // WAL mode handles this differently
            }
        }

        self.journal_offset = 0;
        self.journal_header = 0;
        self.n_rec = 0;

        Ok(())
    }

    /// Playback journal for recovery/rollback
    fn playback_journal(&mut self) -> Result<()> {
        // TODO: Implement full journal playback
        // For now, just reset to original state
        self.db_size = self.db_orig_size;
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
        // TODO: Implement actual refcount tracking
        0
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
