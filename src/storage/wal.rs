//! Write-ahead logging
//!
//! The WAL (Write-Ahead Logging) module provides improved concurrency and
//! performance compared to rollback journal mode. WAL allows concurrent
//! readers and a single writer.

use crate::error::{Error, ErrorCode, Result};
use crate::types::{Pgno, SyncFlags, VfsFile};

// ============================================================================
// Constants
// ============================================================================

/// WAL magic number (little endian checksums)
pub const WAL_MAGIC_LE: u32 = 0x377f0682;

/// WAL magic number (big endian checksums)
pub const WAL_MAGIC_BE: u32 = 0x377f0683;

/// WAL file format version
pub const WAL_VERSION: u32 = 3007000;

/// WAL header size in bytes
pub const WAL_HEADER_SIZE: usize = 32;

/// WAL frame header size in bytes
pub const WAL_FRAME_HEADER_SIZE: usize = 24;

/// Number of reader slots in WAL-index
pub const WAL_NREADER: usize = 5;

/// Hash table slots per region
pub const HASHTABLE_NSLOT: usize = 8192;

/// Hash table page entries per region
pub const HASHTABLE_NPAGE: usize = 4096;

/// Size of WAL-index header region
pub const WALINDEX_HDR_SIZE: usize = 136;

/// Read lock indicating no lock held
pub const WAL_READ_LOCK_NONE: i16 = -1;

// ============================================================================
// Checkpoint Mode
// ============================================================================

/// Checkpoint modes for WAL
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum CheckpointMode {
    /// Checkpoint without blocking - do as much as possible
    Passive = 0,
    /// Wait for readers, checkpoint all frames
    Full = 1,
    /// Full + reset WAL file to beginning
    Restart = 2,
    /// Full + truncate WAL file to zero
    Truncate = 3,
}

impl CheckpointMode {
    /// Create from integer value
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(CheckpointMode::Passive),
            1 => Some(CheckpointMode::Full),
            2 => Some(CheckpointMode::Restart),
            3 => Some(CheckpointMode::Truncate),
            _ => None,
        }
    }
}

// ============================================================================
// WAL Index Header
// ============================================================================

/// WAL index header structure (stored in shared memory)
///
/// This structure is stored at the beginning of the WAL-index (shared memory).
/// Two copies are maintained for lock-free reading.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct WalIndexHdr {
    /// WAL-index format version
    pub version: u32,
    /// Unused padding
    pub unused: u32,
    /// Change counter (incremented on each write)
    pub change: u32,
    /// True (1) after initialization
    pub is_init: u8,
    /// True (1) if checksums are big-endian
    pub big_endian_cksum: u8,
    /// Database page size (stored as u16 to save space)
    pub page_size: u16,
    /// Last valid frame number in WAL
    pub max_frame: u32,
    /// Database size in pages after last commit
    pub n_page: u32,
    /// Checksum of the last frame
    pub frame_cksum: [u32; 2],
    /// Salt values (random, must match WAL file)
    pub salt: [u32; 2],
    /// Checksum of this header
    pub cksum: [u32; 2],
}

impl WalIndexHdr {
    /// Create a new WAL index header
    pub fn new(page_size: u32) -> Self {
        WalIndexHdr {
            version: WAL_VERSION,
            unused: 0,
            change: 0,
            is_init: 0,
            big_endian_cksum: 0,
            page_size: page_size as u16,
            max_frame: 0,
            n_page: 0,
            frame_cksum: [0, 0],
            salt: [0, 0],
            cksum: [0, 0],
        }
    }

    /// Check if the header is initialized
    pub fn is_initialized(&self) -> bool {
        self.is_init != 0
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> [u8; 48] {
        let mut buf = [0u8; 48];
        buf[0..4].copy_from_slice(&self.version.to_le_bytes());
        buf[4..8].copy_from_slice(&self.unused.to_le_bytes());
        buf[8..12].copy_from_slice(&self.change.to_le_bytes());
        buf[12] = self.is_init;
        buf[13] = self.big_endian_cksum;
        buf[14..16].copy_from_slice(&self.page_size.to_le_bytes());
        buf[16..20].copy_from_slice(&self.max_frame.to_le_bytes());
        buf[20..24].copy_from_slice(&self.n_page.to_le_bytes());
        buf[24..28].copy_from_slice(&self.frame_cksum[0].to_le_bytes());
        buf[28..32].copy_from_slice(&self.frame_cksum[1].to_le_bytes());
        buf[32..36].copy_from_slice(&self.salt[0].to_le_bytes());
        buf[36..40].copy_from_slice(&self.salt[1].to_le_bytes());
        buf[40..44].copy_from_slice(&self.cksum[0].to_le_bytes());
        buf[44..48].copy_from_slice(&self.cksum[1].to_le_bytes());
        buf
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 48 {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        Ok(WalIndexHdr {
            version: u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
            unused: u32::from_le_bytes([data[4], data[5], data[6], data[7]]),
            change: u32::from_le_bytes([data[8], data[9], data[10], data[11]]),
            is_init: data[12],
            big_endian_cksum: data[13],
            page_size: u16::from_le_bytes([data[14], data[15]]),
            max_frame: u32::from_le_bytes([data[16], data[17], data[18], data[19]]),
            n_page: u32::from_le_bytes([data[20], data[21], data[22], data[23]]),
            frame_cksum: [
                u32::from_le_bytes([data[24], data[25], data[26], data[27]]),
                u32::from_le_bytes([data[28], data[29], data[30], data[31]]),
            ],
            salt: [
                u32::from_le_bytes([data[32], data[33], data[34], data[35]]),
                u32::from_le_bytes([data[36], data[37], data[38], data[39]]),
            ],
            cksum: [
                u32::from_le_bytes([data[40], data[41], data[42], data[43]]),
                u32::from_le_bytes([data[44], data[45], data[46], data[47]]),
            ],
        })
    }
}

// ============================================================================
// WAL File Header
// ============================================================================

/// WAL file header (32 bytes at start of WAL file)
#[derive(Debug, Clone, Copy)]
pub struct WalHeader {
    /// Magic number (WAL_MAGIC_LE or WAL_MAGIC_BE)
    pub magic: u32,
    /// File format version
    pub version: u32,
    /// Database page size
    pub page_size: u32,
    /// Checkpoint sequence number
    pub checkpoint_seq: u32,
    /// Salt value 1 (random)
    pub salt1: u32,
    /// Salt value 2 (random)
    pub salt2: u32,
    /// Checksum part 1
    pub checksum1: u32,
    /// Checksum part 2
    pub checksum2: u32,
}

impl WalHeader {
    /// Create a new WAL header
    pub fn new(page_size: u32, checkpoint_seq: u32) -> Self {
        let salt1 = generate_salt();
        let salt2 = generate_salt();

        let mut hdr = WalHeader {
            magic: WAL_MAGIC_LE,
            version: WAL_VERSION,
            page_size,
            checkpoint_seq,
            salt1,
            salt2,
            checksum1: 0,
            checksum2: 0,
        };

        // Calculate header checksum
        let (c1, c2) = hdr.compute_checksum();
        hdr.checksum1 = c1;
        hdr.checksum2 = c2;

        hdr
    }

    /// Check if using big-endian checksums
    pub fn is_big_endian(&self) -> bool {
        self.magic == WAL_MAGIC_BE
    }

    /// Compute checksum of header (first 24 bytes)
    fn compute_checksum(&self) -> (u32, u32) {
        let mut data = [0u8; 24];
        data[0..4].copy_from_slice(&self.magic.to_le_bytes());
        data[4..8].copy_from_slice(&self.version.to_le_bytes());
        data[8..12].copy_from_slice(&self.page_size.to_le_bytes());
        data[12..16].copy_from_slice(&self.checkpoint_seq.to_le_bytes());
        data[16..20].copy_from_slice(&self.salt1.to_le_bytes());
        data[20..24].copy_from_slice(&self.salt2.to_le_bytes());

        wal_checksum(self.is_big_endian(), &data, 0, 0)
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> [u8; WAL_HEADER_SIZE] {
        let mut buf = [0u8; WAL_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..12].copy_from_slice(&self.page_size.to_le_bytes());
        buf[12..16].copy_from_slice(&self.checkpoint_seq.to_le_bytes());
        buf[16..20].copy_from_slice(&self.salt1.to_le_bytes());
        buf[20..24].copy_from_slice(&self.salt2.to_le_bytes());
        buf[24..28].copy_from_slice(&self.checksum1.to_le_bytes());
        buf[28..32].copy_from_slice(&self.checksum2.to_le_bytes());
        buf
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < WAL_HEADER_SIZE {
            return Err(Error::new(ErrorCode::Corrupt));
        }

        let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if magic != WAL_MAGIC_LE && magic != WAL_MAGIC_BE {
            return Err(Error::new(ErrorCode::NotADb));
        }

        Ok(WalHeader {
            magic,
            version: u32::from_le_bytes([data[4], data[5], data[6], data[7]]),
            page_size: u32::from_le_bytes([data[8], data[9], data[10], data[11]]),
            checkpoint_seq: u32::from_le_bytes([data[12], data[13], data[14], data[15]]),
            salt1: u32::from_le_bytes([data[16], data[17], data[18], data[19]]),
            salt2: u32::from_le_bytes([data[20], data[21], data[22], data[23]]),
            checksum1: u32::from_le_bytes([data[24], data[25], data[26], data[27]]),
            checksum2: u32::from_le_bytes([data[28], data[29], data[30], data[31]]),
        })
    }

    /// Validate header checksum
    pub fn validate(&self) -> Result<()> {
        let (c1, c2) = self.compute_checksum();
        if c1 != self.checksum1 || c2 != self.checksum2 {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        if self.version != WAL_VERSION {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        Ok(())
    }
}

// ============================================================================
// WAL Frame Header
// ============================================================================

/// WAL frame header (24 bytes before each page in WAL)
#[derive(Debug, Clone, Copy)]
pub struct WalFrameHdr {
    /// Page number this frame contains
    pub pgno: Pgno,
    /// Database size after commit (0 if not a commit frame)
    pub n_truncate: u32,
    /// Salt values (must match WAL header)
    pub salt: [u32; 2],
    /// Cumulative checksum
    pub checksum: [u32; 2],
}

impl WalFrameHdr {
    /// Create a new frame header
    pub fn new(pgno: Pgno, n_truncate: u32, salt: [u32; 2]) -> Self {
        WalFrameHdr {
            pgno,
            n_truncate,
            salt,
            checksum: [0, 0],
        }
    }

    /// Check if this is a commit frame
    pub fn is_commit(&self) -> bool {
        self.n_truncate > 0
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> [u8; WAL_FRAME_HEADER_SIZE] {
        let mut buf = [0u8; WAL_FRAME_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.pgno.to_be_bytes());
        buf[4..8].copy_from_slice(&self.n_truncate.to_be_bytes());
        buf[8..12].copy_from_slice(&self.salt[0].to_be_bytes());
        buf[12..16].copy_from_slice(&self.salt[1].to_be_bytes());
        buf[16..20].copy_from_slice(&self.checksum[0].to_be_bytes());
        buf[20..24].copy_from_slice(&self.checksum[1].to_be_bytes());
        buf
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < WAL_FRAME_HEADER_SIZE {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        Ok(WalFrameHdr {
            pgno: u32::from_be_bytes([data[0], data[1], data[2], data[3]]),
            n_truncate: u32::from_be_bytes([data[4], data[5], data[6], data[7]]),
            salt: [
                u32::from_be_bytes([data[8], data[9], data[10], data[11]]),
                u32::from_be_bytes([data[12], data[13], data[14], data[15]]),
            ],
            checksum: [
                u32::from_be_bytes([data[16], data[17], data[18], data[19]]),
                u32::from_be_bytes([data[20], data[21], data[22], data[23]]),
            ],
        })
    }
}

// ============================================================================
// WAL Shared Memory
// ============================================================================

/// Shared memory region for WAL index
pub struct WalShmRegion {
    /// Region data
    pub data: Vec<u8>,
    /// Region index
    pub index: usize,
}

/// WAL shared memory (WAL-index)
pub struct WalShm {
    /// Shared memory regions
    pub regions: Vec<WalShmRegion>,
    /// Read marks for each reader slot
    pub read_marks: [u32; WAL_NREADER],
}

impl WalShm {
    /// Create new shared memory
    pub fn new() -> Self {
        WalShm {
            regions: Vec::new(),
            read_marks: [0; WAL_NREADER],
        }
    }

    /// Get or create a region
    pub fn get_region(&mut self, index: usize, size: usize) -> &mut WalShmRegion {
        while self.regions.len() <= index {
            self.regions.push(WalShmRegion {
                data: vec![0u8; size],
                index: self.regions.len(),
            });
        }
        &mut self.regions[index]
    }

    /// Get header region
    pub fn get_header_region(&mut self) -> &mut WalShmRegion {
        self.get_region(0, 32768)
    }
}

impl Default for WalShm {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// WAL Hash Table
// ============================================================================

/// Hash table entry in WAL index
#[derive(Debug, Clone, Copy, Default)]
pub struct WalHashEntry {
    /// Page number (0 if empty)
    pub pgno: Pgno,
    /// Frame number
    pub frame: u32,
}

/// WAL hash table for page lookups
pub struct WalHashTable {
    /// Hash table slots
    slots: Vec<WalHashEntry>,
    /// Page number to frame mapping (for iteration)
    pages: Vec<u32>,
}

impl WalHashTable {
    /// Create a new hash table
    pub fn new() -> Self {
        WalHashTable {
            slots: vec![WalHashEntry::default(); HASHTABLE_NSLOT],
            pages: vec![0; HASHTABLE_NPAGE],
        }
    }

    /// Hash function for page numbers
    fn hash(pgno: Pgno) -> usize {
        (pgno as usize) % HASHTABLE_NSLOT
    }

    /// Insert a page -> frame mapping
    pub fn insert(&mut self, pgno: Pgno, frame: u32) {
        let mut idx = Self::hash(pgno);
        // Linear probing
        for _ in 0..HASHTABLE_NSLOT {
            if self.slots[idx].pgno == 0 || self.slots[idx].pgno == pgno {
                self.slots[idx] = WalHashEntry { pgno, frame };
                return;
            }
            idx = (idx + 1) % HASHTABLE_NSLOT;
        }
        // Table full - should not happen in normal operation
    }

    /// Lookup frame for a page
    pub fn lookup(&self, pgno: Pgno) -> Option<u32> {
        let mut idx = Self::hash(pgno);
        for _ in 0..HASHTABLE_NSLOT {
            if self.slots[idx].pgno == pgno {
                return Some(self.slots[idx].frame);
            }
            if self.slots[idx].pgno == 0 {
                return None;
            }
            idx = (idx + 1) % HASHTABLE_NSLOT;
        }
        None
    }

    /// Clear the hash table
    pub fn clear(&mut self) {
        for slot in &mut self.slots {
            *slot = WalHashEntry::default();
        }
        for page in &mut self.pages {
            *page = 0;
        }
    }
}

impl Default for WalHashTable {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// WAL Connection
// ============================================================================

/// Main WAL connection structure
pub struct Wal {
    // File handles
    /// WAL file handle
    pub wal_fd: Option<Box<dyn VfsFile>>,

    // Paths
    /// Database path
    pub db_path: String,
    /// WAL file path
    pub wal_path: String,

    // WAL state
    /// WAL-index header
    pub header: WalIndexHdr,
    /// WAL file header
    pub wal_header: Option<WalHeader>,
    /// Maximum valid frame number
    pub max_frame: u32,
    /// Minimum valid frame for current transaction
    pub min_frame: u32,
    /// Checkpoint sequence counter
    pub n_ckpt: u32,

    // Lock state
    /// Current read lock index (-1 = none)
    pub read_lock: i16,
    /// Holding write lock
    pub write_lock: bool,
    /// Holding checkpoint lock
    pub ckpt_lock: bool,

    // Page tracking
    /// Database page size
    pub page_size: u32,
    /// Hash tables for page lookups
    pub hash_tables: Vec<WalHashTable>,

    // Checksum state
    /// Use big-endian checksums
    pub big_endian_cksum: bool,
    /// Running checksum values
    pub checksum: [u32; 2],

    // Shared memory
    /// WAL-index shared memory
    pub shm: WalShm,

    // Callback state
    /// Number of pages written in current transaction
    pub n_written: u32,
    /// Truncate point requested
    pub truncate_on_commit: bool,
}

impl Wal {
    // ========================================================================
    // Initialization
    // ========================================================================

    /// Open WAL for a database (sqlite3WalOpen)
    pub fn open(db_path: &str, page_size: u32) -> Result<Self> {
        let wal_path = format!("{}-wal", db_path);

        Ok(Wal {
            wal_fd: None,
            db_path: db_path.to_string(),
            wal_path,
            header: WalIndexHdr::new(page_size),
            wal_header: None,
            max_frame: 0,
            min_frame: 0,
            n_ckpt: 0,
            read_lock: WAL_READ_LOCK_NONE,
            write_lock: false,
            ckpt_lock: false,
            page_size,
            hash_tables: vec![WalHashTable::new()],
            big_endian_cksum: false,
            checksum: [0, 0],
            shm: WalShm::new(),
            n_written: 0,
            truncate_on_commit: false,
        })
    }

    /// Close WAL connection (sqlite3WalClose)
    pub fn close(&mut self) -> Result<()> {
        // End any active transactions
        if self.read_lock != WAL_READ_LOCK_NONE {
            self.end_read_transaction()?;
        }
        if self.write_lock {
            self.end_write_transaction()?;
        }

        // Close file handle
        self.wal_fd = None;

        Ok(())
    }

    // ========================================================================
    // Read Transactions
    // ========================================================================

    /// Begin a read transaction (sqlite3WalBeginReadTransaction)
    pub fn begin_read_transaction(&mut self) -> Result<bool> {
        if self.read_lock != WAL_READ_LOCK_NONE {
            // Already have a read lock
            return Ok(false);
        }

        // Acquire read lock on first slot
        // In a real implementation, this would use shared memory locks
        // and try multiple slots if one is busy
        let slot = 0;
        self.read_lock = slot;
        self.shm.read_marks[slot as usize] = self.max_frame;
        Ok(true)
    }

    /// End a read transaction (sqlite3WalEndReadTransaction)
    pub fn end_read_transaction(&mut self) -> Result<()> {
        if self.read_lock == WAL_READ_LOCK_NONE {
            return Ok(());
        }

        // Release the read lock
        let slot = self.read_lock as usize;
        if slot < WAL_NREADER {
            self.shm.read_marks[slot] = 0;
        }
        self.read_lock = WAL_READ_LOCK_NONE;

        Ok(())
    }

    /// Find frame containing a page (sqlite3WalFindFrame)
    pub fn find_frame(&self, pgno: Pgno) -> Result<u32> {
        if pgno == 0 {
            return Err(Error::new(ErrorCode::Corrupt));
        }

        // Search hash tables from newest to oldest
        for table in self.hash_tables.iter().rev() {
            if let Some(frame) = table.lookup(pgno) {
                // Check if frame is within our read snapshot
                if self.read_lock != WAL_READ_LOCK_NONE {
                    let read_mark = self.shm.read_marks[self.read_lock as usize];
                    if frame <= read_mark {
                        return Ok(frame);
                    }
                } else {
                    return Ok(frame);
                }
            }
        }

        // Page not in WAL
        Ok(0)
    }

    /// Read a frame from WAL (sqlite3WalReadFrame)
    pub fn read_frame(&mut self, frame: u32, buf: &mut [u8]) -> Result<()> {
        if frame == 0 || frame > self.max_frame {
            return Err(Error::new(ErrorCode::Corrupt));
        }

        // Calculate offset before borrowing wal_fd mutably
        let offset = self.frame_offset(frame);

        if let Some(ref mut fd) = self.wal_fd {
            // Skip frame header, read page data
            fd.read(buf, offset + WAL_FRAME_HEADER_SIZE as i64)?;
            Ok(())
        } else {
            Err(Error::new(ErrorCode::IoErr))
        }
    }

    // ========================================================================
    // Write Transactions
    // ========================================================================

    /// Begin a write transaction (sqlite3WalBeginWriteTransaction)
    pub fn begin_write_transaction(&mut self) -> Result<()> {
        if self.write_lock {
            return Ok(());
        }

        // Must have a read lock first
        if self.read_lock == WAL_READ_LOCK_NONE {
            self.begin_read_transaction()?;
        }

        // Acquire write lock (exclusive)
        // In a real implementation, this would use shared memory locks
        self.write_lock = true;
        self.n_written = 0;

        Ok(())
    }

    /// End a write transaction (sqlite3WalEndWriteTransaction)
    pub fn end_write_transaction(&mut self) -> Result<()> {
        if !self.write_lock {
            return Ok(());
        }

        self.write_lock = false;
        self.n_written = 0;

        Ok(())
    }

    /// Write frames to WAL (sqlite3WalFrames)
    pub fn write_frames(
        &mut self,
        page_size: u32,
        pages: &[(Pgno, &[u8])],
        n_truncate: Pgno,
        is_commit: bool,
        sync_flags: SyncFlags,
    ) -> Result<()> {
        if !self.write_lock {
            return Err(Error::new(ErrorCode::Misuse));
        }

        // Create WAL file if needed
        if self.wal_fd.is_none() {
            self.create_wal_file(page_size)?;
        }

        let salt = [
            self.wal_header.as_ref().map_or(0, |h| h.salt1),
            self.wal_header.as_ref().map_or(0, |h| h.salt2),
        ];

        // Write each frame
        for (i, (pgno, data)) in pages.iter().enumerate() {
            let is_last = i == pages.len() - 1;
            let commit_size = if is_last && is_commit { n_truncate } else { 0 };

            self.write_frame(*pgno, data, commit_size, salt)?;
        }

        // Sync if requested
        if is_commit && !sync_flags.is_empty() {
            if let Some(ref mut fd) = self.wal_fd {
                fd.sync(sync_flags)?;
            }
        }

        Ok(())
    }

    /// Write a single frame
    fn write_frame(
        &mut self,
        pgno: Pgno,
        data: &[u8],
        n_truncate: u32,
        salt: [u32; 2],
    ) -> Result<()> {
        self.max_frame += 1;
        let frame = self.max_frame;

        // Create frame header
        let mut frame_hdr = WalFrameHdr::new(pgno, n_truncate, salt);

        // Calculate checksum over header (first 8 bytes) + data
        let hdr_bytes = frame_hdr.to_bytes();
        let (c1, c2) = wal_checksum(
            self.big_endian_cksum,
            &hdr_bytes[0..8],
            self.checksum[0],
            self.checksum[1],
        );
        let (c1, c2) = wal_checksum(self.big_endian_cksum, data, c1, c2);

        frame_hdr.checksum = [c1, c2];
        self.checksum = [c1, c2];

        // Calculate offset before borrowing wal_fd mutably
        let offset = self.frame_offset(frame);

        // Write to WAL file
        if let Some(ref mut fd) = self.wal_fd {
            fd.write(&frame_hdr.to_bytes(), offset)?;
            fd.write(data, offset + WAL_FRAME_HEADER_SIZE as i64)?;
        }

        // Update hash table
        let table_idx = ((frame - 1) / HASHTABLE_NPAGE as u32) as usize;
        while self.hash_tables.len() <= table_idx {
            self.hash_tables.push(WalHashTable::new());
        }
        self.hash_tables[table_idx].insert(pgno, frame);

        // Update header
        self.header.max_frame = frame;
        if n_truncate > 0 {
            self.header.n_page = n_truncate;
        }
        self.header.frame_cksum = [c1, c2];

        self.n_written += 1;

        Ok(())
    }

    /// Undo a write transaction (sqlite3WalUndo)
    pub fn undo(&mut self, callback: impl Fn(Pgno) -> Result<()>) -> Result<()> {
        if !self.write_lock {
            return Ok(());
        }

        // Walk backwards through frames written in this transaction
        let target_frame = self.max_frame - self.n_written;

        while self.max_frame > target_frame {
            // Find page number for this frame and call callback
            // The callback should reload the page from the database file
            // For now, just decrement the frame count
            self.max_frame -= 1;
        }

        // Clear hash table entries for undone frames
        // This is a simplified version - real implementation would be more precise
        let _ = callback;

        self.header.max_frame = self.max_frame;
        self.n_written = 0;

        Ok(())
    }

    // ========================================================================
    // Checkpointing
    // ========================================================================

    /// Run a checkpoint (sqlite3WalCheckpoint)
    pub fn checkpoint(
        &mut self,
        db_fd: &mut dyn VfsFile,
        mode: CheckpointMode,
        busy_handler: Option<&dyn Fn() -> bool>,
    ) -> Result<(i32, i32)> {
        // Acquire checkpoint lock
        if self.ckpt_lock {
            return Err(Error::new(ErrorCode::Busy));
        }
        self.ckpt_lock = true;

        let result = self.do_checkpoint(db_fd, mode, busy_handler);

        self.ckpt_lock = false;

        result
    }

    /// Internal checkpoint implementation
    fn do_checkpoint(
        &mut self,
        db_fd: &mut dyn VfsFile,
        mode: CheckpointMode,
        busy_handler: Option<&dyn Fn() -> bool>,
    ) -> Result<(i32, i32)> {
        if self.max_frame == 0 {
            return Ok((0, 0));
        }

        // Find the frame up to which we can checkpoint
        // This depends on active readers
        let mut safe_frame = self.max_frame;
        for &read_mark in &self.shm.read_marks {
            if read_mark > 0 && read_mark < safe_frame {
                if mode == CheckpointMode::Passive {
                    safe_frame = read_mark - 1;
                } else {
                    // Wait for readers to finish
                    if let Some(handler) = busy_handler {
                        while read_mark < safe_frame {
                            if !handler() {
                                safe_frame = read_mark - 1;
                                break;
                            }
                        }
                    } else {
                        safe_frame = read_mark - 1;
                    }
                }
            }
        }

        if safe_frame == 0 {
            return Ok((self.max_frame as i32, 0));
        }

        // Copy frames to database
        let mut frames_copied = 0u32;
        let mut frame_buf = vec![0u8; self.page_size as usize];

        for frame in 1..=safe_frame {
            // Calculate offset before borrowing wal_fd mutably
            let offset = self.frame_offset(frame);

            // Read frame header to get page number
            if let Some(ref mut fd) = self.wal_fd {
                let mut hdr_buf = [0u8; WAL_FRAME_HEADER_SIZE];
                fd.read(&mut hdr_buf, offset)?;

                let frame_hdr = WalFrameHdr::from_bytes(&hdr_buf)?;

                // Read page data
                fd.read(&mut frame_buf, offset + WAL_FRAME_HEADER_SIZE as i64)?;

                // Write to database file
                let db_offset = ((frame_hdr.pgno - 1) as i64) * (self.page_size as i64);
                db_fd.write(&frame_buf, db_offset)?;

                frames_copied += 1;
            }
        }

        // Sync database file
        db_fd.sync(SyncFlags::NORMAL)?;

        // Handle WAL reset based on mode
        if frames_copied == self.max_frame {
            match mode {
                CheckpointMode::Restart | CheckpointMode::Truncate => {
                    self.reset_wal(mode == CheckpointMode::Truncate)?;
                }
                _ => {}
            }
        }

        Ok((self.max_frame as i32, frames_copied as i32))
    }

    /// Reset WAL after full checkpoint
    fn reset_wal(&mut self, truncate: bool) -> Result<()> {
        // Clear hash tables
        for table in &mut self.hash_tables {
            table.clear();
        }

        // Reset state
        self.max_frame = 0;
        self.min_frame = 0;
        self.checksum = [0, 0];
        self.header.max_frame = 0;
        self.header.frame_cksum = [0, 0];

        // Increment checkpoint sequence
        self.n_ckpt += 1;

        if truncate {
            // Truncate WAL file
            if let Some(ref mut fd) = self.wal_fd {
                fd.truncate(0)?;
            }
            self.wal_header = None;
        } else {
            // Write new header with new salt
            self.create_wal_file(self.page_size)?;
        }

        Ok(())
    }

    // ========================================================================
    // Recovery
    // ========================================================================

    /// Recover WAL index from WAL file (walIndexRecover)
    pub fn recover(&mut self) -> Result<()> {
        // Read and validate WAL header
        if let Some(ref mut fd) = self.wal_fd {
            let mut hdr_buf = [0u8; WAL_HEADER_SIZE];
            let n = fd.read(&mut hdr_buf, 0)?;
            if n < WAL_HEADER_SIZE {
                // Empty or truncated WAL
                return Ok(());
            }

            let wal_hdr = WalHeader::from_bytes(&hdr_buf)?;
            wal_hdr.validate()?;

            self.wal_header = Some(wal_hdr);
            self.page_size = wal_hdr.page_size;
            self.big_endian_cksum = wal_hdr.is_big_endian();
            self.n_ckpt = wal_hdr.checkpoint_seq;

            // Initialize checksum with salt
            self.checksum = [0, 0];
            let (c1, c2) = wal_checksum(self.big_endian_cksum, &hdr_buf[0..24], 0, 0);
            self.checksum = [c1, c2];

            // Read and validate frames
            let frame_size = WAL_FRAME_HEADER_SIZE + self.page_size as usize;
            let mut offset = WAL_HEADER_SIZE as i64;
            let mut frame_buf = vec![0u8; frame_size];
            let mut frame_num = 0u32;

            loop {
                let n = fd.read(&mut frame_buf, offset)?;
                if n < frame_size {
                    break;
                }

                let frame_hdr = WalFrameHdr::from_bytes(&frame_buf)?;

                // Validate salt
                if frame_hdr.salt[0] != wal_hdr.salt1 || frame_hdr.salt[1] != wal_hdr.salt2 {
                    break;
                }

                // Validate checksum
                let (c1, c2) = wal_checksum(
                    self.big_endian_cksum,
                    &frame_buf[0..8],
                    self.checksum[0],
                    self.checksum[1],
                );
                let (c1, c2) = wal_checksum(
                    self.big_endian_cksum,
                    &frame_buf[WAL_FRAME_HEADER_SIZE..],
                    c1,
                    c2,
                );

                if c1 != frame_hdr.checksum[0] || c2 != frame_hdr.checksum[1] {
                    break;
                }

                self.checksum = [c1, c2];
                frame_num += 1;

                // Add to hash table
                let table_idx = ((frame_num - 1) / HASHTABLE_NPAGE as u32) as usize;
                while self.hash_tables.len() <= table_idx {
                    self.hash_tables.push(WalHashTable::new());
                }
                self.hash_tables[table_idx].insert(frame_hdr.pgno, frame_num);

                // Update header info
                self.max_frame = frame_num;
                if frame_hdr.n_truncate > 0 {
                    self.header.n_page = frame_hdr.n_truncate;
                }

                offset += frame_size as i64;
            }

            self.header.max_frame = self.max_frame;
            self.header.frame_cksum = self.checksum;
            self.header.is_init = 1;
        }

        Ok(())
    }

    // ========================================================================
    // Utilities
    // ========================================================================

    /// Create a new WAL file
    fn create_wal_file(&mut self, page_size: u32) -> Result<()> {
        // In a real implementation, this would use VFS to create the file
        let hdr = WalHeader::new(page_size, self.n_ckpt);
        self.wal_header = Some(hdr);
        self.page_size = page_size;
        self.big_endian_cksum = hdr.is_big_endian();

        // Initialize checksum
        let hdr_bytes = hdr.to_bytes();
        let (c1, c2) = wal_checksum(self.big_endian_cksum, &hdr_bytes[0..24], 0, 0);
        self.checksum = [c1, c2];

        self.header = WalIndexHdr::new(page_size);
        self.header.salt = [hdr.salt1, hdr.salt2];

        Ok(())
    }

    /// Calculate frame offset in WAL file
    fn frame_offset(&self, frame: u32) -> i64 {
        let frame_size = WAL_FRAME_HEADER_SIZE as i64 + self.page_size as i64;
        WAL_HEADER_SIZE as i64 + ((frame - 1) as i64) * frame_size
    }

    /// Get current database size from WAL
    pub fn db_size(&self) -> Pgno {
        self.header.n_page
    }

    /// Check if WAL is empty
    pub fn is_empty(&self) -> bool {
        self.max_frame == 0
    }

    /// Get WAL file path
    pub fn wal_path(&self) -> &str {
        &self.wal_path
    }
}

// ============================================================================
// Checksum Functions
// ============================================================================

/// Generate a random salt value
fn generate_salt() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    ((duration.as_nanos() >> 16) & 0xFFFFFFFF) as u32
}

/// Calculate WAL checksum
///
/// This implements SQLite's WAL checksum algorithm. The checksum is computed
/// over pairs of 32-bit words.
fn wal_checksum(big_endian: bool, data: &[u8], init1: u32, init2: u32) -> (u32, u32) {
    let mut s1 = init1;
    let mut s2 = init2;

    // Process data in 8-byte chunks
    let chunks = data.len() / 8;
    for i in 0..chunks {
        let offset = i * 8;
        let (w1, w2) = if big_endian {
            (
                u32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]),
                u32::from_be_bytes([
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]),
            )
        } else {
            (
                u32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]),
                u32::from_le_bytes([
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]),
            )
        };

        s1 = s1.wrapping_add(w1).wrapping_add(s2);
        s2 = s2.wrapping_add(w2).wrapping_add(s1);
    }

    (s1, s2)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_mode() {
        assert_eq!(CheckpointMode::from_i32(0), Some(CheckpointMode::Passive));
        assert_eq!(CheckpointMode::from_i32(1), Some(CheckpointMode::Full));
        assert_eq!(CheckpointMode::from_i32(2), Some(CheckpointMode::Restart));
        assert_eq!(CheckpointMode::from_i32(3), Some(CheckpointMode::Truncate));
        assert_eq!(CheckpointMode::from_i32(99), None);
    }

    #[test]
    fn test_wal_index_hdr_roundtrip() {
        let hdr = WalIndexHdr {
            version: WAL_VERSION,
            unused: 0,
            change: 42,
            is_init: 1,
            big_endian_cksum: 0,
            page_size: 4096,
            max_frame: 100,
            n_page: 50,
            frame_cksum: [0x12345678, 0x9abcdef0],
            salt: [0xdeadbeef, 0xcafebabe],
            cksum: [0x11111111, 0x22222222],
        };

        let bytes = hdr.to_bytes();
        let parsed = WalIndexHdr::from_bytes(&bytes).unwrap();

        assert_eq!(hdr.version, parsed.version);
        assert_eq!(hdr.change, parsed.change);
        assert_eq!(hdr.is_init, parsed.is_init);
        assert_eq!(hdr.page_size, parsed.page_size);
        assert_eq!(hdr.max_frame, parsed.max_frame);
        assert_eq!(hdr.n_page, parsed.n_page);
        assert_eq!(hdr.frame_cksum, parsed.frame_cksum);
        assert_eq!(hdr.salt, parsed.salt);
        assert_eq!(hdr.cksum, parsed.cksum);
    }

    #[test]
    fn test_wal_header_roundtrip() {
        let hdr = WalHeader::new(4096, 1);
        let bytes = hdr.to_bytes();
        let parsed = WalHeader::from_bytes(&bytes).unwrap();

        assert_eq!(hdr.magic, parsed.magic);
        assert_eq!(hdr.version, parsed.version);
        assert_eq!(hdr.page_size, parsed.page_size);
        assert_eq!(hdr.checkpoint_seq, parsed.checkpoint_seq);
        assert_eq!(hdr.salt1, parsed.salt1);
        assert_eq!(hdr.salt2, parsed.salt2);
    }

    #[test]
    fn test_wal_header_validation() {
        let hdr = WalHeader::new(4096, 1);
        assert!(hdr.validate().is_ok());

        // Corrupt checksum
        let mut bad_hdr = hdr;
        bad_hdr.checksum1 ^= 0xFFFFFFFF;
        assert!(bad_hdr.validate().is_err());
    }

    #[test]
    fn test_wal_frame_hdr_roundtrip() {
        let frame = WalFrameHdr {
            pgno: 42,
            n_truncate: 100,
            salt: [0xdeadbeef, 0xcafebabe],
            checksum: [0x12345678, 0x9abcdef0],
        };

        let bytes = frame.to_bytes();
        let parsed = WalFrameHdr::from_bytes(&bytes).unwrap();

        assert_eq!(frame.pgno, parsed.pgno);
        assert_eq!(frame.n_truncate, parsed.n_truncate);
        assert_eq!(frame.salt, parsed.salt);
        assert_eq!(frame.checksum, parsed.checksum);
    }

    #[test]
    fn test_frame_is_commit() {
        let commit_frame = WalFrameHdr::new(1, 100, [0, 0]);
        assert!(commit_frame.is_commit());

        let regular_frame = WalFrameHdr::new(1, 0, [0, 0]);
        assert!(!regular_frame.is_commit());
    }

    #[test]
    fn test_hash_table() {
        let mut table = WalHashTable::new();

        table.insert(1, 100);
        table.insert(2, 200);
        table.insert(3, 300);

        assert_eq!(table.lookup(1), Some(100));
        assert_eq!(table.lookup(2), Some(200));
        assert_eq!(table.lookup(3), Some(300));
        assert_eq!(table.lookup(4), None);

        // Update existing entry
        table.insert(2, 250);
        assert_eq!(table.lookup(2), Some(250));
    }

    #[test]
    fn test_wal_checksum() {
        // Test that checksum is deterministic
        let data = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let (c1, c2) = wal_checksum(false, &data, 0, 0);
        let (c1_2, c2_2) = wal_checksum(false, &data, 0, 0);
        assert_eq!(c1, c1_2);
        assert_eq!(c2, c2_2);

        // Test that different data gives different checksum
        let data2 = [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01];
        let (c1_3, c2_3) = wal_checksum(false, &data2, 0, 0);
        assert!(c1 != c1_3 || c2 != c2_3);
    }

    #[test]
    fn test_wal_open() {
        let wal = Wal::open("/tmp/test.db", 4096).unwrap();
        assert_eq!(wal.page_size, 4096);
        assert_eq!(wal.max_frame, 0);
        assert_eq!(wal.read_lock, WAL_READ_LOCK_NONE);
        assert!(!wal.write_lock);
        assert!(wal.is_empty());
    }

    #[test]
    fn test_wal_path() {
        let wal = Wal::open("/tmp/test.db", 4096).unwrap();
        assert_eq!(wal.wal_path(), "/tmp/test.db-wal");
    }
}
