//! B-tree implementation

use std::ptr::NonNull;
use std::sync::{Arc, Weak, RwLock};

use bitflags::bitflags;

use crate::error::{Error, ErrorCode, Result};
use crate::storage::pager::{Pager, PagerFlags, PagerGetFlags, PagerOpenFlags, SavepointOp};
use crate::types::{Connection, OpenFlags, Pgno, RowId, Value, Vfs};

const BTREE_PAGEFLAG_INTKEY: u8 = 0x01;
const BTREE_PAGEFLAG_ZERODATA: u8 = 0x02;
const BTREE_PAGEFLAG_LEAFDATA: u8 = 0x04;
const BTREE_PAGEFLAG_LEAF: u8 = 0x08;

pub const BTREE_AUTOVACUUM_NONE: u8 = 0;
pub const BTREE_AUTOVACUUM_FULL: u8 = 1;
pub const BTREE_AUTOVACUUM_INCR: u8 = 2;

pub const BTREE_INTKEY: u8 = 1;
pub const BTREE_BLOBKEY: u8 = 2;
pub const BTREE_HINT_RANGE: u8 = 0;

pub const BTREE_FREE_PAGE_COUNT: usize = 0;
pub const BTREE_SCHEMA_VERSION: usize = 1;
pub const BTREE_FILE_FORMAT: usize = 2;
pub const BTREE_DEFAULT_CACHE_SIZE: usize = 3;
pub const BTREE_LARGEST_ROOT_PAGE: usize = 4;
pub const BTREE_TEXT_ENCODING: usize = 5;
pub const BTREE_USER_VERSION: usize = 6;
pub const BTREE_INCR_VACUUM: usize = 7;
pub const BTREE_APPLICATION_ID: usize = 8;
pub const BTREE_DATA_VERSION: usize = 15;
pub const SQLITE_N_BTREE_META: usize = 16;
pub const BTCURSOR_MAX_DEPTH: usize = 20;
pub const BT_MAX_LOCAL: u16 = 65501;

pub const PTRMAP_ROOTPAGE: u8 = 1;
pub const PTRMAP_FREEPAGE: u8 = 2;
pub const PTRMAP_OVERFLOW1: u8 = 3;
pub const PTRMAP_OVERFLOW2: u8 = 4;
pub const PTRMAP_BTREE: u8 = 5;
pub const SQLITE_FILE_HEADER: &[u8; 16] = b"SQLite format 3\0";

bitflags! {
    #[derive(Clone, Copy)]
    pub struct BtreeOpenFlags: u8 {
        const OMIT_JOURNAL = 0x01;
        const MEMORY = 0x02;
        const SINGLE = 0x04;
        const UNORDERED = 0x08;
    }
}

bitflags! {
    pub struct BtsFlags: u16 {
        const READ_ONLY = 0x0001;
        const PAGESIZE_FIXED = 0x0002;
        const SECURE_DELETE = 0x0004;
        const OVERWRITE = 0x0008;
        const FAST_SECURE = 0x000c;
        const INITIALLY_EMPTY = 0x0010;
        const NO_WAL = 0x0020;
        const EXCLUSIVE = 0x0040;
        const PENDING = 0x0080;
    }
}

bitflags! {
    pub struct BtreeCursorFlags: u32 {
        const BULKLOAD = 0x0000_0001;
        const SEEK_EQ = 0x0000_0002;
        const WRCSR = 0x0000_0004;
        const FORDELETE = 0x0000_0008;
    }
}

bitflags! {
    pub struct BtreeInsertFlags: u8 {
        const SAVEPOSITION = 0x02;
        const AUXDELETE = 0x04;
        const APPEND = 0x08;
        const PREFORMAT = 0x80;
    }
}

bitflags! {
    pub struct BtCursorFlags: u8 {
        const WRITE = 0x01;
        const VALID_NKEY = 0x02;
        const VALID_OVFL = 0x04;
        const AT_LAST = 0x08;
        const INCRBLOB = 0x10;
        const MULTIPLE = 0x20;
        const PINNED = 0x40;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum TransState {
    None = 0,
    Read = 1,
    Write = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum CursorState {
    Valid = 0,
    Invalid = 1,
    SkipNext = 2,
    RequireSeek = 3,
    Fault = 4,
}

bitflags! {
    pub struct CursorHints: u8 {
        const NONE = 0;
        const BULKLOAD = 0x01;
        const SEEK_EQ = 0x02;
    }
}

pub struct Btree {
    pub db: Option<Arc<dyn Connection>>,
    pub shared: Arc<RwLock<BtShared>>,
    pub in_trans: TransState,
    pub sharable: bool,
    pub locked: bool,
    pub has_incrblob_cur: bool,
    pub want_to_lock: i32,
    pub n_backup: i32,
    pub data_version: u32,
    pub next: Option<NonNull<Btree>>,
    pub prev: Option<NonNull<Btree>>,
}

pub struct BtShared {
    pub pager: Pager,
    pub db: Option<Weak<dyn Connection>>,
    pub cursor_list: Vec<BtCursor>,
    pub page1: Option<MemPage>,
    pub open_flags: BtreeOpenFlags,
    pub auto_vacuum: u8,
    pub incr_vacuum: u8,
    pub do_truncate: bool,
    pub in_transaction: TransState,
    pub max_payload_1byte: u8,
    pub reserve_wanted: u8,
    pub bts_flags: BtsFlags,
    pub max_local: u16,
    pub min_local: u16,
    pub max_leaf: u16,
    pub min_leaf: u16,
    pub page_size: u32,
    pub usable_size: u32,
    pub n_transaction: i32,
    pub n_page: u32,
    pub schema: Option<Arc<dyn std::any::Any>>,
    pub has_content: Option<Vec<u8>>,
    pub temp_space: Vec<u8>,
    pub preformat_size: i32,
    pub schema_cookie: u32,
    pub file_format: u8,
}

impl BtShared {
    fn update_payload_params(&mut self) {
        let usable = self.usable_size;
        if usable < 480 {
            return;
        }
        let usable_minus = usable.saturating_sub(12);
        let max_local = usable_minus.saturating_mul(64) / 255;
        let min_local = usable_minus.saturating_mul(32) / 255;
        self.max_local = max_local.saturating_sub(23) as u16;
        self.min_local = min_local.saturating_sub(23) as u16;
        self.max_leaf = usable.saturating_sub(35) as u16;
        self.min_leaf = self.min_local;
        self.max_payload_1byte = if self.max_local > 127 {
            127
        } else {
            self.max_local as u8
        };
    }
}

pub struct BtCursor {
    pub state: CursorState,
    pub cur_flags: BtCursorFlags,
    pub cur_pager_flags: PagerGetFlags,
    pub hints: CursorHints,
    pub skip_next: i32,
    pub btree: Arc<Btree>,
    pub overflow: Vec<Pgno>,
    pub key: Option<Vec<u8>>,
    pub bt_shared: Weak<RwLock<BtShared>>,
    pub next: Option<NonNull<BtCursor>>,
    pub info: CellInfo,
    pub n_key: i64,
    pub root_page: Pgno,
    pub i_page: i8,
    pub cur_int_key: bool,
    pub ix: u16,
    pub idx_stack: Vec<u16>,
    pub key_info: Option<Arc<KeyInfo>>,
    pub page: Option<MemPage>,
    pub page_stack: Vec<MemPage>,
}

pub struct MemPage {
    pub pgno: Pgno,
    pub data: Vec<u8>,
    pub is_init: bool,
    pub is_leaf: bool,
    pub is_intkey: bool,
    pub is_leafdata: bool,
    pub is_zerodata: bool,
    pub hdr_offset: u8,
    pub child_ptr_size: u8,
    pub max_local: u16,
    pub min_local: u16,
    pub n_cell: u16,
    pub cell_offset: u16,
    pub free_bytes: u16,
    pub rightmost_ptr: Option<Pgno>,
    pub n_overflow: u8,
    pub first_freeblock: u16,
    pub mask_page: u16,
    pub n_free: i32,
}

pub struct BtreePayload {
    pub key: Option<Vec<u8>>,
    pub n_key: RowId,
    pub data: Option<Vec<u8>>,
    pub mem: Vec<Value>,
    pub n_data: i32,
    pub n_zero: i32,
}

pub struct CellInfo {
    pub n_key: i64,
    pub payload: Option<Vec<u8>>,
    pub n_payload: u32,
    pub n_local: u16,
    pub n_size: u16,
}

impl Default for CellInfo {
    fn default() -> Self {
        Self {
            n_key: 0,
            payload: None,
            n_payload: 0,
            n_local: 0,
            n_size: 0,
        }
    }
}

pub struct KeyInfo {
    pub encoding: u8,
}

pub struct UnpackedRecord {
    pub key: Vec<u8>,
}

pub struct IntegrityCheckResult {
    pub errors: Vec<String>,
}

pub struct DbHeader {
    pub page_size: u32,
    pub reserve: u8,
    pub file_format: u8,
    pub schema_cookie: u32,
    pub auto_vacuum: u8,
    pub incr_vacuum: u8,
}

impl DbHeader {
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < 100 {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        let mut page_size = read_u16(data, 16).ok_or(Error::new(ErrorCode::Corrupt))? as u32;
        if page_size == 1 {
            page_size = 65536;
        }
        if page_size < 512 || page_size > 65536 || !page_size.is_power_of_two() {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        let reserve = data[20];
        let file_format = data[18];
        let schema_cookie = read_u32(data, 40).ok_or(Error::new(ErrorCode::Corrupt))?;
        let auto_vacuum = if read_u32(data, 36 + 4 * 4).unwrap_or(0) != 0 {
            1
        } else {
            0
        };
        let incr_vacuum = if read_u32(data, 36 + 7 * 4).unwrap_or(0) != 0 {
            1
        } else {
            0
        };
        Ok(Self {
            page_size,
            reserve,
            file_format,
            schema_cookie,
            auto_vacuum,
            incr_vacuum,
        })
    }
}

pub fn integrity_check(
    _db: &dyn Connection,
    _btree: &Btree,
    _roots: &[Pgno],
    _max_errors: i32,
) -> Result<IntegrityCheckResult> {
    Err(Error::new(ErrorCode::Internal))
}

pub fn fake_valid_cursor(btree: Arc<Btree>) -> BtCursor {
    BtCursor {
        state: CursorState::Valid,
        cur_flags: BtCursorFlags::empty(),
        cur_pager_flags: PagerGetFlags::empty(),
        hints: CursorHints::empty(),
        skip_next: 0,
        btree: Arc::clone(&btree),
        overflow: Vec::new(),
        key: None,
        bt_shared: Arc::downgrade(&btree.shared),
        next: None,
        info: CellInfo::default(),
        n_key: 0,
        root_page: 0,
        i_page: -1,
        cur_int_key: false,
        ix: 0,
        idx_stack: Vec::new(),
        key_info: None,
        page: None,
        page_stack: Vec::new(),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PageLimits {
    pub page_size: u32,
    pub usable_size: u32,
    pub header_offset: usize,
}

impl PageLimits {
    pub fn new(page_size: u32, usable_size: u32) -> Self {
        Self {
            page_size,
            usable_size,
            header_offset: 0,
        }
    }

    pub fn for_page1(page_size: u32, usable_size: u32) -> Self {
        Self {
            page_size,
            usable_size,
            header_offset: 100,
        }
    }

    fn header_start(&self) -> usize {
        self.header_offset
    }

    fn usable_end(&self) -> usize {
        self.usable_size as usize
    }
}

fn read_u16(data: &[u8], offset: usize) -> Option<u16> {
    data.get(offset..offset + 2)
        .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u32(data: &[u8], offset: usize) -> Option<u32> {
    data.get(offset..offset + 4).map(|bytes| {
        u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    })
}

fn write_u32(data: &mut [u8], offset: usize, value: u32) -> Result<()> {
    let bytes = value.to_be_bytes();
    let target = data
        .get_mut(offset..offset + 4)
        .ok_or(Error::new(ErrorCode::Corrupt))?;
    target.copy_from_slice(&bytes);
    Ok(())
}

fn write_u16(data: &mut [u8], offset: usize, value: u16) -> Result<()> {
    let bytes = value.to_be_bytes();
    let target = data
        .get_mut(offset..offset + 2)
        .ok_or(Error::new(ErrorCode::Corrupt))?;
    target.copy_from_slice(&bytes);
    Ok(())
}

fn write_varint(mut value: u64, out: &mut Vec<u8>) {
    if value <= 0x7f {
        out.push(value as u8);
        return;
    }

    let mut buf = [0u8; 9];
    let mut i = 8;
    buf[i] = (value & 0xff) as u8;
    value >>= 8;
    while value > 0 {
        i -= 1;
        buf[i] = ((value & 0x7f) as u8) | 0x80;
        value >>= 7;
    }
    out.extend_from_slice(&buf[i..]);
}

fn build_cell(page: &MemPage, payload: &BtreePayload) -> Result<Vec<u8>> {
    if !page.is_leaf {
        return Err(Error::new(ErrorCode::Internal));
    }

    let mut cell = Vec::new();
    if page.is_intkey && page.is_leafdata {
        let data = payload.data.as_deref().unwrap_or(&[]);
        let payload_size = data.len() + payload.n_zero.max(0) as usize;
        write_varint(payload_size as u64, &mut cell);
        write_varint(payload.n_key as u64, &mut cell);
        cell.extend_from_slice(data);
        if payload.n_zero > 0 {
            cell.extend(std::iter::repeat(0u8).take(payload.n_zero as usize));
        }
        return Ok(cell);
    }

    if page.is_zerodata {
        let key = payload.key.as_deref().ok_or(Error::new(ErrorCode::Misuse))?;
        write_varint(key.len() as u64, &mut cell);
        cell.extend_from_slice(key);
        return Ok(cell);
    }

    Err(Error::new(ErrorCode::Internal))
}

fn read_varint(data: &[u8], offset: usize) -> Result<(u64, usize)> {
    let mut value = 0u64;
    for i in 0..9 {
        let byte = *data.get(offset + i).ok_or(Error::new(ErrorCode::Corrupt))?;
        if i == 8 {
            value = (value << 8) | byte as u64;
            return Ok((value, 9));
        }
        value = (value << 7) | (u64::from(byte & 0x7f));
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
    }
    Err(Error::new(ErrorCode::Corrupt))
}

fn read_varint32(data: &[u8], offset: usize) -> Result<(u32, usize)> {
    let (value, size) = read_varint(data, offset)?;
    if value > u32::MAX as u64 {
        return Err(Error::new(ErrorCode::Corrupt));
    }
    Ok((value as u32, size))
}

impl MemPage {
    pub fn parse(pgno: Pgno, data: Vec<u8>, limits: PageLimits) -> Result<Self> {
        Self::parse_with_shared(pgno, data, limits, None)
    }

    pub fn parse_with_shared(
        pgno: Pgno,
        data: Vec<u8>,
        limits: PageLimits,
        shared: Option<&BtShared>,
    ) -> Result<Self> {
        let header_start = limits.header_start();
        if data.len() < header_start + 8 {
            return Err(Error::new(ErrorCode::Corrupt));
        }

        let flags = data[header_start];
        let is_leaf = (flags & BTREE_PAGEFLAG_LEAF) != 0;
        let is_intkey = (flags & BTREE_PAGEFLAG_INTKEY) != 0;
        let is_zerodata = (flags & BTREE_PAGEFLAG_ZERODATA) != 0;
        let is_leafdata = (flags & BTREE_PAGEFLAG_LEAFDATA) != 0;
        let header_size = if is_leaf { 8 } else { 12 };

        if data.len() < header_start + header_size {
            return Err(Error::new(ErrorCode::Corrupt));
        }

        let first_freeblock =
            read_u16(&data, header_start + 1).ok_or(Error::new(ErrorCode::Corrupt))?;
        let n_cell = read_u16(&data, header_start + 3).ok_or(Error::new(ErrorCode::Corrupt))?;
        let cell_offset = read_u16(&data, header_start + 5).ok_or(Error::new(ErrorCode::Corrupt))?;
        let free_bytes = data[header_start + 7] as u16;
        let rightmost_ptr = if is_leaf {
            None
        } else {
            Some(read_u32(&data, header_start + 8).ok_or(Error::new(ErrorCode::Corrupt))?)
        };
        let child_ptr_size = if is_leaf { 0 } else { 4 };
        let mask_page = limits.page_size.wrapping_sub(1) as u16;

        let mut page = Self {
            pgno,
            data,
            is_init: true,
            is_leaf,
            is_intkey,
            is_leafdata,
            is_zerodata,
            hdr_offset: limits.header_offset as u8,
            child_ptr_size,
            max_local: 0,
            min_local: 0,
            n_cell,
            cell_offset,
            free_bytes,
            rightmost_ptr,
            n_overflow: 0,
            first_freeblock,
            mask_page,
            n_free: -1,
        };

        if let Some(shared) = shared {
            page.apply_shared(shared)?;
        }
        page.n_free = page.compute_free_space(limits)?;

        Ok(page)
    }

    pub fn cell_content_offset(&self, limits: PageLimits) -> Result<usize> {
        if self.cell_offset == 0 && limits.usable_size == 65536 {
            return Ok(65536);
        }
        Ok(self.cell_offset as usize)
    }

    pub fn header_size(&self) -> usize {
        if self.is_leaf {
            8
        } else {
            12
        }
    }

    pub fn cell_ptr(&self, index: u16, limits: PageLimits) -> Result<u16> {
        if index >= self.n_cell {
            return Err(Error::new(ErrorCode::Range));
        }
        let offset = limits.header_start() + self.header_size() + (index as usize * 2);
        read_u16(&self.data, offset).ok_or(Error::new(ErrorCode::Corrupt))
    }

    pub fn child_pgno(&self, cell_offset: u16) -> Result<Pgno> {
        if self.child_ptr_size == 0 {
            return Err(Error::new(ErrorCode::Misuse));
        }
        let start = cell_offset as usize;
        read_u32(&self.data, start).ok_or(Error::new(ErrorCode::Corrupt))
    }

    pub fn cell_offset_for_index(&self, index: u16, limits: PageLimits) -> Result<usize> {
        let ptr = self.cell_ptr(index, limits)? as usize;
        Ok((ptr & self.mask_page as usize) as usize)
    }

    pub fn cell_slice(&self, index: u16, limits: PageLimits) -> Result<&[u8]> {
        let offset = self.cell_offset_for_index(index, limits)?;
        if offset >= self.data.len() {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        Ok(&self.data[offset..])
    }

    pub fn cell_ptrs(&self, limits: PageLimits) -> Result<Vec<u16>> {
        let mut pointers = Vec::with_capacity(self.n_cell as usize);
        for i in 0..self.n_cell {
            pointers.push(self.cell_ptr(i, limits)?);
        }
        Ok(pointers)
    }

    pub fn validate_layout(&self, limits: PageLimits) -> Result<()> {
        if limits.page_size < limits.usable_size {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        if self.data.len() < limits.page_size as usize {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        let header_start = limits.header_start();
        let header_size = self.header_size();
        let usable_end = limits.usable_end();
        if header_start + header_size > usable_end {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        let ptr_array_end = header_start + header_size + (self.n_cell as usize * 2);
        if ptr_array_end > usable_end {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        let cell_offset = self.cell_content_offset(limits)?;
        if cell_offset < ptr_array_end {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        if cell_offset > usable_end {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        self.validate_freeblocks(limits, cell_offset)?;
        Ok(())
    }

    fn apply_shared(&mut self, shared: &BtShared) -> Result<()> {
        let flag_byte = (if self.is_leaf { BTREE_PAGEFLAG_LEAF } else { 0 })
            | (if self.is_intkey { BTREE_PAGEFLAG_INTKEY } else { 0 })
            | (if self.is_leafdata { BTREE_PAGEFLAG_LEAFDATA } else { 0 })
            | (if self.is_zerodata { BTREE_PAGEFLAG_ZERODATA } else { 0 });

        let is_table = (flag_byte & BTREE_PAGEFLAG_INTKEY != 0)
            && (flag_byte & BTREE_PAGEFLAG_LEAFDATA != 0);
        let is_index = (flag_byte & BTREE_PAGEFLAG_ZERODATA != 0);

        if self.is_leaf {
            self.child_ptr_size = 0;
            if is_table {
                self.max_local = shared.max_leaf;
                self.min_local = shared.min_leaf;
            } else if is_index {
                self.max_local = shared.max_local;
                self.min_local = shared.min_local;
            } else {
                return Err(Error::new(ErrorCode::Corrupt));
            }
        } else {
            self.child_ptr_size = 4;
            if is_table {
                self.max_local = shared.max_leaf;
                self.min_local = shared.min_leaf;
            } else if is_index {
                self.max_local = shared.max_local;
                self.min_local = shared.min_local;
            } else {
                return Err(Error::new(ErrorCode::Corrupt));
            }
        }

        Ok(())
    }

    pub fn parse_cell(&self, cell_offset: u16, limits: PageLimits) -> Result<CellInfo> {
        let start = cell_offset as usize;
        if start >= self.data.len() {
            return Err(Error::new(ErrorCode::Corrupt));
        }

        let mut cursor = start;
        if !self.is_leaf {
            cursor = cursor.checked_add(4).ok_or(Error::new(ErrorCode::Corrupt))?;
        }

        let mut info = CellInfo::default();

        if self.is_intkey && !self.is_leaf && self.is_leafdata {
            let (n_key, n_bytes) = read_varint(&self.data, cursor)?;
            cursor = cursor.checked_add(n_bytes).ok_or(Error::new(ErrorCode::Corrupt))?;
            info.n_key = n_key as i64;
            info.n_payload = 0;
            info.n_local = 0;
            info.n_size = (cursor - start) as u16;
            return Ok(info);
        }

        let (payload_size, n1) = if self.is_zerodata {
            let (payload, bytes) = read_varint32(&self.data, cursor)?;
            (payload, bytes)
        } else {
            let (payload, bytes) = read_varint32(&self.data, cursor)?;
            (payload, bytes)
        };
        cursor = cursor.checked_add(n1).ok_or(Error::new(ErrorCode::Corrupt))?;

        let (n_key, n2) = if self.is_intkey {
            read_varint(&self.data, cursor)?
        } else if self.is_zerodata {
            (payload_size as u64, 0usize)
        } else {
            let (key_bytes, bytes) = read_varint32(&self.data, cursor)?;
            (key_bytes as u64, bytes)
        };
        cursor = cursor.checked_add(n2).ok_or(Error::new(ErrorCode::Corrupt))?;

        info.n_key = n_key as i64;
        info.n_payload = payload_size;

        if payload_size as u16 <= self.max_local || self.max_local == 0 {
            let payload_end = cursor
                .checked_add(payload_size as usize)
                .ok_or(Error::new(ErrorCode::Corrupt))?;
            if payload_end > self.data.len() {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            info.n_local = payload_size as u16;
            info.n_size = (payload_end - start) as u16;
            if payload_size > 0 {
                info.payload = Some(self.data[cursor..payload_end].to_vec());
            }
        } else {
            let local = self.payload_to_local(payload_size as i64, limits)?;
            info.n_local = local;
            let payload_end = cursor
                .checked_add(local as usize)
                .ok_or(Error::new(ErrorCode::Corrupt))?;
            if payload_end > self.data.len() {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            info.n_size = (payload_end - start) as u16 + 4;
            info.payload = Some(self.data[cursor..payload_end].to_vec());
        }

        Ok(info)
    }

    fn payload_to_local(&self, n_payload: i64, limits: PageLimits) -> Result<u16> {
        if n_payload < 0 {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        let max_local = self.max_local as i64;
        if max_local == 0 {
            return Ok(n_payload.min(u16::MAX as i64) as u16);
        }
        if n_payload <= max_local {
            return Ok(n_payload as u16);
        }
        let min_local = self.min_local as i64;
        let usable = limits.usable_size as i64 - 4;
        if usable <= 0 {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        let surplus = min_local + (n_payload - min_local) % usable;
        if surplus <= max_local {
            Ok(surplus as u16)
        } else {
            Ok(min_local as u16)
        }
    }

    fn validate_freeblocks(&self, limits: PageLimits, cell_offset: usize) -> Result<()> {
        let usable_end = limits.usable_end();
        let mut next = self.first_freeblock as usize;
        let mut last = cell_offset;
        let mut steps = 0usize;

        while next != 0 {
            if next >= usable_end {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let size = read_u16(&self.data, next + 2).ok_or(Error::new(ErrorCode::Corrupt))?;
            if size < 4 {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let end = next + size as usize;
            if end > usable_end {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            if next < last {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let next_ptr = read_u16(&self.data, next).ok_or(Error::new(ErrorCode::Corrupt))?;
            last = end;
            next = next_ptr as usize;
            steps += 1;
            if steps > self.n_cell as usize + 1 {
                return Err(Error::new(ErrorCode::Corrupt));
            }
        }

        Ok(())
    }

    fn compute_free_space(&self, limits: PageLimits) -> Result<i32> {
        let header_start = limits.header_start();
        let header_size = self.header_size();
        let ptr_array_end = header_start + header_size + (self.n_cell as usize * 2);
        let cell_offset = self.cell_content_offset(limits)?;
        if cell_offset < ptr_array_end {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        let mut n_free = (cell_offset - ptr_array_end) as i32 + (self.free_bytes as i32);

        let usable_end = limits.usable_end();
        let mut next = self.first_freeblock as usize;
        let mut steps = 0usize;
        while next != 0 {
            if next >= usable_end {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let size = read_u16(&self.data, next + 2).ok_or(Error::new(ErrorCode::Corrupt))?;
            if size < 4 {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            n_free += size as i32;
            let next_ptr = read_u16(&self.data, next).ok_or(Error::new(ErrorCode::Corrupt))?;
            next = next_ptr as usize;
            steps += 1;
            if steps > self.n_cell as usize + 1 {
                return Err(Error::new(ErrorCode::Corrupt));
            }
        }

        Ok(n_free)
    }
}

fn pager_open_flags_from_btree(flags: BtreeOpenFlags) -> PagerOpenFlags {
    let mut pager_flags = PagerOpenFlags::empty();
    if flags.contains(BtreeOpenFlags::OMIT_JOURNAL) {
        pager_flags.insert(PagerOpenFlags::OMIT_JOURNAL);
    }
    if flags.contains(BtreeOpenFlags::MEMORY) {
        pager_flags.insert(PagerOpenFlags::MEMORY);
    }
    pager_flags
}

impl Btree {
    /// sqlite3BtreeOpen
    pub fn open<V: Vfs>(
        vfs: &V,
        filename: &str,
        db: Option<Arc<dyn Connection>>,
        flags: BtreeOpenFlags,
        vfs_flags: OpenFlags,
    ) -> Result<Arc<Self>> {
        let pager_flags = pager_open_flags_from_btree(flags);
        let pager = Pager::open(vfs, filename, pager_flags, vfs_flags)?;
        let page_size = pager.page_size;
        let usable_size = pager.usable_size;

        let mut shared = BtShared {
            pager,
            db: db.as_ref().map(Arc::downgrade),
            cursor_list: Vec::new(),
            page1: None,
            open_flags: flags,
            auto_vacuum: BTREE_AUTOVACUUM_NONE,
            incr_vacuum: 0,
            do_truncate: false,
            in_transaction: TransState::None,
            max_payload_1byte: 0,
            reserve_wanted: 0,
            bts_flags: BtsFlags::empty(),
            max_local: 0,
            min_local: 0,
            max_leaf: 0,
            min_leaf: 0,
            page_size,
            usable_size,
            n_transaction: 0,
            n_page: 0,
            schema: None,
            has_content: None,
            temp_space: vec![0u8; page_size as usize],
            preformat_size: 0,
            schema_cookie: 0,
            file_format: 0,
        };

        if let Ok(page) = shared.pager.get(1, PagerGetFlags::empty()) {
            if let Ok(header) = DbHeader::parse(&page.data) {
                if header.page_size != shared.page_size {
                    let _ = shared.pager.set_page_size(header.page_size, header.reserve as i32);
                    shared.page_size = shared.pager.page_size;
                    shared.usable_size = shared.pager.usable_size;
                }
                shared.schema_cookie = header.schema_cookie;
                shared.file_format = header.file_format;
                shared.auto_vacuum = header.auto_vacuum;
                shared.incr_vacuum = header.incr_vacuum;
            }
        }

        shared.update_payload_params();

        let page1_limits = PageLimits::for_page1(shared.page_size, shared.usable_size);
        if let Ok(page) = shared.pager.get(1, PagerGetFlags::empty()) {
            if let Ok(mut mem_page) =
                MemPage::parse_with_shared(1, page.data.clone(), page1_limits, Some(&shared))
            {
                let _ = mem_page.validate_layout(page1_limits);
                shared.page1 = Some(mem_page);
            }
        }

        Ok(Arc::new(Btree {
            db,
            shared: Arc::new(RwLock::new(shared)),
            in_trans: TransState::None,
            sharable: false,
            locked: false,
            has_incrblob_cur: false,
            want_to_lock: 0,
            n_backup: 0,
            data_version: 0,
            next: None,
            prev: None,
        }))
    }

    /// sqlite3BtreeClose
    pub fn close(&mut self) -> Result<()> {
        let mut shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        shared.pager.close()?;
        Ok(())
    }

    /// sqlite3BtreeSetCacheSize
    pub fn set_cache_size(&mut self, size: i32) {
        if let Ok(mut shared) = self.shared.write() {
            shared.pager.set_cache_size(size);
        }
    }

    /// sqlite3BtreeSetSpillSize
    pub fn set_spill_size(&mut self, size: i32) -> i32 {
        if let Ok(mut shared) = self.shared.write() {
            return shared.pager.set_spill_size(size);
        }
        size
    }

    /// sqlite3BtreeSetMmapLimit
    pub fn set_mmap_limit(&mut self, limit: i64) {
        if let Ok(mut shared) = self.shared.write() {
            shared.pager.set_mmap_limit(limit);
        }
    }

    /// sqlite3BtreeSetPagerFlags
    pub fn set_pager_flags(&mut self, flags: PagerFlags) {
        if let Ok(mut shared) = self.shared.write() {
            shared.pager.set_flags(flags);
        }
    }

    /// sqlite3BtreeSetPageSize
    pub fn set_page_size(&mut self, page_size: u32, reserve: i32, fix: bool) -> Result<()> {
        let mut shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        shared.pager.set_page_size(page_size, reserve)?;
        shared.page_size = shared.pager.page_size;
        shared.usable_size = shared.pager.usable_size;
        shared.update_payload_params();
        if reserve >= 0 && reserve <= u8::MAX as i32 {
            shared.reserve_wanted = reserve as u8;
        }
        if fix {
            shared.bts_flags.insert(BtsFlags::PAGESIZE_FIXED);
        }
        Ok(())
    }

    /// sqlite3BtreeGetPageSize
    pub fn page_size(&self) -> u32 {
        self.shared
            .read()
            .map(|shared| shared.page_size)
            .unwrap_or(0)
    }

    /// sqlite3BtreeMaxPageCount
    pub fn max_page_count(&mut self, max: Pgno) -> Pgno {
        if let Ok(mut shared) = self.shared.write() {
            return shared.pager.set_max_page_count(max);
        }
        max
    }

    /// sqlite3BtreeLastPage
    pub fn last_page(&self) -> Pgno {
        self.shared
            .read()
            .map(|shared| shared.pager.db_size)
            .unwrap_or(0)
    }

    /// sqlite3BtreeSecureDelete
    pub fn secure_delete(&mut self, on: bool) -> bool {
        if let Ok(mut shared) = self.shared.write() {
            let was = shared.bts_flags.contains(BtsFlags::SECURE_DELETE);
            if on {
                shared.bts_flags.insert(BtsFlags::SECURE_DELETE);
            } else {
                shared.bts_flags.remove(BtsFlags::SECURE_DELETE | BtsFlags::OVERWRITE);
            }
            return was;
        }
        false
    }

    /// sqlite3BtreeGetRequestedReserve
    pub fn requested_reserve(&self) -> u8 {
        self.shared
            .read()
            .map(|shared| shared.reserve_wanted)
            .unwrap_or(0)
    }

    /// sqlite3BtreeGetReserveNoMutex
    pub fn reserve_bytes(&self) -> u32 {
        self.shared
            .read()
            .map(|shared| shared.page_size.saturating_sub(shared.usable_size))
            .unwrap_or(0)
    }

    /// sqlite3BtreeSetAutoVacuum
    pub fn set_auto_vacuum(&mut self, mode: u8) {
        if let Ok(mut shared) = self.shared.write() {
            shared.auto_vacuum = mode;
        }
    }

    /// sqlite3BtreeGetAutoVacuum
    pub fn auto_vacuum(&self) -> u8 {
        self.shared
            .read()
            .map(|shared| shared.auto_vacuum)
            .unwrap_or(BTREE_AUTOVACUUM_NONE)
    }

    /// sqlite3BtreeBeginTrans
    pub fn begin_trans(&mut self, write: bool) -> Result<()> {
        let mut shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        if write {
            shared.pager.begin(false)?;
            shared.in_transaction = TransState::Write;
        } else {
            shared.pager.shared_lock()?;
            shared.in_transaction = TransState::Read;
        }
        self.in_trans = shared.in_transaction;
        Ok(())
    }

    /// sqlite3BtreeBeginTrans with schema flag
    pub fn begin_trans_with_schema(&mut self, write: bool, _schema_modified: &mut i32) -> Result<()> {
        self.begin_trans(write)
    }

    /// sqlite3BtreeCommitPhaseOne
    pub fn commit_phase_one(&mut self, super_journal: Option<&str>) -> Result<()> {
        let mut shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        shared.pager.commit_phase_one(super_journal)?;
        Ok(())
    }

    /// sqlite3BtreeCommitPhaseTwo
    pub fn commit_phase_two(&mut self, _cleanup: bool) -> Result<()> {
        let mut shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        shared.pager.commit_phase_two()?;
        shared.in_transaction = TransState::None;
        self.in_trans = TransState::None;
        Ok(())
    }

    /// sqlite3BtreeCommit
    pub fn commit(&mut self) -> Result<()> {
        self.commit_phase_one(None)?;
        self.commit_phase_two(false)
    }

    /// sqlite3BtreeRollback
    pub fn rollback(&mut self, _trip_code: i32, _write_only: bool) -> Result<()> {
        let mut shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        shared.pager.rollback()?;
        shared.in_transaction = TransState::None;
        self.in_trans = TransState::None;
        Ok(())
    }

    /// sqlite3BtreeCursor
    pub fn cursor(
        self: &Arc<Self>,
        root_page: Pgno,
        flags: BtreeCursorFlags,
        key_info: Option<Arc<KeyInfo>>,
    ) -> Result<BtCursor> {
        let mut hints = CursorHints::empty();
        if flags.contains(BtreeCursorFlags::BULKLOAD) {
            hints.insert(CursorHints::BULKLOAD);
        }
        if flags.contains(BtreeCursorFlags::SEEK_EQ) {
            hints.insert(CursorHints::SEEK_EQ);
        }
        let cur_flags = if flags.contains(BtreeCursorFlags::WRCSR) {
            BtCursorFlags::WRITE
        } else {
            BtCursorFlags::empty()
        };
        Ok(BtCursor {
            state: CursorState::Invalid,
            cur_flags,
            cur_pager_flags: PagerGetFlags::empty(),
            hints,
            skip_next: 0,
            btree: Arc::clone(self),
            overflow: Vec::new(),
            key: None,
            bt_shared: Arc::downgrade(&self.shared),
            next: None,
            info: CellInfo::default(),
            n_key: 0,
            root_page,
            i_page: -1,
            cur_int_key: false,
            ix: 0,
            idx_stack: Vec::new(),
            key_info,
            page: None,
            page_stack: Vec::new(),
        })
    }

    /// sqlite3BtreeCloseCursor
    pub fn close_cursor(&self, _cursor: BtCursor) -> Result<()> {
        Ok(())
    }

    /// sqlite3BtreeInsert
    pub fn insert(
        &mut self,
        _cursor: &mut BtCursor,
        _payload: &BtreePayload,
        _flags: BtreeInsertFlags,
        _seek_result: i32,
    ) -> Result<()> {
        if _flags.contains(BtreeInsertFlags::PREFORMAT) {
            return Err(Error::new(ErrorCode::Internal));
        }
        let shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        let mut shared_guard = shared;
        let root_pgno = _cursor.root_page;
        let (mut mem_page, limits) = _cursor.load_page(&mut shared_guard, root_pgno)?;
        if !mem_page.is_leaf {
            return Err(Error::new(ErrorCode::Internal));
        }

        let cell = build_cell(&mem_page, _payload)?;
        let cell_size = cell.len();
        if cell_size > mem_page.max_local as usize && mem_page.max_local != 0 {
            return Err(Error::new(ErrorCode::Internal));
        }

        let header_start = limits.header_start();
        let header_size = mem_page.header_size();
        let ptr_array_end = header_start + header_size + (mem_page.n_cell as usize * 2);
        let cell_offset = mem_page.cell_content_offset(limits)?;
        if cell_offset < ptr_array_end + cell_size {
            return Err(Error::new(ErrorCode::Full));
        }
        let new_cell_offset = cell_offset - cell_size;

        let insert_index = if _flags.contains(BtreeInsertFlags::APPEND) || _cursor.state != CursorState::Valid {
            mem_page.n_cell
        } else {
            _cursor.ix.min(mem_page.n_cell)
        };
        let mut page = shared_guard.pager.get(root_pgno, PagerGetFlags::empty())?;
        shared_guard.pager.write(&mut page)?;

        let data = &mut page.data;
        data[new_cell_offset..new_cell_offset + cell_size].copy_from_slice(&cell);

        let ptr_array_start = header_start + header_size;
        let ptr_array_end = ptr_array_start + (mem_page.n_cell as usize * 2);
        let insert_ptr_offset = ptr_array_start + (insert_index as usize * 2);
        if insert_ptr_offset < ptr_array_end {
            data.copy_within(insert_ptr_offset..ptr_array_end, insert_ptr_offset + 2);
        }
        let ptr_write = header_start + header_size + (insert_index as usize * 2);
        write_u16(data, ptr_write, new_cell_offset as u16)?;
        write_u16(data, header_start + 3, mem_page.n_cell + 1)?;
        write_u16(data, header_start + 5, new_cell_offset as u16)?;

        mem_page.data = data.clone();
        mem_page.n_cell += 1;
        mem_page.cell_offset = new_cell_offset as u16;
        _cursor.page = Some(mem_page);
        Ok(())
    }

    /// sqlite3BtreeDelete
    pub fn delete(&mut self, _cursor: &mut BtCursor, _flags: BtreeInsertFlags) -> Result<()> {
        let shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        let mut shared_guard = shared;
        let root_pgno = _cursor.root_page;
        let (mem_page, limits) = _cursor.load_page(&mut shared_guard, root_pgno)?;
        if !mem_page.is_leaf {
            return Err(Error::new(ErrorCode::Internal));
        }
        if _cursor.ix >= mem_page.n_cell {
            return Err(Error::new(ErrorCode::Range));
        }

        let header_start = limits.header_start();
        let header_size = mem_page.header_size();
        let ptr_array_start = header_start + header_size;
        let mut page = shared_guard.pager.get(root_pgno, PagerGetFlags::empty())?;
        shared_guard.pager.write(&mut page)?;
        let data = &mut page.data;

        let cell_offset = mem_page.cell_ptr(_cursor.ix, limits)?;
        let info = mem_page.parse_cell(cell_offset, limits)?;
        let cell_size = info.n_size as i32;

        let from = ptr_array_start + ((_cursor.ix as usize + 1) * 2);
        let to = ptr_array_start + (_cursor.ix as usize * 2);
        let ptr_end = ptr_array_start + (mem_page.n_cell as usize * 2);
        data.copy_within(from..ptr_end, to);

        write_u16(data, header_start + 3, mem_page.n_cell - 1)?;

        _cursor.state = CursorState::Invalid;
        Ok(())
    }

    /// sqlite3BtreeCreateTable
    pub fn create_table(&mut self, _flags: u8) -> Result<Pgno> {
        Err(Error::new(ErrorCode::Internal))
    }

    /// sqlite3BtreeDropTable
    pub fn drop_table(&mut self, _root_page: Pgno) -> Result<()> {
        Err(Error::new(ErrorCode::Internal))
    }

    /// sqlite3BtreeClearTable
    pub fn clear_table(&mut self, _root_page: Pgno) -> Result<i64> {
        Err(Error::new(ErrorCode::Internal))
    }

    /// sqlite3BtreeClearTableOfCursor
    pub fn clear_table_of_cursor(&mut self, _cursor: &mut BtCursor) -> Result<i64> {
        Err(Error::new(ErrorCode::Internal))
    }

    /// sqlite3BtreeTripAllCursors
    pub fn trip_all_cursors(&mut self, _table: i32, _write_only: bool) -> Result<()> {
        Err(Error::new(ErrorCode::Internal))
    }

    /// sqlite3BtreeBeginStmt
    pub fn begin_stmt(&mut self, stmt_id: i32) -> Result<()> {
        self.savepoint(SavepointOp::Begin, stmt_id)
    }

    /// sqlite3BtreeTxnState
    pub fn txn_state(&self) -> TransState {
        self.in_trans
    }

    /// sqlite3BtreeIsInBackup
    pub fn is_in_backup(&self) -> bool {
        self.n_backup > 0
    }

    /// sqlite3BtreeSchema
    pub fn schema(&self) -> Option<Arc<dyn std::any::Any>> {
        self.shared
            .read()
            .ok()
            .and_then(|shared| shared.schema.clone())
    }

    /// sqlite3BtreeSchemaLocked
    pub fn schema_locked(&self) -> bool {
        false
    }

    /// sqlite3BtreeLockTable
    pub fn lock_table(&mut self, _table: i32, _write: bool) -> Result<()> {
        Ok(())
    }

    /// sqlite3BtreeSavepoint
    pub fn savepoint(&mut self, op: SavepointOp, index: i32) -> Result<()> {
        let mut shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        shared.pager.savepoint(op, index)
    }

    /// sqlite3BtreeCheckpoint
    pub fn checkpoint(&mut self, _mode: i32) -> Result<(i32, i32)> {
        Err(Error::new(ErrorCode::Internal))
    }

    /// sqlite3BtreeGetFilename
    pub fn filename(&self) -> Option<String> {
        self.shared
            .read()
            .ok()
            .map(|shared| shared.pager.db_path.clone())
    }

    /// sqlite3BtreeGetJournalname
    pub fn journalname(&self) -> Option<String> {
        self.shared
            .read()
            .ok()
            .map(|shared| shared.pager.journal_path.clone())
    }

    /// sqlite3BtreeCopyFile
    pub fn copy_file(&mut self, _other: &mut Btree) -> Result<()> {
        Err(Error::new(ErrorCode::Internal))
    }

    /// sqlite3BtreeIncrVacuum
    pub fn incr_vacuum(&mut self) -> Result<()> {
        Err(Error::new(ErrorCode::Internal))
    }

    /// sqlite3BtreeGetMeta
    pub fn get_meta(&self, _idx: usize) -> Result<u32> {
        if _idx >= SQLITE_N_BTREE_META {
            return Err(Error::new(ErrorCode::Range));
        }
        if _idx == BTREE_DATA_VERSION {
            return Ok(0);
        }
        let shared = self.shared.read().map_err(|_| Error::new(ErrorCode::Internal))?;
        let page1 = shared.page1.as_ref().ok_or(Error::new(ErrorCode::Corrupt))?;
        let offset = 36usize + (_idx * 4);
        read_u32(&page1.data, offset).ok_or(Error::new(ErrorCode::Corrupt))
    }

    /// sqlite3BtreeUpdateMeta
    pub fn update_meta(&mut self, _idx: usize, _value: u32) -> Result<()> {
        let mut shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        if _idx >= SQLITE_N_BTREE_META {
            return Err(Error::new(ErrorCode::Range));
        }
        let offset = 36usize + (_idx * 4);
        if let Ok(mut page) = shared.pager.get(1, PagerGetFlags::empty()) {
            shared.pager.write(&mut page)?;
            write_u32(&mut page.data, offset, _value)?;
            if let Some(ref mut page1) = shared.page1 {
                let _ = write_u32(&mut page1.data, offset, _value);
            }
            Ok(())
        } else {
            Err(Error::new(ErrorCode::Corrupt))
        }
    }

    /// sqlite3BtreeNewDb
    pub fn new_db(&mut self) -> Result<()> {
        let mut shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        let mut page = shared.pager.get(1, PagerGetFlags::empty())?;
        shared.pager.write(&mut page)?;
        page.data.fill(0);
        page.data[..SQLITE_FILE_HEADER.len()].copy_from_slice(SQLITE_FILE_HEADER);
        let page_size = if shared.page_size == 65536 {
            1
        } else {
            shared.page_size as u16
        };
        write_u16(&mut page.data, 16, page_size)?;
        page.data[18] = 1;
        page.data[19] = 1;
        page.data[20] = shared.reserve_wanted;
        page.data[21] = 64;
        page.data[22] = 32;
        page.data[23] = 32;
        write_u32(&mut page.data, 24, 1)?;
        write_u32(&mut page.data, 28, 1)?;
        write_u32(&mut page.data, 32, 0)?;
        write_u32(&mut page.data, 36, 0)?;
        shared.schema_cookie = 0;
        shared.file_format = 1;
        let limits = PageLimits::for_page1(shared.page_size, shared.usable_size);
        if let Ok(mem_page) = MemPage::parse_with_shared(1, page.data.clone(), limits, Some(&shared)) {
            shared.page1 = Some(mem_page);
        }
        Ok(())
    }

    /// sqlite3BtreeSetVersion
    pub fn set_version(&mut self, _version: i32) -> Result<()> {
        let mut shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        let mut page = shared.pager.get(1, PagerGetFlags::empty())?;
        shared.pager.write(&mut page)?;
        let version = if _version < 0 { 0 } else { _version as u8 };
        page.data[18] = version;
        page.data[19] = version;
        shared.file_format = version;
        if let Some(ref mut page1) = shared.page1 {
            if page1.data.len() >= 20 {
                page1.data[18] = version;
                page1.data[19] = version;
            }
        }
        Ok(())
    }

    /// sqlite3BtreeIsReadonly
    pub fn is_readonly(&self) -> bool {
        self.shared
            .read()
            .map(|shared| shared.bts_flags.contains(BtsFlags::READ_ONLY))
            .unwrap_or(false)
    }

    /// sqlite3BtreeClosesWithCursor (debug)
    pub fn closes_with_cursor(&self, _cursor: &BtCursor) -> bool {
        false
    }

    /// sqlite3BtreeCount
    pub fn count(&mut self, _cursor: &mut BtCursor) -> Result<i64> {
        _cursor.count()
    }

    /// sqlite3BtreeCursorInfo
    pub fn cursor_info(&mut self, _cursor: &mut BtCursor, _op: i32) -> Result<i32> {
        _cursor.cursor_info(_op)
    }

    /// sqlite3BtreeTransferRow
    pub fn transfer_row(&mut self, _source: &mut BtCursor, _dest: &mut BtCursor, _i_rowid: i64) -> Result<()> {
        Err(Error::new(ErrorCode::Internal))
    }
}

impl BtCursor {
    fn load_root_page(&self) -> Result<(MemPage, PageLimits)> {
        let shared = self
            .bt_shared
            .upgrade()
            .ok_or(Error::new(ErrorCode::Internal))?;
        let mut shared_guard = shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        let pgno = self.root_page;
        let limits = if pgno == 1 {
            PageLimits::for_page1(shared_guard.page_size, shared_guard.usable_size)
        } else {
            PageLimits::new(shared_guard.page_size, shared_guard.usable_size)
        };
        let page = shared_guard.pager.get(pgno, PagerGetFlags::empty())?;
        let mut mem_page =
            MemPage::parse_with_shared(pgno, page.data.clone(), limits, Some(&shared_guard))?;
        mem_page.validate_layout(limits)?;
        Ok((mem_page, limits))
    }

    fn load_leaf_root(&mut self) -> Result<(MemPage, PageLimits)> {
        let (mem_page, limits) = self.load_root_page()?;
        if !mem_page.is_leaf {
            return Err(Error::new(ErrorCode::Internal));
        }
        Ok((mem_page, limits))
    }

    fn load_page(
        &self,
        shared: &mut BtShared,
        pgno: Pgno,
    ) -> Result<(MemPage, PageLimits)> {
        let limits = if pgno == 1 {
            PageLimits::for_page1(shared.page_size, shared.usable_size)
        } else {
            PageLimits::new(shared.page_size, shared.usable_size)
        };
        let page = shared.pager.get(pgno, PagerGetFlags::empty())?;
        let mut mem_page =
            MemPage::parse_with_shared(pgno, page.data.clone(), limits, Some(shared))?;
        mem_page.validate_layout(limits)?;
        Ok((mem_page, limits))
    }

    /// sqlite3BtreeCursorSize
    pub fn size() -> usize {
        std::mem::size_of::<BtCursor>()
    }

    /// sqlite3BtreeCursorZero
    pub fn reset(&mut self) {
        self.state = CursorState::Invalid;
        self.cur_flags = BtCursorFlags::empty();
        self.cur_pager_flags = PagerGetFlags::empty();
        self.hints = CursorHints::empty();
        self.skip_next = 0;
        self.overflow.clear();
        self.key = None;
        self.next = None;
        self.info = CellInfo::default();
        self.n_key = 0;
        self.root_page = 0;
        self.i_page = -1;
        self.cur_int_key = false;
        self.ix = 0;
        self.idx_stack.clear();
        self.key_info = None;
        self.page = None;
        self.page_stack.clear();
    }

    /// sqlite3BtreeFirst
    pub fn first(&mut self) -> Result<bool> {
        let (mem_page, limits) = self.load_leaf_root()?;
        if mem_page.n_cell == 0 {
            self.state = CursorState::Invalid;
            return Ok(true);
        }
        let cell_offset = mem_page.cell_ptr(0, limits)?;
        let info = mem_page.parse_cell(cell_offset, limits)?;
        self.info = info;
        self.n_key = self.info.n_key;
        self.state = CursorState::Valid;
        self.ix = 0;
        self.page = Some(mem_page);
        Ok(false)
    }

    /// sqlite3BtreeLast
    pub fn last(&mut self) -> Result<bool> {
        let (mem_page, limits) = self.load_leaf_root()?;
        if mem_page.n_cell == 0 {
            self.state = CursorState::Invalid;
            return Ok(true);
        }
        let last_index = mem_page.n_cell - 1;
        let cell_offset = mem_page.cell_ptr(last_index, limits)?;
        let info = mem_page.parse_cell(cell_offset, limits)?;
        self.info = info;
        self.n_key = self.info.n_key;
        self.state = CursorState::Valid;
        self.ix = last_index;
        self.page = Some(mem_page);
        Ok(false)
    }

    /// sqlite3BtreeNext
    pub fn next(&mut self, _flags: i32) -> Result<()> {
        let page = self.page.as_ref().ok_or(Error::new(ErrorCode::Corrupt))?;
        if !page.is_leaf {
            return Err(Error::new(ErrorCode::Internal));
        }
        let next_ix = self.ix.saturating_add(1);
        if next_ix >= page.n_cell {
            self.state = CursorState::Invalid;
            return Ok(());
        }
        let shared = self
            .bt_shared
            .upgrade()
            .ok_or(Error::new(ErrorCode::Internal))?;
        let shared_guard = shared.read().map_err(|_| Error::new(ErrorCode::Internal))?;
        let limits = if page.pgno == 1 {
            PageLimits::for_page1(shared_guard.page_size, shared_guard.usable_size)
        } else {
            PageLimits::new(shared_guard.page_size, shared_guard.usable_size)
        };
        let cell_offset = page.cell_ptr(next_ix, limits)?;
        let info = page.parse_cell(cell_offset, limits)?;
        self.info = info;
        self.n_key = self.info.n_key;
        self.ix = next_ix;
        self.state = CursorState::Valid;
        Ok(())
    }

    /// sqlite3BtreePrevious
    pub fn previous(&mut self, _flags: i32) -> Result<()> {
        let page = self.page.as_ref().ok_or(Error::new(ErrorCode::Corrupt))?;
        if !page.is_leaf {
            return Err(Error::new(ErrorCode::Internal));
        }
        if self.ix == 0 {
            self.state = CursorState::Invalid;
            return Ok(());
        }
        let prev_ix = self.ix - 1;
        let shared = self
            .bt_shared
            .upgrade()
            .ok_or(Error::new(ErrorCode::Internal))?;
        let shared_guard = shared.read().map_err(|_| Error::new(ErrorCode::Internal))?;
        let limits = if page.pgno == 1 {
            PageLimits::for_page1(shared_guard.page_size, shared_guard.usable_size)
        } else {
            PageLimits::new(shared_guard.page_size, shared_guard.usable_size)
        };
        let cell_offset = page.cell_ptr(prev_ix, limits)?;
        let info = page.parse_cell(cell_offset, limits)?;
        self.info = info;
        self.n_key = self.info.n_key;
        self.ix = prev_ix;
        self.state = CursorState::Valid;
        Ok(())
    }

    /// sqlite3BtreeIsEmpty
    pub fn is_empty(&mut self) -> Result<bool> {
        let (mem_page, _) = self.load_leaf_root()?;
        Ok(mem_page.n_cell == 0)
    }

    /// sqlite3BtreeCount
    pub fn count(&mut self) -> Result<i64> {
        let (mem_page, _) = self.load_leaf_root()?;
        Ok(mem_page.n_cell as i64)
    }

    /// sqlite3BtreeEof
    pub fn eof(&self) -> bool {
        self.state != CursorState::Valid
    }

    /// sqlite3BtreeIntegerKey
    pub fn integer_key(&self) -> RowId {
        self.n_key
    }

    /// sqlite3BtreePayloadSize
    pub fn payload_size(&self) -> u32 {
        self.info.n_payload
    }

    /// sqlite3BtreePayloadFetch
    pub fn payload_fetch(&self) -> Option<&[u8]> {
        self.info.payload.as_deref()
    }

    /// sqlite3BtreePayload
    pub fn payload(&self, offset: u32, amount: u32) -> Result<Vec<u8>> {
        let payload = self.info.payload.as_ref().ok_or(Error::new(ErrorCode::Corrupt))?;
        let start = offset as usize;
        let end = start.checked_add(amount as usize).ok_or(Error::new(ErrorCode::Corrupt))?;
        if end > payload.len() {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        Ok(payload[start..end].to_vec())
    }

    /// sqlite3BtreePayloadChecked
    pub fn payload_checked(&self, offset: u32, amount: u32) -> Result<Vec<u8>> {
        self.payload(offset, amount)
    }

    /// sqlite3BtreePutData
    pub fn put_data(&mut self, _offset: u32, _amount: u32, _data: &[u8]) -> Result<()> {
        Err(Error::new(ErrorCode::Internal))
    }

    /// sqlite3BtreeCursorPin
    pub fn pin(&mut self) {
        self.cur_flags.insert(BtCursorFlags::PINNED);
    }

    /// sqlite3BtreeCursorUnpin
    pub fn unpin(&mut self) {
        self.cur_flags.remove(BtCursorFlags::PINNED);
    }

    /// sqlite3BtreeOffset
    pub fn offset(&self) -> RowId {
        0
    }

    /// sqlite3BtreeMaxRecordSize
    pub fn max_record_size(&self) -> RowId {
        0
    }

    /// sqlite3BtreeCursorHintFlags
    pub fn set_hint_flags(&mut self, flags: BtreeCursorFlags) {
        self.hints = CursorHints::empty();
        if flags.contains(BtreeCursorFlags::BULKLOAD) {
            self.hints.insert(CursorHints::BULKLOAD);
        }
        if flags.contains(BtreeCursorFlags::SEEK_EQ) {
            self.hints.insert(CursorHints::SEEK_EQ);
        }
    }

    /// sqlite3BtreeTableMoveto
    pub fn table_moveto(&mut self, _int_key: RowId, _bias: bool) -> Result<i32> {
        let shared = self
            .bt_shared
            .upgrade()
            .ok_or(Error::new(ErrorCode::Internal))?;
        let mut shared_guard = shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        let mut pgno = self.root_page;
        self.page_stack.clear();
        self.idx_stack.clear();

        loop {
            let (mem_page, limits) = self.load_page(&mut shared_guard, pgno)?;
            if mem_page.is_leaf {
                if mem_page.n_cell == 0 {
                    self.state = CursorState::Invalid;
                    return Ok(1);
                }
                for i in 0..mem_page.n_cell {
                    let cell_offset = mem_page.cell_ptr(i, limits)?;
                    let info = mem_page.parse_cell(cell_offset, limits)?;
                    if info.n_key == _int_key {
                        self.info = info;
                        self.n_key = _int_key;
                        self.ix = i;
                        self.state = CursorState::Valid;
                        self.page = Some(mem_page);
                        return Ok(0);
                    }
                    if _int_key < info.n_key {
                        self.info = info;
                        self.n_key = self.info.n_key;
                        self.ix = i;
                        self.state = CursorState::Valid;
                        self.page = Some(mem_page);
                        return Ok(-1);
                    }
                }
                let last_index = mem_page.n_cell - 1;
                let cell_offset = mem_page.cell_ptr(last_index, limits)?;
                let info = mem_page.parse_cell(cell_offset, limits)?;
                self.info = info;
                self.n_key = self.info.n_key;
                self.ix = last_index;
                self.state = CursorState::Valid;
                self.page = Some(mem_page);
                return Ok(1);
            }

            let mut child = mem_page
                .rightmost_ptr
                .ok_or(Error::new(ErrorCode::Corrupt))?;
            let mut child_index = mem_page.n_cell;
            for i in 0..mem_page.n_cell {
                let cell_offset = mem_page.cell_ptr(i, limits)?;
                let info = mem_page.parse_cell(cell_offset, limits)?;
                if _int_key < info.n_key {
                    child = mem_page.child_pgno(cell_offset)?;
                    child_index = i;
                    break;
                }
            }
            self.page_stack.push(mem_page);
            self.idx_stack.push(child_index);
            pgno = child;
        }
    }

    /// sqlite3BtreeIndexMoveto
    pub fn index_moveto(&mut self, _key: &UnpackedRecord) -> Result<i32> {
        let shared = self
            .bt_shared
            .upgrade()
            .ok_or(Error::new(ErrorCode::Internal))?;
        let mut shared_guard = shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        let mut pgno = self.root_page;
        self.page_stack.clear();
        self.idx_stack.clear();

        loop {
            let (mem_page, limits) = self.load_page(&mut shared_guard, pgno)?;
            if mem_page.is_leaf {
                if mem_page.n_cell == 0 {
                    self.state = CursorState::Invalid;
                    return Ok(1);
                }
                for i in 0..mem_page.n_cell {
                    let cell_offset = mem_page.cell_ptr(i, limits)?;
                    let info = mem_page.parse_cell(cell_offset, limits)?;
                    let payload = info.payload.as_deref().unwrap_or(&[]);
                    match payload.cmp(_key.key.as_slice()) {
                        std::cmp::Ordering::Equal => {
                            self.info = info;
                            self.n_key = self.info.n_key;
                            self.ix = i;
                            self.state = CursorState::Valid;
                            self.page = Some(mem_page);
                            return Ok(0);
                        }
                        std::cmp::Ordering::Greater => {
                            self.info = info;
                            self.n_key = self.info.n_key;
                            self.ix = i;
                            self.state = CursorState::Valid;
                            self.page = Some(mem_page);
                            return Ok(-1);
                        }
                        std::cmp::Ordering::Less => {}
                    }
                }
                let last_index = mem_page.n_cell - 1;
                let cell_offset = mem_page.cell_ptr(last_index, limits)?;
                let info = mem_page.parse_cell(cell_offset, limits)?;
                self.info = info;
                self.n_key = self.info.n_key;
                self.ix = last_index;
                self.state = CursorState::Valid;
                self.page = Some(mem_page);
                return Ok(1);
            }

            let mut child = mem_page
                .rightmost_ptr
                .ok_or(Error::new(ErrorCode::Corrupt))?;
            let mut child_index = mem_page.n_cell;
            for i in 0..mem_page.n_cell {
                let cell_offset = mem_page.cell_ptr(i, limits)?;
                let info = mem_page.parse_cell(cell_offset, limits)?;
                let payload = info.payload.as_deref().unwrap_or(&[]);
                if payload > _key.key.as_slice() {
                    child = mem_page.child_pgno(cell_offset)?;
                    child_index = i;
                    break;
                }
            }
            self.page_stack.push(mem_page);
            self.idx_stack.push(child_index);
            pgno = child;
        }
    }

    /// sqlite3BtreeCursorHasMoved
    pub fn has_moved(&self) -> bool {
        false
    }

    /// sqlite3BtreeCursorHasHint
    pub fn has_hint(&self, mask: u32) -> bool {
        (self.hints.bits() as u32 & mask) != 0
    }

    /// sqlite3BtreeCursorIsValid
    pub fn is_valid(&self) -> bool {
        self.state == CursorState::Valid
    }

    /// sqlite3BtreeCursorIsValidNN
    pub fn is_valid_nn(&self) -> bool {
        self.is_valid()
    }

    /// sqlite3BtreeCursorRestore
    pub fn restore(&mut self) -> Result<bool> {
        Err(Error::new(ErrorCode::Internal))
    }

    /// sqlite3BtreeCursorInfo
    pub fn cursor_info(&self, _opcode: i32) -> Result<i32> {
        Err(Error::new(ErrorCode::Internal))
    }

    /// sqlite3BtreeCursorHint
    pub fn hint(&mut self, _hint: i32) -> Result<()> {
        Ok(())
    }
}
