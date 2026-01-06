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
    pub free_pages: Vec<Pgno>,
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

#[derive(Clone)]
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
    pub parent: Option<Pgno>,
    pub usable_space: u16,
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
    pub overflow_pgno: Option<Pgno>,
}

impl Default for CellInfo {
    fn default() -> Self {
        Self {
            n_key: 0,
            payload: None,
            n_payload: 0,
            n_local: 0,
            n_size: 0,
            overflow_pgno: None,
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

struct OverflowChain {
    first: Option<Pgno>,
    pages: Vec<Vec<u8>>,
}

fn build_cell(
    page: &MemPage,
    limits: PageLimits,
    payload: &BtreePayload,
) -> Result<(Vec<u8>, OverflowChain, bool)> {
    if !page.is_leaf {
        return Err(Error::new(ErrorCode::Internal));
    }

    let mut cell = Vec::new();
    let mut overflow = OverflowChain { first: None, pages: Vec::new() };
    let mut needs_overflow_ptr = false;
    if page.is_intkey && page.is_leafdata {
        let data = payload.data.as_deref().unwrap_or(&[]);
        let payload_size = data.len() + payload.n_zero.max(0) as usize;
        write_varint(payload_size as u64, &mut cell);
        write_varint(payload.n_key as u64, &mut cell);
        let local = page.payload_to_local(payload_size as i64, limits)? as usize;
        cell.extend_from_slice(&data[..std::cmp::min(data.len(), local)]);
        if local > data.len() {
            cell.extend(std::iter::repeat(0u8).take(local - data.len()));
        } else if payload.n_zero > 0 && local >= data.len() {
            let remaining = local - data.len();
            if remaining > 0 {
                cell.extend(std::iter::repeat(0u8).take(remaining));
            }
        }
        if local < payload_size {
            let mut full = Vec::with_capacity(payload_size);
            full.extend_from_slice(data);
            if payload.n_zero > 0 {
                full.extend(std::iter::repeat(0u8).take(payload.n_zero as usize));
            }
            let overflow_bytes = &full[local..];
            overflow = build_overflow_pages(limits, overflow_bytes);
            needs_overflow_ptr = true;
        }
        return Ok((cell, overflow, needs_overflow_ptr));
    }

    if page.is_zerodata {
        let key = payload.key.as_deref().ok_or(Error::new(ErrorCode::Misuse))?;
        let payload_size = key.len();
        write_varint(payload_size as u64, &mut cell);
        let local = page.payload_to_local(payload_size as i64, limits)? as usize;
        cell.extend_from_slice(&key[..std::cmp::min(key.len(), local)]);
        if local < payload_size {
            let overflow_bytes = &key[local..];
            overflow = build_overflow_pages(limits, overflow_bytes);
            needs_overflow_ptr = true;
        }
        return Ok((cell, overflow, needs_overflow_ptr));
    }

    Err(Error::new(ErrorCode::Internal))
}

fn build_overflow_pages(limits: PageLimits, payload: &[u8]) -> OverflowChain {
    let mut pages = Vec::new();
    let mut offset = 0usize;
    let ovfl_size = limits.usable_size.saturating_sub(4) as usize;
    while offset < payload.len() {
        let take = std::cmp::min(ovfl_size, payload.len() - offset);
        let mut page = vec![0u8; limits.page_size as usize];
        page[4..4 + take].copy_from_slice(&payload[offset..offset + take]);
        pages.push(page);
        offset += take;
    }
    OverflowChain { first: None, pages }
}

fn free_overflow_chain(shared: &mut BtShared, start: Pgno) -> Result<()> {
    let mut next = start;
    let mut freed = 0;
    while next != 0 {
        let page = shared.pager.get(next, PagerGetFlags::empty())?;
        let next_pgno = read_u32(&page.data, 0).ok_or(Error::new(ErrorCode::Corrupt))?;
        shared.free_pages.push(next);
        freed += 1;
        next = next_pgno;
    }
    if freed > 0 {
        update_free_page_count(shared, freed)?;
    }
    Ok(())
}

fn allocate_page(shared: &mut BtShared) -> Pgno {
    if let Some(pgno) = shared.free_pages.pop() {
        let _ = update_free_page_count(shared, -1);
        pgno
    } else {
        shared.pager.db_size + 1
    }
}

fn update_free_page_count(shared: &mut BtShared, delta: i32) -> Result<()> {
    let mut page = shared.pager.get(1, PagerGetFlags::empty())?;
    shared.pager.write(&mut page)?;
    let offset = 36usize + (BTREE_FREE_PAGE_COUNT * 4);
    let current = read_u32(&page.data, offset).unwrap_or(0);
    let updated = if delta.is_negative() {
        current.saturating_sub(delta.unsigned_abs())
    } else {
        current.saturating_add(delta as u32)
    };
    write_u32(&mut page.data, offset, updated)?;
    Ok(())
}

fn collapse_root_if_empty(shared: &mut BtShared, root_pgno: Pgno) -> Result<()> {
    let limits = if root_pgno == 1 {
        PageLimits::for_page1(shared.page_size, shared.usable_size)
    } else {
        PageLimits::new(shared.page_size, shared.usable_size)
    };
    let root_page = shared.pager.get(root_pgno, PagerGetFlags::empty())?;
    let mem_page = MemPage::parse_with_shared(root_pgno, root_page.data.clone(), limits, Some(shared))?;
    if mem_page.is_leaf {
        return Ok(());
    }
    if mem_page.n_cell > 0 {
        return Ok(());
    }
    let child_pgno = mem_page.rightmost_ptr.ok_or(Error::new(ErrorCode::Corrupt))?;
    let child_limits = if child_pgno == 1 {
        PageLimits::for_page1(shared.page_size, shared.usable_size)
    } else {
        PageLimits::new(shared.page_size, shared.usable_size)
    };
    let child_page = shared.pager.get(child_pgno, PagerGetFlags::empty())?;
    let mut root_write = shared.pager.get(root_pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut root_write)?;
    root_write.data = child_page.data.clone();
    let _ = MemPage::parse_with_shared(root_pgno, root_write.data.clone(), limits, Some(shared))?;
    Ok(())
}

fn build_leaf_page_data(
    limits: PageLimits,
    flags: u8,
    cells: &[Vec<u8>],
) -> Result<Vec<u8>> {
    let mut data = vec![0u8; limits.page_size as usize];
    let header_start = limits.header_start();
    data[header_start] = flags;
    write_u16(&mut data, header_start + 1, 0)?;
    write_u16(&mut data, header_start + 3, cells.len() as u16)?;
    data[header_start + 7] = 0;

    let mut content_start = limits.usable_size as usize;
    for (i, cell) in cells.iter().enumerate() {
        content_start = content_start
            .checked_sub(cell.len())
            .ok_or(Error::new(ErrorCode::Corrupt))?;
        data[content_start..content_start + cell.len()].copy_from_slice(cell);
        let ptr_offset = header_start + 8 + (i * 2);
        write_u16(&mut data, ptr_offset, content_start as u16)?;
    }
    let ptr_end = header_start + 8 + (cells.len() * 2);
    if content_start < ptr_end {
        return Err(Error::new(ErrorCode::Full));
    }
    write_u16(&mut data, header_start + 5, content_start as u16)?;
    Ok(data)
}

fn build_internal_cell(child_pgno: Pgno, key: &CellInfo, is_index: bool) -> Result<Vec<u8>> {
    let mut cell = Vec::new();
    cell.extend_from_slice(&child_pgno.to_be_bytes());
    if is_index {
        let payload = key.payload.as_ref().ok_or(Error::new(ErrorCode::Internal))?;
        write_varint(payload.len() as u64, &mut cell);
        cell.extend_from_slice(payload);
    } else {
        write_varint(key.n_key as u64, &mut cell);
    }
    Ok(cell)
}

fn build_internal_root_data(
    limits: PageLimits,
    flags: u8,
    left_child: Pgno,
    right_child: Pgno,
    sep: &CellInfo,
    is_index: bool,
) -> Result<Vec<u8>> {
    let cell = build_internal_cell(left_child, sep, is_index)?;
    let mut data = vec![0u8; limits.page_size as usize];
    let header_start = limits.header_start();
    data[header_start] = flags;
    write_u16(&mut data, header_start + 1, 0)?;
    write_u16(&mut data, header_start + 3, 1)?;
    data[header_start + 7] = 0;
    write_u32(&mut data, header_start + 8, right_child)?;
    let mut content_start = limits.usable_size as usize;
    content_start = content_start
        .checked_sub(cell.len())
        .ok_or(Error::new(ErrorCode::Corrupt))?;
    data[content_start..content_start + cell.len()].copy_from_slice(&cell);
    let ptr_offset = header_start + 12;
    write_u16(&mut data, ptr_offset, content_start as u16)?;
    write_u16(&mut data, header_start + 5, content_start as u16)?;
    Ok(data)
}

#[derive(Clone)]
enum InternalKey {
    Int(i64),
    Blob(Vec<u8>),
}

fn build_internal_page_data(
    limits: PageLimits,
    flags: u8,
    keys: &[InternalKey],
    children: &[Pgno],
) -> Result<Vec<u8>> {
    if children.len() != keys.len() + 1 {
        return Err(Error::new(ErrorCode::Misuse));
    }
    let mut data = vec![0u8; limits.page_size as usize];
    let header_start = limits.header_start();
    data[header_start] = flags;
    write_u16(&mut data, header_start + 1, 0)?;
    write_u16(&mut data, header_start + 3, keys.len() as u16)?;
    data[header_start + 7] = 0;
    write_u32(&mut data, header_start + 8, children[keys.len()])?;

    let mut cells = Vec::with_capacity(keys.len());
    for (i, key) in keys.iter().enumerate() {
        let mut cell = Vec::new();
        cell.extend_from_slice(&children[i].to_be_bytes());
        match key {
            InternalKey::Int(k) => write_varint(*k as u64, &mut cell),
            InternalKey::Blob(blob) => {
                write_varint(blob.len() as u64, &mut cell);
                cell.extend_from_slice(blob);
            }
        }
        cells.push(cell);
    }

    let mut content_start = limits.usable_size as usize;
    for (i, cell) in cells.iter().enumerate() {
        content_start = content_start
            .checked_sub(cell.len())
            .ok_or(Error::new(ErrorCode::Corrupt))?;
        data[content_start..content_start + cell.len()].copy_from_slice(cell);
        let ptr_offset = header_start + 12 + (i * 2);
        write_u16(&mut data, ptr_offset, content_start as u16)?;
    }
    let ptr_end = header_start + 12 + (keys.len() * 2);
    if content_start < ptr_end {
        return Err(Error::new(ErrorCode::Full));
    }
    write_u16(&mut data, header_start + 5, content_start as u16)?;
    Ok(data)
}

fn rebuild_internal_children(
    parent: &MemPage,
    parent_limits: PageLimits,
) -> Result<(Vec<InternalKey>, Vec<Pgno>)> {
    let mut keys = Vec::with_capacity(parent.n_cell as usize);
    let mut children = Vec::with_capacity(parent.n_cell as usize + 1);
    for i in 0..parent.n_cell {
        let cell_offset = parent.cell_ptr(i, parent_limits)?;
        let info = parent.parse_cell(cell_offset, parent_limits)?;
        let child = parent.child_pgno(cell_offset)?;
        children.push(child);
        if parent.is_intkey {
            keys.push(InternalKey::Int(info.n_key));
        } else {
            let payload = info.payload.clone().ok_or(Error::new(ErrorCode::Internal))?;
            keys.push(InternalKey::Blob(payload));
        }
    }
    let rightmost = parent.rightmost_ptr.ok_or(Error::new(ErrorCode::Corrupt))?;
    children.push(rightmost);
    Ok((keys, children))
}

fn split_internal_root(
    shared: &mut BtShared,
    root_pgno: Pgno,
    parent: &MemPage,
    parent_limits: PageLimits,
) -> Result<()> {
    let (keys, children) = rebuild_internal_children(parent, parent_limits)?;
    let mid = keys.len() / 2;
    let left_keys = keys[..mid].to_vec();
    let right_keys = keys[mid + 1..].to_vec();
    let sep_key = keys[mid].clone();
    let left_children = children[..mid + 1].to_vec();
    let right_children = children[mid + 1..].to_vec();

    let left_pgno = allocate_page(shared);
    let right_pgno = allocate_page(shared);
    let flags = parent.data[parent_limits.header_start()];
    let left_data = build_internal_page_data(
        PageLimits::new(shared.page_size, shared.usable_size),
        flags,
        &left_keys,
        &left_children,
    )?;
    let right_data = build_internal_page_data(
        PageLimits::new(shared.page_size, shared.usable_size),
        flags,
        &right_keys,
        &right_children,
    )?;

    let mut left_page = shared.pager.get(left_pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut left_page)?;
    left_page.data = left_data;

    let mut right_page = shared.pager.get(right_pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut right_page)?;
    right_page.data = right_data;

    shared.pager.db_size = right_pgno.max(shared.pager.db_size);
    shared.n_page = shared.pager.db_size;

    let sep_info = match sep_key {
        InternalKey::Int(key) => CellInfo {
            n_key: key,
            ..CellInfo::default()
        },
        InternalKey::Blob(payload) => CellInfo {
            payload: Some(payload),
            n_payload: 0,
            ..CellInfo::default()
        },
    };

    let root_flags = if parent.is_intkey {
        BTREE_PAGEFLAG_LEAFDATA | BTREE_PAGEFLAG_INTKEY
    } else {
        BTREE_PAGEFLAG_ZERODATA
    };
    let root_limits = if root_pgno == 1 {
        PageLimits::for_page1(shared.page_size, shared.usable_size)
    } else {
        PageLimits::new(shared.page_size, shared.usable_size)
    };
    let root_data = build_internal_root_data(
        root_limits,
        root_flags,
        left_pgno,
        right_pgno,
        &sep_info,
        !parent.is_intkey,
    )?;
    let mut root_page = shared.pager.get(root_pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut root_page)?;
    root_page.data = root_data;
    Ok(())
}

fn split_internal_with_parent(
    shared: &mut BtShared,
    grandparent: &MemPage,
    grand_limits: PageLimits,
    child_index: u16,
    parent: &MemPage,
    parent_limits: PageLimits,
) -> Result<()> {
    let (keys, children) = rebuild_internal_children(parent, parent_limits)?;
    if keys.len() < 2 {
        return Err(Error::new(ErrorCode::Corrupt));
    }
    let mid = keys.len() / 2;
    let left_keys = keys[..mid].to_vec();
    let right_keys = keys[mid + 1..].to_vec();
    let sep_key = keys[mid].clone();
    let left_children = children[..mid + 1].to_vec();
    let right_children = children[mid + 1..].to_vec();

    let left_pgno = parent.pgno;
    let right_pgno = allocate_page(shared);
    let flags = parent.data[parent_limits.header_start()];
    let left_data = build_internal_page_data(
        PageLimits::new(shared.page_size, shared.usable_size),
        flags,
        &left_keys,
        &left_children,
    )?;
    let right_data = build_internal_page_data(
        PageLimits::new(shared.page_size, shared.usable_size),
        flags,
        &right_keys,
        &right_children,
    )?;

    let mut left_page = shared.pager.get(left_pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut left_page)?;
    left_page.data = left_data;

    let mut right_page = shared.pager.get(right_pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut right_page)?;
    right_page.data = right_data;

    shared.pager.db_size = right_pgno.max(shared.pager.db_size);
    shared.n_page = shared.pager.db_size;

    let (mut keys_gp, mut children_gp) = rebuild_internal_children(grandparent, grand_limits)?;
    let insert_pos = child_index as usize;
    children_gp[insert_pos] = left_pgno;
    children_gp.insert(insert_pos + 1, right_pgno);
    if grandparent.is_intkey {
        if let InternalKey::Int(k) = sep_key {
            keys_gp.insert(insert_pos, InternalKey::Int(k));
        } else {
            return Err(Error::new(ErrorCode::Internal));
        }
    } else {
        if let InternalKey::Blob(blob) = sep_key {
            keys_gp.insert(insert_pos, InternalKey::Blob(blob));
        } else {
            return Err(Error::new(ErrorCode::Internal));
        }
    }

    let grand_flags = grandparent.data[grand_limits.header_start()];
    let new_data = build_internal_page_data(grand_limits, grand_flags, &keys_gp, &children_gp);
    let mut grand_page = shared.pager.get(grandparent.pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut grand_page)?;
    match new_data {
        Ok(data) => {
            grand_page.data = data;
            Ok(())
        }
        Err(_) => split_internal_root(shared, grandparent.pgno, grandparent, grand_limits),
    }
}

fn merge_leaf_with_sibling(
    shared: &mut BtShared,
    parent: &MemPage,
    parent_limits: PageLimits,
    child_index: u16,
    leaf_pgno: Pgno,
    leaf: &MemPage,
    leaf_limits: PageLimits,
) -> Result<()> {
    if !leaf.is_leaf {
        return Err(Error::new(ErrorCode::Misuse));
    }

    let use_left = child_index > 0;
    let sibling_index = if use_left { child_index - 1 } else { child_index + 1 };
    let sibling_pgno = parent.child_pgno_for_index(sibling_index, parent_limits)?;
    let (sibling_page, sibling_limits) = {
        let page = shared.pager.get(sibling_pgno, PagerGetFlags::empty())?;
        let limits = if sibling_pgno == 1 {
            PageLimits::for_page1(shared.page_size, shared.usable_size)
        } else {
            PageLimits::new(shared.page_size, shared.usable_size)
        };
        let mem_page =
            MemPage::parse_with_shared(sibling_pgno, page.data.clone(), limits, Some(shared))?;
        (mem_page, limits)
    };
    if !sibling_page.is_leaf {
        return Err(Error::new(ErrorCode::Misuse));
    }

    let (left_page, right_page, left_pgno, right_pgno, left_limits, right_limits) = if use_left {
        (
            sibling_page,
            leaf.clone(),
            sibling_pgno,
            leaf_pgno,
            sibling_limits,
            leaf_limits,
        )
    } else {
        (
            leaf.clone(),
            sibling_page,
            leaf_pgno,
            sibling_pgno,
            leaf_limits,
            sibling_limits,
        )
    };

    if left_page.n_cell > 1 && right_page.n_cell == 0 {
        return Ok(());
    }

    if left_page.n_cell > 1 && right_page.n_cell > 0 {
        let borrow_from_left = left_page.n_cell > right_page.n_cell;
        let (donor, receiver, donor_limits, receiver_limits, donor_pgno, receiver_pgno) =
            if borrow_from_left {
                (
                    left_page.clone(),
                    right_page.clone(),
                    left_limits,
                    right_limits,
                    left_pgno,
                    right_pgno,
                )
            } else {
                (
                    right_page.clone(),
                    left_page.clone(),
                    right_limits,
                    left_limits,
                    right_pgno,
                    left_pgno,
                )
            };

        let donor_index = if borrow_from_left {
            donor.n_cell - 1
        } else {
            0
        };
        let donor_cell_offset = donor.cell_ptr(donor_index, donor_limits)?;
        let donor_info = donor.parse_cell(donor_cell_offset, donor_limits)?;
        let donor_start = donor_cell_offset as usize;
        let donor_end = donor_start + donor_info.n_size as usize;
        if donor_end > donor.data.len() {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        let borrowed = donor.data[donor_start..donor_end].to_vec();

        let mut donor_cells = Vec::new();
        for i in 0..donor.n_cell {
            if i == donor_index {
                continue;
            }
            let cell_offset = donor.cell_ptr(i, donor_limits)?;
            let info = donor.parse_cell(cell_offset, donor_limits)?;
            let start = cell_offset as usize;
            let end = start + info.n_size as usize;
            if end > donor.data.len() {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            donor_cells.push(donor.data[start..end].to_vec());
        }

        let mut receiver_cells = Vec::new();
        for i in 0..receiver.n_cell {
            let cell_offset = receiver.cell_ptr(i, receiver_limits)?;
            let info = receiver.parse_cell(cell_offset, receiver_limits)?;
            let start = cell_offset as usize;
            let end = start + info.n_size as usize;
            if end > receiver.data.len() {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            receiver_cells.push(receiver.data[start..end].to_vec());
        }
        if borrow_from_left {
            receiver_cells.insert(0, borrowed);
        } else {
            receiver_cells.push(borrowed);
        }

        let flags = donor.data[donor_limits.header_start()];
        let donor_data = build_leaf_page_data(donor_limits, flags, &donor_cells)?;
        let mut donor_page = shared.pager.get(donor_pgno, PagerGetFlags::empty())?;
        shared.pager.write(&mut donor_page)?;
        donor_page.data = donor_data;

        let flags = receiver.data[receiver_limits.header_start()];
        let receiver_data = build_leaf_page_data(receiver_limits, flags, &receiver_cells)?;
        let mut receiver_page = shared.pager.get(receiver_pgno, PagerGetFlags::empty())?;
        shared.pager.write(&mut receiver_page)?;
        receiver_page.data = receiver_data;
        return Ok(());
    }

    let mut cells = Vec::new();
    for i in 0..left_page.n_cell {
        let cell_offset = left_page.cell_ptr(i, left_limits)?;
        let info = left_page.parse_cell(cell_offset, left_limits)?;
        let start = cell_offset as usize;
        let end = start + info.n_size as usize;
        if end > left_page.data.len() {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        cells.push(left_page.data[start..end].to_vec());
    }
    for i in 0..right_page.n_cell {
        let cell_offset = right_page.cell_ptr(i, right_limits)?;
        let info = right_page.parse_cell(cell_offset, right_limits)?;
        let start = cell_offset as usize;
        let end = start + info.n_size as usize;
        if end > right_page.data.len() {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        cells.push(right_page.data[start..end].to_vec());
    }

    let flags = left_page.data[left_limits.header_start()];
    let new_left_data = build_leaf_page_data(left_limits, flags, &cells)?;
    let mut left_db_page = shared.pager.get(left_pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut left_db_page)?;
    left_db_page.data = new_left_data;

    let (mut keys, mut children) = rebuild_internal_children(parent, parent_limits)?;
    let key_remove = if use_left { child_index - 1 } else { child_index };
    if (key_remove as usize) < keys.len() {
        keys.remove(key_remove as usize);
    }
    let child_remove = if use_left { child_index } else { child_index + 1 };
    if (child_remove as usize) < children.len() {
        children.remove(child_remove as usize);
    }

    let parent_flags = parent.data[parent_limits.header_start()];
    let new_parent = build_internal_page_data(parent_limits, parent_flags, &keys, &children);
    let mut parent_page = shared.pager.get(parent.pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut parent_page)?;
    match new_parent {
        Ok(data) => {
            parent_page.data = data;
        }
        Err(_) => {
            split_internal_root(shared, parent.pgno, parent, parent_limits)?;
        }
    }

    let _ = right_pgno;
    Ok(())
}

fn merge_internal_with_sibling(
    shared: &mut BtShared,
    parent: &MemPage,
    parent_limits: PageLimits,
    child_index: u16,
    child_pgno: Pgno,
    child: &MemPage,
    child_limits: PageLimits,
) -> Result<()> {
    if child.is_leaf {
        return Err(Error::new(ErrorCode::Misuse));
    }

    let use_left = child_index > 0;
    let sibling_index = if use_left { child_index - 1 } else { child_index + 1 };
    let sibling_pgno = parent.child_pgno_for_index(sibling_index, parent_limits)?;
    let (sibling_page, sibling_limits) = {
        let page = shared.pager.get(sibling_pgno, PagerGetFlags::empty())?;
        let limits = if sibling_pgno == 1 {
            PageLimits::for_page1(shared.page_size, shared.usable_size)
        } else {
            PageLimits::new(shared.page_size, shared.usable_size)
        };
        let mem_page =
            MemPage::parse_with_shared(sibling_pgno, page.data.clone(), limits, Some(shared))?;
        (mem_page, limits)
    };
    if sibling_page.is_leaf {
        return Err(Error::new(ErrorCode::Misuse));
    }

    let (left_page, right_page, left_pgno, right_pgno, left_limits, right_limits) = if use_left {
        (
            sibling_page,
            child.clone(),
            sibling_pgno,
            child_pgno,
            sibling_limits,
            child_limits,
        )
    } else {
        (
            child.clone(),
            sibling_page,
            child_pgno,
            sibling_pgno,
            child_limits,
            sibling_limits,
        )
    };

    let (mut keys, mut children) = rebuild_internal_children(&left_page, left_limits)?;
    let (right_keys, right_children) = rebuild_internal_children(&right_page, right_limits)?;
    let sep_index = if use_left { child_index - 1 } else { child_index };
    let sep_offset = parent.cell_ptr(sep_index, parent_limits)?;
    let sep_info = parent.parse_cell(sep_offset, parent_limits)?;
    if left_page.is_intkey {
        keys.push(InternalKey::Int(sep_info.n_key));
    } else {
        let payload = sep_info.payload.clone().ok_or(Error::new(ErrorCode::Internal))?;
        keys.push(InternalKey::Blob(payload));
    }
    keys.extend(right_keys);
    children.extend(right_children);

    let flags = left_page.data[left_limits.header_start()];
    let merged_data = build_internal_page_data(left_limits, flags, &keys, &children)?;
    let mut left_db_page = shared.pager.get(left_pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut left_db_page)?;
    left_db_page.data = merged_data;

    let (mut pkeys, mut pchildren) = rebuild_internal_children(parent, parent_limits)?;
    let key_remove = sep_index as usize;
    if key_remove < pkeys.len() {
        pkeys.remove(key_remove);
    }
    let child_remove = if use_left { child_index } else { child_index + 1 };
    if (child_remove as usize) < pchildren.len() {
        pchildren.remove(child_remove as usize);
    }
    let parent_flags = parent.data[parent_limits.header_start()];
    let parent_data = build_internal_page_data(parent_limits, parent_flags, &pkeys, &pchildren);
    let mut parent_page = shared.pager.get(parent.pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut parent_page)?;
    match parent_data {
        Ok(data) => {
            parent_page.data = data;
        }
        Err(_) => {
            split_internal_root(shared, parent.pgno, parent, parent_limits)?;
        }
    }

    let _ = right_pgno;
    Ok(())
}

fn parse_cell_from_bytes(page: &MemPage, limits: PageLimits, cell: &[u8]) -> Result<CellInfo> {
    if cell.is_empty() {
        return Err(Error::new(ErrorCode::Corrupt));
    }
    let mut cursor = 0usize;
    if page.child_ptr_size == 4 {
        if cell.len() < 4 {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        cursor += 4;
    }

    let (payload_size, n1) = read_varint32(cell, cursor)?;
    cursor = cursor.checked_add(n1).ok_or(Error::new(ErrorCode::Corrupt))?;

    let (n_key, n2) = if page.is_intkey {
        read_varint(cell, cursor)?
    } else if page.is_zerodata {
        (payload_size as u64, 0usize)
    } else {
        let (key_bytes, bytes) = read_varint32(cell, cursor)?;
        (key_bytes as u64, bytes)
    };
    cursor = cursor.checked_add(n2).ok_or(Error::new(ErrorCode::Corrupt))?;

    let mut info = CellInfo::default();
    info.n_key = n_key as i64;
    info.n_payload = payload_size;
    if !page.is_intkey || page.is_leaf {
        let end = cursor
            .checked_add(payload_size as usize)
            .ok_or(Error::new(ErrorCode::Corrupt))?;
        if end > cell.len() {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        if payload_size > 0 {
            info.payload = Some(cell[cursor..end].to_vec());
        }
    }
    if payload_size as u16 > page.max_local && page.max_local != 0 {
        info.overflow_pgno = None;
        info.n_local = page.payload_to_local(payload_size as i64, limits)?;
        info.n_size = (cursor + info.n_local as usize) as u16 + 4;
    } else {
        info.n_local = payload_size as u16;
        info.n_size = (cursor + payload_size as usize) as u16;
    }
    Ok(info)
}

fn split_root_leaf(
    shared: &mut BtShared,
    root_pgno: Pgno,
    mem_page: &MemPage,
    limits: PageLimits,
    insert_index: u16,
    new_cell: Vec<u8>,
) -> Result<()> {
    let mut cells = Vec::with_capacity(mem_page.n_cell as usize + 1);
    for i in 0..mem_page.n_cell {
        let cell_offset = mem_page.cell_ptr(i, limits)?;
        let info = mem_page.parse_cell(cell_offset, limits)?;
        let start = cell_offset as usize;
        let end = start + info.n_size as usize;
        if end > mem_page.data.len() {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        cells.push(mem_page.data[start..end].to_vec());
    }

    let insert_at = insert_index.min(mem_page.n_cell) as usize;
    cells.insert(insert_at, new_cell);

    let mid = cells.len() / 2;
    let left_cells = cells[..mid].to_vec();
    let right_cells = cells[mid..].to_vec();
    if right_cells.is_empty() {
        return Err(Error::new(ErrorCode::Corrupt));
    }

    let left_pgno = allocate_page(shared);
    let right_pgno = allocate_page(shared);
    let flags = mem_page.data[limits.header_start()];
    let left_data = build_leaf_page_data(PageLimits::new(shared.page_size, shared.usable_size), flags, &left_cells)?;
    let right_data = build_leaf_page_data(PageLimits::new(shared.page_size, shared.usable_size), flags, &right_cells)?;

    let mut left_page = shared.pager.get(left_pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut left_page)?;
    left_page.data = left_data;

    let mut right_page = shared.pager.get(right_pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut right_page)?;
    right_page.data = right_data;

    shared.pager.db_size = right_pgno.max(shared.pager.db_size);
    shared.n_page = shared.pager.db_size;

    let sep_info = parse_cell_from_bytes(mem_page, limits, &right_cells[0])?;
    let internal_flags = if mem_page.is_intkey {
        BTREE_PAGEFLAG_LEAFDATA | BTREE_PAGEFLAG_INTKEY
    } else {
        BTREE_PAGEFLAG_ZERODATA
    };
    let root_limits = if root_pgno == 1 {
        PageLimits::for_page1(shared.page_size, shared.usable_size)
    } else {
        PageLimits::new(shared.page_size, shared.usable_size)
    };
    let root_data = build_internal_root_data(
        root_limits,
        internal_flags,
        left_pgno,
        right_pgno,
        &sep_info,
        !mem_page.is_intkey,
    )?;
    let mut root_page = shared.pager.get(root_pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut root_page)?;
    root_page.data = root_data;
    Ok(())
}

fn split_leaf_with_parent(
    shared: &mut BtShared,
    parent: &MemPage,
    parent_limits: PageLimits,
    child_index: u16,
    leaf_pgno: Pgno,
    mem_page: &MemPage,
    limits: PageLimits,
    insert_index: u16,
    new_cell: Vec<u8>,
) -> Result<()> {
    let mut cells = Vec::with_capacity(mem_page.n_cell as usize + 1);
    for i in 0..mem_page.n_cell {
        let cell_offset = mem_page.cell_ptr(i, limits)?;
        let info = mem_page.parse_cell(cell_offset, limits)?;
        let start = cell_offset as usize;
        let end = start + info.n_size as usize;
        if end > mem_page.data.len() {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        cells.push(mem_page.data[start..end].to_vec());
    }
    let insert_at = insert_index.min(mem_page.n_cell) as usize;
    cells.insert(insert_at, new_cell);
    let mid = cells.len() / 2;
    let left_cells = cells[..mid].to_vec();
    let right_cells = cells[mid..].to_vec();
    if right_cells.is_empty() {
        return Err(Error::new(ErrorCode::Corrupt));
    }

    let flags = mem_page.data[limits.header_start()];
    let left_data = build_leaf_page_data(PageLimits::new(shared.page_size, shared.usable_size), flags, &left_cells)?;
    let right_data = build_leaf_page_data(PageLimits::new(shared.page_size, shared.usable_size), flags, &right_cells)?;

    let mut left_page = shared.pager.get(leaf_pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut left_page)?;
    left_page.data = left_data;

    let right_pgno = allocate_page(shared);
    let mut right_page = shared.pager.get(right_pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut right_page)?;
    right_page.data = right_data;

    shared.pager.db_size = right_pgno.max(shared.pager.db_size);
    shared.n_page = shared.pager.db_size;

    let sep_info = parse_cell_from_bytes(mem_page, limits, &right_cells[0])?;
    let (mut keys, mut children) = rebuild_internal_children(parent, parent_limits)?;

    let insert_pos = child_index as usize;
    children[insert_pos] = leaf_pgno;
    children.insert(insert_pos + 1, right_pgno);
    if parent.is_intkey {
        keys.insert(insert_pos, InternalKey::Int(sep_info.n_key));
    } else {
        let payload = sep_info.payload.ok_or(Error::new(ErrorCode::Internal))?;
        keys.insert(insert_pos, InternalKey::Blob(payload));
    }

    let parent_flags = parent.data[parent_limits.header_start()];
    let parent_data = build_internal_page_data(parent_limits, parent_flags, &keys, &children);
    let mut parent_page = shared.pager.get(parent.pgno, PagerGetFlags::empty())?;
    shared.pager.write(&mut parent_page)?;
    match parent_data {
        Ok(data) => {
            parent_page.data = data;
            Ok(())
        }
        Err(_) => split_internal_root(shared, parent.pgno, parent, parent_limits),
    }
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
            parent: None,
            usable_space: limits.usable_size as u16,
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

    pub fn child_pgno_for_index(&self, index: u16, limits: PageLimits) -> Result<Pgno> {
        if index < self.n_cell {
            let cell_offset = self.cell_ptr(index, limits)?;
            self.child_pgno(cell_offset)
        } else if index == self.n_cell {
            self.rightmost_ptr.ok_or(Error::new(ErrorCode::Corrupt))
        } else {
            Err(Error::new(ErrorCode::Range))
        }
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
            let overflow_offset = payload_end;
            info.overflow_pgno = read_u32(&self.data, overflow_offset);
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

    fn is_underfull(&self, limits: PageLimits) -> Result<bool> {
        let free = self.compute_free_space(limits)? as i32;
        Ok(free > (limits.usable_size as i32 / 2))
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
            free_pages: Vec::new(),
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
        let mut mem_page = _cursor.load_page(&mut shared_guard, root_pgno)?.0;
        let mut limits = if root_pgno == 1 {
            PageLimits::for_page1(shared_guard.page_size, shared_guard.usable_size)
        } else {
            PageLimits::new(shared_guard.page_size, shared_guard.usable_size)
        };
        if !mem_page.is_leaf {
            if mem_page.is_intkey {
                let _ = _cursor.table_moveto(_payload.n_key, false)?;
            } else if let Some(key) = _payload.key.clone() {
                let _ = _cursor.index_moveto(&UnpackedRecord { key })?;
            }
            if let Some(ref page) = _cursor.page {
                mem_page = page.clone();
                limits = if mem_page.pgno == 1 {
                    PageLimits::for_page1(shared_guard.page_size, shared_guard.usable_size)
                } else {
                    PageLimits::new(shared_guard.page_size, shared_guard.usable_size)
                };
            } else {
                return Err(Error::new(ErrorCode::Internal));
            }
        }

        let (mut cell, mut overflow, needs_overflow_ptr) =
            build_cell(&mem_page, limits, _payload)?;
        if !overflow.pages.is_empty() {
            let pages_len = overflow.pages.len();
            let mut pgno_list = Vec::with_capacity(pages_len);
            for _ in 0..pages_len {
                pgno_list.push(allocate_page(&mut shared_guard));
            }
            let first_pgno = pgno_list[0];
            overflow.first = Some(first_pgno);
            for (idx, mut page) in overflow.pages.into_iter().enumerate() {
                let pgno = pgno_list[idx];
                let next_pgno = if idx + 1 < pages_len {
                    pgno_list[idx + 1]
                } else {
                    0
                };
                let _ = write_u32(&mut page, 0, next_pgno);
                let mut db_page = shared_guard.pager.get(pgno, PagerGetFlags::empty())?;
                shared_guard.pager.write(&mut db_page)?;
                db_page.data = page;
                shared_guard.pager.db_size = pgno.max(shared_guard.pager.db_size);
            }
            shared_guard.n_page = shared_guard.pager.db_size;
            if needs_overflow_ptr {
                cell.extend_from_slice(&first_pgno.to_be_bytes());
            }
        }

        let cell_size = cell.len();
        if cell_size > mem_page.max_local as usize && mem_page.max_local != 0 {
            return Err(Error::new(ErrorCode::Internal));
        }

        // Calculate insert_index before checking if split is needed
        let insert_index = if _flags.contains(BtreeInsertFlags::APPEND) || _cursor.state != CursorState::Valid {
            mem_page.n_cell
        } else {
            _cursor.ix.min(mem_page.n_cell)
        };

        let header_start = limits.header_start();
        let header_size = mem_page.header_size();
        let ptr_array_end = header_start + header_size + (mem_page.n_cell as usize * 2);
        let cell_offset = mem_page.cell_content_offset(limits)?;
        if cell_offset < ptr_array_end + cell_size {
            if mem_page.is_leaf {
                if let (Some(parent), Some(child_index)) =
                    (_cursor.page_stack.last(), _cursor.idx_stack.last())
                {
                    let parent_limits = if parent.pgno == 1 {
                        PageLimits::for_page1(shared_guard.page_size, shared_guard.usable_size)
                    } else {
                        PageLimits::new(shared_guard.page_size, shared_guard.usable_size)
                    };
                    split_leaf_with_parent(
                        &mut shared_guard,
                        parent,
                        parent_limits,
                        *child_index,
                        root_pgno,
                        &mem_page,
                        limits,
                        insert_index,
                        cell,
                    )?;
                    return Ok(());
                }
                if root_pgno == mem_page.pgno {
                    split_root_leaf(
                        &mut shared_guard,
                        root_pgno,
                        &mem_page,
                        limits,
                        insert_index,
                        cell,
                    )?;
                    return Ok(());
                }
            } else if let (Some(parent), Some(child_index)) =
                (_cursor.page_stack.get(_cursor.page_stack.len().saturating_sub(2)),
                 _cursor.idx_stack.get(_cursor.idx_stack.len().saturating_sub(2)))
            {
                let parent_limits = if parent.pgno == 1 {
                    PageLimits::for_page1(shared_guard.page_size, shared_guard.usable_size)
                } else {
                    PageLimits::new(shared_guard.page_size, shared_guard.usable_size)
                };
                split_internal_with_parent(
                    &mut shared_guard,
                    parent,
                    parent_limits,
                    *child_index,
                    &mem_page,
                    limits,
                )?;
                return Ok(());
            } else if root_pgno == mem_page.pgno {
                split_internal_root(&mut shared_guard, root_pgno, &mem_page, limits)?;
                return Ok(());
            }
            return Err(Error::new(ErrorCode::Full));
        }
        let new_cell_offset = cell_offset - cell_size;
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
        if let Some(overflow_pgno) = info.overflow_pgno {
            free_overflow_chain(&mut shared_guard, overflow_pgno)?;
        }
        let cell_size = info.n_size as i32;

        let from = ptr_array_start + ((_cursor.ix as usize + 1) * 2);
        let to = ptr_array_start + (_cursor.ix as usize * 2);
        let ptr_end = ptr_array_start + (mem_page.n_cell as usize * 2);
        data.copy_within(from..ptr_end, to);

        let new_n_cell = mem_page.n_cell - 1;
        write_u16(data, header_start + 3, new_n_cell)?;

        if mem_page.is_underfull(leaf_limits).unwrap_or(false) {
            if let (Some(parent), Some(child_index)) =
                (_cursor.page_stack.last(), _cursor.idx_stack.last())
            {
                let parent_limits = if parent.pgno == 1 {
                    PageLimits::for_page1(shared_guard.page_size, shared_guard.usable_size)
                } else {
                    PageLimits::new(shared_guard.page_size, shared_guard.usable_size)
                };
                let leaf_limits = if root_pgno == 1 {
                    PageLimits::for_page1(shared_guard.page_size, shared_guard.usable_size)
                } else {
                    PageLimits::new(shared_guard.page_size, shared_guard.usable_size)
                };
                if mem_page.is_leaf {
                    let _ = merge_leaf_with_sibling(
                        &mut shared_guard,
                        parent,
                        parent_limits,
                        *child_index,
                        root_pgno,
                        &mem_page,
                        leaf_limits,
                    );
                } else {
                    let _ = merge_internal_with_sibling(
                        &mut shared_guard,
                        parent,
                        parent_limits,
                        *child_index,
                        root_pgno,
                        &mem_page,
                        leaf_limits,
                    );
                }
            }
        }

        _cursor.state = CursorState::Invalid;
        let _ = collapse_root_if_empty(&mut shared_guard, root_pgno);
        Ok(())
    }

    /// sqlite3BtreeCreateTable
    pub fn create_table(&mut self, _flags: u8) -> Result<Pgno> {
        let mut shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        let new_pgno = allocate_page(&mut shared);
        let mut page = shared.pager.get(new_pgno, PagerGetFlags::empty())?;
        shared.pager.write(&mut page)?;
        page.data.fill(0);
        let limits = PageLimits::new(shared.page_size, shared.usable_size);
        let header_start = limits.header_start();
        let flags = if _flags & BTREE_INTKEY != 0 {
            BTREE_PAGEFLAG_LEAFDATA | BTREE_PAGEFLAG_INTKEY | BTREE_PAGEFLAG_LEAF
        } else {
            BTREE_PAGEFLAG_ZERODATA | BTREE_PAGEFLAG_LEAF
        };
        page.data[header_start] = flags;
        write_u16(&mut page.data, header_start + 1, 0)?;
        write_u16(&mut page.data, header_start + 3, 0)?;
        write_u16(&mut page.data, header_start + 5, shared.usable_size as u16)?;
        page.data[header_start + 7] = 0;
        shared.pager.db_size = new_pgno.max(shared.pager.db_size);
        shared.n_page = shared.pager.db_size;
        Ok(new_pgno)
    }

    /// sqlite3BtreeDropTable
    pub fn drop_table(&mut self, _root_page: Pgno) -> Result<()> {
        if _root_page == 0 {
            return Err(Error::new(ErrorCode::Range));
        }
        if _root_page != 1 {
            let mut shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
            shared.free_pages.push(_root_page);
            update_free_page_count(&mut shared, 1)?;
        }
        Ok(())
    }

    /// sqlite3BtreeClearTable
    pub fn clear_table(&mut self, _root_page: Pgno) -> Result<i64> {
        let mut shared = self.shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        let mut page = shared.pager.get(_root_page, PagerGetFlags::empty())?;
        shared.pager.write(&mut page)?;
        let limits = if _root_page == 1 {
            PageLimits::for_page1(shared.page_size, shared.usable_size)
        } else {
            PageLimits::new(shared.page_size, shared.usable_size)
        };
        let mem_page = MemPage::parse_with_shared(_root_page, page.data.clone(), limits, Some(&shared))?;
        for i in 0..mem_page.n_cell {
            let cell_offset = mem_page.cell_ptr(i, limits)?;
            let info = mem_page.parse_cell(cell_offset, limits)?;
            if let Some(overflow_pgno) = info.overflow_pgno {
                free_overflow_chain(&mut shared, overflow_pgno)?;
            }
        }
        let header_start = limits.header_start();
        write_u16(&mut page.data, header_start + 1, 0)?;
        write_u16(&mut page.data, header_start + 3, 0)?;
        write_u16(&mut page.data, header_start + 5, shared.usable_size as u16)?;
        page.data[header_start + 7] = 0;
        Ok(0)
    }

    /// sqlite3BtreeClearTableOfCursor
    pub fn clear_table_of_cursor(&mut self, _cursor: &mut BtCursor) -> Result<i64> {
        self.clear_table(_cursor.root_page)
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
    fn set_to_cell(&mut self, page: MemPage, limits: PageLimits, index: u16) -> Result<()> {
        let cell_offset = page.cell_ptr(index, limits)?;
        let info = page.parse_cell(cell_offset, limits)?;
        self.info = info;
        self.n_key = self.info.n_key;
        self.ix = index;
        self.state = CursorState::Valid;
        self.page = Some(page);
        Ok(())
    }

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

    fn descend_leftmost(&mut self, shared: &mut BtShared, pgno: Pgno) -> Result<(MemPage, PageLimits)> {
        let mut current_pgno = pgno;
        loop {
            let (page, limits) = self.load_page(shared, current_pgno)?;
            if page.is_leaf {
                return Ok((page, limits));
            }
            self.page_stack.push(page);
            self.idx_stack.push(0);
            let child = self.page_stack.last().unwrap().child_pgno_for_index(0, limits)?;
            current_pgno = child;
        }
    }

    fn descend_rightmost(&mut self, shared: &mut BtShared, pgno: Pgno) -> Result<(MemPage, PageLimits)> {
        let mut current_pgno = pgno;
        loop {
            let (page, limits) = self.load_page(shared, current_pgno)?;
            if page.is_leaf {
                return Ok((page, limits));
            }
            let child_index = page.n_cell;
            self.page_stack.push(page);
            self.idx_stack.push(child_index);
            let parent = self.page_stack.last().unwrap();
            let child = parent.child_pgno_for_index(child_index, limits)?;
            current_pgno = child;
        }
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
        let shared = self
            .bt_shared
            .upgrade()
            .ok_or(Error::new(ErrorCode::Internal))?;
        let mut shared_guard = shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        self.page_stack.clear();
        self.idx_stack.clear();
        let (mem_page, limits) = self.descend_leftmost(&mut shared_guard, self.root_page)?;
        if mem_page.n_cell == 0 {
            self.state = CursorState::Invalid;
            return Ok(true);
        }
        self.set_to_cell(mem_page, limits, 0)?;
        Ok(false)
    }

    /// sqlite3BtreeLast
    pub fn last(&mut self) -> Result<bool> {
        let shared = self
            .bt_shared
            .upgrade()
            .ok_or(Error::new(ErrorCode::Internal))?;
        let mut shared_guard = shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        self.page_stack.clear();
        self.idx_stack.clear();
        let (mem_page, limits) = self.descend_rightmost(&mut shared_guard, self.root_page)?;
        if mem_page.n_cell == 0 {
            self.state = CursorState::Invalid;
            return Ok(true);
        }
        let last_index = mem_page.n_cell - 1;
        self.set_to_cell(mem_page, limits, last_index)?;
        Ok(false)
    }

    /// sqlite3BtreeNext
    pub fn next(&mut self, _flags: i32) -> Result<()> {
        let page = self.page.as_ref().ok_or(Error::new(ErrorCode::Corrupt))?;
        if !page.is_leaf {
            return Err(Error::new(ErrorCode::Internal));
        }
        let next_ix = self.ix.saturating_add(1);
        let shared = self
            .bt_shared
            .upgrade()
            .ok_or(Error::new(ErrorCode::Internal))?;
        let mut shared_guard = shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        let limits = if page.pgno == 1 {
            PageLimits::for_page1(shared_guard.page_size, shared_guard.usable_size)
        } else {
            PageLimits::new(shared_guard.page_size, shared_guard.usable_size)
        };
        if next_ix < page.n_cell {
            self.set_to_cell(page.clone(), limits, next_ix)?;
            return Ok(());
        }
        while let (Some(parent), Some(child_index)) = (self.page_stack.pop(), self.idx_stack.pop())
        {
            if child_index < parent.n_cell {
                let next_child = child_index + 1;
                self.page_stack.push(parent);
                self.idx_stack.push(next_child);
                let parent_ref = self.page_stack.last().unwrap();
                let parent_limits = if parent_ref.pgno == 1 {
                    PageLimits::for_page1(shared_guard.page_size, shared_guard.usable_size)
                } else {
                    PageLimits::new(shared_guard.page_size, shared_guard.usable_size)
                };
                let child_pgno = parent_ref.child_pgno_for_index(next_child, parent_limits)?;
                let (leaf, leaf_limits) =
                    self.descend_leftmost(&mut shared_guard, child_pgno)?;
                if leaf.n_cell == 0 {
                    self.state = CursorState::Invalid;
                    return Ok(());
                }
                self.set_to_cell(leaf, leaf_limits, 0)?;
                return Ok(());
            }
        }
        self.state = CursorState::Invalid;
        Ok(())
    }

    /// sqlite3BtreePrevious
    pub fn previous(&mut self, _flags: i32) -> Result<()> {
        let page = self.page.as_ref().ok_or(Error::new(ErrorCode::Corrupt))?;
        if !page.is_leaf {
            return Err(Error::new(ErrorCode::Internal));
        }
        let shared = self
            .bt_shared
            .upgrade()
            .ok_or(Error::new(ErrorCode::Internal))?;
        let mut shared_guard = shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        let limits = if page.pgno == 1 {
            PageLimits::for_page1(shared_guard.page_size, shared_guard.usable_size)
        } else {
            PageLimits::new(shared_guard.page_size, shared_guard.usable_size)
        };
        if self.ix > 0 {
            let prev_ix = self.ix - 1;
            self.set_to_cell(page.clone(), limits, prev_ix)?;
            return Ok(());
        }
        while let (Some(parent), Some(child_index)) = (self.page_stack.pop(), self.idx_stack.pop())
        {
            if child_index > 0 {
                let prev_child = child_index - 1;
                self.page_stack.push(parent);
                self.idx_stack.push(prev_child);
                let parent_ref = self.page_stack.last().unwrap();
                let parent_limits = if parent_ref.pgno == 1 {
                    PageLimits::for_page1(shared_guard.page_size, shared_guard.usable_size)
                } else {
                    PageLimits::new(shared_guard.page_size, shared_guard.usable_size)
                };
                let child_pgno = parent_ref.child_pgno_for_index(prev_child, parent_limits)?;
                let (leaf, leaf_limits) =
                    self.descend_rightmost(&mut shared_guard, child_pgno)?;
                if leaf.n_cell == 0 {
                    self.state = CursorState::Invalid;
                    return Ok(());
                }
                let last_index = leaf.n_cell - 1;
                self.set_to_cell(leaf, leaf_limits, last_index)?;
                return Ok(());
            }
        }
        self.state = CursorState::Invalid;
        Ok(())
    }

    /// sqlite3BtreeIsEmpty
    pub fn is_empty(&mut self) -> Result<bool> {
        let shared = self
            .bt_shared
            .upgrade()
            .ok_or(Error::new(ErrorCode::Internal))?;
        let mut shared_guard = shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        self.page_stack.clear();
        self.idx_stack.clear();
        let (mem_page, _) = self.descend_leftmost(&mut shared_guard, self.root_page)?;
        Ok(mem_page.n_cell == 0)
    }

    /// sqlite3BtreeCount
    pub fn count(&mut self) -> Result<i64> {
        let mut total = 0i64;
        let _ = self.first()?;
        while self.is_valid() {
            total += 1;
            self.next(0)?;
        }
        Ok(total)
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
        if self.info.overflow_pgno.is_some() {
            None
        } else {
            self.info.payload.as_deref()
        }
    }

    /// sqlite3BtreePayload
    pub fn payload(&self, offset: u32, amount: u32) -> Result<Vec<u8>> {
        let payload = if self.info.overflow_pgno.is_some() {
            self.read_overflow_payload()?
        } else {
            self.info.payload.clone().ok_or(Error::new(ErrorCode::Corrupt))?
        };
        let start = offset as usize;
        let end = start.checked_add(amount as usize).ok_or(Error::new(ErrorCode::Corrupt))?;
        if end > payload.len() {
            return Err(Error::new(ErrorCode::Corrupt));
        }
        Ok(payload[start..end].to_vec())
    }

    fn read_overflow_payload(&self) -> Result<Vec<u8>> {
        let shared = self
            .bt_shared
            .upgrade()
            .ok_or(Error::new(ErrorCode::Internal))?;
        let mut shared_guard = shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        let mut result = self.info.payload.clone().unwrap_or_default();
        let mut remaining = self.info.n_payload.saturating_sub(self.info.n_local as u32);
        let mut next = self.info.overflow_pgno.ok_or(Error::new(ErrorCode::Corrupt))?;
        let ovfl_size = shared_guard.usable_size.saturating_sub(4) as usize;

        while remaining > 0 {
            let page = shared_guard.pager.get(next, PagerGetFlags::empty())?;
            if page.data.len() < 4 {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            let next_pgno = read_u32(&page.data, 0).ok_or(Error::new(ErrorCode::Corrupt))?;
            let take = std::cmp::min(remaining as usize, ovfl_size);
            let start = 4;
            let end = start + take;
            if end > page.data.len() {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            result.extend_from_slice(&page.data[start..end]);
            remaining -= take as u32;
            if remaining == 0 {
                break;
            }
            if next_pgno == 0 {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            next = next_pgno;
        }

        Ok(result)
    }

    /// sqlite3BtreePayloadChecked
    pub fn payload_checked(&self, offset: u32, amount: u32) -> Result<Vec<u8>> {
        self.payload(offset, amount)
    }

    /// sqlite3BtreePutData
    pub fn put_data(&mut self, _offset: u32, _amount: u32, _data: &[u8]) -> Result<()> {
        if self.info.overflow_pgno.is_some() {
            return Err(Error::new(ErrorCode::Internal));
        }
        let page = self.page.as_ref().ok_or(Error::new(ErrorCode::Corrupt))?;
        let shared = self
            .bt_shared
            .upgrade()
            .ok_or(Error::new(ErrorCode::Internal))?;
        let mut shared_guard = shared.write().map_err(|_| Error::new(ErrorCode::Internal))?;
        let limits = if page.pgno == 1 {
            PageLimits::for_page1(shared_guard.page_size, shared_guard.usable_size)
        } else {
            PageLimits::new(shared_guard.page_size, shared_guard.usable_size)
        };
        let cell_offset = page.cell_ptr(self.ix, limits)?;
        let mut cursor = cell_offset as usize;
        if page.child_ptr_size == 4 {
            cursor += 4;
        }
        let (_, n1) = read_varint32(&page.data, cursor)?;
        cursor += n1;
        if page.is_intkey {
            let (_, n2) = read_varint(&page.data, cursor)?;
            cursor += n2;
        } else {
            let (_, n2) = read_varint32(&page.data, cursor)?;
            cursor += n2;
        }
        let payload_start = cursor;
        let payload_end = payload_start + self.info.n_local as usize;
        let write_start = payload_start + _offset as usize;
        let write_end = write_start + _amount as usize;
        if write_end > payload_end || _data.len() != _amount as usize {
            return Err(Error::new(ErrorCode::Range));
        }
        let mut db_page = shared_guard.pager.get(page.pgno, PagerGetFlags::empty())?;
        shared_guard.pager.write(&mut db_page)?;
        db_page.data[write_start..write_end].copy_from_slice(_data);
        Ok(())
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
