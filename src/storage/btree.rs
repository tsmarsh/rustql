//! B-tree implementation

use std::sync::{Arc, Weak, RwLock};

use bitflags::bitflags;

use crate::error::{Error, Result};

type Pgno = u32;

const BTREE_PAGEFLAG_INTKEY: u8 = 0x01;
const BTREE_PAGEFLAG_LEAF: u8 = 0x08;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransState {
    None,
    Read,
    Write,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorState {
    Invalid,
    Valid,
    RequireSeek,
    Fault,
}

bitflags! {
    pub struct CursorHints: u8 {
        const NONE = 0;
    }
}

pub struct Btree {
    pub shared: Arc<RwLock<BtShared>>,
}

pub struct BtShared {
    pub pager: Pager,
    pub db: Weak<()>,
    pub cursor_list: Vec<BtCursor>,
    pub page1: Option<MemPage>,
    pub page_size: u32,
    pub usable_size: u32,
    pub n_transaction: i32,
    pub in_transaction: TransState,
    pub schema_cookie: u32,
    pub file_format: u8,
}

pub struct BtCursor {
    pub btree: Arc<Btree>,
    pub root_page: Pgno,
    pub page_stack: Vec<MemPage>,
    pub idx_stack: Vec<u16>,
    pub state: CursorState,
    pub hints: CursorHints,
    pub key: Option<Vec<u8>>,
}

pub struct MemPage {
    pub pgno: Pgno,
    pub data: Vec<u8>,
    pub is_init: bool,
    pub is_leaf: bool,
    pub is_intkey: bool,
    pub n_cell: u16,
    pub cell_offset: u16,
    pub free_bytes: u16,
    pub rightmost_ptr: Option<Pgno>,
    pub n_overflow: u8,
}

pub struct Pager;

fn read_u16(data: &[u8], offset: usize) -> Option<u16> {
    data.get(offset..offset + 2)
        .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u32(data: &[u8], offset: usize) -> Option<u32> {
    data.get(offset..offset + 4).map(|bytes| {
        u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    })
}

impl MemPage {
    pub fn parse(pgno: Pgno, data: Vec<u8>) -> Result<Self> {
        if data.len() < 8 {
            return Err(Error);
        }

        let flags = data[0];
        let is_leaf = (flags & BTREE_PAGEFLAG_LEAF) != 0;
        let is_intkey = (flags & BTREE_PAGEFLAG_INTKEY) != 0;
        let header_size = if is_leaf { 8 } else { 12 };

        if data.len() < header_size {
            return Err(Error);
        }

        let n_cell = read_u16(&data, 3).ok_or(Error)?;
        let cell_offset = read_u16(&data, 5).ok_or(Error)?;
        let free_bytes = data[7] as u16;
        let rightmost_ptr = if is_leaf {
            None
        } else {
            Some(read_u32(&data, 8).ok_or(Error)?)
        };

        Ok(Self {
            pgno,
            data,
            is_init: true,
            is_leaf,
            is_intkey,
            n_cell,
            cell_offset,
            free_bytes,
            rightmost_ptr,
            n_overflow: 0,
        })
    }

    pub fn header_size(&self) -> usize {
        if self.is_leaf {
            8
        } else {
            12
        }
    }

    pub fn cell_ptr(&self, index: u16) -> Result<u16> {
        if index >= self.n_cell {
            return Err(Error);
        }
        let offset = self.header_size() + (index as usize * 2);
        read_u16(&self.data, offset).ok_or(Error)
    }

    pub fn cell_ptrs(&self) -> Result<Vec<u16>> {
        let mut pointers = Vec::with_capacity(self.n_cell as usize);
        for i in 0..self.n_cell {
            pointers.push(self.cell_ptr(i)?);
        }
        Ok(pointers)
    }
}

impl Btree {
    /// sqlite3BtreeOpen
    pub fn open() -> Result<Self> {
        Err(Error)
    }

    /// sqlite3BtreeClose
    pub fn close(&mut self) -> Result<()> {
        Err(Error)
    }

    /// sqlite3BtreeSetPageSize
    pub fn set_page_size(&mut self, _size: u32) -> Result<()> {
        Err(Error)
    }

    /// sqlite3BtreeGetPageSize
    pub fn page_size(&self) -> u32 {
        0
    }

    /// sqlite3BtreeBeginTrans
    pub fn begin_trans(&mut self) -> Result<()> {
        Err(Error)
    }

    /// sqlite3BtreeCommit
    pub fn commit(&mut self) -> Result<()> {
        Err(Error)
    }

    /// sqlite3BtreeRollback
    pub fn rollback(&mut self) -> Result<()> {
        Err(Error)
    }

    /// sqlite3BtreeCursor
    pub fn cursor(&self, _root_page: Pgno) -> Result<BtCursor> {
        Err(Error)
    }

    /// sqlite3BtreeCloseCursor
    pub fn close_cursor(&self, _cursor: BtCursor) -> Result<()> {
        Err(Error)
    }

    /// sqlite3BtreeFirst
    pub fn first(&self, _cursor: &mut BtCursor) -> Result<()> {
        Err(Error)
    }

    /// sqlite3BtreeLast
    pub fn last(&self, _cursor: &mut BtCursor) -> Result<()> {
        Err(Error)
    }

    /// sqlite3BtreeNext
    pub fn next(&self, _cursor: &mut BtCursor) -> Result<()> {
        Err(Error)
    }

    /// sqlite3BtreePrevious
    pub fn previous(&self, _cursor: &mut BtCursor) -> Result<()> {
        Err(Error)
    }

    /// sqlite3BtreeInsert
    pub fn insert(&mut self, _cursor: &mut BtCursor, _key: &[u8], _data: &[u8]) -> Result<()> {
        Err(Error)
    }

    /// sqlite3BtreeDelete
    pub fn delete(&mut self, _cursor: &mut BtCursor) -> Result<()> {
        Err(Error)
    }

    /// sqlite3BtreeKey
    pub fn key(&self, _cursor: &BtCursor) -> Result<Vec<u8>> {
        Err(Error)
    }

    /// sqlite3BtreeData
    pub fn data(&self, _cursor: &BtCursor) -> Result<Vec<u8>> {
        Err(Error)
    }

    /// sqlite3BtreeCreateTable
    pub fn create_table(&mut self) -> Result<Pgno> {
        Err(Error)
    }

    /// sqlite3BtreeDropTable
    pub fn drop_table(&mut self, _root_page: Pgno) -> Result<()> {
        Err(Error)
    }

    /// sqlite3BtreeClearTable
    pub fn clear_table(&mut self, _root_page: Pgno) -> Result<()> {
        Err(Error)
    }
}
