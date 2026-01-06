//! B-tree implementation

use std::sync::{Arc, Weak, RwLock};

use bitflags::bitflags;

use crate::error::{Error, Result};

type Pgno = u32;

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
    pub n_overflow: u8,
}

pub struct Pager;

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
