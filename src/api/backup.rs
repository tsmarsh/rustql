//! Online backup API
//!
//! Translation of sqlite3/src/backup.c. Provides incremental backup
//! support for copying a live database to another database.

use std::sync::Arc;

use crate::error::{Error, ErrorCode, Result};
use crate::storage::btree::{Btree, TransState};
use crate::storage::pager::{JournalMode, PagerGetFlags};
use crate::types::Pgno;

use super::connection::SqliteConnection;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupStepResult {
    More,
    Done,
}

pub struct Backup {
    dest_btree: Arc<Btree>,
    dest_db: String,
    src_btree: Arc<Btree>,
    src_db: String,
    dest_pgno: Pgno,
    src_npage: Pgno,
    remaining: i32,
    pagecount: i32,
    is_attached: bool,
    dest_schema: u32,
}

impl Backup {
    fn new(
        dest_btree: Arc<Btree>,
        dest_db: String,
        src_btree: Arc<Btree>,
        src_db: String,
    ) -> Result<Self> {
        let dest_schema = btree_schema_cookie(&dest_btree)?;
        Ok(Self {
            dest_btree,
            dest_db,
            src_btree,
            src_db,
            dest_pgno: 1,
            src_npage: 0,
            remaining: -1,
            pagecount: -1,
            is_attached: false,
            dest_schema,
        })
    }

    pub fn step(&mut self, n_page: i32) -> Result<BackupStepResult> {
        let src_state = self.src_btree.txn_state();
        if src_state == TransState::Write {
            return Err(Error::new(ErrorCode::Busy));
        }

        if self.dest_pgno == 1 {
            self.ensure_dest_page_size()?;
        }

        self.src_npage = btree_page_count(&self.src_btree)?;

        let mut copied = 0;
        let max_pages = if n_page < 0 { i32::MAX } else { n_page };
        while self.dest_pgno <= self.src_npage && copied < max_pages {
            self.copy_page(self.dest_pgno)?;
            self.dest_pgno += 1;
            copied += 1;
        }

        self.update_progress(self.src_npage);

        if self.dest_pgno > self.src_npage {
            truncate_dest(&self.dest_btree, self.src_npage)?;
            return Ok(BackupStepResult::Done);
        }

        if !self.is_attached {
            self.attach();
        }

        Ok(BackupStepResult::More)
    }

    pub fn remaining(&self) -> i32 {
        self.remaining
    }

    pub fn pagecount(&self) -> i32 {
        self.pagecount
    }

    pub fn finish(mut self) -> Result<()> {
        self.detach();
        Ok(())
    }

    pub fn on_page_modified(&mut self, pgno: Pgno) {
        if pgno < self.dest_pgno {
            self.dest_pgno = pgno;
        }
    }

    fn ensure_dest_page_size(&self) -> Result<()> {
        let (dest_pages, dest_page_size, dest_mode, dest_is_memdb) =
            btree_pager_state(&self.dest_btree)?;
        let src_page_size = btree_page_size(&self.src_btree)?;

        if dest_pages > 0 && dest_page_size != src_page_size {
            return Err(Error::new(ErrorCode::ReadOnly));
        }

        if (dest_mode == JournalMode::Wal || dest_is_memdb) && dest_page_size != src_page_size {
            return Err(Error::new(ErrorCode::ReadOnly));
        }

        if dest_pages == 0 && dest_page_size != src_page_size {
            self.dest_btree.set_page_size(src_page_size, 0, false)?;
        }

        Ok(())
    }

    fn copy_page(&self, pgno: Pgno) -> Result<()> {
        let src_data = with_shared_write(&self.src_btree, |shared| {
            let mut src_page = shared.pager.get(pgno, PagerGetFlags::empty())?;
            let data = src_page.data.clone();
            crate::storage::pager::Pager::page_unref(&mut src_page);
            Ok(data)
        })?;

        with_shared_write(&self.dest_btree, |shared| {
            let mut dest_page = shared.pager.get(pgno, PagerGetFlags::empty())?;
            shared.pager.write(&mut dest_page)?;
            if dest_page.data.len() != src_data.len() {
                return Err(Error::new(ErrorCode::Corrupt));
            }
            dest_page.data.copy_from_slice(&src_data);
            crate::storage::pager::Pager::page_unref(&mut dest_page);
            Ok(())
        })?;

        Ok(())
    }

    fn update_progress(&mut self, src_npage: Pgno) {
        self.pagecount = src_npage as i32;
        if self.dest_pgno > src_npage {
            self.remaining = 0;
        } else {
            self.remaining = (src_npage + 1 - self.dest_pgno) as i32;
        }
    }

    fn attach(&mut self) {
        self.src_btree.backup_started();
        self.dest_btree.backup_started();
        self.is_attached = true;
    }

    fn detach(&mut self) {
        if self.is_attached {
            self.src_btree.backup_finished();
            self.dest_btree.backup_finished();
            self.is_attached = false;
        }
    }
}

impl Drop for Backup {
    fn drop(&mut self) {
        self.detach();
    }
}

// ============================================================================
// Public API Functions
// ============================================================================

pub fn sqlite3_backup_init(
    dest_conn: &SqliteConnection,
    dest_db: &str,
    src_conn: &SqliteConnection,
    src_db: &str,
) -> Result<Backup> {
    if std::ptr::eq(dest_conn, src_conn) && dest_db.eq_ignore_ascii_case(src_db) {
        return Err(Error::with_message(
            ErrorCode::Error,
            "source and destination must be distinct",
        ));
    }

    let dest_btree = find_btree(dest_conn, dest_db)?;
    let src_btree = find_btree(src_conn, src_db)?;

    if dest_btree.txn_state() != TransState::None {
        return Err(Error::with_message(
            ErrorCode::Error,
            "destination database is in use",
        ));
    }

    if btree_is_readonly(&dest_btree)? {
        return Err(Error::new(ErrorCode::ReadOnly));
    }

    Backup::new(
        dest_btree,
        dest_db.to_string(),
        src_btree,
        src_db.to_string(),
    )
}

pub fn sqlite3_backup_step(backup: &mut Backup, n_page: i32) -> Result<BackupStepResult> {
    backup.step(n_page)
}

pub fn sqlite3_backup_remaining(backup: &Backup) -> i32 {
    backup.remaining()
}

pub fn sqlite3_backup_pagecount(backup: &Backup) -> i32 {
    backup.pagecount()
}

pub fn sqlite3_backup_finish(backup: Backup) -> Result<()> {
    backup.finish()
}

// ============================================================================
// Helpers
// ============================================================================

fn find_btree(conn: &SqliteConnection, name: &str) -> Result<Arc<Btree>> {
    let db = conn.find_db(name).ok_or_else(|| {
        Error::with_message(ErrorCode::Error, format!("unknown database {}", name))
    })?;
    let btree = db.btree.as_ref().ok_or_else(|| {
        Error::with_message(ErrorCode::Error, format!("database {} is not open", name))
    })?;
    Ok(Arc::clone(btree))
}

fn with_shared_write<T, F>(btree: &Btree, f: F) -> Result<T>
where
    F: FnOnce(&mut crate::storage::btree::BtShared) -> Result<T>,
{
    let mut shared = btree
        .shared
        .write()
        .map_err(|_| Error::new(ErrorCode::Internal))?;
    f(&mut shared)
}

fn with_shared_read<T, F>(btree: &Btree, f: F) -> Result<T>
where
    F: FnOnce(&crate::storage::btree::BtShared) -> T,
{
    let shared = btree
        .shared
        .read()
        .map_err(|_| Error::new(ErrorCode::Internal))?;
    Ok(f(&shared))
}

fn btree_page_count(btree: &Btree) -> Result<Pgno> {
    with_shared_read(btree, |shared| shared.pager.page_count())
}

fn btree_page_size(btree: &Btree) -> Result<u32> {
    with_shared_read(btree, |shared| shared.pager.get_page_size())
}

fn btree_is_readonly(btree: &Btree) -> Result<bool> {
    with_shared_read(btree, |shared| shared.pager.is_readonly())
}

fn btree_schema_cookie(btree: &Btree) -> Result<u32> {
    with_shared_read(btree, |shared| shared.schema_cookie)
}

fn btree_pager_state(btree: &Btree) -> Result<(Pgno, u32, JournalMode, bool)> {
    with_shared_read(btree, |shared| {
        (
            shared.pager.page_count(),
            shared.pager.get_page_size(),
            shared.pager.get_journal_mode(),
            shared.pager.is_memdb(),
        )
    })
}

fn truncate_dest(btree: &Btree, pgno: Pgno) -> Result<()> {
    with_shared_write(btree, |shared| {
        shared.pager.truncate_image(pgno);
        Ok(())
    })
}
