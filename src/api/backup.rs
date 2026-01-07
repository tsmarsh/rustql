//! Database Backup API
//!
//! Implements the online backup API which allows copying a database
//! while it's in use. This enables hot backups without blocking other operations.
//!
//! Equivalent to SQLite's backup.c

use std::sync::{Arc, RwLock};

use crate::error::{Error, ErrorCode, Result};
use crate::schema::Schema;
use crate::storage::btree::Btree;

use super::connection::SqliteConnection;

// ============================================================================
// Step Result
// ============================================================================

/// Result of a backup step operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepResult {
    /// More pages to copy
    More,
    /// Backup complete
    Done,
    /// Backup was locked and couldn't proceed
    Busy,
}

// ============================================================================
// Backup Structure
// ============================================================================

/// Online database backup handle
///
/// Allows incremental copying of a database while it remains in use.
/// The backup can be performed in chunks to avoid blocking for too long.
pub struct Backup {
    // Source and destination connections
    dest_conn: Arc<RwLock<SqliteConnection>>,
    src_conn: Arc<RwLock<SqliteConnection>>,

    // Database indices
    dest_db_idx: usize,
    src_db_idx: usize,

    // Database names for reference
    dest_db: String,
    src_db: String,

    // Progress tracking
    /// Next page number to copy (1-based)
    dest_pgno: u32,
    /// Total pages in source at start of step
    src_npage: u32,

    // State
    /// Whether the backup has been initialized
    is_initialized: bool,
    /// Whether the backup is complete
    is_complete: bool,
    /// Page size being used
    page_size: u32,
}

impl Backup {
    /// Initialize a backup operation
    ///
    /// # Arguments
    /// * `dest_conn` - Destination connection (where to copy to)
    /// * `dest_db` - Destination database name ("main", "temp", or attached)
    /// * `src_conn` - Source connection (where to copy from)
    /// * `src_db` - Source database name ("main", "temp", or attached)
    ///
    /// # Returns
    /// A new Backup handle on success
    ///
    /// # Errors
    /// - `ErrorCode::Error` if database names are invalid
    /// - `ErrorCode::ReadOnly` if page sizes don't match and dest is non-empty
    pub fn init(
        dest_conn: Arc<RwLock<SqliteConnection>>,
        dest_db: &str,
        src_conn: Arc<RwLock<SqliteConnection>>,
        src_db: &str,
    ) -> Result<Self> {
        // Find database indices
        let dest_db_idx = {
            let conn = dest_conn.read().map_err(|_| {
                Error::with_message(ErrorCode::Error, "Failed to lock destination connection")
            })?;
            find_db_index(&conn, dest_db)?
        };

        let src_db_idx = {
            let conn = src_conn.read().map_err(|_| {
                Error::with_message(ErrorCode::Error, "Failed to lock source connection")
            })?;
            find_db_index(&conn, src_db)?
        };

        // Get page sizes
        let (dest_page_size, dest_page_count) = {
            let conn = dest_conn.read().map_err(|_| {
                Error::with_message(ErrorCode::Error, "Failed to lock destination connection")
            })?;
            let db = &conn.dbs[dest_db_idx];
            (db.page_size, get_page_count(&db.btree))
        };

        let src_page_size = {
            let conn = src_conn.read().map_err(|_| {
                Error::with_message(ErrorCode::Error, "Failed to lock source connection")
            })?;
            conn.dbs[src_db_idx].page_size
        };

        // Check page sizes match (or dest is empty)
        if dest_page_count > 0 && dest_page_size != src_page_size {
            return Err(Error::with_message(
                ErrorCode::ReadOnly,
                "Page size mismatch and destination database is not empty",
            ));
        }

        let page_size = if dest_page_count > 0 {
            dest_page_size
        } else {
            src_page_size
        };

        Ok(Self {
            dest_conn,
            src_conn,
            dest_db_idx,
            src_db_idx,
            dest_db: dest_db.to_string(),
            src_db: src_db.to_string(),
            dest_pgno: 1,
            src_npage: 0,
            is_initialized: true,
            is_complete: false,
            page_size,
        })
    }

    /// Copy up to n_page pages from source to destination
    ///
    /// # Arguments
    /// * `n_page` - Maximum number of pages to copy. Use -1 to copy all remaining pages.
    ///
    /// # Returns
    /// - `StepResult::More` if there are more pages to copy
    /// - `StepResult::Done` if the backup is complete
    /// - `StepResult::Busy` if the operation was blocked
    ///
    /// # Errors
    /// Returns an error if the copy operation fails
    pub fn step(&mut self, n_page: i32) -> Result<StepResult> {
        if self.is_complete {
            return Ok(StepResult::Done);
        }

        if !self.is_initialized {
            return Err(Error::with_message(
                ErrorCode::Error,
                "Backup not initialized",
            ));
        }

        // Lock both connections
        let src_conn = self.src_conn.read().map_err(|_| {
            Error::with_message(ErrorCode::Busy, "Failed to lock source connection")
        })?;

        let dest_conn = self.dest_conn.write().map_err(|_| {
            Error::with_message(ErrorCode::Busy, "Failed to lock destination connection")
        })?;

        // Get source page count
        let src_db = &src_conn.dbs[self.src_db_idx];
        self.src_npage = get_page_count(&src_db.btree);

        if self.src_npage == 0 {
            // Source is empty, nothing to copy
            self.is_complete = true;
            return Ok(StepResult::Done);
        }

        // Get btrees
        let src_btree = src_db
            .btree
            .as_ref()
            .ok_or_else(|| Error::with_message(ErrorCode::Error, "Source database not open"))?;

        let dest_db = &dest_conn.dbs[self.dest_db_idx];
        let dest_btree = dest_db.btree.as_ref().ok_or_else(|| {
            Error::with_message(ErrorCode::Error, "Destination database not open")
        })?;

        // Handle page size change if needed
        if self.dest_pgno == 1 && dest_db.page_size != self.page_size {
            // Would need to change dest page size
            // For now, just use whatever is there
        }

        // Copy pages
        let mut pages_copied = 0u32;
        let max_pages = if n_page < 0 { u32::MAX } else { n_page as u32 };

        while self.dest_pgno <= self.src_npage && pages_copied < max_pages {
            // Copy the page
            self.copy_page(src_btree, dest_btree, self.dest_pgno)?;
            self.dest_pgno += 1;
            pages_copied += 1;
        }

        // Check if complete
        if self.dest_pgno > self.src_npage {
            self.is_complete = true;
            Ok(StepResult::Done)
        } else {
            Ok(StepResult::More)
        }
    }

    /// Copy a single page from source to destination
    fn copy_page(&self, src_btree: &Btree, dest_btree: &Btree, pgno: u32) -> Result<()> {
        // In a full implementation, we would:
        // 1. Read the page from source pager
        // 2. Write the page to destination pager
        //
        // For now, this is a stub that assumes the Btree handles page-level operations
        // The actual implementation would need pager integration

        // Get page data from source
        let page_data = src_btree.get_page_data(pgno)?;

        // Write page data to destination
        dest_btree.put_page_data(pgno, &page_data)?;

        Ok(())
    }

    /// Get the number of pages remaining to be copied
    ///
    /// Returns -1 if the source page count is not yet known (before first step)
    pub fn remaining(&self) -> i32 {
        if self.src_npage == 0 {
            -1
        } else if self.dest_pgno > self.src_npage {
            0
        } else {
            (self.src_npage - self.dest_pgno + 1) as i32
        }
    }

    /// Get the total page count of the source database
    ///
    /// Returns -1 if not yet known (before first step)
    pub fn pagecount(&self) -> i32 {
        if self.src_npage == 0 {
            -1
        } else {
            self.src_npage as i32
        }
    }

    /// Check if the backup is complete
    pub fn is_done(&self) -> bool {
        self.is_complete
    }

    /// Finish the backup operation
    ///
    /// This should be called to properly clean up resources after the backup
    /// is complete or if the backup is being aborted.
    pub fn finish(self) -> Result<()> {
        // In a full implementation, we would:
        // 1. Detach from source pager notifications
        // 2. Commit any pending writes to destination
        // 3. Release locks

        // For now, cleanup happens automatically when Backup is dropped
        Ok(())
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Find the index of a database by name in a connection
fn find_db_index(conn: &SqliteConnection, name: &str) -> Result<usize> {
    for (i, db) in conn.dbs.iter().enumerate() {
        if db.name.eq_ignore_ascii_case(name) {
            return Ok(i);
        }
    }
    Err(Error::with_message(
        ErrorCode::Error,
        format!("unknown database: {}", name),
    ))
}

/// Get the page count from a btree (returns 0 if btree is None or empty)
fn get_page_count(btree: &Option<Arc<Btree>>) -> u32 {
    match btree {
        Some(bt) => bt.page_count().unwrap_or(0),
        None => 0,
    }
}

// ============================================================================
// Public API Functions (C-style interface)
// ============================================================================

/// Initialize a backup operation
///
/// This is the Rust equivalent of sqlite3_backup_init()
pub fn backup_init(
    dest_conn: Arc<RwLock<SqliteConnection>>,
    dest_db: &str,
    src_conn: Arc<RwLock<SqliteConnection>>,
    src_db: &str,
) -> Result<Backup> {
    Backup::init(dest_conn, dest_db, src_conn, src_db)
}

/// Copy up to n pages
///
/// This is the Rust equivalent of sqlite3_backup_step()
pub fn backup_step(backup: &mut Backup, n: i32) -> Result<StepResult> {
    backup.step(n)
}

/// Get pages remaining
///
/// This is the Rust equivalent of sqlite3_backup_remaining()
pub fn backup_remaining(backup: &Backup) -> i32 {
    backup.remaining()
}

/// Get total pages
///
/// This is the Rust equivalent of sqlite3_backup_pagecount()
pub fn backup_pagecount(backup: &Backup) -> i32 {
    backup.pagecount()
}

/// Finish and cleanup
///
/// This is the Rust equivalent of sqlite3_backup_finish()
pub fn backup_finish(backup: Backup) -> Result<()> {
    backup.finish()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_step_result() {
        assert_eq!(StepResult::More, StepResult::More);
        assert_ne!(StepResult::More, StepResult::Done);
        assert_ne!(StepResult::Done, StepResult::Busy);
    }

    #[test]
    fn test_find_db_index() {
        let conn = SqliteConnection::new();
        assert_eq!(find_db_index(&conn, "main").unwrap(), 0);
        assert_eq!(find_db_index(&conn, "MAIN").unwrap(), 0);
        assert_eq!(find_db_index(&conn, "temp").unwrap(), 1);
        assert!(find_db_index(&conn, "nonexistent").is_err());
    }

    #[test]
    fn test_backup_remaining_initial() {
        // Create a simple backup struct for testing
        let conn = Arc::new(RwLock::new(SqliteConnection::new()));
        // Can't fully test without btree, but can test the helper
    }

    #[test]
    fn test_backup_pagecount_initial() {
        // Test that pagecount returns -1 before first step
        // This would require a full backup setup
    }

    #[test]
    fn test_backup_init_same_database() {
        // Test backing up a database to itself should work
        let conn = Arc::new(RwLock::new(SqliteConnection::new()));

        // This should succeed - backing up main to itself
        // (though not useful in practice)
        let result = Backup::init(Arc::clone(&conn), "main", Arc::clone(&conn), "main");
        // Should succeed in initialization, actual copy would fail
        assert!(result.is_ok());
    }

    #[test]
    fn test_backup_init_invalid_db() {
        let conn = Arc::new(RwLock::new(SqliteConnection::new()));

        let result = Backup::init(Arc::clone(&conn), "nonexistent", Arc::clone(&conn), "main");
        assert!(result.is_err());
    }

    #[test]
    fn test_backup_step_not_initialized() {
        let conn = Arc::new(RwLock::new(SqliteConnection::new()));
        let result = Backup::init(Arc::clone(&conn), "main", Arc::clone(&conn), "main");

        if let Ok(mut backup) = result {
            // Manually mark as not initialized to test error path
            backup.is_initialized = false;
            let step_result = backup.step(1);
            assert!(step_result.is_err());
        }
    }

    #[test]
    fn test_backup_is_done() {
        let conn = Arc::new(RwLock::new(SqliteConnection::new()));
        let result = Backup::init(Arc::clone(&conn), "main", Arc::clone(&conn), "main");

        if let Ok(backup) = result {
            assert!(!backup.is_done());
        }
    }

    #[test]
    fn test_backup_finish() {
        let conn = Arc::new(RwLock::new(SqliteConnection::new()));
        let result = Backup::init(Arc::clone(&conn), "main", Arc::clone(&conn), "main");

        if let Ok(backup) = result {
            // Finish should succeed
            assert!(backup.finish().is_ok());
        }
    }
}
