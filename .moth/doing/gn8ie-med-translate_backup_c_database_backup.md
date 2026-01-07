# Translate backup.c - Database Backup

## Overview
Translate the online backup API which allows copying a database while it's in use. This enables hot backups without blocking other operations.

## Source Reference
- `sqlite3/src/backup.c` - 767 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Backup
Main backup object:
```rust
pub struct Backup {
    // Source and destination
    dest_conn: Arc<Connection>,     // Destination connection
    dest_db: String,                // Destination database name
    src_conn: Arc<Connection>,      // Source connection
    src_db: String,                 // Source database name

    // Pagers
    dest_pager: Arc<Pager>,
    src_pager: Arc<Pager>,

    // Progress tracking
    dest_pgno: Pgno,                // Next page to copy
    src_npage: Pgno,                // Total pages in source
    remaining: i32,                 // Pages remaining
    pagecount: i32,                 // Total pages

    // State
    is_attached: bool,              // Backup registered with pager

    // Schema tracking
    dest_schema: u32,               // Destination schema cookie
}
```

## Key Functions

### Initialization
```rust
impl Backup {
    /// Initialize a backup operation
    /// sqlite3_backup_init()
    pub fn init(
        dest_conn: Arc<Connection>,
        dest_db: &str,
        src_conn: Arc<Connection>,
        src_db: &str,
    ) -> Result<Backup> {
        // Validate database names
        let dest_idx = dest_conn.find_db_index(dest_db)?;
        let src_idx = src_conn.find_db_index(src_db)?;

        // Get pagers
        let dest_pager = dest_conn.get_pager(dest_idx)?;
        let src_pager = src_conn.get_pager(src_idx)?;

        // Check page sizes match (or dest is empty)
        let dest_page_size = dest_pager.page_size();
        let src_page_size = src_pager.page_size();
        if dest_pager.page_count()? > 0 && dest_page_size != src_page_size {
            return Err(Error::new(ErrorCode::ReadOnly));
        }

        Ok(Backup {
            dest_conn,
            dest_db: dest_db.to_string(),
            src_conn,
            src_db: src_db.to_string(),
            dest_pager,
            src_pager,
            dest_pgno: 1,
            src_npage: 0,
            remaining: -1,
            pagecount: -1,
            is_attached: false,
            dest_schema: 0,
        })
    }
}
```

### Step Operation
```rust
impl Backup {
    /// Copy up to n_page pages
    /// sqlite3_backup_step()
    pub fn step(&mut self, n_page: i32) -> Result<StepResult> {
        // Lock both databases
        let _src_guard = BtreeGuard::new(&self.src_conn.main_btree());
        let _dest_guard = BtreeGuard::new(&self.dest_conn.main_btree());

        // Get source page count
        self.src_npage = self.src_pager.page_count()?;

        // Handle page size change if needed
        if self.dest_pgno == 1 {
            self.set_dest_page_size()?;
        }

        // Copy pages
        let mut pages_copied = 0;
        while self.dest_pgno <= self.src_npage {
            if n_page >= 0 && pages_copied >= n_page {
                break;
            }

            self.copy_page(self.dest_pgno)?;
            self.dest_pgno += 1;
            pages_copied += 1;
        }

        // Check if complete
        if self.dest_pgno > self.src_npage {
            // Truncate destination if source shrank
            self.dest_pager.truncate(self.src_npage)?;
            Ok(StepResult::Done)
        } else {
            Ok(StepResult::More)
        }
    }

    /// Copy a single page
    fn copy_page(&mut self, pgno: Pgno) -> Result<()> {
        // Read from source
        let src_page = self.src_pager.get(pgno)?;

        // Write to destination
        let dest_page = self.dest_pager.get(pgno)?;
        self.dest_pager.write(&dest_page)?;

        // Copy data
        dest_page.data.copy_from_slice(&src_page.data);

        // Release pages
        self.src_pager.unref(src_page);
        self.dest_pager.unref(dest_page);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepResult {
    More,   // More pages to copy
    Done,   // Backup complete
}
```

### Progress Tracking
```rust
impl Backup {
    /// Get remaining page count
    /// sqlite3_backup_remaining()
    pub fn remaining(&self) -> i32 {
        if self.src_npage == 0 {
            -1
        } else {
            (self.src_npage - self.dest_pgno + 1) as i32
        }
    }

    /// Get total page count
    /// sqlite3_backup_pagecount()
    pub fn pagecount(&self) -> i32 {
        self.src_npage as i32
    }
}
```

### Cleanup
```rust
impl Backup {
    /// Finish backup operation
    /// sqlite3_backup_finish()
    pub fn finish(mut self) -> Result<()> {
        // Detach from pagers
        if self.is_attached {
            self.src_pager.detach_backup(&self);
            self.dest_pager.detach_backup(&self);
        }

        Ok(())
    }
}

impl Drop for Backup {
    fn drop(&mut self) {
        // Ensure cleanup happens even if finish() not called
        if self.is_attached {
            let _ = self.src_pager.detach_backup(self);
            let _ = self.dest_pager.detach_backup(self);
        }
    }
}
```

## Handling Concurrent Modifications

The backup must handle cases where the source database changes during backup:

```rust
impl Backup {
    /// Called by pager when source page is modified
    pub fn on_page_modified(&mut self, pgno: Pgno) {
        // If we've already copied this page, need to re-copy it
        if pgno < self.dest_pgno {
            // Mark for re-copy by resetting dest_pgno
            self.dest_pgno = pgno;
        }
    }

    /// Check if source schema has changed
    fn check_schema(&self) -> Result<()> {
        let current = self.src_pager.schema_cookie()?;
        if self.dest_schema != 0 && self.dest_schema != current {
            // Schema changed, may need to restart
            return Err(Error::new(ErrorCode::Schema));
        }
        Ok(())
    }
}
```

## Public API

```rust
/// Create a backup of a database
pub fn backup_init(
    dest: &Connection,
    dest_name: &str,
    src: &Connection,
    src_name: &str,
) -> Result<Backup>;

/// Copy up to n pages (n < 0 means all)
pub fn backup_step(backup: &mut Backup, n: i32) -> Result<StepResult>;

/// Get pages remaining
pub fn backup_remaining(backup: &Backup) -> i32;

/// Get total pages
pub fn backup_pagecount(backup: &Backup) -> i32;

/// Finish and cleanup
pub fn backup_finish(backup: Backup) -> Result<()>;
```

## Usage Example

```rust
// Backup main database to a file
let backup = backup_init(&dest_conn, "main", &src_conn, "main")?;

loop {
    match backup_step(&mut backup, 100)? {
        StepResult::More => {
            println!("Progress: {}/{}",
                backup_pagecount(&backup) - backup_remaining(&backup),
                backup_pagecount(&backup));
        }
        StepResult::Done => break,
    }

    // Could sleep here to reduce load
}

backup_finish(backup)?;
```

## Acceptance Criteria
- [ ] Backup struct with source/destination tracking
- [ ] init() to set up backup operation
- [ ] step() to copy pages incrementally
- [ ] remaining()/pagecount() for progress
- [ ] finish() for cleanup
- [ ] Handle concurrent source modifications
- [ ] Handle page size differences
- [ ] Proper locking during operations

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `backup.test` - Core backup functionality
- `backup2.test` - Additional backup scenarios
- `backup4.test` - Backup edge cases
- `backup5.test` - Backup with concurrent access
- `backup_ioerr.test` - Backup I/O error handling
- `backup_malloc.test` - Backup memory allocation failures
