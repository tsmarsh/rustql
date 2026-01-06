//! Incremental BLOB I/O API
//!
//! This module implements SQLite's incremental BLOB I/O functions which allow
//! reading and writing portions of BLOB values without loading the entire
//! BLOB into memory. This is useful for large BLOBs.
//!
//! Translation of sqlite3/src/vdbeblob.c

use crate::error::{Error, ErrorCode, Result};

/// Blob handle for incremental I/O
///
/// This corresponds to SQLite's `sqlite3_blob` / `Incrblob` structure.
pub struct Blob {
    /// Database name (e.g., "main", "temp")
    db_name: String,
    /// Table name
    table: String,
    /// Column name
    column: String,
    /// Current rowid
    rowid: i64,
    /// Whether blob is writable
    writable: bool,
    /// Size of blob in bytes
    size: i32,
    /// The actual blob data (cached)
    data: Vec<u8>,
    /// Whether the handle has been invalidated
    invalidated: bool,
    /// Column index in table
    col_idx: i32,
}

impl Blob {
    /// Create a new blob handle (internal use)
    fn new(
        db_name: String,
        table: String,
        column: String,
        rowid: i64,
        writable: bool,
        col_idx: i32,
        data: Vec<u8>,
    ) -> Self {
        let size = data.len() as i32;
        Self {
            db_name,
            table,
            column,
            rowid,
            writable,
            size,
            data,
            invalidated: false,
            col_idx,
        }
    }

    /// Check if this blob handle is still valid
    pub fn is_valid(&self) -> bool {
        !self.invalidated
    }

    /// Invalidate this blob handle
    fn invalidate(&mut self) {
        self.invalidated = true;
    }
}

// ============================================================================
// Public API Functions
// ============================================================================

/// Open a BLOB for incremental I/O
///
/// Opens a handle to the BLOB located in row `rowid`, column `column`,
/// table `table` in database `db_name`.
///
/// # Arguments
/// * `db_name` - Database name ("main", "temp", or attached db name)
/// * `table` - Table name
/// * `column` - Column name
/// * `rowid` - Row ID containing the BLOB
/// * `flags` - 0 for read-only, 1 for read-write
///
/// # Errors
/// * `ErrorCode::NotFound` - Row doesn't exist
/// * `ErrorCode::Error` - Column is not a BLOB or TEXT type
///
/// # SQLite Equivalent
/// `sqlite3_blob_open()`
pub fn sqlite3_blob_open(
    db_name: &str,
    table: &str,
    column: &str,
    rowid: i64,
    flags: i32,
) -> Result<Blob> {
    let writable = flags != 0;

    // This is a stub implementation - in a real implementation we would:
    // 1. Look up the database by name
    // 2. Find the table in the schema
    // 3. Find the column in the table
    // 4. Open a cursor on the table
    // 5. Seek to the rowid
    // 6. Extract the blob data and offset

    // For now, create a placeholder blob
    // In practice, this would need a Connection reference to query the actual data
    Err(Error::with_message(
        ErrorCode::Error,
        "blob_open requires database connection - use Connection::blob_open() instead",
    ))
}

/// Close a BLOB handle
///
/// Closes a previously opened BLOB handle, releasing any resources.
///
/// # SQLite Equivalent
/// `sqlite3_blob_close()`
pub fn sqlite3_blob_close(blob: Blob) -> Result<()> {
    // Resources released when blob is dropped
    drop(blob);
    Ok(())
}

/// Read data from a BLOB handle
///
/// Reads `buf.len()` bytes from the BLOB starting at offset `offset`.
///
/// # Arguments
/// * `blob` - BLOB handle
/// * `buf` - Buffer to read into
/// * `offset` - Byte offset to start reading from
///
/// # Errors
/// * `ErrorCode::Error` - Offset/size out of range
/// * `ErrorCode::Abort` - BLOB handle has been invalidated
///
/// # SQLite Equivalent
/// `sqlite3_blob_read()`
pub fn sqlite3_blob_read(blob: &Blob, buf: &mut [u8], offset: i32) -> Result<()> {
    // Check if handle is valid
    if blob.invalidated {
        return Err(Error::new(ErrorCode::Abort));
    }

    // Validate bounds
    let n = buf.len() as i32;
    if offset < 0 || n < 0 || offset + n > blob.size {
        return Err(Error::with_message(
            ErrorCode::Error,
            "blob read out of range",
        ));
    }

    // Copy data to buffer
    let start = offset as usize;
    let end = start + n as usize;
    buf.copy_from_slice(&blob.data[start..end]);

    Ok(())
}

/// Write data to a BLOB handle
///
/// Writes `data.len()` bytes to the BLOB starting at offset `offset`.
/// The size of the BLOB cannot be changed using this function.
///
/// # Arguments
/// * `blob` - BLOB handle (must be opened for writing)
/// * `data` - Data to write
/// * `offset` - Byte offset to start writing at
///
/// # Errors
/// * `ErrorCode::ReadOnly` - BLOB was not opened for writing
/// * `ErrorCode::Error` - Offset/size out of range
/// * `ErrorCode::Abort` - BLOB handle has been invalidated
///
/// # SQLite Equivalent
/// `sqlite3_blob_write()`
pub fn sqlite3_blob_write(blob: &mut Blob, data: &[u8], offset: i32) -> Result<()> {
    // Check if handle is valid
    if blob.invalidated {
        return Err(Error::new(ErrorCode::Abort));
    }

    // Check writable
    if !blob.writable {
        return Err(Error::with_message(
            ErrorCode::ReadOnly,
            "blob was opened for reading only",
        ));
    }

    // Validate bounds
    let n = data.len() as i32;
    if offset < 0 || n < 0 || offset + n > blob.size {
        return Err(Error::with_message(
            ErrorCode::Error,
            "blob write out of range",
        ));
    }

    // Write data
    let start = offset as usize;
    let end = start + n as usize;
    blob.data[start..end].copy_from_slice(data);

    // Note: In the full implementation, this would write through to the
    // underlying btree cursor using BtreePutData

    Ok(())
}

/// Get the size of a BLOB in bytes
///
/// # SQLite Equivalent
/// `sqlite3_blob_bytes()`
pub fn sqlite3_blob_bytes(blob: &Blob) -> i32 {
    if blob.invalidated {
        0
    } else {
        blob.size
    }
}

/// Move a BLOB handle to a different row
///
/// Repositions an existing BLOB handle to point to a different row in the
/// same table. This is faster than closing and reopening the handle.
///
/// # Arguments
/// * `blob` - BLOB handle
/// * `rowid` - New row ID
///
/// # Errors
/// * `ErrorCode::NotFound` - Row doesn't exist
/// * `ErrorCode::Error` - Column in new row is not BLOB/TEXT
/// * `ErrorCode::Abort` - BLOB handle has been invalidated
///
/// # SQLite Equivalent
/// `sqlite3_blob_reopen()`
pub fn sqlite3_blob_reopen(blob: &mut Blob, rowid: i64) -> Result<()> {
    // Check if handle is valid
    if blob.invalidated {
        return Err(Error::new(ErrorCode::Abort));
    }

    // In the full implementation, this would:
    // 1. Seek the cursor to the new rowid
    // 2. Verify the row exists and column is BLOB/TEXT
    // 3. Update size and offset

    // For now, just update the rowid (stub)
    blob.rowid = rowid;

    // In practice, would need to re-fetch the data
    Err(Error::with_message(
        ErrorCode::Error,
        "blob_reopen requires database connection - use Connection::blob_reopen() instead",
    ))
}

// ============================================================================
// Helper Functions
// ============================================================================

impl Blob {
    /// Get the database name this blob belongs to
    pub fn database_name(&self) -> &str {
        &self.db_name
    }

    /// Get the table name this blob belongs to
    pub fn table_name(&self) -> &str {
        &self.table
    }

    /// Get the column name this blob belongs to
    pub fn column_name(&self) -> &str {
        &self.column
    }

    /// Get the current rowid
    pub fn rowid(&self) -> i64 {
        self.rowid
    }

    /// Check if this blob is writable
    pub fn is_writable(&self) -> bool {
        self.writable
    }

    /// Get the blob size in bytes
    pub fn size(&self) -> i32 {
        self.size
    }

    /// Read the entire blob contents
    pub fn read_all(&self) -> Result<Vec<u8>> {
        if self.invalidated {
            return Err(Error::new(ErrorCode::Abort));
        }
        Ok(self.data.clone())
    }

    /// Create a blob handle from existing data (for testing/internal use)
    pub fn from_data(
        db_name: &str,
        table: &str,
        column: &str,
        rowid: i64,
        writable: bool,
        data: Vec<u8>,
    ) -> Self {
        Self::new(
            db_name.to_string(),
            table.to_string(),
            column.to_string(),
            rowid,
            writable,
            0,
            data,
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_blob(writable: bool) -> Blob {
        Blob::from_data(
            "main",
            "test_table",
            "data",
            1,
            writable,
            vec![0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99],
        )
    }

    #[test]
    fn test_blob_bytes() {
        let blob = create_test_blob(false);
        assert_eq!(sqlite3_blob_bytes(&blob), 10);
    }

    #[test]
    fn test_blob_read() {
        let blob = create_test_blob(false);

        // Read first 4 bytes
        let mut buf = [0u8; 4];
        sqlite3_blob_read(&blob, &mut buf, 0).unwrap();
        assert_eq!(buf, [0x00, 0x11, 0x22, 0x33]);

        // Read middle 3 bytes
        let mut buf = [0u8; 3];
        sqlite3_blob_read(&blob, &mut buf, 4).unwrap();
        assert_eq!(buf, [0x44, 0x55, 0x66]);

        // Read last 2 bytes
        let mut buf = [0u8; 2];
        sqlite3_blob_read(&blob, &mut buf, 8).unwrap();
        assert_eq!(buf, [0x88, 0x99]);
    }

    #[test]
    fn test_blob_read_out_of_bounds() {
        let blob = create_test_blob(false);

        // Negative offset
        let mut buf = [0u8; 4];
        assert!(sqlite3_blob_read(&blob, &mut buf, -1).is_err());

        // Offset + length > size
        let mut buf = [0u8; 5];
        assert!(sqlite3_blob_read(&blob, &mut buf, 8).is_err());
    }

    #[test]
    fn test_blob_write() {
        let mut blob = create_test_blob(true);

        // Write to middle
        sqlite3_blob_write(&mut blob, &[0xAA, 0xBB], 4).unwrap();

        // Verify write
        let mut buf = [0u8; 4];
        sqlite3_blob_read(&blob, &mut buf, 3).unwrap();
        assert_eq!(buf, [0x33, 0xAA, 0xBB, 0x66]);
    }

    #[test]
    fn test_blob_write_readonly() {
        let mut blob = create_test_blob(false);

        // Should fail - blob is read-only
        assert!(sqlite3_blob_write(&mut blob, &[0xAA], 0).is_err());
    }

    #[test]
    fn test_blob_write_out_of_bounds() {
        let mut blob = create_test_blob(true);

        // Offset + length > size
        assert!(sqlite3_blob_write(&mut blob, &[0xAA; 5], 8).is_err());
    }

    #[test]
    fn test_blob_close() {
        let blob = create_test_blob(false);
        assert!(sqlite3_blob_close(blob).is_ok());
    }

    #[test]
    fn test_blob_properties() {
        let blob = create_test_blob(false);
        assert_eq!(blob.database_name(), "main");
        assert_eq!(blob.table_name(), "test_table");
        assert_eq!(blob.column_name(), "data");
        assert_eq!(blob.rowid(), 1);
        assert!(!blob.is_writable());
        assert_eq!(blob.size(), 10);
    }

    #[test]
    fn test_blob_read_all() {
        let blob = create_test_blob(false);
        let data = blob.read_all().unwrap();
        assert_eq!(
            data,
            vec![0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99]
        );
    }

    #[test]
    fn test_blob_invalidated() {
        let mut blob = create_test_blob(false);
        blob.invalidate();

        assert!(!blob.is_valid());
        assert_eq!(sqlite3_blob_bytes(&blob), 0);

        let mut buf = [0u8; 4];
        let result = sqlite3_blob_read(&blob, &mut buf, 0);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, ErrorCode::Abort);
    }
}
