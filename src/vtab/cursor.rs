//! Virtual table cursor trait
//!
//! Defines the interface for iterating over virtual table rows.

use crate::error::Result;
use crate::vdbe::mem::Mem;

use super::ConstraintValue;

/// Cursor for iterating over virtual table rows
///
/// This trait mirrors SQLite's virtual table cursor methods:
/// - xFilter: Initialize/reset the cursor with constraints
/// - xNext: Advance to the next row
/// - xEof: Check if at end of results
/// - xColumn: Get column value at current position
/// - xRowid: Get rowid at current position
///
/// # Lifecycle
///
/// 1. Cursor is created by `VtabTable::open_cursor()`
/// 2. `filter()` is called to initialize with constraints
/// 3. While `!eof()`:
///    - `rowid()` and `column()` retrieve current row data
///    - `next()` advances to the next row
/// 4. Cursor is dropped when iteration is complete
///
/// # Example
///
/// ```ignore
/// let mut cursor = table.open_cursor(idx_num, idx_str, &constraints)?;
/// cursor.filter(idx_num, idx_str, &constraints)?;
///
/// while !cursor.eof() {
///     let rowid = cursor.rowid()?;
///     let col0 = cursor.column(0)?;
///     println!("Row {}: {:?}", rowid, col0);
///     cursor.next()?;
/// }
/// ```
pub trait VtabCursor: Send + Sync {
    /// Initialize or reset the cursor with filter constraints
    ///
    /// This is called at the start of a query to position the cursor
    /// at the first matching row.
    ///
    /// # Arguments
    ///
    /// * `index_num` - The index number chosen by xBestIndex
    /// * `index_str` - The index string chosen by xBestIndex
    /// * `constraints` - Constraint values, in order specified by xBestIndex
    fn filter(
        &mut self,
        index_num: i32,
        index_str: Option<&str>,
        constraints: &[ConstraintValue],
    ) -> Result<()>;

    /// Advance to the next row
    ///
    /// After this call, either `eof()` returns true, or `rowid()`/`column()`
    /// will return data for the next row.
    fn next(&mut self) -> Result<()>;

    /// Check if at end of results
    ///
    /// Returns true if there are no more rows to iterate over.
    fn eof(&self) -> bool;

    /// Get current row's rowid
    ///
    /// Returns the rowid of the current row. This is typically a unique
    /// integer identifier for the row.
    fn rowid(&self) -> Result<i64>;

    /// Get column value at current position
    ///
    /// # Arguments
    ///
    /// * `col_idx` - Zero-based column index
    ///
    /// # Returns
    ///
    /// The value of the specified column in the current row.
    fn column(&self, col_idx: usize) -> Result<Mem>;

    /// Save cursor position for savepoint
    ///
    /// This is called when a savepoint is created. The cursor should
    /// remember its current position so it can be restored if the
    /// savepoint is rolled back.
    fn savepoint(&mut self, _point: i32) -> Result<()> {
        Ok(())
    }

    /// Release savepoint
    ///
    /// Called when a savepoint is released (committed).
    fn release(&mut self, _point: i32) -> Result<()> {
        Ok(())
    }

    /// Rollback to savepoint
    ///
    /// Called when a savepoint is rolled back. The cursor should
    /// restore its position to what it was when `savepoint()` was called.
    fn rollback_to(&mut self, _point: i32) -> Result<()> {
        Ok(())
    }
}

/// A simple in-memory cursor that iterates over a fixed set of rows
///
/// This is useful for virtual tables that compute their entire result
/// set upfront (like sqlite_master or simple table-valued functions).
pub struct MemoryCursor {
    /// All rows to iterate over
    rows: Vec<(i64, Vec<Mem>)>,

    /// Current position (index into rows)
    position: usize,
}

impl MemoryCursor {
    /// Create a new memory cursor with the given rows
    ///
    /// Each row is a tuple of (rowid, column_values).
    pub fn new(rows: Vec<(i64, Vec<Mem>)>) -> Self {
        Self { rows, position: 0 }
    }

    /// Create an empty cursor
    pub fn empty() -> Self {
        Self::new(vec![])
    }

    /// Get the number of rows
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl VtabCursor for MemoryCursor {
    fn filter(
        &mut self,
        _index_num: i32,
        _index_str: Option<&str>,
        _constraints: &[ConstraintValue],
    ) -> Result<()> {
        // Reset to beginning
        self.position = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.position += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.position >= self.rows.len()
    }

    fn rowid(&self) -> Result<i64> {
        if self.position < self.rows.len() {
            Ok(self.rows[self.position].0)
        } else {
            Ok(0)
        }
    }

    fn column(&self, col_idx: usize) -> Result<Mem> {
        if self.position < self.rows.len() {
            let row = &self.rows[self.position].1;
            if col_idx < row.len() {
                Ok(row[col_idx].clone())
            } else {
                Ok(Mem::new())
            }
        } else {
            Ok(Mem::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_cursor() {
        let rows = vec![
            (1, vec![Mem::from_int(100), Mem::from_str("hello")]),
            (2, vec![Mem::from_int(200), Mem::from_str("world")]),
            (3, vec![Mem::from_int(300), Mem::from_str("test")]),
        ];

        let mut cursor = MemoryCursor::new(rows);

        // Filter resets position
        cursor.filter(0, None, &[]).unwrap();
        assert!(!cursor.eof());

        // First row
        assert_eq!(cursor.rowid().unwrap(), 1);
        assert_eq!(cursor.column(0).unwrap().to_int(), 100);
        assert_eq!(cursor.column(1).unwrap().to_str(), "hello");

        // Advance
        cursor.next().unwrap();
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid().unwrap(), 2);

        // Advance to third
        cursor.next().unwrap();
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid().unwrap(), 3);

        // Advance past end
        cursor.next().unwrap();
        assert!(cursor.eof());
    }

    #[test]
    fn test_empty_cursor() {
        let mut cursor = MemoryCursor::empty();
        cursor.filter(0, None, &[]).unwrap();
        assert!(cursor.eof());
    }
}
