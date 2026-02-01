//! Virtual table module and table traits
//!
//! This module defines the core traits for virtual table modules:
//! - `VtabModule`: Factory trait for creating virtual table instances
//! - `VtabTable`: Trait for a single virtual table instance
//! - `DbContext`: Database access context passed to modules during creation

use std::sync::Arc;

use crate::error::{Error, ErrorCode, Result};
use crate::schema::Affinity;
use crate::types::Value;
use crate::vdbe::mem::Mem;

use super::cursor::VtabCursor;
use super::index_info::IndexInfo;

// ============================================================================
// Database Context for Virtual Table Module Access
// ============================================================================

/// Database context passed to virtual table modules during creation
///
/// This trait allows modules to query the database during xCreate/xConnect
/// without needing to access global state. The implementation is provided
/// by the VDBE engine when executing VCreate opcodes.
pub trait DbContext: Send + Sync {
    /// Execute a query and return the result rows
    ///
    /// Each row is a Vec of Values for the columns.
    fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>>;

    /// Execute a statement that doesn't return rows (INSERT, UPDATE, DELETE)
    fn exec(&self, sql: &str) -> Result<()>;
}

/// A virtual table module (mirrors sqlite3_module)
///
/// This is a factory for creating virtual table instances. Each registered
/// module can create multiple table instances with different configurations.
///
/// # SQLite Correspondence
///
/// - `xCreate`: Called for `CREATE VIRTUAL TABLE` (creates storage)
/// - `xConnect`: Called when accessing existing table (connects to storage)
/// - `xDestroy`: Called for `DROP TABLE` (destroys storage)
///
/// # Example
///
/// ```ignore
/// struct MyModule;
///
/// impl VtabModule for MyModule {
///     fn name(&self) -> &str {
///         "mymodule"
///     }
///
///     fn create(
///         &self,
///         db_name: &str,
///         table_name: &str,
///         args: &[String],
///     ) -> Result<Arc<dyn VtabTable>> {
///         Ok(Arc::new(MyTable::new(table_name, args)?))
///     }
///
///     fn connect(
///         &self,
///         db_name: &str,
///         table_name: &str,
///         args: &[String],
///     ) -> Result<Arc<dyn VtabTable>> {
///         // For most modules, create and connect are the same
///         self.create(db_name, table_name, args)
///     }
/// }
/// ```
pub trait VtabModule: Send + Sync {
    /// Module name (e.g., "fts3", "fts5", "rtree")
    fn name(&self) -> &str;

    /// Module version (for API compatibility)
    fn version(&self) -> i32 {
        1
    }

    /// Create a new virtual table (xCreate)
    ///
    /// This is called for `CREATE VIRTUAL TABLE` statements. The module
    /// should create any necessary storage/shadow tables.
    ///
    /// # Arguments
    ///
    /// * `db_name` - Database name (usually "main")
    /// * `table_name` - Name of the virtual table
    /// * `args` - Arguments from the CREATE statement
    ///
    /// # Returns
    ///
    /// The table schema as a CREATE TABLE statement (without the table name),
    /// and the table instance.
    fn create(
        &self,
        db_name: &str,
        table_name: &str,
        args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)>;

    /// Create a new virtual table with database context (xCreate)
    ///
    /// This variant is called when a database context is available (e.g., during
    /// VCreate opcode execution). Modules that need to query the database during
    /// creation should override this method.
    ///
    /// The default implementation calls `create()`.
    fn create_with_ctx(
        &self,
        db_name: &str,
        table_name: &str,
        args: &[String],
        _ctx: &dyn DbContext,
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        // Default: ignore context and call regular create
        self.create(db_name, table_name, args)
    }

    /// Connect to an existing virtual table (xConnect)
    ///
    /// This is called when accessing a virtual table that was created
    /// in a previous session. The module should reconnect to existing
    /// storage/shadow tables.
    ///
    /// For most modules, this is identical to `create()`.
    fn connect(
        &self,
        db_name: &str,
        table_name: &str,
        args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        // Default: same as create
        self.create(db_name, table_name, args)
    }

    /// Connect to an existing virtual table with database context (xConnect)
    ///
    /// This variant is called when a database context is available.
    /// Modules that need to query the database during connection should
    /// override this method.
    ///
    /// The default implementation calls `connect()`.
    fn connect_with_ctx(
        &self,
        db_name: &str,
        table_name: &str,
        args: &[String],
        _ctx: &dyn DbContext,
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        // Default: ignore context and call regular connect
        self.connect(db_name, table_name, args)
    }

    /// Destroy a virtual table (xDestroy)
    ///
    /// Called for `DROP TABLE`. The module should remove any storage/shadow
    /// tables it created.
    fn destroy(&self, _table: Arc<dyn VtabTable>) -> Result<()> {
        Ok(())
    }

    /// Check if this module creates shadow tables
    ///
    /// If true, tables with names like `{vtab}_content` are protected
    /// from direct modification.
    fn uses_shadow_tables(&self) -> bool {
        false
    }

    /// Get the list of shadow table suffixes
    ///
    /// For FTS3, this would return ["_content", "_segments", "_segdir", "_stat"].
    fn shadow_table_suffixes(&self) -> Vec<&'static str> {
        vec![]
    }

    /// Check if a table name is a shadow table of this module
    fn is_shadow_table(&self, base_name: &str, table_name: &str) -> bool {
        for suffix in self.shadow_table_suffixes() {
            if table_name == format!("{}{}", base_name, suffix) {
                return true;
            }
        }
        false
    }
}

/// A single virtual table instance
///
/// This trait represents an active virtual table that can be queried.
/// It provides query planning (xBestIndex) and cursor creation (xOpen).
///
/// # SQLite Correspondence
///
/// - `xBestIndex`: Query planning
/// - `xOpen`: Create cursor for iteration
/// - `xDisconnect`: Clean up when disconnecting
/// - `xUpdate`: INSERT/UPDATE/DELETE operations
/// - `xBegin/xSync/xCommit/xRollback`: Transaction support
/// - `xFindFunction`: Register custom functions
/// - `xRename`: Rename the table
/// - `xIntegrity`: Integrity checking
pub trait VtabTable: Send + Sync {
    /// Get the table name
    fn table_name(&self) -> &str;

    /// Get the database name
    fn db_name(&self) -> &str {
        "main"
    }

    /// Get number of columns
    fn column_count(&self) -> usize;

    /// Get column name
    fn column_name(&self, col_idx: usize) -> &str;

    /// Get column affinity/type
    fn column_affinity(&self, _col_idx: usize) -> Affinity {
        Affinity::Blob
    }

    /// Determine best index strategy for query (xBestIndex)
    ///
    /// This method is called by the query planner to determine the best
    /// way to access the virtual table given the query constraints.
    ///
    /// The implementation should:
    /// 1. Examine `info.constraints` to see what WHERE clause terms are available
    /// 2. Set `info.idx_num` and `info.idx_str` to identify the chosen strategy
    /// 3. Mark which constraints will be handled via `info.use_constraint()`
    /// 4. Set `info.estimated_cost` and `info.estimated_rows`
    /// 5. Set `info.order_by_consumed` if output is already sorted
    ///
    /// # Default Behavior
    ///
    /// If not overridden, performs a full table scan.
    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // Default: full table scan
        info.idx_num = 0;
        info.estimated_cost = 1_000_000.0;
        info.estimated_rows = 1_000_000;
        Ok(())
    }

    /// Open a cursor for iteration (xOpen)
    ///
    /// Creates a new cursor that will be used to iterate over rows.
    /// The cursor's `filter()` method will be called before iteration begins.
    fn open_cursor(&self) -> Result<Box<dyn VtabCursor>>;

    /// Disconnect from the table (xDisconnect)
    ///
    /// Called when a database connection is closed. The table should
    /// release any per-connection resources.
    fn disconnect(&self) -> Result<()> {
        Ok(())
    }

    /// Update the table (xUpdate)
    ///
    /// Handles INSERT, UPDATE, and DELETE operations.
    ///
    /// # Arguments
    ///
    /// * `rowid` - For DELETE/UPDATE: the rowid to modify. None for INSERT.
    /// * `new_rowid` - For INSERT/UPDATE: the new rowid. None to auto-generate.
    /// * `columns` - For INSERT/UPDATE: column values. None = keep existing value.
    ///
    /// # Returns
    ///
    /// The rowid of the inserted/updated row.
    ///
    /// # Default Behavior
    ///
    /// Returns a "read-only" error.
    fn update(
        &self,
        _rowid: Option<i64>,
        _new_rowid: Option<i64>,
        _columns: &[Option<Mem>],
    ) -> Result<i64> {
        Err(Error::with_message(
            ErrorCode::ReadOnly,
            "virtual table is read-only",
        ))
    }

    /// Begin a transaction (xBegin)
    fn begin(&self) -> Result<()> {
        Ok(())
    }

    /// Sync changes before commit (xSync)
    fn sync(&self) -> Result<()> {
        Ok(())
    }

    /// Commit a transaction (xCommit)
    fn commit(&self) -> Result<()> {
        Ok(())
    }

    /// Rollback a transaction (xRollback)
    fn rollback(&self) -> Result<()> {
        Ok(())
    }

    /// Create savepoint (xSavepoint)
    fn savepoint(&self, _point: i32) -> Result<()> {
        Ok(())
    }

    /// Release savepoint (xRelease)
    fn release(&self, _point: i32) -> Result<()> {
        Ok(())
    }

    /// Rollback to savepoint (xRollbackTo)
    fn rollback_to(&self, _point: i32) -> Result<()> {
        Ok(())
    }

    /// Find a custom function (xFindFunction)
    ///
    /// Returns a custom implementation for a SQL function when called
    /// on this virtual table. For example, FTS tables override the
    /// MATCH function.
    ///
    /// # Arguments
    ///
    /// * `name` - Function name
    /// * `arg_count` - Number of arguments
    ///
    /// # Returns
    ///
    /// A function implementation, or None to use the default.
    fn find_function(
        &self,
        _name: &str,
        _arg_count: i32,
    ) -> Option<Arc<dyn Fn(&[Mem]) -> Result<Mem> + Send + Sync>> {
        None
    }

    /// Rename the table (xRename)
    fn rename(&self, _new_name: &str) -> Result<()> {
        Err(Error::with_message(
            ErrorCode::Error,
            "rename not supported for this virtual table",
        ))
    }

    /// Check table integrity (xIntegrity)
    ///
    /// Returns Ok(true) if the table is valid, Ok(false) if invalid,
    /// or an error if the check could not be performed.
    fn integrity(&self) -> Result<bool> {
        Ok(true)
    }

    /// Check if a name is a shadow table for this virtual table
    fn is_shadow_table(&self, table_name: &str) -> bool {
        let base = self.table_name();
        // Common shadow table patterns
        let suffixes = [
            "_content",
            "_segments",
            "_segdir",
            "_stat",
            "_data",
            "_idx",
            "_docsize",
            "_config",
        ];
        for suffix in suffixes {
            if table_name == format!("{}{}", base, suffix) {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vtab::cursor::MemoryCursor;

    struct TestModule;

    impl VtabModule for TestModule {
        fn name(&self) -> &str {
            "test"
        }

        fn create(
            &self,
            _db_name: &str,
            table_name: &str,
            _args: &[String],
        ) -> Result<(String, Arc<dyn VtabTable>)> {
            let schema = "(a TEXT, b INTEGER)".to_string();
            Ok((
                schema,
                Arc::new(TestTable {
                    name: table_name.to_string(),
                }),
            ))
        }
    }

    struct TestTable {
        name: String,
    }

    impl VtabTable for TestTable {
        fn table_name(&self) -> &str {
            &self.name
        }

        fn column_count(&self) -> usize {
            2
        }

        fn column_name(&self, col_idx: usize) -> &str {
            match col_idx {
                0 => "a",
                1 => "b",
                _ => "",
            }
        }

        fn open_cursor(&self) -> Result<Box<dyn VtabCursor>> {
            let rows = vec![
                (1, vec![Mem::from_str("hello"), Mem::from_int(42)]),
                (2, vec![Mem::from_str("world"), Mem::from_int(100)]),
            ];
            Ok(Box::new(MemoryCursor::new(rows)))
        }
    }

    #[test]
    fn test_module_create() {
        let module = TestModule;
        let (schema, table) = module.create("main", "t1", &[]).unwrap();
        assert_eq!(schema, "(a TEXT, b INTEGER)");
        assert_eq!(table.table_name(), "t1");
        assert_eq!(table.column_count(), 2);
    }

    #[test]
    fn test_table_cursor() {
        let module = TestModule;
        let (_, table) = module.create("main", "t1", &[]).unwrap();

        let mut cursor = table.open_cursor().unwrap();
        cursor.filter(0, None, &[]).unwrap();

        assert!(!cursor.eof());
        assert_eq!(cursor.rowid().unwrap(), 1);
        assert_eq!(cursor.column(0).unwrap().to_str(), "hello");
        assert_eq!(cursor.column(1).unwrap().to_int(), 42);

        cursor.next().unwrap();
        assert!(!cursor.eof());
        assert_eq!(cursor.rowid().unwrap(), 2);

        cursor.next().unwrap();
        assert!(cursor.eof());
    }
}
