//! Virtual table module system
//!
//! This module provides the infrastructure for SQLite-compatible virtual tables.
//! It defines traits for modules, tables, and cursors that mirror SQLite's
//! `sqlite3_module` interface.
//!
//! # Architecture
//!
//! - `VtabModule`: Factory for creating virtual table instances (xCreate/xConnect)
//! - `VtabTable`: A single virtual table instance with query planning (xBestIndex)
//! - `VtabCursor`: Iterator over virtual table rows (xFilter/xNext/xColumn/xRowid)
//! - `VtabRegistry`: Global registry for module and instance lookup
//!
//! # Usage
//!
//! ```ignore
//! // Register a module
//! let registry = VtabRegistry::new();
//! registry.register(Arc::new(Fts3VtabModule))?;
//!
//! // Create a virtual table
//! let module = registry.get_module("fts3")?.unwrap();
//! let table = module.create_or_connect(true, &["main", "fts3", "mytable", "content"])?;
//! registry.register_instance("main.mytable", table)?;
//!
//! // Query the table
//! let instance = registry.get_instance("main.mytable")?.unwrap();
//! let cursor = instance.open_cursor(0, None, &[])?;
//! ```

mod cursor;
mod declare_vtab;
mod index_info;
mod module;
mod registry;

pub use cursor::VtabCursor;
pub use declare_vtab::{declare_vtab, DeclaredColumn, DeclaredSchema};
pub use index_info::{
    ConstraintUsage, ConstraintValue, IndexConstraint, IndexInfo, OrderBy,
    SQLITE_INDEX_CONSTRAINT_EQ, SQLITE_INDEX_CONSTRAINT_GE, SQLITE_INDEX_CONSTRAINT_GLOB,
    SQLITE_INDEX_CONSTRAINT_GT, SQLITE_INDEX_CONSTRAINT_IS, SQLITE_INDEX_CONSTRAINT_ISNOT,
    SQLITE_INDEX_CONSTRAINT_ISNOTNULL, SQLITE_INDEX_CONSTRAINT_ISNULL, SQLITE_INDEX_CONSTRAINT_LE,
    SQLITE_INDEX_CONSTRAINT_LIKE, SQLITE_INDEX_CONSTRAINT_LIMIT, SQLITE_INDEX_CONSTRAINT_LT,
    SQLITE_INDEX_CONSTRAINT_MATCH, SQLITE_INDEX_CONSTRAINT_NE, SQLITE_INDEX_CONSTRAINT_OFFSET,
    SQLITE_INDEX_CONSTRAINT_REGEXP,
};
pub use module::{VtabModule, VtabTable};
pub use registry::VtabRegistry;
