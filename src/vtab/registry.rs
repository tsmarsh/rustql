//! Virtual table module registry
//!
//! Provides a global registry for virtual table modules and instances.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error::{Error, ErrorCode, Result};

use super::module::{VtabModule, VtabTable};

/// Global registry of virtual table modules and instances
///
/// The registry has two levels:
/// 1. **Modules**: Named factories for creating virtual tables (e.g., "fts3", "rtree")
/// 2. **Instances**: Active virtual table instances (e.g., "main.documents")
///
/// # Thread Safety
///
/// The registry uses `RwLock` for thread-safe access. Multiple readers can
/// access the registry concurrently, but writes require exclusive access.
///
/// # Example
///
/// ```ignore
/// let registry = VtabRegistry::new();
///
/// // Register a module
/// registry.register_module(Arc::new(Fts3Module))?;
///
/// // Create a table
/// let module = registry.get_module("fts3")?.unwrap();
/// let (schema, table) = module.create("main", "documents", &["content".to_string()])?;
/// registry.register_instance("main", "documents", table)?;
///
/// // Access the table later
/// let instance = registry.get_instance("main", "documents")?.unwrap();
/// let cursor = instance.open_cursor()?;
/// ```
pub struct VtabRegistry {
    /// Registered modules by name (lowercase)
    modules: RwLock<HashMap<String, Arc<dyn VtabModule>>>,

    /// Active table instances by fully-qualified name ("db.table")
    instances: RwLock<HashMap<String, Arc<dyn VtabTable>>>,
}

impl VtabRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            modules: RwLock::new(HashMap::new()),
            instances: RwLock::new(HashMap::new()),
        }
    }

    /// Register a virtual table module
    ///
    /// The module will be available for `CREATE VIRTUAL TABLE ... USING <name>`.
    pub fn register_module(&self, module: Arc<dyn VtabModule>) -> Result<()> {
        let name = module.name().to_lowercase();
        let mut modules = self.modules.write().map_err(|_| {
            Error::with_message(ErrorCode::Internal, "failed to acquire registry lock")
        })?;
        modules.insert(name, module);
        Ok(())
    }

    /// Unregister a virtual table module
    pub fn unregister_module(&self, name: &str) -> Result<Option<Arc<dyn VtabModule>>> {
        let mut modules = self.modules.write().map_err(|_| {
            Error::with_message(ErrorCode::Internal, "failed to acquire registry lock")
        })?;
        Ok(modules.remove(&name.to_lowercase()))
    }

    /// Get a registered module by name
    pub fn get_module(&self, name: &str) -> Result<Option<Arc<dyn VtabModule>>> {
        let modules = self.modules.read().map_err(|_| {
            Error::with_message(ErrorCode::Internal, "failed to acquire registry lock")
        })?;
        Ok(modules.get(&name.to_lowercase()).cloned())
    }

    /// Check if a module is registered
    pub fn has_module(&self, name: &str) -> bool {
        self.modules
            .read()
            .map(|m| m.contains_key(&name.to_lowercase()))
            .unwrap_or(false)
    }

    /// List all registered module names
    pub fn list_modules(&self) -> Result<Vec<String>> {
        let modules = self.modules.read().map_err(|_| {
            Error::with_message(ErrorCode::Internal, "failed to acquire registry lock")
        })?;
        Ok(modules.keys().cloned().collect())
    }

    /// Register a virtual table instance
    ///
    /// # Arguments
    ///
    /// * `db_name` - Database name (e.g., "main", "temp")
    /// * `table_name` - Table name
    /// * `table` - The table instance
    pub fn register_instance(
        &self,
        db_name: &str,
        table_name: &str,
        table: Arc<dyn VtabTable>,
    ) -> Result<()> {
        let key = Self::instance_key(db_name, table_name);
        let mut instances = self.instances.write().map_err(|_| {
            Error::with_message(ErrorCode::Internal, "failed to acquire registry lock")
        })?;
        instances.insert(key, table);
        Ok(())
    }

    /// Unregister a virtual table instance
    pub fn unregister_instance(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Result<Option<Arc<dyn VtabTable>>> {
        let key = Self::instance_key(db_name, table_name);
        let mut instances = self.instances.write().map_err(|_| {
            Error::with_message(ErrorCode::Internal, "failed to acquire registry lock")
        })?;
        Ok(instances.remove(&key))
    }

    /// Get a virtual table instance
    pub fn get_instance(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Result<Option<Arc<dyn VtabTable>>> {
        let key = Self::instance_key(db_name, table_name);
        let instances = self.instances.read().map_err(|_| {
            Error::with_message(ErrorCode::Internal, "failed to acquire registry lock")
        })?;
        Ok(instances.get(&key).cloned())
    }

    /// Check if an instance exists
    pub fn has_instance(&self, db_name: &str, table_name: &str) -> bool {
        let key = Self::instance_key(db_name, table_name);
        self.instances
            .read()
            .map(|i| i.contains_key(&key))
            .unwrap_or(false)
    }

    /// List all instance names for a database
    pub fn list_instances(&self, db_name: &str) -> Result<Vec<String>> {
        let prefix = format!("{}.", db_name.to_lowercase());
        let instances = self.instances.read().map_err(|_| {
            Error::with_message(ErrorCode::Internal, "failed to acquire registry lock")
        })?;
        Ok(instances
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .map(|k| k[prefix.len()..].to_string())
            .collect())
    }

    /// Create the instance key
    fn instance_key(db_name: &str, table_name: &str) -> String {
        format!("{}.{}", db_name.to_lowercase(), table_name.to_lowercase())
    }

    /// Create and register a virtual table
    ///
    /// This is a convenience method that:
    /// 1. Looks up the module
    /// 2. Calls module.create()
    /// 3. Registers the instance
    ///
    /// # Returns
    ///
    /// The table schema (for sqlite_master) and the table instance.
    pub fn create_virtual_table(
        &self,
        module_name: &str,
        db_name: &str,
        table_name: &str,
        args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        let module = self.get_module(module_name)?.ok_or_else(|| {
            Error::with_message(ErrorCode::Error, format!("no such module: {}", module_name))
        })?;

        let (schema, table) = module.create(db_name, table_name, args)?;
        self.register_instance(db_name, table_name, table.clone())?;

        Ok((schema, table))
    }

    /// Connect to an existing virtual table
    ///
    /// Similar to `create_virtual_table`, but calls `module.connect()` instead.
    pub fn connect_virtual_table(
        &self,
        module_name: &str,
        db_name: &str,
        table_name: &str,
        args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        let module = self.get_module(module_name)?.ok_or_else(|| {
            Error::with_message(ErrorCode::Error, format!("no such module: {}", module_name))
        })?;

        let (schema, table) = module.connect(db_name, table_name, args)?;
        self.register_instance(db_name, table_name, table.clone())?;

        Ok((schema, table))
    }

    /// Drop a virtual table
    ///
    /// This:
    /// 1. Looks up the instance
    /// 2. Looks up the module
    /// 3. Calls module.destroy()
    /// 4. Unregisters the instance
    pub fn drop_virtual_table(
        &self,
        module_name: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<()> {
        let instance = self.unregister_instance(db_name, table_name)?;

        if let Some(table) = instance {
            if let Some(module) = self.get_module(module_name)? {
                module.destroy(table)?;
            }
        }

        Ok(())
    }
}

impl Default for VtabRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for VtabRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let modules = self.list_modules().unwrap_or_default();
        f.debug_struct("VtabRegistry")
            .field("modules", &modules)
            .finish()
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
            Ok((
                "(x TEXT)".to_string(),
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
            1
        }

        fn column_name(&self, _col_idx: usize) -> &str {
            "x"
        }

        fn open_cursor(&self) -> Result<Box<dyn crate::vtab::cursor::VtabCursor>> {
            Ok(Box::new(MemoryCursor::empty()))
        }
    }

    #[test]
    fn test_module_registration() {
        let registry = VtabRegistry::new();

        // Initially no modules
        assert!(!registry.has_module("test"));

        // Register module
        registry.register_module(Arc::new(TestModule)).unwrap();
        assert!(registry.has_module("test"));
        assert!(registry.has_module("TEST")); // Case insensitive

        // Get module
        let module = registry.get_module("test").unwrap().unwrap();
        assert_eq!(module.name(), "test");

        // List modules
        let modules = registry.list_modules().unwrap();
        assert_eq!(modules, vec!["test"]);

        // Unregister
        registry.unregister_module("test").unwrap();
        assert!(!registry.has_module("test"));
    }

    #[test]
    fn test_instance_registration() {
        let registry = VtabRegistry::new();
        registry.register_module(Arc::new(TestModule)).unwrap();

        // Create virtual table
        let (schema, table) = registry
            .create_virtual_table("test", "main", "t1", &[])
            .unwrap();
        assert_eq!(schema, "(x TEXT)");
        assert_eq!(table.table_name(), "t1");

        // Check instance exists
        assert!(registry.has_instance("main", "t1"));
        assert!(registry.has_instance("MAIN", "T1")); // Case insensitive

        // Get instance
        let instance = registry.get_instance("main", "t1").unwrap().unwrap();
        assert_eq!(instance.table_name(), "t1");

        // List instances
        let instances = registry.list_instances("main").unwrap();
        assert_eq!(instances, vec!["t1"]);

        // Drop
        registry.drop_virtual_table("test", "main", "t1").unwrap();
        assert!(!registry.has_instance("main", "t1"));
    }

    #[test]
    fn test_missing_module_error() {
        let registry = VtabRegistry::new();

        let result = registry.create_virtual_table("nonexistent", "main", "t1", &[]);
        assert!(result.is_err());
    }
}
