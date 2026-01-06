# Translate attach.c - ATTACH DATABASE

## Overview
Translate ATTACH/DETACH database functionality for working with multiple database files.

## Source Reference
- `sqlite3/src/attach.c` - ~600 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Attached Database Info
```rust
/// Information about an attached database
pub struct AttachedDb {
    /// Schema name (e.g., "main", "temp", "attached_name")
    pub name: String,
    /// Database filename
    pub filename: String,
    /// B-tree handle
    pub btree: Option<Arc<BtShared>>,
    /// Schema for this database
    pub schema: Arc<RwLock<Schema>>,
    /// Is this a temp database?
    pub is_temp: bool,
    /// Is the schema loaded?
    pub schema_loaded: bool,
}

/// Maximum number of attached databases
pub const MAX_ATTACHED: usize = 10;
```

### ATTACH/DETACH Statements
```rust
#[derive(Debug, Clone)]
pub struct AttachStmt {
    /// Database file expression
    pub filename: Expr,
    /// Schema name
    pub schema_name: String,
    /// Optional key for encrypted databases
    pub key: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct DetachStmt {
    /// Schema name to detach
    pub schema_name: String,
}
```

## ATTACH Implementation

```rust
impl Connection {
    /// Attach a database file
    pub fn attach_database(&mut self, filename: &str, schema_name: &str) -> Result<()> {
        // Validate schema name
        if schema_name.eq_ignore_ascii_case("main") || schema_name.eq_ignore_ascii_case("temp") {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("cannot ATTACH database with reserved name: {}", schema_name)
            ));
        }

        // Check for duplicate name
        if self.dbs.iter().any(|db| db.name.eq_ignore_ascii_case(schema_name)) {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("database {} is already in use", schema_name)
            ));
        }

        // Check attachment limit
        if self.dbs.len() >= MAX_ATTACHED + 2 {  // +2 for main and temp
            return Err(Error::with_message(
                ErrorCode::Error,
                "too many attached databases"
            ));
        }

        // Check authorizer
        if let Some(ref auth) = self.authorizer {
            let rc = auth(
                AuthAction::Attach,
                filename,
                None,
                None,
            );
            if rc != AuthResult::Ok {
                return Err(Error::with_code(ErrorCode::Auth));
            }
        }

        // Open the database file
        let flags = if filename == ":memory:" {
            OpenFlags::READWRITE | OpenFlags::CREATE | OpenFlags::MEMORY
        } else {
            OpenFlags::READWRITE | OpenFlags::CREATE
        };

        let btree = BtShared::open(&self.vfs, filename, flags)?;

        // Create attached database entry
        let db = AttachedDb {
            name: schema_name.to_string(),
            filename: filename.to_string(),
            btree: Some(Arc::new(btree)),
            schema: Arc::new(RwLock::new(Schema::new())),
            is_temp: filename == ":memory:" || filename.is_empty(),
            schema_loaded: false,
        };

        self.dbs.push(DbInfo {
            name: schema_name.to_string(),
            btree: db.btree.clone(),
            schema: Some(db.schema.clone()),
            safety_level: SafetyLevel::Full,
            busy: false,
        });

        self.n_db += 1;

        // Load schema for new database
        self.load_schema(schema_name)?;

        Ok(())
    }

    /// Detach a database
    pub fn detach_database(&mut self, schema_name: &str) -> Result<()> {
        // Cannot detach main or temp
        if schema_name.eq_ignore_ascii_case("main") || schema_name.eq_ignore_ascii_case("temp") {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("cannot detach database {}", schema_name)
            ));
        }

        // Find the database
        let idx = self.dbs.iter()
            .position(|db| db.name.eq_ignore_ascii_case(schema_name))
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such database: {}", schema_name)
            ))?;

        // Check for active transactions
        if self.dbs[idx].busy {
            return Err(Error::with_message(
                ErrorCode::Busy,
                "database is locked"
            ));
        }

        // Check authorizer
        if let Some(ref auth) = self.authorizer {
            let rc = auth(
                AuthAction::Detach,
                schema_name,
                None,
                None,
            );
            if rc != AuthResult::Ok {
                return Err(Error::with_code(ErrorCode::Auth));
            }
        }

        // Close B-tree
        if let Some(ref btree) = self.dbs[idx].btree {
            btree.close()?;
        }

        // Remove from list
        self.dbs.remove(idx);
        self.n_db -= 1;

        Ok(())
    }
}
```

## ATTACH Statement Compilation

```rust
impl<'a> Parse<'a> {
    pub fn compile_attach(&mut self, attach: &AttachStmt) -> Result<()> {
        // Evaluate filename expression
        let filename_reg = self.alloc_mem();
        self.compile_expr_target(&attach.filename, filename_reg)?;

        // Evaluate schema name (it's just a string)
        let schema_reg = self.alloc_mem();
        self.add_string_op(schema_reg, &attach.schema_name);

        // Generate attach operation
        self.add_op(Opcode::Attach, filename_reg, schema_reg, 0);

        Ok(())
    }

    pub fn compile_detach(&mut self, detach: &DetachStmt) -> Result<()> {
        // Schema name is a constant
        let schema_reg = self.alloc_mem();
        self.add_string_op(schema_reg, &detach.schema_name);

        // Generate detach operation
        self.add_op(Opcode::Detach, schema_reg, 0, 0);

        Ok(())
    }
}
```

## VDBE Opcodes

```rust
impl Vdbe {
    fn exec_attach(&mut self, filename_reg: i32, schema_reg: i32) -> Result<()> {
        let filename = self.mem[filename_reg as usize].as_str();
        let schema_name = self.mem[schema_reg as usize].as_str();

        self.db.attach_database(filename, schema_name)?;

        Ok(())
    }

    fn exec_detach(&mut self, schema_reg: i32) -> Result<()> {
        let schema_name = self.mem[schema_reg as usize].as_str();

        self.db.detach_database(schema_name)?;

        Ok(())
    }
}
```

## Schema Resolution with Attached Databases

```rust
impl Connection {
    /// Find a table by name, searching attached databases
    pub fn find_table(&self, name: &str, schema: Option<&str>) -> Option<Arc<Table>> {
        if let Some(schema_name) = schema {
            // Explicit schema
            self.dbs.iter()
                .find(|db| db.name.eq_ignore_ascii_case(schema_name))
                .and_then(|db| db.schema.as_ref())
                .and_then(|schema| schema.read().ok())
                .and_then(|s| s.tables.get(name).cloned())
        } else {
            // Search all databases in order
            for db in &self.dbs {
                if let Some(ref schema) = db.schema {
                    if let Ok(s) = schema.read() {
                        if let Some(table) = s.tables.get(name) {
                            return Some(table.clone());
                        }
                    }
                }
            }
            None
        }
    }

    /// Get database index by schema name
    pub fn get_db_index(&self, schema_name: &str) -> Option<usize> {
        self.dbs.iter()
            .position(|db| db.name.eq_ignore_ascii_case(schema_name))
    }

    /// List all attached databases
    pub fn list_databases(&self) -> Vec<(&str, &str)> {
        self.dbs.iter()
            .filter_map(|db| {
                db.btree.as_ref().map(|bt| {
                    (db.name.as_str(), bt.filename())
                })
            })
            .collect()
    }
}
```

## Cross-Database Operations

```rust
impl<'a> Parse<'a> {
    /// Resolve qualified name to (db_index, table)
    pub fn resolve_table(&self, name: &QualifiedName) -> Result<(usize, Arc<Table>)> {
        let db_idx = if let Some(ref schema) = name.schema {
            self.conn.get_db_index(schema)
                .ok_or_else(|| Error::with_message(
                    ErrorCode::Error,
                    format!("unknown database: {}", schema)
                ))?
        } else {
            // Search main, then temp, then attached
            let search_order = [0, 1];  // main=0, temp=1
            let mut found = None;

            for &idx in &search_order {
                if idx < self.conn.dbs.len() {
                    if let Some(table) = self.find_table_in_db(idx, &name.name) {
                        found = Some((idx, table));
                        break;
                    }
                }
            }

            // Search attached
            if found.is_none() {
                for idx in 2..self.conn.dbs.len() {
                    if let Some(table) = self.find_table_in_db(idx, &name.name) {
                        found = Some((idx, table));
                        break;
                    }
                }
            }

            found.map(|(idx, _)| idx).ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", name.name)
            ))?
        };

        let table = self.find_table_in_db(db_idx, &name.name)
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", name.name)
            ))?;

        Ok((db_idx, table))
    }

    fn find_table_in_db(&self, db_idx: usize, name: &str) -> Option<Arc<Table>> {
        self.conn.dbs.get(db_idx)
            .and_then(|db| db.schema.as_ref())
            .and_then(|schema| schema.read().ok())
            .and_then(|s| s.tables.get(name).cloned())
    }
}
```

## Authorizer Actions

```rust
#[derive(Debug, Clone, Copy)]
pub enum AuthAction {
    Attach,
    Detach,
    // ... other actions
}

impl Connection {
    pub fn set_authorizer(&mut self, auth: Option<Authorizer>) {
        self.authorizer = auth;
    }
}

pub type Authorizer = fn(AuthAction, &str, Option<&str>, Option<&str>) -> AuthResult;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AuthResult {
    Ok,
    Deny,
    Ignore,
}
```

## Acceptance Criteria
- [ ] ATTACH DATABASE filename AS name
- [ ] DETACH DATABASE name
- [ ] Schema name validation
- [ ] Attachment limit enforcement
- [ ] Cannot detach main/temp
- [ ] Cross-database table references (schema.table)
- [ ] Schema loading for attached databases
- [ ] Authorizer callback for ATTACH/DETACH
- [ ] In-memory attached databases
- [ ] database_list pragma support
- [ ] Error handling for missing files
- [ ] Transaction handling across databases
