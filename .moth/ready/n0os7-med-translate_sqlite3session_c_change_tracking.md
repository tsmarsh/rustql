# Translate sqlite3session.c - Change Tracking

## Overview
Translate session extension for tracking and applying database changes.

## Source Reference
- `sqlite3/ext/session/sqlite3session.c` - Change tracking (6,762 lines)

## Design Fidelity
- SQLite's "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite's observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Session
```rust
/// Database change tracking session
pub struct Session {
    /// Database connection
    db: *mut Connection,
    /// Database name (usually "main")
    db_name: String,
    /// Tables being tracked
    tables: HashMap<String, SessionTable>,
    /// Is session enabled
    enabled: bool,
    /// Is indirect mode (changes from triggers count as direct)
    indirect: bool,
    /// Pre-update hook installed
    hook_installed: bool,
}

/// Table tracking info
struct SessionTable {
    /// Table name
    name: String,
    /// Column count
    n_col: i32,
    /// Primary key columns (bitmap)
    pk_cols: Vec<bool>,
    /// Pending changes (keyed by PK value)
    changes: HashMap<Vec<u8>, SessionChange>,
}

/// Single row change
#[derive(Debug, Clone)]
pub struct SessionChange {
    /// Change type
    op: ChangeOp,
    /// Old values (for UPDATE/DELETE)
    old: Option<Vec<Value>>,
    /// New values (for UPDATE/INSERT)
    new: Option<Vec<Value>>,
    /// Is indirect change
    indirect: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}
```

### Changeset
```rust
/// Serialized set of changes
pub struct Changeset {
    /// Raw changeset data
    data: Vec<u8>,
}

/// Changeset iterator
pub struct ChangesetIter<'a> {
    /// Data being iterated
    data: &'a [u8],
    /// Current position
    pos: usize,
    /// Current table name
    current_table: Option<String>,
    /// Current column count
    current_n_col: i32,
    /// Current PK columns
    current_pk: Vec<bool>,
}

/// Single change from iterator
pub struct ChangesetChange {
    /// Table name
    pub table: String,
    /// Operation
    pub op: ChangeOp,
    /// Number of columns
    pub n_col: i32,
    /// Primary key columns
    pub pk: Vec<bool>,
    /// Old values (for UPDATE/DELETE)
    pub old: Option<Vec<Value>>,
    /// New values (for UPDATE/INSERT)
    pub new: Option<Vec<Value>>,
    /// Is indirect
    pub indirect: bool,
}
```

### Patchset
```rust
/// Patchset - like changeset but without old values for UPDATE
pub struct Patchset {
    data: Vec<u8>,
}

impl Patchset {
    /// Convert from changeset
    pub fn from_changeset(changeset: &Changeset) -> Result<Self> {
        let mut data = Vec::new();
        let mut iter = changeset.iter();

        while let Some(change) = iter.next()? {
            // Write table header if needed
            // For UPDATE, only write new values (not old)
            // For DELETE, only write PK values
            // For INSERT, write all new values
            Self::encode_change(&mut data, &change, true)?;
        }

        Ok(Self { data })
    }
}
```

## Session Operations

### Create and Configure
```rust
impl Session {
    /// Create new session
    pub fn new(db: &mut Connection, db_name: &str) -> Result<Self> {
        let mut session = Self {
            db: db as *mut _,
            db_name: db_name.to_string(),
            tables: HashMap::new(),
            enabled: true,
            indirect: false,
            hook_installed: false,
        };

        // Install pre-update hook
        session.install_hook()?;

        Ok(session)
    }

    /// Attach table for tracking
    pub fn attach(&mut self, table: Option<&str>) -> Result<()> {
        if let Some(name) = table {
            // Attach specific table
            self.attach_table(name)?;
        } else {
            // Attach all tables (wildcard)
            let tables = self.list_tables()?;
            for t in tables {
                self.attach_table(&t)?;
            }
        }
        Ok(())
    }

    fn attach_table(&mut self, name: &str) -> Result<()> {
        // Get table schema
        let (n_col, pk_cols) = self.get_table_info(name)?;

        self.tables.insert(name.to_string(), SessionTable {
            name: name.to_string(),
            n_col,
            pk_cols,
            changes: HashMap::new(),
        });

        Ok(())
    }

    fn get_table_info(&self, name: &str) -> Result<(i32, Vec<bool>)> {
        let db = unsafe { &*self.db };
        let sql = format!("PRAGMA table_info('{}')", name);
        let mut stmt = db.prepare(&sql)?;

        let mut columns = Vec::new();
        while stmt.step()? == StepResult::Row {
            let is_pk = stmt.column_int(5)? > 0;
            columns.push(is_pk);
        }

        Ok((columns.len() as i32, columns))
    }

    /// Enable/disable session
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Set indirect mode
    pub fn set_indirect(&mut self, indirect: bool) {
        self.indirect = indirect;
    }

    fn install_hook(&mut self) -> Result<()> {
        if self.hook_installed {
            return Ok(());
        }

        let db = unsafe { &mut *self.db };

        // Install pre-update hook
        db.set_preupdate_hook(Some(Box::new(|args| {
            self.preupdate_callback(args);
        })))?;

        self.hook_installed = true;
        Ok(())
    }
}
```

### Change Tracking
```rust
impl Session {
    /// Pre-update callback - called before each change
    fn preupdate_callback(&mut self, args: &PreupdateArgs) {
        if !self.enabled {
            return;
        }

        let table_name = args.table_name();

        // Check if table is tracked
        let table = match self.tables.get_mut(table_name) {
            Some(t) => t,
            None => return,
        };

        // Get primary key value
        let pk_value = self.compute_pk_value(args, &table.pk_cols);

        match args.op() {
            SQLITE_INSERT => {
                self.handle_insert(table, pk_value, args);
            }
            SQLITE_UPDATE => {
                self.handle_update(table, pk_value, args);
            }
            SQLITE_DELETE => {
                self.handle_delete(table, pk_value, args);
            }
            _ => {}
        }
    }

    fn compute_pk_value(&self, args: &PreupdateArgs, pk_cols: &[bool]) -> Vec<u8> {
        let mut key = Vec::new();

        for (i, &is_pk) in pk_cols.iter().enumerate() {
            if is_pk {
                let value = if args.op() == SQLITE_INSERT {
                    args.new_value(i as i32)
                } else {
                    args.old_value(i as i32)
                };
                self.encode_value(&mut key, &value);
            }
        }

        key
    }

    fn handle_insert(&mut self, table: &mut SessionTable, pk: Vec<u8>, args: &PreupdateArgs) {
        // Check for existing change on same PK
        if let Some(existing) = table.changes.get(&pk) {
            match existing.op {
                ChangeOp::Delete => {
                    // DELETE then INSERT = UPDATE
                    let old = existing.old.clone();
                    let new = self.get_new_values(args, table.n_col);
                    table.changes.insert(pk, SessionChange {
                        op: ChangeOp::Update,
                        old,
                        new: Some(new),
                        indirect: self.indirect,
                    });
                }
                _ => {
                    // INSERT after INSERT or UPDATE - keep new values
                    let new = self.get_new_values(args, table.n_col);
                    if let Some(change) = table.changes.get_mut(&pk) {
                        change.new = Some(new);
                    }
                }
            }
        } else {
            // New INSERT
            let new = self.get_new_values(args, table.n_col);
            table.changes.insert(pk, SessionChange {
                op: ChangeOp::Insert,
                old: None,
                new: Some(new),
                indirect: self.indirect,
            });
        }
    }

    fn handle_update(&mut self, table: &mut SessionTable, pk: Vec<u8>, args: &PreupdateArgs) {
        if let Some(existing) = table.changes.get_mut(&pk) {
            // Already have a change - update new values
            let new = self.get_new_values(args, table.n_col);
            existing.new = Some(new);
        } else {
            // First update on this row
            let old = self.get_old_values(args, table.n_col);
            let new = self.get_new_values(args, table.n_col);
            table.changes.insert(pk, SessionChange {
                op: ChangeOp::Update,
                old: Some(old),
                new: Some(new),
                indirect: self.indirect,
            });
        }
    }

    fn handle_delete(&mut self, table: &mut SessionTable, pk: Vec<u8>, args: &PreupdateArgs) {
        if let Some(existing) = table.changes.get(&pk) {
            match existing.op {
                ChangeOp::Insert => {
                    // INSERT then DELETE = no change
                    table.changes.remove(&pk);
                }
                _ => {
                    // UPDATE then DELETE = DELETE with original old values
                    let old = existing.old.clone();
                    table.changes.insert(pk, SessionChange {
                        op: ChangeOp::Delete,
                        old,
                        new: None,
                        indirect: self.indirect,
                    });
                }
            }
        } else {
            // First delete on this row
            let old = self.get_old_values(args, table.n_col);
            table.changes.insert(pk, SessionChange {
                op: ChangeOp::Delete,
                old: Some(old),
                new: None,
                indirect: self.indirect,
            });
        }
    }
}
```

### Generate Changeset
```rust
impl Session {
    /// Generate changeset from tracked changes
    pub fn generate_changeset(&self) -> Result<Changeset> {
        let mut data = Vec::new();

        for (_, table) in &self.tables {
            if table.changes.is_empty() {
                continue;
            }

            // Write table header
            self.write_table_header(&mut data, table)?;

            // Write changes
            for (_, change) in &table.changes {
                self.write_change(&mut data, table, change)?;
            }
        }

        Ok(Changeset { data })
    }

    fn write_table_header(&self, data: &mut Vec<u8>, table: &SessionTable) -> Result<()> {
        // Table name length (varint)
        sqlite3_put_varint(data, table.name.len() as u64);
        // Table name
        data.extend_from_slice(table.name.as_bytes());
        // Column count (varint)
        sqlite3_put_varint(data, table.n_col as u64);
        // PK flags
        for &is_pk in &table.pk_cols {
            data.push(if is_pk { 1 } else { 0 });
        }

        Ok(())
    }

    fn write_change(&self, data: &mut Vec<u8>, table: &SessionTable, change: &SessionChange) -> Result<()> {
        // Operation byte
        let op_byte = match change.op {
            ChangeOp::Insert => SQLITE_INSERT as u8,
            ChangeOp::Update => SQLITE_UPDATE as u8,
            ChangeOp::Delete => SQLITE_DELETE as u8,
        };
        data.push(op_byte | if change.indirect { 0x80 } else { 0 });

        // Old values (for UPDATE/DELETE)
        if let Some(old) = &change.old {
            for (i, val) in old.iter().enumerate() {
                if change.op == ChangeOp::Update && !table.pk_cols[i] {
                    // For UPDATE, skip non-PK unchanged columns
                    if let (Some(new), ChangeOp::Update) = (&change.new, change.op) {
                        if values_equal(&old[i], &new[i]) {
                            data.push(0); // undefined
                            continue;
                        }
                    }
                }
                self.encode_value(data, val);
            }
        }

        // New values (for INSERT/UPDATE)
        if let Some(new) = &change.new {
            for val in new {
                self.encode_value(data, val);
            }
        }

        Ok(())
    }

    fn encode_value(&self, data: &mut Vec<u8>, value: &Value) {
        match value {
            Value::Null => data.push(0),
            Value::Integer(i) => {
                data.push(1);
                sqlite3_put_varint(data, *i as u64);
            }
            Value::Float(f) => {
                data.push(2);
                data.extend_from_slice(&f.to_be_bytes());
            }
            Value::Text(s) => {
                data.push(3);
                sqlite3_put_varint(data, s.len() as u64);
                data.extend_from_slice(s.as_bytes());
            }
            Value::Blob(b) => {
                data.push(4);
                sqlite3_put_varint(data, b.len() as u64);
                data.extend_from_slice(b);
            }
        }
    }
}
```

### Apply Changeset
```rust
/// Apply changeset to database
pub fn changeset_apply(
    db: &mut Connection,
    changeset: &Changeset,
    filter: Option<&dyn Fn(&str) -> bool>,
    conflict: Option<&dyn Fn(&ChangesetChange, ConflictType) -> ConflictAction>,
) -> Result<()> {
    let mut iter = changeset.iter();

    while let Some(change) = iter.next()? {
        // Check filter
        if let Some(f) = filter {
            if !f(&change.table) {
                continue;
            }
        }

        let result = apply_single_change(db, &change);

        if let Err(e) = result {
            // Conflict handling
            let conflict_type = classify_conflict(&e);

            if let Some(handler) = conflict {
                let action = handler(&change, conflict_type);
                match action {
                    ConflictAction::Omit => continue,
                    ConflictAction::Replace => {
                        apply_with_replace(db, &change)?;
                    }
                    ConflictAction::Abort => return Err(e),
                }
            } else {
                return Err(e);
            }
        }
    }

    Ok(())
}

fn apply_single_change(db: &mut Connection, change: &ChangesetChange) -> Result<()> {
    match change.op {
        ChangeOp::Insert => {
            let columns: Vec<_> = (0..change.n_col).map(|i| format!("c{}", i)).collect();
            let placeholders: Vec<_> = (0..change.n_col).map(|_| "?").collect();
            let sql = format!(
                "INSERT INTO \"{}\" ({}) VALUES ({})",
                change.table,
                columns.join(", "),
                placeholders.join(", ")
            );

            let mut stmt = db.prepare(&sql)?;
            if let Some(new) = &change.new {
                for (i, val) in new.iter().enumerate() {
                    stmt.bind_value(i as i32 + 1, val)?;
                }
            }
            stmt.step()?;
        }
        ChangeOp::Delete => {
            let pk_clause = build_pk_clause(&change.pk, change.n_col);
            let sql = format!("DELETE FROM \"{}\" WHERE {}", change.table, pk_clause);

            let mut stmt = db.prepare(&sql)?;
            bind_pk_values(&mut stmt, &change.pk, change.old.as_ref().unwrap())?;
            stmt.step()?;
        }
        ChangeOp::Update => {
            let set_clause = build_update_set(change)?;
            let pk_clause = build_pk_clause(&change.pk, change.n_col);
            let sql = format!(
                "UPDATE \"{}\" SET {} WHERE {}",
                change.table, set_clause, pk_clause
            );

            let mut stmt = db.prepare(&sql)?;
            bind_update_values(&mut stmt, change)?;
            stmt.step()?;
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub enum ConflictType {
    Data,
    NotFound,
    Conflict,
    Constraint,
    ForeignKey,
}

#[derive(Debug, Clone, Copy)]
pub enum ConflictAction {
    Omit,
    Replace,
    Abort,
}
```

### Changeset Inversion
```rust
impl Changeset {
    /// Invert changeset (for undo)
    pub fn invert(&self) -> Result<Changeset> {
        let mut data = Vec::new();
        let mut iter = self.iter();

        while let Some(change) = iter.next()? {
            let inverted = match change.op {
                ChangeOp::Insert => ChangesetChange {
                    op: ChangeOp::Delete,
                    old: change.new.clone(),
                    new: None,
                    ..change
                },
                ChangeOp::Delete => ChangesetChange {
                    op: ChangeOp::Insert,
                    old: None,
                    new: change.old.clone(),
                    ..change
                },
                ChangeOp::Update => ChangesetChange {
                    op: ChangeOp::Update,
                    old: change.new.clone(),
                    new: change.old.clone(),
                    ..change
                },
            };

            encode_change(&mut data, &inverted)?;
        }

        Ok(Changeset { data })
    }
}
```

## Acceptance Criteria
- [ ] Session creation and configuration
- [ ] Table attachment (specific and wildcard)
- [ ] Pre-update hook integration
- [ ] INSERT change tracking
- [ ] UPDATE change tracking
- [ ] DELETE change tracking
- [ ] Change coalescing (INSERT+DELETE=nothing, etc.)
- [ ] Changeset generation
- [ ] Changeset iteration
- [ ] Changeset application
- [ ] Conflict detection and handling
- [ ] Patchset generation
- [ ] Changeset inversion
- [ ] Indirect change tracking
