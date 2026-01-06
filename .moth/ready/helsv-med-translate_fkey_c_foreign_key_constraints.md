# Translate fkey.c - Foreign Key Constraints

## Overview
Translate foreign key constraint enforcement including INSERT/UPDATE/DELETE checks and CASCADE actions.

## Source Reference
- `sqlite3/src/fkey.c` - 1,484 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Foreign Key Definition
```rust
#[derive(Debug, Clone)]
pub struct ForeignKey {
    /// Parent table (referenced table)
    pub to_table: String,
    /// Columns in this table (child)
    pub from_columns: Vec<String>,
    /// Columns in parent table
    pub to_columns: Vec<String>,
    /// ON DELETE action
    pub on_delete: FkAction,
    /// ON UPDATE action
    pub on_update: FkAction,
    /// Deferrable state
    pub deferrable: FkDeferrable,
    /// Constraint name
    pub name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FkAction {
    NoAction,
    Restrict,
    SetNull,
    SetDefault,
    Cascade,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FkDeferrable {
    NotDeferrable,
    Deferred,
    Immediate,
}
```

### FK Trigger Context
```rust
/// Context for FK trigger generation
struct FkContext<'a> {
    /// The table with the foreign key
    table: &'a Table,
    /// The foreign key being checked
    fk: &'a ForeignKey,
    /// Parent table
    parent: &'a Table,
    /// Register for old values
    old_reg: Option<i32>,
    /// Register for new values
    new_reg: Option<i32>,
}
```

## Foreign Key Checking

### INSERT Check
```rust
impl<'a> Parse<'a> {
    /// Generate FK check for INSERT
    pub fn fk_check_insert(&mut self, table: &Table, new_reg: i32) -> Result<()> {
        if !self.conn.foreign_keys_enabled() {
            return Ok(());
        }

        for fk in &table.foreign_keys {
            let parent = self.find_table(&fk.to_table, None)?;

            // Find parent index on referenced columns
            let parent_idx = self.find_parent_index(&parent, &fk.to_columns)?;

            // For each FK column, check parent exists
            self.generate_existence_check(fk, &parent, parent_idx, new_reg)?;
        }

        Ok(())
    }

    fn generate_existence_check(
        &mut self,
        fk: &ForeignKey,
        parent: &Table,
        parent_idx: Option<&Index>,
        new_reg: i32,
    ) -> Result<()> {
        // Skip if any FK column is NULL (NULL = valid)
        let skip_label = self.make_label();
        for (i, col_name) in fk.from_columns.iter().enumerate() {
            let col_idx = self.find_column_index(col_name)?;
            let val_reg = new_reg + col_idx as i32;
            self.add_op(Opcode::IsNull, val_reg, skip_label, 0);
        }

        // Look up parent row
        if let Some(idx) = parent_idx {
            // Use index
            let cursor = self.alloc_cursor();
            self.add_op(Opcode::OpenRead, cursor, idx.root_page as i32, 0);

            // Build key from FK values
            let key_reg = self.alloc_mem();
            self.build_fk_key(fk, new_reg, key_reg)?;

            // Seek
            let found_label = self.make_label();
            self.add_op(Opcode::SeekGE, cursor, found_label, key_reg);

            // Not found - constraint violation
            self.add_op(Opcode::FkError, 0, 0, 0);

            self.resolve_label(found_label);
            self.add_op(Opcode::Close, cursor, 0, 0);
        } else {
            // Full table scan
            self.generate_fk_table_scan(fk, parent, new_reg)?;
        }

        self.resolve_label(skip_label);

        Ok(())
    }
}
```

### UPDATE Check
```rust
impl<'a> Parse<'a> {
    /// Generate FK check for UPDATE
    pub fn fk_check_update(
        &mut self,
        table: &Table,
        old_reg: i32,
        new_reg: i32,
        changed_cols: &HashSet<usize>,
    ) -> Result<()> {
        if !self.conn.foreign_keys_enabled() {
            return Ok(());
        }

        // Check this table's FKs (child side)
        for fk in &table.foreign_keys {
            // Only check if FK columns changed
            let fk_cols_changed = fk.from_columns.iter().any(|col| {
                self.find_column_index(col)
                    .map(|idx| changed_cols.contains(&idx))
                    .unwrap_or(false)
            });

            if fk_cols_changed {
                let parent = self.find_table(&fk.to_table, None)?;
                let parent_idx = self.find_parent_index(&parent, &fk.to_columns)?;
                self.generate_existence_check(fk, &parent, parent_idx, new_reg)?;
            }
        }

        // Check FKs referencing this table (parent side)
        self.fk_check_parent_update(table, old_reg, new_reg, changed_cols)?;

        Ok(())
    }

    fn fk_check_parent_update(
        &mut self,
        table: &Table,
        old_reg: i32,
        new_reg: i32,
        changed_cols: &HashSet<usize>,
    ) -> Result<()> {
        // Find all FKs that reference this table
        let referencing = self.find_referencing_fks(&table.name)?;

        for (child_table, fk) in referencing {
            // Check if referenced columns changed
            let ref_cols_changed = fk.to_columns.iter().any(|col| {
                self.find_column_index_in_table(table, col)
                    .map(|idx| changed_cols.contains(&idx))
                    .unwrap_or(false)
            });

            if ref_cols_changed {
                self.generate_fk_action(
                    &fk,
                    &child_table,
                    table,
                    fk.on_update,
                    old_reg,
                    Some(new_reg),
                )?;
            }
        }

        Ok(())
    }
}
```

### DELETE Check
```rust
impl<'a> Parse<'a> {
    /// Generate FK check for DELETE
    pub fn fk_check_delete(&mut self, table: &Table, old_reg: i32) -> Result<()> {
        if !self.conn.foreign_keys_enabled() {
            return Ok(());
        }

        // Find all FKs that reference this table
        let referencing = self.find_referencing_fks(&table.name)?;

        for (child_table, fk) in referencing {
            self.generate_fk_action(
                &fk,
                &child_table,
                table,
                fk.on_delete,
                old_reg,
                None,
            )?;
        }

        Ok(())
    }
}
```

## FK Actions

```rust
impl<'a> Parse<'a> {
    fn generate_fk_action(
        &mut self,
        fk: &ForeignKey,
        child_table: &Table,
        parent_table: &Table,
        action: FkAction,
        old_reg: i32,
        new_reg: Option<i32>,
    ) -> Result<()> {
        match action {
            FkAction::NoAction | FkAction::Restrict => {
                // Check for referencing rows - error if found
                self.generate_fk_restrict(fk, child_table, parent_table, old_reg)?;
            }
            FkAction::Cascade => {
                if new_reg.is_some() {
                    // UPDATE CASCADE - update child rows
                    self.generate_fk_cascade_update(fk, child_table, old_reg, new_reg.unwrap())?;
                } else {
                    // DELETE CASCADE - delete child rows
                    self.generate_fk_cascade_delete(fk, child_table, old_reg)?;
                }
            }
            FkAction::SetNull => {
                self.generate_fk_set_null(fk, child_table, old_reg)?;
            }
            FkAction::SetDefault => {
                self.generate_fk_set_default(fk, child_table, old_reg)?;
            }
        }

        Ok(())
    }

    fn generate_fk_restrict(
        &mut self,
        fk: &ForeignKey,
        child_table: &Table,
        parent_table: &Table,
        old_reg: i32,
    ) -> Result<()> {
        // Check if any child rows reference the old parent key
        let cursor = self.alloc_cursor();

        // Find index on child FK columns
        let child_idx = self.find_child_index(child_table, &fk.from_columns)?;

        if let Some(idx) = child_idx {
            self.add_op(Opcode::OpenRead, cursor, idx.root_page as i32, 0);

            // Build key from old parent values
            let key_reg = self.alloc_mem();
            self.build_parent_key(fk, parent_table, old_reg, key_reg)?;

            let not_found = self.make_label();
            self.add_op(Opcode::SeekGE, cursor, not_found, key_reg);

            // Found - FK violation
            self.add_fk_error_op(fk)?;

            self.resolve_label(not_found);
            self.add_op(Opcode::Close, cursor, 0, 0);
        } else {
            // Full scan
            self.generate_fk_scan_restrict(fk, child_table, parent_table, old_reg)?;
        }

        Ok(())
    }

    fn generate_fk_cascade_delete(
        &mut self,
        fk: &ForeignKey,
        child_table: &Table,
        old_reg: i32,
    ) -> Result<()> {
        // Find and delete all child rows
        let cursor = self.alloc_cursor();
        self.add_op(Opcode::OpenWrite, cursor, child_table.root_page as i32, 0);

        // Scan for matching rows
        let loop_start = self.make_label();
        let loop_end = self.make_label();

        self.add_op(Opcode::Rewind, cursor, loop_end, 0);
        self.resolve_label(loop_start);

        // Check if row matches
        let match_label = self.make_label();
        let next_label = self.make_label();

        for (i, col_name) in fk.from_columns.iter().enumerate() {
            let child_col_idx = self.find_column_index_in_table(child_table, col_name)?;
            let parent_col_idx = self.find_column_index_in_table(&self.current_table()?, &fk.to_columns[i])?;

            let child_val = self.alloc_mem();
            self.add_op(Opcode::Column, cursor, child_col_idx as i32, child_val);

            let parent_val = old_reg + parent_col_idx as i32;

            self.add_op(Opcode::Ne, child_val, next_label, parent_val);
        }

        // Match - delete row
        self.add_op(Opcode::Delete, cursor, 0, 0);

        self.resolve_label(next_label);
        self.add_op(Opcode::Next, cursor, loop_start, 0);

        self.resolve_label(loop_end);
        self.add_op(Opcode::Close, cursor, 0, 0);

        Ok(())
    }

    fn generate_fk_set_null(
        &mut self,
        fk: &ForeignKey,
        child_table: &Table,
        old_reg: i32,
    ) -> Result<()> {
        // Update matching child rows, setting FK columns to NULL
        let cursor = self.alloc_cursor();
        self.add_op(Opcode::OpenWrite, cursor, child_table.root_page as i32, 0);

        // Similar loop structure as cascade delete
        // But instead of delete, UPDATE SET col = NULL

        Ok(())
    }
}
```

## Deferred Constraints

```rust
impl Connection {
    /// Deferred FK violations
    pub fn deferred_fk_violations(&self) -> i64 {
        self.deferred_fk_count
    }

    /// Check deferred constraints
    pub fn check_deferred_fk(&self) -> Result<()> {
        if self.deferred_fk_count > 0 {
            return Err(Error::with_message(
                ErrorCode::Constraint,
                format!("{} foreign key constraint violations", self.deferred_fk_count)
            ));
        }
        Ok(())
    }
}

impl<'a> Parse<'a> {
    fn handle_deferred_fk(&mut self, fk: &ForeignKey) -> Result<()> {
        match fk.deferrable {
            FkDeferrable::NotDeferrable => {
                // Immediate check - generate inline
                Ok(())
            }
            FkDeferrable::Immediate | FkDeferrable::Deferred => {
                // Defer to statement or transaction end
                self.add_op(Opcode::FkCounter, 0, 1, 0);  // Increment deferred count
                Ok(())
            }
        }
    }
}
```

## PRAGMA foreign_key_check

```rust
impl<'a> Parse<'a> {
    pub fn pragma_foreign_key_check(&mut self, table_name: Option<&str>) -> Result<()> {
        self.set_num_columns(4);
        self.set_column_names(&["table", "rowid", "parent", "fkid"]);

        let tables: Vec<Arc<Table>> = if let Some(name) = table_name {
            vec![self.find_table(name, None)?]
        } else {
            self.conn.schema.read().unwrap().tables.values().cloned().collect()
        };

        for table in tables {
            for (fk_idx, fk) in table.foreign_keys.iter().enumerate() {
                self.check_fk_violations(&table, fk, fk_idx)?;
            }
        }

        Ok(())
    }

    fn check_fk_violations(&mut self, table: &Table, fk: &ForeignKey, fk_idx: usize) -> Result<()> {
        // Generate code to find FK violations and output rows

        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] FK constraint parsing in CREATE TABLE
- [ ] FK check on INSERT
- [ ] FK check on UPDATE
- [ ] FK check on DELETE
- [ ] ON DELETE CASCADE
- [ ] ON DELETE SET NULL
- [ ] ON DELETE SET DEFAULT
- [ ] ON DELETE RESTRICT
- [ ] ON UPDATE CASCADE
- [ ] ON UPDATE SET NULL
- [ ] ON UPDATE RESTRICT
- [ ] Deferred constraints
- [ ] PRAGMA foreign_keys enable/disable
- [ ] PRAGMA foreign_key_check
- [ ] PRAGMA foreign_key_list
- [ ] Multi-column foreign keys
- [ ] Self-referencing foreign keys
