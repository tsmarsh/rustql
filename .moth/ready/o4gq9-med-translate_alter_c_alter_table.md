# Translate alter.c - ALTER TABLE

## Overview
Translate ALTER TABLE statement handling including column operations, table renaming, and constraint modifications.

## Source Reference
- `sqlite3/src/alter.c` - 2,329 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Alter Table Statement
```rust
#[derive(Debug, Clone)]
pub enum AlterTableStmt {
    /// RENAME TABLE old_name TO new_name
    RenameTable {
        old_name: QualifiedName,
        new_name: String,
    },
    /// RENAME COLUMN old_name TO new_name
    RenameColumn {
        table: QualifiedName,
        old_name: String,
        new_name: String,
    },
    /// ADD COLUMN
    AddColumn {
        table: QualifiedName,
        column: ColumnDef,
    },
    /// DROP COLUMN
    DropColumn {
        table: QualifiedName,
        column_name: String,
    },
}
```

### Rename Context
```rust
/// Context for renaming operations
struct RenameCtx {
    /// Original SQL text
    sql: String,
    /// Positions requiring modification
    edits: Vec<RenameEdit>,
    /// Error if any
    error: Option<Error>,
}

struct RenameEdit {
    /// Start position in original SQL
    start: usize,
    /// End position in original SQL
    end: usize,
    /// Replacement text
    replacement: String,
}
```

## ALTER TABLE Handlers

### RENAME TABLE
```rust
impl Schema {
    pub fn alter_rename_table(
        &mut self,
        db: &mut Connection,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        // Validate new name doesn't exist
        if self.tables.contains_key(new_name) {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("table {} already exists", new_name)
            ));
        }

        // Get the table
        let table = self.tables.remove(old_name)
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", old_name)
            ))?;

        // Update table name
        let mut table = Arc::try_unwrap(table).unwrap_or_else(|arc| (*arc).clone());
        table.name = new_name.to_string();

        // Update CREATE TABLE SQL
        if let Some(ref sql) = table.sql {
            table.sql = Some(self.rename_in_sql(sql, old_name, new_name)?);
        }

        // Update triggers that reference this table
        let triggers_to_update: Vec<_> = self.triggers.iter()
            .filter(|(_, t)| t.table == old_name)
            .map(|(name, _)| name.clone())
            .collect();

        for trigger_name in triggers_to_update {
            if let Some(mut trigger) = self.triggers.remove(&trigger_name) {
                let trigger = Arc::make_mut(&mut trigger);
                trigger.table = new_name.to_string();
                if let Some(ref sql) = trigger.sql {
                    trigger.sql = Some(self.rename_in_sql(sql, old_name, new_name)?);
                }
                self.triggers.insert(trigger_name, trigger);
            }
        }

        // Update views that reference this table
        for view in self.views.values_mut() {
            let view = Arc::make_mut(view);
            if let Some(ref sql) = view.sql {
                if sql.contains(old_name) {
                    view.sql = Some(self.rename_in_sql(sql, old_name, new_name)?);
                }
            }
        }

        // Update foreign keys pointing to this table
        for other_table in self.tables.values_mut() {
            let other = Arc::make_mut(other_table);
            for fk in &mut other.foreign_keys {
                if fk.to_table == old_name {
                    fk.to_table = new_name.to_string();
                }
            }
        }

        // Insert with new name
        self.tables.insert(new_name.to_string(), Arc::new(table));

        // Update sqlite_master
        self.update_master_name(db, "table", old_name, new_name)?;

        Ok(())
    }
}
```

### RENAME COLUMN
```rust
impl Schema {
    pub fn alter_rename_column(
        &mut self,
        db: &mut Connection,
        table_name: &str,
        old_col: &str,
        new_col: &str,
    ) -> Result<()> {
        // Get the table
        let table = self.tables.get_mut(table_name)
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", table_name)
            ))?;

        let table = Arc::make_mut(table);

        // Find the column
        let col_idx = table.columns.iter()
            .position(|c| c.name == old_col)
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such column: {}", old_col)
            ))?;

        // Check new name doesn't conflict
        if table.columns.iter().any(|c| c.name == new_col) {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("column {} already exists", new_col)
            ));
        }

        // Rename the column
        table.columns[col_idx].name = new_col.to_string();

        // Update CREATE TABLE SQL
        if let Some(ref sql) = table.sql {
            table.sql = Some(self.rename_column_in_sql(sql, old_col, new_col)?);
        }

        // Update indexes that reference this column
        for index in &mut table.indexes {
            for col in &mut index.columns {
                if col.name == old_col {
                    col.name = new_col.to_string();
                }
            }
            if let Some(ref sql) = index.sql {
                index.sql = Some(self.rename_column_in_sql(sql, old_col, new_col)?);
            }
        }

        // Update triggers that reference this column
        for trigger in self.triggers.values_mut() {
            if trigger.table == table_name {
                let trigger = Arc::make_mut(trigger);
                if let Some(ref sql) = trigger.sql {
                    trigger.sql = Some(self.rename_column_in_sql(sql, old_col, new_col)?);
                }
            }
        }

        // Update views that might reference this column
        for view in self.views.values_mut() {
            let view = Arc::make_mut(view);
            if let Some(ref sql) = view.sql {
                // Only update if the view references this table
                view.sql = Some(self.rename_column_in_sql(sql, old_col, new_col)?);
            }
        }

        // Update sqlite_master
        self.update_master_sql(db, "table", table_name, table.sql.as_deref().unwrap_or(""))?;

        Ok(())
    }
}
```

### ADD COLUMN
```rust
impl Schema {
    pub fn alter_add_column(
        &mut self,
        db: &mut Connection,
        table_name: &str,
        column: &ColumnDef,
    ) -> Result<()> {
        // Validate column constraints
        // Cannot add PRIMARY KEY column
        if column.is_pk {
            return Err(Error::with_message(
                ErrorCode::Error,
                "cannot add a PRIMARY KEY column"
            ));
        }

        // Cannot add UNIQUE column
        if column.unique {
            return Err(Error::with_message(
                ErrorCode::Error,
                "cannot add a UNIQUE column"
            ));
        }

        // NOT NULL requires default
        if column.not_null && column.default_value.is_none() {
            return Err(Error::with_message(
                ErrorCode::Error,
                "cannot add a NOT NULL column without DEFAULT"
            ));
        }

        // Get the table
        let table = self.tables.get_mut(table_name)
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", table_name)
            ))?;

        let table = Arc::make_mut(table);

        // Check name doesn't conflict
        if table.columns.iter().any(|c| c.name == column.name) {
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("duplicate column name: {}", column.name)
            ));
        }

        // Add the column
        table.columns.push(column.clone());

        // Update CREATE TABLE SQL
        if let Some(ref sql) = table.sql {
            table.sql = Some(self.add_column_to_sql(sql, column)?);
        }

        // Update sqlite_master
        self.update_master_sql(db, "table", table_name, table.sql.as_deref().unwrap_or(""))?;

        Ok(())
    }
}
```

### DROP COLUMN
```rust
impl Schema {
    pub fn alter_drop_column(
        &mut self,
        db: &mut Connection,
        table_name: &str,
        column_name: &str,
    ) -> Result<()> {
        let table = self.tables.get(table_name)
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", table_name)
            ))?;

        // Find the column
        let col_idx = table.columns.iter()
            .position(|c| c.name == column_name)
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such column: {}", column_name)
            ))?;

        // Cannot drop if it's the only column
        if table.columns.len() == 1 {
            return Err(Error::with_message(
                ErrorCode::Error,
                "cannot drop the only column"
            ));
        }

        // Cannot drop PRIMARY KEY
        if table.columns[col_idx].is_pk {
            return Err(Error::with_message(
                ErrorCode::Error,
                "cannot drop PRIMARY KEY column"
            ));
        }

        // Check if column is used in an index
        for index in &table.indexes {
            if index.columns.iter().any(|c| c.name == column_name) {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("cannot drop column used in index: {}", index.name)
                ));
            }
        }

        // Check if column is used in foreign key
        for fk in &table.foreign_keys {
            if fk.from_columns.contains(&column_name.to_string()) {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    "cannot drop column used in foreign key"
                ));
            }
        }

        // For DROP COLUMN, we need to rebuild the table
        self.rebuild_table_without_column(db, table_name, column_name)?;

        Ok(())
    }

    fn rebuild_table_without_column(
        &mut self,
        db: &mut Connection,
        table_name: &str,
        column_name: &str,
    ) -> Result<()> {
        // 1. Create new table with remaining columns
        let new_table_name = format!("sqlite_alter_temp_{}", table_name);

        // 2. Copy data to new table (excluding dropped column)
        let remaining_cols: Vec<_> = self.tables[table_name].columns.iter()
            .filter(|c| c.name != column_name)
            .map(|c| c.name.clone())
            .collect();

        let col_list = remaining_cols.join(", ");
        let copy_sql = format!(
            "INSERT INTO {} SELECT {} FROM {}",
            new_table_name, col_list, table_name
        );

        // 3. Drop old table
        // 4. Rename new table to old name
        // 5. Update schema

        Ok(())
    }
}
```

## SQL Text Manipulation

```rust
impl Schema {
    fn rename_in_sql(&self, sql: &str, old: &str, new: &str) -> Result<String> {
        // Parse the SQL to find exact positions of identifiers
        let tokens = tokenize_sql(sql)?;
        let mut result = sql.to_string();
        let mut offset = 0i32;

        for token in &tokens {
            if token.is_identifier() && token.text.eq_ignore_ascii_case(old) {
                let start = (token.offset as i32 + offset) as usize;
                let end = start + token.text.len();

                result.replace_range(start..end, new);
                offset += new.len() as i32 - old.len() as i32;
            }
        }

        Ok(result)
    }

    fn rename_column_in_sql(&self, sql: &str, old_col: &str, new_col: &str) -> Result<String> {
        // Similar to rename_in_sql but specifically for column names
        // Must be careful to only replace in column contexts
        let tokens = tokenize_sql(sql)?;
        let mut result = sql.to_string();
        let mut offset = 0i32;

        for token in &tokens {
            if token.is_identifier() && token.text.eq_ignore_ascii_case(old_col) {
                let start = (token.offset as i32 + offset) as usize;
                let end = start + token.text.len();

                result.replace_range(start..end, new_col);
                offset += new_col.len() as i32 - old_col.len() as i32;
            }
        }

        Ok(result)
    }

    fn add_column_to_sql(&self, sql: &str, column: &ColumnDef) -> Result<String> {
        // Find the closing paren of CREATE TABLE
        let col_sql = column.to_sql();

        // Insert before final ')'
        if let Some(pos) = sql.rfind(')') {
            let mut result = sql.to_string();
            result.insert_str(pos, &format!(", {}", col_sql));
            Ok(result)
        } else {
            Err(Error::with_message(ErrorCode::Error, "malformed CREATE TABLE"))
        }
    }
}
```

## Acceptance Criteria
- [ ] ALTER TABLE RENAME TO
- [ ] ALTER TABLE RENAME COLUMN
- [ ] ALTER TABLE ADD COLUMN
- [ ] ALTER TABLE DROP COLUMN
- [ ] Update CREATE TABLE SQL in sqlite_master
- [ ] Update triggers referencing table/column
- [ ] Update views referencing table/column
- [ ] Update indexes on renamed columns
- [ ] Update foreign keys
- [ ] Constraint validation for ADD COLUMN
- [ ] Table rebuild for DROP COLUMN
- [ ] Error handling for invalid operations
