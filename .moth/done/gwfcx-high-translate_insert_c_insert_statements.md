# Translate insert.c - INSERT Statements

## Overview
Translate INSERT statement compilation including INSERT...VALUES, INSERT...SELECT, and INSERT...DEFAULT VALUES.

## Source Reference
- `sqlite3/src/insert.c` - 3,393 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Functions

### INSERT Compilation
- `sqlite3Insert()` - Main INSERT compiler
- `autoIncBegin()` - Initialize autoincrement
- `autoIncEnd()` - Finalize autoincrement
- `xferOptimization()` - INSERT...SELECT optimization

### Code Generation
```rust
impl<'a> Parse<'a> {
    /// Compile INSERT statement
    pub fn compile_insert(&mut self, insert: &InsertStmt) -> Result<()> {
        // Find table
        let table = self.find_table(&insert.table.name, insert.table.database.as_deref())?
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", insert.table.name)
            ))?;

        // Open table cursor for writing
        let cursor = self.alloc_cursor();
        self.add_op(Opcode::OpenWrite, cursor, table.root_page as i32, table.columns.len() as i32);

        // Handle different INSERT sources
        match &insert.source {
            InsertSource::Values(rows) => {
                self.compile_insert_values(&table, cursor, &insert.columns, rows)?;
            }
            InsertSource::Select(select) => {
                self.compile_insert_select(&table, cursor, &insert.columns, select)?;
            }
            InsertSource::DefaultValues => {
                self.compile_insert_defaults(&table, cursor)?;
            }
        }

        // Handle RETURNING if present
        if let Some(returning) = &insert.returning {
            self.compile_returning(returning)?;
        }

        Ok(())
    }

    fn compile_insert_values(
        &mut self,
        table: &Table,
        cursor: i32,
        columns: &Option<Vec<String>>,
        rows: &[Vec<Expr>],
    ) -> Result<()> {
        // Map columns to indices
        let col_map = self.map_insert_columns(table, columns)?;

        for row in rows {
            // Generate new rowid
            let rowid_reg = self.alloc_mem();
            if table.autoincrement {
                self.add_op(Opcode::NewRowid, cursor, rowid_reg, 0);
            } else {
                self.add_op(Opcode::Null, 0, rowid_reg, 0);
            }

            // Evaluate and store each column value
            let data_reg = self.alloc_mem_n(table.columns.len() as i32);
            for (i, col) in table.columns.iter().enumerate() {
                let dest = data_reg + i as i32;
                if let Some(val_idx) = col_map.get(i) {
                    // Value provided
                    self.compile_expr_target(&row[*val_idx], dest)?;
                } else if let Some(default) = &col.default_value {
                    // Use default
                    self.compile_default_value(default, dest)?;
                } else {
                    // NULL
                    self.add_op(Opcode::Null, 0, dest, 0);
                }
            }

            // Check constraints
            self.check_constraints(table, data_reg)?;

            // Build record and insert
            let record_reg = self.alloc_mem();
            self.add_op(Opcode::MakeRecord, data_reg, table.columns.len() as i32, record_reg);
            self.add_op(Opcode::Insert, cursor, record_reg, rowid_reg);
        }

        Ok(())
    }
}
```

### Conflict Handling
```rust
impl<'a> Parse<'a> {
    fn compile_conflict_action(&mut self, action: ConflictAction) -> Result<()> {
        match action {
            ConflictAction::Abort => {
                // Default - abort transaction on conflict
            }
            ConflictAction::Rollback => {
                // Rollback entire transaction
            }
            ConflictAction::Fail => {
                // Fail but keep prior changes
            }
            ConflictAction::Ignore => {
                // Skip conflicting row
            }
            ConflictAction::Replace => {
                // Delete existing, insert new
            }
        }
        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] INSERT INTO...VALUES
- [ ] INSERT INTO...SELECT
- [ ] INSERT INTO...DEFAULT VALUES
- [ ] Column list handling
- [ ] Default value insertion
- [ ] AUTOINCREMENT handling
- [ ] Constraint checking (NOT NULL, UNIQUE, CHECK)
- [ ] Conflict handling (OR REPLACE, etc.)
- [ ] Foreign key checking
- [ ] RETURNING clause
- [ ] Trigger invocation
