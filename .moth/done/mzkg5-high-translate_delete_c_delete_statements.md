# Translate delete.c - DELETE Statements

## Overview
Translate DELETE statement compilation including WHERE filtering, trigger handling, and truncate optimization.

## Source Reference
- `sqlite3/src/delete.c` - 1,030 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Functions

### DELETE Compilation
- `sqlite3DeleteFrom()` - Main DELETE compiler
- Truncate optimization (DELETE without WHERE)
- Index cleanup for deleted rows

### Code Generation
```rust
impl<'a> Parse<'a> {
    /// Compile DELETE statement
    pub fn compile_delete(&mut self, delete: &DeleteStmt) -> Result<()> {
        // Find table
        let table = self.find_table(&delete.table.name, None)?
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", delete.table.name)
            ))?;

        // Check for truncate optimization
        // DELETE without WHERE and no triggers
        if delete.where_clause.is_none() &&
           !self.has_triggers(&table, TriggerType::Delete) {
            return self.compile_truncate(&table);
        }

        // Open table cursor for writing
        let cursor = self.alloc_cursor();
        self.add_op(Opcode::OpenWrite, cursor, table.root_page as i32, table.columns.len() as i32);

        // Also open index cursors
        let index_cursors = self.open_index_cursors(&table)?;

        // Generate WHERE loop (or full scan)
        let where_info = if let Some(ref where_expr) = delete.where_clause {
            self.generate_where_begin(&[cursor], where_expr)?
        } else {
            self.add_op(Opcode::Rewind, cursor, 0, 0);
            WhereInfo::full_scan()
        };

        // Get rowid
        let rowid_reg = self.alloc_mem();
        self.add_op(Opcode::Rowid, cursor, rowid_reg, 0);

        // Fire BEFORE DELETE triggers
        if self.has_triggers(&table, TriggerType::BeforeDelete) {
            self.fire_triggers(&table, TriggerType::BeforeDelete)?;
        }

        // Delete from indexes
        for (idx_cursor, index) in index_cursors.iter() {
            self.add_op(Opcode::IdxDelete, *idx_cursor, rowid_reg, 0);
        }

        // Delete from main table
        self.add_op(Opcode::Delete, cursor, 0, 0);

        // Fire AFTER DELETE triggers
        if self.has_triggers(&table, TriggerType::AfterDelete) {
            self.fire_triggers(&table, TriggerType::AfterDelete)?;
        }

        // Update change counter
        self.add_op(Opcode::AddChangeCount, 1, 0, 0);

        // Continue loop
        self.add_op(Opcode::Next, cursor, where_info.addr_first, 0);

        // RETURNING clause
        if let Some(returning) = &delete.returning {
            self.compile_returning(returning)?;
        }

        Ok(())
    }

    fn compile_truncate(&mut self, table: &Table) -> Result<()> {
        // Fast path: clear entire table
        self.add_op(Opcode::Clear, table.root_page as i32, 0, 0);

        // Clear indexes too
        for index in &table.indexes {
            self.add_op(Opcode::Clear, index.root_page as i32, 0, 0);
        }

        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] DELETE FROM...WHERE
- [ ] DELETE FROM (all rows)
- [ ] Truncate optimization
- [ ] Index entry deletion
- [ ] Trigger handling (BEFORE/AFTER DELETE)
- [ ] RETURNING clause
- [ ] Foreign key enforcement
- [ ] Change counter updates
- [ ] LIMIT clause (SQLite extension)
