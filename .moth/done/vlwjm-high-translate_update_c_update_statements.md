# Translate update.c - UPDATE Statements

## Overview
Translate UPDATE statement compilation including SET clause, WHERE filtering, and constraint checking.

## Source Reference
- `sqlite3/src/update.c` - 1,362 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Functions

### UPDATE Compilation
- `sqlite3Update()` - Main UPDATE compiler
- Update one-pass vs two-pass strategies
- Index updates when indexed columns change

### Code Generation
```rust
impl<'a> Parse<'a> {
    /// Compile UPDATE statement
    pub fn compile_update(&mut self, update: &UpdateStmt) -> Result<()> {
        // Find table
        let table = self.find_table(&update.table.name, None)?
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", update.table.name)
            ))?;

        // Open table cursor
        let cursor = self.alloc_cursor();
        self.add_op(Opcode::OpenWrite, cursor, table.root_page as i32, table.columns.len() as i32);

        // Determine which columns are updated
        let updated_cols = self.map_update_columns(&table, &update.set)?;

        // Check if one-pass update is possible
        // (when rowid doesn't change and no index on updated columns)
        let one_pass = self.can_use_one_pass(&table, &updated_cols);

        if one_pass {
            self.compile_update_one_pass(&table, cursor, update, &updated_cols)?;
        } else {
            self.compile_update_two_pass(&table, cursor, update, &updated_cols)?;
        }

        Ok(())
    }

    fn compile_update_one_pass(
        &mut self,
        table: &Table,
        cursor: i32,
        update: &UpdateStmt,
        updated_cols: &HashSet<usize>,
    ) -> Result<()> {
        // Generate WHERE loop
        let where_info = if let Some(ref where_expr) = update.where_clause {
            self.generate_where_begin(&[cursor], where_expr)?
        } else {
            // Full table scan
            self.add_op(Opcode::Rewind, cursor, 0, 0);
            WhereInfo::full_scan()
        };

        // Read current row
        let old_rowid = self.alloc_mem();
        self.add_op(Opcode::Rowid, cursor, old_rowid, 0);

        // Evaluate new values
        let data_reg = self.alloc_mem_n(table.columns.len() as i32);
        for (i, col) in table.columns.iter().enumerate() {
            let dest = data_reg + i as i32;
            if let Some(expr) = update.set.get(&col.name) {
                // Column is being updated
                self.compile_expr_target(expr, dest)?;
            } else {
                // Keep existing value
                self.add_op(Opcode::Column, cursor, i as i32, dest);
            }
        }

        // Check constraints
        self.check_constraints(table, data_reg)?;

        // Build new record and update
        let record_reg = self.alloc_mem();
        self.add_op(Opcode::MakeRecord, data_reg, table.columns.len() as i32, record_reg);
        self.add_op(Opcode::Insert, cursor, record_reg, old_rowid);

        // Continue loop
        self.add_op(Opcode::Next, cursor, where_info.addr_first, 0);

        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] UPDATE...SET with expressions
- [ ] WHERE clause filtering
- [ ] One-pass optimization
- [ ] Two-pass for complex cases
- [ ] Index updates
- [ ] Constraint checking
- [ ] Conflict handling
- [ ] RETURNING clause
- [ ] Trigger invocation (BEFORE/AFTER UPDATE)
- [ ] Foreign key handling
