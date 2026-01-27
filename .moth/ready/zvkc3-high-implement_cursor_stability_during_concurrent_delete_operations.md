# Implement Cursor Stability During Concurrent DELETE Operations

## Problem

When a DELETE statement executes while another statement is actively iterating
over the same table (via a cursor), the iteration should continue correctly
without corruption or spurious errors. Currently, RustQL returns empty results
in these scenarios.

## Failing Tests

- `delete-9.2`, `delete-9.3`, `delete-9.4`, `delete-9.5` in `sqlite3/test/delete.test`

## Test Case Analysis

```sql
-- Setup
CREATE TABLE t5(a, b);
CREATE TABLE t6(c, d);
INSERT INTO t5 VALUES(1, 2), (3, 4), (5, 6);
INSERT INTO t6 VALUES('a', 'b'), ('c', 'd');
CREATE INDEX i5 ON t5(a);
CREATE INDEX i6 ON t6(c);

-- delete-9.2: DELETE all rows while iterating
-- TCL: db eval { SELECT t5.rowid AS r, c, d FROM t5, t6 ORDER BY a } {
--        if {$r==2} { db eval { DELETE FROM t5 } }
--        lappend res $r $c $d
--      }
-- Expected: {1 a b 1 c d 2 a b {} c d}
-- The iteration should continue even after DELETE removes all rows from t5

-- delete-9.3: DELETE single row (rowid=2) while iterating
-- Expected: {1 a b 1 c d 2 a b {} c d 3 a b 3 c d}

-- delete-9.4: DELETE different row (rowid=1) while iterating at rowid=2
-- Expected: {1 a b 1 c d 2 a b 2 c d 3 a b 3 c d}

-- delete-9.5: DELETE future row (rowid=3) while iterating at rowid=2
-- Expected: {1 a b 1 c d 2 a b 2 c d}
```

## Root Cause

The VDBE cursor implementation doesn't handle the case where rows are deleted
from a table while another cursor is actively iterating over it. When DELETE
removes rows:

1. The B-tree structure may change (page splits, merges)
2. The iterating cursor's position may become invalid
3. SQLite uses "deferred seek" and cursor hints to maintain stability

## SQLite's Approach

SQLite handles this through several mechanisms:

1. **Cursor position saving**: Before any write operation, cursors save their
   position (key + rowid). After the write, they restore position.

2. **Statement journals**: Modifications are tracked so cursors can see a
   consistent view.

3. **BTREE_FORDELETE hint**: Cursors opened for iteration during DELETE get
   special treatment to maintain stability.

4. **OP_IfSmaller optimization**: SQLite checks if the table has shrunk and
   adjusts iteration accordingly.

## Implementation Plan

### Phase 1: Cursor Position Saving

In `src/storage/btree/cursor.rs`:

```rust
impl BtCursor {
    /// Save cursor position before write operations
    pub fn save_position(&mut self) -> Result<()> {
        if self.state == CursorState::Valid {
            self.saved_key = self.current_key();
            self.saved_rowid = self.current_rowid();
            self.position_saved = true;
        }
        Ok(())
    }

    /// Restore cursor position after write operations
    pub fn restore_position(&mut self) -> Result<bool> {
        if !self.position_saved {
            return Ok(true);
        }
        // Seek to saved position, return false if exact match not found
        self.seek_to_saved()?;
        self.position_saved = false;
        Ok(self.state == CursorState::Valid)
    }
}
```

### Phase 2: Write Operation Coordination

In `src/vdbe/engine/mod.rs`, before Delete opcode:

```rust
// Save positions of all read cursors on the same table
fn save_table_cursors(&mut self, table_root: Pgno) {
    for cursor in &mut self.cursors {
        if let Some(c) = cursor {
            if c.root_page == table_root && !c.is_write {
                c.btree_cursor.as_mut().map(|bc| bc.save_position());
            }
        }
    }
}

// After Delete, restore positions
fn restore_table_cursors(&mut self, table_root: Pgno) {
    for cursor in &mut self.cursors {
        if let Some(c) = cursor {
            if c.root_page == table_root && !c.is_write {
                c.btree_cursor.as_mut().map(|bc| bc.restore_position());
            }
        }
    }
}
```

### Phase 3: Index Cursor Stability

For index-based iteration (ORDER BY uses index):
- Save the index key being iterated
- After DELETE, re-seek to >= saved key
- Handle case where saved key was deleted (move to next)

## Files to Modify

1. `src/storage/btree/cursor.rs` - Add position saving/restoring
2. `src/storage/btree/mod.rs` - Coordinate cursor saves during writes
3. `src/vdbe/engine/mod.rs` - Call save/restore around Delete opcode
4. `src/vdbe/cursor.rs` - Add saved position fields to VdbeCursor

## Testing

```bash
make test-delete  # Should pass delete-9.2 through delete-9.5
```

## References

- SQLite source: `btree.c` functions `saveCursorPosition()`, `restoreCursorPosition()`
- SQLite source: `vdbe.c` OP_Delete handling of BTREE_FORDELETE
- SQLite docs: https://sqlite.org/isolation.html
