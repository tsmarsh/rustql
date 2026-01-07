# Wire VDBE Cursor Ops to B-tree

## Problem
SELECT queries hang indefinitely because VDBE cursor opcodes are **stub implementations** that don't actually interact with the B-tree.

Current code in `src/vdbe/engine.rs`:
```rust
Opcode::Rewind => {
    // Move cursor to first row, jump to P2 if empty
    if let Some(cursor) = self.cursor_mut(op.p1) {
        / Placeholder: In real implementation, this would call btree
        cursor.state = CursorState::AtEnd; // Assume empty for now
    }
    // BUG: Should jump to P2 if empty, but doesn't!
}

Opcode::Next => {
    // Move cursor to next row, jump to P2 if done
    if let Some(cursor) = self.cursor_mut(op.p1) {
        / Placeholder: In real implementation, this would call btree
        cursor.state = CursorState::AtEnd;
    }
    // BUG: Should jump to P2 for next row, but doesn't!
}
```

## Impact
- **All SELECT queries hang** - The test suite shows 20,875 test cases cannot run
- CREATE TABLE and INSERT work (don't need cursor iteration)
- This is the #1 blocker for SQLite compatibility testing

## Progress

### Completed
- [x] Added `btree: Option<Arc<Btree>>` to `DbInfo` struct
- [x] Created `StubVfs` implementation for `types::Vfs` trait
- [x] Modified `sqlite3_open_v2` to create Btree on database open
- [x] Updated `sqlite3_close` to clean up btree

### Remaining
- [ ] Wire CREATE TABLE to register schema and create btree root pages
- [ ] Wire INSERT to encode records and write to btree
- [ ] Wire OpenRead/OpenWrite to create real BtreeCursor from connection
- [ ] Wire Rewind to use BtCursor::first()
- [ ] Wire Next to use BtCursor::next()
- [ ] Wire Column to decode record data
- [ ] Wire MakeRecord to encode column data

## Architecture Notes

The B-tree cursor API exists in `src/storage/btree.rs`:
- `BtCursor::first() -> Result<bool>` - Move to first row
- `BtCursor::next(flags) -> Result<()>` - Move to next row
- `BtCursor.state` - CursorState::Valid when positioned
- `BtCursor.info` - CellInfo with payload data

The VDBE needs to:
1. Get the Btree from the connection when opening a cursor
2. Create a BtCursor via `Btree::cursor(root_page, flags, key_info)`
3. Use BtCursor methods for navigation (first, next, previous)
4. Decode record data from `BtCursor.info.payload`

## Required Changes

### 1. Wire `Rewind` to B-tree
```rust
Opcode::Rewind => {
    let cursor = self.cursor_mut(op.p1)?;
    let has_row = cursor.btree_cursor.first()?; // Move to first row
    if !has_row {
        self.pc = op.p2; // Jump to end if empty
    }
}
```

### 2. Wire `Next` to B-tree
```rust
Opcode::Next => {
    let cursor = self.cursor_mut(op.p1)?;
    let has_row = cursor.btree_cursor.next()?;
    if has_row {
        self.pc = op.p2; // Jump back to loop start
    }
    // Fall through to end if no more rows
}
```

### 3. Wire `Column` to read actual data
```rust
Opcode::Column => {
    let cursor = self.cursor(op.p1)?;
    let value = cursor.btree_cursor.column(op.p2)?;
    self.mem_set(op.p3, value);
}
```

### 4. Wire `OpenRead`/`OpenWrite` to B-tree
The cursor should get a real BtreeCursor from the pager/btree layer.

## Files to Modify
- `src/api/connection.rs` - DbInfo now has btree field (DONE)
- `src/vdbe/engine.rs` - Fix Rewind, Next, Prev, Column, OpenRead, OpenWrite, Close
- `src/vdbe/cursor.rs` - Add BtreeCursor integration (if needed)
- `src/storage/btree.rs` - May need cursor iteration methods
- `src/executor/prepare.rs` - CREATE TABLE needs real bytecode

## Testing
After this fix:
```bash
# Should complete instead of hanging
cargo test --test sqlite_compat_test -- --ignored --nocapture
```

## Acceptance Criteria
- [ ] `Rewind` moves cursor to first row, jumps to P2 if empty
- [ ] `Next` moves to next row, jumps to P2 if more rows exist
- [ ] `Prev` moves to previous row, jumps to P2 if more rows exist
- [ ] `Column` reads actual column data from cursor position
- [ ] `OpenRead`/`OpenWrite` create cursors backed by real B-tree
- [ ] Simple SELECT returns correct data: `SELECT * FROM t1`
- [ ] SQLite compatibility test suite can execute (even if tests fail)
