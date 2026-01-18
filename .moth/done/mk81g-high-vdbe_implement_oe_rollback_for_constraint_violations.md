# VDBE: Implement OE_ROLLBACK for constraint violations

## Problem
When a constraint violation occurs with ON CONFLICT ROLLBACK, the VDBE logs a TODO but doesn't actually rollback the transaction.

## SQLite Reference
- `sqlite3/src/vdbe.c`: OP_Halt with OE_ROLLBACK calls `sqlite3RollbackAll()`

## RustQL Location
- `src/vdbe/engine.rs:2866` - Contains TODO comment

## Current Code
```rust
OE_ROLLBACK => {
    // TODO: Actually rollback the transaction
}
```

## Required Changes
1. Call transaction rollback on the btree/pager
2. Reset all cursor positions
3. Clear any pending changes
4. Return appropriate error code

## Tests
- `sqlite3/test/conflict.test`
- `sqlite3/test/rollback.test`
