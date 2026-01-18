# VDBE: Validate table existence in OpenRead/OpenWrite

## Problem
OpenRead/OpenWrite opcodes don't validate that the table exists before creating a cursor. This causes "database disk image is malformed" (Corrupt) errors instead of proper "no such table: X" errors.

## SQLite Reference
- `sqlite3/src/vdbe.c`: OP_OpenRead/OP_OpenWrite check schema before opening

## RustQL Location
- `src/vdbe/engine.rs`: OpenRead/OpenWrite opcode handlers

## Impact
This single issue causes ~20% of TCL test failures. Tests expect:
```
1 {no such table: test1}
```
But get:
```
0 {}  (with "Corrupt" error on stderr)
```

## Required Changes
1. Before creating BtCursor, verify table exists in schema
2. Return proper ErrorCode::Error with "no such table: X" message
3. Handle both regular tables and sqlite_master specially

## Tests
- `sqlite3/test/select1.test` (select1-1.1)
- `sqlite3/test/insert.test` (insert-1.1)
- `sqlite3/test/delete.test` (delete-1.1)
