# Support Multi-DB Transactions In VDBE

## Problem
`OP_Transaction` only supports `P1==0` (main DB) and ignores temp/attached DBs. SQLite supports per-db transactions and proper btree masks.

## SQLite References
- `sqlite3/src/vdbe.c`: `OP_Transaction`
- `sqlite3/src/btree.c`: transaction handling per-db
- `sqlite3/src/attach.c`: attached DB behavior

## RustQL Targets
- `src/vdbe/engine.rs`: `Opcode::Transaction`
- `src/api/connection.rs` and schema state for multi-db

## Requirements
- Support `P1` database index for main/temp/attached DBs.
- Validate db index and btree mask like SQLite.
- Ensure proper state updates for read vs write transactions per db.

## Tests
- Unit tests using temp DB and attached DB: begin read/write transactions and verify db index routing.
- Tcl coverage to target:
  - `sqlite3/test/attach.test`
  - `sqlite3/test/tempdb.test`
  - `sqlite3/test/trans*.test`

