# Implement Cookie Schema Side-Effects

## Problem
`ReadCookie`, `SetCookie`, and `VerifyCookie` are main-db only and do not update schema caches, file format, or expire prepared statements. SQLite updates schema state and invalidates prepared statements when cookies change.

## SQLite References
- `sqlite3/src/vdbe.c`: `OP_ReadCookie`, `OP_SetCookie`, `OP_VerifyCookie`
- `sqlite3/src/build.c`: schema change handling, `sqlite3ExpirePreparedStatements`
- `sqlite3/src/fkey.c`: `sqlite3FkClearTriggerCache`

## RustQL Targets
- `src/vdbe/engine.rs`: cookie opcodes
- `src/schema/mod.rs` and connection state for schema cache/file format
- Statement cache invalidation (if present)

## Requirements
- Support `P1` database index (main and temp) when possible.
- `SetCookie` should update in-memory schema cookie/file format and mark schema change.
- Temp DB cookie change must expire prepared statements.
- `VerifyCookie` should return schema error on mismatch.

## Tests
- Unit tests for `SetCookie` + `VerifyCookie` with schema cookie mismatches.
- Tcl coverage to target:
  - `sqlite3/test/schema.test`
  - `sqlite3/test/pragma.test` (schema_version/user_version)
  - `sqlite3/test/tempdb.test`

## Notes
- Mirror SQLite side-effects; avoid refactors.
