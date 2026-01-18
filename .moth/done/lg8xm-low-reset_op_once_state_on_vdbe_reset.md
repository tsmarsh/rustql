# Reset OP_Once State On Vdbe Reset

## Problem
`OP_Once` uses `once_flags` but the set is never cleared in `Vdbe::reset`, so it persists across re-execution, unlike SQLite.

## SQLite References
- `sqlite3/src/vdbe.c`: `OP_Once` and statement re-execution semantics

## RustQL Targets
- `src/vdbe/engine.rs`: clear `once_flags` in `Vdbe::reset`

## Requirements
- `once_flags` must be cleared on reset and when a new program begins.

## Tests
- Unit test: execute a VDBE with `OP_Once`, reset VM, and ensure `OP_Once` triggers again.
- Tcl coverage to target:
  - `sqlite3/test/once.test` (if present)
  - otherwise `sqlite3/test/pragma.test` or `sqlite3/test/select*.test` that reuse statements

