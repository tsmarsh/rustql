# Implement DeferredSeek And OpenPseudo Alt-Map

## Problem
`OP_DeferredSeek` is missing and `OP_OpenPseudo` lacks alt-cursor mapping and deferred seek integration. SQLite uses deferred seek to avoid unnecessary table reads and maps columns from index to table cursor.

## SQLite References
- `sqlite3/src/vdbe.c`: `OP_DeferredSeek` and `OP_FinishSeek`
- `sqlite3/src/update.c` / `sqlite3/src/delete.c`: `OP_FinishSeek` placement
- `sqlite3/src/vdbe.c`: alt-map fields (`aAltMap`, `pAltCursor`, `movetoTarget`, `deferredMoveto`)

## RustQL Targets
- `src/vdbe/engine.rs`: implement `Opcode::DeferredSeek` and extend `OpenPseudo`/`Column` behavior
- `src/vdbe/engine.rs`: extend `VdbeCursor` with `moveto_target`, `deferred_moveto`, alt-map, and alt-cursor references

## Requirements
- `DeferredSeek` should set table cursor deferred state using index cursor rowid.
- `FinishSeek` must complete deferred movement using stored target.
- `Column` should redirect reads through the alt-map when deferred seek is active (match SQLite’s alt-map behavior).
- Ensure correct behavior for null rows and ephemeral cursors.

## Tests
- Unit tests that join through an index with deferred seek, verifying that table cursor isn’t moved until first column read.
- Tcl coverage to target:
  - `sqlite3/test/where*.test`
  - `sqlite3/test/update*.test`
  - `sqlite3/test/delete*.test`

## Notes
- Keep control flow aligned with SQLite; avoid refactors.
