# Epic: Storage Engine Fidelity (Btree/Pager/WAL)

## Intent
Align RustQL's storage engine with SQLite's observable behavior for btree,
pager, and WAL subsystems, including crash recovery, locking, and file format
compatibility.

## Architectural Alignment
- Keep SQLite file format and recovery semantics.
- Preserve SQLite's performance intent and error behavior.

## Scope
- Btree operations and page formats.
- Pager cache behavior, journaling, and WAL integration.
- Locking, recovery, and durability semantics.

## Out of Scope
- New storage formats or non-SQLite durability models.

## Acceptance Criteria (How We Assert This Is Done)
- RustQL databases are readable by SQLite and vice versa:
  - Create in RustQL, open in SQLite, `PRAGMA integrity_check` passes.
  - Create in SQLite, open in RustQL, `PRAGMA integrity_check` passes.
- SQLite Tcl tests listed in this epic pass in RustQL, with no untracked
  exclusions.
- Crash recovery behavior matches SQLite for a defined suite of WAL/pager
  failure tests (documented in RustQL integration tests).
- `docs/btree.md` updated to reflect the final storage behavior.

## Test Targets (SQLite Tcl)
Run each with `testfixture test/<name>.test`:
- `sqlite3/test/btree*.test`
- `sqlite3/test/btreefault.test`
- `sqlite3/test/pager*.test`
- `sqlite3/test/wal*.test`
- `sqlite3/test/journal*.test`
- `sqlite3/test/corrupt*.test`
- `sqlite3/test/crash*.test`
- `sqlite3/test/autovacuum*.test`
- `sqlite3/test/backup*.test`
- `sqlite3/test/atomic*.test`
- `sqlite3/test/e_wal.test`
- `sqlite3/test/e_walauto.test`
- `sqlite3/test/e_walckpt.test`
- `sqlite3/test/e_walhook.test`
- `sqlite3/test/e_vacuum.test`

## Verification Steps
- Run SQLite storage-related Tcl suites.
- Execute cross-compatibility checks with SQLite CLI.
- Validate recovery by running WAL/journal tests under forced interrupts.
