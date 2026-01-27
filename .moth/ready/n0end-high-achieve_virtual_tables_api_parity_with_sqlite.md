# Epic: Virtual Tables (SQLite API Parity)

## Intent
Bring RustQL's virtual table (vtab) architecture into exact shape with SQLite's
module registry and x* lifecycle, removing bespoke dispatch paths and ensuring
all vtabs execute through the same VDBE-driven contract.

## Architectural Alignment
- Match SQLite's vtab model: module registry + per-table instance + cursor.
- VDBE and executor call into vtab methods rather than module-specific branches.
- Preserve SQLite's observable behavior and error semantics.

## Scope
- Vtab module registry (create, connect, destroy, disconnect).
- Vtab lifecycle and cursor methods (xBestIndex, xFilter, xNext, xColumn,
  xRowid, xUpdate, xRename, xBegin, xSync, xCommit, xRollback).
- VDBE opcodes and executor wiring aligned to SQLite's vtab execution flow.
- Removal of fts3/fts5 special-casing outside module implementation code.

## Out of Scope
- Feature extensions beyond SQLite's API.
- New module types not present in SQLite.

## Acceptance Criteria (How We Assert This Is Done)
- Vtab API shape matches SQLite: implemented method set and call ordering
  consistent with `sqlite3/ext/` and `sqlite3/src/vtab.c`.
- No module-specific branches in VDBE or executor for fts3/fts5:
  `rg "fts3|fts5" src/vdbe src/executor` only hits module registries or
  vtab implementations (allowlist documented in this epic).
- `docs/architecture.md` and `docs/differences.md` updated to reflect the
  SQLite-shaped vtab pipeline and current parity status.
- SQLite Tcl tests listed in this epic pass in RustQL, with no untracked
  exclusions.
- New or updated RustQL integration tests exercise:
  - xBestIndex constraint handling,
  - cursor lifecycle (xFilter/xNext/xColumn/xRowid),
  - transactional hooks (xBegin/xSync/xCommit/xRollback),
  - error propagation equivalence.

## Test Targets (SQLite Tcl)
Run each with `testfixture test/<name>.test`:
- `sqlite3/test/vtab1.test`
- `sqlite3/test/vtab2.test`
- `sqlite3/test/vtab3.test`
- `sqlite3/test/vtab4.test`
- `sqlite3/test/vtab5.test`
- `sqlite3/test/vtab6.test`
- `sqlite3/test/vtab7.test`
- `sqlite3/test/vtab8.test`
- `sqlite3/test/vtab9.test`
- `sqlite3/test/vtabA.test`
- `sqlite3/test/vtabB.test`
- `sqlite3/test/vtabC.test`
- `sqlite3/test/vtabD.test`
- `sqlite3/test/vtabE.test`
- `sqlite3/test/vtabF.test`
- `sqlite3/test/vtabH.test`
- `sqlite3/test/vtabI.test`
- `sqlite3/test/vtabJ.test`
- `sqlite3/test/vtabK.test`
- `sqlite3/test/vtabL.test`
- `sqlite3/test/vtab_alter.test`
- `sqlite3/test/vtabdistinct.test`
- `sqlite3/test/vtabdrop.test`
- `sqlite3/test/vtab_err.test`
- `sqlite3/test/vtabrhs1.test`
- `sqlite3/test/vtab_shared.test`
- `sqlite3/test/bestindex1.test`
- `sqlite3/test/bestindex2.test`
- `sqlite3/test/bestindex3.test`
- `sqlite3/test/bestindex4.test`
- `sqlite3/test/bestindex5.test`
- `sqlite3/test/bestindex6.test`
- `sqlite3/test/bestindex7.test`
- `sqlite3/test/bestindex8.test`
- `sqlite3/test/bestindex9.test`
- `sqlite3/test/bestindexA.test`
- `sqlite3/test/bestindexB.test`
- `sqlite3/test/bestindexC.test`
- `sqlite3/test/bestindexD.test`
- `sqlite3/test/bestindexE.test`

## Assessment Patterns
- No `fts3`/`fts5` branches in `src/vdbe` or `src/executor` other than vtab
  registry or vtab module implementation files.

## Verification Steps
- Run the vtab-focused Tcl tests and validate parity with SQLite output.
- Confirm there are no remaining module-specific branches outside vtab impls.
- Review VDBE opcode flow to ensure vtab execution uses the generic dispatch.
