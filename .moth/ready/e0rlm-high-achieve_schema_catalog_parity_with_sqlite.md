# Epic: Schema & Catalog Parity

## Intent
Match SQLite's schema modeling and catalog behavior, including sqlite_master
contents, schema cookies, and DDL semantics across temp and attached schemas.

## Architectural Alignment
- DDL should update schema and runtime behavior exactly like SQLite.
- Schema registry is authoritative and consistent with sqlite_master.

## Scope
- Full DDL modeling (CREATE/DROP/ALTER).
- sqlite_master/sqlite_schema content and ordering.
- Schema cookies, temp schema, and attached database behavior.

## Out of Scope
- Non-SQLite schema extensions.

## Acceptance Criteria (How We Assert This Is Done)
- sqlite_master/sqlite_schema contents match SQLite for:
  - table/index/view definitions,
  - rootpage values,
  - SQL text normalization where SQLite does so.
- SQLite Tcl tests listed in this epic pass in RustQL, with no untracked
  exclusions.
- Schema cookie behavior matches SQLite across transaction boundaries.
- `docs/differences.md` updated to remove schema-related gaps when parity is met.

## Test Targets (SQLite Tcl)
Run each with `testfixture test/<name>.test`:
- `sqlite3/test/schema*.test`
- `sqlite3/test/alter*.test`
- `sqlite3/test/attach*.test`
- `sqlite3/test/pragma*.test`
- `sqlite3/test/createtab.test`
- `sqlite3/test/altertab*.test`
- `sqlite3/test/altercol.test`
- `sqlite3/test/alterdropcol.test`
- `sqlite3/test/alterdropcol2.test`
- `sqlite3/test/altertrig.test`
- `sqlite3/test/e_createtable.test`
- `sqlite3/test/e_resolve.test`

## Verification Steps
- Run schema and pragma Tcl suites.
- Compare sqlite_master rows between RustQL and SQLite for a curated DDL set.
- Validate schema cookie changes with transaction tests.
