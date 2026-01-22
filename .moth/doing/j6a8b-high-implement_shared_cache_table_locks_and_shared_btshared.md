# Implement shared-cache table locks and shared BtShared

## Problem
Our B-tree layer uses `Arc<RwLock<BtShared>>` per connection, but there is no global shared-cache list or table-lock tracking. SQLite supports shared cache with per-table locks and shared schema cache; our implementation effectively ignores shared-cache semantics.

Consequences:
- `sqlite3_enable_shared_cache()` / shared-cache PRAGMAs behave as no-ops.
- Tests that rely on shared-cache table locks or shared schema invalidation will diverge from SQLite.

Code refs:
- `src/storage/btree.rs:157` (BtShared per-connection only)
- SQLite reference: `sqlite3/src/btree.c` shared-cache list and lock functions.

## SQLite Behavior
When shared cache is enabled:
- Connections share a single `BtShared` instance per database file.
- Table-level locks prevent conflicting schema/data writes.
- Schema changes propagate across connections via schema cookie/data-version.

## Expected Fix
- Implement a global shared-cache registry keyed by database path + VFS.
- Add table-level lock tracking (equivalent to `BtLock` and `pLock` list in SQLite).
- Support shared-cache enable/disable and expose proper locking errors.

## Concrete Tests (Tcl)
Add tests in `sqlite3/test/shared_cache.test` (new) or extend `shared.test`:

```tcl
# Enable shared cache for this process
sqlite3_enable_shared_cache 1

sqlite3 db1 test.db
sqlite3 db2 test.db

# Schema change in db1 should be visible in db2 without reopen
execsql {CREATE TABLE t(x);} db1
set tables [execsql {SELECT name FROM sqlite_master WHERE type='table' AND name='t';} db2]
# Expect: t is visible

# Table-level locking: write lock in db1 blocks db2 write
execsql {BEGIN IMMEDIATE; INSERT INTO t VALUES(1);} db1
set rc [catch {execsql {INSERT INTO t VALUES(2);} db2} msg]
# Expect: rc==1 and msg contains "database table is locked"

# Cleanup
execsql {ROLLBACK;} db1
```

## Success Criteria
- Shared-cache PRAGMAs are honored.
- Schema changes are visible across shared-cache connections without reopening.
- Conflicting writes return table-lock errors consistent with SQLite.
