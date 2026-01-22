# Implement WAL index reconstruction on open

## Problem
Our WAL index (hash tables + header) is purely in-memory and not reconstructed from the WAL file when opening a connection. If a connection opens an existing WAL file, it does not rebuild the index, so frames are not visible to readers.

Consequences:
- New connections do not see committed changes that are only in the WAL file.
- WAL recovery after crash or after closing a connection is broken.

Code refs:
- `src/storage/wal.rs:431` (hash tables in-memory only)
- `src/storage/wal.rs:563` (open does not scan WAL file)

## SQLite Behavior
On WAL open, SQLite initializes the WAL-index in shared memory and can reconstruct it by scanning the WAL file if needed (`walIndexRecover`). This ensures readers can see committed frames even if the index was lost.

## Expected Fix
- When opening WAL, validate header + salts and reconstruct the WAL-index by scanning the WAL file if the shared index is uninitialized or stale.
- Populate hash tables and `max_frame` from the WAL file.
- Ensure `find_frame()` works for connections that did not write the WAL.

## Concrete Tests (Tcl)
Add a test in `sqlite3/test/wal_recovery.test`:

```tcl
sqlite3 db1 test.db
execsql {PRAGMA journal_mode=WAL; CREATE TABLE t(x);} db1
execsql {INSERT INTO t VALUES(1);} db1

# Close writer connection without checkpoint
close db1

# New connection should see row via WAL recovery
sqlite3 db2 test.db
set rows [execsql {SELECT x FROM t;} db2]
# Expect: rows == {1}
```

## Success Criteria
- New connections can read committed data present only in WAL file.
- WAL index reconstruction works after process restart.
