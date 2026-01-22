# Fix WAL shared memory and multi-connection read slots

## Problem
Our WAL shared memory (`WalShm`) is an in-process `Vec<u8>` and read locks always use slot 0. This diverges from SQLite’s shared WAL-index, which allows multiple readers and a writer to coexist across connections/processes.

Consequences:
- Multiple readers cannot coexist correctly.
- A writer can overwrite a reader’s snapshot because only one read mark is tracked.
- Busy/lock behavior does not match SQLite.

Code refs:
- `src/storage/wal.rs:370` (WalShm in-process only)
- `src/storage/wal.rs:609` (read lock always slot 0)

## SQLite Behavior
SQLite uses shared-memory WAL-index with up to `WAL_NREADER` read slots, with OS-level locks for each slot. Each connection picks a free slot and records its snapshot in `read_marks`.

## Expected Fix
- Implement shared memory regions backed by the VFS shared-memory API (`xShmMap`, `xShmLock`, etc.).
- Implement read-slot acquisition, contention, and release logic per SQLite.
- Track multiple read marks and preserve snapshots during write transactions.

## Concrete Tests (Tcl)
Add a test in `sqlite3/test/wal_multireader.test`:

```tcl
sqlite3 db1 test.db
sqlite3 db2 test.db
sqlite3 db3 test.db

execsql {PRAGMA journal_mode=WAL;} db1
execsql {CREATE TABLE t(x); INSERT INTO t VALUES(1);} db1

# db2 starts a read transaction and holds snapshot
execsql {BEGIN; SELECT count(*) FROM t;} db2

# db1 writes new row and commits
execsql {BEGIN; INSERT INTO t VALUES(2); COMMIT;} db1

# db2 should still see snapshot (count=1)
set c2 [execsql {SELECT count(*) FROM t;} db2]

# db3 should see latest (count=2)
set c3 [execsql {SELECT count(*) FROM t;} db3]

# Expect: c2==1, c3==2
```

## Success Criteria
- Multiple concurrent readers get stable snapshots.
- Writers do not corrupt or overwrite reader snapshots.
- WAL read slots behave like SQLite (`WAL_NREADER`).
