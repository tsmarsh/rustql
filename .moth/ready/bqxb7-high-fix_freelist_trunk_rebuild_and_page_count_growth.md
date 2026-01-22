# Fix freelist trunk rebuild and page_count growth

## Problem
Our freelist persistence rebuilds the trunk chain by allocating **new trunk pages at EOF on every commit** whenever `free_pages` is non-empty. This grows the database file even when all freed pages could be reused.

Current behavior:
- `save_freelist()` allocates fresh trunk pages at end-of-file and updates header.
- Existing trunk pages are never reused, so repeated insert/delete cycles increase `PRAGMA page_count`.

Code refs:
- `src/storage/btree.rs:1393` (rebuild freelist)
- `src/storage/btree.rs:1417` (allocates new trunk pages)

## SQLite Behavior
SQLite updates freelist trunk/leaf pages in-place, reusing existing trunk pages when possible. Repeated insert/delete cycles do **not** grow the file unless the logical high-water mark changes.

## Expected Fix
- Reuse existing trunk pages when rebuilding the freelist.
- Only allocate new trunk pages if there are more trunk pages required than currently available.
- Ensure `BTREE_FREE_PAGE_COUNT` matches trunk+leaf counts without inflating `page_count`.
- Keep freelist header offsets (32/36) consistent with SQLite.

## Concrete Test (Tcl)
Add a new test to `sqlite3/test/freelist2.test` (or extend `freelist.test`) that ensures page_count does not grow across cycles:

```tcl
reset_db
execsql {PRAGMA page_size=1024; PRAGMA auto_vacuum=0;}
execsql {CREATE TABLE t(x);}

# First cycle: allocate pages, then free them
execsql {BEGIN; WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<2000)
          INSERT INTO t SELECT zeroblob(800) FROM c; COMMIT;}
execsql {DELETE FROM t;}
set pc1 [execsql {PRAGMA page_count;}]
set fc1 [execsql {PRAGMA freelist_count;}]

# Second cycle should not increase page_count
execsql {BEGIN; WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<2000)
          INSERT INTO t SELECT zeroblob(800) FROM c; COMMIT;}
execsql {DELETE FROM t;}
set pc2 [execsql {PRAGMA page_count;}]
set fc2 [execsql {PRAGMA freelist_count;}]

# Expectations
# - pc2 == pc1 (no file growth)
# - freelist_count remains consistent
set ::test_results [list $pc1 $pc2 $fc1 $fc2]
```

Expected assertions:
- `pc2 == pc1`
- `fc2 >= 1` (freelist exists)

## Success Criteria
- Repeated insert/delete cycles do not increase `PRAGMA page_count`.
- Freelist trunk pages are reused instead of reallocated.
