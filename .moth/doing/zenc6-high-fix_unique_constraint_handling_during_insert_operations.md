# Fix UNIQUE constraint handling during INSERT operations [high]

## Problem
Multiple sub-tests in the `insert.test` TCL suite are failing due to incorrect or incomplete handling of `UNIQUE` constraints during `INSERT` operations. This manifests as incorrect error reporting, mismatched error messages, or unexpected database state after a constraint violation.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/insert.test`
- **Failing Sub-tests:**
  - `insert-16.4`
  - `insert-17.1`
  - `insert-17.5`
  - `insert-17.6`
  - `insert-17.7`
  - `insert-17.8`
  - `insert-17.10`
  - `insert-17.11`
  - `insert-17.12`
  - `insert-17.13`
  - `insert-17.14`
  - `insert-17.15`
- **Error Excerpts from `test-results/insert.log` (examples):**
  ```
  insert-16.4...
  ! insert-16.4 expected: [1 {UNIQUE constraint failed: t1.a}]
  ! insert-16.4 got:      [0 {}]

  insert-17.1...
  ! insert-17.1 expected: [1 {UNIQUE constraint failed: t0.rowid}]
  ! insert-17.1 got:      [1 {UNIQUE constraint failed: t0.bb}]

  insert-17.5...
  Error: UNIQUE constraint failed: t2.b

  insert-17.6...
  ! insert-17.6 expected: [3 4]
  ! insert-17.6 got:      []

  insert-17.11...
  ! insert-17.11 expected: [1 1 1 1 x 2 1 3 2 x 4 4 8 9 x]
  ! insert-17.11 got:      []
  ```

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/insert.c`: Core logic for `INSERT` statements, including initial constraint checks.
  - `sqlite3/src/build.c`: Schema validation and definition of constraints (e.g., `CREATE TABLE` parsing).
  - `sqlite3/src/vdbe.c` / `sqlite3/src/vdbeapi.c`: Virtual machine opcodes for enforcing constraints (e.g., `Opcode::NoConflict`, `Opcode::Abort`).
  - `sqlite3/src/btree.c`: Lower-level B-tree operations that unique indexes rely on.
  - `sqlite3/src/trigger.c`: If `ON CONFLICT` clauses involve triggers.
  - `sqlite3/src/where.c`: If `INSERT ... SELECT` involves complex `WHERE` clauses.

## Progress Notes

### 2025-01-28: Session 2
- **Tests passing: 71/83 (85.5%)**
- **Fixed issues:**
  - Index maintenance during UPDATE (delete old entries, insert new entries)
  - Duplicate index entries in table.indexes (ParseSchemaIndex was adding duplicates)
  - insert-6.3 (UPDATE OR REPLACE with WHERE on UNIQUE column) now passes
  - All insert-16.x tests now pass (insert-16.1 through insert-16.7)
  - insert-17.2, 17.4, 17.5, 17.7 now pass

- **Remaining failures (9 tests):**
  - insert-17.1, 17.3: Wrong constraint name (reports "t0.bb" instead of "t0.rowid")
    - Need to check rowid constraints before secondary indexes
  - insert-17.6, 17.8: AFTER DELETE triggers not firing during REPLACE conflict resolution
    - Need to implement trigger firing for conflict-deleted rows
  - insert-17.10-17.15: Complex scenarios involving partial indexes and recursive triggers

### 2025-01-27: Session 1
- Fixed DELETE operations (rows weren't being deleted)
- Fixed conflict_flags() returning wrong values (OE_REPLACE was 4 instead of 5)
- Added index deletion for REPLACE conflict handling
- Fixed btree stale cache issue
- Tests passing: 70/83 (84.3%)

## Acceptance Criteria
This moth is considered done when all listed `insert-16.*` and `insert-17.*` sub-tests pass without errors or unexpected results.
To verify, run:
```bash
make test-insert
```
