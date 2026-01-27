# Fix insert2-4.1: 'no such table' error during INSERT with dependency check [high]

## Problem
The `insert2-4.1` sub-test in the `insert2.test` TCL suite is failing with an `Error: no such table: DepCheck`. This points to a problem with `rustql`'s schema resolution or table lookup mechanisms when an `INSERT` statement refers to a table that either doesn't exist or isn't being correctly identified in the current schema context. This could involve temporary tables, virtual tables, or internal schema access.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/insert2.test`
- **Failing Sub-test:** `insert2-4.1`
- **Error Excerpt from `test-results/insert2.log`:**
  ```
  insert2-4.1...
  Error: no such table: DepCheck
  ```

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/insert.c`: Logic responsible for processing INSERT statements, including table validation.
  - `sqlite3/src/resolve.c`: General name resolution logic for tables and columns.
  - `sqlite3/src/schema.c`: Manages the in-memory representation of the database schema.
  - `sqlite3/src/build.c`: Functions related to parsing and building SQL constructs, which might involve table references.

## Acceptance Criteria
This moth is considered done when the `insert2-4.1` sub-test (or the entire `insert2.test` suite if run as `make test-insert2`) passes without errors.
To verify, run:
```bash
make test-insert2
```
