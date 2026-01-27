# Fix insert3-4.1: 'table already exists' error during INSERT-related DDL [high]

## Problem
The `insert3-4.1` sub-test in the `insert3.test` TCL suite is failing with an `Error: table "t1" already exists`. This indicates a problem in `rustql`'s DDL (Data Definition Language) processing or schema management. The test likely involves an `INSERT` statement combined with a `CREATE TABLE` (e.g., `CREATE TABLE AS SELECT`) where the target table already exists, and `rustql` is not handling this scenario according to SQLite's expected behavior (e.g., silently succeeding with `IF NOT EXISTS` or providing a specific error code).

## Failing Test Details
- **TCL Test File:** `sqlite3/test/insert3.test`
- **Failing Sub-test:** `insert3-4.1`
- **Error Excerpt from `test-results/insert3.log`:**
  ```
  insert3-4.1...
  Error: table "t1" already exists
  ```

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/build.c`: Responsible for constructing and processing DDL statements.
  - `sqlite3/src/main.c`: General DDL command handling.
  - `sqlite3/src/schema.c`: Manages the database schema, including checking for existing tables.
  - `sqlite3/src/insert.c`: If the table creation is part of an `INSERT ... SELECT` statement.

## Acceptance Criteria
This moth is considered done when the `insert3-4.1` sub-test (or the entire `insert3.test` suite if run as `make test-insert3`) passes without errors.
To verify, run:
```bash
make test-insert3
```
