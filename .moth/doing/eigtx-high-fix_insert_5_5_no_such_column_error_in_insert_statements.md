# Fix insert-5.5: 'no such column' error in INSERT statements [high]

## Problem
The `insert-5.5` sub-test in the `insert.test` TCL suite is failing with a "no such column" error. This indicates a problem in how `rustql` resolves column names during INSERT operations.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/insert.test`
- **Failing Sub-test:** `insert-5.5`
- **Error Excerpt from `test-results/insert.log`:**
  ```
  insert-5.5...
  Error: no such column: name
  ```

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/insert.c`: Handles the compilation and execution of INSERT statements.
  - `sqlite3/src/resolve.c`: Contains logic for name resolution, including column names.
  - `sqlite3/src/parse.y`: The grammar definition, which dictates how column references are parsed.
  - `sqlite3/src/schema.c`: For schema lookup and column definitions.

## Acceptance Criteria
This moth is considered done when the `insert-5.5` sub-test (or the entire `insert.test` suite if run as `make test-insert`) passes without errors.
To verify, run:
```bash
make test-insert
```
