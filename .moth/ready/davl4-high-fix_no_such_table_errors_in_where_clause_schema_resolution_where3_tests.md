# Fix 'no such table' errors in WHERE clause schema resolution (WHERE3 tests) [high]

## Problem
A series of sub-tests within the `where3.test` TCL suite are failing with the error `no such table: tA`. This indicates a problem with `rustql`'s schema resolution mechanism specifically when evaluating expressions within `WHERE` clauses. The table `tA` is not being correctly identified or is inaccessible from the context of the `WHERE` clause, suggesting potential bugs in scope management, table alias handling, or the underlying table lookup logic.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/where3.test`
- **Failing Sub-tests:**
  - `where3-2.1`
  - `where3-2.1.1` through `where3-2.1.5`
  - `where3-2.2` through `where3-2.7`
- **Error Excerpts from `test-results/where3.log` (examples):**
  ```
  where3-2.1...
  Error: no such table: tA
  ```

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/where.c`: Integration of `WHERE` clause processing with table and schema lookups.
  - `sqlite3/src/resolve.c`: Core logic for resolving references (including tables and columns) against defined scopes.
  - `sqlite3/src/schema.c`: Manages the in-memory database schema and provides functions for table definition retrieval.
  - `sqlite3/src/parse.y`: Defines the grammar rules for table and column references.

## Acceptance Criteria
This moth is considered done when all listed `where3-2.*` sub-tests pass without errors, confirming that `rustql` correctly resolves table names within `WHERE` clause contexts.
To verify, run:
```bash
make test-where3
```
