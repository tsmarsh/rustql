# Fix widespread schema/name resolution issues in VIEW tests [high]

## Problem
The `view.test` TCL suite highlights a pervasive set of errors related to `rustql`'s schema and name resolution capabilities when working with views. A large number of sub-tests fail with messages indicating `no such table`, `no such view`, or `no such column`, often coupled with incorrect error codes or unexpected data results. This suggests deep-seated issues within `rustql`'s name binding, scope management, and schema lookup mechanisms, particularly for objects referenced within view definitions or when querying views.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/view.test`
- **Failing Sub-tests (examples covering various `view-*` tests):**
    *   `view-1.2`: `! view-1.2 expected: [1 {no such table: v1}] got: [0 {1 2 4 5 7 8}]` (Expected error, got data)
    *   `view-1.3.1`: `Error: no such column: a`
    *   `view-1.4`: `! view-1.4 expected: [1 {no such table: v1}] got: [1 {no such column: a}]` (Mismatched error message)
    *   `view-1.11`: `Error: no such table`
    *   `view-3.1`: `Error: no such table: v1`
    *   `view-3.3.1`: `Error: no such view: v1`
    *   `view-5.2`: `Error: no such column: t1.x`
    *   `view-7.3`: `Error: no such view: test`
    *   `view-10.1`: `Error: no such table: v_t3_a`
    ... (This pattern is extensive across the `view.test` file)

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/resolve.c`: The primary source for name resolution within SQL statements, including tables, views, and columns.
  - `sqlite3/src/schema.c`: Manages the in-memory representation of the database schema, including definitions of tables and views.
  - `sqlite3/src/view.c`: Specific logic for views, how their underlying SELECT statements are stored and processed.
  - `sqlite3/src/expr.c`: How column references within expressions (used in view SELECTs) are handled.
  - `sqlite3/src/parse.y`: The grammar defining how identifiers are parsed.

## Acceptance Criteria
This moth is considered done when all `view.test` sub-tests that currently fail due to `no such table/view/column` errors or related mismatched error messages related to name resolution pass, indicating correct and consistent schema and name resolution behavior for views.
To verify, run:
```bash
make test-view
```
