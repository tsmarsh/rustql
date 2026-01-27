# Fix incorrect query results and data mismatches when querying views (VIEW tests) [high]

## Problem
The `view.test` TCL suite highlights numerous instances where querying defined views in `rustql` produces incorrect or unexpected result sets. These failures manifest as mismatches in expected row counts, column values, data ordering, or overall data presentation, including issues with string case sensitivity. This indicates underlying problems in how `rustql` translates a view's definition into an executable query, processes the data returned by that query, or manages the output formatting.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/view.test`
- **Failing Sub-tests (examples):**
  - `view-2.1`: `! view-2.1 expected: [x 7 a 8 b 9 c 10] got: [a 7 b 8 c 9]` (Result set mismatch)
  - `view-3.2`: `! view-3.2 expected: [x 7 a 8 b 9 c 10] got: [a 7 b 8 c 9]` (Result set mismatch)
  - `view-3.3.2`: `! view-3.3.2 expected: [a 2 b+c 7 c 4] got: [a 1 ? 5 c 3]` (Result set mismatch)
  - `view-3.4`: `! view-3.4 expected: [a 2 a 3 a 5 a 6] got: [a 1 a 2 a 4 a 5]` (Result set mismatch)
  - `view-8.4`: `! view-8.4 expected: [3] got: [7]` (Value mismatch)
  - `view-9.3`: `! view-9.3 expected: [1 2 4] got: [15]` (Result mismatch from complex view query)
  - `view-11.1`: `! view-11.1 expected: [This this THIS] got: [THIS]` (String processing/case sensitivity)
- **Error Excerpts from `test-results/view.log` (examples):**
  ```
  view-2.1...
  ! view-2.1 expected: [x 7 a 8 b 9 c 10]
  ! view-2.1 got:      [a 7 b 8 c 9]

  view-11.1...
  ! view-11.1 expected: [This this THIS]
  ! view-11.1 got:      [THIS]
  ```

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/select.c`: The compiler and executor for `SELECT` statements, which constitute the core of a view's query.
  - `sqlite3/src/view.c`: Handles how a view's stored `SELECT` statement is retrieved and compiled for execution.
  - `sqlite3/src/vdbe.c` / `sqlite3/src/vdbeapi.c`: The virtual machine where row fetching, data processing, and result set assembly occur.
  - `sqlite3/src/expr.c`: Evaluation of expressions within the view's query.
  - `sqlite3/src/mem.c`: Type handling, coercion, and comparison of data values.

## Acceptance Criteria
This moth is considered done when all `view.test` sub-tests that currently fail due to incorrect query results or data mismatches when querying views produce the exact expected output.
To verify, run:
```bash
make test-view
```
