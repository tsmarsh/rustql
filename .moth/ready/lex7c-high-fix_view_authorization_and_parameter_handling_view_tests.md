# Fix view authorization and parameter handling (VIEW tests) [high]

## Problem
This moth addresses specific failures in the `view.test` TCL suite concerning `rustql`'s implementation of view authorization/access control and its rules for parameter handling within view definitions or queries. `view-1.1.100` indicates a mismatch in how access restrictions to views are reported, while `view-12.1` and `view-12.2` show incorrect behavior or error messaging when SQL parameters are used in conjunction with views, suggesting `rustql` deviates from SQLite's expected parameter binding and validation for views.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/view.test`
- **Failing Sub-tests and Error Categories:**
    *   **Authorization/Access Control:**
        - `view-1.1.100`: `! view-1.1.100 expected: [1 {access to view "v1" prohibited}] got: [0 {1 2 4 5 7 8}]` (Expected authorization error vs. unexpected data output)
    *   **Parameter Handling in Views:**
        - `view-12.1`: `! view-12.1 expected: [1 {parameters are not allowed in views}] got: [0 {}]` (Expected parameter error vs. no data)
        - `view-12.2`: `! view-12.2 expected: [1 {parameters are not allowed in views}] got: [1 {view "v12" already exists}]` (Mismatched error message)

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/auth.c`: Implements the `sqlite3_set_authorizer` callback and associated authorization logic.
  - `sqlite3/src/view.c`: How view definitions are compiled and stored, and their interaction with security features.
  - `sqlite3/src/prepare.c`: SQL statement preparation, including parameter recognition and binding.
  - `sqlite3/src/main.c`: General error reporting and message generation.

## Acceptance Criteria
This moth is considered done when all listed `view.test` sub-tests related to view authorization and parameter handling pass and produce the exact expected behavior or error messages according to SQLite's reference implementation.
To verify, run:
```bash
make test-view
```
