# Fix VIEW DDL, constraint handling, and error messaging (VIEW tests) [high]

## Problem
This moth compiles a range of failures observed in the `view.test` TCL suite, all stemming from `rustql`'s incomplete or incorrect implementation of `VIEW` DDL (Data Definition Language), the enforcement of constraints and rules pertaining to views, and the accuracy of generated error messages. Specific issues include: `view already exists` errors, misbehavior or incorrect error reporting when attempting to modify or drop views, validation of column counts in view definitions, adherence to rules against indexing views, and proper handling of cross-database object references within views. Parsing-related syntax errors in view definitions are also present.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/view.test`
- **Failing Sub-tests and Error Categories:**
    *   **View DDL/Existence/Modification:**
        - `view-1.3`: `Error: view "v1" already exists`
        - `view-2.2` to `view-2.4`: `! view-2.2 expected: [1 {cannot modify v2 because it is a view}] got: [1 {no such table: v2}]` (Mismatched error message)
        - `view-4.1`: `! view-4.1 expected: [1 {use DROP TABLE to delete table t1}] got: [1 {no such view: t1}]` (Mismatched error)
        - `view-4.3`: `! view-4.3 expected: [1 {use DROP VIEW to delete view v1}] got: [1 {no such table: v1}]` (Mismatched error)
        - `view-4.5`: `! view-4.5 expected: [1 {views may not be indexed}] got: [1 {no such table: v1}]` (Mismatched error)
    *   **Column Count/Value Mismatches in DDL/Views:**
        - `view-2.5`: `Error: table t1 has 3 columns but 4 values were supplied`
        - `view-3.3.5`: `! view-3.3.5 expected: [1 {expected 2 columns for 'v1err' but got 3}] got: [0 {1 5 1 4 11 1 7 17 1}]`
        - `view-3.3.6`: `! view-3.3.6 expected: [1 {expected 4 columns for 'v1err' but got 3}] got: [0 {1 5 1 4 11 1 7 17 1}]`
    *   **Syntax/Parsing Errors in View DDL:**
        - `view-3.3.4`: `! view-3.3.4 expected: [1 {syntax error after column name "y"}] got: [1 {near "DESC": syntax error}]`
    *   **Cross-Database View Referencing:**
        - `view-13.1`: `! view-13.1 expected: [1 {view v13 cannot reference objects in database two}] got: [1 {table "t2" already exists}]`

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/view.c`: Core implementation of `CREATE VIEW`, `DROP VIEW`, and other view-specific logic.
  - `sqlite3/src/build.c`: Compilation of DDL statements, including validation of view definitions.
  - `sqlite3/src/parse.y`: The SQL grammar definition, essential for correct parsing of view DDL.
  - `sqlite3/src/schema.c`: Manages the database schema and performs checks for object existence and compatibility.
  - `sqlite3/src/main.c`: General error reporting and message generation for SQL operations.

## Acceptance Criteria
This moth is considered done when all listed `view.test` sub-tests related to `VIEW` DDL, constraint violations, and error messaging pass and produce the exact expected behavior or error messages according to SQLite's reference implementation.
To verify, run:
```bash
make test-view
```
