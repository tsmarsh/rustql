# Fix missing functions, ESCAPE clause validation, column resolution, and syntax errors in EXPR tests [high]

## Problem
This moth consolidates several distinct but crucial errors observed in the `expr.test` TCL suite, all related to expression processing. These issues span from missing function implementations (e.g., `IMPLIES_NONNULL_ROW`), incorrect validation of SQL clauses (`LIKE ... ESCAPE`), failures in column name resolution within expressions, and general syntax errors during parsing. Addressing these will improve the robustness and compatibility of `rustql`'s expression engine.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/expr.test`
- **Failing Sub-tests and Error Categories:**
    *   **Missing Function Implementation:**
        - `expr-16.100`: `Error: no such function: IMPLIES_NONNULL_ROW`
        - `expr-16.101`: `Error: no such function: IMPLIES_NONNULL_ROW`
        - `expr-16.102`: `Error: no such function: IMPLIES_NONNULL_ROW`
    *   **`LIKE ... ESCAPE` Clause Validation:**
        - `expr-10.1`: `! expr-10.1 expected: [1 {ESCAPE expression must be a single character}] got: [0 1]`
        - `expr-10.2`: `! expr-10.2 expected: [1 {ESCAPE expression must be a single character}] got: [0 0]`
    *   **Column Resolution Errors:**
        - `expr-13.8`: `Error: no such column: `
        - `expr-13.9`: `Error: no such column: `
    *   **Generic Syntax/Parsing Error:**
        - `expr-1.127`: `! expr-1.127 expected: [1 {near "#1": syntax error}] got: [1 {unexpected character '#' at line 2}]`

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/func.c`: For defining and registering SQL functions. `IMPLIES_NONNULL_ROW` may be an internal helper.
  - `sqlite3/src/expr.c`: Handles the compilation of expressions, including validation of operators and function calls.
  - `sqlite3/src/parse.y` / `sqlite3/src/tokenize.c`: The grammar and tokenizer, critical for correct syntax parsing and error reporting for invalid tokens.
  - `sqlite3/src/resolve.c`: Manages the lookup and resolution of column and table names within SQL statements.
  - `sqlite3/src/where.c`: Logic for the `LIKE` operator and its `ESCAPE` clause.

## Acceptance Criteria
This moth is considered done when all listed sub-tests related to missing functions, `ESCAPE` clause validation, column resolution, and general syntax errors pass, ensuring that `rustql` correctly parses, validates, and executes these types of expressions.
To verify, run:
```bash
make test-expr
```
