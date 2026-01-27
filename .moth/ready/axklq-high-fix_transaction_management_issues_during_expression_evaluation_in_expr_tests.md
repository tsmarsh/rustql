# Fix transaction management issues during expression evaluation in EXPR tests [high]

## Problem
Several sub-tests in the `expr.test` TCL suite are failing with the error `cannot start a transaction within a transaction`. This indicates an unexpected interaction between expression evaluation and transaction management. Expressions should typically be side-effect free regarding transactions, suggesting that either `rustql`'s expression compilation generates incorrect VDBE opcodes that trigger transaction operations, or its transaction state machine is erroneously detecting nested transactions within expression contexts.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/expr.test`
- **Failing Sub-tests:**
  - `expr-1.46a`
  - `expr-1.107`
  - `expr-1.214`
  - `expr-1.234`
- **Error Excerpts from `test-results/expr.log`:**
  ```
  expr-1.46a...
  Error: cannot start a transaction within a transaction
  ```

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/expr.c`: Examine expression compilation routines for any unintended generation of transaction-related VDBE opcodes.
  - `sqlite3/src/vdbe.c` / `sqlite3/src/vdbeapi.c`: Review the execution of opcodes related to transaction control (`Opcode::Transaction`, `Opcode::AutoCommit`, `Opcode::Savepoint`, etc.) and how they interact with the VDBE stack and expression results.
  - `sqlite3/src/main.c`: Overall database transaction state management.

## Acceptance Criteria
This moth is considered done when the `expr.test` sub-tests that produce `cannot start a transaction within a transaction` errors pass without such transaction-related issues, confirming that expression evaluation does not inadvertently alter or conflict with the database's transaction state.
To verify, run:
```bash
make test-expr
```
