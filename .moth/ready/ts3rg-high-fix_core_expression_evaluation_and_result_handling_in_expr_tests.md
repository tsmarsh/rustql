# Fix core expression evaluation and result handling in EXPR tests [high]

## Problem
The `expr.test` TCL suite exhibits a widespread failure pattern where most sub-tests (e.g., `expr-1.*`, `expr-2.*`, `expr-3.*`, etc.) report an empty result (`got: []`) when a specific value (`expected: [X]`) is anticipated. This indicates a fundamental issue within `rustql`'s expression evaluation mechanism. Potential causes include: expressions not being computed correctly, results not being pushed onto the VDBE stack, or errors in how the test harness retrieves the evaluated results. This is a critical blocker for many other features that rely on correct expression processing.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/expr.test`
- **Failing Sub-tests (examples):**
  - `expr-1.2`: `! expr-1.2 expected: [-10] got: []`
  - `expr-1.3`: `! expr-1.3 expected: [200] got: []`
  - `expr-2.1`: `! expr-2.1 expected: [3.57] got: []`
  - `expr-3.1`: `! expr-3.1 expected: [1] got: []`
  - ... (This pattern is prevalent across most failing `expr-*` sub-tests)
- **Error Excerpts from `test-results/expr.log` (examples):**
  ```
  expr-1.2...
  ! expr-1.2 expected: [-10]
  ! expr-1.2 got:      []

  expr-2.22...
  ! expr-2.22 expected: [-1.11]
  ! expr-2.22 got:      []
  ```

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/expr.c`: The central component for parsing, analyzing, and compiling expressions into VDBE opcodes.
  - `sqlite3/src/vdbe.c`: Contains the VDBE execution loop and much of the logic for evaluating individual opcodes generated from expressions.
  - `sqlite3/src/vdbeapi.c`: Functions for interacting with the VDBE, including result retrieval.
  - `sqlite3/src/opcodes.h`: Definitions of VDBE opcodes used for expression evaluation (e.g., `Opcode::Add`, `Opcode::Subtract`, `Opcode::Eq`).
  - `sqlite3/src/mem.c`: Internal `Mem` object handling, data storage, and type conversions which are crucial for expression results.

## Acceptance Criteria
This moth is considered done when a significant majority of the `expr.test` sub-tests that currently show an `expected: [X] got: []` failure pattern begin to pass. The primary goal is for expression evaluation to consistently produce and return the correct values.
To verify, run:
```bash
make test-expr
```
