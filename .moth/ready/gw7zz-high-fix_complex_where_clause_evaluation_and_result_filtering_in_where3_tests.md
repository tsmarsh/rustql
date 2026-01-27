# Fix complex WHERE clause evaluation and result filtering in WHERE3 tests [high]

## Problem
The `where3.test` TCL suite exposes significant issues in `rustql`'s ability to correctly evaluate complex `WHERE` clauses, particularly those involving multiple conditions, table joins, and filtering. The observed failures indicate that the filtering logic is either incorrectly applied or completely bypassed, leading to result sets that contain many more rows than expected (i.e., less restrictive filtering). This is a critical deficiency for accurate query processing.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/where3.test`
- **Failing Sub-tests:**
  - `where3-1.1`: `! where3-1.1 expected: [222 two 2 222 {} {}] got: [111 one 2 222 {} {} 222 two 2 222 {} {} 333 three 2 222 {} {}]`
  - `where3-1.2`: `! where3-1.2 expected: [1 {Value for C1.1} {Value for C2.1} 2 {} {Value for C2.2} 3 {Value for C1.3} {Value for C2.3}] got: [1 {Value for C1.1} {Value for C2.1} 1 {} {Value for C2.2} 1 {} {Value for C2.3} 2 {} {Value for C2.1} 2 {} {Value for C2.2} 2 {} {Value for C2.3} 3 {} {Value for C2.1} 3 {} {Value for C2.2} 3 {} {Value for C2.3}]`
- **Error Excerpts from `test-results/where3.log` (examples):**
  ```
  where3-1.1...
  ! where3-1.1 expected: [222 two 2 222 {} {}]
  ! where3-1.1 got:      [111 one 2 222 {} {} 222 two 2 222 {} {} 333 three 2 222 {} {}]
  ```

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/where.c`: The central module for parsing, analyzing, and optimizing `WHERE` clauses, including join conditions.
  - `sqlite3/src/wherecode.c`: Responsible for generating the VDBE opcodes that implement the `WHERE` clause logic.
  - `sqlite3/src/vdbe.c` / `sqlite3/src/vdbeapi.c`: The VDBE execution engine, particularly opcodes related to conditional jumps, comparisons, and row filtering (`Opcode::If`, `Opcode::Ne`, `Opcode::Eq`, `Opcode::IdxGE`, `Opcode::SeekLT`, etc.).
  - `sqlite3/src/expr.c`: For the correct evaluation of individual expressions within `WHERE` conditions.
  - `sqlite3/src/select.c`: Overall `SELECT` statement compilation, of which `WHERE` is a key part.

## Acceptance Criteria
This moth is considered done when `where3-1.1`, `where3-1.2`, and any other similar sub-tests within `where3.test` that demonstrate incorrect result filtering due to complex `WHERE` clause evaluation produce the exact expected result sets.
To verify, run:
```bash
make test-where3
```
