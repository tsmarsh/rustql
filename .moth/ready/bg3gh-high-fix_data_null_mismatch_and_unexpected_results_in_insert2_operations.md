# Fix data/null mismatch and unexpected results in INSERT2 operations [high]

## Problem
Several sub-tests in the `insert2.test` TCL suite are exhibiting data mismatches, incorrect handling of NULL values, or other unexpected results after `INSERT` operations. This suggests problems with how `rustql` serializes, stores, retrieves, and interprets various data types, especially in scenarios involving specific value patterns or explicit NULLs.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/insert2.test`
- **Failing Sub-tests:**
  - `insert2-3.4`
  - `insert2-3.5`
  - `insert2-3.6`
  - `insert2-3.8`
  - `insert2-5.1`
  - `insert2-5.2`
  - `insert2-6.3`
- **Error Excerpts from `test-results/insert2.log` (examples):**
  ```
  insert2-3.4...
  ! insert2-3.4 expected: [160]
  ! insert2-3.4 got:      [157]

  insert2-5.1...
  ! insert2-5.1 expected: [1 2 1 3]
  ! insert2-5.1 got:      [1 2 {} 3]

  insert2-6.3...
  ! insert2-6.3 expected: [0]
  ! insert2-6.3 got:      []
  ```

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/insert.c`: High-level processing of `INSERT` statements and values.
  - `sqlite3/src/vdbe.c` / `sqlite3/src/vdbeapi.c`: VDBE opcodes and runtime logic for value storage, retrieval, type affinity, and null handling.
  - `sqlite3/src/mem.c`: Internal `Mem` object handling, type conversions, and comparison.
  - `sqlite3/src/btree.c`: Physical storage and retrieval of records from B-trees, including how different data types are encoded.

## Acceptance Criteria
This moth is considered done when all listed `insert2-3.*`, `insert2-5.*`, and `insert2-6.3` sub-tests pass without errors or unexpected results.
To verify, run:
```bash
make test-insert2
```
