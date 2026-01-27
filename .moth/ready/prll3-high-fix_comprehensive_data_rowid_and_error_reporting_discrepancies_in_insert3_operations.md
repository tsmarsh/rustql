# Fix comprehensive data, rowid, and error reporting discrepancies in INSERT3 operations [high]

## Problem
The `insert3.test` TCL suite reveals a wide array of issues within `rustql`'s `INSERT` implementation, encompassing incorrect data storage/retrieval, inconsistent rowid and primary key handling, malformed complex value structures, improper handling of NULL, string, and float types, and discrepancies in error reporting for "no such column" and "UNIQUE constraint failed" scenarios. These failures collectively suggest deep-seated behavioral differences from SQLite in how `INSERT` statements are processed and their effects on data and schema are managed.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/insert3.test`
- **Failing Sub-tests and Error Categories:**
    *   **Data Mismatches (Rowid/Primary Key/Complex Value Structures):**
        - `insert3-1.0`: `! insert3-1.0 expected: [5 1 hello 1] got: [1 1 hello 2]`
        - `insert3-1.1`: `! insert3-1.1 expected: [5 2 hello 2] got: [1 2 2 1 hello 3]`
        - `insert3-1.2`: `! insert3-1.2 expected: [5 2 453 1 hello 2] got: [1 4 1 1 2 2 hello 4]`
        - `insert3-1.4.1`: `! insert3-1.4.1 expected: [a: 5 4 b: 10 2 b: 20 1 a: 453 2 a: hello 4 b: hi 2 b: world 1] got: [a: 1 5 a: 1 2 a: 2 3 a: 4 1 a: hello 5 b: hi 1 b: world 1]`
        - `insert3-1.4.2`: `! insert3-1.4.2 expected: [a: 5 4 b: 10 2 b: 20 1 a: 453 2 a: hello 4 b: hi 2 b: world 1] got: [a: 1 2 a: 1 5 a: 2 3 a: 4 1 a: hello 5 b: hi 1 b: world 1]`
    *   **Unique Constraint Failures:** (These failures are closely related to `zenc6: Fix UNIQUE constraint handling during INSERT operations` and should be addressed in conjunction.)
        - `insert3-1.5`: `Error: UNIQUE constraint failed: table.rowid`
        - `insert3-2.1`: `Error: UNIQUE constraint failed: table.rowid`
    *   **Null/String/Float Handling Data Mismatches:**
        - `insert3-2.2`: `! insert3-2.2 expected: [1 b c -1 987 c -1 b 876] got: [1 b c {} 987 c {} b 876]`
        - `insert3-3.5`: `! insert3-3.5 expected: [1 xyz] got: [1 {}]`
        - `insert3-3.6`: `! insert3-3.6 expected: [1 xyz 2 xyz] got: [1 {} 2 {}]`
        - `insert3-3.7`: `! insert3-3.7 expected: [{} 4.3 hi] got: [{} {} {}]`
    *   **"no such column" Error Reporting:** (These failures are closely related to `eigtx: Fix insert-5.5: 'no such column' error in INSERT statements` and should be addressed in conjunction.)
        - `insert3-3.2`: `! insert3-3.2 expected: [1 {no such column: nosuchcol}] got: [0 {}]`
        - `insert3-3.4`: `! insert3-3.4 expected: [1 {no such column: nosuchcol}] got: [0 {}]`

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/insert.c`: Primary source for `INSERT` statement processing, including rowid allocation, value assignment, and conflict handling.
  - `sqlite3/src/vdbe.c` / `sqlite3/src/vdbeapi.c`: VDBE opcodes and runtime functions that manage data types, serialisation, deserialisation, comparison, and error propagation.
  - `sqlite3/src/mem.c`: Defines the internal `Mem` data structure and its associated type coercion and manipulation routines.
  - `sqlite3/src/btree.c`: Manages the physical storage of data and indexes, including rowid allocation strategies and data encoding.
  - `sqlite3/src/build.c`: For schema-related aspects like column definitions and constraints.
  - `sqlite3/src/expr.c`: Evaluation of expressions that provide values for `INSERT` statements.
  - `sqlite3/src/resolve.c`: For correct column and table name resolution during statement compilation.

## Acceptance Criteria
This moth is considered done when all listed `insert3-*` sub-tests pass without errors or unexpected results.
To verify, run:
```bash
make test-insert3
```
