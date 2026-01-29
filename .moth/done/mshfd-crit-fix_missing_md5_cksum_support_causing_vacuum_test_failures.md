# Fix missing md5/cksum support causing VACUUM test failures [crit]

## Problem
The `vacuum.test` TCL test suite, which aims to verify the `VACUUM` command functionality, is failing immediately due to missing support for `md5` and `cksum` functions/commands. This blocks any meaningful testing or development of the `VACUUM` feature in `rustql`. The errors suggest either that these utility functions are not implemented in `rustql`'s TCL extension or they are not being exposed correctly to the TCL test environment.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/vacuum.test`
- **Failing Sub-tests:** `vacuum-1.1`, `vacuum-1.1b`
- **Error Excerpts from `test-results/vacuum.log`:**
  ```
  vacuum-1.1...
  Error: invalid command name "md5"
  vacuum-1.1b...
  ! vacuum-1.1b expected: [1 {bad function}]
  ! vacuum-1.1b got:      [0 {t1 i2 i1}]

  Error in vacuum.test: can't read "cksum": no such variable
  ```

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/func.c`: Contains definitions for many built-in SQL functions, potentially including `md5`.
  - `sqlite3/src/tclsqlite.c`: The SQLite TCL extension implementation, responsible for exposing SQLite's functionality to TCL scripts, including commands and functions.
  - `sqlite3/src/main.c`: General command processing and registration.
  - `sqlite3/src/os_unix.c` / `sqlite3/src/os_win.c`: OS-level utilities that might be related to `cksum` type functionality.

## Acceptance Criteria
This moth is considered done when the `vacuum-1.1` and `vacuum-1.1b` sub-tests pass. This will unblock further testing of the `VACUUM` command itself. The successful execution of `make test-vacuum` should proceed past these initial errors.
To verify, run:
```bash
make test-vacuum
```
