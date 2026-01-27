# Fix TCL harness error when parsing EXPLAIN QUERY PLAN output (WHERE3 tests) [high]

## Problem
The `where3.test` TCL suite is encountering a critical error within its test harness when attempting to validate the output of `EXPLAIN QUERY PLAN` for sub-test `where3-3.0a`. The error `can't read "cx(0)": no such variable` indicates a failure in the TCL script (`query_plan_graph` and `append_graph` procedures) to correctly parse or interpret the formatted output produced by `rustql`'s `EXPLAIN QUERY PLAN` command. This effectively blocks proper testing and verification of the query optimizer's behavior.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/where3.test`
- **Failing Sub-test:** `where3-3.0a` (The error occurs in the helper procedures for this test)
- **Error Excerpts from `test-results/where3.log`:**
  ```
  Error in where3.test: can't read "cx(0)": no such variable
      while executing
  "set x $cx($level)"
      (procedure "append_graph" line 4)
      invoked from within
  "append_graph "  " dx cx 0"
      (procedure "query_plan_graph" line 7)
      invoked from within
  "query_plan_graph $sql"
      (procedure "do_eqp_test" line 4)
  ```

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/vdbe.c` / `sqlite3/src/vdbeapi.c`: Components responsible for generating the human-readable output of `EXPLAIN QUERY PLAN`. The format of this output needs to match what the TCL harness expects.
  - `sqlite3/src/vdbeaux.c`: May contain helper functions for formatting VDBE output.
- **Relevant TCL Files:**
  - `sqlite3/test/tester.tcl`: Contains the `do_eqp_test`, `query_plan_graph`, and `append_graph` procedures that are failing. Analysis of these scripts is crucial to understand the expected input format.

## Acceptance Criteria
This moth is considered done when the `where3-3.0a` sub-test (and any other `EXPLAIN QUERY PLAN` tests within `where3.test`) executes without the TCL harness errors, correctly parsing and validating `rustql`'s query plan output.
To verify, run:
```bash
make test-where3
```
