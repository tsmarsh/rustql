# Summary
- Test: sqlite3/test/update.test
- Repro: `testfixture test/update.test`
- Failing cases: update-5.4.1, update-5.4.2, update-5.4.3, update-5.5.2, update-5.5.5, update-6.1.2, update-7.1.1, update-7.1.2, update-7.4.1, update-7.4.2, update-7.4.3, update-7.5.1, update-7.5.4, update-11.2, update-11.3, update-11.4, update-14.2, update-14.4, update-20.20, update-21.3
- Primary errors: ! update-5.4.1 expected: [78 128] | ! update-5.4.1 got:      [] | ! update-5.4.2 expected: [778 128]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-update-3641344-1769533960709
DEBUG: tester.tcl sourced, db=db
Running update.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/update.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/update.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
update-1.1... Ok
update-2.1... Ok
update-3.1... Ok
update-3.2... Ok
update-3.3... Ok
update-3.4... Ok
update-3.5... Ok
update-3.5.1... Ok
update-3.5.2... Ok
update-3.5.3... Ok
update-3.6... Ok
update-3.7... Ok
update-3.8... Ok
update-3.9... Ok
update-3.10... Ok
update-3.11... Ok
update-3.12... Ok
update-3.13... Ok
update-3.14... Ok
update-4.0... Ok
update-4.1... Ok
update-4.2... Ok
update-4.3... Ok
update-4.4... Ok
update-4.5... Ok
update-4.6... Ok
update-4.7... Ok
update-5.0... Ok
update-5.1... Ok
update-5.2... Ok
update-5.3... Ok
update-5.4... Ok
update-5.4.1...
! update-5.4.1 expected: [78 128]
! update-5.4.1 got:      []
update-5.4.2...
! update-5.4.2 expected: [778 128]
! update-5.4.2 got:      []
update-5.4.3...
! update-5.4.3 expected: [8 88 8 128 8 256 8 888]
! update-5.4.3 got:      [8 88 8 256 8 888]
update-5.5... Ok
update-5.5.1... Ok
update-5.5.2...
! update-5.5.2 expected: [78 128]
! update-5.5.2 got:      []
update-5.5.3... Ok
update-5.5.4... Ok
update-5.5.5...
! update-5.5.5 expected: [8 88 8 128 8 256 8 888]
! update-5.5.5 got:      [8 88 8 256 8 888]
update-5.6... Ok
update-5.6.1... Ok
update-5.6.2... Ok
update-5.6.3... Ok
update-5.6.4... Ok
update-5.6.5... Ok
update-6.0... Ok
update-6.1... Ok
update-6.1.1... Ok
update-6.1.2...
! update-6.1.2 expected: [8 89]
! update-6.1.2 got:      []
update-6.1.3... Ok
update-6.2... Ok
update-6.3... Ok
update-6.3.1... Ok
update-6.3.2... Ok
update-6.3.3... Ok
update-6.4... Ok
update-6.4.1... Ok
update-6.4.2... Ok
update-6.4.3... Ok
update-6.5... Ok
update-6.5.1... Ok
update-6.5.2... Ok
update-6.5.3... Ok
update-6.5.4... Ok
update-6.6... Ok
update-6.6.1... Ok
update-6.6.2... Ok
update-6.6.3... Ok
update-6.6.4... Ok
update-7.0... Ok
update-7.1... Ok
update-7.1.1...
! update-7.1.1 expected: [8 89 8 257 8 889]
! update-7.1.1 got:      [8 88 8 256 8 888]
update-7.1.2...
! update-7.1.2 expected: [8 89]
! update-7.1.2 got:      []
update-7.1.3... Ok
update-7.2... Ok
update-7.3... Ok
update-7.3.1... Ok
update-7.3.2... Ok
update-7.3.3... Ok
update-7.4... Ok
update-7.4.1...
! update-7.4.1 expected: [78 128]
! update-7.4.1 got:      []
update-7.4.2...
! update-7.4.2 expected: [778 128]
! update-7.4.2 got:      []
update-7.4.3...
! update-7.4.3 expected: [8 88 8 128 8 256 8 888]
! update-7.4.3 got:      [8 88 8 256 8 888]
update-7.5... Ok
update-7.5.1...
! update-7.5.1 expected: [78 128]
! update-7.5.1 got:      []
update-7.5.2... Ok
update-7.5.3... Ok
update-7.5.4...
! update-7.5.4 expected: [8 88 8 128 8 256 8 888]
! update-7.5.4 got:      [8 88 8 256 8 888]
update-7.6... Ok
update-7.6.1... Ok
update-7.6.2... Ok
update-7.6.3... Ok
update-7.6.4... Ok
update-9.1... Ok
update-9.2... Ok
update-9.3... Ok
update-9.4... Ok
update-10.1... Ok
update-10.2... Ok
update-10.3... Ok
update-10.4... Ok
update-10.5... Ok
update-10.6... Ok
update-10.7... Ok
update-10.8... Ok
update-10.9... Ok
update-10.10... Ok
update-11.1... Ok
update-11.2...
! update-11.2 expected: [1 15 2 8]
! update-11.2 got:      [1 15 2 7]
update-11.3...
! update-11.3 expected: [1 16 2 9]
! update-11.3 got:      [1 16 2 7]
update-11.4...
! update-11.4 expected: [1 16 2 10]
! update-11.4 got:      [1 16 2 7]
update-12.1... Ok
update-13.1... Ok
update-13.2... Ok
update-13.3...
Error: no such column: rowid
update-13.3...
Error: no such column: rowid
update-13.4... Ok
update-13.5...
Error: no such column: rowid
update-13.6... Ok
update-14.1... Ok
update-14.2...
! update-14.2 expected: [1 {no such column: nosuchcol}]
! update-14.2 got:      [0 {}]
update-14.3... Ok
update-14.4...
! update-14.4 expected: [1 {no such column: nosuchcol}]
! update-14.4 got:      [0 {}]
update-15.1...
Error: no such column: c
update-16.1... Ok
update-17.10...
Error: near "1": syntax error
update-18.10... Ok
update-18.20...
Error: near "0": syntax error
update-19.10... Ok
update-20.10... Ok
update-20.20...
! update-20.20 expected: [1 {constraint failed}]
! update-20.20 got:      [1 {UNIQUE constraint failed: t1.a}]
update-20.30... Ok
update-21.1... Ok
update-21.2...
Error: query aborted
update-21.3...
! update-21.3 expected: [3 NULL 6 -54]
! update-21.3 got:      [100 NULL 100 -54]
update-21.4...
Error: query aborted
update-21.11... Ok

Error in update.test: can't read "cx(0)": no such variable
can't read "cx(0)": no such variable
    while executing
"set x $cx($level)"
    (procedure "append_graph" line 4)
    invoked from within
"append_graph "  " dx cx 0"
    (procedure "query_plan_graph" line 7)
    invoked from within
"query_plan_graph $sql"
    (procedure "do_eqp_test" line 4)
    invoked from within
"do_eqp_test update-21.12 {
  WITH t3(x,y) AS (SELECT d, row_number()OVER() FROM t2)
    UPDATE t1 SET b=(SELECT y FROM t3 WHERE t1.a=t3.x);
} {
  QUER..."
    (file "/tank/repos/rustql-architecture/sqlite3/test/update.test" line 775)
    invoked from within
"source $test_file"

==========================================
Test: update
Time: 15s
Status: FAILED
