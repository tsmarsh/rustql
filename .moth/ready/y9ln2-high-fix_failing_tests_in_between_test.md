# Summary
- Test: sqlite3/test/between.test
- Repro: `testfixture test/between.test`
- Failing cases: between-1.2.1, between-1.3.1, between-1.4, between-2.1.1, between-2.1.2, between-2.1.3, between-2.1.4, between-2.1.5
- Primary errors: ! between-1.2.1 expected: [5 2 36 38 6 2 49 51 sort t1 i1w] | ! between-1.2.1 got:      [nosort t1 i1w] | ! between-1.3.1 expected: [5 2 36 38 6 2 49 51 sort t1 i1w]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-between-3641403-1769533960736
DEBUG: tester.tcl sourced, db=db
Running between.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/between.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/between.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
between-1.0... Ok
between-1.1.1... Ok
between-1.1.2... Ok
between-1.2.1...
! between-1.2.1 expected: [5 2 36 38 6 2 49 51 sort t1 i1w]
! between-1.2.1 got:      [nosort t1 i1w]
between-1.2.2... Ok
between-1.3.1...
! between-1.3.1 expected: [5 2 36 38 6 2 49 51 sort t1 i1w]
! between-1.3.1 got:      [1 0 4 4 2 1 9 10 3 1 16 17 4 2 25 27 5 2 36 38 6 2 49 51 sort t1 i1w]
between-1.3.2... Ok
between-1.4...
! between-1.4 expected: [5 2 36 38 6 2 49 51 sort t1 *]
! between-1.4 got:      [nosort t1 i1w]
between-1.5.1... Ok
between-1.5.2... Ok
between-1.5.3... Ok
between-2.0... Ok
between-2.1.1...
! between-2.1.1 expected: [0]
! between-2.1.1 got:      [1]
between-2.1.2...
! between-2.1.2 expected: [0]
! between-2.1.2 got:      [1]
between-2.1.3...
! between-2.1.3 expected: [0]
! between-2.1.3 got:      [1]
between-2.1.4...
! between-2.1.4 expected: [1]
! between-2.1.4 got:      [0]
between-2.1.5...
! between-2.1.5 expected: [1]
! between-2.1.5 got:      [0]
between-2.1.6... Ok
between-2.1.7... Ok
between-3.0... Ok
between-3.1... Ok
between-3.2... Ok
Running "between"

Error in between.test: couldn't read file "between": no such file or directory
couldn't read file "between": no such file or directory
    while executing
"source between"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/between.test" line 160)
    invoked from within
"source $test_file"

==========================================
Test: between
Time: 0s
Status: FAILED
