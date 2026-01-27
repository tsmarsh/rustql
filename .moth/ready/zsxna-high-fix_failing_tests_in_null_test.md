# Summary
- Test: sqlite3/test/null.test
- Repro: `testfixture test/null.test`
- Failing cases: null-6.5, null-6.6, null-7.1, null-7.2, null-8.11, null-8.13, null-9.2, null-9.3, null-10.1
- Primary errors: ! null-6.5 expected: [1 {1st ORDER BY term does not match any column in the result set}] | ! null-6.5 got:      [0 {{} 0 1}] | ! null-6.6 expected: [1 {1st ORDER BY term does not match any column in the result set}]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-null-3641378-1769533960728
DEBUG: tester.tcl sourced, db=db
Running null.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/null.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/null.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
null-1.0... Ok
null-1.1... Ok
null-1.2... Ok
null-2.1... Ok
null-2.2... Ok
null-2.3... Ok
null-2.4... Ok
null-2.5... Ok
null-2.6... Ok
null-2.7... Ok
null-2.8... Ok
null-3.1... Ok
null-3.2... Ok
null-4.1... Ok
null-4.2... Ok
null-4.3... Ok
null-4.4... Ok
null-4.5... Ok
null-5.1... Ok
null-6.1... Ok
null-6.2... Ok
null-6.3... Ok
null-6.4... Ok
null-6.5...
! null-6.5 expected: [1 {1st ORDER BY term does not match any column in the result set}]
! null-6.5 got:      [0 {{} 0 1}]
null-6.6...
! null-6.6 expected: [1 {1st ORDER BY term does not match any column in the result set}]
! null-6.6 got:      [0 {{} 0 1}]
null-7.1...
! null-7.1 expected: [1 2 3]
! null-7.1 got:      [1 2 3 4]
null-7.2...
! null-7.2 expected: [1 2 3]
! null-7.2 got:      [1 2 3 4]
null-8.1... Ok
null-8.2... Ok
null-8.3... Ok
null-8.4... Ok
null-8.5... Ok
null-8.11...
! null-8.11 expected: []
! null-8.11 got:      [2]
null-8.12... Ok
null-8.13...
! null-8.13 expected: [1]
! null-8.13 got:      [1 2]
null-8.14... Ok
null-8.15... Ok
null-9.1...
Error: UNIQUE constraint failed: t5.a
null-9.2...
! null-9.2 expected: [1 {} one 1 {} i]
! null-9.2 got:      [1 {} one]
null-9.3...
! null-9.3 expected: [{} x two {} x ii]
! null-9.3 got:      []
null-10.1...
! null-10.1 expected: []
! null-10.1 got:      [0]
Running "null"

Error in null.test: couldn't read file "null": no such file or directory
couldn't read file "null": no such file or directory
    while executing
"source null"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/null.test" line 307)
    invoked from within
"source $test_file"

==========================================
Test: null
Time: 0s
Status: FAILED
