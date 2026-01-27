# Summary
- Test: sqlite3/test/insert2.test
- Repro: `testfixture test/insert2.test`
- Failing cases: insert2-3.4, insert2-3.5, insert2-3.6, insert2-3.8, insert2-5.1, insert2-5.2, insert2-6.3
- Primary errors: ! insert2-3.4 expected: [160] | ! insert2-3.4 got:      [157] | ! insert2-3.5 expected: [320]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-insert2-3641342-1769533960709
DEBUG: tester.tcl sourced, db=db
Running insert2.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/insert2.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/insert2.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
insert2-1.0... Ok
insert2-1.1.1... Ok
insert2-1.1.2... Ok
insert2-1.1.3... Ok
insert2-1.2.1... Ok
insert2-1.2.2... Ok
insert2-1.3.1... Ok
insert2-1.3.2... Ok
insert2-1.4... Ok
insert2-2.0... Ok
insert2-2.1... Ok
insert2-2.2... Ok
insert2-2.3... Ok
insert2-3.0... Ok
insert2-3.1... Ok
insert2-3.2... Ok
insert2-3.2.1... Ok
insert2-3.3... Ok
insert2-3.4...
! insert2-3.4 expected: [160]
! insert2-3.4 got:      [157]
insert2-3.5...
! insert2-3.5 expected: [320]
! insert2-3.5 got:      [314]
insert2-3.6...
! insert2-3.6 expected: [160]
! insert2-3.6 got:      [445]
insert2-3.7... Ok
insert2-3.8...
! insert2-3.8 expected: [159]
! insert2-3.8 got:      [444]
insert2-3.9... Ok
insert2-4.1...
Error: no such table: DepCheck
insert2-5.1...
! insert2-5.1 expected: [1 2 1 3]
! insert2-5.1 got:      [1 2 {} 3]
insert2-5.2...
! insert2-5.2 expected: [1 2 1 3 1 4]
! insert2-5.2 got:      [1 2 {} 3 {} 4]
insert2-6.0... Ok
insert2-6.1... Ok
insert2-6.2... Ok
insert2-6.3...
! insert2-6.3 expected: [0]
! insert2-6.3 got:      []
Running "insert2"

Error in insert2.test: couldn't read file "insert2": no such file or directory
couldn't read file "insert2": no such file or directory
    while executing
"source insert2"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/insert2.test" line 298)
    invoked from within
"source $test_file"

==========================================
Test: insert2
Time: 0s
Status: FAILED
