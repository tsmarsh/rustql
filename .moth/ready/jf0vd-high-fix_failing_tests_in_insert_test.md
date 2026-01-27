# Summary
- Test: sqlite3/test/insert.test
- Repro: `testfixture test/insert.test`
- Failing cases: insert-6.3, insert-6.4, insert-15.1, insert-16.4, insert-17.1, insert-17.3, insert-17.6, insert-17.8, insert-17.11, insert-17.12, insert-17.14, insert-17.15
- Primary errors: Error: no such column: name | ! insert-6.3 expected: [2 4] | ! insert-6.3 got:      [1 4]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-insert-3641338-1769533960708
DEBUG: tester.tcl sourced, db=db
Running insert.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/insert.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/insert.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
insert-1.1... Ok
insert-1.2... Ok
insert-1.3... Ok
insert-1.3b... Ok
insert-1.3c... Ok
insert-1.3d... Ok
insert-1.4... Ok
insert-1.5... Ok
insert-1.5b... Ok
insert-1.5c... Ok
insert-1.6... Ok
insert-1.6b... Ok
insert-1.6c... Ok
insert-2.1... Ok
insert-2.2... Ok
insert-2.3... Ok
insert-2.4... Ok
insert-2.10... Ok
insert-2.11... Ok
insert-2.12... Ok
insert-3.1... Ok
insert-3.2... Ok
insert-3.3... Ok
insert-3.4... Ok
insert-3.5... Ok
insert-3.5... Ok
insert-4.1... Ok
insert-4.2... Ok
insert-4.3... Ok
insert-4.4... Ok
insert-4.5... Ok
insert-4.6... Ok
insert-4.7... Ok
insert-5.1... Ok
insert-5.2... Ok
insert-5.3... Ok
insert-5.4... Ok
insert-5.5...
Error: no such column: name
insert-5.6... Ok
insert-5.7... Ok
insert-6.1... Ok
insert-6.2... Ok
insert-6.3...
! insert-6.3 expected: [2 4]
! insert-6.3 got:      [1 4]
insert-6.4...
! insert-6.4 expected: []
! insert-6.4 got:      [2 3]
insert-6.5... Ok
insert-6.6... Ok
insert-7.1... Ok
insert-7.2... Ok
insert-7.3... Ok
insert-8.1... Ok
insert-9.1... Ok
insert-9.2... Ok
insert-10.1... Ok
insert-10.2... Ok
insert-11.1... Ok
insert-12.1... Ok
insert-12.2... Ok
insert-12.3... Ok
insert-13.1...
Error: near "-": syntax error
insert-14.1... Ok
insert-14.2... Ok
insert-15.1...
! insert-15.1 expected: [4 33000]
! insert-15.1 got:      [4 31245]
insert-16.1... Ok
insert-16.2... Ok
insert-16.3... Ok
insert-16.4...
! insert-16.4 expected: [1 {UNIQUE constraint failed: t1.a}]
! insert-16.4 got:      [0 {}]
insert-16.5... Ok
insert-16.6... Ok
insert-16.7... Ok
insert-17.1...
! insert-17.1 expected: [1 {UNIQUE constraint failed: t0.rowid}]
! insert-17.1 got:      [1 {UNIQUE constraint failed: t0.bb}]
insert-17.2... Ok
insert-17.3...
! insert-17.3 expected: [1 {UNIQUE constraint failed: t1.c}]
! insert-17.3 got:      [1 {UNIQUE constraint failed: t1.b}]
insert-17.4... Ok
insert-17.5...
Error: UNIQUE constraint failed: t2.b
insert-17.6...
! insert-17.6 expected: [3 4]
! insert-17.6 got:      []
insert-17.7...
Error: UNIQUE constraint failed: t2.b
insert-17.8...
! insert-17.8 expected: [3]
! insert-17.8 got:      []
insert-17.10...
Error: UNIQUE constraint failed: t3.b
insert-17.11...
! insert-17.11 expected: [1 1 1 1 x 2 1 3 2 x 4 4 8 9 x]
! insert-17.11 got:      []
insert-17.12...
! insert-17.12 expected: [1 1 1 1 x 4 4 8 9 x 5 1 11 2 x]
! insert-17.12 got:      [5 1 11 2 x]
insert-17.13...
Error: UNIQUE constraint failed: t3.b
insert-17.14...
! insert-17.14 expected: [1 {UNIQUE constraint failed: t3.b}]
! insert-17.14 got:      [0 {}]
insert-17.15...
! insert-17.15 expected: [1 {UNIQUE constraint failed: t3.d}]
! insert-17.15 got:      [0 {}]
Running "insert"

Error in insert.test: couldn't read file "insert": no such file or directory
couldn't read file "insert": no such file or directory
    while executing
"source insert"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/insert.test" line 603)
    invoked from within
"source $test_file"

==========================================
Test: insert
Time: 0s
Status: FAILED
