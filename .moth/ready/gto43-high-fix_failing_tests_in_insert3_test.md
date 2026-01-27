# Summary
- Test: sqlite3/test/insert3.test
- Repro: `testfixture test/insert3.test`
- Failing cases: insert3-1.0, insert3-1.1, insert3-1.2, insert3-1.4.1, insert3-1.4.2, insert3-2.2, insert3-3.2, insert3-3.4, insert3-3.5, insert3-3.6, insert3-3.7
- Primary errors: ! insert3-1.0 expected: [5 1 hello 1] | ! insert3-1.0 got:      [1 1 hello 2] | ! insert3-1.1 expected: [5 2 hello 2]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-insert3-3641340-1769533960708
DEBUG: tester.tcl sourced, db=db
Running insert3.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/insert3.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/insert3.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
insert3-1.0...
! insert3-1.0 expected: [5 1 hello 1]
! insert3-1.0 got:      [1 1 hello 2]
insert3-1.1...
! insert3-1.1 expected: [5 2 hello 2]
! insert3-1.1 got:      [1 2 2 1 hello 3]
insert3-1.2...
! insert3-1.2 expected: [5 2 453 1 hello 2]
! insert3-1.2 got:      [1 4 1 1 2 2 hello 4]
insert3-1.3... Ok
insert3-1.4.1...
! insert3-1.4.1 expected: [a: 5 4 b: 10 2 b: 20 1 a: 453 2 a: hello 4 b: hi 2 b: world 1]
! insert3-1.4.1 got:      [a: 1 5 a: 1 2 a: 2 3 a: 4 1 a: hello 5 b: hi 1 b: world 1]
insert3-1.4.2...
! insert3-1.4.2 expected: [a: 5 4 b: 10 2 b: 20 1 a: 453 2 a: hello 4 b: hi 2 b: world 1]
! insert3-1.4.2 got:      [a: 1 2 a: 1 5 a: 2 3 a: 4 1 a: hello 5 b: hi 1 b: world 1]
insert3-1.5...
Error: UNIQUE constraint failed: table.rowid
insert3-2.1...
Error: UNIQUE constraint failed: table.rowid
insert3-2.2...
! insert3-2.2 expected: [1 b c -1 987 c -1 b 876]
! insert3-2.2 got:      [1 b c {} 987 c {} b 876]
insert3-3.1... Ok
insert3-3.2...
! insert3-3.2 expected: [1 {no such column: nosuchcol}]
! insert3-3.2 got:      [0 {}]
insert3-3.3... Ok
insert3-3.4...
! insert3-3.4 expected: [1 {no such column: nosuchcol}]
! insert3-3.4 got:      [0 {}]
insert3-3.5...
! insert3-3.5 expected: [1 xyz]
! insert3-3.5 got:      [1 {}]
insert3-3.6...
! insert3-3.6 expected: [1 xyz 2 xyz]
! insert3-3.6 got:      [1 {} 2 {}]
insert3-3.7...
! insert3-3.7 expected: [{} 4.3 hi]
! insert3-3.7 got:      [{} {} {}]
insert3-4.1...
Error: table "t1" already exists
insert3-4.2... Ok
Running "insert3"

Error in insert3.test: couldn't read file "insert3": no such file or directory
couldn't read file "insert3": no such file or directory
    while executing
"source insert3"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/insert3.test" line 205)
    invoked from within
"source $test_file"

==========================================
Test: insert3
Time: 0s
Status: FAILED
