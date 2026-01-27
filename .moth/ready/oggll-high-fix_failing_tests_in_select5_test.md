# Summary
- Test: sqlite3/test/select5.test
- Repro: `testfixture test/select5.test`
- Failing cases: select5-2.1.2, select5-2.2, select5-2.3, select5-2.4, select5-3.1, select5-5.2, select5-5.5, select5-5.11, select5-6.1, select5-6.2, select5-8.1, select5-8.2, select5-8.3, select5-8.4, select5-8.5, select5-8.6, select5-8.7, select5-8.8, select5-9.1
- Primary errors: Error: no such function: count | Error: no such function: count | ! select5-2.1.2 expected: [1 {no such column: temp.t1.y}]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-select5-3641339-1769533960708
DEBUG: tester.tcl sourced, db=db
Running select5.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/select5.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/select5.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
select5-1.0... Ok
select5-1.1... Ok
select5-1.2...
Error: no such function: count
select5-1.3...
Error: no such function: count
select5-2.1.1... Ok
select5-2.1.2...
! select5-2.1.2 expected: [1 {no such column: temp.t1.y}]
! select5-2.1.2 got:      [0 {{} 15 {} 8 {} 4 {} 2 {} 1 {} 1}]
select5-2.2...
! select5-2.2 expected: [1 {no such function: z}]
! select5-2.2 got:      [1 {no such function: Z}]
select5-2.3...
! select5-2.3 expected: [0 {8 2 9 1 10 1}]
! select5-2.3 got:      [1 {no such function: count}]
select5-2.4...
! select5-2.4 expected: [1 {no such function: z}]
! select5-2.4 got:      [1 {no such function: Z}]
select5-2.5... Ok
select5-3.1...
! select5-3.1 expected: [1 1 5.0 2 1 5.0 3 1 5.0]
! select5-3.1 got:      []
select5-4.1... Ok
select5-4.2... Ok
select5-4.3... Ok
select5-4.4... Ok
select5-4.5... Ok
select5-5.1... Ok
select5-5.2...
! select5-5.2 expected: [1 6]
! select5-5.2 got:      [1 1 6]
select5-5.3... Ok
select5-5.4... Ok
select5-5.5...
! select5-5.5 expected: [1 2 6 4]
! select5-5.5 got:      [1 2 1 4 6 4]
select5-5.11...
! select5-5.11 expected: [3 2 2 1 5 4 4 1 7 24 4 6]
! select5-5.11 got:      [2 {} {} {} 4 {} {} {} 4 {} {} {}]
select5-6.1...
! select5-6.1 expected: [1 4 2 {}]
! select5-6.1 got:      [3 4]
select5-6.2...
! select5-6.2 expected: [1 1 2 {} 2 1 3 {} 3 1 {} 5 4 2 {} 6 5 2 {} {} 6 1 7 8]
! select5-6.2 got:      [2 1 3 {} 5 6 2 {} 6 1 7 8]
select5-7.2...
Error: no such column: cnt
select5-8.1...
! select5-8.1 expected: [one 2 two 1]
! select5-8.1 got:      [one 1]
select5-8.2...
! select5-8.2 expected: [one 2 two 1]
! select5-8.2 got:      [one 1]
select5-8.3...
! select5-8.3 expected: [one 2 two 1]
! select5-8.3 got:      [{} 1]
select5-8.4...
! select5-8.4 expected: [one 2 two 1]
! select5-8.4 got:      [one 1]
select5-8.5...
! select5-8.5 expected: [one 6 two 3]
! select5-8.5 got:      [one 4 two 1]
select5-8.6...
! select5-8.6 expected: [two 1 one 2]
! select5-8.6 got:      [one 1]
select5-8.7...
! select5-8.7 expected: [two 3 one 6]
! select5-8.7 got:      [two 1 one 4]
select5-8.8...
! select5-8.8 expected: [two 3 one 9]
! select5-8.8 got:      [two 1 one 5]
select5-9.1...
! select5-9.1 expected: [NULL NULL | 1 NULL |]
! select5-9.1 got:      [1 NULL | NULL NULL | 1 NULL |]
Running "select5"

Error in select5.test: couldn't read file "select5": no such file or directory
couldn't read file "select5": no such file or directory
    while executing
"source select5"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/select5.test" line 262)
    invoked from within
"source $test_file"

==========================================
Test: select5
Time: 0s
Status: FAILED
