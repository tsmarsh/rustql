# Summary
- Test: sqlite3/test/select6.test
- Repro: `testfixture test/select6.test`
- Failing cases: select6-1.6, select6-1.7, select6-1.8, select6-1.9, select6-2.6, select6-2.7, select6-2.8, select6-3.7, select6-3.9, select6-4.1, select6-4.2, select6-4.3, select6-5.1, select6-5.2, select6-8.2, select6-8.3, select6-8.4, select6-8.5, select6-8.6, select6-10.3, select6-10.4, select6-10.5, select6-10.6, select6-10.7, select6-10.8, select6-11.1, select6-11.2, select6-11.3, select6-11.4, select6-11.5
- Primary errors: ! select6-1.6 expected: [1 1 1 1 2 2 3 2 4 3 7 3 8 4 15 4 5 5 20 5] | ! select6-1.6 got:      [1 1 1 1 1 1 3 2 1 1 7 3 1 1 15 4 1 1 20 5 2 2 1 1 2 2 3 2 2 2 7 3 2 2 15 4 2 2 20 5 4 3 1 1 4 3 3 2 4 3 7 3 4 3 15 4 4 3 20 5 8 4 1 1 8 4 3 2 8 4 7 3 8 4 15 4 8 4 20 5 5 5 1 1 5 5 3 2 5 5 7 3 5 5 15 4 5 5 20 5] | ! select6-1.7 expected: [1 1 1 1 2 2 3 2 3 4 7 4 4 8 15 8 5 5 20 5]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-select6-3641336-1769533960708
DEBUG: tester.tcl sourced, db=db
Running select6.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/select6.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/select6.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
select6-1.0... Ok
select6-1.1... Ok
select6-1.2... Ok
select6-1.3... Ok
select6-1.4... Ok
select6-1.5... Ok
select6-1.6...
! select6-1.6 expected: [1 1 1 1 2 2 3 2 4 3 7 3 8 4 15 4 5 5 20 5]
! select6-1.6 got:      [1 1 1 1 1 1 3 2 1 1 7 3 1 1 15 4 1 1 20 5 2 2 1 1 2 2 3 2 2 2 7 3 2 2 15 4 2 2 20 5 4 3 1 1 4 3 3 2 4 3 7 3 4 3 15 4 4 3 20 5 8 4 1 1 8 4 3 2 8 4 7 3 8 4 15 4 8 4 20 5 5 5 1 1 5 5 3 2 5 5 7 3 5 5 15 4 5 5 20 5]
select6-1.7...
! select6-1.7 expected: [1 1 1 1 2 2 3 2 3 4 7 4 4 8 15 8 5 5 20 5]
! select6-1.7 got:      [1 1 1 1 1 1 3 1 1 1 7 1 1 1 15 1 1 1 20 1 2 2 1 2 2 2 3 2 2 2 7 2 2 2 15 2 2 2 20 2 3 4 1 4 3 4 3 4 3 4 7 4 3 4 15 4 3 4 20 4 4 8 1 8 4 8 3 8 4 8 7 8 4 8 15 8 4 8 20 8 5 5 1 5 5 5 3 5 5 5 7 5 5 5 15 5 5 5 20 5]
select6-1.8...
! select6-1.8 expected: [1 1 1 2 2 3 3 4 7 4 8 15 5 5 20]
! select6-1.8 got:      [1 1 1 2 2 1 3 4 1 4 8 1 5 5 1 1 1 3 2 2 3 3 4 3 4 8 3 5 5 3 1 1 7 2 2 7 3 4 7 4 8 7 5 5 7 1 1 15 2 2 15 3 4 15 4 8 15 5 5 15 1 1 20 2 2 20 3 4 20 4 8 20 5 5 20]
select6-1.9...
! select6-1.9 expected: [1 1 1 2 2 2 3 4 3 4 7 7 4 8 15 12 5 5 20 21]
! select6-1.9 got:      [1 1 1 {} 2 2 1 {} 3 4 1 {} 4 8 1 {} 5 5 1 {} 1 1 3 {} 2 2 3 {} 3 4 3 {} 4 8 3 {} 5 5 3 {} 1 1 7 {} 2 2 7 {} 3 4 7 {} 4 8 7 {} 5 5 7 {} 1 1 15 {} 2 2 15 {} 3 4 15 {} 4 8 15 {} 5 5 15 {} 1 1 20 {} 2 2 20 {} 3 4 20 {} 4 8 20 {} 5 5 20 {}]
select6-2.0... Ok
select6-2.1... Ok
select6-2.2... Ok
select6-2.3... Ok
select6-2.4... Ok
select6-2.5... Ok
select6-2.6...
! select6-2.6 expected: [1 1 1 1 2 2 3 2 4 3 7 3 8 4 15 4 5 5 20 5]
! select6-2.6 got:      [1 1 1 1 1 1 3 2 1 1 7 3 1 1 15 4 1 1 20 5 2 2 1 1 2 2 3 2 2 2 7 3 2 2 15 4 2 2 20 5 4 3 1 1 4 3 3 2 4 3 7 3 4 3 15 4 4 3 20 5 8 4 1 1 8 4 3 2 8 4 7 3 8 4 15 4 8 4 20 5 5 5 1 1 5 5 3 2 5 5 7 3 5 5 15 4 5 5 20 5]
select6-2.7...
! select6-2.7 expected: [1 1 1 1 2 2 3 2 3 4 7 4 4 8 15 8 5 5 20 5]
! select6-2.7 got:      [1 1 1 1 1 1 3 1 1 1 7 1 1 1 15 1 1 1 20 1 2 2 1 2 2 2 3 2 2 2 7 2 2 2 15 2 2 2 20 2 3 4 1 4 3 4 3 4 3 4 7 4 3 4 15 4 3 4 20 4 4 8 1 8 4 8 3 8 4 8 7 8 4 8 15 8 4 8 20 8 5 5 1 5 5 5 3 5 5 5 7 5 5 5 15 5 5 5 20 5]
select6-2.8...
! select6-2.8 expected: [1 1 1 2 2 3 3 4 7 4 8 15 5 5 20]
! select6-2.8 got:      [1 1 1 2 2 1 3 4 1 4 8 1 5 5 1 1 1 3 2 2 3 3 4 3 4 8 3 5 5 3 1 1 7 2 2 7 3 4 7 4 8 7 5 5 7 1 1 15 2 2 15 3 4 15 4 8 15 5 5 15 1 1 20 2 2 20 3 4 20 4 8 20 5 5 20]
select6-2.9...
Error: no such column: q
select6-3.1... Ok
select6-3.2...
Error: no such column: q
select6-3.3... Ok
select6-3.4... Ok
select6-3.5... Ok
select6-3.6... Ok
select6-3.7...
! select6-3.7 expected: []
! select6-3.7 got:      [10.5 3.7 14.2]
select6-3.8... Ok
select6-3.9...
! select6-3.9 expected: []
! select6-3.9 got:      [11.5 4.0 15.5]
select6-3.10...
Error: no such column: b
select6-3.11...
Error: no such column: b
select6-3.12...
Error: no such column: b
select6-3.13...
Error: no such column: b
select6-3.14... Ok
select6-3.15... Ok
select6-4.1...
! select6-4.1 expected: [8 4 12 9 4 13]
! select6-4.1 got:      [8 4 12 9 4 13 10 4 14 11 4 15 12 4 16 13 4 17 14 4 18 15 4 19]
select6-4.2...
! select6-4.2 expected: [1 2 3 4]
! select6-4.2 got:      [1 2 3 4 5]
select6-4.3...
! select6-4.3 expected: [1 2 3 4]
! select6-4.3 got:      [1 2 3 4 5]
select6-4.4... Ok
select6-4.5... Ok
select6-5.1...
! select6-5.1 expected: [8 5 8 9 6 9 10 7 10]
! select6-5.1 got:      [7 4 8 7 4 9 7 4 10 7 4 11 7 4 12 7 4 13 7 4 14 7 4 15 8 5 8 8 5 9 8 5 10 8 5 11 8 5 12 8 5 13 8 5 14 8 5 15 9 6 8 9 6 9 9 6 10 9 6 11 9 6 12 9 6 13 9 6 14 9 6 15 10 7 8 10 7 9 10 7 10 10 7 11 10 7 12 10 7 13 10 7 14 10 7 15]
select6-5.2...
! select6-5.2 expected: [8 5 8 9 6 9 10 7 10]
! select6-5.2 got:      [7 4 8 7 4 9 7 4 10 7 4 11 7 4 12 7 4 13 7 4 14 7 4 15 8 5 8 8 5 9 8 5 10 8 5 11 8 5 12 8 5 13 8 5 14 8 5 15 9 6 8 9 6 9 9 6 10 9 6 11 9 6 12 9 6 13 9 6 14 9 6 15 10 7 8 10 7 9 10 7 10 10 7 11 10 7 12 10 7 13 10 7 14 10 7 15]
select6-6.1... Ok
select6-6.2... Ok
select6-6.3... Ok
select6-6.4... Ok
select6-6.5... Ok
select6-6.6... Ok
select6-7.1... Ok
select6-7.2... Ok
select6-7.3...
Error: no such column: c
select6-7.4... Ok
select6-8.1... Ok
select6-8.2...
! select6-8.2 expected: [1 1 11 111 2 2 22 222 2 2 22 222]
! select6-8.2 got:      [1 1 11 111 1 2 22 222 2 1 11 111 2 2 22 222 2 1 11 111 2 2 22 222 3 1 11 111 3 2 22 222]
select6-8.3...
! select6-8.3 expected: [1]
! select6-8.3 got:      [0]
select6-8.4...
! select6-8.4 expected: [1 1 11 111 2 2 22 222]
! select6-8.4 got:      [1 1 11 111 1 2 22 222 2 1 11 111 2 2 22 222 3 1 11 111 3 2 22 222]
select6-8.5...
! select6-8.5 expected: [1 1 11 111 111]
! select6-8.5 got:      [1 1 11 111 111 1 2 22 222 111 2 1 11 111 111 2 2 22 222 111 2 1 11 111 111 2 2 22 222 111 3 1 11 111 111 3 2 22 222 111]
select6-8.6...
! select6-8.6 expected: [1]
! select6-8.6 got:      [0]
select6-9.1... Ok
select6-9.2... Ok
select6-9.3... Ok
select6-9.4... Ok
select6-9.5... Ok
select6-9.6... Ok
select6-9.7... Ok
select6-9.8... Ok
select6-9.9... Ok
select6-9.10... Ok
select6-9.11... Ok
select6-10.1... Ok
select6-10.2... Ok
select6-10.3...
! select6-10.3 expected: [1 {SELECTs to the left and right of UNION ALL do not have the same number of result columns}]
! select6-10.3 got:      [0 {}]
select6-10.4...
! select6-10.4 expected: [1 {SELECTs to the left and right of UNION ALL do not have the same number of result columns}]
! select6-10.4 got:      [0 {}]
select6-10.5...
! select6-10.5 expected: [1 {SELECTs to the left and right of UNION ALL do not have the same number of result columns}]
! select6-10.5 got:      [0 {}]
select6-10.6...
! select6-10.6 expected: [1 {SELECTs to the left and right of UNION ALL do not have the same number of result columns}]
! select6-10.6 got:      [0 {}]
select6-10.7...
! select6-10.7 expected: [1 {SELECTs to the left and right of UNION ALL do not have the same number of result columns}]
! select6-10.7 got:      [0 {}]
select6-10.8...
! select6-10.8 expected: [1 {SELECTs to the left and right of UNION ALL do not have the same number of result columns}]
! select6-10.8 got:      [0 {}]
select6-11.1...
! select6-11.1 expected: [1 1 one | 2 2 two | 3 3 three |]
! select6-11.1 got:      [6 {} {} |]
select6-11.2...
! select6-11.2 expected: [1 1 one | 2 2 two | 3 3 three |]
! select6-11.2 got:      [6 {} {} |]
select6-11.3...
! select6-11.3 expected: [1 1 | 3 3 |]
! select6-11.3 got:      [6 {} |]
select6-11.4...
! select6-11.4 expected: [1 1 | 3 3 | 2 2 |]
! select6-11.4 got:      [6 {} |]
select6-11.5...
! select6-11.5 expected: [1 1 bbb | 2 2 aaa | 3 3 bbb |]
! select6-11.5 got:      [6 {} bbb |]
select6-11.100... Ok
select6-12.100... Ok
select6-13.100... Ok
select6-13.110... Ok
select6-13.120... Ok
Running "select6"

Error in select6.test: couldn't read file "select6": no such file or directory
couldn't read file "select6": no such file or directory
    while executing
"source select6"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/select6.test" line 670)
    invoked from within
"source $test_file"

==========================================
Test: select6
Time: 0s
Status: FAILED
