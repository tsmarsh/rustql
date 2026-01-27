# Summary
- Test: sqlite3/test/select4.test
- Repro: `testfixture test/select4.test`
- Failing cases: select4-1.1d, select4-1.1e, select4-1.1g, select4-1.2, select4-2.2, select4-3.1.2, select4-3.1.3, select4-3.2, select4-4.1.3, select4-4.1.4, select4-4.2, select4-5.1, select4-5.2c, select4-5.2d, select4-5.3, select4-5.3-3807-1, select4-7.1, select4-9.1, select4-9.2, select4-9.3, select4-9.4, select4-9.9.2, select4-9.10, select4-9.11, select4-11.1, select4-11.2, select4-11.3, select4-11.4, select4-11.5, select4-11.6, select4-11.7, select4-11.8, select4-11.11, select4-11.12, select4-11.13, select4-11.14, select4-11.15, select4-11.16, select4-12.1, select4-17.3
- Primary errors: ! select4-1.1d expected: [0 1 2 3 4 5 5 6 7 8] | ! select4-1.1d got:      [] | ! select4-1.1e expected: [8 7 6 5 5 4 3 2 1 0]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-select4-3641332-1769533960708
DEBUG: tester.tcl sourced, db=db
Running select4.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/select4.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/select4.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
select4-1.0... Ok
select4-1.1a... Ok
select4-1.1b... Ok
select4-1.1c... Ok
select4-1.1d...
! select4-1.1d expected: [0 1 2 3 4 5 5 6 7 8]
! select4-1.1d got:      []
select4-1.1e...
! select4-1.1e expected: [8 7 6 5 5 4 3 2 1 0]
! select4-1.1e got:      []
select4-1.1f... Ok
select4-1.1g...
! select4-1.1g expected: [0 1 2 3 4 5 3 4]
! select4-1.1g got:      []
select4-1.2...
! select4-1.2 expected: [0 1 2 2 3 3 3 3]
! select4-1.2 got:      []
select4-1.3... Ok
select4-1.4... Ok
select4-2.1... Ok
select4-2.2...
! select4-2.2 expected: [0 1 2 2 3 3 3 3]
! select4-2.2 got:      []
select4-2.3... Ok
select4-2.4... Ok
select4-2.5... Ok
select4-3.1.1... Ok
select4-3.1.2...
! select4-3.1.2 expected: [0 1 2 3 4]
! select4-3.1.2 got:      []
select4-3.1.3...
! select4-3.1.3 expected: [4 3 2 1 0]
! select4-3.1.3 got:      []
select4-3.2...
! select4-3.2 expected: [0 1 2 2]
! select4-3.2 got:      []
select4-3.3... Ok
select4-4.1.1... Ok
select4-4.1.2... Ok
select4-4.1.3...
! select4-4.1.3 expected: [5 6]
! select4-4.1.3 got:      []
select4-4.1.4...
! select4-4.1.4 expected: [6 5]
! select4-4.1.4 got:      []
select4-4.2...
! select4-4.2 expected: [3]
! select4-4.2 got:      []
select4-4.3... Ok
select4-4.4... Ok
select4-5.1...
! select4-5.1 expected: [1 {no such table: t2}]
! select4-5.1 got:      [1 {no such column: log}]
select4-5.2... Ok
select4-5.2b... Ok
select4-5.2c...
! select4-5.2c expected: [1 {1st ORDER BY term does not match any column in the result set}]
! select4-5.2c got:      [1 {no such column: xyzzy}]
select4-5.2d...
! select4-5.2d expected: [1 {1st ORDER BY term does not match any column in the result set}]
! select4-5.2d got:      [1 {no such column: xyzzy}]
select4-5.2e... Ok
select4-5.2f... Ok
select4-5.2g... Ok
select4-5.2h... Ok
select4-5.2i... Ok
select4-5.2j... Ok
select4-5.2k... Ok
select4-5.3...
! select4-5.3 expected: [1 {SELECTs to the left and right of UNION ALL do not have the same number of result columns}]
! select4-5.3 got:      [0 {0 1 1 2 2 3 2 4 3 5 3 6 3 7 3 8 4 9 4 10 4 11 4 12 4 13 4 14 4 15 4 16 5 17 5 18 5 19 5 20 5 21 5 22 5 23 5 24 5 25 5 26 5 27 5 28 5 29 5 30 5 31 5 {} 6 {} 7 {} 8 {}}]
select4-5.3-3807-1...
! select4-5.3-3807-1 expected: [1 {SELECTs to the left and right of UNION do not have the same number of result columns}]
! select4-5.3-3807-1 got:      [0 {1 2 4}]
select4-5.4... Ok
select4-6.1... Ok
select4-6.2...
Error: misuse of aggregate: count()
select4-6.3... Ok
select4-6.3.1... Ok
select4-6.4... Ok
select4-6.5... Ok
select4-6.6... Ok
select4-6.7... Ok
select4-7.1...
! select4-7.1 expected: [0 1 1 1 2 2 3 4 4 8 5 15]
! select4-7.1 got:      []
select4-7.2...
Error: no such column: n
select4-7.3...
Error: no such column: n
select4-7.4...
Error: no such column: n
select4-8.1... Ok
select4-8.2... Ok
select4-9.1...
! select4-9.1 expected: [x 0 y 1]
! select4-9.1 got:      [x 1 y 1.1]
select4-9.2...
! select4-9.2 expected: [x 0 y 1]
! select4-9.2 got:      [x 1 y 1.1]
select4-9.3...
! select4-9.3 expected: [x 0 y 1]
! select4-9.3 got:      []
select4-9.4...
! select4-9.4 expected: [x 0 y 1]
! select4-9.4 got:      []
select4-9.5... Ok
select4-9.6... Ok
select4-9.7... Ok
select4-9.8... Ok
select4-9.9.1... Ok
select4-9.9.2...
! select4-9.9.2 expected: []
! select4-9.9.2 got:      [a 1 b 2 a 3 b 4]
select4-9.10...
! select4-9.10 expected: [a 1 b 2]
! select4-9.10 got:      [a 1 b 2 a 3 b 4]
select4-9.11...
! select4-9.11 expected: [a 1 b 2]
! select4-9.11 got:      [a 1 b 2 a 3 b 4]
select4-9.12... Ok
select4-10.1... Ok
select4-10.2... Ok
select4-10.3... Ok
select4-10.4... Ok
select4-10.5... Ok
select4-10.6... Ok
select4-10.7... Ok
select4-10.8... Ok
select4-10.9... Ok
select4-11.1...
! select4-11.1 expected: [1 {SELECTs to the left and right of UNION do not have the same number of result columns}]
! select4-11.1 got:      [0 {}]
select4-11.2...
! select4-11.2 expected: [1 {SELECTs to the left and right of UNION do not have the same number of result columns}]
! select4-11.2 got:      [0 {}]
select4-11.3...
! select4-11.3 expected: [1 {SELECTs to the left and right of UNION ALL do not have the same number of result columns}]
! select4-11.3 got:      [0 {}]
select4-11.4...
! select4-11.4 expected: [1 {SELECTs to the left and right of UNION ALL do not have the same number of result columns}]
! select4-11.4 got:      [0 {}]
select4-11.5...
! select4-11.5 expected: [1 {SELECTs to the left and right of EXCEPT do not have the same number of result columns}]
! select4-11.5 got:      [0 {}]
select4-11.6...
! select4-11.6 expected: [1 {SELECTs to the left and right of EXCEPT do not have the same number of result columns}]
! select4-11.6 got:      [0 {}]
select4-11.7...
! select4-11.7 expected: [1 {SELECTs to the left and right of INTERSECT do not have the same number of result columns}]
! select4-11.7 got:      [0 {}]
select4-11.8...
! select4-11.8 expected: [1 {SELECTs to the left and right of INTERSECT do not have the same number of result columns}]
! select4-11.8 got:      [0 {}]
select4-11.11...
! select4-11.11 expected: [1 {SELECTs to the left and right of INTERSECT do not have the same number of result columns}]
! select4-11.11 got:      [0 {}]
select4-11.12...
! select4-11.12 expected: [1 {SELECTs to the left and right of EXCEPT do not have the same number of result columns}]
! select4-11.12 got:      [0 {}]
select4-11.13...
! select4-11.13 expected: [1 {SELECTs to the left and right of UNION ALL do not have the same number of result columns}]
! select4-11.13 got:      [0 {}]
select4-11.14...
! select4-11.14 expected: [1 {SELECTs to the left and right of UNION do not have the same number of result columns}]
! select4-11.14 got:      [0 {}]
select4-11.15...
! select4-11.15 expected: [1 {SELECTs to the left and right of UNION do not have the same number of result columns}]
! select4-11.15 got:      [0 {}]
select4-11.16...
! select4-11.16 expected: [1 {SELECTs to the left and right of UNION do not have the same number of result columns}]
! select4-11.16 got:      [1 {near "UNION": syntax error}]
select4-12.1...
! select4-12.1 expected: [1 {SELECTs to the left and right of UNION do not have the same number of result columns}]
! select4-12.1 got:      [0 {1 2 4}]
select4-13.1...
Error: no such database: db
select4-14.1...
Error: no such database: db
select4-14.2...
Error: no such database: db
select4-14.3...
Error: no such database: db
select4-14.4...
Error: no such database: db
select4-14.5...
Error: no such database: db
select4-14.6...
Error: no such database: db
select4-14.7...
Error: no such database: db
select4-14.8...
Error: no such database: db
select4-14.9...
Error: no such database: db
select4-14.10...
Error: no such database: db
select4-14.11...
Error: no such database: db
select4-14.12...
Error: no such database: db
select4-14.13...
Error: no such database: db
select4-14.14...
Error: no such database: db
select4-14.15...
Error: no such database: db
select4-14.16...
Error: no such database: db
select4-14.17...
Error: no such database: db
select4-15.1...
Error: no such database: db
select4-16.1...
Error: no such database: db
select4-16.2...
Error: no such database: db
select4-16.3...
Error: no such database: db
select4-17.1...
Error: no such database: db
select4-17.2...
Error: no such database: db
select4-17.3...
! select4-17.3 expected: [1 {LIMIT clause should come after UNION not before}]
! select4-17.3 got:      [1 {no such database: db}]
select4-18.1...
Error: near "WITH": syntax error
select4-18.2...
Error: near "WITH": syntax error
select4-18.3...
Error: no such column: z1.aa
select4-19.1... Ok
Running "select4"

Error in select4.test: couldn't read file "select4": no such file or directory
couldn't read file "select4": no such file or directory
    while executing
"source select4"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/select4.test" line 1043)
    invoked from within
"source $test_file"

==========================================
Test: select4
Time: 0s
Status: FAILED
