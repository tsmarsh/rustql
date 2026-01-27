# Summary
- Test: sqlite3/test/trans.test
- Repro: `testfixture test/trans.test`
- Failing cases: trans-1.2.4, trans-1.9, trans-1.10, trans-2.1b, trans-2.2, trans-2.4, trans-2.5, trans-2.6, trans-3.1, trans-3.1b, trans-3.1c, trans-3.1d, trans-3.2, trans-3.3, trans-3.4, trans-3.5, trans-3.6, trans-3.7, trans-3.8, trans-3.9, trans-3.10b, trans-3.11, trans-3.12, trans-3.13, trans-3.14, trans-4.2, trans-4.3, trans-4.4, trans-4.5, trans-4.7, trans-4.8, trans-4.9, trans-4.10, trans-4.11, trans-5.2d, trans-5.6, trans-5.7, trans-5.8, trans-5.9, trans-5.10, trans-5.11, trans-5.12, trans-5.13, trans-5.15, trans-5.17, trans-5.22, trans-6.5, trans-6.10, trans-6.14, trans-6.23, trans-6.24, trans-6.25, trans-6.26, trans-6.27, trans-6.28, trans-6.33, trans-6.34, trans-6.35, trans-6.36, trans-6.37, trans-6.38, trans-6.39
- Primary errors: Error: UNIQUE constraint failed: one.a | Error: UNIQUE constraint failed: two.a | ! trans-1.2.4 expected: [-1]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-trans-3641443-1769533960766
DEBUG: tester.tcl sourced, db=db
Running trans.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/trans.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/trans.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
trans-1.0...
Error: UNIQUE constraint failed: one.a
trans-1.0.1... Ok
trans-1.1...
Error: UNIQUE constraint failed: two.a
trans-1.2.1... Ok
trans-1.2.2... Ok
trans-1.2.3... Ok
trans-1.2.4...
! trans-1.2.4 expected: [-1]
! trans-1.2.4 got:      [0]
trans-1.9...
! trans-1.9 expected: [one two three]
! trans-1.9 got:      [one]
trans-1.10...
! trans-1.10 expected: [I V X]
! trans-1.10 got:      [I]
trans-1.11... Ok
trans-2.1... Ok
trans-2.1b...
! trans-2.1b expected: [0]
! trans-2.1b got:      [1]
trans-2.2...
! trans-2.2 expected: [0 {}]
! trans-2.2 got:      [1 {database is locked}]
trans-2.3... Ok
trans-2.4...
! trans-2.4 expected: [0 {}]
! trans-2.4 got:      [1 {database is locked}]
trans-2.5...
! trans-2.5 expected: [0 {}]
! trans-2.5 got:      [1 {near "'foo'": syntax error}]
trans-2.6...
! trans-2.6 expected: [0 {}]
! trans-2.6 got:      [1 {near "'foo'": syntax error}]
trans-2.10...
Error: database is locked
trans-2.11... Ok
trans-3.1...
! trans-3.1 expected: [1 2 3]
! trans-3.1 got:      [{}]
trans-3.1b...
! trans-3.1b expected: [2]
! trans-3.1b got:      [1]
trans-3.1c...
! trans-3.1c expected: [2]
! trans-3.1c got:      [1]
trans-3.1d...
! trans-3.1d expected: [0]
! trans-3.1d got:      [1]
trans-3.2...
! trans-3.2 expected: [0 {1 5 10}]
! trans-3.2 got:      [0 {{}}]
trans-3.3...
! trans-3.3 expected: [0 {1 2 3}]
! trans-3.3 got:      [0 {{}}]
trans-3.4...
! trans-3.4 expected: [0 {}]
! trans-3.4 got:      [1 {database is locked}]
trans-3.5...
! trans-3.5 expected: [0 {1 5 10}]
! trans-3.5 got:      [0 {{}}]
trans-3.6...
! trans-3.6 expected: [0 {1 2 3}]
! trans-3.6 got:      [0 {{}}]
trans-3.7...
! trans-3.7 expected: [0 {}]
! trans-3.7 got:      [1 {database is locked}]
trans-3.8...
! trans-3.8 expected: [0 {1 5 10}]
! trans-3.8 got:      [0 {{}}]
trans-3.9...
! trans-3.9 expected: [0 {1 2 3}]
! trans-3.9 got:      [0 {{}}]
trans-3.10...
Error: cannot commit - no transaction is active
trans-3.10b...
! trans-3.10b expected: [0]
! trans-3.10b got:      [1]
trans-3.11...
! trans-3.11 expected: [0 {1 4 5 10}]
! trans-3.11 got:      [0 {{}}]
trans-3.12...
! trans-3.12 expected: [0 {1 2 3 4}]
! trans-3.12 got:      [0 {{}}]
trans-3.13...
! trans-3.13 expected: [0 {1 4 5 10}]
! trans-3.13 got:      [0 {{}}]
trans-3.14...
! trans-3.14 expected: [0 {1 2 3 4}]
! trans-3.14 got:      [0 {{}}]
trans-3.15... Ok
trans-4.1... Ok
trans-4.2...
! trans-4.2 expected: [1 {cannot rollback - no transaction is active}]
! trans-4.2 got:      [0 {}]
trans-4.3...
! trans-4.3 expected: [0 {1 4 5 10}]
! trans-4.3 got:      [0 {{}}]
trans-4.4...
! trans-4.4 expected: [0 {1 4 5 10}]
! trans-4.4 got:      [0 {{}}]
trans-4.5...
! trans-4.5 expected: [0 {1 2 3 4}]
! trans-4.5 got:      [0 {{}}]
trans-4.6... Ok
trans-4.7...
! trans-4.7 expected: [0 {1 4 5 10}]
! trans-4.7 got:      [0 {{}}]
trans-4.8...
! trans-4.8 expected: [0 {1 2 3 4}]
! trans-4.8 got:      [0 {{}}]
trans-4.9...
! trans-4.9 expected: [0 {1 4 5 10}]
! trans-4.9 got:      [1 {cannot commit - no transaction is active}]
trans-4.10...
! trans-4.10 expected: [0 {1 4 5 10}]
! trans-4.10 got:      [0 {{}}]
trans-4.11...
! trans-4.11 expected: [0 {1 2 3 4}]
! trans-4.11 got:      [0 {{}}]
trans-4.12... Ok
trans-4.98... Ok
trans-4.99... Ok
trans-5.1... Ok
trans-5.2... Ok
trans-5.2b... Ok
trans-5.2c... Ok
trans-5.2d...
! trans-5.2d expected: [0]
! trans-5.2d got:      [1]
trans-5.3... Ok
trans-5.4... Ok
trans-5.5... Ok
trans-5.6...
! trans-5.6 expected: []
! trans-5.6 got:      [one two]
trans-5.7...
! trans-5.7 expected: [1 {no such table: one}]
! trans-5.7 got:      [0 {{} one}]
trans-5.8...
! trans-5.8 expected: []
! trans-5.8 got:      [one sqlite_autoindex_one_1 sqlite_autoindex_two_1 two]
trans-5.9...
! trans-5.9 expected: [t1]
! trans-5.9 got:      [one sqlite_autoindex_one_1 sqlite_autoindex_two_1 t1 two]
trans-5.10...
! trans-5.10 expected: [i1 t1]
! trans-5.10 got:      [i1 one sqlite_autoindex_one_1 sqlite_autoindex_two_1 t1 two]
trans-5.11...
! trans-5.11 expected: [i1 t1]
! trans-5.11 got:      [i1 one sqlite_autoindex_one_1 sqlite_autoindex_two_1 t1 two]
trans-5.12...
! trans-5.12 expected: [i2a i2b t2]
! trans-5.12 got:      [i2a i2b one sqlite_autoindex_one_1 sqlite_autoindex_two_1 t2 two]
trans-5.13...
! trans-5.13 expected: [i1 t1]
! trans-5.13 got:      [one sqlite_autoindex_one_1 sqlite_autoindex_two_1 t1 two]
trans-5.14...
Error: no such index: i1
trans-5.15...
! trans-5.15 expected: [i1 t1]
! trans-5.15 got:      [one sqlite_autoindex_one_1 sqlite_autoindex_two_1 t1 two]
trans-5.16...
Error: no such index: i1
trans-5.17...
! trans-5.17 expected: [i2x i2y t1 t2]
! trans-5.17 got:      [one sqlite_autoindex_one_1 sqlite_autoindex_two_1 t1 two]
trans-5.18...
Error: no such table: t2
trans-5.19...
Error: no such column: y
trans-5.20...
Error: no such table: t2
trans-5.21... Ok
trans-5.22...
! trans-5.22 expected: [i2x i2y t1 t2]
! trans-5.22 got:      [one sqlite_autoindex_one_1 sqlite_autoindex_two_1 two]
trans-5.23...
Error: no such table: t2
trans-5.23... Ok
trans-6.1...
Error: no such table: t1
trans-6.2...
Error: no such table: t1
trans-6.3...
Error: no such table: t1
trans-6.4...
Error: no such table: t1
trans-6.5...
! trans-6.5 expected: [p 1 q 2 r 3]
! trans-6.5 got:      []
trans-6.6... Ok
trans-6.7... Ok
trans-6.10...
! trans-6.10 expected: [a 1 b 2 c 3]
! trans-6.10 got:      []
trans-6.11... Ok
trans-6.12... Ok
trans-6.13... Ok
trans-6.14...
! trans-6.14 expected: [p 1 q 2 r 3]
! trans-6.14 got:      []
trans-6.15... Ok
trans-6.16... Ok
trans-6.20... Ok
trans-6.21... Ok
trans-6.22... Ok
trans-6.23...
! trans-6.23 expected: [4 -5 -6 1 -2 -3]
! trans-6.23 got:      [{} {} {} {} {} {}]
trans-6.24...
! trans-6.24 expected: [4 -5 -6 1 -2 -3]
! trans-6.24 got:      [{} {} {} {} {} {}]
trans-6.25...
! trans-6.25 expected: [1 -2 -3 4 -5 -6]
! trans-6.25 got:      []
trans-6.26...
! trans-6.26 expected: [4 -5 -6 1 -2 -3]
! trans-6.26 got:      []
trans-6.27...
! trans-6.27 expected: [4 -5 -6 1 -2 -3]
! trans-6.27 got:      [{} {} {} {} {} {}]
trans-6.28...
! trans-6.28 expected: [1 -2 -3 4 -5 -6]
! trans-6.28 got:      []
trans-6.30... Ok
trans-6.31... Ok
trans-6.32... Ok
trans-6.33...
! trans-6.33 expected: [4 -5 -6 1 -2 -3]
! trans-6.33 got:      [{} {} {} {} {} {}]
trans-6.34...
! trans-6.34 expected: [4 -5 -6 1 -2 -3]
! trans-6.34 got:      [{} {} {} {} {} {}]
trans-6.35...
! trans-6.35 expected: [1 -2 -3 4 -5 -6]
! trans-6.35 got:      []
trans-6.36...
! trans-6.36 expected: [4 -5 -6 1 -2 -3]
! trans-6.36 got:      []
trans-6.37...
! trans-6.37 expected: [1 -2 -3 4 -5 -6]
! trans-6.37 got:      []
trans-6.38...
! trans-6.38 expected: [4 -5 -6 1 -2 -3]
! trans-6.38 got:      [{} {} {} {} {} {}]
trans-6.39...
! trans-6.39 expected: [1 -2 -3 4 -5 -6]
! trans-6.39 got:      []
trans-6.40... Ok
trans-7.1...
Error: no such table: t2

Error in trans.test: can't read "checksum": no such variable
can't read "checksum": no such variable
    while executing
"do_test trans-7.2 {
  execsql {SELECT md5sum(x,y,z) FROM t2}
} $checksum"
    (file "/tank/repos/rustql-architecture/sqlite3/test/trans.test" line 727)
    invoked from within
"source $test_file"

==========================================
Test: trans
Time: 0s
Status: FAILED
