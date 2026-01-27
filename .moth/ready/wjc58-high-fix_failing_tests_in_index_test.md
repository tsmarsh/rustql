# Summary
- Test: sqlite3/test/index.test
- Repro: `testfixture test/index.test`
- Failing cases: index-1.1c, index-1.1d, index-2.1, index-2.1b, index-2.2, index-5.1, index-6.2, index-6.2b, index-7.2, index-10.5, index-10.6, index-10.7, index-10.8, index-11.1, index-12.1, index-12.2, index-12.3, index-12.4, index-12.5, index-12.6, index-12.7, index-13.3.0, index-13.3.1, index-13.3.2, index-14.4, index-14.8, index-14.9, index-14.10, index-14.11, index-15.2, index-15.3, index-16.1, index-16.2, index-16.3, index-16.5, index-17.1, index-17.2, index-17.3, index-18.2, index-18.3, index-18.4, index-19.3, index-19.6, index-21.1, index-21.2
- Primary errors: ! index-1.1c expected: [index1 {CREATE INDEX index1 ON test1(f1)} test1 index] | ! index-1.1c got:      [] | ! index-1.1d expected: [index1 test1]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-index-3641375-1769533960726
DEBUG: tester.tcl sourced, db=db
Running index.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/index.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/index.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
index-1.1... Ok
index-1.1b... Ok
index-1.1c...
! index-1.1c expected: [index1 {CREATE INDEX index1 ON test1(f1)} test1 index]
! index-1.1c got:      []
index-1.1d...
! index-1.1d expected: [index1 test1]
! index-1.1d got:      [test1]
index-1.2... Ok
index-2.1...
! index-2.1 expected: [1 {no such table: main.test1}]
! index-2.1 got:      [1 {no such table: test1}]
index-2.1b...
! index-2.1b expected: [1 {no such column: f4}]
! index-2.1b got:      [0 {}]
index-2.2...
! index-2.2 expected: [1 {no such column: f4}]
! index-2.2 got:      [1 {index index1 already exists}]
index-3.1... Ok
index-3.2.1... Ok
index-3.2.2... Ok
index-3.2.3... Ok
index-3.3... Ok
index-4.1... Ok
index-4.2... Ok
index-4.3... Ok
index-4.4... Ok
index-4.5... Ok
index-4.6... Ok
index-4.7... Ok
index-4.8... Ok
index-4.9... Ok
index-4.10... Ok
index-4.11... Ok
index-4.12... Ok
index-4.13... Ok
index-4.14... Ok
index-5.1...
! index-5.1 expected: [1 {table sqlite_master may not be indexed}]
! index-5.1 got:      [1 {no such table: sqlite_master}]
index-5.2... Ok
index-6.1... Ok
index-6.1.1... Ok
index-6.1b... Ok
index-6.1c... Ok
index-6.2...
! index-6.2 expected: [1 {there is already a table named test1}]
! index-6.2 got:      [0 {}]
index-6.2b...
! index-6.2b expected: [index1 test1 test2]
! index-6.2b got:      [index1 test1 test1 test2]
index-6.3... Ok
index-6.4... Ok
index-6.5... Ok
index-7.1...
Error: UNIQUE constraint failed: test1.f2
index-7.2...
! index-7.2 expected: [16]
! index-7.2 got:      []
index-7.3... Ok
index-7.4... Ok
index-7.5... Ok
index-8.1... Ok
index-9.1... Ok
index-9.2... Ok
index-9.3... Ok
index-10.0... Ok
index-10.1... Ok
index-10.2... Ok
index-10.3... Ok
index-10.4... Ok
index-10.5...
! index-10.5 expected: [1 3 5 7 9]
! index-10.5 got:      [1 2 3 4 5 6 7 8 9]
index-10.6...
! index-10.6 expected: [1]
! index-10.6 got:      [1 2]
index-10.7...
! index-10.7 expected: []
! index-10.7 got:      [2]
index-10.8...
! index-10.8 expected: [0]
! index-10.8 got:      [0 2]
index-10.9... Ok
index-11.1...
! index-11.1 expected: [0.1 2]
! index-11.1 got:      [0.1 49]
index-11.2... Ok
index-12.1...
! index-12.1 expected: [0 0 abc -1 1 0 0]
! index-12.1 got:      [0.0 0.0 abc -1.0 1.0 0 0]
index-12.2...
! index-12.2 expected: [0 0 0 0]
! index-12.2 got:      [0.0 0.0 0 0]
index-12.3...
! index-12.3 expected: [0 0 -1 0 0]
! index-12.3 got:      [0.0 0.0 -1.0 0 0]
index-12.4...
! index-12.4 expected: [0 0 abc 1 0 0]
! index-12.4 got:      [0.0 0.0 abc 1.0 0 0]
index-12.5...
! index-12.5 expected: [0 0 0 0]
! index-12.5 got:      [0.0 0.0 0 0]
index-12.6...
! index-12.6 expected: [0 0 -1 0 0]
! index-12.6 got:      [0.0 0.0 -1.0 0 0]
index-12.7...
! index-12.7 expected: [0 0 abc 1 0 0]
! index-12.7 got:      [0.0 0.0 abc 1.0 0 0]
index-12.8... Ok
index-13.1... Ok
index-13.2... Ok
index-13.3.0...
! index-13.3.0 expected: [1 {index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped}]
! index-13.3.0 got:      [1 {index sqlite_autoindex_t5_1 may not be dropped}]
index-13.3.1...
! index-13.3.1 expected: [1 {index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped}]
! index-13.3.1 got:      [1 {index sqlite_autoindex_t5_2 may not be dropped}]
index-13.3.2...
! index-13.3.2 expected: [1 {index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped}]
! index-13.3.2 got:      [1 {index sqlite_autoindex_t5_3 may not be dropped}]
index-13.4... Ok
index-13.5... Ok
index-14.1... Ok
index-14.2... Ok
index-14.3... Ok
index-14.4...
! index-14.4 expected: [4]
! index-14.4 got:      [1 4]
index-14.5... Ok
index-14.6... Ok
index-14.7... Ok
index-14.8...
! index-14.8 expected: [5 2 1]
! index-14.8 got:      [3 5 2 1]
index-14.9...
! index-14.9 expected: [5 2 1 4]
! index-14.9 got:      [3 5 2 1 4]
index-14.10...
! index-14.10 expected: [5 2 1]
! index-14.10 got:      [3 5 2 1]
index-14.11...
! index-14.11 expected: [5]
! index-14.11 got:      [3 5]
index-14.12... Ok
index-15.1... Ok
index-15.2...
! index-15.2 expected: [13 14 15 12 8 5 2 1 3 6 10 11 9 4 7]
! index-15.2 got:      [9 12 8 10 11 13 15 14 1 5 2 4 3 7 6]
index-15.3...
! index-15.3 expected: [1 2 3 5 6 8 10 11 12 13 14 15]
! index-15.3 got:      []
index-15.4... Ok
index-16.1...
! index-16.1 expected: [1]
! index-16.1 got:      [2]
index-16.2...
! index-16.2 expected: [1]
! index-16.2 got:      [2]
index-16.3...
! index-16.3 expected: [1]
! index-16.3 got:      [2]
index-16.4... Ok
index-16.5...
! index-16.5 expected: [2]
! index-16.5 got:      [1]
index-17.1...
! index-17.1 expected: [sqlite_autoindex_t7_1 sqlite_autoindex_t7_2 sqlite_autoindex_t7_3]
! index-17.1 got:      [sqlite_autoindex_t7_1 sqlite_autoindex_t7_2]
index-17.2...
! index-17.2 expected: [1 {index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped}]
! index-17.2 got:      [1 {index sqlite_autoindex_t7_1 may not be dropped}]
index-17.3...
! index-17.3 expected: [1 {index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped}]
! index-17.3 got:      [1 {index sqlite_autoindex_t7_1 may not be dropped}]
index-17.4... Ok
index-18.1... Ok
index-18.1.2... Ok
index-18.2...
! index-18.2 expected: [1 {object name reserved for internal use: sqlite_i1}]
! index-18.2 got:      [0 {}]
index-18.3...
! index-18.3 expected: [1 {object name reserved for internal use: sqlite_v1}]
! index-18.3 got:      [0 {}]
index-18.4...
! index-18.4 expected: [1 {object name reserved for internal use: sqlite_tr1}]
! index-18.4 got:      [0 {}]
index-18.5... Ok
index-19.1... Ok
index-19.2... Ok
index-19.3...
! index-19.3 expected: [1 {cannot start a transaction within a transaction}]
! index-19.3 got:      [0 {}]
index-19.4... Ok
index-19.5... Ok
index-19.6...
! index-19.6 expected: [1 {conflicting ON CONFLICT clauses specified}]
! index-19.6 got:      [0 {}]
index-19.7... Ok
index-19.8... Ok
index-20.1... Ok
index-20.2... Ok
index-21.1...
! index-21.1 expected: [1 {cannot create a TEMP index on non-TEMP table "t6"}]
! index-21.1 got:      [0 {}]
index-21.2...
! index-21.2 expected: [0 {9 5 1}]
! index-21.2 got:      [1 {table "t6" already exists}]
index-22.0...
Error: near "==": syntax error
index-23.0...
Error: near "GLOB": syntax error
index-23.1...
Error: UNIQUE constraint failed: t1.index_0
Running "index"

Error in index.test: couldn't read file "index": no such file or directory
couldn't read file "index": no such file or directory
    while executing
"source index"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/index.test" line 769)
    invoked from within
"source $test_file"

==========================================
Test: index
Time: 0s
Status: FAILED
