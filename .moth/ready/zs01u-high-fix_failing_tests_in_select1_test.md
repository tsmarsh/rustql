# Summary
- Test: sqlite3/test/select1.test
- Repro: `testfixture test/select1.test`
- Failing cases: select1-6.9.3, select1-7.2, select1-9.2, select1-9.3, select1-9.5, select1-14.1, select1-16.1, select1-16.2, select1-19.20, select1-19.21, select1-20.20
- Primary errors: Error: UNIQUE constraint failed: tkt2526.c | ! select1-6.9.3 expected: [{test1 . f1} 11 {test1 . f2} 22] | ! select1-6.9.3 got:      [test1.f1 11 test1.f2 22]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-select1-3641328-1769533960708
DEBUG: tester.tcl sourced, db=db
Running select1.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/select1.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/select1.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
select1-1.1... Ok
select1-1.2... Ok
select1-1.3... Ok
select1-1.4... Ok
select1-1.5... Ok
select1-1.6... Ok
select1-1.7... Ok
select1-1.8... Ok
select1-1.8.1... Ok
select1-1.8.2... Ok
select1-1.8.3... Ok
select1-1.9... Ok
select1-1.9.1... Ok
select1-1.9.2... Ok
select1-1.10... Ok
select1-1.11... Ok
select1-1.11.1... Ok
select1-1.11.2... Ok
select1-1.12... Ok
select1-1.13... Ok
select1-2.0... Ok
select1-2.1... Ok
select1-2.2... Ok
select1-2.3... Ok
select1-2.4... Ok
select1-2.5... Ok
select1-2.5.1... Ok
select1-2.5.2... Ok
select1-2.5.3... Ok
select1-2.6... Ok
select1-2.7... Ok
select1-2.8... Ok
select1-2.8.1... Ok
select1-2.8.2... Ok
select1-2.8.3... Ok
select1-2.9... Ok
select1-2.10... Ok
select1-2.11... Ok
select1-2.12... Ok
select1-2.13... Ok
select1-2.13.1... Ok
select1-2.13.2... Ok
select1-2.14... Ok
select1-2.15... Ok
select1-2.16... Ok
select1-2.17... Ok
select1-2.17.1... Ok
select1-2.18... Ok
select1-2.19... Ok
select1-2.20... Ok
select1-2.21... Ok
select1-2.22... Ok
select1-2.23...
Error: UNIQUE constraint failed: tkt2526.c
select1-3.1... Ok
select1-3.2... Ok
select1-3.3... Ok
select1-3.4... Ok
select1-3.5... Ok
select1-3.6... Ok
select1-3.7... Ok
select1-3.8... Ok
select1-3.9... Ok
select1-4.1... Ok
select1-4.2... Ok
select1-4.3... Ok
select1-4.4... Ok
select1-4.5... Ok
select1-4.5... Ok
select1-4.6... Ok
select1-4.8... Ok
select1-4.9.1... Ok
select1-4.9.2... Ok
select1-4.10.1... Ok
select1-4.10.2... Ok
select1-4.11... Ok
select1-4.12... Ok
select1-4.13... Ok
select1-5.1... Ok
select1-6.1... Ok
select1-6.1.1... Ok
select1-6.1.2... Ok
select1-6.1.3... Ok
select1-6.1.4... Ok
select1-6.1.5... Ok
select1-6.1.6... Ok
select1-6.2... Ok
select1-6.3... Ok
select1-6.3.1... Ok
select1-6.4... Ok
select1-6.4a... Ok
select1-6.5... Ok
select1-6.5.1... Ok
select1-6.6... Ok
select1-6.7... Ok
select1-6.8... Ok
select1-6.8b... Ok
select1-6.8c... Ok
select1-6.9.1... Ok
select1-6.9.2... Ok
select1-6.9.3...
! select1-6.9.3 expected: [{test1 . f1} 11 {test1 . f2} 22]
! select1-6.9.3 got:      [test1.f1 11 test1.f2 22]
select1-6.9.4... Ok
select1-6.9.5... Ok
select1-6.9.6... Ok
select1-6.9.7... Ok
select1-6.9.8... Ok
select1-6.9.9... Ok
select1-6.9.10... Ok
select1-6.9.11... Ok
select1-6.9.12... Ok
select1-6.9.13... Ok
select1-6.9.14... Ok
select1-6.9.15... Ok
select1-6.9.16... Ok
select1-6.10... Ok
select1-6.11... Ok
select1-6.20...
Error: no such column: a
select1-6.21...
Error: no such column: a
select1-6.22...
Error: no such column: a
select1-6.23...
Error: no such column: a
select1-7.1... Ok
select1-7.2...
! select1-7.2 expected: [1 {near "WHERE": syntax error}]
! select1-7.2 got:      [1 {no such column: WHERE}]
select1-7.3... Ok
select1-7.4... Ok
select1-7.5... Ok
select1-7.6... Ok
select1-7.7... Ok
select1-7.8... Ok
select1-7.9... Ok
select1-8.1... Ok
select1-8.2... Ok
select1-8.3... Ok
select1-8.5... Ok
select1-9.2...
! select1-9.2 expected: [f1 f2]
! select1-9.2 got:      []
select1-9.3...
! select1-9.3 expected: [f1 f2]
! select1-9.3 got:      []
select1-9.4... Ok
select1-9.5...
! select1-9.5 expected: [f1 f2]
! select1-9.5 got:      []
select1-10.1... Ok
select1-10.2... Ok
select1-10.3... Ok
select1-10.4... Ok
select1-10.5... Ok
select1-10.6... Ok
select1-10.7... Ok
select1-11.1... Ok
select1-11.2.1... Ok
select1-11.2.2... Ok
select1-11.4.1... Ok
select1-11.4.2... Ok
select1-11.5.1... Ok
select1-11.6... Ok
select1-11.7... Ok
select1-11.8... Ok
select1-11.9... Ok
select1-11.10... Ok
select1-11.11... Ok
select1-11.12... Ok
select1-11.13... Ok
select1-11.14... Ok
select1-11.15... Ok
select1-11.16... Ok
select1-12.1... Ok
select1-12.2... Ok
select1-12.3... Ok
select1-12.4... Ok
select1-12.5... Ok
select1-12.6... Ok
select1-12.7... Ok
select1-12.8... Ok
select1-12.9... Ok
select1-12.10... Ok
select1-13.1... Ok
select1-14.1...
! select1-14.1 expected: []
! select1-14.1 got:      [table test1 test1 2 {CREATE TABLE test1 (f1 int, f2 int)} table t5 t5 8 {CREATE TABLE t5 (a, b)} table t3 t3 4 {CREATE TABLE t3 (a, b)} table t4 t4 5 {CREATE TABLE t4 (a, b)} table tkt2526 tkt2526 6 {CREATE TABLE tkt2526 (a, b, c PRIMARY KEY)} table test2 test2 3 {CREATE TABLE test2 (r1 real, r2 real)} table abc abc 11 {CREATE TABLE abc (a, b, c, PRIMARY KEY (a, b))} table t6 t6 10 {CREATE TABLE t6 (a TEXT, b TEXT)} index sqlite_autoindex_tkt2526_1 tkt2526 0 {CREATE UNIQUE INDEX sqlite_autoindex_tkt2526_1 ON tkt2526(c)}]
select1-14.2... Ok
select1-16.1...
! select1-16.1 expected: [1 {no tables specified}]
! select1-16.1 got:      [0 1]
select1-16.2...
! select1-16.2 expected: [1 {near "#1": syntax error}]
! select1-16.2 got:      [1 {unexpected character '#' at line 2}]
select1-17.1... Ok
select1-17.2... Ok
select1-17.3... Ok
select1-18.1...
Error: no such column: x
select1-18.2...
Error: no such column: x
select1-18.3...
Error: no such column: c
select1-18.4...
Error: no such column: c
select1-19.10... Ok
select1-19.20...
! select1-19.20 expected: [1 {table t1 has 1 columns but 7 values were supplied}]
! select1-19.20 got:      [0 {}]
select1-19.21...
! select1-19.21 expected: [1 {table t1 has 1 columns but 15 values were supplied}]
! select1-19.21 got:      [0 {}]
select1-20.10...
Error: ambiguous column name: t1.a
select1-20.20...
! select1-20.20 expected: [10 1]
! select1-20.20 got:      [123 123]
select1-21.1...
Error: no such column: z
Running "select1"

Error in select1.test: couldn't read file "select1": no such file or directory
couldn't read file "select1": no such file or directory
    while executing
"source select1"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/select1.test" line 1213)
    invoked from within
"source $test_file"

==========================================
Test: select1
Time: 1s
Status: FAILED
