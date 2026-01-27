# Summary
- Test: sqlite3/test/attach.test
- Repro: `testfixture test/attach.test`
- Failing cases: attach-1.3.1, attach-1.3.2, attach-1.3.3, attach-1.3.4, attach-1.3.5, attach-1.7, attach-1.9, attach-1.11b, attach-1.12.2, attach-1.15, attach-1.16, attach-1.17
- Primary errors: Error: no such table: t2 | ! attach-1.3.1 expected: [test.db] | ! attach-1.3.1 got:      [0]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-attach-3641431-1769533960756
DEBUG: tester.tcl sourced, db=db
Running attach.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/attach.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/attach.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
attach-1.1... Ok
attach-1.2... Ok
attach-1.3...
Error: no such table: t2
attach-1.3.1...
! attach-1.3.1 expected: [test.db]
! attach-1.3.1 got:      [0]
attach-1.3.2...
! attach-1.3.2 expected: [test.db]
! attach-1.3.2 got:      [0]
attach-1.3.3...
! attach-1.3.3 expected: []
! attach-1.3.3 got:      [0]
attach-1.3.4...
! attach-1.3.4 expected: [test2.db]
! attach-1.3.4 got:      [0]
attach-1.3.5...
! attach-1.3.5 expected: []
! attach-1.3.5 got:      [0]
attach-1.4...
Error: no such table: t2
attach-1.5... Ok
attach-1.6... Ok
attach-1.7...
! attach-1.7 expected: [1 {no such table: two.t2}]
! attach-1.7 got:      [1 {no such table: t2}]
attach-1.8... Ok
attach-1.9...
! attach-1.9 expected: [0 {}]
! attach-1.9 got:      [0 {table t1 t1 2 {CREATE TABLE t1 (a, b)}}]
attach-1.10... Ok
attach-1.11... Ok
attach-1.11b...
! attach-1.11b expected: [0 main 2 db2 3 db3 4 db4 5 db5 6 db6 7 db7 8 db8 9 db9]
! attach-1.11b got:      [0 main 1 temp 2 db2 3 db3 4 db4 5 db5 6 db6 7 db7 8 db8 9 db9]
attach-1.12... Ok
attach-1.12.2...
! attach-1.12.2 expected: [1]
! attach-1.12.2 got:      [0]
attach-1.13... Ok
attach-1.14... Ok
attach-1.15...
! attach-1.15 expected: [1 {database main is already in use}]
! attach-1.15 got:      [1 {cannot attach database main}]
attach-1.16...
! attach-1.16 expected: [1 {database temp is already in use}]
! attach-1.16 got:      [1 {cannot attach database temp}]
attach-1.17...
! attach-1.17 expected: [1 {database MAIN is already in use}]
! attach-1.17 got:      [1 {cannot attach database MAIN}]
attach-1.18... Ok

Error in attach.test: can't read "SQLITE_MAX_ATTACHED": no such variable
can't read "SQLITE_MAX_ATTACHED": no such variable
    while executing
"if {$SQLITE_MAX_ATTACHED==10} {
  do_test attach-1.19 {
    catchsql {
      ATTACH 'test.db' as db12;
    }
  } {1 {too many attached databases - max..."
    (file "/tank/repos/rustql-architecture/sqlite3/test/attach.test" line 170)
    invoked from within
"source $test_file"

==========================================
Test: attach
Time: 0s
Status: FAILED
