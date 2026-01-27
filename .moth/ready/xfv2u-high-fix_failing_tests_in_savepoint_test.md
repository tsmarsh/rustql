# Summary
- Test: sqlite3/test/savepoint.test
- Repro: `testfixture test/savepoint.test`
- Failing cases: savepoint-1.4.1, savepoint-1.4.3, savepoint-1.4.5, savepoint-1.4.7, savepoint-2.2, savepoint-2.3, savepoint-2.4, savepoint-2.5, savepoint-2.6, savepoint-2.7, savepoint-2.8, savepoint-2.9, savepoint-2.10, savepoint-2.11, savepoint-3.1, savepoint-3.2, savepoint-3.3, savepoint-3.4, savepoint-3.5, savepoint-4.1, savepoint-4.4, savepoint-4.5, savepoint-5.3.2.3, savepoint-5.4.3, savepoint-10.1.1, savepoint-10.1.2, savepoint-10.2.1, savepoint-10.2.3, savepoint-10.2.4, savepoint-10.2.5, savepoint-10.2.7, savepoint-10.2.8, savepoint-10.2.9, savepoint-10.2.10, savepoint-10.2.11, savepoint-10.2.12, savepoint-10.2.13, savepoint-11.8, savepoint-12.3, savepoint-13.1, savepoint-13.4
- Primary errors: ! savepoint-1.4.1 expected: [1] | ! savepoint-1.4.1 got:      [0] | ! savepoint-1.4.3 expected: [1]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-savepoint-3641449-1769533960769
DEBUG: tester.tcl sourced, db=db
Running savepoint.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/savepoint.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/savepoint.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
savepoint-1.1... Ok
savepoint-1.2... Ok
savepoint-1.3... Ok
savepoint-1.4.1...
! savepoint-1.4.1 expected: [1]
! savepoint-1.4.1 got:      [0]
savepoint-1.4.2... Ok
savepoint-1.4.3...
! savepoint-1.4.3 expected: [1]
! savepoint-1.4.3 got:      [0]
savepoint-1.4.4... Ok
savepoint-1.4.5...
! savepoint-1.4.5 expected: [1]
! savepoint-1.4.5 got:      [0]
savepoint-1.4.6... Ok
savepoint-1.4.7...
! savepoint-1.4.7 expected: [1]
! savepoint-1.4.7 got:      [0]
savepoint-1.5... Ok
savepoint-1.6... Ok
savepoint-2.1... Ok
savepoint-2.2...
! savepoint-2.2 expected: [1 2 3]
! savepoint-2.2 got:      [2 3 4]
savepoint-2.3...
! savepoint-2.3 expected: [1 2 3 4 5 6]
! savepoint-2.3 got:      [2 3 4 4 5 6]
savepoint-2.4...
! savepoint-2.4 expected: [1 2 3]
! savepoint-2.4 got:      [2 3 4 4 5 6]
savepoint-2.5...
! savepoint-2.5 expected: [1 2 3 7 8 9 10 11 12]
! savepoint-2.5 got:      [2 3 4 4 5 6 7 8 9 10 11 12]
savepoint-2.6...
! savepoint-2.6 expected: [1 2 3 7 8 9]
! savepoint-2.6 got:      [2 3 4 4 5 6 7 8 9 10 11 12]
savepoint-2.7...
! savepoint-2.7 expected: [1 2 3 7 8 9 10 11 12]
! savepoint-2.7 got:      [2 3 4 4 5 6 7 8 9 10 11 12 10 11 12]
savepoint-2.8...
! savepoint-2.8 expected: [1 2 3]
! savepoint-2.8 got:      [2 3 4 4 5 6 7 8 9 10 11 12 10 11 12]
savepoint-2.9...
! savepoint-2.9 expected: [1 2 3 a b c d e f]
! savepoint-2.9 got:      [2 3 4 4 5 6 7 8 9 10 11 12 10 11 12 a b c d e f]
savepoint-2.10...
! savepoint-2.10 expected: [1 2 3 a b c d e f]
! savepoint-2.10 got:      [2 3 4 4 5 6 7 8 9 10 11 12 10 11 12 a b c d e f]
savepoint-2.11...
! savepoint-2.11 expected: []
! savepoint-2.11 got:      [2 3 4 4 5 6 7 8 9 10 11 12 10 11 12 a b c]
savepoint-3.1...
! savepoint-3.1 expected: [main unlocked temp closed]
! savepoint-3.1 got:      [main unlocked temp unlocked]
savepoint-3.2...
! savepoint-3.2 expected: [main reserved temp closed]
! savepoint-3.2 got:      [main unlocked temp unlocked]
savepoint-3.3...
! savepoint-3.3 expected: [main reserved temp closed]
! savepoint-3.3 got:      [main unlocked temp unlocked]
savepoint-3.4...
! savepoint-3.4 expected: [main reserved temp closed]
! savepoint-3.4 got:      [main unlocked temp unlocked]
savepoint-3.5...
! savepoint-3.5 expected: [main unlocked temp closed]
! savepoint-3.5 got:      [main unlocked temp unlocked]
savepoint-4.1...
! savepoint-4.1 expected: [{CREATE TABLE t1(a, b, c)} {CREATE TABLE t2(d, e, f)}]
! savepoint-4.1 got:      [{CREATE TABLE t1 (a, b, c)} {CREATE TABLE t2 (d, e, f)}]
savepoint-4.2... Ok
savepoint-4.3... Ok
savepoint-4.4...
! savepoint-4.4 expected: [I II]
! savepoint-4.4 got:      [III IV V]
savepoint-4.5...
! savepoint-4.5 expected: [{CREATE TABLE t1(a, b, c)} {CREATE TABLE t2(d, e, f)}]
! savepoint-4.5 got:      [{CREATE TABLE t1 (a, b, c)} {CREATE TABLE t2 (d, e, f)} {CREATE TABLE t3 (g, h)}]
savepoint-4.6...
Error: table "t3" already exists
savepoint-4.7...
Error: table "t3" already exists
savepoint-4.8...
Error: cannot commit - no transaction is active
savepoint-5.1.1...
Error: incrblob not implemented
savepoint-5.1.2...
Error: can't read "fd": no such variable
savepoint-5.2...
Error: no such savepoint: abc
savepoint-5.3.1... Ok
savepoint-5.3.2.1...
Error: incrblob not implemented
savepoint-5.3.2.2... Ok
savepoint-5.3.2.3...
! savepoint-5.3.2.3 expected: [0]
! savepoint-5.3.2.3 got:      [1]
savepoint-5.3.3... Ok
savepoint-5.3.4...
Error: can't read "fd": no such variable
savepoint-5.3.5...
Error: can't read "fd": no such variable
savepoint-5.4.1... Ok
savepoint-5.4.2...
Error: unable to open database: database is locked
savepoint-5.4.3...
! savepoint-5.4.3 expected: [1 {database is locked}]
! savepoint-5.4.3 got:      [0 {}]
savepoint-5.4.4...
Error: invalid command name "db2"
savepoint-5.4.5... Ok
savepoint-5.4.6... Ok
savepoint-6.1... Ok
savepoint-6.2... Ok
savepoint-6.3...
Error: unknown pragma: incr_vacuum
savepoint-6.4... Ok
savepoint-7.1...
Error: database disk image is malformed
savepoint-7.2.1... Ok
savepoint-7.2.2... Ok
savepoint-7.3.1...
Error: table "t2" already exists
savepoint-7.3.2... Ok
savepoint-7.4.1... Ok
savepoint-7.5.1... Ok
savepoint-7.5.2... Ok
savepoint-8-1... Ok
savepoint-8-2... Ok
savepoint-10.1.1...
! savepoint-10.1.1 expected: [0 {}]
! savepoint-10.1.1 got:      [1 {cannot ATTACH database within transaction}]
savepoint-10.1.2...
! savepoint-10.1.2 expected: [0 {}]
! savepoint-10.1.2 got:      [1 {database aux is locked}]
savepoint-10.1.3... Ok
savepoint-10.2.1...
! savepoint-10.2.1 expected: [t1 t2 t3]
! savepoint-10.2.1 got:      [t1 t3 t2 t1 t3 t2 t1 t3 t2]
savepoint-10.2.2... Ok
savepoint-10.2.3...
! savepoint-10.2.3 expected: [main reserved temp unlocked aux1 unlocked aux2 unlocked]
! savepoint-10.2.3 got:      [main unlocked temp unlocked aux1 unlocked aux2 unlocked]
savepoint-10.2.4...
! savepoint-10.2.4 expected: [main reserved temp unlocked aux1 unlocked aux2 reserved]
! savepoint-10.2.4 got:      [main unlocked temp unlocked aux1 unlocked aux2 unlocked]
savepoint-10.2.5...
! savepoint-10.2.5 expected: [main reserved temp unlocked aux1 reserved aux2 reserved]
! savepoint-10.2.5 got:      [main unlocked temp unlocked aux1 unlocked aux2 unlocked]
savepoint-10.2.6... Ok
savepoint-10.2.7...
! savepoint-10.2.7 expected: []
! savepoint-10.2.7 got:      [5 6]
savepoint-10.2.8...
! savepoint-10.2.8 expected: [main reserved temp unlocked aux1 reserved aux2 reserved]
! savepoint-10.2.8 got:      [main unlocked temp unlocked aux1 unlocked aux2 unlocked]
savepoint-10.2.9... Ok
savepoint-10.2.9...
! savepoint-10.2.9 expected: [1 2 5 6 3 4]
! savepoint-10.2.9 got:      [1 2 5 6 5 6 3 4]
savepoint-10.2.9... Ok
savepoint-10.2.10...
! savepoint-10.2.10 expected: [1 2 a b 5 6 c d 3 4 e f]
! savepoint-10.2.10 got:      [1 2 a b 5 6 5 6 c d 3 4 e f]
savepoint-10.2.11...
! savepoint-10.2.11 expected: [1 2 a b 5 6 3 4]
! savepoint-10.2.11 got:      [1 2 a b 5 6 5 6 c d 3 4 e f]
savepoint-10.2.12...
! savepoint-10.2.12 expected: [1 2 a b 5 6 3 4]
! savepoint-10.2.12 got:      [1 2 a b 5 6 5 6 c d 3 4 e f g h]
savepoint-10.2.13...
! savepoint-10.2.13 expected: [1 2 5 6 3 4]
! savepoint-10.2.13 got:      [5 6 5 6 3 4 e f]
savepoint-10.2.14... Ok
savepoint-11.1... Ok
savepoint-11.2... Ok
savepoint-11.3... Ok
savepoint-11.4... Ok
savepoint-11.5... Ok
savepoint-11.6...
Error: table "t3" already exists
savepoint-11.7... Ok
savepoint-11.8...
! savepoint-11.8 expected: [8192]
! savepoint-11.8 got:      [20480]
savepoint-11.9... Ok
savepoint-11.10... Ok
savepoint-11.11... Ok
savepoint-11.12... Ok
savepoint-12.1... Ok
savepoint-12.2... Ok
savepoint-12.3...
! savepoint-12.3 expected: [1]
! savepoint-12.3 got:      [0]
savepoint-12.4... Ok
savepoint-13.1...
! savepoint-13.1 expected: [off]
! savepoint-13.1 got:      []
savepoint-13.2... Ok
savepoint-13.3... Ok
savepoint-13.4...
! savepoint-13.4 expected: [1 2 3 4 5 6 7 8 9 10 11 12]
! savepoint-13.4 got:      [1 2 3 4 5 6 7 8 9 10 11 12 13 14]

Error in savepoint.test: invalid command name "faultsim_delete_and_reopen"
invalid command name "faultsim_delete_and_reopen"
    while executing
"faultsim_delete_and_reopen"
    (procedure "do_multiclient_test" line 17)
    invoked from within
"do_multiclient_test tn {
  do_test savepoint-14.$tn.1 {
    sql1 {
      CREATE TABLE foo(x);
      INSERT INTO foo VALUES(1);
      INSERT INTO foo V..."
    (file "/tank/repos/rustql-architecture/sqlite3/test/savepoint.test" line 926)
    invoked from within
"source $test_file"

==========================================
Test: savepoint
Time: 0s
Status: FAILED
