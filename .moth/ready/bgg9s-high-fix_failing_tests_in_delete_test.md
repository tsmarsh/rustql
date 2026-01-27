# Summary
- Test: sqlite3/test/delete.test
- Repro: `testfixture test/delete.test`
- Failing cases: delete-8.1, delete-8.3, delete-8.5, delete-9.2, delete-9.3, delete-9.4, delete-9.5, delete-11.1
- Primary errors: Error: internal error | Error: internal error | ! delete-8.1 expected: [1 {attempt to write a readonly database}]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-delete-3641346-1769533960709
DEBUG: tester.tcl sourced, db=db
Running delete.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/delete.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/delete.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
delete-1.1... Ok
delete-2.1... Ok
delete-3.1.1... Ok
delete-3.1.2... Ok
delete-3.1.3... Ok
delete-3.1.4... Ok
delete-3.1.5... Ok
delete-3.1.6.1... Ok
delete-3.1.6.2... Ok
delete-3.1.7... Ok
delete-3.2... Ok
delete-4.1... Ok
delete-4.2... Ok
delete-4.3... Ok
delete-5.1.1... Ok
delete-5.1.2... Ok
delete-5.2.1... Ok
delete-5.2.2... Ok
delete-5.2.3... Ok
delete-5.2.4... Ok
delete-5.2.5... Ok
delete-5.2.6... Ok
delete-5.3... Ok
delete-5.4.1... Ok
delete-5.4.2... Ok
delete-5.5... Ok
delete-5.6... Ok
delete-5.7... Ok
delete-5.8... Ok
delete-6.1... Ok
delete-6.2... Ok
delete-6.3... Ok
delete-6.4... Ok
delete-6.5.1... Ok
delete-6.5.2... Ok
delete-6.6... Ok
delete-6.7... Ok
delete-6.8...
Error: internal error
delete-6.9... Ok
delete-6.10...
Error: internal error
delete-6.11... Ok
delete-7.1... Ok
delete-7.2... Ok
delete-7.3... Ok
delete-7.4... Ok
delete-7.5... Ok
delete-7.6... Ok
delete-7.7... Ok
delete-8.0... Ok
Warning: Failed to open btree: unable to open database file: Permission denied (os error 13)
delete-8.1...
! delete-8.1 expected: [1 {attempt to write a readonly database}]
! delete-8.1 got:      [1 {no such table: t3}]
delete-8.2...
Error: no such table: t3
delete-8.3...
! delete-8.3 expected: [1 {attempt to write a readonly database}]
! delete-8.3 got:      [1 {no such table: t3}]
delete-8.4...
Error: no such table: t3
delete-8.5...
! delete-8.5 expected: [1 {attempt to write a readonly database}]
! delete-8.5 got:      [1 {no such column: a}]
delete-8.6...
Error: no such table: t3
delete-8.7... Ok
delete-9.1... Ok
delete-9.2...
! delete-9.2 expected: [1 a b 1 c d 2 a b {} c d]
! delete-9.2 got:      []
delete-9.3...
! delete-9.3 expected: [1 a b 1 c d 2 a b {} c d 3 a b 3 c d]
! delete-9.3 got:      []
delete-9.4...
! delete-9.4 expected: [1 a b 1 c d 2 a b 2 c d 3 a b 3 c d]
! delete-9.4 got:      []
delete-9.5...
! delete-9.5 expected: [1 a b 1 c d 2 a b 2 c d]
! delete-9.5 got:      []
delete-10.0... Ok
delete-10.1... Ok
delete-10.2... Ok
delete-11.0...
Error: no such table: cnt
delete-11.1...
! delete-11.1 expected: [6 2 12 4 18 6 19 23 20 40]
! delete-11.1 got:      []
delete-12.0... Ok
Running "delete"

Error in delete.test: couldn't read file "delete": no such file or directory
couldn't read file "delete": no such file or directory
    while executing
"source delete"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/delete.test" line 443)
    invoked from within
"source $test_file"

==========================================
Test: delete
Time: 1s
Status: FAILED
