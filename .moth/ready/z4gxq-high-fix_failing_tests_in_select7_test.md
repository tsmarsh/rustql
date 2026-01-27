# Summary
- Test: sqlite3/test/select7.test
- Repro: `testfixture test/select7.test`
- Failing cases: select7-5.1, select7-5.2, select7-5.3, select7-5.4
- Primary errors: Error: near "NULL": syntax error | Error: no such column: P.pk | Error: no such column: P.pk

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-select7-3641335-1769533960708
DEBUG: tester.tcl sourced, db=db
Running select7.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/select7.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/select7.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
select7-1.1... Ok
select7-2.1...
Error: near "NULL": syntax error
select7-3.1... Ok
select7-4.1...
Error: no such column: P.pk
select7-4.2...
Error: no such column: P.pk
select7-5.1...
! select7-5.1 expected: [1 {sub-select returns 2 columns - expected 1}]
! select7-5.1 got:      [0 0]
select7-5.2...
! select7-5.2 expected: [1 {sub-select returns 2 columns - expected 1}]
! select7-5.2 got:      [0 0]
select7-5.3...
! select7-5.3 expected: [1 {sub-select returns 2 columns - expected 1}]
! select7-5.3 got:      [0 0]
select7-5.4...
! select7-5.4 expected: [1 {sub-select returns 2 columns - expected 1}]
! select7-5.4 got:      [0 0]

Error in select7.test: can't read "SQLITE_MAX_COMPOUND_SELECT": no such variable
can't read "SQLITE_MAX_COMPOUND_SELECT": no such variable
    while executing
"ifcapable compound {
    if {$SQLITE_MAX_COMPOUND_SELECT>0} {
      set sql {SELECT 0}
      set result 0
        for {set i 1} {$i<$SQLITE_MAX_COMPOU..."
    (file "/tank/repos/rustql-architecture/sqlite3/test/select7.test" line 139)
    invoked from within
"source $test_file"

==========================================
Test: select7
Time: 0s
Status: FAILED
