# Summary
- Test: sqlite3/test/vacuum.test
- Repro: `testfixture test/vacuum.test`
- Failing cases: vacuum-1.1b
- Primary errors: Error: invalid command name "md5" | ! vacuum-1.1b expected: [1 {bad function}] | ! vacuum-1.1b got:      [0 {t1 i2 i1}]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-vacuum-3641433-1769533960758
DEBUG: tester.tcl sourced, db=db
Running vacuum.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/vacuum.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/vacuum.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
vacuum-1.1...
Error: invalid command name "md5"
vacuum-1.1b...
! vacuum-1.1b expected: [1 {bad function}]
! vacuum-1.1b got:      [0 {t1 i2 i1}]

Error in vacuum.test: can't read "cksum": no such variable
can't read "cksum": no such variable
    while executing
"do_test vacuum-1.2 {
  execsql {
    VACUUM;
  }
  cksum
} $cksum"
    (file "/tank/repos/rustql-architecture/sqlite3/test/vacuum.test" line 70)
    invoked from within
"source $test_file"

==========================================
Test: vacuum
Time: 0s
Status: FAILED
