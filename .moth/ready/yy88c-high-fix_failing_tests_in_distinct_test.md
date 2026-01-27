# Summary
- Test: sqlite3/test/distinct.test
- Repro: `testfixture test/distinct.test`
- Failing cases: distinct-1.1.1, distinct-1.2.1, distinct-1.8.1, distinct-1.8, distinct-1.9, distinct-1.10, distinct-1.11, distinct-1.12.1, distinct-1.12.2, distinct-1.13.1, distinct-1.13.2, distinct-1.14.1, distinct-1.15, distinct-1.16.1, distinct-1.16, distinct-1.17, distinct-1.21, distinct-1.22, distinct-1.24, distinct-1.26.1
- Primary errors: ! distinct-1.1.1 expected: [0] | ! distinct-1.1.1 got:      [1] | ! distinct-1.2.1 expected: [0]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-distinct-3641398-1769533960733
DEBUG: tester.tcl sourced, db=db
Running distinct.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/distinct.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/distinct.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
distinct-1.0... Ok
distinct-1.1.1...
! distinct-1.1.1 expected: [0]
! distinct-1.1.1 got:      [1]
distinct-1.1.2... Ok
distinct-1.2.1...
! distinct-1.2.1 expected: [0]
! distinct-1.2.1 got:      [1]
distinct-1.2.2... Ok
distinct-1.3... Ok
distinct-1.4... Ok
distinct-1.5... Ok
distinct-1.6... Ok
distinct-1.7... Ok
distinct-1.8.1...
! distinct-1.8.1 expected: [0]
! distinct-1.8.1 got:      [1]
distinct-1.8.2... Ok
distinct-1.8...
! distinct-1.8 expected: [0]
! distinct-1.8 got:      [1]
distinct-1.9...
! distinct-1.9 expected: [0]
! distinct-1.9 got:      [1]
distinct-1.10...
! distinct-1.10 expected: [0]
! distinct-1.10 got:      [1]
distinct-1.11...
! distinct-1.11 expected: [0]
! distinct-1.11 got:      [1]
distinct-1.12.1...
! distinct-1.12.1 expected: [0]
! distinct-1.12.1 got:      [1]
distinct-1.12.2...
! distinct-1.12.2 expected: [0]
! distinct-1.12.2 got:      [1]
distinct-1.13.1...
! distinct-1.13.1 expected: [0]
! distinct-1.13.1 got:      [1]
distinct-1.13.2...
! distinct-1.13.2 expected: [0]
! distinct-1.13.2 got:      [1]
distinct-1.14.1...
! distinct-1.14.1 expected: [0]
! distinct-1.14.1 got:      [1]
distinct-1.14.2... Ok
distinct-1.15...
! distinct-1.15 expected: [0]
! distinct-1.15 got:      [1]
distinct-1.16.1...
! distinct-1.16.1 expected: [0]
! distinct-1.16.1 got:      [1]
distinct-1.16.2... Ok
distinct-1.16...
! distinct-1.16 expected: [0]
! distinct-1.16 got:      [1]
distinct-1.17...
! distinct-1.17 expected: [0]
! distinct-1.17 got:      [1]
distinct-1.18... Ok
distinct-1.19... Ok
distinct-1.20... Ok
distinct-1.21...
! distinct-1.21 expected: [0]
! distinct-1.21 got:      [1]
distinct-1.22...
! distinct-1.22 expected: [0]
! distinct-1.22 got:      [1]
distinct-1.24...
! distinct-1.24 expected: [0]
! distinct-1.24 got:      [1]
distinct-1.25... Ok
distinct-1.26.1...
! distinct-1.26.1 expected: [0]
! distinct-1.26.1 got:      [1]
distinct-1.26.2... Ok

Error in distinct.test: no such column: name
no such column: name
    while executing
"$db eval "
      SELECT name, type FROM $master
      WHERE type IN('table', 'view') AND name NOT LIKE 'sqliteX_%' ESCAPE 'X'
    ""
    (procedure "drop_all_tables" line 12)
    invoked from within
"drop_all_tables"
    (file "/tank/repos/rustql-architecture/sqlite3/test/distinct.test" line 147)
    invoked from within
"source $test_file"

==========================================
Test: distinct
Time: 0s
Status: FAILED
