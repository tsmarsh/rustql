# Summary
- Test: sqlite3/test/trigger2.test
- Repro: `testfixture test/trigger2.test`
- Failing cases: trigger2-1.1.3, trigger2-1.2.3, trigger2-1.3.3, trigger2-1.4.3, trigger2-1.5.3, trigger2-1.6.3, trigger2-1.7.3, trigger2-2.1-before, trigger2-2.6-before, trigger2-2.9-before, trigger2-2.10-before, trigger2-2.11-before, trigger2-2.12-before, trigger2-2.13-before, trigger2-2.13-after, trigger2-2.14-before, trigger2-3.1, trigger2-6.1a, trigger2-6.1b, trigger2-6.1c, trigger2-6.1e, trigger2-6.1f, trigger2-6.1g, trigger2-6.1h, trigger2-6.2c, trigger2-6.2d, trigger2-6.2e, trigger2-6.2f.2, trigger2-6.2g, trigger2-6.2h, trigger2-11.2
- Primary errors: Error: expected number but got "" | Error: expected number but got "" | ! trigger2-1.1.3 expected: [1 0 0 0 0 5 6 2 0 0 5 6 5 6]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-trigger2-3641368-1769533960727
DEBUG: tester.tcl sourced, db=db
Running trigger2.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/trigger2.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/trigger2.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
trigger2-1.1.1...
Error: expected number but got ""
trigger2-1.1.2...
Error: expected number but got ""
trigger2-1.1.3...
! trigger2-1.1.3 expected: [1 0 0 0 0 5 6 2 0 0 5 6 5 6]
! trigger2-1.1.3 got:      [{} 0 0 {} {} 5 6 {} 0 0 {} {} 0 0]
trigger2-1.1.4... Ok
trigger2-1.2.1...
Error: expected number but got ""
trigger2-1.2.2...
Error: expected number but got ""
trigger2-1.2.3...
! trigger2-1.2.3 expected: [1 0 0 0 0 5 6 2 0 0 5 6 5 6]
! trigger2-1.2.3 got:      [{} 0 0 {} {} {} 6 {} 0 0 {} {} 0 {}]
trigger2-1.2.4... Ok
trigger2-1.3.1...
Error: expected number but got ""
trigger2-1.3.2...
Error: expected number but got ""
trigger2-1.3.3...
! trigger2-1.3.3 expected: [1 0 0 0 0 5 6 2 0 0 5 6 5 6]
! trigger2-1.3.3 got:      [{} 0 0 {} {} 5 6 {} 0 0 {} {} 0 0]
trigger2-1.3.4... Ok
trigger2-1.4.1...
Error: expected number but got ""
trigger2-1.4.2...
Error: expected number but got ""
trigger2-1.4.3...
! trigger2-1.4.3 expected: [1 0 0 0 0 5 6 2 0 0 5 6 5 6]
! trigger2-1.4.3 got:      [{} 0 0 {} {} 5 6 {} 0 0 {} {} 0 0]
trigger2-1.4.4... Ok
trigger2-1.5.1...
Error: expected number but got ""
trigger2-1.5.2...
Error: expected number but got ""
trigger2-1.5.3...
! trigger2-1.5.3 expected: [1 0 0 0 0 5 6 2 0 0 5 6 5 6]
! trigger2-1.5.3 got:      [{} 0 0 {} {} 5 6 {} 0 0 {} {} 0 0]
trigger2-1.5.4... Ok
trigger2-1.6.1...
Error: expected number but got ""
trigger2-1.6.2...
Error: expected number but got ""
trigger2-1.6.3...
! trigger2-1.6.3 expected: [1 0 0 0 0 5 6 2 0 0 5 6 5 6]
! trigger2-1.6.3 got:      [{} 0 0 {} {} 5 6 {} 0 0 {} {} 0 0]
trigger2-1.6.4... Ok
trigger2-1.7.1...
Error: expected number but got ""
trigger2-1.7.2...
Error: expected number but got ""
trigger2-1.7.3...
! trigger2-1.7.3 expected: [1 0 0 0 0 5 6 2 0 0 5 6 5 6]
! trigger2-1.7.3 got:      [{} 0 0 {} {} {} 6 {} 0 0 {} {} 0 {}]
trigger2-1.7.4... Ok
trigger2-2.1-before...
! trigger2-2.1-before expected: [1 2 10 1 2 3 10 20 30]
! trigger2-2.1-before got:      [3 {	} 10 1 2 3 10 20 30]
trigger2-2.1-after... Ok
trigger2-2.1-integrity... Ok
trigger2-2.2-before... Ok
trigger2-2.2-after... Ok
trigger2-2.2-integrity... Ok
trigger2-2.3-before... Ok
trigger2-2.3-after... Ok
trigger2-2.3-integrity... Ok
trigger2-2.4-before... Ok
trigger2-2.4-after... Ok
trigger2-2.4-integrity... Ok
trigger2-2.5-before... Ok
trigger2-2.5-after... Ok
trigger2-2.5-integrity... Ok
trigger2-2.6-before...
! trigger2-2.6-before expected: [1 2 3 1 2 3 10 20 30 3 2 3]
! trigger2-2.6-before got:      [2 3 3 1 2 3 10 20 30 3 2 3]
trigger2-2.6-after... Ok
trigger2-2.6-integrity... Ok
trigger2-2.7-before... Ok
trigger2-2.7-after... Ok
trigger2-2.7-integrity... Ok
trigger2-2.8-before... Ok
trigger2-2.8-after... Ok
trigger2-2.8-integrity... Ok
trigger2-2.9-before...
! trigger2-2.9-before expected: [1 2 3 10 20 30]
! trigger2-2.9-before got:      [10 1 3 10 20 30]
trigger2-2.9-after... Ok
trigger2-2.9-integrity... Ok
trigger2-2.10-before...
! trigger2-2.10-before expected: [1 2 10 500 20 3]
! trigger2-2.10-before got:      [�� 2 20]
trigger2-2.10-after... Ok
trigger2-2.10-integrity... Ok
trigger2-2.11-before...
! trigger2-2.11-before expected: [500 0.0 3]
! trigger2-2.11-before got:      []
trigger2-2.11-after... Ok
trigger2-2.11-integrity... Ok
trigger2-2.12-before...
! trigger2-2.12-before expected: [500 20 {} 1 2 3]
! trigger2-2.12-before got:      [500 20 {} 20 700 2]
trigger2-2.12-after...
Error: too many levels of trigger recursion (max 1000)
trigger2-2.12-integrity... Ok
trigger2-2.13-before...
! trigger2-2.13-before expected: [1 2 10 1 2 3 10 20 30 1 2 3]
! trigger2-2.13-before got:      [1 2 10 1 2 3 10 20 30]
trigger2-2.13-after...
! trigger2-2.13-after expected: [1 2 10 1 2 3 10 20 30 1 2 10]
! trigger2-2.13-after got:      [1 2 10 1 2 3 10 20 30]
trigger2-2.13-integrity... Ok
trigger2-2.14-before...
! trigger2-2.14-before expected: [1 2 3 10 20 30 1 2 3]
! trigger2-2.14-before got:      [1 2 3 10 20 30]
trigger2-2.14-after... Ok
trigger2-2.14-integrity... Ok
trigger2-2.15-before... Ok
trigger2-2.15-after... Ok
trigger2-2.15-integrity... Ok
trigger2-3.1...
! trigger2-3.1 expected: [3]
! trigger2-3.1 got:      [2]
trigger2-3.2...
Error: UNIQUE constraint failed: table.rowid
trigger2-3.3... Ok
trigger2-4.1...
Error: no such table: tblA
trigger2-4.2...
Error: UNIQUE constraint failed: table.rowid
trigger2-5... Ok
trigger2-6.1a...
! trigger2-6.1a expected: [1 2 3]
! trigger2-6.1a got:      [1 2 3 1 0 0]
trigger2-6.1b...
! trigger2-6.1b expected: [1 {UNIQUE constraint failed: tbl.a}]
! trigger2-6.1b got:      [0 {}]
trigger2-6.1c...
! trigger2-6.1c expected: [1 2 3]
! trigger2-6.1c got:      [1 2 3 1 0 0 2 2 3 2 0 0]
trigger2-6.1d... Ok
trigger2-6.1e...
! trigger2-6.1e expected: [1 2 3 2 2 3]
! trigger2-6.1e got:      [1 2 3 1 0 0 2 2 3 2 0 0]
trigger2-6.1f...
! trigger2-6.1f expected: [1 2 3 2 0 0]
! trigger2-6.1f got:      [1 2 3 1 0 0 2 2 3 2 0 0 2 2 3 2 0 0]
trigger2-6.1g...
! trigger2-6.1g expected: [1 {UNIQUE constraint failed: tbl.a}]
! trigger2-6.1g got:      [0 {}]
trigger2-6.1h...
! trigger2-6.1h expected: []
! trigger2-6.1h got:      [1 2 3 1 0 0 2 2 3 2 0 0 2 2 3 2 0 0 3 2 3 3 0 0]
trigger2-6.2a...
Error: UNIQUE constraint failed: tbl.a
trigger2-6.2b... Ok
trigger2-6.2c...
! trigger2-6.2c expected: [1 2 10 6 3 4]
! trigger2-6.2c got:      [4 2 10 4 0 10]
trigger2-6.2d...
! trigger2-6.2d expected: [1 {UNIQUE constraint failed: tbl.a}]
! trigger2-6.2d got:      [0 {}]
trigger2-6.2e...
! trigger2-6.2e expected: [4 2 10 6 3 4]
! trigger2-6.2e got:      [4 2 10 4 0 10]
trigger2-6.2f.1...
Error: UNIQUE constraint failed: tbl.a
trigger2-6.2f.2...
! trigger2-6.2f.2 expected: [1 3 10 2 3 4]
! trigger2-6.2f.2 got:      [1 2 10 2 3 4 2 0 0]
trigger2-6.2g...
! trigger2-6.2g expected: [1 {UNIQUE constraint failed: tbl.a}]
! trigger2-6.2g got:      [0 {}]
trigger2-6.2h...
! trigger2-6.2h expected: [4 2 3 6 3 4]
! trigger2-6.2h got:      [4 2 10 4 3 10 4 0 10]
trigger2-7.1... Ok
trigger2-7.2...
Error: no such table: abcd
trigger2-7.3...
Error: no such table: abcd
trigger2-7.4...
Error: no such column: a
trigger2-8.1... Ok
trigger2-8.2...
Error: no such column: x
trigger2-8.3...
Error: no such column: x
trigger2-8.4...
Error: no such column: y
trigger2-8.5...
Error: no such table: v1
trigger2-8.6...
Error: no such table: v1
trigger2-9.1...
Error: no such column: a
trigger2-9.99... Ok
trigger2-10.1...
Error: no such table: v2
trigger2-11.1... Ok
trigger2-11.2...
! trigger2-11.2 expected: [1 {trigger cannot use variables}]
! trigger2-11.2 got:      [1 {near "ON": syntax error}]
Running "trigger2"

Error in trigger2.test: couldn't read file "trigger2": no such file or directory
couldn't read file "trigger2": no such file or directory
    while executing
"source trigger2"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/trigger2.test" line 792)
    invoked from within
"source $test_file"

==========================================
Test: trigger2
Time: 0s
Status: FAILED
