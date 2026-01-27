# Summary
- Test: sqlite3/test/orderby1.test
- Repro: `testfixture test/orderby1.test`
- Failing cases: orderby1-1.2b, orderby1-1.3b, orderby1-2.1a, orderby1-2.1b, orderby1-2.1c, orderby1-2.1d, orderby1-2.2a, orderby1-2.2b, orderby1-2.3a, orderby1-2.3b, orderby1-2.4a, orderby1-2.4b, orderby1-2.4c, orderby1-2.5a, orderby1-2.5b, orderby1-2.5c, orderby1-2.6a, orderby1-2.6b, orderby1-2.6c, orderby1-3.2b, orderby1-3.3b
- Primary errors: ! orderby1-1.2b expected: [/ORDER BY/] | ! orderby1-1.2b got:      [0 0 0 {SCAN album} 1 0 0 {SCAN track}] | ! orderby1-1.3b expected: [/ORDER BY/]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-orderby1-3641411-1769533960740
DEBUG: tester.tcl sourced, db=db
Running orderby1.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/orderby1.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/orderby1.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
orderby1-1.0... Ok
orderby1-1.1a... Ok
orderby1-1.1b... Ok
orderby1-1.2a... Ok
orderby1-1.2b...
! orderby1-1.2b expected: [/ORDER BY/]
! orderby1-1.2b got:      [0 0 0 {SCAN album} 1 0 0 {SCAN track}]
orderby1-1.3a... Ok
orderby1-1.3b...
! orderby1-1.3b expected: [/ORDER BY/]
! orderby1-1.3b got:      [0 0 0 {SCAN album} 1 0 0 {SCAN track}]
orderby1-1.4a... Ok
orderby1-1.4b... Ok
orderby1-1.4c... Ok
orderby1-1.5a... Ok
orderby1-1.5b... Ok
orderby1-1.5c... Ok
orderby1-1.6a... Ok
orderby1-1.6b... Ok
orderby1-1.6c... Ok
orderby1-2.0...
Error: UNIQUE constraint failed: album.aid
orderby1-2.1a...
! orderby1-2.1a expected: [one-a one-c two-a two-b three-a three-c]
! orderby1-2.1a got:      []
orderby1-2.1b...
! orderby1-2.1b expected: [/ORDER BY/]
! orderby1-2.1b got:      [0 0 0 {SCAN album} 1 0 0 {SCAN track}]
orderby1-2.1c...
! orderby1-2.1c expected: [one-a one-c two-a two-b three-a three-c]
! orderby1-2.1c got:      []
orderby1-2.1d...
! orderby1-2.1d expected: [/ORDER BY/]
! orderby1-2.1d got:      [0 0 0 {SCAN album} 1 0 0 {SCAN track}]
orderby1-2.2a...
! orderby1-2.2a expected: [one-a one-c two-a two-b three-a three-c]
! orderby1-2.2a got:      []
orderby1-2.2b...
! orderby1-2.2b expected: [/ORDER BY/]
! orderby1-2.2b got:      [0 0 0 {SCAN album} 1 0 0 {SCAN track}]
orderby1-2.3a...
! orderby1-2.3a expected: [one-a one-c two-a two-b three-a three-c]
! orderby1-2.3a got:      []
orderby1-2.3b...
! orderby1-2.3b expected: [/ORDER BY/]
! orderby1-2.3b got:      [0 0 0 {SCAN album} 1 0 0 {SCAN track}]
orderby1-2.4a...
! orderby1-2.4a expected: [three-a three-c two-a two-b one-a one-c]
! orderby1-2.4a got:      []
orderby1-2.4b...
! orderby1-2.4b expected: [three-a three-c two-a two-b one-a one-c]
! orderby1-2.4b got:      []
orderby1-2.4c...
! orderby1-2.4c expected: [/ORDER BY/]
! orderby1-2.4c got:      [0 0 0 {SCAN album} 1 0 0 {SCAN track}]
orderby1-2.5a...
! orderby1-2.5a expected: [one-c one-a two-b two-a three-c three-a]
! orderby1-2.5a got:      []
orderby1-2.5b...
! orderby1-2.5b expected: [one-c one-a two-b two-a three-c three-a]
! orderby1-2.5b got:      []
orderby1-2.5c...
! orderby1-2.5c expected: [/ORDER BY/]
! orderby1-2.5c got:      [0 0 0 {SCAN album} 1 0 0 {SCAN track}]
orderby1-2.6a...
! orderby1-2.6a expected: [three-c three-a two-b two-a one-c one-a]
! orderby1-2.6a got:      []
orderby1-2.6b...
! orderby1-2.6b expected: [three-c three-a two-b two-a one-c one-a]
! orderby1-2.6b got:      []
orderby1-2.6c...
! orderby1-2.6c expected: [/ORDER BY/]
! orderby1-2.6c got:      [0 0 0 {SCAN album} 1 0 0 {SCAN track}]
orderby1-3.0... Ok
orderby1-3.1a... Ok
orderby1-3.1b... Ok
orderby1-3.2a... Ok
orderby1-3.2b...
! orderby1-3.2b expected: [/ORDER BY/]
! orderby1-3.2b got:      [0 0 0 {SCAN album} 1 0 0 {SCAN track}]
orderby1-3.3a... Ok
orderby1-3.3b...
! orderby1-3.3b expected: [/ORDER BY/]
! orderby1-3.3b got:      [0 0 0 {SCAN album} 1 0 0 {SCAN track}]
orderby1-3.4a... Ok
orderby1-3.4b... Ok
orderby1-3.4c... Ok
orderby1-3.5a... Ok
orderby1-3.5b... Ok
orderby1-3.5c... Ok
orderby1-3.6a... Ok
orderby1-3.6b... Ok
orderby1-3.6c... Ok
orderby1-4.0... Ok

Error in orderby1.test: can't read "cx(0)": no such variable
can't read "cx(0)": no such variable
    while executing
"set x $cx($level)"
    (procedure "append_graph" line 4)
    invoked from within
"append_graph "  " dx cx 0"
    (procedure "query_plan_graph" line 7)
    invoked from within
"query_plan_graph $sql"
    (procedure "do_eqp_test" line 4)
    invoked from within
"do_eqp_test 5.0 {
  SELECT 5 ORDER BY 1
} {
  QUERY PLAN
  `--SCAN CONSTANT ROW
}"
    (file "/tank/repos/rustql-architecture/sqlite3/test/orderby1.test" line 460)
    invoked from within
"source $test_file"

==========================================
Test: orderby1
Time: 0s
Status: FAILED
