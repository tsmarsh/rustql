# Summary
- Test: sqlite3/test/subquery.test
- Repro: `testfixture test/subquery.test`
- Failing cases: subquery-1.2, subquery-1.3, subquery-1.4, subquery-1.9.1, subquery-1.9.2, subquery-1.10.1, subquery-1.10.2, subquery-1.10.4, subquery-1.10.5, subquery-2.5.2, subquery-2.5.3.1, subquery-3.4.1, subquery-3.4.3, subquery-3.5.4, subquery-3.5.5, subquery-3.5.6, subquery-4.1.1, subquery-5.2, subquery-6.2, subquery-6.4, subquery-8.1
- Primary errors: ! subquery-1.2 expected: [1 3 3 13 5 31 7 57] | ! subquery-1.2 got:      [1 {} 3 {} 5 {} 7 {}] | ! subquery-1.3 expected: [3]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-subquery-3641361-1769533960713
DEBUG: tester.tcl sourced, db=db
Running subquery.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/subquery.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/subquery.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
subquery-1.1... Ok
subquery-1.2...
! subquery-1.2 expected: [1 3 3 13 5 31 7 57]
! subquery-1.2 got:      [1 {} 3 {} 5 {} 7 {}]
subquery-1.3...
! subquery-1.3 expected: [3]
! subquery-1.3 got:      [{}]
subquery-1.4...
! subquery-1.4 expected: [13 31 57]
! subquery-1.4 got:      [{} {} {}]
subquery-1.5... Ok
subquery-1.6... Ok
subquery-1.7... Ok
subquery-1.8... Ok
subquery-1.9.1...
! subquery-1.9.1 expected: [0 1 1 1]
! subquery-1.9.1 got:      [{} {} {} {}]
subquery-1.9.2...
! subquery-1.9.2 expected: [3 5 7]
! subquery-1.9.2 got:      []
subquery-1.10.1...
! subquery-1.10.1 expected: [1 3 3 13 5 31 7 57]
! subquery-1.10.1 got:      [1 {} 3 {} 5 {} 7 {}]
subquery-1.10.2...
! subquery-1.10.2 expected: [1 3 3 13 5 31 7 57]
! subquery-1.10.2 got:      [1 {} 3 {} 5 {} 7 {}]
subquery-1.10.3... Ok
subquery-1.10.4...
! subquery-1.10.4 expected: [2002-2 30 2002-3 25 2002-4 15]
! subquery-1.10.4 got:      [2002-1 45 2002-2 30 2002-3 25 2002-4 15]
subquery-1.10.5...
! subquery-1.10.5 expected: [2002-2 30 2002-3 25 2002-4 15]
! subquery-1.10.5 got:      [2002-1 45 2002-2 30 2002-3 25 2002-4 15]
subquery-1.10.6... Ok
subquery-2.1... Ok
subquery-2.2.1... Ok
subquery-2.2.2... Ok
subquery-2.2.3... Ok
subquery-2.3.1... Ok
subquery-2.3.2... Ok
subquery-2.3.3... Ok
subquery-2.4.1... Ok
subquery-2.4.2... Ok
subquery-2.4.3... Ok
subquery-2.5.1... Ok
subquery-2.5.2...
! subquery-2.5.2 expected: [10.0]
! subquery-2.5.2 got:      []
subquery-2.5.3.1...
! subquery-2.5.3.1 expected: [10.0]
! subquery-2.5.3.1 got:      []
subquery-2.5.3.2... Ok
subquery-2.5.4... Ok
subquery-3.1... Ok
subquery-3.1.1... Ok
subquery-3.2... Ok
subquery-3.3.1... Ok
subquery-3.3.2... Ok
subquery-3.3.3... Ok
subquery-3.3.4... Ok
subquery-3.3.5... Ok
subquery-3.4.1...
! subquery-3.4.1 expected: [107 4.0]
! subquery-3.4.1 got:      [106 4.5 107 4.0]
subquery-3.4.2...
Error: no such column: avg1
subquery-3.4.3...
! subquery-3.4.3 expected: [106 4.5 0 1 107 4.0 1 0]
! subquery-3.4.3 got:      [106 4.5 1 {} 107 4.0 1 {}]
subquery-3.5.1... Ok
subquery-3.5.2... Ok
subquery-3.5.3... Ok
subquery-3.5.4...
! subquery-3.5.4 expected: [1 {misuse of aggregate: count()}]
! subquery-3.5.4 got:      [0 2]
subquery-3.5.5...
! subquery-3.5.5 expected: [1 {misuse of aggregate: count()}]
! subquery-3.5.5 got:      [0 2]
subquery-3.5.6...
! subquery-3.5.6 expected: [1 {misuse of aggregate: count()}]
! subquery-3.5.6 got:      [1 {no such column: x}]
subquery-3.5.7... Ok
subquery-4.1.1...
! subquery-4.1.1 expected: [1]
! subquery-4.1.1 got:      [2]
subquery-4.2... Ok
subquery-4.2.1... Ok
subquery-4.2.2... Ok
subquery-5.1... Ok
subquery-5.2...
! subquery-5.2 expected: [1]
! subquery-5.2 got:      [0]
subquery-6.1... Ok
subquery-6.2...
! subquery-6.2 expected: [4]
! subquery-6.2 got:      [0]
subquery-6.3... Ok
subquery-6.4...
! subquery-6.4 expected: [1]
! subquery-6.4 got:      [0]
subquery-8.1...
! subquery-8.1 expected: []
! subquery-8.1 got:      [{} 0]
subquery-9.1... Ok
subquery-9.2... Ok
subquery-9.3... Ok
subquery-9.4... Ok
subquery-10.1...
Error: no such table: sqlite_master

Error in subquery.test: can't read "cx(0)": no such variable
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
"do_eqp_test subquery-10.2 {
  WITH v1(aa,cc,bb) AS (SELECT aa, cc, bb FROM t1 WHERE bb=12345),
       v2(aa,mx)    AS (SELECT aa, max(xx) FROM t2 GROU..."
    (file "/tank/repos/rustql-architecture/sqlite3/test/subquery.test" line 630)
    invoked from within
"source $test_file"

==========================================
Test: subquery
Time: 0s
Status: FAILED
