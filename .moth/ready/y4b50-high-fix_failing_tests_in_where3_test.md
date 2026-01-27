# Summary
- Test: sqlite3/test/where3.test
- Repro: `testfixture test/where3.test`
- Failing cases: where3-1.1, where3-1.2
- Primary errors: ! where3-1.1 expected: [222 two 2 222 {} {}] | ! where3-1.1 got:      [111 one 2 222 {} {} 222 two 2 222 {} {} 333 three 2 222 {} {}] | ! where3-1.2 expected: [1 {Value for C1.1} {Value for C2.1} 2 {} {Value for C2.2} 3 {Value for C1.3} {Value for C2.3}]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-where3-3641353-1769533960709
DEBUG: tester.tcl sourced, db=db
Running where3.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/where3.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/where3.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
where3-1.1...
! where3-1.1 expected: [222 two 2 222 {} {}]
! where3-1.1 got:      [111 one 2 222 {} {} 222 two 2 222 {} {} 333 three 2 222 {} {}]
where3-1.1.1... Ok
where3-1.2...
! where3-1.2 expected: [1 {Value for C1.1} {Value for C2.1} 2 {} {Value for C2.2} 3 {Value for C1.3} {Value for C2.3}]
! where3-1.2 got:      [1 {Value for C1.1} {Value for C2.1} 1 {} {Value for C2.2} 1 {} {Value for C2.3} 2 {} {Value for C2.1} 2 {} {Value for C2.2} 2 {} {Value for C2.3} 3 {} {Value for C2.1} 3 {} {Value for C2.2} 3 {} {Value for C2.3}]
where3-1.2.1... Ok
where3-2.1...
Error: no such table: tA
where3-2.1.1...
Error: no such table: tA
where3-2.1.2...
Error: no such table: tA
where3-2.1.3...
Error: no such table: tA
where3-2.1.4...
Error: no such table: tA
where3-2.1.5...
Error: no such table: tA
where3-2.2...
Error: no such table: tA
where3-2.3...
Error: no such table: tA
where3-2.4...
Error: no such table: tA
where3-2.5...
Error: no such table: tA
where3-2.6...
Error: no such table: tA
where3-2.7...
Error: no such table: tA
where3-3.0... Ok

Error in where3.test: can't read "cx(0)": no such variable
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
"do_eqp_test where3-3.0a {
  SELECT * FROM t302, t301 WHERE t302.x=5 AND t301.a=t302.y;
} {
  QUERY PLAN
  |--SCAN t302
  `--SEARCH t301 USING INTEGER ..."
    (file "/tank/repos/rustql-architecture/sqlite3/test/where3.test" line 241)
    invoked from within
"source $test_file"

==========================================
Test: where3
Time: 0s
Status: FAILED
