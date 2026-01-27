# Summary
- Test: sqlite3/test/join2.test
- Repro: `testfixture test/join2.test`
- Failing cases: join2-1.5, join2-1.6, join2-1.7, join2-2.1, join2-2.1b
- Primary errors: ! join2-1.5 expected: [1 11 111 1111 3 33 333 {}] | ! join2-1.5 got:      [1 11 111 1111 1 11 333 {} 1 11 444 {} 2 22 111 {} 2 22 333 {} 2 22 444 {} 3 33 111 {} 3 33 333 {} 3 33 444 {}] | ! join2-1.6 expected: [1 11 111 1111]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-join2-3641356-1769533960711
DEBUG: tester.tcl sourced, db=db
Running join2.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/join2.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/join2.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
join2-1.1... Ok
join2-1.2... Ok
join2-1.3... Ok
join2-1.4... Ok
join2-1.5...
! join2-1.5 expected: [1 11 111 1111 3 33 333 {}]
! join2-1.5 got:      [1 11 111 1111 1 11 333 {} 1 11 444 {} 2 22 111 {} 2 22 333 {} 2 22 444 {} 3 33 111 {} 3 33 333 {} 3 33 444 {}]
join2-1.6...
! join2-1.6 expected: [1 11 111 1111]
! join2-1.6 got:      [1 11 111 1111 2 22 {} 5555 3 33 {} 5555]
join2-1.6-rj...
Error: no such column: t1.c
join2-1.7...
! join2-1.7 expected: [1 11 111 1111 2 22 {} {} 3 33 {} {}]
! join2-1.7 got:      [1 11 111 1111 2 22 {} 5555 3 33 {} 5555]
join2-1.7-rj...
Error: no such column: t3.b
join2-2.0... Ok
join2-2.1...
! join2-2.1 expected: [1 {ON clause references tables to its right}]
! join2-2.1 got:      [0 {one one one}]
join2-2.1b...
! join2-2.1b expected: [1 {ON clause references tables to its right}]
! join2-2.1b got:      [0 {one one one}]
join2-2.2... Ok
join2-3.0... Ok

Error in join2.test: no such column: t2.k3
no such column: t2.k3
    while executing
"db eval "EXPLAIN QUERY PLAN $sql" {
    set dx($id) $detail
    lappend cx($parent) $id
  }"
    (procedure "query_plan_graph" line 2)
    invoked from within
"query_plan_graph $sql"
    (procedure "do_eqp_test" line 4)
    invoked from within
"do_eqp_test 3.1 {
  SELECT v2 FROM t1 LEFT JOIN t2 USING (k2) LEFT JOIN t3_1 USING (k3);
} {
  QUERY PLAN
  |--SCAN t1
  `--SEARCH t2 USING INTEGER PR..."
    (file "/tank/repos/rustql-architecture/sqlite3/test/join2.test" line 127)
    invoked from within
"source $test_file"

==========================================
Test: join2
Time: 0s
Status: FAILED
