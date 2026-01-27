# Summary
- Test: sqlite3/test/where2.test
- Repro: `testfixture test/where2.test`
- Failing cases: where2-1.1, where2-2.1, where2-2.3, where2-3.1, where2-3.2, where2-4.1, where2-4.2, where2-4.6a, where2-4.6b, where2-4.6c, where2-4.6d, where2-4.6x, where2-4.6y, where2-5.1, where2-5.2a, where2-5.2b, where2-6.1.1, where2-6.1.2, where2-6.2, where2-6.5, where2-6.6, where2-6.7, where2-6.10, where2-6.11, where2-6.11.2, where2-6.11.3, where2-6.11.4, where2-6.12, where2-6.12.2, where2-6.12.3, where2-6.13, where2-6.20, where2-6.21, where2-6.22, where2-6.23, where2-7.2, where2-7.4, where2-11.1, where2-12.1, where2-14.1
- Primary errors: ! where2-1.1 expected: [85 6 7396 7402 nosort t1 i1w] | ! where2-1.1 got:      [85 6 7396 7402 nosort t1 i1xy] | ! where2-2.1 expected: [85 6 7396 7402 nosort t1 i1w]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-where2-3641350-1769533960709
DEBUG: tester.tcl sourced, db=db
Running where2.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/where2.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/where2.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
where2-1.0... Ok
where2-1.1...
! where2-1.1 expected: [85 6 7396 7402 nosort t1 i1w]
! where2-1.1 got:      [85 6 7396 7402 nosort t1 i1xy]
where2-1.3... Ok
where2-2.1...
! where2-2.1 expected: [85 6 7396 7402 nosort t1 i1w]
! where2-2.1 got:      [85 6 7396 7402 sort t1 i1w]
where2-2.2... Ok
where2-2.3...
! where2-2.3 expected: [85 6 7396 7402 nosort t1 *]
! where2-2.3 got:      [85 6 7396 7402 sort t1 *]
where2-2.4...
Error: no such table: cnt
where2-2.5...
Error: no such column: x
where2-2.5b...
Error: no such column: x
where2-2.6...
Error: no such column: x
where2-2.6b...
Error: no such column: x
where2-3.1...
! where2-3.1 expected: [1 0 4 4 2 1 9 10 nosort t1 *]
! where2-3.1 got:      [1 0 4 4 2 1 9 10 sort t1 *]
where2-3.2...
! where2-3.2 expected: [100 6 10201 10207 99 6 10000 10006 nosort t1 *]
! where2-3.2 got:      [100 6 10201 10207 99 6 10000 10006 sort t1 *]
where2-4.1...
! where2-4.1 expected: [99 6 10000 10006 100 6 10201 10207 sort t1 i1zyx]
! where2-4.1 got:      [99 6 10000 10006 100 6 10201 10207 sort t1 i1xy]
where2-4.2...
! where2-4.2 expected: [99 6 10000 10006 sort t1 i1zyx]
! where2-4.2 got:      [99 6 10000 10006 sort t1 i1xy]
where2-4.3... Ok
where2-4.4...
Error: no such column: w
where2-4.5...
Error: no such column: y
where2-4.6a...
! where2-4.6a expected: [99 6 10000 10006 nosort t1 i1xy]
! where2-4.6a got:      [99 6 10000 10006 sort t1 *]
where2-4.6b...
! where2-4.6b expected: [99 6 10000 10006 nosort t1 i1xy]
! where2-4.6b got:      [99 6 10000 10006 sort t1 *]
where2-4.6c...
! where2-4.6c expected: [99 6 10000 10006 nosort t1 i1xy]
! where2-4.6c got:      [99 6 10000 10006 sort t1 *]
where2-4.6d...
! where2-4.6d expected: [99 6 10000 10006 sort t1 i1xy]
! where2-4.6d got:      [99 6 10000 10006 sort t1 *]
where2-4.6x...
! where2-4.6x expected: [99 6 10000 10006 100 6 10201 10207 sort t1 i1zyx]
! where2-4.6x got:      [99 6 10000 10006 100 6 10201 10207 sort t1 *]
where2-4.6y...
! where2-4.6y expected: [100 6 10201 10207 99 6 10000 10006 sort t1 i1zyx]
! where2-4.6y got:      [100 6 10201 10207 99 6 10000 10006 sort t1 *]
where2-4.7...
Error: no such column: w
where2-5.1...
! where2-5.1 expected: [99 6 10000 10006 nosort t1 i1w]
! where2-5.1 got:      [99 6 10000 10006 sort t1 i1w]
where2-5.2a...
! where2-5.2a expected: [99 6 10000 10006 nosort t1 i1w]
! where2-5.2a got:      [99 6 10000 10006 sort t1 *]
where2-5.2b...
! where2-5.2b expected: [99 6 10000 10006 nosort t1 i1w]
! where2-5.2b got:      [99 6 10000 10006 sort t1 *]
where2-6.1.1...
! where2-6.1.1 expected: [99 6 10000 10006 100 6 10201 10207 sort t1 i1w]
! where2-6.1.1 got:      [99 6 10000 10006 100 6 10201 10207 sort t1 *]
where2-6.1.2...
! where2-6.1.2 expected: [99 6 10000 10006 100 6 10201 10207 sort t1 i1w]
! where2-6.1.2 got:      [99 6 10000 10006 100 6 10201 10207 sort t1 *]
where2-6.2...
! where2-6.2 expected: [6 2 49 51 99 6 10000 10006 100 6 10201 10207 sort t1 i1w]
! where2-6.2 got:      [6 2 49 51 99 6 10000 10006 100 6 10201 10207 sort t1 *]
where2-6.3... Ok
where2-6.4... Ok
where2-6.5... Ok
where2-6.5...
! where2-6.5 expected: [1 0 4 4 2 1 9 10 sort a i1w b i1zyx]
! where2-6.5 got:      [1 0 4 4 2 1 9 10 sort b * a i1w]
where2-6.6...
! where2-6.6 expected: [1 0 4 4 2 1 9 10 sort a i1w b i1zyx]
! where2-6.6 got:      [1 0 4 4 2 1 9 10 sort b * a i1w]
where2-6.7...
! where2-6.7 expected: [123 0123 nosort t2249b * t2249a sqlite_autoindex_t2249a_1]
! where2-6.7 got:      [nosort t2249b * t2249a sqlite_autoindex_t2249a_1]
where2-6.9... Ok
where2-6.9.2... Ok
where2-6.10...
! where2-6.10 expected: [nosort t2249b * t2249a sqlite_autoindex_t2249a_1]
! where2-6.10 got:      [nosort t2249b * t2249a *]
where2-6.11...
! where2-6.11 expected: [123 0123 nosort t2249b * t2249a sqlite_autoindex_t2249a_1]
! where2-6.11 got:      [123 0123 nosort t2249b * t2249a *]
where2-6.11.2...
! where2-6.11.2 expected: [123 0123 nosort t2249b * t2249a sqlite_autoindex_t2249a_1]
! where2-6.11.2 got:      [123 0123 nosort t2249b * t2249a *]
where2-6.11.3...
! where2-6.11.3 expected: [123 0123 nosort t2249b * t2249a sqlite_autoindex_t2249a_1]
! where2-6.11.3 got:      [123 0123 nosort t2249b * t2249a *]
where2-6.11.4...
! where2-6.11.4 expected: [123 0123 nosort t2249b * t2249a sqlite_autoindex_t2249a_1]
! where2-6.11.4 got:      [123 0123 nosort t2249b * t2249a *]
where2-6.12...
! where2-6.12 expected: [nosort t2249b * t2249a sqlite_autoindex_t2249a_1]
! where2-6.12 got:      [nosort t2249b * t2249a *]
where2-6.12.2...
! where2-6.12.2 expected: [nosort t2249b * t2249a sqlite_autoindex_t2249a_1]
! where2-6.12.2 got:      [nosort t2249b * t2249a *]
where2-6.12.3...
! where2-6.12.3 expected: [nosort t2249b * t2249a sqlite_autoindex_t2249a_1]
! where2-6.12.3 got:      [nosort t2249b * t2249a *]
where2-6.13...
! where2-6.13 expected: [nosort t2249b * t2249a sqlite_autoindex_t2249a_1]
! where2-6.13 got:      [nosort t2249b * t2249a *]
where2-6.20...
! where2-6.20 expected: [0123 0123 nosort x sqlite_autoindex_t2249a_1 y sqlite_autoindex_t2249a_1]
! where2-6.20 got:      [0123 0123 nosort x * y sqlite_autoindex_t2249a_1]
where2-6.21...
! where2-6.21 expected: [0123 0123 nosort x sqlite_autoindex_t2249a_1 y sqlite_autoindex_t2249a_1]
! where2-6.21 got:      [0123 0123 nosort x * y *]
where2-6.22...
! where2-6.22 expected: [0123 0123 nosort x sqlite_autoindex_t2249a_1 y sqlite_autoindex_t2249a_1]
! where2-6.22 got:      [0123 0123 nosort x * y *]
where2-6.23...
! where2-6.23 expected: [0123 0123 nosort x sqlite_autoindex_t2249a_1 y sqlite_autoindex_t2249a_1]
! where2-6.23 got:      [0123 0123 nosort x * y *]
where2-7.1... Ok
where2-7.2...
! where2-7.2 expected: [1 2 3 nosort]
! where2-7.2 got:      [1 2 3 sort]
where2-7.3... Ok
where2-7.4...
! where2-7.4 expected: [1 2 3 2 3 nosort]
! where2-7.4 got:      [1 2 3 2 3 sort]
where2-8.1... Ok
where2-8.2... Ok
where2-8.3... Ok
where2-8.4... Ok
where2-8.5... Ok
where2-8.6... Ok
where2-8.7... Ok
where2-8.8... Ok
where2-8.9... Ok
where2-8.10... Ok
where2-8.11... Ok
where2-8.12... Ok
where2-8.13... Ok
where2-8.14... Ok
where2-8.15... Ok
where2-8.16... Ok
where2-8.17... Ok
where2-8.18... Ok
where2-8.19... Ok
where2-8.20... Ok
where2-11.1...
! where2-11.1 expected: [3 9]
! where2-11.1 got:      []
where2-11.2... Ok
where2-11.3... Ok
where2-11.4... Ok
where2-12.1...
! where2-12.1 expected: [/SEARCH b .*SEARCH b /]
! where2-12.1 got:      [0 0 0 {SCAN a} 1 0 0 {SCAN b}]
where2-13.1... Ok
where2-14.1...
! where2-14.1 expected: []
! where2-14.1 got:      [1 2 3 4]
where2-15.1... Ok
Running "where2"

Error in where2.test: couldn't read file "where2": no such file or directory
couldn't read file "where2": no such file or directory
    while executing
"source where2"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/where2.test" line 797)
    invoked from within
"source $test_file"

==========================================
Test: where2
Time: 0s
Status: FAILED
