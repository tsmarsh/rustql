# Summary
- Test: sqlite3/test/join3.test
- Repro: `testfixture test/join3.test`
- Failing cases: join3-3.1
- Primary errors: ! join3-3.1 expected: [1 {at most 64 tables in a join}] | ! join3-3.1 got:      [0 {1 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64}] | Error in join3.test: couldn't read file "join3": no such file or directory

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-join3-3641357-1769533960711
DEBUG: tester.tcl sourced, db=db
Running join3.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/join3.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/join3.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
join3-1.1... Ok
join3-1.2... Ok
join3-1.3... Ok
join3-1.4... Ok
join3-1.5... Ok
join3-1.6... Ok
join3-1.7... Ok
join3-1.8... Ok
join3-1.9... Ok
join3-1.10... Ok
join3-1.11... Ok
join3-1.12... Ok
join3-1.13... Ok
join3-1.14... Ok
join3-1.15... Ok
join3-1.16... Ok
join3-1.17... Ok
join3-1.18... Ok
join3-1.19... Ok
join3-1.20... Ok
join3-1.21... Ok
join3-1.22... Ok
join3-1.23... Ok
join3-1.24... Ok
join3-1.25... Ok
join3-1.26... Ok
join3-1.27... Ok
join3-1.28... Ok
join3-1.29... Ok
join3-1.30... Ok
join3-1.31... Ok
join3-1.32... Ok
join3-1.33... Ok
join3-1.34... Ok
join3-1.35... Ok
join3-1.36... Ok
join3-1.37... Ok
join3-1.38... Ok
join3-1.39... Ok
join3-1.40... Ok
join3-1.41... Ok
join3-1.42... Ok
join3-1.43... Ok
join3-1.44... Ok
join3-1.45... Ok
join3-1.46... Ok
join3-1.47... Ok
join3-1.48... Ok
join3-1.49... Ok
join3-1.50... Ok
join3-1.51... Ok
join3-1.52... Ok
join3-1.53... Ok
join3-1.54... Ok
join3-1.55... Ok
join3-1.56... Ok
join3-1.57... Ok
join3-1.58... Ok
join3-1.59... Ok
join3-1.60... Ok
join3-1.61... Ok
join3-1.62... Ok
join3-1.63... Ok
join3-1.64... Ok
join3-2.1... Ok
join3-2.2... Ok
join3-2.3... Ok
join3-2.4... Ok
join3-2.5... Ok
join3-2.6... Ok
join3-2.7... Ok
join3-2.8... Ok
join3-2.9... Ok
join3-2.10... Ok
join3-2.11... Ok
join3-2.12... Ok
join3-2.13... Ok
join3-2.14... Ok
join3-2.15... Ok
join3-2.16... Ok
join3-2.17... Ok
join3-2.18... Ok
join3-2.19... Ok
join3-2.20... Ok
join3-2.21... Ok
join3-2.22... Ok
join3-2.23... Ok
join3-2.24... Ok
join3-2.25... Ok
join3-2.26... Ok
join3-2.27... Ok
join3-2.28... Ok
join3-2.29... Ok
join3-2.30... Ok
join3-2.31... Ok
join3-2.32... Ok
join3-2.33... Ok
join3-2.34... Ok
join3-2.35... Ok
join3-2.36... Ok
join3-2.37... Ok
join3-2.38... Ok
join3-2.39... Ok
join3-2.40... Ok
join3-2.41... Ok
join3-2.42... Ok
join3-2.43... Ok
join3-2.44... Ok
join3-2.45... Ok
join3-2.46... Ok
join3-2.47... Ok
join3-2.48... Ok
join3-2.49... Ok
join3-2.50... Ok
join3-2.51... Ok
join3-2.52... Ok
join3-2.53... Ok
join3-2.54... Ok
join3-2.55... Ok
join3-2.56... Ok
join3-2.57... Ok
join3-2.58... Ok
join3-2.59... Ok
join3-2.60... Ok
join3-2.61... Ok
join3-2.62... Ok
join3-2.63... Ok
join3-2.64... Ok
join3-3.1...
! join3-3.1 expected: [1 {at most 64 tables in a join}]
! join3-3.1 got:      [0 {1 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64}]
Running "join3"

Error in join3.test: couldn't read file "join3": no such file or directory
couldn't read file "join3": no such file or directory
    while executing
"source join3"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/join3.test" line 62)
    invoked from within
"source $test_file"

==========================================
Test: join3
Time: 0s
Status: FAILED
