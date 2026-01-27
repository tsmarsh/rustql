# Summary
- Test: sqlite3/test/pragma2.test
- Repro: `testfixture test/pragma2.test`
- Failing cases: pragma2-1.3, pragma2-1.4, pragma2-2.5, pragma2-3.1, pragma2-3.2, pragma2-3.3, pragma2-4.1, pragma2-4.4, pragma2-4.5.1, pragma2-4.5.2, pragma2-4.5.3, pragma2-4.5.4, pragma2-4.6, pragma2-4.8, pragma2-5.1, pragma2-5.3
- Primary errors: ! pragma2-1.3 expected: [1] | ! pragma2-1.3 got:      [0] | ! pragma2-1.4 expected: [1]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-pragma2-3641439-1769533960764
DEBUG: tester.tcl sourced, db=db
Running pragma2.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/pragma2.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/pragma2.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
pragma2-1.1... Ok
pragma2-1.2... Ok
pragma2-1.3...
! pragma2-1.3 expected: [1]
! pragma2-1.3 got:      [0]
pragma2-1.4...
! pragma2-1.4 expected: [1]
! pragma2-1.4 got:      [0]
pragma2-2.1... Ok
pragma2-2.2... Ok
pragma2-2.3...
Error: near "$": syntax error
pragma2-2.4...
Error: could not read "test2.db": no such file or directory
pragma2-2.5...
! pragma2-2.5 expected: [9]
! pragma2-2.5 got:      [0]
pragma2-3.1...
! pragma2-3.1 expected: [9 1 1]
! pragma2-3.1 got:      [0 0 0]
pragma2-3.2...
! pragma2-3.2 expected: [1 1]
! pragma2-3.2 got:      [0 0]
pragma2-3.3...
! pragma2-3.3 expected: [9 9]
! pragma2-3.3 got:      [0 0]
pragma2-4.1...
! pragma2-4.1 expected: [2000 2000 2000]
! pragma2-4.1 got:      [1 1 1]
pragma2-4.2... Ok
pragma2-4.3... Ok
pragma2-4.4...
! pragma2-4.4 expected: [main exclusive temp unknown]
! pragma2-4.4 got:      [main unlocked temp unlocked]
pragma2-4.5.1...
! pragma2-4.5.1 expected: [0 main reserved temp unknown]
! pragma2-4.5.1 got:      [0 main unlocked temp unlocked]
pragma2-4.5.2...
! pragma2-4.5.2 expected: [100000 main reserved temp unknown]
! pragma2-4.5.2 got:      [1 main unlocked temp unlocked]
pragma2-4.5.3...
! pragma2-4.5.3 expected: [50 main exclusive temp unknown]
! pragma2-4.5.3 got:      [1 main unlocked temp unlocked]
pragma2-4.5.4...
! pragma2-4.5.4 expected: [50 main exclusive temp unknown]
! pragma2-4.5.4 got:      [1 main unlocked temp unlocked]
pragma2-4.6...
! pragma2-4.6 expected: [main unlocked temp unknown aux1 reserved]
! pragma2-4.6 got:      [main unlocked temp unlocked aux1 unlocked]
pragma2-4.7... Ok
pragma2-4.8...
! pragma2-4.8 expected: [main unlocked temp unknown aux1 exclusive]
! pragma2-4.8 got:      [main unlocked temp unlocked aux1 unlocked]
pragma2-5.1...
! pragma2-5.1 expected: [2]
! pragma2-5.1 got:      [1]
pragma2-5.2... Ok
pragma2-5.3...
! pragma2-5.3 expected: [3]
! pragma2-5.3 got:      [1]
Running "pragma2"

Error in pragma2.test: couldn't read file "pragma2": no such file or directory
couldn't read file "pragma2": no such file or directory
    while executing
"source pragma2"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/pragma2.test" line 275)
    invoked from within
"source $test_file"

==========================================
Test: pragma2
Time: 0s
Status: FAILED
