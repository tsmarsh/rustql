# Summary
- Test: sqlite3/test/types3.test
- Repro: `testfixture test/types3.test`
- Failing cases: types3-1.2, types3-1.3, types3-1.4, types3-1.5, types3-1.6, types3-2.1, types3-2.2, types3-2.4.1, types3-2.4.2
- Primary errors: ! types3-1.2 expected: [int integer] | ! types3-1.2 got:      [integer] | ! types3-1.3 expected: [int integer]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-types3-3641388-1769533960733
DEBUG: tester.tcl sourced, db=db
Running types3.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/types3.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/types3.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
types3-1.1... Ok
types3-1.2...
! types3-1.2 expected: [int integer]
! types3-1.2 got:      [integer]
types3-1.3...
! types3-1.3 expected: [int integer]
! types3-1.3 got:      [integer]
types3-1.4...
! types3-1.4 expected: [double real]
! types3-1.4 got:      [real]
types3-1.5...
! types3-1.5 expected: [bytearray blob]
! types3-1.5 got:      [text]
types3-1.6...
! types3-1.6 expected: [bytearray text]
! types3-1.6 got:      [text]
types3-2.1...
! types3-2.1 expected: [bytearray]
! types3-2.1 got:      []
types3-2.2...
! types3-2.2 expected: [int]
! types3-2.2 got:      []
types3-2.3... Ok
types3-2.4.1...
! types3-2.4.1 expected: [double]
! types3-2.4.1 got:      []
types3-2.4.2...
! types3-2.4.2 expected: [double]
! types3-2.4.2 got:      []
types3-2.5... Ok
types3-2.6... Ok
types3-3.1... Ok
types3-3.2...
Error: no such function: ADD_TEXT_TYPE
types3-3.3...
Error: no such function: ADD_INT_TYPE
types3-3.4...
Error: no such function: ADD_REAL_TYPE
types3-3.5...
Error: no such function: ADD_TEXT_TYPE
Running "types3"

Error in types3.test: couldn't read file "types3": no such file or directory
couldn't read file "types3": no such file or directory
    while executing
"source types3"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/types3.test" line 124)
    invoked from within
"source $test_file"

==========================================
Test: types3
Time: 0s
Status: FAILED
