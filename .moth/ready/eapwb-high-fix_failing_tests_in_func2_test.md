# Summary
- Test: sqlite3/test/func2.test
- Repro: `testfixture test/func2.test`
- Failing cases: func2-1.2.1, func2-1.2.2, func2-1.2.3, func2-2.1.2, func2-2.1.3, func2-2.1.4, func2-3.1.2, func2-3.1.3, func2-3.1.4
- Primary errors: ! func2-1.2.1 expected: [1 {wrong number of arguments to function SUBSTR()}] | ! func2-1.2.1 got:      [1 {wrong number of arguments to function substr()}] | ! func2-1.2.2 expected: [1 {wrong number of arguments to function SUBSTR()}]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-func2-3641410-1769533960741
DEBUG: tester.tcl sourced, db=db
Running func2.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/func2.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/func2.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
func2-1.1... Ok
func2-1.2.1...
! func2-1.2.1 expected: [1 {wrong number of arguments to function SUBSTR()}]
! func2-1.2.1 got:      [1 {wrong number of arguments to function substr()}]
func2-1.2.2...
! func2-1.2.2 expected: [1 {wrong number of arguments to function SUBSTR()}]
! func2-1.2.2 got:      [1 {wrong number of arguments to function substr()}]
func2-1.2.3...
! func2-1.2.3 expected: [1 {wrong number of arguments to function SUBSTR()}]
! func2-1.2.3 got:      [1 {wrong number of arguments to function substr()}]
func2-1.3... Ok
func2-1.4... Ok
func2-1.5... Ok
func2-1.6... Ok
func2-1.7... Ok
func2-1.8... Ok
func2-1.9... Ok
func2-1.10... Ok
func2-1.11... Ok
func2-1.12... Ok
func2-1.13... Ok
func2-1.14... Ok
func2-1.15... Ok
func2-1.16... Ok
func2-1.17.1... Ok
func2-1.17.2... Ok
func2-1.18... Ok
func2-1.19.0... Ok
func2-1.19.1... Ok
func2-1.19.2... Ok
func2-1.20... Ok
func2-1.21... Ok
func2-1.22... Ok
func2-1.23... Ok
func2-1.24... Ok
func2-1.25.0... Ok
func2-1.25.1... Ok
func2-1.25.2... Ok
func2-1.26... Ok
func2-1.27... Ok
func2-1.28.0... Ok
func2-1.28.1... Ok
func2-1.28.2... Ok
func2-1.29.1... Ok
func2-1.29.2... Ok
func2-1.30.0... Ok
func2-1.30.1... Ok
func2-1.30.2... Ok
func2-1.30.3... Ok
func2-1.31.0... Ok
func2-1.31.1... Ok
func2-1.31.2... Ok
func2-1.32.0... Ok
func2-1.32.1... Ok
func2-1.33.0... Ok
func2-1.33.1... Ok
func2-1.33.2... Ok
func2-1.34.0... Ok
func2-1.34.1... Ok
func2-1.34.2... Ok
func2-1.35.1... Ok
func2-1.35.2... Ok
func2-1.36... Ok
func2-1.37... Ok
func2-1.38.0... Ok
func2-1.38.1... Ok
func2-1.38.2... Ok
func2-2.1.1... Ok
func2-2.1.2...
! func2-2.1.2 expected: [1 {wrong number of arguments to function SUBSTR()}]
! func2-2.1.2 got:      [1 {wrong number of arguments to function substr()}]
func2-2.1.3...
! func2-2.1.3 expected: [1 {wrong number of arguments to function SUBSTR()}]
! func2-2.1.3 got:      [1 {wrong number of arguments to function substr()}]
func2-2.1.4...
! func2-2.1.4 expected: [1 {wrong number of arguments to function SUBSTR()}]
! func2-2.1.4 got:      [1 {wrong number of arguments to function substr()}]
func2-2.2.0... Ok
func2-2.2.1... Ok
func2-2.2.2... Ok
func2-2.2.3... Ok
func2-2.2.4... Ok
func2-2.2.5... Ok
func2-2.2.6... Ok
func2-2.3.0... Ok
func2-2.3.1... Ok
func2-2.3.2... Ok
func2-2.3.3... Ok
func2-2.3.4... Ok
func2-2.3.5... Ok
func2-2.3.6... Ok
func2-2.4.0... Ok
func2-2.4.1... Ok
func2-2.4.2... Ok
func2-2.5.0... Ok
func2-2.5.1... Ok
func2-2.5.2... Ok
func2-2.5.3... Ok
func2-2.6.0... Ok
func2-2.6.1... Ok
func2-2.6.2... Ok
func2-2.6.3... Ok
func2-2.7.0... Ok
func2-2.7.1... Ok
func2-2.7.2... Ok
func2-2.8.0... Ok
func2-2.8.1... Ok
func2-2.8.2... Ok
func2-2.8.3... Ok
func2-3.1.1... Ok
func2-3.1.2...
! func2-3.1.2 expected: [1 {wrong number of arguments to function SUBSTR()}]
! func2-3.1.2 got:      [1 {wrong number of arguments to function substr()}]
func2-3.1.3...
! func2-3.1.3 expected: [1 {wrong number of arguments to function SUBSTR()}]
! func2-3.1.3 got:      [1 {wrong number of arguments to function substr()}]
func2-3.1.4...
! func2-3.1.4 expected: [1 {wrong number of arguments to function SUBSTR()}]
! func2-3.1.4 got:      [1 {wrong number of arguments to function substr()}]
func2-3.2.0... Ok
func2-3.2.1... Ok
func2-3.2.2... Ok
func2-3.2.3... Ok
func2-3.3.0... Ok
func2-3.3.1... Ok
func2-3.3.2... Ok
func2-3.3.3... Ok
func2-3.4.0... Ok
func2-3.4.1... Ok
func2-3.4.2... Ok
func2-3.4.3... Ok
func2-3.5.0... Ok
func2-3.5.1... Ok
func2-3.5.2... Ok
func2-3.5.3... Ok
func2-3.6.0... Ok
func2-3.6.1... Ok
func2-3.6.2... Ok
func2-3.6.3... Ok
func2-3.7.0... Ok
func2-3.7.1... Ok
func2-3.7.2... Ok
func2-3.8.0... Ok
func2-3.8.1... Ok
func2-3.8.2... Ok
func2-3.9.0... Ok
func2-3.9.1... Ok
func2-3.9.2... Ok
func2-3.10... Ok
Running "func2"

Error in func2.test: couldn't read file "func2": no such file or directory
couldn't read file "func2": no such file or directory
    while executing
"source func2"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/func2.test" line 534)
    invoked from within
"source $test_file"

==========================================
Test: func2
Time: 0s
Status: FAILED
