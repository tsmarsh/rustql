# Summary
- Test: sqlite3/test/func3.test
- Repro: `testfixture test/func3.test`
- Failing cases: func3-1.4, func3-2.2, func3-3.2, func3-4.1, func3-4.2, func3-5.8, func3-5.9, func3-5.10, func3-5.20, func3-5.39, func3-5.59, func3-6.0
- Primary errors: ! func3-1.4 expected: [1] | ! func3-1.4 got:      [0] | ! func3-2.2 expected: [1]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-func3-3641412-1769533960740
DEBUG: tester.tcl sourced, db=db
Running func3.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/func3.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/func3.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
func3-1.1... Ok
func3-1.2... Ok
func3-1.3... Ok
func3-1.4...
! func3-1.4 expected: [1]
! func3-1.4 got:      [0]
func3-2.1... Ok
func3-2.2...
! func3-2.2 expected: [1]
! func3-2.2 got:      [0]
func3-3.1... Ok
func3-3.2...
! func3-3.2 expected: [1]
! func3-3.2 got:      [0]
func3-4.1...
! func3-4.1 expected: [1 SQLITE_MISUSE]
! func3-4.1 got:      [0 0]
func3-4.2...
! func3-4.2 expected: [1]
! func3-4.2 got:      [0]
func3-5.1... Ok
func3-5.2...
Error: invalid integer
func3-5.3... Ok
func3-5.4... Ok
func3-5.5... Ok
func3-5.6... Ok
func3-5.7... Ok
func3-5.8...
! func3-5.8 expected: [1 {second argument to likelihood() must be a constant between 0.0 and 1.0}]
! func3-5.8 got:      [0 123]
func3-5.9...
! func3-5.9 expected: [1 {second argument to likelihood() must be a constant between 0.0 and 1.0}]
! func3-5.9 got:      [0 123]
func3-5.10...
! func3-5.10 expected: [1 {second argument to likelihood() must be a constant between 0.0 and 1.0}]
! func3-5.10 got:      [0 123]
func3-5.20...
! func3-5.20 expected: [{} Real 0 4 0 Real(1.0) 0 {} 1 String8 0 5 0 Text(\"2.0\") 0 {} 2 Add 5 4 2 Unused 0 {} 3 Integer 4 6 0 Unused 0 {} 4 Integer 11 7 0 Unused 0 {} 5 Multiply 7 6 3 Unused 0 {} 6 Function 2 2 1 Text(\"min\") 0 {} 7 ResultRow 1 1 0 Unused 0 {} 8 Halt 0 0 0 Unused 0 {}]
! func3-5.20 got:      [{} Real 0 6 0 Real(1.0) 0 {} 1 String8 0 7 0 Text(\"2.0\") 0 {} 2 Add 7 6 4 Unused 0 {} 3 Integer 4 8 0 Unused 0 {} 4 Integer 11 9 0 Unused 0 {} 5 Multiply 9 8 5 Unused 0 {} 6 Function 2 4 2 Text(\"min\") 0 {} 7 Real 0 3 0 Real(0.5) 0 {} 8 Function 2 2 1 Text(\"likelihood\") 0 {} 9 ResultRow 1 1 0 Unused 0 {} 10 Halt 0 0 0 Unused 0 {}]
func3-5.30... Ok
func3-5.31...
Error: invalid integer
func3-5.32... Ok
func3-5.33... Ok
func3-5.34... Ok
func3-5.35... Ok
func3-5.39...
! func3-5.39 expected: [{} Real 0 4 0 Real(1.0) 0 {} 1 String8 0 5 0 Text(\"2.0\") 0 {} 2 Add 5 4 2 Unused 0 {} 3 Integer 4 6 0 Unused 0 {} 4 Integer 11 7 0 Unused 0 {} 5 Multiply 7 6 3 Unused 0 {} 6 Function 2 2 1 Text(\"min\") 0 {} 7 ResultRow 1 1 0 Unused 0 {} 8 Halt 0 0 0 Unused 0 {}]
! func3-5.39 got:      [{} Real 0 5 0 Real(1.0) 0 {} 1 String8 0 6 0 Text(\"2.0\") 0 {} 2 Add 6 5 3 Unused 0 {} 3 Integer 4 7 0 Unused 0 {} 4 Integer 11 8 0 Unused 0 {} 5 Multiply 8 7 4 Unused 0 {} 6 Function 2 3 2 Text(\"min\") 0 {} 7 Function 1 2 1 Text(\"unlikely\") 0 {} 8 ResultRow 1 1 0 Unused 0 {} 9 Halt 0 0 0 Unused 0 {}]
func3-5.40... Ok
func3-5.41... Ok
func3-5.41... Ok
func3-5.50... Ok
func3-5.51...
Error: invalid integer
func3-5.52... Ok
func3-5.53... Ok
func3-5.54... Ok
func3-5.55... Ok
func3-5.59...
! func3-5.59 expected: [{} Real 0 4 0 Real(1.0) 0 {} 1 String8 0 5 0 Text(\"2.0\") 0 {} 2 Add 5 4 2 Unused 0 {} 3 Integer 4 6 0 Unused 0 {} 4 Integer 11 7 0 Unused 0 {} 5 Multiply 7 6 3 Unused 0 {} 6 Function 2 2 1 Text(\"min\") 0 {} 7 ResultRow 1 1 0 Unused 0 {} 8 Halt 0 0 0 Unused 0 {}]
! func3-5.59 got:      [{} Real 0 5 0 Real(1.0) 0 {} 1 String8 0 6 0 Text(\"2.0\") 0 {} 2 Add 6 5 3 Unused 0 {} 3 Integer 4 7 0 Unused 0 {} 4 Integer 11 8 0 Unused 0 {} 5 Multiply 8 7 4 Unused 0 {} 6 Function 2 3 2 Text(\"min\") 0 {} 7 Function 1 2 1 Text(\"likely\") 0 {} 8 ResultRow 1 1 0 Unused 0 {} 9 Halt 0 0 0 Unused 0 {}]
func3-6.0...
! func3-6.0 expected: []
! func3-6.0 got:      [0]
Running "func3"

Error in func3.test: couldn't read file "func3": no such file or directory
couldn't read file "func3": no such file or directory
    while executing
"source func3"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/func3.test" line 211)
    invoked from within
"source $test_file"

==========================================
Test: func3
Time: 0s
Status: FAILED
