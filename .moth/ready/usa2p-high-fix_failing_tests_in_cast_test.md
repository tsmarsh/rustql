# Summary
- Test: sqlite3/test/cast.test
- Repro: `testfixture test/cast.test`
- Failing cases: cast-1.5, cast-1.6, cast-1.9, cast-1.10, cast-1.28, cast-1.38, cast-1.39, cast-1.45, cast-1.46, cast-1.48, cast-1.49, cast-1.50, cast-1.51, cast-1.53, cast-1.66, cast-1.67, cast-1.69, cast-3.3, cast-3.7, cast-3.13, cast-3.17, cast-3.23, cast-3.24, cast-3.32.1, cast-3.32.2, cast-3.32.3, cast-4.1, cast-4.2, cast-4.3, cast-4.4, cast-5.1, cast-5.2, cast-5.3, cast-7.1, cast-7.3, cast-7.4, cast-7.10, cast-7.32, cast-10.1, cast-10.3, cast-10.5
- Primary errors: ! cast-1.5 expected: [0] | ! cast-1.5 got:      [abc] | ! cast-1.6 expected: [integer]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-cast-3641386-1769533960730
DEBUG: tester.tcl sourced, db=db
Running cast.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/cast.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/cast.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
cast-1.1... Ok
cast-1.2... Ok
cast-1.3... Ok
cast-1.4... Ok
cast-1.5...
! cast-1.5 expected: [0]
! cast-1.5 got:      [abc]
cast-1.6...
! cast-1.6 expected: [integer]
! cast-1.6 got:      [blob]
cast-1.7... Ok
cast-1.8... Ok
cast-1.9...
! cast-1.9 expected: [0]
! cast-1.9 got:      [abc]
cast-1.10...
! cast-1.10 expected: [integer]
! cast-1.10 got:      [blob]
cast-1.11... Ok
cast-1.12... Ok
cast-1.13... Ok
cast-1.14... Ok
cast-1.15... Ok
cast-1.16... Ok
cast-1.17... Ok
cast-1.18... Ok
cast-1.19... Ok
cast-1.20... Ok
cast-1.21... Ok
cast-1.22... Ok
cast-1.23... Ok
cast-1.24... Ok
cast-1.25... Ok
cast-1.26... Ok
cast-1.27... Ok
cast-1.28...
! cast-1.28 expected: [blob]
! cast-1.28 got:      [integer]
cast-1.29... Ok
cast-1.30... Ok
cast-1.31... Ok
cast-1.32... Ok
cast-1.33... Ok
cast-1.34... Ok
cast-1.35... Ok
cast-1.36... Ok
cast-1.37... Ok
cast-1.38...
! cast-1.38 expected: [blob]
! cast-1.38 got:      [real]
cast-1.39...
! cast-1.39 expected: [123]
! cast-1.39 got:      [123.456]
cast-1.38...
! cast-1.38 expected: [integer]
! cast-1.38 got:      [real]
cast-1.41... Ok
cast-1.42... Ok
cast-1.43... Ok
cast-1.44... Ok
cast-1.45...
! cast-1.45 expected: [123]
! cast-1.45 got:      [123abc]
cast-1.46...
! cast-1.46 expected: [integer]
! cast-1.46 got:      [text]
cast-1.47... Ok
cast-1.48...
! cast-1.48 expected: [blob]
! cast-1.48 got:      [text]
cast-1.49...
! cast-1.49 expected: [123]
! cast-1.49 got:      [123abc]
cast-1.50...
! cast-1.50 expected: [integer]
! cast-1.50 got:      [text]
cast-1.51...
! cast-1.51 expected: [123.5]
! cast-1.51 got:      [123.5abc]
cast-1.53...
! cast-1.53 expected: [123]
! cast-1.53 got:      [123.5abc]
cast-1.60... Ok
cast-1.61... Ok
cast-1.62... Ok
cast-1.63... Ok
cast-1.64... Ok
cast-1.65... Ok
cast-1.66...
! cast-1.66 expected: [0.0]
! cast-1.66 got:      [abc]
cast-1.67...
! cast-1.67 expected: [real]
! cast-1.67 got:      [text]
cast-1.68... Ok
cast-1.69...
! cast-1.69 expected: [real]
! cast-1.69 got:      [blob]
cast-2.1... Ok
cast-2.2... Ok
cast-3.1... Ok
cast-3.2... Ok
cast-3.3...
! cast-3.3 expected: [9.22337203685477e+18]
! cast-3.3 got:      [9223372036854775000.0]
cast-3.4... Ok
cast-3.5... Ok
cast-3.6... Ok
cast-3.7...
! cast-3.7 expected: [-9.22337203685477e+18]
! cast-3.7 got:      [-9223372036854775000.0]
cast-3.8... Ok
cast-3.11... Ok
cast-3.12... Ok
cast-3.13...
! cast-3.13 expected: [9.22337203685477e+18]
! cast-3.13 got:      [9223372036854775000.0]
cast-3.14... Ok
cast-3.15... Ok
cast-3.16... Ok
cast-3.17...
! cast-3.17 expected: [-9.22337203685477e+18]
! cast-3.17 got:      [-9223372036854775000.0]
cast-3.18... Ok
cast-3.21... Ok
cast-3.22... Ok
cast-3.23...
! cast-3.23 expected: [9.22337203685477e+18]
! cast-3.23 got:      [9223372036854774800]
cast-3.24...
! cast-3.24 expected: [9223372036854774784]
! cast-3.24 got:      [9223372036854774800]
cast-3.31... Ok
cast-3.32.1...
! cast-3.32.1 expected: [SQLITE_ROW]
! cast-3.32.1 got:      [0]
cast-3.32.2...
! cast-3.32.2 expected: [12345]
! cast-3.32.2 got:      [0]
cast-3.32.3...
! cast-3.32.3 expected: [SQLITE_OK]
! cast-3.32.3 got:      [0]
cast-4.1...
! cast-4.1 expected: [abc 0]
! cast-4.1 got:      [abc abc]
cast-4.2...
! cast-4.2 expected: [0 abc]
! cast-4.2 got:      [abc abc]
cast-4.3...
! cast-4.3 expected: [abc 0 abc]
! cast-4.3 got:      [abc abc abc]
cast-4.4...
! cast-4.4 expected: [0 abc 0.0 abc]
! cast-4.4 got:      [abc abc abc abc]
cast-5.1...
! cast-5.1 expected: [9223372036854775807 9223372036854775807 9223372036854775807]
! cast-5.1 got:      [9223372036854775808 {  +000009223372036854775808} 12345678901234567890123]
cast-5.2...
! cast-5.2 expected: [-9223372036854775808 -9223372036854775808 -9223372036854775808]
! cast-5.2 got:      [-9223372036854775808 -9223372036854775809 -12345678901234567890123]
cast-5.3...
! cast-5.3 expected: [123 12300000 12300000.0]
! cast-5.3 got:      [123e+5 12300000.0 12300000.0]
cast-6.1... Ok
cast-7.1...
! cast-7.1 expected: [0]
! cast-7.1 got:      [-]
cast-7.2... Ok
cast-7.3...
! cast-7.3 expected: [0]
! cast-7.3 got:      [+]
cast-7.4...
! cast-7.4 expected: [0]
! cast-7.4 got:      [/]
cast-7.10...
! cast-7.10 expected: [-2851427734582196970]
! cast-7.10 got:      [-2851427734582196700.0]
cast-7.11... Ok
cast-7.12... Ok
cast-7.20... Ok
cast-7.30...
Error: cannot negate non-numeric value
cast-7.31... Ok
cast-7.32...
! cast-7.32 expected: [0]
! cast-7.32 got:      [.]
cast-7.33...
Error: cannot negate non-numeric value
cast-7.40... Ok
cast-7.41... Ok
cast-7.42... Ok
cast-7.43... Ok
cast-8.1... Ok
cast-8.2... Ok
cast-9.0...
Error: no such column: v1.c0
cast-9.1... Ok
cast-9.2... Ok
cast-9.3... Ok
cast-9.4... Ok
cast-9.5... Ok
cast-9.10... Ok
cast-9.11... Ok
cast-9.12... Ok
cast-9.13... Ok
cast-10.1...
! cast-10.1 expected: [44.0 55]
! cast-10.1 got:      [44.0]
cast-10.2... Ok
cast-10.3...
! cast-10.3 expected: [44.0 55]
! cast-10.3 got:      [44.0]
cast-10.4... Ok
cast-10.5...
! cast-10.5 expected: [X 44.0 X 55]
! cast-10.5 got:      [X 44.0]
cast-10.6... Ok
Running "cast"

Error in cast.test: couldn't read file "cast": no such file or directory
couldn't read file "cast": no such file or directory
    while executing
"source cast"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/cast.test" line 555)
    invoked from within
"source $test_file"

==========================================
Test: cast
Time: 0s
Status: FAILED
