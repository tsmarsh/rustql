# Summary
- Test: sqlite3/test/like.test
- Repro: `testfixture test/like.test`
- Failing cases: like-3.3.100.cnt, like-3.3.105, like-3.3.106, like-3.11, like-3.13, like-3.18, like-3.23, like-3.24, like-3.25, like-3.26, like-3.27, like-4.2, like-4.3, like-4.5, like-5.1, like-5.3, like-5.4, like-5.6, like-5.7, like-5.8, like-5.11, like-5.13, like-5.14, like-5.16, like-5.17, like-5.18, like-5.21, like-5.22, like-5.23, like-5.24, like-8.3, like-8.4, like-9.1, like-9.3.2, like-10.5b, like-10.10, like-10.11, like-10.12, like-10.13, like-10.14, like-10.15, like-11.1, like-11.2, like-11.3, like-11.4, like-11.5, like-11.6, like-11.7, like-11.8, like-11.9, like-11.10, like-12.11, like-12.13, like-12.15, like-15.101, like-15.112, like-15.121
- Primary errors: ! like-3.3.100.cnt expected: [0] | ! like-3.3.100.cnt got:      [2] | Error: near "$": syntax error

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-like-3641423-1769533960749
DEBUG: tester.tcl sourced, db=db
Running like.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/like.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/like.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
like-1.0... Ok
like-1.1... Ok
like-1.2... Ok
like-1.3... Ok
like-1.4... Ok
like-1.5.1... Ok
like-1.5.2... Ok
like-1.5.3... Ok
like-1.6... Ok
like-1.7... Ok
like-1.8... Ok
like-1.9... Ok
like-1.10... Ok
like-2.1... Ok
like-2.2... Ok
like-2.3... Ok
like-2.4... Ok
like-3.1... Ok
like-3.2... Ok
like-3.3.100... Ok
like-3.3.100.cnt...
! like-3.3.100.cnt expected: [0]
! like-3.3.100.cnt got:      [2]
like-3.3.102...
Error: near "$": syntax error
like-3.3.103... Ok
like-3.3.104...
Error: near "$": syntax error
like-3.3.105...
! like-3.3.105 expected: [12]
! like-3.3.105 got:      [0]
like-3.3.105... Ok
like-3.3.106...
! like-3.3.106 expected: [0]
! like-3.3.106 got:      [2]
like-3.4.2... Ok
like-3.4.3... Ok
like-3.4.4... Ok
like-3.4.5... Ok
like-3.5... Ok
like-3.6... Ok
like-3.7... Ok
like-3.8... Ok
like-3.9... Ok
like-3.10... Ok
like-3.11...
! like-3.11 expected: [abcd bcd nosort {} i1]
! like-3.11 got:      [abcd bcd sort t1 *]
like-3.12... Ok
like-3.13...
! like-3.13 expected: [ABC {ABC abc xyz} abc abcd nosort {} i1]
! like-3.13 got:      [ABC {ABC abc xyz} abc abcd sort t1 *]
like-3.14... Ok
like-3.15... Ok
like-3.16... Ok
like-3.17... Ok
like-3.18...
! like-3.18 expected: [12]
! like-3.18 got:      [0]
like-3.19... Ok
like-3.20... Ok
like-3.21... Ok
like-3.22... Ok
like-3.23...
! like-3.23 expected: [abd acd nosort {} i1]
! like-3.23 got:      [abd acd sort t1 *]
like-3.24...
! like-3.24 expected: [6]
! like-3.24 got:      [0]
like-3.25...
! like-3.25 expected: [a nosort {} i1]
! like-3.25 got:      [a sort t1 *]
like-3.26...
! like-3.26 expected: [abcd nosort {} i1]
! like-3.26 got:      [abcd sort t1 *]
like-3.27...
! like-3.27 expected: [nosort {} i1]
! like-3.27 got:      [nosort t1 *]
like-4.1... Ok
like-4.2...
! like-4.2 expected: [0]
! like-4.2 got:      [2]
like-4.3...
! like-4.3 expected: [abc abcd nosort {} i1]
! like-4.3 got:      [abc abcd sort t1 *]
like-4.4... Ok
like-4.5...
! like-4.5 expected: [abc abcd nosort {} i1]
! like-4.5 got:      [abc abcd sort t1 *]
like-4.6... Ok
like-5.1...
! like-5.1 expected: [ABC {ABC abc xyz} abc abcd nosort {} i1]
! like-5.1 got:      [ABC {ABC abc xyz} abc abcd sort t1 *]
like-5.2... Ok
like-5.3...
! like-5.3 expected: [abc ABC {ABC abc xyz} abcd nosort {} i2]
! like-5.3 got:      [ABC {ABC abc xyz} abc abcd sort t2 *]
like-5.4...
! like-5.4 expected: [0]
! like-5.4 got:      [12]
like-5.5... Ok
like-5.6...
! like-5.6 expected: [12]
! like-5.6 got:      [2]
like-5.7...
! like-5.7 expected: [abc abcd nosort {} i2]
! like-5.7 got:      [abc abcd sort t2 *]
like-5.8...
! like-5.8 expected: [12]
! like-5.8 got:      [0]
like-5.11...
! like-5.11 expected: [ABC {ABC abc xyz} abc abcd nosort {} i1]
! like-5.11 got:      [ABC {ABC abc xyz} abc abcd sort t1 *]
like-5.12... Ok
like-5.13...
! like-5.13 expected: [abc ABC {ABC abc xyz} abcd nosort {} i2]
! like-5.13 got:      [ABC {ABC abc xyz} abc abcd sort t2 *]
like-5.14...
! like-5.14 expected: [0]
! like-5.14 got:      [12]
like-5.15... Ok
like-5.16...
! like-5.16 expected: [12]
! like-5.16 got:      [2]
like-5.17...
! like-5.17 expected: [ABC {ABC abc xyz} nosort {} i2]
! like-5.17 got:      [ABC {ABC abc xyz} sort t2 *]
like-5.18...
! like-5.18 expected: [12]
! like-5.18 got:      [0]
like-5.21...
! like-5.21 expected: [zz-lower-lower zZ-lower-upper Zz-upper-lower ZZ-upper-upper nosort {} i2]
! like-5.21 got:      [ZZ-upper-upper zZ-lower-upper Zz-upper-lower zz-lower-lower nosort t2 *]
like-5.22...
! like-5.22 expected: [zz-lower-lower zZ-lower-upper Zz-upper-lower ZZ-upper-upper nosort {} i2]
! like-5.22 got:      [ZZ-upper-upper zZ-lower-upper Zz-upper-lower zz-lower-lower nosort t2 *]
like-5.23...
! like-5.23 expected: [zz-lower-lower zZ-lower-upper Zz-upper-lower ZZ-upper-upper nosort {} i2]
! like-5.23 got:      [ZZ-upper-upper zZ-lower-upper Zz-upper-lower zz-lower-lower nosort t2 *]
like-5.24...
! like-5.24 expected: [zz-lower-lower zZ-lower-upper Zz-upper-lower ZZ-upper-upper nosort {} i2]
! like-5.24 got:      [ZZ-upper-upper zZ-lower-upper Zz-upper-lower zz-lower-lower nosort t2 *]
like-5.25... Ok
like-5.26... Ok
like-5.27... Ok
like-5.28... Ok
like-6.1... Ok
like-7.1... Ok
like-8.1... Ok
like-8.2... Ok
like-8.3...
! like-8.3 expected: [1 abcdef 1 ghijkl 1 mnopqr 2 ghijkl]
! like-8.3 got:      [1 ghijkl 2 ghijkl]
like-8.4...
! like-8.4 expected: [1 abcdef 1 ghijkl 1 mnopqr 2 abcdef 2 ghijkl 2 mnopqr]
! like-8.4 got:      [1 ghijkl 2 ghijkl]
like-9.1...
! like-9.1 expected: [xyz scan 0 sort 0]
! like-9.1 got:      [xyz scan 19 sort 0]
like-9.2... Ok
like-9.3.1... Ok
like-9.3.2...
! like-9.3.2 expected: [1]
! like-9.3.2 got:      [0]
like-9.4.1... Ok
like-9.4.2... Ok
like-9.4.3... Ok
like-9.5.1... Ok
like-9.5.2... Ok
like-10.1... Ok
like-10.2... Ok
like-10.3... Ok
like-10.4... Ok
like-10.5b...
! like-10.5b expected: [12 123 scan 3 like 0]
! like-10.5b got:      [12 123 scan 5 like 6]
like-10.6... Ok
like-10.10...
! like-10.10 expected: [12 123 scan 5 like 6]
! like-10.10 got:      [2 4 scan 5 like 0]
like-10.11...
! like-10.11 expected: [12 123 scan 5 like 6]
! like-10.11 got:      [2 4 scan 5 like 0]
like-10.12...
! like-10.12 expected: [12 123 scan 5 like 6]
! like-10.12 got:      [2 4 scan 5 like 0]
like-10.13...
! like-10.13 expected: [12 123 scan 5 like 6]
! like-10.13 got:      [2 4 scan 5 like 0]
like-10.14...
! like-10.14 expected: [12 123 scan 3 like 0]
! like-10.14 got:      [2 4 scan 5 like 0]
like-10.15...
! like-10.15 expected: [12 123 scan 5 like 6]
! like-10.15 got:      [scan 5 like 0]
like-11.0... Ok
like-11.1...
! like-11.1 expected: [abc abcd ABC ABCD nosort t11 *]
! like-11.1 got:      [abc abcd ABC ABCD sort t11 *]
like-11.2...
! like-11.2 expected: [abc abcd nosort t11 *]
! like-11.2 got:      [abc abcd sort t11 *]
like-11.3...
! like-11.3 expected: [abc abcd ABC ABCD sort {} t11b]
! like-11.3 got:      [abc abcd ABC ABCD sort t11 *]
like-11.4...
! like-11.4 expected: [abc abcd nosort t11 *]
! like-11.4 got:      [abc abcd sort {} t11b]
like-11.5...
! like-11.5 expected: [abc abcd ABC ABCD sort {} t11bnc]
! like-11.5 got:      [abc abcd ABC ABCD sort t11 *]
like-11.6...
! like-11.6 expected: [abc abcd ABC ABCD sort {} t11bnc]
! like-11.6 got:      [abc abcd ABC ABCD sort t11 *]
like-11.7...
! like-11.7 expected: [abc abcd sort {} t11bb]
! like-11.7 got:      [abc abcd sort {} t11bnc]
like-11.8...
! like-11.8 expected: [abc abcd sort {} t11bb]
! like-11.8 got:      [abc abcd sort t11 *]
like-11.9...
! like-11.9 expected: [abc abcd ABC ABCD sort {} t11cnc]
! like-11.9 got:      [abc abcd ABC ABCD sort t11 *]
like-11.10...
! like-11.10 expected: [abc abcd sort {} t11cb]
! like-11.10 got:      [abc abcd sort t11 *]
like-12.1... Ok
like-12.2... Ok
like-12.3... Ok
like-12.4... Ok
like-12.5... Ok
like-12.6... Ok
like-12.11...
! like-12.11 expected: [/SEARCH/]
! like-12.11 got:      [0 0 0 {SCAN t12nc}]
like-12.12... Ok
like-12.13...
! like-12.13 expected: [/SEARCH/]
! like-12.13 got:      [0 0 0 {SCAN t12nc}]
like-12.14... Ok
like-12.15...
! like-12.15 expected: [/SEARCH/]
! like-12.15 got:      [0 0 0 {SCAN t12nc}]
like-12.16... Ok
like-13.1... Ok
like-13.2... Ok
like-13.3... Ok
like-13.4... Ok
like-14.1...
Error: can't read "::sqlite_options(configslower)": no such element in array
like-14.2...
Error: can't read "::sqlite_options(configslower)": no such element in array
like-15.100... Ok
like-15.101...
! like-15.101 expected: [/SEARCH/]
! like-15.101 got:      [0 0 0 {SCAN t15}]
like-15.102... Ok
like-15.103... Ok
like-15.110... Ok
like-15.111... Ok
like-15.112...
! like-15.112 expected: [/SEARCH/]
! like-15.112 got:      [0 0 0 {SCAN t15}]
like-15.120... Ok
like-15.121...
! like-15.121 expected: [/SEARCH/]
! like-15.121 got:      [0 0 0 {SCAN t15}]
like-16.0... Ok
like-16.1... Ok
like-16.2... Ok
like-17.0... Ok
like-17.1... Ok
like-17.1... Ok
like-18.0... Ok
like-18.1... Ok
like-18.2... Ok
Running "like"

Error in like.test: couldn't read file "like": no such file or directory
couldn't read file "like": no such file or directory
    while executing
"source like"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/like.test" line 1163)
    invoked from within
"source $test_file"

==========================================
Test: like
Time: 0s
Status: FAILED
