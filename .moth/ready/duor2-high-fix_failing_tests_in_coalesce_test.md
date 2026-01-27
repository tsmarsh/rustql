# Summary
- Test: sqlite3/test/coalesce.test
- Repro: `testfixture test/coalesce.test`
- Failing cases: unknown (see log)
- Primary errors: Error in coalesce.test: couldn't read file "coalesce": no such file or directory | couldn't read file "coalesce": no such file or directory

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-coalesce-3641397-1769533960734
DEBUG: tester.tcl sourced, db=db
Running coalesce.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/coalesce.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/coalesce.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
coalesce-1.0... Ok
coalesce-1.1... Ok
coalesce-1.2... Ok
coalesce-1.3... Ok
coalesce-1.4... Ok
coalesce-1.5... Ok
coalesce-1.6... Ok
coalesce-1.7... Ok
coalesce-1.8... Ok
Running "coalesce"

Error in coalesce.test: couldn't read file "coalesce": no such file or directory
couldn't read file "coalesce": no such file or directory
    while executing
"source coalesce"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/coalesce.test" line 84)
    invoked from within
"source $test_file"

==========================================
Test: coalesce
Time: 0s
Status: FAILED
