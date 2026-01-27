# Summary
- Test: sqlite3/test/index2.test
- Repro: `testfixture test/index2.test`
- Failing cases: unknown (see log)
- Primary errors: Error in index2.test: couldn't read file "index2": no such file or directory | couldn't read file "index2": no such file or directory

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-index2-3641374-1769533960725
DEBUG: tester.tcl sourced, db=db
Running index2.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/index2.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/index2.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
index2-1.1... Ok
index2-1.2... Ok
index2-1.3... Ok
index2-1.4... Ok
index2-1.5... Ok
index2-2.1... Ok
index2-2.2... Ok
Running "index2"

Error in index2.test: couldn't read file "index2": no such file or directory
couldn't read file "index2": no such file or directory
    while executing
"source index2"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/index2.test" line 73)
    invoked from within
"source $test_file"

==========================================
Test: index2
Time: 1s
Status: FAILED
