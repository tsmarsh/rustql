# Summary
- Test: sqlite3/test/trans2.test
- Repro: `testfixture test/trans2.test`
- Failing cases: unknown (see log)
- Primary errors: Error in trans2.test: invalid command name "md5"

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-trans2-3641450-1769533960769
DEBUG: tester.tcl sourced, db=db
Running trans2.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/trans2.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/trans2.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test

Error in trans2.test: invalid command name "md5"
invalid command name "md5"
    while executing
"md5 $x"
    (procedure "hash1" line 7)
    invoked from within
"hash1"
    (file "/tank/repos/rustql-architecture/sqlite3/test/trans2.test" line 94)
    invoked from within
"source $test_file"

==========================================
Test: trans2
Time: 0s
Status: FAILED
