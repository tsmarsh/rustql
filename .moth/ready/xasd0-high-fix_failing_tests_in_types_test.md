# Summary
- Test: sqlite3/test/types.test
- Repro: `testfixture test/types.test`
- Failing cases: unknown (see log)
- Primary errors: Error in types.test: invalid command name "sqlite3_rekey"

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-types-3641382-1769533960729
DEBUG: tester.tcl sourced, db=db
Running types.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/types.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/types.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test

Error in types.test: invalid command name "sqlite3_rekey"
invalid command name "sqlite3_rekey"
    while executing
"sqlite3_rekey $DB {}"
    (file "/tank/repos/rustql-architecture/sqlite3/test/types.test" line 43)
    invoked from within
"source $test_file"

==========================================
Test: types
Time: 0s
Status: FAILED
