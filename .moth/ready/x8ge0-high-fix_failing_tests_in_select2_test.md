# Summary
- Test: sqlite3/test/select2.test
- Repro: `testfixture test/select2.test`
- Failing cases: unknown (see log)
- Primary errors: Error in select2.test: couldn't read file "select2": no such file or directory | couldn't read file "select2": no such file or directory

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-select2-3641330-1769533960708
DEBUG: tester.tcl sourced, db=db
Running select2.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/select2.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/select2.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
select2-1.1... Ok
select2-1.2... Ok
select2-2.0.2... Ok
time without cache: 844128 microseconds per iteration
select2-2.1... Ok
select2-2.2... Ok
select2-3.1... Ok
select2-3.2a... Ok
select2-3.2b... Ok
select2-3.2c... Ok
select2-3.2d... Ok
select2-3.2e... Ok
select2-3.3... Ok
select2-4.1... Ok
select2-4.2... Ok
select2-4.3... Ok
select2-4.4... Ok
select2-4.5... Ok
select2-4.6... Ok
select2-4.7... Ok
Running "select2"

Error in select2.test: couldn't read file "select2": no such file or directory
couldn't read file "select2": no such file or directory
    while executing
"source select2"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/select2.test" line 185)
    invoked from within
"source $test_file"

==========================================
Test: select2
Time: 3s
Status: FAILED
