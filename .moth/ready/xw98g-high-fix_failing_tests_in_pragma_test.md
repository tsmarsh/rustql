# Summary
- Test: sqlite3/test/pragma.test
- Repro: `testfixture test/pragma.test`
- Failing cases: pragma-1.9.1, pragma-1.9.2, pragma-1.10, pragma-1.11.1, pragma-1.11.2, pragma-1.12, pragma-1.14.1, pragma-1.14.3, pragma-1.14.4, pragma-1.15.2, pragma-1.15.4, pragma-3.3, pragma-3.4, pragma-3.5, pragma-3.5.2, pragma-3.6, pragma-3.6b, pragma-3.7, pragma-3.9a, pragma-3.9b, pragma-3.10, pragma-3.11, pragma-3.12, pragma-3.13, pragma-3.14, pragma-3.15, pragma-3.16, pragma-3.17, pragma-3.18, pragma-3.21, pragma-3.22, pragma-3.23, pragma-3.24, pragma-3.25, pragma-4.6, pragma-5.1, pragma-5.2, pragma-6.2.2, pragma-6.3.1, pragma-6.6.2, pragma-6.8, pragma-7.2, pragma-7.3, pragma-8.1.9, pragma-8.1.10, pragma-8.1.16, pragma-8.1.17
- Primary errors: ! pragma-1.9.1 expected: [123 123 2] | ! pragma-1.9.1 got:      [-2000 -2000 2] | ! pragma-1.9.2 expected: [123 123 2]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-pragma-3641438-1769533960761
DEBUG: tester.tcl sourced, db=db
Running pragma.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/pragma.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/pragma.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
pragma-1.1... Ok
pragma-1.2... Ok
pragma-1.3... Ok
pragma-1.4... Ok
pragma-1.5... Ok
pragma-1.6... Ok
pragma-1.7... Ok
pragma-1.8... Ok
pragma-1.9.1...
! pragma-1.9.1 expected: [123 123 2]
! pragma-1.9.1 got:      [-2000 -2000 2]
pragma-1.9.2...
! pragma-1.9.2 expected: [123 123 2]
! pragma-1.9.2 got:      [-2000 -2000 2]
pragma-1.10...
! pragma-1.10 expected: [123 123 1]
! pragma-1.10 got:      [-2000 -2000 1]
pragma-1.11.1...
! pragma-1.11.1 expected: [123 123 3]
! pragma-1.11.1 got:      [-2000 -2000 3]
pragma-1.11.2...
! pragma-1.11.2 expected: [123 123 2]
! pragma-1.11.2 got:      [-2000 -2000 2]
pragma-1.12...
! pragma-1.12 expected: [123 123 2]
! pragma-1.12 got:      [-2000 -2000 2]
pragma-1.13... Ok
pragma-1.14... Ok
pragma-1.14.1...
! pragma-1.14.1 expected: [4]
! pragma-1.14.1 got:      [2]
pragma-1.14.2... Ok
pragma-1.14.3...
! pragma-1.14.3 expected: [0]
! pragma-1.14.3 got:      [3]
pragma-1.14.4...
! pragma-1.14.4 expected: [2]
! pragma-1.14.4 got:      [3]
pragma-1.15.1... Ok
pragma-1.15.2...
! pragma-1.15.2 expected: [-2000]
! pragma-1.15.2 got:      [0]
pragma-1.15.3... Ok
pragma-1.15.4... Ok
pragma-1.15.3... Ok
pragma-1.15.4... Ok
pragma-1.15.4...
! pragma-1.15.4 expected: [256]
! pragma-1.15.4 got:      [-2000]
pragma-1.17...
Error: unknown pragma: parser_trace
pragma-1.18...
Error: unknown pragma: bogus
pragma-2.1... Ok
pragma-2.2... Ok
pragma-2.3... Ok
pragma-2.4... Ok
pragma-3.1... Ok
pragma-3.2...
Error: can't read "rootpage": no such variable
pragma-3.3...
! pragma-3.3 expected: [{wrong # of entries in index i2}]
! pragma-3.3 got:      [ok]
pragma-3.4...
! pragma-3.4 expected: [{wrong # of entries in index i2} {row 1 missing from index i2} {row 2 missing from index i2} {wrong # of entries in index i2} {row 1 missing from index i2} {row 2 missing from index i2}]
! pragma-3.4 got:      [ok]
pragma-3.5...
! pragma-3.5 expected: [{wrong # of entries in index i2} {row 1 missing from index i2} {row 2 missing from index i2} {wrong # of entries in index i2}]
! pragma-3.5 got:      [ok]
pragma-3.5.2...
! pragma-3.5.2 expected: [1 {no such table: 4}]
! pragma-3.5.2 got:      [0 ok]
pragma-3.6...
! pragma-3.6 expected: [1 {no such table: xyz}]
! pragma-3.6 got:      [0 ok]
pragma-3.6b...
! pragma-3.6b expected: [0 {{wrong # of entries in index i2} {row 1 missing from index i2} {row 2 missing from index i2}}]
! pragma-3.6b got:      [0 ok]
pragma-3.6c... Ok
pragma-3.7...
! pragma-3.7 expected: [{wrong # of entries in index i2} {row 1 missing from index i2} {row 2 missing from index i2} {wrong # of entries in index i2} {row 1 missing from index i2} {row 2 missing from index i2}]
! pragma-3.7 got:      [ok]
pragma-3.8... Ok
pragma-3.8.1... Ok
pragma-3.8.2... Ok
pragma-3.9a...
! pragma-3.9a expected: [{*** in database t2 ***
Page 4: never used
Page 5: never used
Page 6: never used} {wrong # of entries in index i2} {row 1 missing from index i2} {row 2 missing from index i2}]
! pragma-3.9a got:      [ok]
pragma-3.9b...
! pragma-3.9b expected: [{wrong # of entries in index i2} {row 1 missing from index i2} {row 2 missing from index i2}]
! pragma-3.9b got:      [ok]
pragma-3.9c... Ok
pragma-3.10...
! pragma-3.10 expected: [{*** in database t2 ***
Page 4: never used}]
! pragma-3.10 got:      [ok]
pragma-3.11...
! pragma-3.11 expected: [{*** in database t2 ***
Page 4: never used
Page 5: never used
Page 6: never used} {wrong # of entries in index i2} {row 1 missing from index i2}]
! pragma-3.11 got:      [ok]
pragma-3.12...
! pragma-3.12 expected: [{*** in database t2 ***
Page 4: never used
Page 5: never used
Page 6: never used} {wrong # of entries in index i2}]
! pragma-3.12 got:      [ok]
pragma-3.13...
! pragma-3.13 expected: [{*** in database t2 ***
Page 4: never used
Page 5: never used
Page 6: never used}]
! pragma-3.13 got:      [ok]
pragma-3.14...
! pragma-3.14 expected: [{*** in database t2 ***
Page 4: never used
Page 5: never used}]
! pragma-3.14 got:      [ok]
pragma-3.15...
! pragma-3.15 expected: [{*** in database t2 ***
Page 4: never used
Page 5: never used
Page 6: never used} {wrong # of entries in index i2} {row 1 missing from index i2} {row 2 missing from index i2} {*** in database t3 ***
Page 4: never used
Page 5: never used
Page 6: never used} {wrong # of entries in index i2} {row 1 missing from index i2} {row 2 missing from index i2}]
! pragma-3.15 got:      [ok]
pragma-3.16...
! pragma-3.16 expected: [{*** in database t2 ***
Page 4: never used
Page 5: never used
Page 6: never used} {wrong # of entries in index i2} {row 1 missing from index i2} {row 2 missing from index i2} {*** in database t3 ***
Page 4: never used
Page 5: never used
Page 6: never used} {wrong # of entries in index i2}]
! pragma-3.16 got:      [ok]
pragma-3.17...
! pragma-3.17 expected: [{*** in database t2 ***
Page 4: never used
Page 5: never used
Page 6: never used} {wrong # of entries in index i2} {row 1 missing from index i2} {row 2 missing from index i2} {*** in database t3 ***
Page 4: never used
Page 5: never used}]
! pragma-3.17 got:      [ok]
pragma-3.18...
! pragma-3.18 expected: [{*** in database t2 ***
Page 4: never used
Page 5: never used
Page 6: never used} {wrong # of entries in index i2}]
! pragma-3.18 got:      [ok]
pragma-3.19... Ok
pragma-3.20...
Error: table sqlite_master may not be modified
pragma-3.21...
! pragma-3.21 expected: [{non-unique entry in index t1a} {NULL value in t1x.a} {non-unique entry in index t1a}]
! pragma-3.21 got:      [ok]
pragma-3.22...
! pragma-3.22 expected: [{non-unique entry in index t1a} {NULL value in t1x.a}]
! pragma-3.22 got:      [ok]
pragma-3.23...
! pragma-3.23 expected: [{non-unique entry in index t1a}]
! pragma-3.23 got:      [ok]
pragma-3.24...
! pragma-3.24 expected: [1 0.25 ok]
! pragma-3.24 got:      [1 ok]
pragma-3.25...
! pragma-3.25 expected: [1 0.25 {} ok]
! pragma-3.25 got:      [1 ok]
pragma-3.30...
Error: no such table: c
pragma-3.40...
Error: no such column: name
pragma-4.1... Ok
pragma-4.2... Ok
pragma-4.3... Ok
pragma-4.4... Ok
pragma-4.5... Ok
pragma-4.6...
! pragma-4.6 expected: [456 456]
! pragma-4.6 got:      [-2000 -2000]
pragma-5.0... Ok
pragma-5.1...
! pragma-5.1 expected: [1 {Safety level may not be changed inside a transaction}]
! pragma-5.1 got:      [0 {}]
pragma-5.2...
! pragma-5.2 expected: [2]
! pragma-5.2 got:      [0]
pragma-6.1...
Error: no such table: sqlite_temp_master
pragma-6.2... Ok
pragma-6.2.1...
Error: missing pragma argument
pragma-6.2.2...
! pragma-6.2.2 expected: [0 a TEXT 0 CURRENT_TIMESTAMP 0 1 b {} 0 5+3 2 2 c TEXT 0 <<NULL>> 3 3 d INTEGER 0 NULL 0 4 e TEXT 0 '' 1]
! pragma-6.2.2 got:      [0 a TEXT 0 CURRENT_TIME 0 1 b {} 0 '(expression' 0 2 c TEXT 0 <<NULL>> 0 3 d INTEGER 0 'NULL' 0 4 e TEXT 0 '' 0]
pragma-6.2.3... Ok
pragma-6.3.1...
! pragma-6.3.1 expected: [0 0 t2 a b {NO ACTION} {NO ACTION} NONE]
! pragma-6.3.1 got:      []
pragma-6.3.2...
Error: missing pragma argument
pragma-6.3.3...
Error: no such table
pragma-6.3.4... Ok
pragma-6.4...
Error: near "(": syntax error
pragma-6.5.1...
Error: near "(": syntax error
pragma-6.5.1b...
Error: unknown pragma: index_xinfo
pragma-6.5.1c... Ok
pragma-6.5.2...
Error: no such index
pragma-6.6.1...
Error: table "trial" already exists
pragma-6.6.2...
! pragma-6.6.2 expected: [0 col_temp {} 0 {} 0]
! pragma-6.6.2 got:      [0 col_main {} 0 {} 0]
pragma-6.6.3...
Error: no such table
pragma-6.6.4... Ok
pragma-6.7...
Error: near "(": syntax error
pragma-6.8...
! pragma-6.8 expected: [0 a {} 0 {} 1 1 b {} 0 {} 2 2 c {} 0 {} 4]
! pragma-6.8 got:      [0 a {} 0 {} 0 1 b {} 0 {} 0 2 c {} 0 {} 0]
pragma-7.1.1...
Error: near "(": syntax error
pragma-7.1.2...
Error: no such table
pragma-7.2...
! pragma-7.2 expected: [1 {unsupported encoding: bogus}]
! pragma-7.2 got:      [0 {}]
pragma-7.3...
! pragma-7.3 expected: [main unlocked temp closed]
! pragma-7.3 got:      [main unlocked temp unlocked]
pragma-8.1.1...
Error: unknown pragma: schema_version
pragma-8.1.2...
Error: unknown pragma: schema_version
pragma-8.1.3...
Error: unknown pragma: schema_version
pragma-8.1.4...
Error: unknown pragma: schema_version
pragma-8.1.5... Ok
pragma-8.1.6...
Error: unknown pragma: schema_version
pragma-8.1.7... Ok
pragma-8.1.8...
Error: unknown pragma: schema_version
pragma-8.1.9...
! pragma-8.1.9 expected: [SQLITE_ERROR]
! pragma-8.1.9 got:      [0]
pragma-8.1.10...
! pragma-8.1.10 expected: [SQLITE_SCHEMA]
! pragma-8.1.10 got:      [0]
pragma-8.1.11...
Error: database is locked
pragma-8.1.12...
Error: unknown pragma: schema_version
pragma-8.1.13...
Error: unknown pragma: schema_version
pragma-8.1.14...
Error: no such database: db2
pragma-8.1.15...
Error: unknown pragma: schema_version
pragma-8.1.16...
! pragma-8.1.16 expected: [SQLITE_ERROR]
! pragma-8.1.16 got:      [0]
pragma-8.1.17...
! pragma-8.1.17 expected: [SQLITE_SCHEMA]
! pragma-8.1.17 got:      [0]
pragma-8.1.18... Ok
pragma-8.2.1...
Error: unknown pragma: user_version
pragma-8.2.2...
Error: unknown pragma: user_version
pragma-8.2.3.1...
Error: unknown pragma: user_version
pragma-8.2.3.2...
Error: unknown pragma: user_version
pragma-8.2.4.1...
Error: unknown pragma: schema_version
pragma-8.2.4.2...
Error: unknown pragma: user_version
pragma-8.2.4.3...
Error: unknown pragma: schema_version
pragma-8.2.5...
Error: unknown pragma: user_version
pragma-8.2.6...
Error: unknown pragma: user_version
pragma-8.2.7...
Error: unknown pragma: user_version
pragma-8.2.8...
Error: unknown pragma: user_version
pragma-8.2.9...
Error: unknown pragma: user_version
pragma-8.2.10...
Error: unknown pragma: user_version
pragma-8.2.11...
Error: unknown pragma: user_version
pragma-8.2.12...
Error: unknown pragma: user_version
pragma-8.2.13...
Error: unknown pragma: user_version
pragma-8.2.14...
Error: unknown pragma: user_version
pragma-8.2.15...
Error: unknown pragma: user_version
pragma-8.3.1...
Error: unknown pragma: application_id
pragma-8.3.2...
Error: unknown pragma: Application_ID
pragma-9.1...
Error: unknown pragma: temp_store

Error in pragma.test: can't read "TEMP_STORE": no such variable
can't read "TEMP_STORE": no such variable
    while executing
"ifcapable pager_pragmas {
do_test pragma-9.1 {
  db close
  sqlite3 db test.db
  execsql {
    PRAGMA temp_store;
  }
} {0}
if {$TEMP_STORE<=1} {
  do..."
    (file "/tank/repos/rustql-architecture/sqlite3/test/pragma.test" line 1238)
    invoked from within
"source $test_file"

==========================================
Test: pragma
Time: 0s
Status: FAILED
