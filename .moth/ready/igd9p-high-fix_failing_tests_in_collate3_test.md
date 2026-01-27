# Summary
- Test: sqlite3/test/collate3.test
- Repro: `testfixture test/collate3.test`
- Failing cases: collate3-1.1, collate3-1.1.2, collate3-1.2, collate3-1.3, collate3-1.4, collate3-1.5, collate3-1.6.1, collate3-1.6.2, collate3-1.6.3, collate3-1.6.4, collate3-1.7.1, collate3-1.7.2, collate3-1.7.4, collate3-1.7.3, collate3-2.1, collate3-2.2, collate3-2.3, collate3-2.4, collate3-2.5, collate3-2.7.1, collate3-2.7.2, collate3-2.8, collate3-2.9, collate3-2.10, collate3-2.11, collate3-2.13, collate3-2.14, collate3-2.15, collate3-2.16, collate3-2.17, collate3-3.1, collate3-3.2, collate3-3.3, collate3-3.4, collate3-3.5, collate3-3.8, collate3-3.10, collate3-3.11, collate3-3.12, collate3-3.13, collate3-3.14, collate3-4.7, collate3-4.8.1, collate3-4.9, collate3-4.10, collate3-4.11, collate3-5.0, collate3-5.3, collate3-5.5, collate3-5.7, collate3-5.8
- Primary errors: ! collate3-1.1 expected: [1 {no such collation sequence: garbage}] | ! collate3-1.1 got:      [0 {}] | ! collate3-1.1.2 expected: [1 {no such collation sequence: garbage}]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-collate3-3641458-1769533960786
DEBUG: tester.tcl sourced, db=db
Running collate3.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/collate3.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/collate3.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
collate3-1.0... Ok
collate3-1.1...
! collate3-1.1 expected: [1 {no such collation sequence: garbage}]
! collate3-1.1 got:      [0 {}]
collate3-1.1.2...
! collate3-1.1.2 expected: [1 {no such collation sequence: garbage}]
! collate3-1.1.2 got:      [0 {}]
collate3-1.2...
! collate3-1.2 expected: [1 {no such collation sequence: garbage}]
! collate3-1.2 got:      [0 {}]
collate3-1.3...
! collate3-1.3 expected: [1 {no such collation sequence: garbage}]
! collate3-1.3 got:      [0 {}]
collate3-1.4...
! collate3-1.4 expected: [abc1 Abc2 aBc3]
! collate3-1.4 got:      [Abc2 aBc3 abc1]
collate3-1.5...
! collate3-1.5 expected: [1 {no such collation sequence: caseless}]
! collate3-1.5 got:      [0 {Abc2 aBc3 abc1}]
collate3-1.6.1...
! collate3-1.6.1 expected: [abc1 Abc2 aBc3]
! collate3-1.6.1 got:      [Abc2 aBc3 abc1]
collate3-1.6.2...
! collate3-1.6.2 expected: [1 {no such collation sequence: caseless}]
! collate3-1.6.2 got:      [0 {Abc2 aBc3 abc1}]
collate3-1.6.3...
! collate3-1.6.3 expected: [1 {no such collation sequence: caseless}]
! collate3-1.6.3 got:      [0 ok]
collate3-1.6.4...
! collate3-1.6.4 expected: [1 {no such collation sequence: caseless}]
! collate3-1.6.4 got:      [0 {}]
collate3-1.7.1...
! collate3-1.7.1 expected: [abc1 Abc2 aBc3]
! collate3-1.7.1 got:      [Abc2 aBc3 abc1]
collate3-1.7.2...
! collate3-1.7.2 expected: [1 {no such collation sequence: caseless}]
! collate3-1.7.2 got:      [0 {Abc2 aBc3 abc1}]
collate3-1.7.4...
! collate3-1.7.4 expected: [1 {no such collation sequence: caseless}]
! collate3-1.7.4 got:      [0 {}]
collate3-1.7.3...
! collate3-1.7.3 expected: [1 {no such collation sequence: caseless}]
! collate3-1.7.3 got:      [0 ok]
collate3-1.7.4...
! collate3-1.7.4 expected: [1 {no such collation sequence: caseless}]
! collate3-1.7.4 got:      [0 {}]
collate3-1.7.5... Ok
collate3-1.7.6... Ok
collate3-1.8... Ok
collate3-2.0...
Error: table "collate3t1" already exists
collate3-2.1...
! collate3-2.1 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.1 got:      [0 {}]
collate3-2.2...
! collate3-2.2 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.2 got:      [0 {}]
collate3-2.3...
! collate3-2.3 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.3 got:      [1 {database disk image is malformed}]
collate3-2.4...
! collate3-2.4 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.4 got:      [1 {table "collate3t2" already exists}]
collate3-2.5...
! collate3-2.5 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.5 got:      [0 {}]
collate3-2.6... Ok
collate3-2.7.1...
! collate3-2.7.1 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.7.1 got:      [0 {}]
collate3-2.7.2...
! collate3-2.7.2 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.7.2 got:      [0 {}]
collate3-2.8...
! collate3-2.8 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.8 got:      [0 {}]
collate3-2.9...
! collate3-2.9 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.9 got:      [0 {}]
collate3-2.10...
! collate3-2.10 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.10 got:      [0 {}]
collate3-2.11...
! collate3-2.11 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.11 got:      [0 {}]
collate3-2.12... Ok
collate3-2.13...
! collate3-2.13 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.13 got:      [0 {10 20}]
collate3-2.14...
! collate3-2.14 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.14 got:      [0 {}]
collate3-2.15...
! collate3-2.15 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.15 got:      [0 10]
collate3-2.16...
! collate3-2.16 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.16 got:      [0 {10 20}]
collate3-2.17...
! collate3-2.17 expected: [1 {no such collation sequence: string_compare}]
! collate3-2.17 got:      [0 {}]
collate3-3.0...
Error: index collate3t1_i1 already exists
collate3-3.1...
! collate3-3.1 expected: [1 {no such collation sequence: string_compare}]
! collate3-3.1 got:      [1 {table collate3t1 has 1 columns but 2 values were supplied}]
collate3-3.2...
! collate3-3.2 expected: [1 {no such collation sequence: string_compare}]
! collate3-3.2 got:      [0 {}]
collate3-3.3...
! collate3-3.3 expected: [0 {}]
! collate3-3.3 got:      [1 {no such column: c2}]
collate3-3.4...
! collate3-3.4 expected: [1 {no such collation sequence: string_compare}]
! collate3-3.4 got:      [0 {}]
collate3-3.5...
! collate3-3.5 expected: [0 {xxx xxx}]
! collate3-3.5 got:      [0 {}]
collate3-3.6... Ok
collate3-3.8...
! collate3-3.8 expected: [1 {no such collation sequence: string_compare}]
! collate3-3.8 got:      [0 ok]
collate3-3.9... Ok
collate3-3.10...
! collate3-3.10 expected: [1 {no such collation sequence: string_compare}]
! collate3-3.10 got:      [0 {}]
collate3-3.11...
! collate3-3.11 expected: [1 {no such collation sequence: string_compare}]
! collate3-3.11 got:      [0 {}]
collate3-3.12...
! collate3-3.12 expected: [1 {no such collation sequence: string_compare}]
! collate3-3.12 got:      [1 {database disk image is malformed}]
collate3-3.13...
! collate3-3.13 expected: [1 {no such collation sequence: string_compare}]
! collate3-3.13 got:      [1 {table "collate3t2" already exists}]
collate3-3.14...
! collate3-3.14 expected: [1 {no such collation sequence: string_compare}]
! collate3-3.14 got:      [0 {}]
collate3-3.15... Ok
collate3-4.6... Ok
collate3-4.7...
! collate3-4.7 expected: [1 {no such collation sequence: user_defined}]
! collate3-4.7 got:      [1 {no such column: a}]
collate3-4.8.1...
! collate3-4.8.1 expected: [0 {hello {}}]
! collate3-4.8.1 got:      [1 {no such column: a}]
collate3-4.8.2... Ok
collate3-4.8.3... Ok
collate3-4.9...
! collate3-4.9 expected: [2 {} 12 {} 101 {}]
! collate3-4.9 got:      [2 {} 101 {} 12 {}]
collate3-4.10...
! collate3-4.10 expected: [1 {no such collation sequence: user_defined}]
! collate3-4.10 got:      [1 {no such table: collate3v1}]
collate3-4.11...
! collate3-4.11 expected: [0 {2 {} 12 {} 101 {}}]
! collate3-4.11 got:      [1 {no such table: collate3v1}]
collate3-4.12... Ok
collate3-5.0...
! collate3-5.0 expected: [1 {no such collation sequence: unk}]
! collate3-5.0 got:      [0 10]
collate3-5.1... Ok
collate3-5.2... Ok
collate3-5.3...
! collate3-5.3 expected: [1]
! collate3-5.3 got:      [0]
collate3-5.4... Ok
collate3-5.5...
! collate3-5.5 expected: [1]
! collate3-5.5 got:      [0]
collate3-5.6... Ok
collate3-5.7...
! collate3-5.7 expected: [1 {no such collation sequence: unk}]
! collate3-5.7 got:      [1 {no such column: a}]
collate3-5.8...
! collate3-5.8 expected: [0 {}]
! collate3-5.8 got:      [1 {no such column: a}]
collate3-5.9... Ok
Running "collate3"

Error in collate3.test: couldn't read file "collate3": no such file or directory
couldn't read file "collate3": no such file or directory
    while executing
"source collate3"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/collate3.test" line 531)
    invoked from within
"source $test_file"

==========================================
Test: collate3
Time: 0s
Status: FAILED
