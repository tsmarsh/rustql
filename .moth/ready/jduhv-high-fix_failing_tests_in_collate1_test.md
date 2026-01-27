# Summary
- Test: sqlite3/test/collate1.test
- Repro: `testfixture test/collate1.test`
- Failing cases: collate1-1.1, collate1-1.2, collate1-1.3, collate1-1.4, collate1-1.5, collate1-1.6, collate1-1.7, collate1-2.2, collate1-2.3, collate1-2.4, collate1-2.5, collate1-2.6, collate1-2.12.1, collate1-2.12.2, collate1-2.12.3, collate1-2.12.4, collate1-2.13, collate1-2.14, collate1-2.15, collate1-2.16, collate1-3.0, collate1-3.1, collate1-3.2, collate1-3.3, collate1-3.4, collate1-4.3, collate1-4.4.1, collate1-5.1, collate1-5.3, collate1-6.2, collate1-6.3, collate1-6.4, collate1-6.7, collate1-7.1, collate1-8.0, collate1-8.2, collate1-10.0
- Primary errors: ! collate1-1.1 expected: [{} 0x119 0x2D] | ! collate1-1.1 got:      [{} 323831 3435] | ! collate1-1.2 expected: [{} 0x2D 0x119]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-collate1-3641454-1769533960772
DEBUG: tester.tcl sourced, db=db
Running collate1.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/collate1.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/collate1.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
collate1-1.0... Ok
collate1-1.1...
! collate1-1.1 expected: [{} 0x119 0x2D]
! collate1-1.1 got:      [{} 323831 3435]
collate1-1.2...
! collate1-1.2 expected: [{} 0x2D 0x119]
! collate1-1.2 got:      [3435 {} 323831]
collate1-1.3...
! collate1-1.3 expected: [0x119 0x2D {}]
! collate1-1.3 got:      [3435 {} 323831]
collate1-1.4...
! collate1-1.4 expected: [{} 0x2D 0x119]
! collate1-1.4 got:      [3435 {} 323831]
collate1-1.5...
! collate1-1.5 expected: [{} 0x2D 0x119]
! collate1-1.5 got:      [{} 323831 3435]
collate1-1.6...
! collate1-1.6 expected: [{} 0x2D 0x119]
! collate1-1.6 got:      [{} 323831 3435]
collate1-1.7...
! collate1-1.7 expected: [0x119 0x2D {}]
! collate1-1.7 got:      [3435 323831 {}]
collate1-1.99... Ok
collate1-2.0... Ok
collate1-2.2...
! collate1-2.2 expected: [{} {} 5 0xA 5 0x11 7 0xA 11 0x11 11 0x101]
! collate1-2.2 got:      [5 0x11 5 0xA {} {} 7 0xA 11 0x11 11 0x101]
collate1-2.3...
! collate1-2.3 expected: [{} {} 11 0x11 11 0x101 5 0xA 5 0x11 7 0xA]
! collate1-2.3 got:      [5 0x11 5 0xA {} {} 7 0xA 11 0x11 11 0x101]
collate1-2.4...
! collate1-2.4 expected: [7 0xA 5 0xA 5 0x11 11 0x11 11 0x101 {} {}]
! collate1-2.4 got:      [5 0x11 5 0xA {} {} 7 0xA 11 0x11 11 0x101]
collate1-2.5...
! collate1-2.5 expected: [7 0xA 5 0x11 5 0xA 11 0x101 11 0x11 {} {}]
! collate1-2.5 got:      [5 0x11 5 0xA {} {} 7 0xA 11 0x11 11 0x101]
collate1-2.6...
! collate1-2.6 expected: [{} {} 11 0x11 11 0x101 5 0xA 5 0x11 7 0xA]
! collate1-2.6 got:      [5 0x11 5 0xA {} {} 7 0xA 11 0x11 11 0x101]
collate1-2.12.1...
! collate1-2.12.1 expected: [{} {} 5 0xA 5 0x11 7 0xA 11 0x11 11 0x101]
! collate1-2.12.1 got:      [{} {} 11 0x11 11 0x101 5 0x11 5 0xA 7 0xA]
collate1-2.12.2...
! collate1-2.12.2 expected: [{} {} 5 0xA 5 0x11 7 0xA 11 0x11 11 0x101]
! collate1-2.12.2 got:      [5 0x11 5 0xA {} {} 7 0xA 11 0x11 11 0x101]
collate1-2.12.3...
! collate1-2.12.3 expected: [{} {} 5 0xA 5 0x11 7 0xA 11 0x11 11 0x101]
! collate1-2.12.3 got:      [{} {} 11 0x101 5 0x11 11 0x11 5 0xA 7 0xA]
collate1-2.12.4...
! collate1-2.12.4 expected: [{} {} 5 0xA 5 0x11 7 0xA 11 0x11 11 0x101]
! collate1-2.12.4 got:      [{} {} 11 0x101 11 0x11 5 0x11 5 0xA 7 0xA]
collate1-2.13...
! collate1-2.13 expected: [{} {} 11 0x11 11 0x101 5 0xA 5 0x11 7 0xA]
! collate1-2.13 got:      [{} {} 11 0x101 11 0x11 5 0x11 5 0xA 7 0xA]
collate1-2.14...
! collate1-2.14 expected: [7 0xA 5 0xA 5 0x11 11 0x11 11 0x101 {} {}]
! collate1-2.14 got:      [5 0x11 5 0xA {} {} 7 0xA 11 0x11 11 0x101]
collate1-2.15...
! collate1-2.15 expected: [7 0xA 5 0x11 5 0xA 11 0x101 11 0x11 {} {}]
! collate1-2.15 got:      [7 0xA 5 0xA 5 0x11 11 0x11 11 0x101 {} {}]
collate1-2.16...
! collate1-2.16 expected: [{} {} 11 0x11 11 0x101 5 0xA 5 0x11 7 0xA]
! collate1-2.16 got:      [5 0x11 5 0xA {} {} 7 0xA 11 0x11 11 0x101]
collate1-2.99... Ok
collate1-3.0...
! collate1-3.0 expected: [{} {} 1 1 0x5 5 0x45 69]
! collate1-3.0 got:      [{} {} 0x45 69 0x5 5 1 1]
collate1-3.1...
! collate1-3.1 expected: [{} {} 1 1 0x5 5 0x45 69]
! collate1-3.1 got:      [{} {} 0x45 69 0x5 5 1 1]
collate1-3.2...
! collate1-3.2 expected: [{} {} 1 1 0x5 5 0x45 69]
! collate1-3.2 got:      [{} {} 0x45 69 0x5 5 1 1]
collate1-3.3...
! collate1-3.3 expected: [{} {} 1 1 0x5 5 0x45 69]
! collate1-3.3 got:      [{} {} 0x45 69 0x5 5 1 1]
collate1-3.4...
! collate1-3.4 expected: [{} {} 1 1 0x5 5 0x45 69]
! collate1-3.4 got:      [{} {} 0x45 69 0x5 5 1 1]
collate1-3.5... Ok
collate1-3.5.1... Ok
collate1-3.6... Ok
collate1-4.0... Ok
collate1-4.1... Ok
collate1-4.2... Ok
collate1-4.3...
! collate1-4.3 expected: [{} 1 12 101]
! collate1-4.3 got:      [{} 1.0 12.0 101.0]
collate1-4.4... Ok
collate1-4.4.1...
! collate1-4.4.1 expected: [{} 1 12 101]
! collate1-4.4.1 got:      [{} 1 101 12]
collate1-4.5... Ok
collate1-5.1...
! collate1-5.1 expected: [1 2]
! collate1-5.1 got:      [1]
collate1-5.2... Ok
collate1-5.3...
! collate1-5.3 expected: [1 2]
! collate1-5.3 got:      [1]
collate1-6.1...
Error: no such column: """
collate1-6.2...
! collate1-6.2 expected: [1 {no such collation sequence: """}]
! collate1-6.2 got:      [0 {}]
collate1-6.3...
! collate1-6.3 expected: [1 {no such collation sequence: """}]
! collate1-6.3 got:      [0 {}]
collate1-6.4...
! collate1-6.4 expected: [1 {no such collation sequence: """}]
! collate1-6.4 got:      [0 0]
collate1-6.5...
Error: near "'"""'": syntax error
collate1-6.6...
Error: no such table: p1
collate1-6.7...
! collate1-6.7 expected: [1 {FOREIGN KEY constraint failed}]
! collate1-6.7 got:      [1 {no such table: p1}]
collate1-6.8...
Error: no such table: p1
collate1-7.0... Ok
collate1-7.1...
! collate1-7.1 expected: [DEF abc]
! collate1-7.1 got:      [abc DEF]
collate1-7.2... Ok
collate1-8.0...
! collate1-8.0 expected: [0]
! collate1-8.0 got:      [1]
collate1-8.1... Ok
collate1-8.2...
! collate1-8.2 expected: [{ } 1]
! collate1-8.2 got:      []
collate1-9.0... Ok
collate1-10.0...
! collate1-10.0 expected: [1 {no such collation sequence: x}]
! collate1-10.0 got:      [1 {no such column: z}]
Running "collate1"

Error in collate1.test: couldn't read file "collate1": no such file or directory
couldn't read file "collate1": no such file or directory
    while executing
"source collate1"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/collate1.test" line 452)
    invoked from within
"source $test_file"

==========================================
Test: collate1
Time: 0s
Status: FAILED
