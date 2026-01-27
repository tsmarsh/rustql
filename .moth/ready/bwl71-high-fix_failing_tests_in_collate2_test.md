# Summary
- Test: sqlite3/test/collate2.test
- Repro: `testfixture test/collate2.test`
- Failing cases: collate2-1.2, collate2-1.2.1, collate2-1.2.2, collate2-1.2.3, collate2-1.2.4, collate2-1.2.5, collate2-1.2.6, collate2-1.2.7, collate2-1.3, collate2-1.3.1, collate2-1.3.2, collate2-1.3.3, collate2-1.5, collate2-1.5.1, collate2-1.6, collate2-1.8, collate2-1.11, collate2-1.12, collate2-1.14, collate2-1.15, collate2-1.17, collate2-1.17.1, collate2-1.18, collate2-1.20, collate2-1.23, collate2-1.26, collate2-2.2, collate2-2.3, collate2-2.5, collate2-2.6, collate2-2.8, collate2-2.11, collate2-2.12, collate2-2.14, collate2-2.15, collate2-2.17, collate2-2.18, collate2-2.20, collate2-2.22, collate2-2.23, collate2-2.24, collate2-2.25, collate2-2.26, collate2-2.27, collate2-3.2, collate2-3.3, collate2-3.5, collate2-3.6, collate2-3.8, collate2-3.11, collate2-3.12, collate2-3.14, collate2-3.15, collate2-3.16, collate2-3.17, collate2-3.18, collate2-3.20, collate2-3.22, collate2-3.23, collate2-3.24, collate2-3.25, collate2-3.26, collate2-3.27, collate2-4.1, collate2-4.3, collate2-4.4, collate2-5.0, collate2-5.2, collate2-5.4.1, collate2-5.4.2, collate2-5.5.2, collate2-6.2, collate2-6.3, collate2-6.4, collate2-6.5, collate2-6.6, collate2-6.7
- Primary errors: ! collate2-1.2 expected: [ab aB Ab AB ba bA Ba BA bb bB Bb BB] | ! collate2-1.2 got:      [ab bA bB ba bb] | ! collate2-1.2.1 expected: [ab aB Ab AB ba bA Ba BA bb bB Bb BB]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-collate2-3641455-1769533960775
DEBUG: tester.tcl sourced, db=db
Running collate2.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/collate2.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/collate2.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
collate2-1.0... Ok
collate2-1.1... Ok
collate2-1.1.1... Ok
collate2-1.1.2... Ok
collate2-1.1.3... Ok
collate2-1.2...
! collate2-1.2 expected: [ab aB Ab AB ba bA Ba BA bb bB Bb BB]
! collate2-1.2 got:      [ab bA bB ba bb]
collate2-1.2.1...
! collate2-1.2.1 expected: [ab aB Ab AB ba bA Ba BA bb bB Bb BB]
! collate2-1.2.1 got:      [ab bA bB ba bb]
collate2-1.2.2...
! collate2-1.2.2 expected: [ab aB Ab AB ba bA Ba BA bb bB Bb BB]
! collate2-1.2.2 got:      [ab bA bB ba bb]
collate2-1.2.3...
! collate2-1.2.3 expected: [ab aB Ab AB ba bA Ba BA bb bB Bb BB]
! collate2-1.2.3 got:      [ab bA bB ba bb]
collate2-1.2.4...
! collate2-1.2.4 expected: [ab aB Ab AB ba bA Ba BA bb bB Bb BB]
! collate2-1.2.4 got:      [ab bA bB ba bb]
collate2-1.2.5...
! collate2-1.2.5 expected: [ab aB Ab AB ba bA Ba BA bb bB Bb BB]
! collate2-1.2.5 got:      [ab bA bB ba bb]
collate2-1.2.6...
! collate2-1.2.6 expected: [ab aB Ab AB ba bA Ba BA bb bB Bb BB]
! collate2-1.2.6 got:      [ab bA bB ba bb]
collate2-1.2.7...
! collate2-1.2.7 expected: [ab aB Ab AB ba bA Ba BA bb bB Bb BB]
! collate2-1.2.7 got:      [ab bA bB ba bb]
collate2-1.3...
! collate2-1.3 expected: [ba Ab Bb ab bb]
! collate2-1.3 got:      [ab bA bB ba bb]
collate2-1.3.1...
! collate2-1.3.1 expected: [ba Ab Bb ab bb]
! collate2-1.3.1 got:      [ab bA bB ba bb]
collate2-1.3.2...
! collate2-1.3.2 expected: [ba Ab Bb ab bb]
! collate2-1.3.2 got:      [ab bA bB ba bb]
collate2-1.3.3...
! collate2-1.3.3 expected: [ba Ab Bb ab bb]
! collate2-1.3.3 got:      [ab bA bB ba bb]
collate2-1.4... Ok
collate2-1.5...
! collate2-1.5 expected: []
! collate2-1.5 got:      [AA AB Aa Ab BA BB Ba Bb aA aB]
collate2-1.5.1...
! collate2-1.5.1 expected: []
! collate2-1.5.1 got:      [AA AB Aa Ab BA BB Ba Bb aA aB]
collate2-1.6...
! collate2-1.6 expected: [AA BA aA bA AB BB aB bB Aa Ba]
! collate2-1.6 got:      [AA AB Aa Ab BA BB Ba Bb aA aB]
collate2-1.7... Ok
collate2-1.8...
! collate2-1.8 expected: [aa aA Aa AA]
! collate2-1.8 got:      [aa]
collate2-1.9... Ok
collate2-1.10... Ok
collate2-1.11...
! collate2-1.11 expected: [aa aA Aa AA ab aB Ab AB ba bA Ba BA bb bB Bb BB]
! collate2-1.11 got:      [aa ab bA bB ba bb]
collate2-1.12...
! collate2-1.12 expected: [aa ba Ab Bb ab bb]
! collate2-1.12 got:      [aa ab bA bB ba bb]
collate2-1.13... Ok
collate2-1.14...
! collate2-1.14 expected: [aa aA Aa AA]
! collate2-1.14 got:      [AA AB Aa Ab BA BB Ba Bb aA aB aa]
collate2-1.15...
! collate2-1.15 expected: [AA BA aA bA AB BB aB bB Aa Ba aa]
! collate2-1.15 got:      [AA AB Aa Ab BA BB Ba Bb aA aB aa]
collate2-1.16... Ok
collate2-1.17...
! collate2-1.17 expected: [aa aA Aa AA ab aB Ab AB ba bA Ba BA bb bB Bb BB]
! collate2-1.17 got:      [Aa Ab BA BB Ba Bb]
collate2-1.17.1...
! collate2-1.17.1 expected: [aa aA Aa AA ab aB Ab AB ba bA Ba BA bb bB Bb BB]
! collate2-1.17.1 got:      [Aa Ab BA BB Ba Bb]
collate2-1.18...
! collate2-1.18 expected: [Aa Ba aa ba Ab Bb]
! collate2-1.18 got:      [Aa Ab BA BB Ba Bb]
collate2-1.19... Ok
collate2-1.20...
! collate2-1.20 expected: [aa aA Aa AA]
! collate2-1.20 got:      [aa]
collate2-1.21... Ok
collate2-1.22... Ok
collate2-1.23...
! collate2-1.23 expected: [aa aA Aa AA bb bB Bb BB]
! collate2-1.23 got:      [aa bb]
collate2-1.24... Ok
collate2-1.25... Ok
collate2-1.26...
! collate2-1.26 expected: [aa bb aA bB Aa Bb AA BB]
! collate2-1.26 got:      [aa bb]
collate2-1.27... Ok
collate2-2.1... Ok
collate2-2.2...
! collate2-2.2 expected: [aa aA Aa AA]
! collate2-2.2 got:      [AA AB Aa Ab BA BB Ba Bb aA aB aa]
collate2-2.3...
! collate2-2.3 expected: [AA BA aA bA AB BB aB bB Aa Ba aa]
! collate2-2.3 got:      [AA AB Aa Ab BA BB Ba Bb aA aB aa]
collate2-2.4... Ok
collate2-2.5...
! collate2-2.5 expected: [aa aA Aa AA ab aB Ab AB ba bA Ba BA bb bB Bb BB]
! collate2-2.5 got:      [aa ab bA bB ba bb]
collate2-2.6...
! collate2-2.6 expected: [aa ba Ab Bb ab bb]
! collate2-2.6 got:      [aa ab bA bB ba bb]
collate2-2.7... Ok
collate2-2.8...
! collate2-2.8 expected: [ab ba bb aB bA bB Ab Ba Bb AB BA BB]
! collate2-2.8 got:      [ab ba bb aA aB bA bB Aa Ab Ba Bb AA AB BA BB]
collate2-2.9... Ok
collate2-2.10... Ok
collate2-2.11...
! collate2-2.11 expected: []
! collate2-2.11 got:      [AA AB Aa Ab BA BB Ba Bb aA aB]
collate2-2.12...
! collate2-2.12 expected: [AA BA aA bA AB BB aB bB Aa Ba]
! collate2-2.12 got:      [AA AB Aa Ab BA BB Ba Bb aA aB]
collate2-2.13... Ok
collate2-2.14...
! collate2-2.14 expected: [ab aB Ab AB ba bA Ba BA bb bB Bb BB]
! collate2-2.14 got:      [ab bA bB ba bb]
collate2-2.15...
! collate2-2.15 expected: [ba Ab Bb ab bb]
! collate2-2.15 got:      [ab bA bB ba bb]
collate2-2.16... Ok
collate2-2.17...
! collate2-2.17 expected: []
! collate2-2.17 got:      [AA AB aA aB aa ab bA bB ba bb]
collate2-2.18...
! collate2-2.18 expected: [AA BA aA bA AB BB aB bB ab bb]
! collate2-2.18 got:      [AA AB aA aB aa ab bA bB ba bb]
collate2-2.19... Ok
collate2-2.20...
! collate2-2.20 expected: [{} ab ba bb aB bA bB Ab Ba Bb AB BA BB]
! collate2-2.20 got:      [{} ab ba bb aA aB bA bB Aa Ab Ba Bb AA AB BA BB]
collate2-2.21... Ok
collate2-2.22...
! collate2-2.22 expected: [ab ba aA aB bA bB Aa Ab Ba Bb AA AB BA BB]
! collate2-2.22 got:      [{} ab ba aA aB bA bB Aa Ab Ba Bb AA AB BA BB]
collate2-2.23...
! collate2-2.23 expected: [ab ba aB bA Ab Ba AB BA]
! collate2-2.23 got:      [{} ab ba aA aB bA bB Aa Ab Ba Bb AA AB BA BB]
collate2-2.24...
! collate2-2.24 expected: [ab ba aA aB bA bB Aa Ab Ba Bb AA AB BA BB]
! collate2-2.24 got:      [{} ab ba aA aB bA bB Aa Ab Ba Bb AA AB BA BB]
collate2-2.25...
! collate2-2.25 expected: [ab ba aA aB bA bB Aa Ab Ba Bb AA AB BA BB]
! collate2-2.25 got:      [{} ab ba aA aB bA bB Aa Ab Ba Bb AA AB BA BB]
collate2-2.26...
! collate2-2.26 expected: [ab ba aB bA Ab Ba AB BA]
! collate2-2.26 got:      [{} ab ba aA aB bA bB Aa Ab Ba Bb AA AB BA BB]
collate2-2.27...
! collate2-2.27 expected: [ab ba aA aB bA bB Aa Ab Ba Bb AA AB BA BB]
! collate2-2.27 got:      [{} ab ba aA aB bA bB Aa Ab Ba Bb AA AB BA BB]
collate2-3.1... Ok
collate2-3.2...
! collate2-3.2 expected: [{} 0 1 1 1 0 1 1 1 0 1 1 1 0 1 1 1]
! collate2-3.2 got:      [{} 0 1 1 1 0 0 1 1 0 0 0 0 0 0 0 0]
collate2-3.3...
! collate2-3.3 expected: [{} 0 1 1 1 0 0 0 0 0 1 0 1 0 0 0 0]
! collate2-3.3 got:      [{} 0 1 1 1 0 0 1 1 0 0 0 0 0 0 0 0]
collate2-3.4... Ok
collate2-3.5...
! collate2-3.5 expected: [{} 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
! collate2-3.5 got:      [{} 0 0 0 0 1 1 0 0 1 1 1 1 1 1 1 1]
collate2-3.6...
! collate2-3.6 expected: [{} 0 0 0 0 1 1 1 1 1 0 1 0 1 1 1 1]
! collate2-3.6 got:      [{} 0 0 0 0 1 1 0 0 1 1 1 1 1 1 1 1]
collate2-3.7... Ok
collate2-3.8...
! collate2-3.8 expected: [{} 1 0 0 0 1 0 0 0 1 0 0 0 1 0 0 0]
! collate2-3.8 got:      [{} 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
collate2-3.9... Ok
collate2-3.10... Ok
collate2-3.11...
! collate2-3.11 expected: [{} 1 0 0 0 1 0 0 0 1 0 0 0 1 0 0 0]
! collate2-3.11 got:      [{} 1 0 0 0 1 1 0 0 1 1 1 1 1 1 1 1]
collate2-3.12...
! collate2-3.12 expected: [{} 1 0 0 0 1 1 1 1 1 0 1 0 1 1 1 1]
! collate2-3.12 got:      [{} 1 0 0 0 1 1 0 0 1 1 1 1 1 1 1 1]
collate2-3.13... Ok
collate2-3.14...
! collate2-3.14 expected: [{} 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1]
! collate2-3.14 got:      [{} 1 1 1 1 0 0 1 1 0 0 0 0 0 0 0 0]
collate2-3.15...
! collate2-3.15 expected: [{} 1 1 1 1 0 0 0 0 0 1 0 1 0 0 0 0]
! collate2-3.15 got:      [{} 1 1 1 1 0 0 1 1 0 0 0 0 0 0 0 0]
collate2-3.16...
! collate2-3.16 expected: [{} 0 0 0 0 0 0 0 0 1 1 1 1 0 0 1 1]
! collate2-3.16 got:      [1 0 0 0 0 0 0 0 0 1 1 1 1 0 0 1 1]
collate2-3.17...
! collate2-3.17 expected: [{} 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1]
! collate2-3.17 got:      [1 0 0 0 0 0 0 0 0 1 1 1 1 0 0 1 1]
collate2-3.18...
! collate2-3.18 expected: [{} 1 0 1 0 0 0 0 0 1 1 1 1 0 0 0 0]
! collate2-3.18 got:      [1 0 0 0 0 0 0 0 0 1 1 1 1 0 0 1 1]
collate2-3.19... Ok
collate2-3.20...
! collate2-3.20 expected: [0 1 0 0 0 1 0 0 0 1 0 0 0 1 0 0 0]
! collate2-3.20 got:      [0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
collate2-3.21... Ok
collate2-3.22...
! collate2-3.22 expected: [{} 1 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0]
! collate2-3.22 got:      [0 1 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0]
collate2-3.23...
! collate2-3.23 expected: [{} 1 0 0 1 1 0 0 1 1 0 0 1 1 0 0 1]
! collate2-3.23 got:      [0 1 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0]
collate2-3.24...
! collate2-3.24 expected: [{} 1 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0]
! collate2-3.24 got:      [0 1 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0]
collate2-3.25...
! collate2-3.25 expected: [{} 1 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0]
! collate2-3.25 got:      [0 1 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0]
collate2-3.26...
! collate2-3.26 expected: [{} 1 0 0 1 1 0 0 1 1 0 0 1 1 0 0 1]
! collate2-3.26 got:      [0 1 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0]
collate2-3.27...
! collate2-3.27 expected: [{} 1 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0]
! collate2-3.27 got:      [0 1 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0]
collate2-4.0... Ok
collate2-4.1...
! collate2-4.1 expected: [aa aA Aa AA]
! collate2-4.1 got:      [aa]
collate2-4.2... Ok
collate2-4.3...
! collate2-4.3 expected: [aa aA Aa AA]
! collate2-4.3 got:      [aa]
collate2-4.4...
! collate2-4.4 expected: [aa aA Aa AA]
! collate2-4.4 got:      [aa]
collate2-4.5... Ok
collate2-5.0...
! collate2-5.0 expected: [aa aA Aa AA]
! collate2-5.0 got:      [aa]
collate2-5.1... Ok
collate2-5.2...
! collate2-5.2 expected: [aa aA Aa AA]
! collate2-5.2 got:      [aa]
collate2-5.3... Ok
collate2-5.4.1...
! collate2-5.4.1 expected: [{} aa {} {} {} aa {} {} {} aa {} {} {} aa {} {} {}]
! collate2-5.4.1 got:      [{} aa {} {} {} {} {} {} {} {} {} {} {} {} {} {} {}]
collate2-5.4.2...
! collate2-5.4.2 expected: [{} aa {} {} {} aa {} {} {} aa {} {} {} aa {} {} {}]
! collate2-5.4.2 got:      [aa]
collate2-5.4.3... Ok
collate2-5.5.1... Ok
collate2-5.5.2...
! collate2-5.5.2 expected: [aa aa]
! collate2-5.5.2 got:      [{} {} aa aa ab {} ba {} bb {} aA {} aB {} bA {} bB {} Aa {} Ab {} Ba {} Bb {} AA {} AB {} BA {} BB {}]
collate2-6.1... Ok
collate2-6.2...
! collate2-6.2 expected: [b B]
! collate2-6.2 got:      [b]
collate2-6.3...
! collate2-6.3 expected: [b B]
! collate2-6.3 got:      [b]
collate2-6.4...
! collate2-6.4 expected: [b B]
! collate2-6.4 got:      [b]
collate2-6.5...
! collate2-6.5 expected: [b B]
! collate2-6.5 got:      [b]
collate2-6.6...
! collate2-6.6 expected: [b B]
! collate2-6.6 got:      [b]
collate2-6.7...
! collate2-6.7 expected: [b B]
! collate2-6.7 got:      [b]
Running "collate2"

Error in collate2.test: couldn't read file "collate2": no such file or directory
couldn't read file "collate2": no such file or directory
    while executing
"source collate2"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/collate2.test" line 742)
    invoked from within
"source $test_file"

==========================================
Test: collate2
Time: 0s
Status: FAILED
