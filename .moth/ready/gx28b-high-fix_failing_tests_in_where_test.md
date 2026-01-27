# Summary
- Test: sqlite3/test/where.test
- Repro: `testfixture test/where.test`
- Failing cases: where-1.8.2, where-4.2, where-5.2, where-5.3a, where-5.3b, where-5.3c, where-5.3d, where-5.4, where-5.5, where-5.6, where-5.7, where-5.8, where-5.9, where-5.10, where-5.11, where-5.12, where-5.13, where-5.14, where-5.15, where-6.2, where-6.7.1, where-6.7.2, where-6.8a, where-6.8b, where-6.9.2, where-6.9.3, where-6.9.4, where-6.9.5, where-6.9.6, where-6.9.7, where-6.9.8, where-6.9.9, where-6.11, where-6.12, where-6.13, where-6.14, where-6.15, where-6.19, where-6.20, where-6.21, where-6.22, where-6.23, where-6.24, where-6.25, where-6.26, where-6.27, where-7.1, where-7.2, where-7.3, where-7.4, where-7.5, where-7.6, where-7.7, where-7.8, where-7.9, where-7.10, where-7.11, where-7.12, where-7.13, where-7.14, where-7.16, where-7.18, where-7.20, where-7.22, where-7.24, where-7.26, where-7.28, where-7.30, where-7.31, where-7.32, where-7.33, where-7.34, where-7.35, where-8.1, where-10.1, where-12.1, where-12.2, where-12.3, where-12.4, where-12.5, where-12.6, where-12.9, where-12.10, where-12.11, where-12.12, where-13.1, where-13.2, where-13.3, where-13.4, where-13.5, where-13.6, where-13.9, where-13.10, where-13.11, where-13.12, where-14.1, where-14.2, where-14.3, where-14.4, where-14.7.2, where-99.0, where-17.4, where-18.1rj, where-18.3rj, where-18.4, where-18.4rj, where-18.5, where-18.6, where-19.0, where-21.1, where-24.2.1, where-24.2.2, where-24.2.3, where-24.3.1, where-24.3.2, where-24.3.3, where-24.4.1, where-24.4.2, where-24.4.3, where-25.1, where-25.2, where-25.5, where-26.2, where-26.4, where-26.8, where-30.1
- Primary errors: ! where-1.8.2 expected: [/**SEARCH t1 USING INDEX i1xy (x=? AND y=?)**/] | ! where-1.8.2 got:      [0 0 0 {SEARCH t1 USING COVERING INDEX i1xy (x=? AND y=?)}] | ! where-4.2 expected: [1 0 4 0]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-where-3641349-1769533960709
DEBUG: tester.tcl sourced, db=db
Running where.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/where.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/where.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
where-1.0... Ok
where-1.1.1... Ok
where-1.1.1b... Ok
where-1.1.2... Ok
where-1.1.2b... Ok
where-1.1.3... Ok
where-1.1.4... Ok
where-1.1.5... Ok
where-1.1.6... Ok
where-1.1.7... Ok
where-1.1.8... Ok
where-1.1.9... Ok
where-1.2.1... Ok
where-1.2.2... Ok
where-1.3.1... Ok
where-1.3.2... Ok
where-1.3.3... Ok
where-1.4.1... Ok
where-1.4.1b... Ok
where-1.4.2... Ok
where-1.4.2b... Ok
where-1.4.3... Ok
where-1.4.4... Ok
where-1.5... Ok
where-1.5.2... Ok
where-1.6... Ok
where-1.7... Ok
where-1.8... Ok
where-1.8.2...
! where-1.8.2 expected: [/**SEARCH t1 USING INDEX i1xy (x=? AND y=?)**/]
! where-1.8.2 got:      [0 0 0 {SEARCH t1 USING COVERING INDEX i1xy (x=? AND y=?)}]
where-1.8.3... Ok
where-1.9... Ok
where-1.10... Ok
where-1.11... Ok
where-1.11b... Ok
where-1.12... Ok
where-1.12b... Ok
where-1.13... Ok
where-1.14... Ok
where-1.14b... Ok
where-1.15... Ok
where-1.16... Ok
where-1.17... Ok
where-1.18... Ok
where-1.18b... Ok
where-1.19... Ok
where-1.20... Ok
where-1.21... Ok
where-1.22... Ok
where-1.22b... Ok
where-1.23... Ok
where-1.24... Ok
where-1.25... Ok
where-1.27... Ok
where-1.28... Ok
where-1.29... Ok
where-1.30... Ok
where-1.31... Ok
where-1.33... Ok
where-1.33.1... Ok
where-1.33.2... Ok
where-1.33.3... Ok
where-1.33.4... Ok
where-1.33.5... Ok
where-1.34... Ok
where-1.35... Ok
where-1.36... Ok
where-1.37... Ok
where-1.38... Ok
where-1.39... Ok
where-1.40... Ok
where-1.41... Ok
where-2.1... Ok
where-2.2... Ok
where-2.3... Ok
where-2.4... Ok
where-2.5... Ok
where-2.6... Ok
where-2.7... Ok
where-3.1... Ok
where-3.2... Ok
where-3.3... Ok
where-4.1... Ok
where-4.2...
! where-4.2 expected: [1 0 4 0]
! where-4.2 got:      [1 0 4 1]
where-4.3... Ok
where-4.4... Ok
where-4.5... Ok
where-4.6... Ok
where-4.7... Ok
where-5.1... Ok
where-5.2...
! where-5.2 expected: [1 0 4 2 1 9 3 1 16 102]
! where-5.2 got:      [1 0 4 2 1 9 3 1 16 99]
where-5.3a...
! where-5.3a expected: [1 0 4 2 1 9 3 1 16 12]
! where-5.3a got:      [1 0 4 2 1 9 3 1 16 99]
where-5.3b...
! where-5.3b expected: [1 0 4 2 1 9 3 1 16 12]
! where-5.3b got:      [1 0 4 2 1 9 3 1 16 99]
where-5.3c...
! where-5.3c expected: [1 0 4 2 1 9 3 1 16 12]
! where-5.3c got:      [1 0 4 2 1 9 3 1 16 99]
where-5.3d...
! where-5.3d expected: [3 1 16 2 1 9 1 0 4 11]
! where-5.3d got:      [3 1 16 2 1 9 1 0 4 99]
where-5.4...
! where-5.4 expected: [1 0 4 2 1 9 3 1 16 102]
! where-5.4 got:      [1 0 4 2 1 9 3 1 16 99]
where-5.5...
! where-5.5 expected: [2 1 9 4 2 25 3]
! where-5.5 got:      [1 0 4 2 1 9 3 1 16 4 2 25 5 2 36 6 2 49 7 2 64 8 3 81 9 3 100 10 3 121 11 3 144 12 3 169 13 3 196 14 3 225 15 3 256 16 4 289 17 4 324 18 4 361 19 4 400 20 4 441 21 4 484 22 4 529 23 4 576 24 4 625 25 4 676 26 4 729 27 4 784 28 4 841 29 4 900 30 4 961 31 4 1024 32 5 1089 33 5 1156 34 5 1225 35 5 1296 36 5 1369 37 5 1444 38 5 1521 39 5 1600 40 5 1681 41 5 1764 42 5 1849 43 5 1936 44 5 2025 45 5 2116 46 5 2209 47 5 2304 48 5 2401 49 5 2500 50 5 2601 51 5 2704 52 5 2809 53 5 2916 54 5 3025 55 5 3136 56 5 3249 57 5 3364 58 5 3481 59 5 3600 60 5 3721 61 5 3844 62 5 3969 63 5 4096 64 6 4225 65 6 4356 66 6 4489 67 6 4624 68 6 4761 69 6 4900 70 6 5041 71 6 5184 72 6 5329 73 6 5476 74 6 5625 75 6 5776 76 6 5929 77 6 6084 78 6 6241 79 6 6400 80 6 6561 81 6 6724 82 6 6889 83 6 7056 84 6 7225 85 6 7396 86 6 7569 87 6 7744 88 6 7921 89 6 8100 90 6 8281 91 6 8464 92 6 8649 93 6 8836 94 6 9025 95 6 9216 96 6 9409 97 6 9604 98 6 9801 99 6 10000 100 6 10201 99]
where-5.6...
! where-5.6 expected: [2 1 9 4 2 25 103]
! where-5.6 got:      [2 1 9 4 2 25 399]
where-5.7...
! where-5.7 expected: [2 1 9 4 2 25 9]
! where-5.7 got:      [2 1 9 4 2 25 399]
where-5.8...
! where-5.8 expected: [2 1 9 4 2 25 103]
! where-5.8 got:      [2 1 9 4 2 25 399]
where-5.9...
! where-5.9 expected: [2 1 9 3 1 16 6]
! where-5.9 got:      [2 1 9 3 1 16 99]
where-5.10...
! where-5.10 expected: [2 1 9 3 1 16 199]
! where-5.10 got:      [2 1 9 3 1 16 99]
where-5.11...
! where-5.11 expected: [79 6 6400 89 6 8100 199]
! where-5.11 got:      [79 6 6400 89 6 8100 99]
where-5.12...
! where-5.12 expected: [79 6 6400 89 6 8100 7]
! where-5.12 got:      [79 6 6400 89 6 8100 39]
where-5.13...
! where-5.13 expected: [2 1 9 3 1 16 6]
! where-5.13 got:      [2 1 9 3 1 16 99]
where-5.14...
! where-5.14 expected: [2 1 9 5]
! where-5.14 got:      [2 1 9 99]
where-5.15...
! where-5.15 expected: [2 1 9 3 1 16 9]
! where-5.15 got:      [2 1 9 3 1 16 99]
where-5.100... Ok
where-5.101... Ok
where-5.102... Ok
where-5.103... Ok
where-6.1... Ok
where-6.2...
! where-6.2 expected: [1 100 4 2 99 9 3 98 16 nosort]
! where-6.2 got:      [1 100 4 2 99 9 3 98 16 sort]
where-6.3... Ok
where-6.4... Ok
where-6.5... Ok
where-6.6... Ok
where-6.7.1...
! where-6.7.1 expected: [/1 100 4 2 99 9 3 98 16 .* nosort/]
! where-6.7.1 got:      [1 100 4 2 99 9 3 98 16 4 97 25 5 96 36 6 95 49 7 94 64 8 93 81 9 92 100 10 91 121 sort]
where-6.7.2...
! where-6.7.2 expected: [1 100 4 nosort]
! where-6.7.2 got:      [1 100 4 sort]
where-6.8a...
! where-6.8a expected: [1 100 4 2 99 9 3 98 16 nosort]
! where-6.8a got:      [1 100 4 2 99 9 3 98 16 sort]
where-6.8b...
! where-6.8b expected: [9 92 100 7 94 64 5 96 36 nosort]
! where-6.8b got:      [9 92 100 7 94 64 5 96 36 sort]
where-6.9.1... Ok
where-6.9.1.1... Ok
where-6.9.1.2... Ok
where-6.9.2...
! where-6.9.2 expected: [1 100 4 nosort]
! where-6.9.2 got:      [1 100 4 sort]
where-6.9.3...
! where-6.9.3 expected: [1 100 4 nosort]
! where-6.9.3 got:      [1 100 4 sort]
where-6.9.4...
! where-6.9.4 expected: [1 100 4 nosort]
! where-6.9.4 got:      [1 100 4 sort]
where-6.9.5...
! where-6.9.5 expected: [1 100 4 nosort]
! where-6.9.5 got:      [1 100 4 sort]
where-6.9.6...
! where-6.9.6 expected: [1 100 4 nosort]
! where-6.9.6 got:      [1 100 4 sort]
where-6.9.7...
! where-6.9.7 expected: [1 100 4 nosort]
! where-6.9.7 got:      [1 100 4 sort]
where-6.9.8...
! where-6.9.8 expected: [1 100 4 nosort]
! where-6.9.8 got:      [1 100 4 sort]
where-6.9.9...
! where-6.9.9 expected: [1 100 4 nosort]
! where-6.9.9 got:      [1 100 4 sort]
where-6.10... Ok
where-6.11...
! where-6.11 expected: [1 100 4 nosort]
! where-6.11 got:      [1 100 4 sort]
where-6.12...
! where-6.12 expected: [1 100 4 nosort]
! where-6.12 got:      [1 100 4 sort]
where-6.13...
! where-6.13 expected: [100 1 10201 99 2 10000 98 3 9801 nosort]
! where-6.13 got:      [100 1 10201 99 2 10000 98 3 9801 sort]
where-6.13.1... Ok
where-6.14...
! where-6.14 expected: [100 1 10201 99 2 10000 98 3 9801 nosort]
! where-6.14 got:      [100 1 10201 99 2 10000 98 3 9801 sort]
where-6.15...
! where-6.15 expected: [1 0 2 1 3 1 nosort]
! where-6.15 got:      [1 0 2 1 3 1 sort]
where-6.16... Ok
where-6.19...
! where-6.19 expected: [4 9 16 nosort]
! where-6.19 got:      [4 9 16 sort]
where-6.20...
! where-6.20 expected: [4 9 16 nosort]
! where-6.20 got:      [4 9 16 sort]
where-6.21...
! where-6.21 expected: [4 9 16 nosort]
! where-6.21 got:      [4 9 16 sort]
where-6.22...
! where-6.22 expected: [4 9 16 nosort]
! where-6.22 got:      [4 9 16 sort]
where-6.23...
! where-6.23 expected: [9 16 25 nosort]
! where-6.23 got:      [9 16 25 sort]
where-6.24...
! where-6.24 expected: [9 16 25 nosort]
! where-6.24 got:      [9 16 25 sort]
where-6.25...
! where-6.25 expected: [9 16 nosort]
! where-6.25 got:      [9 16 sort]
where-6.26...
! where-6.26 expected: [4 9 16 25 nosort]
! where-6.26 got:      [4 9 16 25 sort]
where-6.27...
! where-6.27 expected: [4 9 16 25 nosort]
! where-6.27 got:      [4 9 16 25 sort]
where-7.1...
! where-7.1 expected: [8 9 10 11 12 13 14 15 nosort]
! where-7.1 got:      [8 9 10 11 12 13 14 15 sort]
where-7.2...
! where-7.2 expected: [15 14 13 12 11 10 9 8 nosort]
! where-7.2 got:      [15 14 13 12 11 10 9 8 sort]
where-7.3...
! where-7.3 expected: [10 11 12 nosort]
! where-7.3 got:      [10 11 12 sort]
where-7.4...
! where-7.4 expected: [15 14 13 nosort]
! where-7.4 got:      [15 14 13 sort]
where-7.5...
! where-7.5 expected: [15 14 13 12 11 nosort]
! where-7.5 got:      [15 14 13 12 11 sort]
where-7.6...
! where-7.6 expected: [15 14 13 12 11 10 nosort]
! where-7.6 got:      [15 14 13 12 11 10 sort]
where-7.7...
! where-7.7 expected: [12 11 10 nosort]
! where-7.7 got:      [12 11 10 sort]
where-7.8...
! where-7.8 expected: [13 12 11 10 nosort]
! where-7.8 got:      [13 12 11 10 sort]
where-7.9...
! where-7.9 expected: [13 12 11 nosort]
! where-7.9 got:      [13 12 11 sort]
where-7.10...
! where-7.10 expected: [12 11 10 nosort]
! where-7.10 got:      [12 11 10 sort]
where-7.11...
! where-7.11 expected: [10 11 12 nosort]
! where-7.11 got:      [10 11 12 sort]
where-7.12...
! where-7.12 expected: [10 11 12 13 nosort]
! where-7.12 got:      [10 11 12 13 sort]
where-7.13...
! where-7.13 expected: [11 12 13 nosort]
! where-7.13 got:      [11 12 13 sort]
where-7.14...
! where-7.14 expected: [10 11 12 nosort]
! where-7.14 got:      [10 11 12 sort]
where-7.15... Ok
where-7.16...
! where-7.16 expected: [8 nosort]
! where-7.16 got:      [8 sort]
where-7.17... Ok
where-7.18...
! where-7.18 expected: [15 nosort]
! where-7.18 got:      [15 sort]
where-7.19... Ok
where-7.20...
! where-7.20 expected: [8 nosort]
! where-7.20 got:      [8 sort]
where-7.21... Ok
where-7.22...
! where-7.22 expected: [15 nosort]
! where-7.22 got:      [15 sort]
where-7.23... Ok
where-7.24...
! where-7.24 expected: [1 nosort]
! where-7.24 got:      [1 sort]
where-7.25... Ok
where-7.26...
! where-7.26 expected: [100 nosort]
! where-7.26 got:      [100 sort]
where-7.27... Ok
where-7.28...
! where-7.28 expected: [1 nosort]
! where-7.28 got:      [1 sort]
where-7.29... Ok
where-7.30...
! where-7.30 expected: [100 nosort]
! where-7.30 got:      [100 sort]
where-7.31...
! where-7.31 expected: [10201 10000 9801 nosort]
! where-7.31 got:      [10201 10000 9801 sort]
where-7.32...
! where-7.32 expected: [16 9 4 nosort]
! where-7.32 got:      [16 9 4 sort]
where-7.33...
! where-7.33 expected: [25 16 9 4 nosort]
! where-7.33 got:      [25 16 9 4 sort]
where-7.34...
! where-7.34 expected: [16 9 nosort]
! where-7.34 got:      [16 9 sort]
where-7.35...
! where-7.35 expected: [16 9 4 nosort]
! where-7.35 got:      [16 9 4 sort]
where-8.1...
! where-8.1 expected: [30 29 28 nosort]
! where-8.1 got:      [30 29 28 sort]
where-8.2... Ok
where-9.1... Ok
where-9.2... Ok
where-9.3... Ok
where-10.1...
! where-10.1 expected: []
! where-10.1 got:      [1]
where-10.2... Ok
where-10.3... Ok
where-10.4... Ok
where-11.1... Ok
where-12.1...
! where-12.1 expected: [4 four 1 one nosort]
! where-12.1 got:      [4 four 1 one sort]
where-12.2...
! where-12.2 expected: [4 four 1 one nosort]
! where-12.2 got:      [4 four 1 one sort]
where-12.3...
! where-12.3 expected: [1 one 4 four nosort]
! where-12.3 got:      [1 one 4 four sort]
where-12.4...
! where-12.4 expected: [1 one 4 four nosort]
! where-12.4 got:      [1 one 4 four sort]
where-12.5...
! where-12.5 expected: [1 one 4 four nosort]
! where-12.5 got:      [1 one 4 four sort]
where-12.6...
! where-12.6 expected: [1 one 4 four nosort]
! where-12.6 got:      [1 one 4 four sort]
where-12.7... Ok
where-12.8... Ok
where-12.9...
! where-12.9 expected: [4 four 1 one nosort]
! where-12.9 got:      [4 four 1 one sort]
where-12.10...
! where-12.10 expected: [4 four 1 one nosort]
! where-12.10 got:      [4 four 1 one sort]
where-12.11...
! where-12.11 expected: [4 four 1 one nosort]
! where-12.11 got:      [4 four 1 one sort]
where-12.12...
! where-12.12 expected: [1 one 4 four nosort]
! where-12.12 got:      [1 one 4 four sort]
where-13.1...
! where-13.1 expected: [4 four 1 one nosort]
! where-13.1 got:      [4 four 1 one sort]
where-13.2...
! where-13.2 expected: [4 four 1 one nosort]
! where-13.2 got:      [4 four 1 one sort]
where-13.3...
! where-13.3 expected: [1 one 4 four nosort]
! where-13.3 got:      [1 one 4 four sort]
where-13.4...
! where-13.4 expected: [1 one 4 four nosort]
! where-13.4 got:      [1 one 4 four sort]
where-13.5...
! where-13.5 expected: [1 one 4 four nosort]
! where-13.5 got:      [1 one 4 four sort]
where-13.6...
! where-13.6 expected: [1 one 4 four nosort]
! where-13.6 got:      [1 one 4 four sort]
where-13.7... Ok
where-13.8... Ok
where-13.9...
! where-13.9 expected: [4 four 1 one nosort]
! where-13.9 got:      [4 four 1 one sort]
where-13.10...
! where-13.10 expected: [4 four 1 one nosort]
! where-13.10 got:      [4 four 1 one sort]
where-13.11...
! where-13.11 expected: [4 four 1 one nosort]
! where-13.11 got:      [4 four 1 one sort]
where-13.12...
! where-13.12 expected: [1 one 4 four nosort]
! where-13.12 got:      [1 one 4 four sort]
where-14.1...
! where-14.1 expected: [1/4 1/1 4/4 4/1 nosort]
! where-14.1 got:      [1/4 1/1 4/4 4/1 sort]
where-14.2...
! where-14.2 expected: [1/1 1/4 4/1 4/4 nosort]
! where-14.2 got:      [1/1 1/4 4/1 4/4 sort]
where-14.3...
! where-14.3 expected: [1/4 1/1 4/4 4/1 nosort]
! where-14.3 got:      [1/1 1/4 4/1 4/4 sort]
where-14.4...
! where-14.4 expected: [1/4 1/1 4/4 4/1 nosort]
! where-14.4 got:      [1/1 1/4 4/1 4/4 sort]
where-14.5... Ok
where-14.6... Ok
where-14.7... Ok
where-14.7.1... Ok
where-14.7.2...
! where-14.7.2 expected: [4/4 4/1 1/4 1/1 nosort]
! where-14.7.2 got:      [4/1 4/4 1/1 1/4 sort]
where-14.8... Ok
where-14.9... Ok
where-14.10... Ok
where-14.11... Ok
where-14.12... Ok
where-15.1...
Error: table "t1" already exists
where-16.1... Ok
where-16.2... Ok
where-16.3... Ok
where-16.4... Ok
where-99.0...
! where-99.0 expected: [ok]
! where-99.0 got:      [{*** table sqlite_stat1 has invalid root page}]
where-17.1... Ok
where-17.2... Ok
where-17.3... Ok
where-17.4...
! where-17.4 expected: [1.5 42]
! where-17.4 got:      [1.5 43]
where-17.5... Ok
where-18.1... Ok
where-18.1rj...
! where-18.1rj expected: [1]
! where-18.1rj got:      []
where-18.2... Ok
where-18.3... Ok
where-18.3rj...
! where-18.3rj expected: [1]
! where-18.3rj got:      []
where-18.4...
! where-18.4 expected: [1]
! where-18.4 got:      [1 1 1 1 1]
where-18.4rj...
! where-18.4rj expected: [1]
! where-18.4rj got:      []
where-18.5...
! where-18.5 expected: [1 2]
! where-18.5 got:      [1 1 1 1 1 2]
where-18.6...
! where-18.6 expected: [1 2]
! where-18.6 got:      [1 1 1 1 1 2 2]
where-19.0...
! where-19.0 expected: [/.* sqlite_autoindex_t191_1 .* sqlite_autoindex_t191_2 .*/]
! where-19.0 got:      [0 0 0 {SEARCH t192 USING INTEGER PRIMARY KEY (rowid=?)} 1 0 0 {SCAN t191}]
where-20.0... Ok
where-21.0... Ok
where-21.1...
! where-21.1 expected: [4 1 0 4 0 1]
! where-21.1 got:      [4 0 1 4 1 0 5 0 1 5 1 0]
where-22.1...
Error: no such database: db
where-23.0...
Error: no such database: db
where-24.0... Ok
where-24.1.1... Ok
where-24.1.2... Ok
where-24.1.3... Ok
where-24-1.4... Ok
where-24.2.1...
! where-24.2.1 expected: [one two three]
! where-24.2.1 got:      []
where-24.2.2...
! where-24.2.2 expected: [one two three]
! where-24.2.2 got:      []
where-24.2.3...
! where-24.2.3 expected: [three two one]
! where-24.2.3 got:      []
where-24-2.4... Ok
where-24.3.1...
! where-24.3.1 expected: [two three four]
! where-24.3.1 got:      [four]
where-24.3.2...
! where-24.3.2 expected: [two three four]
! where-24.3.2 got:      [four]
where-24.3.3...
! where-24.3.3 expected: [four three two]
! where-24.3.3 got:      [four]
where-24-3.4... Ok
where-24.4.1...
! where-24.4.1 expected: [two three]
! where-24.4.1 got:      []
where-24.4.2...
! where-24.4.2 expected: [two three]
! where-24.4.2 got:      []
where-24.4.3...
! where-24.4.3 expected: [three two]
! where-24.4.3 got:      []
where-24-4.4... Ok
where-24.5.1... Ok
where-24.5.2... Ok
where-24.5.3... Ok
where-24-5.4... Ok
where-24.6.1... Ok
where-24.6.2... Ok
where-24.6.3... Ok
where-24-6.4... Ok
where-24.7.1... Ok
where-24.7.2... Ok
where-24.7.3... Ok
where-24-7.4... Ok
where-24.7.1... Ok
where-24.7.2... Ok
where-24.7.3... Ok
where-24-7.4... Ok
where-24.8.1... Ok
where-24.8.2... Ok
where-24.8.3... Ok
where-24-8.4... Ok
where-24.9.1... Ok
where-24.9.2... Ok
where-24.9.3... Ok
where-24-9.4... Ok
where-25.0...
Error: table sqlite_schema may not be modified
where-25.1...
! where-25.1 expected: [1 {database disk image is malformed}]
! where-25.1 got:      [0 {}]
where-25.2...
! where-25.2 expected: [1 {database disk image is malformed}]
! where-25.2 got:      [0 {}]
where-25.3...
Error: table sqlite_schema may not be modified
where-25.4... Ok
where-25.5...
! where-25.5 expected: [1 {corrupt database}]
! where-25.5 got:      [1 {database disk image is malformed}]
where-26.1... Ok
where-26.2...
! where-26.2 expected: [1 a]
! where-26.2 got:      []
where-26.3... Ok
where-26.4...
! where-26.4 expected: [1 a]
! where-26.4 got:      []
where-26.5... Ok
where-26.6... Ok
where-26.7... Ok
where-26.8...
! where-26.8 expected: [1]
! where-26.8 got:      [{}]
where-27.1... Ok
where-27.2... Ok
where-28.1... Ok
where-30.1...
! where-30.1 expected: [SCAN]
! where-30.1 got:      []
Running "where"

Error in where.test: couldn't read file "where": no such file or directory
couldn't read file "where": no such file or directory
    while executing
"source where"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/where.test" line 1707)
    invoked from within
"source $test_file"

==========================================
Test: where
Time: 0s
Status: FAILED
