# Summary
- Test: sqlite3/test/types2.test
- Repro: `testfixture test/types2.test`
- Failing cases: types2-1.5.1, types2-1.5.2, types2-1.5.3, types2-1.9.1, types2-1.9.2, types2-1.9.3, types2-2.1, types2-2.2, types2-2.3, types2-2.4, types2-2.5, types2-2.6, types2-2.7, types2-2.8, types2-2.9, types2-2.10, types2-2.11, types2-2.12, types2-3.1, types2-3.2, types2-3.3, types2-3.4, types2-4.7.1, types2-4.7.2, types2-4.7.3, types2-4.11.1, types2-4.11.2, types2-4.11.3, types2-5.1.1, types2-5.1.2, types2-5.1.3, types2-5.6.1, types2-5.6.2, types2-5.6.3, types2-5.14.1, types2-5.14.2, types2-5.14.3, types2-5.15.1, types2-5.15.2, types2-5.15.3, types2-5.40.1, types2-5.40.2, types2-5.40.3, types2-5.42.1, types2-5.42.2, types2-5.42.3, types2-5.43.1, types2-5.43.2, types2-5.43.3, types2-6.3, types2-6.4, types2-6.5, types2-6.7, types2-6.8, types2-7.2.1, types2-7.2.2, types2-7.2.3, types2-7.3.1, types2-7.3.2, types2-7.3.3, types2-7.4.1, types2-7.4.2, types2-7.4.3, types2-7.5.1, types2-7.5.2, types2-7.5.3, types2-7.7.1, types2-7.7.2, types2-7.7.3, types2-7.8.1, types2-7.8.2, types2-7.8.3, types2-7.9.1, types2-7.9.2, types2-7.9.3, types2-7.10.1, types2-7.10.2, types2-7.10.3, types2-8.1, types2-8.2, types2-8.3, types2-8.4, types2-8.5, types2-8.6, types2-8.9, types2-8.8
- Primary errors: ! types2-1.5.1 expected: [1] | ! types2-1.5.1 got:      [0] | ! types2-1.5.2 expected: [1]

## Log
DEBUG: About to source tester.tcl, pwd=/tmp/rustql-test-types2-3641384-1769533960727
DEBUG: tester.tcl sourced, db=db
Running types2.test...
==========================================
DEBUG: argv0=/tank/repos/rustql-architecture/sqlite3/test/types2.test, test_file=/tank/repos/rustql-architecture/sqlite3/test/types2.test
DEBUG: testdir=/tank/repos/rustql-architecture/sqlite3/test
types2-1.1.1... Ok
types2-1.1.2... Ok
types2-1.1.3... Ok
types2-1.2.1... Ok
types2-1.2.2... Ok
types2-1.2.3... Ok
types2-1.3.1... Ok
types2-1.3.2... Ok
types2-1.3.3... Ok
types2-1.4.1... Ok
types2-1.4.2... Ok
types2-1.4.3... Ok
types2-1.5.1...
! types2-1.5.1 expected: [1]
! types2-1.5.1 got:      [0]
types2-1.5.2...
! types2-1.5.2 expected: [1]
! types2-1.5.2 got:      []
types2-1.5.3...
! types2-1.5.3 expected: []
! types2-1.5.3 got:      [1]
types2-1.6.1... Ok
types2-1.6.2... Ok
types2-1.6.3... Ok
types2-1.7.1... Ok
types2-1.7.2... Ok
types2-1.7.3... Ok
types2-1.8.1... Ok
types2-1.8.2... Ok
types2-1.8.3... Ok
types2-1.9.1...
! types2-1.9.1 expected: [1]
! types2-1.9.1 got:      [0]
types2-1.9.2...
! types2-1.9.2 expected: [1]
! types2-1.9.2 got:      []
types2-1.9.3...
! types2-1.9.3 expected: []
! types2-1.9.3 got:      [1]
types2-1.10.1... Ok
types2-1.10.2... Ok
types2-1.10.3... Ok
types2-1.11.1... Ok
types2-1.11.2... Ok
types2-1.11.3... Ok
types2-1.12.1... Ok
types2-1.12.2... Ok
types2-1.12.3... Ok
types2-1.13.1... Ok
types2-1.13.2... Ok
types2-1.13.3... Ok
types2-1.14.1... Ok
types2-1.14.2... Ok
types2-1.14.3... Ok
types2-1.15.1... Ok
types2-1.15.2... Ok
types2-1.15.3... Ok
types2-1.16.1... Ok
types2-1.16.2... Ok
types2-1.16.3... Ok
types2-1.17.1... Ok
types2-1.17.2... Ok
types2-1.17.3... Ok
types2-1.18.1... Ok
types2-1.18.2... Ok
types2-1.18.3... Ok
types2-1.19.1... Ok
types2-1.19.2... Ok
types2-1.19.3... Ok
types2-1.20.1... Ok
types2-1.20.2... Ok
types2-1.20.3... Ok
types2-1.21.1... Ok
types2-1.21.2... Ok
types2-1.21.3... Ok
types2-1.22.1... Ok
types2-1.22.2... Ok
types2-1.22.3... Ok
types2-1.23.1... Ok
types2-1.23.2... Ok
types2-1.23.3... Ok
types2-1.24.1... Ok
types2-1.24.2... Ok
types2-1.24.3... Ok
types2-1.25.1... Ok
types2-1.25.2... Ok
types2-1.25.3... Ok
types2-1.26.1... Ok
types2-1.26.2... Ok
types2-1.26.3... Ok
types2-1.27.1... Ok
types2-1.27.2... Ok
types2-1.27.3... Ok
types2-1.28.1... Ok
types2-1.28.2... Ok
types2-1.28.3... Ok
types2-2.1...
! types2-2.1 expected: [1 2 3 4]
! types2-2.1 got:      [1 2]
types2-2.2...
! types2-2.2 expected: [1 2 3 4]
! types2-2.2 got:      [1 2]
types2-2.3...
! types2-2.3 expected: [1 2 3 4]
! types2-2.3 got:      [1 2]
types2-2.4...
! types2-2.4 expected: [1 2 3 4]
! types2-2.4 got:      [4]
types2-2.5...
! types2-2.5 expected: [5 6 7 8]
! types2-2.5 got:      [5 6]
types2-2.6...
! types2-2.6 expected: [5 6 7 8]
! types2-2.6 got:      [5 6]
types2-2.7...
! types2-2.7 expected: [5 6 7 8]
! types2-2.7 got:      [5 6]
types2-2.8...
! types2-2.8 expected: [5 6 7 8]
! types2-2.8 got:      [5 6]
types2-2.9...
! types2-2.9 expected: [5 7]
! types2-2.9 got:      [7]
types2-2.10...
! types2-2.10 expected: [6 8]
! types2-2.10 got:      [8]
types2-2.11...
! types2-2.11 expected: [5 7]
! types2-2.11 got:      [7]
types2-2.12...
! types2-2.12 expected: [6 8]
! types2-2.12 got:      [8]
types2-2.10...
! types2-2.10 expected: [9 10]
! types2-2.10 got:      [3 4 7 8 9 10 11 12]
types2-2.11...
! types2-2.11 expected: [9 10]
! types2-2.11 got:      [3 4 7 8 9 10 11 12]
types2-2.12... Ok
types2-2.13... Ok
types2-3.1...
! types2-3.1 expected: [1 2 3 4]
! types2-3.1 got:      [1 2]
types2-3.2...
! types2-3.2 expected: [1 2 3 4]
! types2-3.2 got:      [1 2]
types2-3.3...
! types2-3.3 expected: [1 2 3 4]
! types2-3.3 got:      [1 2]
types2-3.4...
! types2-3.4 expected: [1 2 3 4]
! types2-3.4 got:      [1 2]
types2-3.1...
! types2-3.1 expected: [1 2 3 4]
! types2-3.1 got:      [1 2]
types2-3.2...
! types2-3.2 expected: [1 2 3 4]
! types2-3.2 got:      [1 2]
types2-3.3...
! types2-3.3 expected: [1 2 3 4]
! types2-3.3 got:      [1 2]
types2-3.4...
! types2-3.4 expected: [1 2 3 4]
! types2-3.4 got:      [1 2]
types2-3.1...
! types2-3.1 expected: [1 2 3 4]
! types2-3.1 got:      [1 2]
types2-3.2...
! types2-3.2 expected: [1 2 3 4 5 7]
! types2-3.2 got:      [1 2]
types2-3.3...
! types2-3.3 expected: [1 2 3 4]
! types2-3.3 got:      [1 2]
types2-3.4...
! types2-3.4 expected: [1 2 3 4 5 7]
! types2-3.4 got:      [1 2]
types2-3.1... Ok
types2-3.2... Ok
types2-3.3...
! types2-3.3 expected: [1 2 3 4 5 6 9 10]
! types2-3.3 got:      [1 2]
types2-3.3...
! types2-3.3 expected: [1 2 3 4 5 6 7 9 10]
! types2-3.3 got:      [1 2]
types2-4.1.1... Ok
types2-4.1.2... Ok
types2-4.1.3... Ok
types2-4.2.1... Ok
types2-4.2.2... Ok
types2-4.2.3... Ok
types2-4.3.1... Ok
types2-4.3.2... Ok
types2-4.3.3... Ok
types2-4.4.1... Ok
types2-4.4.2... Ok
types2-4.4.3... Ok
types2-4.5.1... Ok
types2-4.5.2... Ok
types2-4.5.3... Ok
types2-4.6.1... Ok
types2-4.6.2... Ok
types2-4.6.3... Ok
types2-4.7.1...
! types2-4.7.1 expected: [0]
! types2-4.7.1 got:      [1]
types2-4.7.2...
! types2-4.7.2 expected: []
! types2-4.7.2 got:      [1]
types2-4.7.3...
! types2-4.7.3 expected: [1]
! types2-4.7.3 got:      []
types2-4.8.1... Ok
types2-4.8.2... Ok
types2-4.8.3... Ok
types2-4.9.1... Ok
types2-4.9.2... Ok
types2-4.9.3... Ok
types2-4.10.1... Ok
types2-4.10.2... Ok
types2-4.10.3... Ok
types2-4.11.1...
! types2-4.11.1 expected: [0]
! types2-4.11.1 got:      [1]
types2-4.11.2...
! types2-4.11.2 expected: []
! types2-4.11.2 got:      [1]
types2-4.11.3...
! types2-4.11.3 expected: [1]
! types2-4.11.3 got:      []
types2-4.12.1... Ok
types2-4.12.2... Ok
types2-4.12.3... Ok
types2-4.13.1... Ok
types2-4.13.2... Ok
types2-4.13.3... Ok
types2-4.14.1... Ok
types2-4.14.2... Ok
types2-4.14.3... Ok
types2-4.15.1... Ok
types2-4.15.2... Ok
types2-4.15.3... Ok
types2-4.16.1... Ok
types2-4.16.2... Ok
types2-4.16.3... Ok
types2-4.17.1... Ok
types2-4.17.2... Ok
types2-4.17.3... Ok
types2-4.18.1... Ok
types2-4.18.2... Ok
types2-4.18.3... Ok
types2-4.19.1... Ok
types2-4.19.2... Ok
types2-4.19.3... Ok
types2-4.20.1... Ok
types2-4.20.2... Ok
types2-4.20.3... Ok
types2-4.21.1... Ok
types2-4.21.2... Ok
types2-4.21.3... Ok
types2-4.22.1... Ok
types2-4.22.2... Ok
types2-4.22.3... Ok
types2-4.23.1... Ok
types2-4.23.2... Ok
types2-4.23.3... Ok
types2-4.24.1... Ok
types2-4.24.2... Ok
types2-4.24.3... Ok
types2-4.25.1... Ok
types2-4.25.2... Ok
types2-4.25.3... Ok
types2-4.26.1... Ok
types2-4.26.2... Ok
types2-4.26.3... Ok
types2-4.27.1... Ok
types2-4.27.2... Ok
types2-4.27.3... Ok
types2-4.28.1... Ok
types2-4.28.2... Ok
types2-4.28.3... Ok
types2-5.1.1...
! types2-5.1.1 expected: [1]
! types2-5.1.1 got:      [0]
types2-5.1.2...
! types2-5.1.2 expected: [1]
! types2-5.1.2 got:      []
types2-5.1.3...
! types2-5.1.3 expected: []
! types2-5.1.3 got:      [1]
types2-5.2.1... Ok
types2-5.2.2... Ok
types2-5.2.3... Ok
types2-5.3.1... Ok
types2-5.3.2... Ok
types2-5.3.3... Ok
types2-5.4.1... Ok
types2-5.4.2... Ok
types2-5.4.3... Ok
types2-5.5.1... Ok
types2-5.5.2... Ok
types2-5.5.3... Ok
types2-5.6.1...
! types2-5.6.1 expected: [1]
! types2-5.6.1 got:      [0]
types2-5.6.2...
! types2-5.6.2 expected: [1]
! types2-5.6.2 got:      []
types2-5.6.3...
! types2-5.6.3 expected: []
! types2-5.6.3 got:      [1]
types2-5.7.1... Ok
types2-5.7.2... Ok
types2-5.7.3... Ok
types2-5.8.1... Ok
types2-5.8.2... Ok
types2-5.8.3... Ok
types2-5.9.1... Ok
types2-5.9.2... Ok
types2-5.9.3... Ok
types2-5.10.1... Ok
types2-5.10.2... Ok
types2-5.10.3... Ok
types2-5.11.1... Ok
types2-5.11.2... Ok
types2-5.11.3... Ok
types2-5.12.1... Ok
types2-5.12.2... Ok
types2-5.12.3... Ok
types2-5.13.1... Ok
types2-5.13.2... Ok
types2-5.13.3... Ok
types2-5.14.1...
! types2-5.14.1 expected: [1]
! types2-5.14.1 got:      [0]
types2-5.14.2...
! types2-5.14.2 expected: [1]
! types2-5.14.2 got:      []
types2-5.14.3...
! types2-5.14.3 expected: []
! types2-5.14.3 got:      [1]
types2-5.15.1...
! types2-5.15.1 expected: [1]
! types2-5.15.1 got:      [0]
types2-5.15.2...
! types2-5.15.2 expected: [1]
! types2-5.15.2 got:      []
types2-5.15.3...
! types2-5.15.3 expected: []
! types2-5.15.3 got:      [1]
types2-5.16.1... Ok
types2-5.16.2... Ok
types2-5.16.3... Ok
types2-5.17.1... Ok
types2-5.17.2... Ok
types2-5.17.3... Ok
types2-5.18.1... Ok
types2-5.18.2... Ok
types2-5.18.3... Ok
types2-5.19.1... Ok
types2-5.19.2... Ok
types2-5.19.3... Ok
types2-5.20.1... Ok
types2-5.20.2... Ok
types2-5.20.3... Ok
types2-5.21.1... Ok
types2-5.21.2... Ok
types2-5.21.3... Ok
types2-5.22.1... Ok
types2-5.22.2... Ok
types2-5.22.3... Ok
types2-5.23.1... Ok
types2-5.23.2... Ok
types2-5.23.3... Ok
types2-5.24.1... Ok
types2-5.24.2... Ok
types2-5.24.3... Ok
types2-5.25.1... Ok
types2-5.25.2... Ok
types2-5.25.3... Ok
types2-5.26.1... Ok
types2-5.26.2... Ok
types2-5.26.3... Ok
types2-5.27.1... Ok
types2-5.27.2... Ok
types2-5.27.3... Ok
types2-5.30.1... Ok
types2-5.30.2... Ok
types2-5.30.3... Ok
types2-5.31.1... Ok
types2-5.31.2... Ok
types2-5.31.3... Ok
types2-5.32.1... Ok
types2-5.32.2... Ok
types2-5.32.3... Ok
types2-5.33.1... Ok
types2-5.33.2... Ok
types2-5.33.3... Ok
types2-5.34.1... Ok
types2-5.34.2... Ok
types2-5.34.3... Ok
types2-5.35.1... Ok
types2-5.35.2... Ok
types2-5.35.3... Ok
types2-5.36.1... Ok
types2-5.36.2... Ok
types2-5.36.3... Ok
types2-5.37.1... Ok
types2-5.37.2... Ok
types2-5.37.3... Ok
types2-5.40.1...
! types2-5.40.1 expected: [1]
! types2-5.40.1 got:      [0]
types2-5.40.2...
! types2-5.40.2 expected: [1]
! types2-5.40.2 got:      []
types2-5.40.3...
! types2-5.40.3 expected: []
! types2-5.40.3 got:      [1]
types2-5.41.1... Ok
types2-5.41.2... Ok
types2-5.41.3... Ok
types2-5.42.1...
! types2-5.42.1 expected: [1]
! types2-5.42.1 got:      [0]
types2-5.42.2...
! types2-5.42.2 expected: [1]
! types2-5.42.2 got:      []
types2-5.42.3...
! types2-5.42.3 expected: []
! types2-5.42.3 got:      [1]
types2-5.43.1...
! types2-5.43.1 expected: [1]
! types2-5.43.1 got:      [0]
types2-5.43.2...
! types2-5.43.2 expected: [1]
! types2-5.43.2 got:      []
types2-5.43.3...
! types2-5.43.3 expected: []
! types2-5.43.3 got:      [1]
types2-6.1... Ok
types2-6.2... Ok
types2-6.3...
! types2-6.3 expected: [1 3 9 11]
! types2-6.3 got:      [1 3]
types2-6.4...
! types2-6.4 expected: [6 8 10 12]
! types2-6.4 got:      []
types2-6.5...
! types2-6.5 expected: [1 2 3 4 9 10 11 12]
! types2-6.5 got:      [9 10 11 12]
types2-6.6... Ok
types2-6.7...
! types2-6.7 expected: [1 2 3 4 9 10 11 12]
! types2-6.7 got:      [9 10 11]
types2-6.8...
! types2-6.8 expected: [5 6 7 8 9 10 11 12]
! types2-6.8 got:      [5 6 7 9 10 11]
types2-6.9... Ok
types2-7.1.1... Ok
types2-7.1.2... Ok
types2-7.1.3... Ok
types2-7.2.1...
! types2-7.2.1 expected: [1]
! types2-7.2.1 got:      [0]
types2-7.2.2...
! types2-7.2.2 expected: [1]
! types2-7.2.2 got:      []
types2-7.2.3...
! types2-7.2.3 expected: []
! types2-7.2.3 got:      [1]
types2-7.3.1...
! types2-7.3.1 expected: [1]
! types2-7.3.1 got:      [0]
types2-7.3.2...
! types2-7.3.2 expected: [1]
! types2-7.3.2 got:      []
types2-7.3.3...
! types2-7.3.3 expected: []
! types2-7.3.3 got:      [1]
types2-7.4.1...
! types2-7.4.1 expected: [1]
! types2-7.4.1 got:      [0]
types2-7.4.2...
! types2-7.4.2 expected: [1]
! types2-7.4.2 got:      []
types2-7.4.3...
! types2-7.4.3 expected: []
! types2-7.4.3 got:      [1]
types2-7.5.1...
! types2-7.5.1 expected: [1]
! types2-7.5.1 got:      [0]
types2-7.5.2...
! types2-7.5.2 expected: [1]
! types2-7.5.2 got:      []
types2-7.5.3...
! types2-7.5.3 expected: []
! types2-7.5.3 got:      [1]
types2-7.6.1... Ok
types2-7.6.2... Ok
types2-7.6.3... Ok
types2-7.7.1...
! types2-7.7.1 expected: [1]
! types2-7.7.1 got:      [0]
types2-7.7.2...
! types2-7.7.2 expected: [1]
! types2-7.7.2 got:      []
types2-7.7.3...
! types2-7.7.3 expected: []
! types2-7.7.3 got:      [1]
types2-7.8.1...
! types2-7.8.1 expected: [1]
! types2-7.8.1 got:      [0]
types2-7.8.2...
! types2-7.8.2 expected: [1]
! types2-7.8.2 got:      []
types2-7.8.3...
! types2-7.8.3 expected: []
! types2-7.8.3 got:      [1]
types2-7.9.1...
! types2-7.9.1 expected: [1]
! types2-7.9.1 got:      [0]
types2-7.9.2...
! types2-7.9.2 expected: [1]
! types2-7.9.2 got:      []
types2-7.9.3...
! types2-7.9.3 expected: []
! types2-7.9.3 got:      [1]
types2-7.10.1...
! types2-7.10.1 expected: [1]
! types2-7.10.1 got:      [0]
types2-7.10.2...
! types2-7.10.2 expected: [1]
! types2-7.10.2 got:      []
types2-7.10.3...
! types2-7.10.3 expected: []
! types2-7.10.3 got:      [1]
types2-7.6.1... Ok
types2-7.6.2... Ok
types2-7.6.3... Ok
types2-7.7.1... Ok
types2-7.7.2... Ok
types2-7.7.3... Ok
types2-7.8.1...
! types2-7.8.1 expected: [1]
! types2-7.8.1 got:      [0]
types2-7.8.2...
! types2-7.8.2 expected: [1]
! types2-7.8.2 got:      []
types2-7.8.3...
! types2-7.8.3 expected: []
! types2-7.8.3 got:      [1]
types2-7.9.1...
! types2-7.9.1 expected: [1]
! types2-7.9.1 got:      [0]
types2-7.9.2...
! types2-7.9.2 expected: [1]
! types2-7.9.2 got:      []
types2-7.9.3...
! types2-7.9.3 expected: []
! types2-7.9.3 got:      [1]
types2-7.10.1... Ok
types2-7.10.2... Ok
types2-7.10.3... Ok
types2-7.11.1... Ok
types2-7.11.2... Ok
types2-7.11.3... Ok
types2-7.12.1... Ok
types2-7.12.2... Ok
types2-7.12.3... Ok
types2-7.13.1... Ok
types2-7.13.2... Ok
types2-7.13.3... Ok
types2-7.14.1... Ok
types2-7.14.2... Ok
types2-7.14.3... Ok
types2-7.15.1... Ok
types2-7.15.2... Ok
types2-7.15.3... Ok
types2-8.1...
! types2-8.1 expected: [1 2 3 4]
! types2-8.1 got:      [1 2 3]
types2-8.2...
! types2-8.2 expected: [1 2 3 4]
! types2-8.2 got:      [1 3]
types2-8.3...
! types2-8.3 expected: [1 2 3 4]
! types2-8.3 got:      []
types2-8.4...
! types2-8.4 expected: [1 2 3 4]
! types2-8.4 got:      [1]
types2-8.5...
! types2-8.5 expected: [5 6 7 8]
! types2-8.5 got:      []
types2-8.6...
! types2-8.6 expected: [5 6 7 8]
! types2-8.6 got:      []
types2-8.7... Ok
types2-8.8... Ok
types2-8.9...
! types2-8.9 expected: [9 10 11 12]
! types2-8.9 got:      [9 10 11]
types2-8.6...
! types2-8.6 expected: [9 10 11 12]
! types2-8.6 got:      [9 11]
types2-8.7... Ok
types2-8.8...
! types2-8.8 expected: [9 10]
! types2-8.8 got:      [9]
Running "types2"

Error in types2.test: couldn't read file "types2": no such file or directory
couldn't read file "types2": no such file or directory
    while executing
"source types2"
    ("uplevel" body line 1)
    invoked from within
"uplevel #0 source $extra"
    (procedure "finish_test" line 14)
    invoked from within
"finish_test"
    (file "/tank/repos/rustql-architecture/sqlite3/test/types2.test" line 340)
    invoked from within
"source $test_file"

==========================================
Test: types2
Time: 0s
Status: FAILED
