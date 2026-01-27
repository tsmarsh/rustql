# Problem
Transaction/savepoint/vacuum tests fail with constraint, locking, and missing
checksum behaviors, indicating engine-level state management gaps.

# Scope
- Transaction state transitions and lock handling.
- Savepoint semantics and error propagation.
- VACUUM checksum/harness integration (md5/cksum variables).

# Acceptance Criteria
- trans/savepoint/vacuum pass for these representative cases:
  trans-1.2.4, trans-1.9, trans-1.10, trans-2.1b, trans-2.2, trans-2.4,
  savepoint-1.4.1, savepoint-1.4.3, savepoint-1.4.5, savepoint-1.4.7,
  savepoint-2.2, savepoint-2.3, vacuum-1.1b.
- Errors no longer include "database is locked", "cannot commit - no transaction
  is active", or missing md5/cksum errors during vacuum.

# Repro
`testfixture test/trans.test`
`testfixture test/savepoint.test`
`testfixture test/vacuum.test`

# Observed Errors
- Error: database is locked
- Error: cannot commit - no transaction is active
- Error: invalid command name "md5"
- Error in vacuum.test: can't read "cksum": no such variable
