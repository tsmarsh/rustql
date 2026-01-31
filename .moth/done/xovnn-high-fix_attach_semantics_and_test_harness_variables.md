# Problem
attach.test fails on missing attached tables and missing harness variables
(SQLITE_MAX_ATTACHED), indicating attach schema plumbing and harness config gaps.

# Scope
- Attach database schema registration and name resolution.
- Populate SQLITE_MAX_ATTACHED variable for test harness.
- Error message alignment for attach name conflicts.

# Acceptance Criteria
- attach.test passes for representative cases:
  attach-1.3.1..attach-1.3.5, attach-1.7.
- No "no such table: t2" errors for attached db cases.
- SQLITE_MAX_ATTACHED is defined for attach-1.19 branch.

# Repro
`testfixture test/attach.test`

# Observed Errors
- Error: no such table: t2
- Error in attach.test: can't read "SQLITE_MAX_ATTACHED": no such variable
