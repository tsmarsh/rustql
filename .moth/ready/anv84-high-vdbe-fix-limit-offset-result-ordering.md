# VDBE: Fix LIMIT/OFFSET result ordering

## Problem
LIMIT and OFFSET produce incorrect results - appears to return wrong subset of rows or in wrong order.

## Test Failures
```
! limit-1.2.1 expected: [0 1 2 3 4]
! limit-1.2.1 got:      [27 28 29 30 31]

! limit-1.2.2 expected: [2 3 4 5 6]
! limit-1.2.2 got:      [25 26 27 28 29]
```

## SQLite Reference
- `sqlite3/src/vdbe.c`: OP_IfSmaller, OP_Limit, OP_Offset handling
- `sqlite3/src/select.c`: LIMIT/OFFSET code generation

## RustQL Location
- `src/vdbe/engine.rs`: Limit-related opcodes
- `src/executor/select.rs`: LIMIT/OFFSET compilation

## Required Changes
1. Verify LIMIT counter is decremented correctly
2. Check OFFSET skips correct number of rows
3. Ensure ResultRow respects limits
4. Test with ORDER BY + LIMIT combinations

## Tests
- `sqlite3/test/limit.test`
- `sqlite3/test/select4.test`
