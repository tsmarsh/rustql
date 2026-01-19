# VDBE: Detect misuse of aliased aggregates

## Problem
Using an aliased aggregate in a context where aggregates aren't allowed should error.

## Test Failures
```
! select1-2.21 expected: [1 {misuse of aliased aggregate m}]
! select1-2.21 got:      [0 {}]
```

## SQLite Behavior
```sql
SELECT max(a) AS m, b FROM t1 WHERE m > 5;
-- Error: misuse of aliased aggregate m
```

## Required Changes
1. Track aggregate aliases during compilation
2. Detect references to aggregates in WHERE/JOIN clauses
3. Return specific error message

## Files
- `src/executor/select.rs` - Aggregate handling
- `src/executor/wherecode.rs` - WHERE clause compilation
