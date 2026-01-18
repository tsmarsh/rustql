# VDBE: Validate ORDER BY column index range

## Problem
ORDER BY with numeric index doesn't validate the index is within result column range.

## Test Failures
```
! select1-4.10.1 expected: [1 {1st ORDER BY term out of range - should be between 1 and 2}]
! select1-4.10.1 got:      [0 {1 10 2 9}]
```

## SQLite Behavior
```sql
SELECT a, b FROM t1 ORDER BY 5;
-- Error: 1st ORDER BY term out of range - should be between 1 and 2
```

## Required Changes
1. During ORDER BY compilation, check numeric indices
2. Validate index is between 1 and column count
3. Return descriptive error with actual range

## Files
- `src/executor/select.rs` - ORDER BY handling
- `src/executor/prepare.rs` - Query compilation
