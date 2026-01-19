# VDBE: Detect ambiguous column references in queries

## Problem
Queries with ambiguous column names (same column in multiple tables) don't return errors.

## Test Failures
```
! select1-6.8 expected: [1 {ambiguous column name: f1}]
! select1-6.8 got:      [0 {}]
```

## SQLite Behavior
```sql
SELECT f1 FROM t1, t2;  -- Both have f1
-- Error: ambiguous column name: f1
```

## Required Changes
1. During name resolution, track all matching columns
2. If count > 1 and no table qualifier, return error
3. Provide helpful error message with column name

## Files
- `src/executor/prepare.rs` - Name resolution
- `src/executor/select.rs` - Column binding
