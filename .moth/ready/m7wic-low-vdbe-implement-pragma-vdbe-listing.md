# VDBE: Implement PRAGMA vdbe_listing

## Problem
`PRAGMA vdbe_listing` should print bytecode to stdout during prepare. Used for debugging.

## SQLite Behavior
```sql
PRAGMA vdbe_listing=ON;
SELECT * FROM t1;
-- Prints bytecode to stdout
```

## Required Changes
1. Add vdbe_listing flag to connection
2. During prepare, if enabled, print opcodes
3. Format similar to EXPLAIN output
