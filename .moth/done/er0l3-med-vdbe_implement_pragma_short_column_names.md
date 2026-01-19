# VDBE: Implement PRAGMA short_column_names

## Problem
`PRAGMA short_column_names` controls whether result columns use short names (column only) or include table prefix.

## SQLite Behavior
```sql
PRAGMA short_column_names=ON;  -- Default
SELECT t1.x FROM t1;
-- Column name: "x"

PRAGMA short_column_names=OFF;
SELECT t1.x FROM t1;
-- Column name: "t1.x"
```

## Required Changes
1. Add `short_column_names` flag to connection (default ON)
2. Modify column name generation in executor
3. Interact correctly with full_column_names pragma

## Tests
- `sqlite3/test/select1.test` (select1-6.9.*)
