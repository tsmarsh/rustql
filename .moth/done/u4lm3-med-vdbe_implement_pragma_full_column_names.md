# VDBE: Implement PRAGMA full_column_names

## Problem
`PRAGMA full_column_names` controls whether result columns include table.column format.

## SQLite Behavior
```sql
PRAGMA full_column_names=ON;
SELECT x FROM t1;
-- Column name: "t1.x"
```

## Required Changes
1. Add `full_column_names` flag to connection (default OFF)
2. Modify column name generation
3. Full names override short names when both set

## Tests
- `sqlite3/test/select1.test` (select1-6.*)
