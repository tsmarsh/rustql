# VDBE: Implement PRAGMA count_changes

## Problem
`PRAGMA count_changes` is not implemented. When enabled, INSERT/UPDATE/DELETE should return the number of affected rows.

## SQLite Behavior
```sql
PRAGMA count_changes=ON;
DELETE FROM t1 WHERE x=5;
-- Returns: 3 (if 3 rows deleted)
```

## Required Changes
1. Add `count_changes` flag to connection state
2. Modify ResultRow to return change count when enabled
3. Track changes in INSERT/UPDATE/DELETE opcodes

## Tests
- `sqlite3/test/pragma.test`
- `sqlite3/test/delete.test`
