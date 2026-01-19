# VDBE: Implement PRAGMA integrity_check

## Problem
`PRAGMA integrity_check` verifies database file integrity but is not implemented.

## SQLite Behavior
```sql
PRAGMA integrity_check;
-- Returns "ok" or list of errors
```

## Required Changes
1. Implement btree page chain verification
2. Check freelist consistency
3. Verify index entries match table data
4. Return descriptive error messages

## Tests
- `sqlite3/test/pragma.test`
- `sqlite3/test/corrupt*.test`
