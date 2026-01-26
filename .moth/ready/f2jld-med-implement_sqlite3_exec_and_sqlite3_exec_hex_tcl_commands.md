# Implement sqlite3_exec and sqlite3_exec_hex TCL Commands

These TCL commands are needed for the like.test and other tests.

## Missing Commands

### sqlite3_exec
Used in like.test:
```tcl
sqlite3_exec db {PRAGMA case_sensitive_like=on}
```
Should execute SQL and return result code + results.

### sqlite3_exec_hex
Used in like.test for Unicode/blob testing:
```tcl
sqlite3_exec_hex db {SELECT x FROM t1 WHERE x LIKE '%c3%'}
```
Executes SQL with hex-encoded bytes in the query.

## Current Behavior

```
Error: invalid command name "sqlite3_exec"
Error: invalid command name "sqlite3_exec_hex"
```

## Required Implementation

In `src/tcl_ext.rs`:

1. `sqlite3_exec db sql` - Execute SQL, return {result_code result_list}
2. `sqlite3_exec_hex db sql` - Decode hex escapes in SQL, then execute

## Affected Tests

- like-1.5.1 (sqlite3_exec)
- like-9.3.x through like-9.5.x (sqlite3_exec_hex)

## Files to Modify

- `src/tcl_ext.rs` - Add the new commands
