# Implement REGEXP and MATCH Operators with User-Defined Functions

The REGEXP and MATCH operators in SQLite require user-defined functions to be registered.

## Current Behavior

Tests in like.test register custom functions:
```tcl
db function regexp -argcount 2 test_regexp
db function match -argcount 2 test_match
```

Then use them:
```sql
SELECT x FROM t1 WHERE x REGEXP 'abc';
SELECT x FROM t1 WHERE x MATCH '*abc*';
```

Expected: Functions are called, results returned
Got: Empty results or errors

## Required Implementation

1. Support `db function` TCL command to register user-defined functions
2. REGEXP operator should call the registered `regexp` function
3. MATCH operator should call the registered `match` function
4. If no function is registered, return appropriate error

## Affected Tests

- like-2.1 through like-2.4

## Files to Modify

- TCL extension: support `db function` command for registering functions
- `src/func.rs` - user-defined function registration
- `src/vdbe.rs` - call registered functions for REGEXP/MATCH

## Note

OP_Regexp is already implemented (moth zq0ko). This issue is about the function registration and invocation infrastructure in the TCL test extension.
