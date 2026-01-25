# Fix length() function argument validation and GROUP BY

## Problem
The `length()` function has issues:
1. Does not error on wrong number of arguments (0 or 2+)
2. Returns empty/null instead of proper values in GROUP BY context

## Required Changes

### 1. Argument Validation
- `length(*)` → error: "wrong number of arguments to function length()"
- `length(t1,5)` → error: "wrong number of arguments to function length()"

### 2. GROUP BY Support
`SELECT length(t1), count(*) FROM tbl1 GROUP BY length(t1)` should work correctly

## File to Modify
`src/functions/scalar.rs` - `func_length()` function

## TCL Tests That Must Pass
```
func-1.1   - length(*) should error "wrong number of arguments to function length()"
func-1.2   - length(t1,5) should error "wrong number of arguments to function length()"
func-1.3   - GROUP BY length(t1) should return 2 1 4 2 7 1 8 1
```

## Verification
```bash
make test-func 2>&1 | grep "^func-1\.[123]"
# All should show Ok
```
