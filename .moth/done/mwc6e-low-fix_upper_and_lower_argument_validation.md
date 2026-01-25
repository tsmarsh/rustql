# Fix upper() and lower() argument validation

## Problem
The `upper()` and `lower()` functions do not properly validate argument count.

## Required Changes
- `upper(a,5)` → error: "wrong number of arguments to function upper()"
- `upper(*)` → error: "wrong number of arguments to function upper()"
- Same for `lower()`

## File to Modify
`src/functions/scalar.rs` - `func_upper()` and `func_lower()` functions

## TCL Tests That Must Pass
```
func-5.4   - upper(a,5) should error "wrong number of arguments to function upper()"
func-5.5   - upper(*) should error "wrong number of arguments to function upper()"
```

## Verification
```bash
make test-func 2>&1 | grep "^func-5\."
# All should show Ok
```
