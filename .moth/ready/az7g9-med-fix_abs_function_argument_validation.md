# Fix abs() function argument validation

## Problem
The `abs()` function does not properly validate argument count and has formatting issues:
1. Does not error on wrong number of arguments
2. Returns 0 instead of 0.0 for text input that parses to 0

## Required Changes

### 1. Argument Validation
- `abs()` with 0 arguments → error: "wrong number of arguments to function abs()"
- `abs(a,b)` with 2+ arguments → error: "wrong number of arguments to function abs()"

### 2. Text Input Handling
When input is text that evaluates to 0.0 (like "this", "program"), should return `0.0` not `0`

## File to Modify
`src/functions/scalar.rs` - `func_abs()` function

## TCL Tests That Must Pass
```
func-4.1   - abs(a,b) should error "wrong number of arguments to function abs()"
func-4.2   - abs() should error "wrong number of arguments to function abs()"
func-4.4.2 - abs(t1) from tbl1 should return 0.0, 0.0, 0.0, 0.0, 0.0
```

## Verification
```bash
make test-func 2>&1 | grep "^func-4\.[12]"
# Both should show Ok
```
