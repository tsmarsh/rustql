# Fix round() function formatting and argument validation

## Problem
The `round()` function has two main issues:
1. Returns integer format (e.g., `0`) instead of float format (e.g., `0.0`) for whole numbers
2. Does not properly validate argument count (should error on 0 or 3+ arguments)

## Required Changes

### 1. Float Formatting
`round()` must always return a REAL type with decimal formatting:
- `round(2)` should return `2.0` not `2`
- `round(-5)` should return `-5.0` not `-5`

### 2. Argument Validation
- `round()` with 0 arguments → error: "wrong number of arguments to function round()"
- `round(a,b,c)` with 3+ arguments → error: "wrong number of arguments to function round()"
- `round(x)` with 1 argument → OK (rounds to integer, returns as float)
- `round(x,n)` with 2 arguments → OK (rounds to n decimal places)

## File to Modify
`src/functions/scalar.rs` - `func_round()` function

## TCL Tests That Must Pass
```
func-4.5   - round() with no args should error
func-4.6   - round(b,2) should return -2.0, 1.23, 2.0
func-4.7   - round(b,0) should return 2.0, 1.0, -2.0
func-4.8   - round(c) should return 3.0, -12346.0, -5.0
func-4.9   - round(c,a) should return 3.0, -12345.68, -5.0
func-4.10  - concatenation with round() should show .0 suffix
func-4.11  - round() with no args should error
func-4.12  - coalesce(round(a,2),'nil') should return 1.0, nil, 345.0, nil, 67890.0
func-4.16  - round(b,2.0) should work same as round(b,2)
func-4.17.* (998 tests) - round() with various values must return .0 suffix
func-4.18.* (998 tests) - round() with precision must be accurate
func-4.20-4.40 - edge cases for rounding
```

## Verification
```bash
make test-func 2>&1 | grep "^func-4\." | grep -c "Ok$"
# Should show significant improvement from current 6/998
```
