# Fix trim(), ltrim(), rtrim() edge cases

## Problem
The trim functions may have edge cases with multi-character trim patterns.

## File to Modify
`src/functions/scalar.rs` - `func_trim()`, `func_ltrim()`, `func_rtrim()` functions

## TCL Tests That Must Pass
```
func-19.* - Various trim() tests
func-20.* - Various trim() tests
```

## Verification
```bash
make test-func 2>&1 | grep "^func-19\."
make test-func 2>&1 | grep "^func-20\."
# All should show Ok
```
