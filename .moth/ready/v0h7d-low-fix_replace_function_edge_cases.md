# Fix replace() function edge cases

## Problem
The `replace()` function may have edge cases with empty strings or NULL handling.

## File to Modify
`src/functions/scalar.rs` - `func_replace()` function

## TCL Tests That Must Pass
```
func-18.* - Various replace() tests
```
