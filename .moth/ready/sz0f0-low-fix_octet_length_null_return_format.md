# Fix octet_length() NULL return format

## Problem
`octet_length(NULL)` returns empty string instead of the literal string "NULL".

## File to Modify
`src/functions/scalar.rs` - `func_octet_length()` function

## TCL Tests That Must Pass
```
func-1.6   - SELECT octet_length(NULL) should return NULL
```
