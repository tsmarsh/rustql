# Fix char() and unicode() functions

## Problem
The `char()` function builds a string from Unicode code points.
The `unicode()` function returns the Unicode code point of the first character.

## File to Modify
`src/functions/scalar.rs` - `func_char()` and `func_unicode()` functions

## TCL Tests That Must Pass
```
func-27.* - char() function tests
```
