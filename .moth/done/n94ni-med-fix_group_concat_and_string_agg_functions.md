# Fix group_concat() and string_agg() functions

## Problem
The `group_concat()` and `string_agg()` aggregate functions may have issues with:
1. Custom separators
2. NULL handling
3. Order of concatenation

## File to Modify
`src/functions/aggregate.rs` - group_concat implementation

## TCL Tests That Must Pass
```
func-21.* - Various group_concat() tests with separators
func-22.* - group_concat() ordering tests
func-23.* - NULL handling in group_concat()
```
