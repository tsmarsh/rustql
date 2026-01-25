# Fix substr() edge cases with large numbers

## Problem
The `substr()` function has issues with edge cases:
1. Incorrect handling of negative start positions near string boundaries
2. Incorrect handling of very large (0x100000000+) position arguments
3. Incorrect handling of very large length arguments

## Required Changes
- `substr('abcdefg',0x100000001,2)` should return empty string
- `substr('abcdefg',1,0x100000002)` should return full string 'abcdefg'
- `substr(x'313233343536373839',0x7ffffffffffffffe,5)` should return X''
- `substr(t1,-4,2)` where t1='is' should return empty (string too short)

## File to Modify
`src/functions/scalar.rs` - `func_substr()` function

## TCL Tests That Must Pass
```
func-2.7   - substr(t1,-4,2) should return fr {} gr wa th (empty for short strings)
func-2.11  - substr('abcdefg',0x100000001,2) should return {}
func-2.12  - substr('abcdefg',1,0x100000002) should return abcdefg
func-2.13  - substr(x'...',0x7ffffffffffffffe,5) should return X''
```
