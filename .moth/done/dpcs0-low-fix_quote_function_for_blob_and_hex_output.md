# Fix quote() function for blob and hex output

## Problem
The `quote()` function may not format blob output correctly as X'...' hex strings.

## File to Modify
`src/functions/scalar.rs` - `func_quote()` function

## TCL Tests That Must Pass
```
func-10.1  - quote(NULL) should return NULL
func-10.2  - quote('') should return ''
func-10.3  - quote('abc') should return 'abc'
func-10.4  - quote(x'1234') should return X'1234'
```
