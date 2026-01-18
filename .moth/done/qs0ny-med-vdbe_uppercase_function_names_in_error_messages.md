# VDBE: Uppercase function names in error messages

## Problem
Error messages use lowercase function names, but SQLite uses uppercase.

## Test Failures
```
! select1-2.9 expected: [1 {wrong number of arguments to function MAX()}]
! select1-2.9 got:      [1 {wrong number of arguments to function max()}]
```

## Required Changes
1. Store canonical (uppercase) function names
2. Use uppercase in error message formatting
3. Apply to all built-in functions: MAX, MIN, SUM, COUNT, AVG, etc.

## Files
- `src/functions/` - Function registration
- `src/vdbe/engine.rs` - Error message generation
