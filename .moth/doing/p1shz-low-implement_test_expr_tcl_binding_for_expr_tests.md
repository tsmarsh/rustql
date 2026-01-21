# Fix ROLLBACK for Memory Databases

## Original Problem

Many expression tests use a `test_expr` command that was thought to be missing.

## Actual Problem

The `test_expr` TCL proc is defined in expr.test and works fine. The real issue is that **ROLLBACK doesn't properly restore data in memory databases**.

## Failing Behavior

```
Before BEGIN: [["1", "2"]]
In transaction: [["1", "2"]]
After UPDATE: [["10", "20"]]
After ROLLBACK: []  <-- Should be [["1", "2"]]
```

The ROLLBACK is clearing all data instead of restoring to pre-BEGIN state.

## Root Cause Investigation

The pager's `playback_journal()` for memory databases:
1. Takes mem_journal records
2. Iterates through and attempts to restore pages
3. Uses `pcache.fetch(pgno, false)` which may return None if page not in cache

The issue is likely that pages aren't being properly restored during rollback playback.

## Files to Fix

- `src/storage/pager.rs` - `playback_journal()` function (line 1282)
- Memory journal handling may need to create pages if they don't exist in cache

## Test Impact

This affects all tests that use BEGIN/ROLLBACK pattern:
- expr.test (most tests fail)
- Many other transaction-based tests
