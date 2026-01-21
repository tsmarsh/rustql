# Fix ROLLBACK for Memory Databases

## Original Problem

Many expression tests use a `test_expr` command that was thought to be missing.

## Actual Problem

The `test_expr` TCL proc is defined in expr.test and works fine. The real issue is that **ROLLBACK doesn't properly restore data in memory databases**.

## Failing Behavior (Before Fix)

```
Before BEGIN: [["1", "2"]]
In transaction: [["1", "2"]]
After UPDATE: [["10", "20"]]
After ROLLBACK: []  <-- Should be [["1", "2"]]
```

## Root Cause

In `src/storage/pager.rs`, the `playback_journal()` function was iterating through journal records in **reverse** order when restoring pages. The comment said "we want to restore the earliest (original) version" but the code did the opposite:

- Journal records: [(page2, original_data), (page2, modified_data)]
- Reverse iteration gets: modified_data first
- HashSet tracks restored pages, so original_data is skipped
- Result: modified data is restored instead of original!

## Fix

Changed `playback_journal()` to iterate in **forward** order instead of reverse:

```rust
// BEFORE (buggy):
for (pgno, original_data) in journal_records.into_iter().rev() {

// AFTER (fixed):
for (pgno, original_data) in journal_records.into_iter() {
```

The first journal entry for each page contains the original data (saved before any modifications), so forward iteration correctly restores the original state.

## After Fix

```
Before BEGIN: [["1", "2"]]
After UPDATE: [["10", "20"]]
After ROLLBACK: [["1", "2"]]  <-- Correctly restored!
```

## Files Changed

- `src/storage/pager.rs` - `playback_journal()` function
- `tests/where_debug.rs` - Added regression test with assertions
