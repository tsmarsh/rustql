# Fix Float Formatting to Match SQLite (3.0 vs 3)

## Problem

Float values that are whole numbers should be displayed with `.0` suffix in SQLite format, but we're displaying them as integers.

## Failing Tests

```
select3-2.3.1 expected: [0 1.0 1 2.0 2 3.5 3 6.5 4 12.5 5 24.0]
select3-2.3.1 got:      [0 1 1 2 2 3.5 3 6.5 4 12.5 5 24]

select6-3.5 expected: [4.0 3.0 7.0]
select6-3.5 got:      [4 4 8]

select3-1.2 expected: [1 0 31 5 496 124 16.0 4.0]
select3-1.2 got:      [1 0 31 5 496 124 16 4]
```

## SQLite Behavior

SQLite displays float values with decimal point even when they're whole numbers:
- `1.0` not `1`
- `16.0` not `16`

This distinguishes float types from integer types in output.

## Files to Investigate

- `src/types/mod.rs` or `src/vdbe/mem.rs` - Value to string conversion
- `src/api/stmt.rs` - sqlite3_column_text implementation
