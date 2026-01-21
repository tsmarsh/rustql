# Fix ORDER BY Validation and PRAGMA empty_result_callbacks

## Problem

Several tests fail due to missing ORDER BY validation and a missing PRAGMA.

## Failing Tests (5 total)

### ORDER BY validation missing (should error)
```
select1-6.11 expected: [1 {1st ORDER BY term does not match any column in the result set}]
select1-6.11 got:      [0 {f1 11 f1 33 f1 122 f1 144}]
```
When ORDER BY references a column number that doesn't exist in the result set, SQLite errors. We execute the query instead.

### ORDER BY with LIMIT in UNION returning wrong results
```
select1-6.23 expected: [b d]
select1-6.23 got:      [a b]
```
This appears to be a UNION with ORDER BY and LIMIT issue.

### Missing PRAGMA empty_result_callbacks
```
select1-9.2: Error: unknown pragma: empty_result_callbacks

select1-9.3 expected: [f1 f2]
select1-9.3 got:      []

select1-9.5 expected: [f1 f2]
select1-9.5 got:      []
```
This PRAGMA controls whether column names are returned for queries with zero rows.

## SQLite Behavior Reference

```sql
-- ORDER BY validation
SELECT f1, f2 FROM test1 ORDER BY 3;
-- Error: 1st ORDER BY term does not match any column in the result set

-- empty_result_callbacks controls column name return for empty results
PRAGMA empty_result_callbacks=ON;
SELECT * FROM test1 WHERE 0;
-- Returns column names even though no rows match
```

## Required Changes

1. **ORDER BY column number validation**: Check that numeric ORDER BY references (ORDER BY 1, ORDER BY 2, etc.) don't exceed the number of result columns

2. **PRAGMA empty_result_callbacks**: Implement this PRAGMA to control whether column metadata is returned for empty result sets

3. **Investigate select1-6.23**: The UNION ORDER BY LIMIT logic may have edge cases

## Files to Investigate

- `src/executor/select.rs` - ORDER BY validation, compile_order_by
- `src/vdbe/engine.rs` - PRAGMA handling
- `src/api/stmt.rs` - Column metadata for empty results
