# Fix Subquery in FROM Clause Join Behavior

## Problem

When a subquery is used in the FROM clause, it produces wrong row counts - often a Cartesian product instead of the expected result.

## Failing Tests

```
subquery-1.5 expected: [1 1 3 3 5 5 7 7]
subquery-1.5 got:      [1 1 1 3 1 5 1 7 3 1 3 3 3 5 3 7 5 1 5 3 5 5 5 7 7 1 7 3 7 5 7 7]

subquery-1.6 expected: [1 1 3 3 5 5 7 7]
subquery-1.6 got:      [1 1 1 3 1 5 1 7 3 1 3 3 3 5 3 7 5 1 5 3 5 5 5 7 7 1 7 3 7 5 7 7]

select6-1.8 expected: [1 1 1 2 2 3 3 4 7 4 8 15 5 5 20]
select6-1.8 got:      [1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 2 2 2 ... (many more rows)]
```

## Analysis

Expected 4 or 8 rows but getting 16 or 60+ rows suggests:
- The subquery is being joined as a Cartesian product with the outer table
- Or the subquery cursor is not being properly reset/managed between iterations

## Files to Investigate

- `src/executor/select.rs` - FROM clause subquery compilation
- `src/vdbe/engine.rs` - Cursor management for subquery tables
