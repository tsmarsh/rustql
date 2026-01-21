# Fix Correlated Scalar Subquery Evaluation

## Problem

Correlated scalar subqueries return wrong values or NULL instead of the expected correlated result.

## Failing Tests

```
subquery-1.1 expected: [1 1 3 9 5 25]
subquery-1.1 got:      [1 49 3 49 5 49]

subquery-1.2 expected: [1 3 3 13 5 31 7 57]
subquery-1.2 got:      [1 {} 3 {} 5 {} 7 {}]

subquery-1.10.1 expected: [1 3 3 13 5 31 7 57]
subquery-1.10.1 got:      [1 {} 3 {} 5 {} 7 {}]
```

## Analysis

1. **subquery-1.1**: The scalar subquery `(SELECT x*x FROM t1 WHERE x=a)` should return the square of the correlated column `a`, but returns 49 (7*7, the last value) for all rows.

2. **subquery-1.2**: Correlated subqueries returning NULL instead of computed values.

The issue is likely that:
- Correlated subqueries are being evaluated once (with the last outer row) instead of per-row
- Or the correlation binding isn't being passed correctly to the inner query

## Files to Investigate

- `src/executor/select.rs` - Scalar subquery compilation
- `src/vdbe/engine.rs` - Subquery execution and correlation handling
