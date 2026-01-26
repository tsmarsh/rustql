# Implement LIKE Index Optimization for Prefix Patterns

SQLite has a critical optimization where `WHERE x LIKE 'prefix%'` queries can use an index on column `x` instead of doing a full table scan.

## Current Behavior

Tests show:
```
Expected: [abc abcd nosort {} i1]  (uses index i1, no sort needed)
Got:      [ABC abc abcd sort t1 *] (full table scan, needs sort)
```

## Required Implementation

When a LIKE pattern has a literal prefix (no wildcards at start):
1. Extract the prefix before the first `%` or `_`
2. Convert LIKE to range scan: `x >= 'prefix' AND x < 'prefiy'` (increment last char)
3. Use index seek instead of full scan
4. Must respect `PRAGMA case_sensitive_like` setting

## Affected Tests

- like-3.x through like-5.x (40+ tests)
- Tests check EXPLAIN output for `nosort {} i1` pattern

## Files to Modify

- `src/where.rs` or `src/wherecode.rs` - add LIKE optimization in query planner
- Need to detect LIKE with literal prefix pattern
- Generate appropriate index constraints

## References

- SQLite source: `where.c` - `likeOptimization()` function
- like.test lines 150+ test the optimization behavior
