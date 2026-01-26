# Implement Multi-Index Query Planning

## Problem

When a table has multiple indexes that could satisfy a query, we need to choose the best one. Currently, our query planner has basic index selection but doesn't fully implement SQLite's cost-based optimization.

### Example

```sql
CREATE TABLE t1(a, b, c);
CREATE INDEX i1 ON t1(a);
CREATE INDEX i2 ON t1(b);
CREATE INDEX i3 ON t1(a, b);

SELECT * FROM t1 WHERE a = 5 AND b = 10;
```

**Options**:
1. Use `i1` on `a`, filter `b` at runtime
2. Use `i2` on `b`, filter `a` at runtime
3. Use `i3` on `(a, b)` - BEST: no runtime filtering needed

### Current State

Our `QueryPlanner` in `where_clause.rs`:
- Iterates through indexes
- Counts matching equality columns
- Picks index with most matches
- Doesn't fully consider:
  - Index statistics (sqlite_stat1)
  - Composite index prefix matching
  - Range constraint handling
  - OR clause index optimization

## Technical Details

### SQLite's Index Selection Algorithm

1. **Enumerate all possible index uses** for each table
2. **Estimate cost** for each option using:
   - Index statistics (if available)
   - Column selectivity estimates
   - I/O cost model
3. **Consider combinations** for multi-table queries
4. **Pick lowest total cost**

### Cost Model Components

```rust
struct IndexCost {
    /// Estimated rows returned
    rows_out: f64,

    /// Number of index seeks required
    n_seek: i32,

    /// Number of table lookups (0 for covering index)
    n_lookup: i32,

    /// Whether ORDER BY is satisfied
    order_by_satisfied: bool,

    /// Total estimated cost
    total_cost: f64,
}
```

### Required Improvements

1. **Better statistics usage**:
   ```rust
   fn estimate_rows_with_stats(
       index: &IndexInfo,
       eq_cols: i32,
       range_cols: i32,
   ) -> f64 {
       if let Some(stats) = &index.stats {
           // Use actual statistics from sqlite_stat1
           stats.estimate_rows(eq_cols, range_cols)
       } else {
           // Fall back to heuristics
           table_rows / 10.0_f64.powi(eq_cols)
       }
   }
   ```

2. **Composite index prefix matching**:
   ```rust
   // For index on (a, b, c):
   // - WHERE a=1 uses 1 column
   // - WHERE a=1 AND b=2 uses 2 columns
   // - WHERE b=2 uses 0 columns (can't skip 'a')
   // - WHERE a=1 AND c=3 uses 1 column (can't skip 'b')
   ```

3. **Range constraint handling**:
   ```rust
   // For index on (a, b):
   // - WHERE a=1 AND b>5 uses equality on 'a', range on 'b'
   // - WHERE a>1 AND b=5 uses range on 'a' only ('b' unusable)
   ```

4. **OR clause optimization**:
   ```sql
   -- Can use index union:
   SELECT * FROM t1 WHERE a=1 OR a=2;

   -- May need full scan:
   SELECT * FROM t1 WHERE a=1 OR b=2;
   ```

### Files to Modify

- `src/executor/where_clause.rs` - Main query planner
- `src/schema/mod.rs` - Index statistics structures

### Test Cases

- index.test - Various index selection scenarios
- where.test - Complex WHERE clause optimization

### Definition of Done

- [ ] Cost model considers all relevant factors
- [ ] Statistics (sqlite_stat1) used when available
- [ ] Composite indexes handled correctly
- [ ] Range constraints integrated with equality
- [ ] EXPLAIN QUERY PLAN shows chosen index
- [ ] Performance improvement on complex queries
- [ ] No regression in existing tests

### Dependencies

- Should be done after short-circuit evaluation (ooqhg)
- Works with covering index detection (z7ug9)
