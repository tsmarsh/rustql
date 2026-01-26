# Implement Covering Index Detection in Query Planner

## Problem

While we now have the infrastructure to use covering indexes (alt_map in DeferredSeek), the query planner doesn't actually detect when an index is covering. The `is_covering` field in `IndexInfo` is always set to `false`.

### Current State

In `src/executor/where_clause.rs`:
```rust
IndexInfo {
    name: idx.name.clone(),
    columns: index_cols.clone(),
    is_primary: idx.is_primary_key,
    is_unique: idx.unique,
    is_covering: false,  // Always false!
    stats: idx.stats.clone(),
}
```

### What "Covering" Means

An index is "covering" for a query if it contains ALL columns needed by the query:
- SELECT list columns
- WHERE clause columns
- ORDER BY columns
- GROUP BY columns

### Benefits of Proper Detection

1. **Better EXPLAIN QUERY PLAN output**: Show "COVERING INDEX" vs just "INDEX"
2. **Cost estimation**: Covering indexes are cheaper (no table lookup)
3. **Index selection**: Prefer covering indexes when multiple options exist
4. **Future optimizations**: Skip opening table cursor entirely for covering queries

## Technical Details

### Required Changes

1. **Track needed columns during planning**:
   ```rust
   struct QueryColumnNeeds {
       select_columns: HashSet<(i32, i32)>,  // (table_idx, col_idx)
       where_columns: HashSet<(i32, i32)>,
       order_by_columns: HashSet<(i32, i32)>,
       group_by_columns: HashSet<(i32, i32)>,
   }
   ```

2. **Check if index covers needed columns**:
   ```rust
   fn is_index_covering(index: &IndexInfo, needed: &QueryColumnNeeds, table_idx: i32) -> bool {
       let needed_for_table: HashSet<i32> = needed.all_columns()
           .filter(|(t, _)| *t == table_idx)
           .map(|(_, c)| c)
           .collect();

       let index_columns: HashSet<i32> = index.columns.iter().copied().collect();

       needed_for_table.is_subset(&index_columns)
   }
   ```

3. **Set `is_covering` during index evaluation**:
   ```rust
   // In find_best_index() or similar
   let is_covering = is_index_covering(&index, &query_needs, table_idx);
   if is_covering {
       // Reduce cost estimate (no table I/O)
       cost *= 0.5;
   }
   ```

4. **Update EXPLAIN QUERY PLAN output**:
   ```rust
   // Change from:
   "SEARCH t1 USING INDEX i1w (w=?)"
   // To:
   "SEARCH t1 USING COVERING INDEX i1w (w=?)"
   ```

### Files to Modify

- `src/executor/where_clause.rs` - Query planner, IndexInfo
- `src/executor/select/mod.rs` - Column tracking, build_query_planner()
- `src/executor/explain.rs` - EXPLAIN QUERY PLAN formatting

### Test Cases

- where-1.8.2: Should show "COVERING INDEX" in EXPLAIN output
- Queries selecting only indexed columns should prefer covering indexes

### Definition of Done

- [ ] Query planner tracks which columns are needed
- [ ] `is_covering` field correctly set based on query needs
- [ ] EXPLAIN QUERY PLAN shows "COVERING INDEX" when appropriate
- [ ] Cost estimation favors covering indexes
- [ ] No regression in existing tests
