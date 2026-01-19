# VDBE: Implement ANALYZE and query statistics

## Problem
ANALYZE command and sqlite_stat1/sqlite_stat4 tables not implemented. Query planner can't make informed index choices.

## SQLite Behavior
```sql
ANALYZE;
-- Populates sqlite_stat1 with index statistics
SELECT * FROM sqlite_stat1;
-- tbl, idx, stat columns
```

## Required Changes
1. Implement ANALYZE statement
2. Create sqlite_stat1 system table
3. Collect row count and index selectivity
4. Use statistics in query planning

## Files
- `src/executor/analyze.rs` - ANALYZE implementation
- `src/schema/mod.rs` - System tables
