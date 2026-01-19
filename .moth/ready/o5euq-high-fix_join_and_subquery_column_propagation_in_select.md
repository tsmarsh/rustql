# Fix JOIN and Subquery Column Propagation in SELECT

## Problem

JOINs with subqueries and NATURAL JOINs are not properly propagating all columns to the result set. Some columns are missing entirely or have incorrect values.

## Failing Tests (8 total)

### NATURAL JOIN returning wrong column values
```
select1-11.5.1 expected: [a 1 b 4 b 4]
select1-11.5.1 got:      [a 1 b 2 t4.b 4]

select1-11.6 expected: [a 1 b 4 b 4]
select1-11.6 got:      [a 1 b 2 y.b 4]

select1-11.8 expected: [b 4 a 3 b 4]
select1-11.8 got:      [t3.b 2 a 3 b 4]

select1-11.9 expected: [b 4 a 3 b 4]
select1-11.9 got:      [x.b 2 a 3 b 4]
```

The common pattern: column `b` should be 4 (from the right table) but we're getting 2 (from the left table) in some positions.

### Subquery JOIN missing columns entirely
```
select1-6.9.7 expected: [a.f1 11 a.f2 22 (subquery-0).5 5 (subquery-0).6 6]
select1-6.9.7 got:      [f1 11 f2 22]

select1-6.9.8 expected: [a.f1 11 a.f2 22 b.x 5 b.y 6]
select1-6.9.8 got:      [f1 11 f2 22]
```

The subquery columns (5, 6 and x, y) are completely missing from the output.

### Aggregate subquery JOIN missing columns
```
select1-11.14 expected: [a 1 b 2 max(a) 3 max(b) 4]
select1-11.14 got:      [a 1 b 2]

select1-11.15 expected: [max(a) 3 max(b) 4 a 1 b 2]
select1-11.15 got:      [a 1 b 2]
```

When joining a regular table with an aggregate subquery, the aggregate results are missing.

## SQLite Behavior Reference

```sql
-- NATURAL JOIN should use the value from the right table for common columns
CREATE TABLE t3(a, b);
CREATE TABLE t4(b, c);
INSERT INTO t3 VALUES(1, 2);
INSERT INTO t4 VALUES(4, 5);

SELECT * FROM t3 NATURAL JOIN t4;
-- Expected: a=1, b=4, c=5  (b comes from t4)

-- Subquery joins should include all columns
SELECT * FROM test1 AS a, (SELECT 5, 6) AS b;
-- Should include columns from both test1 and the subquery
```

## Root Cause

1. **NATURAL JOIN**: The join condition is being applied but the column value selection is wrong - using left table value instead of right table value for shared columns.

2. **Subquery columns**: When a subquery is used as a table source, its columns are not being registered or compiled into the result set.

## Files to Investigate

- `src/executor/select.rs` - compile_from_clause, compile_join
- Subquery table registration and cursor setup
- NATURAL JOIN column coalescing logic
