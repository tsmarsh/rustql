# Implement INSERT...SELECT with Complex Queries and Proper Column Mapping

## Problem
INSERT...SELECT statements fail because:
1. Only supports "simple SELECT from a table" (single table, no WHERE)
2. Column mapping between SELECT results and INSERT targets broken
3. ORDER BY, LIMIT, aggregates, joins, and subqueries not supported

### Current Failures
- insert2-1.2.1: INSERT...SELECT with WHERE clause fails
- insert2-1.2.2: INSERT...SELECT with aggregates fails
- insert2-1.3.1: INSERT...SELECT with joins fails
- insert2-1.3.2: INSERT...SELECT with subqueries fails
- insert2-1.4: INSERT...SELECT with ORDER BY and LIMIT fails
- insert2-4.2 through 4.5: Various INSERT...SELECT scenarios

Error: "INSERT...SELECT requires a simple SELECT from a table"

### Current Pass Rate
- insert2.test: 13/31 (42%) - mostly failing on complex SELECT

## Root Causes
1. **Query restriction**: Artificial limitation requiring "simple SELECT"
2. **Column mapping**: No logic to map SELECT columns to INSERT columns
3. **Row iteration**: VDBE doesn't properly iterate INSERT...SELECT results
4. **Value conversion**: Type coercion not applied correctly

## TCL Tests That Must Pass

### From insert2.test (required for 75% pass rate)
- insert2-1.2.1: INSERT...SELECT with WHERE clause on source table
- insert2-1.2.2: INSERT...SELECT with COUNT(*) aggregate
- insert2-1.3.1: INSERT...SELECT with JOIN of two tables
- insert2-1.3.2: INSERT...SELECT with correlated subquery
- insert2-1.4: INSERT...SELECT with ORDER BY and LIMIT
- insert2-4.2: INSERT...SELECT with arithmetic expressions
- insert2-4.3: INSERT...SELECT with qualified column names (table.column)
- insert2-4.4: INSERT...SELECT with multiple value expressions
- insert2-4.5: INSERT...SELECT with GROUP BY and HAVING
- insert2-4.6: INSERT...SELECT with UNION
- insert2-4.7: INSERT...SELECT with EXCEPT
- insert2-4.8: INSERT...SELECT with INTERSECT

## Implementation Tasks

### Task 1: Remove Query Restriction (Priority: Highest)
- [ ] Remove "simple SELECT" validation in executor/insert.rs
- [ ] Support arbitrary SELECT queries in INSERT...SELECT
- [ ] Maintain type checking between SELECT and INSERT columns

### Task 2: Column Mapping Engine (Priority: Highest)
- [ ] Build mapping between SELECT result columns and INSERT target columns
- [ ] Handle implicit column count matching
- [ ] Handle explicit column list in INSERT...SELECT

### Task 3: VDBE Row Iteration (Priority: High)
- [ ] Extend VDBE to iterate rows from SELECT subquery
- [ ] Apply row constraints validation
- [ ] Handle multi-row results properly

### Task 4: Expression and Aggregate Support (Priority: High)
- [ ] Support COUNT(*), SUM(), AVG(), MIN(), MAX() in SELECT
- [ ] Support arithmetic expressions in SELECT
- [ ] Support string concatenation and functions

### Task 5: Complex Query Features (Priority: High)
- [ ] Support WHERE clauses in source SELECT
- [ ] Support JOIN operations (INNER, LEFT, CROSS)
- [ ] Support subqueries in FROM and WHERE
- [ ] Support ORDER BY and LIMIT
- [ ] Support GROUP BY and HAVING
- [ ] Support UNION, EXCEPT, INTERSECT

## Files to Modify
- src/executor/insert.rs - Remove restriction, add column mapping
- src/parser/resolve.rs - Column mapping for INSERT...SELECT
- src/vdbe/engine.rs - Row iteration in INSERT operations
- src/vdbe/ops.rs - New operations for INSERT...SELECT execution

## Definition of Done
- [ ] insert2.test pass rate: >=75% (24+ of 31)
- [ ] All complex SELECT features supported in INSERT...SELECT
- [ ] Column mapping works with explicit and implicit columns
- [ ] No regression in other INSERT tests
- [ ] All TCL tests above passing
