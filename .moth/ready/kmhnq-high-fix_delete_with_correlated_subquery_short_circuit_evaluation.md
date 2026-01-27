# Fix DELETE with Correlated Subquery Short-Circuit Evaluation

## Problem

When a DELETE statement's WHERE clause contains both:
1. A short-circuit operator (AND/OR)
2. A correlated subquery that references the table being deleted

The subquery may execute AFTER some rows have been deleted, causing incorrect
results. The subquery should see the table state as it was BEFORE any deletions.

## Failing Tests

- `delete-12.0` in `sqlite3/test/delete.test`

## Test Case Analysis

```sql
CREATE TABLE t0(vkey INTEGER, pkey INTEGER, c1 INTEGER);
INSERT INTO t0 VALUES(2,1,-20),(2,2,NULL),(2,3,0),(8,4,95);

-- This DELETE should only keep row (8,4,95)
DELETE FROM t0 WHERE NOT (
    (t0.vkey <= t0.c1) AND
    (t0.vkey <> (SELECT vkey FROM t0 ORDER BY vkey LIMIT 1 OFFSET 2))
);

SELECT * FROM t0;
-- Expected: {8 4 95}
-- Current:  {2 1 -20 2 2 {} 2 3 0 8 4 95}  (nothing deleted)
```

## Analysis of the Bug

The WHERE clause has structure: `NOT (A AND B)` where:
- A = `t0.vkey <= t0.c1`
- B = `t0.vkey <> (SELECT vkey FROM t0 ORDER BY vkey LIMIT 1 OFFSET 2)`

For each row being evaluated:
1. If A is false, short-circuit skips B (subquery not executed)
2. If A is true, B executes the subquery

The subquery `SELECT vkey FROM t0 ORDER BY vkey LIMIT 1 OFFSET 2` returns
the 3rd smallest vkey. Initially that's `2` (rows sorted: 2,2,2,8 -> offset 2 = 2).

**The problem**: If rows are deleted during iteration, the subquery may see
a modified table and return different results for later rows.

## Root Cause

Two potential issues:

1. **Subquery evaluation timing**: The correlated subquery should be evaluated
   against the original table state, not the partially-modified state.

2. **WHERE clause evaluation**: The entire WHERE clause might not be evaluating
   correctly, resulting in no rows matching the DELETE condition.

## SQLite's Solution

SQLite handles this by:

1. **Statement journals**: Track modifications during statement execution
2. **Snapshot isolation**: Subqueries see table state at statement start
3. **Rowid collection**: Collect all rowids to delete FIRST, then delete them

The key insight: DELETE should be two-phase:
1. **Selection phase**: Evaluate WHERE for all rows, collect rowids to delete
2. **Deletion phase**: Delete the collected rowids

## Implementation Plan

### Phase 1: Verify WHERE Clause Evaluation

First, verify the WHERE clause is being evaluated correctly:

```sql
-- Debug: what does the WHERE evaluate to for each row?
SELECT *,
    (vkey <= c1) as A,
    (vkey <> (SELECT vkey FROM t0 ORDER BY vkey LIMIT 1 OFFSET 2)) as B,
    NOT ((vkey <= c1) AND (vkey <> (SELECT vkey FROM t0 ORDER BY vkey LIMIT 1 OFFSET 2))) as should_delete
FROM t0;
```

### Phase 2: Two-Phase DELETE Implementation

Modify `src/executor/delete.rs` to use two-phase approach when WHERE contains
correlated subqueries:

```rust
fn compile_delete_with_correlated_subquery(&mut self, delete: &DeleteStmt) -> Result<()> {
    // Phase 1: Collect rowids to delete
    let ephemeral_cursor = self.alloc_cursor();
    self.emit(Opcode::OpenEphemeral, ephemeral_cursor, 1, 0, P4::Unused);

    // Scan table, evaluate WHERE, store matching rowids
    self.emit(Opcode::Rewind, self.table_cursor, end_label, 0, P4::Unused);
    // ... evaluate WHERE ...
    // ... if matches, store rowid in ephemeral ...
    self.emit(Opcode::Next, self.table_cursor, loop_label, 0, P4::Unused);

    // Phase 2: Delete collected rowids
    self.emit(Opcode::Rewind, ephemeral_cursor, done_label, 0, P4::Unused);
    // ... for each rowid, seek and delete ...
}
```

### Phase 3: Subquery Snapshot Isolation

For more complex cases, implement snapshot isolation:

```rust
// Before DELETE starts, create read-only snapshot for subqueries
fn create_statement_snapshot(&mut self) {
    // Mark current transaction state
    // Subqueries should read from this snapshot
}
```

## Files to Modify

1. `src/executor/delete.rs` - Two-phase DELETE for correlated subqueries
2. `src/executor/select.rs` - Ensure correlated subqueries work correctly
3. `src/vdbe/engine/mod.rs` - Subquery execution with proper table state

## Debugging Steps

1. Test the subquery alone:
```sql
SELECT vkey FROM t0 ORDER BY vkey LIMIT 1 OFFSET 2;  -- Should return 2
```

2. Test WHERE clause components:
```sql
SELECT *, (vkey <= c1) FROM t0;  -- Check column A
```

3. Test full WHERE:
```sql
SELECT * FROM t0 WHERE NOT ((vkey <= c1) AND (vkey <> 2));
```

## Testing

```bash
make test-delete  # Should pass delete-12.0
```

## Edge Cases

1. **Self-referencing subquery with aggregates**: `WHERE x > (SELECT AVG(x) FROM t)`
2. **Multiple correlated subqueries**: Evaluate all against original state
3. **Nested DELETE in trigger**: Each DELETE sees its own starting state

## References

- SQLite forum: https://sqlite.org/forum/forumpost/e61252062c9d286d
- SQLite source: `delete.c` handling of `WHERE_ONEPASS_DESIRED`
- Similar issue for UPDATE: `update-21.4` test
