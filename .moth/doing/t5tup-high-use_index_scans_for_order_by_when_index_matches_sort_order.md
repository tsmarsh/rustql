# Use Index Scans for ORDER BY When Index Matches Sort Order

## Problem

When a query has `ORDER BY` on an indexed column, RustQL currently uses a table scan with a sorter (SorterSort/SorterNext opcodes) instead of using the index to produce results in sorted order.

Example:
```sql
CREATE TABLE t5(a, b);
CREATE INDEX i5 ON t5(a);
SELECT t5.rowid AS r, c, d FROM t5, t6 ORDER BY a;
```

RustQL generates:
```
OpenRead cursor on t5 (table)
... collect into sorter ...
SorterSort
SorterNext loop
```

SQLite generates:
```
OpenRead cursor on i5 (index)
... iterate index, fetch rows on-demand ...
IdxRowid to get rowid from index
```

## Impact

1. **Performance**: Sorter requires collecting all data before outputting
2. **Cursor stability**: With sorters, cursors are closed before results are output, so DELETE during iteration doesn't work correctly (delete-9.* tests)
3. **Memory**: Sorter holds all rows in memory

## Solution

Modify the query planner to:
1. Detect when ORDER BY columns match an existing index
2. Use index scan (OpenRead on index) instead of table scan + sorter
3. Use IdxRowid to fetch rowids from index entries
4. Fetch row data on-demand using the rowid

## Affected Tests

- delete-9.2, delete-9.3, delete-9.5 - require index scans for cursor stability to work
- Likely other tests that depend on index-ordered iteration

## Files to Modify

- `src/executor/select/mod.rs` - query planning and code generation
- May need to implement/improve index scan opcodes

## Progress

### Done

1. **Index detection** (commit b8a5713):
   - Extended `check_order_by_satisfied_inner` to search ALL indexes on a table
   - When ORDER BY column matches first column of an index, stores index name in `order_by_index` field
   - Added `index_first_column_matches` helper method

### TODO

2. **Code generation changes**:
   - When `order_by_index` is set, need to modify body compilation:
     - Open INDEX cursor instead of (or in addition to) table cursor
     - Use Rewind/Next on index cursor for iteration
     - Use IdxRowid to get rowids from index entries
     - Seek table cursor to rowid to fetch actual column data

   Reference: Existing index scan code at line ~1023 in select/mod.rs shows the pattern for WHERE-based index scans

3. **Handle JOINs**:
   - Current detection only works for single-table queries
   - For delete-9 tests (cross join), need to handle case where ORDER BY table has an index
   - May need to restructure loop nesting to put index-ordered table as outer loop

4. **DESC support**:
   - Currently only ASC order is detected
   - DESC would require backward index iteration (Prev opcode)
