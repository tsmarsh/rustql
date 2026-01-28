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

## Files Modified

- `src/executor/select/mod.rs` - query planning and code generation
- `src/vdbe/engine/mod.rs` - Rowid opcode deferred seek handling
- `src/storage/btree/mod.rs` - cursor staleness detection for index cursors

## Progress

### Done

1. **Index detection**:
   - Extended `check_order_by_satisfied_inner` to search ALL indexes on a table
   - When ORDER BY column matches first column of an index, stores (table_name, index_name) in `order_by_index` field
   - Added `index_first_column_matches` helper method
   - Changed `order_by_index` from `Option<String>` to `Option<(String, String)>` to support multi-table queries

2. **Code generation for ORDER BY index scans**:
   - When `order_by_index` is set, generates index scan code:
     - OpenRead on INDEX cursor
     - Rewind/Next loop on index cursor (not table cursor)
     - DeferredSeek to set up table cursor from index cursor's rowid
   - Integrated with existing multi-table join compilation

3. **Rowid opcode deferred seek handling**:
   - Fixed Rowid opcode to properly handle deferred_moveto mode
   - When in deferred seek mode, actually performs the seek to verify row exists
   - Returns NULL if the target row was deleted

4. **Cursor staleness detection for index cursors**:
   - Fixed `BtCursor::next()` to detect entry shifts after DELETE
   - For index cursors, compares actual payload DATA (not just n_key which is payload size)
   - When entry at current position differs, doesn't advance (already at "next")

### Test Results

All delete-9.* tests now pass:
- delete-9.1 - Ok
- delete-9.2 - Ok (DELETE ALL during cross join)
- delete-9.3 - Ok (DELETE single row during cross join)
- delete-9.4 - Ok
- delete-9.5 - Ok

### TODO

1. **DESC support**:
   - Currently only ASC order is detected
   - DESC would require backward index iteration (Prev opcode)

2. **Multi-column index ORDER BY**:
   - Currently only matches ORDER BY on first column of index
   - Could be extended to match ORDER BY a, b when index is on (a, b)
