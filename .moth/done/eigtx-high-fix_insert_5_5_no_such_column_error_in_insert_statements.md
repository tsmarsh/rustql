# Fix insert-5.5: Temp table rootpage in wrong database [high]

## Problem
The `insert-5.5` test expects temp tables to have rootpage starting at 2 (separate temp database), but RustQL was allocating temp table pages from the main database btree.

## Root Cause Analysis
The test `SELECT rootpage FROM sqlite_temp_master WHERE name='t4'` was returning `8` instead of `2`.

SQLite maintains separate btrees for:
- Main database (db_idx=0)
- Temp database (db_idx=1) - in-memory

RustQL had several issues:
1. **CreateBtree opcode hardcoded P1=0** - Now fixed to use `if create.temporary { 1 } else { 0 }`
2. **VdbeEngine only had main btree** - Added `temp_btree: Option<Arc<Btree>>` field
3. **CreateBtree didn't create temp btree** - Now creates in-memory btree when P1=1
4. **OpenRead/OpenWrite used wrong btree** - Now checks table.db_idx to select btree
5. **Temp btree not persisted across statements** - Now stored in connection's `dbs[1].btree`
6. **Insert/Delete opcodes used wrong btree** - Now use cursor's btree reference

## Solution
1. Added `set_temp_btree()` method to Vdbe
2. In CreateBtree (P1=1), store btree in both VDBE and connection's `dbs[1].btree`
3. In `stmt.rs` step(), pass temp btree from connection to VDBE
4. Start/commit/rollback transactions on temp btree alongside main btree
5. Added `btree: Option<Arc<Btree>>` field to VdbeCursor
6. OpenRead/OpenWrite now store btree reference on cursor
7. Insert/Delete opcodes now use cursor's btree instead of self.btree

## Files Modified
- `src/vdbe/engine/mod.rs`: Added temp_btree field, set_temp_btree method, cursor.btree field, modified CreateBtree/OpenRead/OpenWrite/Insert/Delete opcodes
- `src/executor/prepare.rs`: compile_create_table now uses correct db_idx for temp tables
- `src/api/stmt.rs`: Pass temp btree to VDBE, start/commit/rollback temp btree transactions

## Test Results
- insert-5.1 through insert-5.5, insert-5.7: **PASS**
- insert-5.6 fails (INSERT...SELECT cross-table, separate issue)
- All cargo tests pass

## Acceptance Criteria
- insert-5.5 passes (rootpage check) ✓
- INSERT/SELECT on temp tables work ✓
