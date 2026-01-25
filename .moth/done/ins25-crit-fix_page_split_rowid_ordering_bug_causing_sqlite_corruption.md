# Fix Page Split Rowid Ordering Bug Causing SQLite Corruption

## Problem

When inserting more than ~348 rows into a table, SQLite reports:
```
Tree 2 page 2 cell 0: Rowid 175 out of order
```

This indicates the B-tree page split logic is not maintaining proper rowid ordering.

## Reproduction

```bash
rm -f /tmp/test.db
echo "CREATE TABLE t1(a, b);" > /tmp/test.sql
for i in $(seq 1 350); do
  echo "INSERT INTO t1 VALUES($i, $((i*i)));"
done >> /tmp/test.sql
./target/release/rustql /tmp/test.db < /tmp/test.sql
sqlite3 /tmp/test.db "pragma integrity_check;"
```

Expected: `ok`
Actual: `Tree 2 page 2 cell 0: Rowid 175 out of order`

## Root Cause

The page split functions in `src/storage/btree/mod.rs` are likely:
1. Not maintaining sorted order of cells after split
2. Incorrectly distributing cells between left and right pages
3. Not properly positioning the separator key

## Files to Investigate

- `src/storage/btree/mod.rs`:
  - `split_root_leaf()`
  - `split_leaf_with_parent()`
  - `balance()` (if exists)

## Impact

- Database files with >348 rows are corrupted and unreadable by SQLite
- This violates the critical SQLite compatibility requirement in CLAUDE.md
