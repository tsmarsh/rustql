# Implement Short-Circuit WHERE Evaluation

## Problem

Our SELECT compiler generates bytecode that evaluates ALL WHERE conditions before combining them with AND/OR, rather than short-circuiting when an early condition fails. This causes unnecessary table lookups and incorrect `search_count` values.

### Current Behavior

For a query like:
```sql
SELECT w, x, y FROM t1 WHERE w = 11 AND x > 2
```

Our compiler generates:
```
1. Read w, evaluate (w = 11), store result in reg4
2. Read x, evaluate (x > 2), store result in reg5  ← ALWAYS READS x
3. AND reg4, reg5 → reg3
4. IfNot reg3, skip
5. Output row
```

This means even when `w != 11`, we still read `x` from the table, triggering an unnecessary deferred seek completion.

### Expected Behavior (SQLite)

SQLite generates:
```
1. Read w
2. If w != 11, jump to next_row  ← SHORT-CIRCUIT
3. Read x
4. If x <= 2, jump to next_row
5. Output row
```

When `w != 11`, SQLite never reads `x`, avoiding the table lookup entirely.

### Impact

- **search_count discrepancy**: Tests expect 3 searches, we do 4
- **Performance**: Extra I/O for table lookups that aren't needed
- **WHERE tests**: ~15-20 tests fail due to this issue

## Technical Details

### Current Code Location

The WHERE compilation happens in `src/executor/select/mod.rs`:
- `compile_where_condition()` method (around line 4800)
- Generates code for each term independently, then combines with AND/OR

### Required Changes

1. **Rewrite AND evaluation** to use conditional jumps:
   ```rust
   // For: A AND B AND C
   // Generate:
   //   evaluate A
   //   IfNot A, skip_label
   //   evaluate B
   //   IfNot B, skip_label
   //   evaluate C
   //   IfNot C, skip_label
   //   ... body ...
   // skip_label:
   ```

2. **Rewrite OR evaluation** to use conditional jumps:
   ```rust
   // For: A OR B OR C
   // Generate:
   //   evaluate A
   //   If A, body_label
   //   evaluate B
   //   If B, body_label
   //   evaluate C
   //   IfNot C, skip_label
   // body_label:
   //   ... body ...
   // skip_label:
   ```

3. **Order terms by cost**:
   - Evaluate index-covered columns first (no table lookup)
   - Evaluate cheap comparisons before expensive functions
   - Evaluate selective conditions first (more likely to short-circuit)

### Files to Modify

- `src/executor/select/mod.rs` - Main WHERE compilation
- `src/executor/select/types.rs` - May need new structures for term ordering

### Test Cases

After implementation, these should pass:
- where-1.4.1 through where-1.11b (search_count = 3, not 4)
- where-2.1 through where-2.7 (search_count = 6, not 9-12)

### Definition of Done

- [ ] AND expressions short-circuit on first false
- [ ] OR expressions short-circuit on first true
- [ ] search_count matches SQLite for single-table WHERE queries
- [ ] WHERE tests pass rate increases to 60%+
- [ ] No regression in other test suites
