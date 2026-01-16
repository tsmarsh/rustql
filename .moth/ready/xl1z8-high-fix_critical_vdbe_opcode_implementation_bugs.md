# Fix Critical VDBE Opcode Implementation Bugs

## Summary

A comprehensive review of `src/vdbe/engine.rs` against SQLite's `vdbe.c` revealed several critical implementation bugs in existing opcodes. These are not missing opcodes (tracked separately) but shortcuts and deviations from SQLite behavior in opcodes that ARE implemented.

## Critical Issues (Cause Incorrect Behavior)

### 1. NewRowid - Missing Random Fallback & AUTOINCREMENT

**File:** `src/vdbe/engine.rs` lines 2503-2540

**Problem:** When the max rowid is reached (i64::MAX), SQLite falls back to a randomness algorithm to find unused rowids. rustql just does `saturating_add(1)` which will silently fail.

**SQLite behavior (vdbe.c lines 5680-5700):**
```c
if( pC->useRandomRowid ){
  do{
    sqlite3_randomness(sizeof(v), &v);
    v &= (MAX_ROWID>>1); v++;
  }while(/* check if rowid exists */);
}
```

**rustql behavior:**
```rust
new_rowid = last_rowid.saturating_add(1);  // Will fail at MAX
```

**Also missing:** P3 register for AUTOINCREMENT tracking.

### 2. Insert - Missing Change Tracking

**File:** `src/vdbe/engine.rs` lines 2542-2643

**Problem:** SQLite tracks row changes for `sqlite3_changes()` and last rowid for `sqlite3_last_insert_rowid()`. rustql ignores these flags.

**Missing from rustql:**
```rust
// Should implement:
// if op.p5 & OPFLAG_NCHANGE != 0 {
//     self.n_change += 1;
//     if op.p5 & OPFLAG_LASTROWID != 0 {
//         // Set last_rowid on connection
//     }
// }
```

### 3. Delete - Missing Change Tracking

**File:** `src/vdbe/engine.rs` lines 2741-2834

**Problem:** Same as Insert - doesn't increment change counter.

### 4. Column - Missing Affinity Application

**File:** `src/vdbe/engine.rs` lines 1787-2095

**Problem:** SQLite applies column affinity from schema when reading values. rustql returns raw values without affinity conversion, causing type mismatches.

### 5. Ne (Not Equal) - Buggy NULL Handling

**File:** `src/vdbe/engine.rs` lines 1057-1077

**Problem:** The NULL handling logic is inconsistent with SQLite's three-valued logic:
```rust
if !nulleq && (left.is_null() || right.is_null()) {
    // For Ne with NULL, SQLite typically DOES jump (NULL != X is true)
    // Actually this is subtle - let's check if either is NULL
    if left.is_null() || right.is_null() {
        self.pc = op.p2;  // This is WRONG for standard SQL NULL semantics
    }
}
```

The comment even admits uncertainty. SQLite's behavior: `NULL != X` returns NULL (unknown), not true.

### 6. decode_record_values - Complete Stub (Breaks FK)

**File:** `src/vdbe/engine.rs` lines 3656-3663

**Problem:** This function is used for FK checking but returns all NULLs:
```rust
fn decode_record_values(&self, _data: &[u8], n_fields: usize) -> Vec<Value> {
    // TODO: Implement proper record format decoding when needed
    vec![Value::Null; n_fields]  // FK checking completely broken!
}
```

### 7. FkIfZero - Off-by-One Bug

**File:** `src/vdbe/engine.rs` lines 3489-3496

**Problem:** Uses `op.p2 - 1` but pc is already incremented:
```rust
if self.deferred_fk_counter == 0 {
    self.pc = op.p2 - 1; // -1 because we increment after  <-- WRONG
}
```

The pc is incremented at the START of exec_op (line 837), not after, so this is an off-by-one error.

## Medium Priority Issues

### 8. Ephemeral Tables Use Vec/HashSet Instead of B-tree

**Problem:** SQLite uses real B-trees for ephemeral tables which scale properly. rustql uses:
```rust
ephemeral_rows: Vec<(i64, Vec<u8>)>
ephemeral_set: HashSet<Vec<u8>>
```

This doesn't scale for large temporary results.

### 9. Sorter is In-Memory Only

**Problem:** SQLite's sorter can spill to disk for large sorts. rustql sorts entirely in memory, causing OOM for large ORDER BY operations.

### 10. TriggerTest Always Skips Triggers

**File:** `src/vdbe/engine.rs` lines 3627-3636

**Problem:** Hardcoded to skip all triggers:
```rust
Opcode::TriggerTest => {
    // For now, always skip (jump to P3) - triggers are disabled
    self.pc = op.p3 - 1;
}
```

## Testing

After fixes, these should pass:
- `sqlite3_changes()` returns correct count after INSERT/UPDATE/DELETE
- `sqlite3_last_insert_rowid()` returns correct rowid
- AUTOINCREMENT tables generate correct sequential rowids
- FK constraint checks work correctly
- NULL comparisons follow SQL standard three-valued logic

## Files to Modify

- `src/vdbe/engine.rs` - Main opcode implementations
- `src/api/connection.rs` - May need last_rowid field
- `src/vdbe/auxdata.rs` - Proper record decoding for FK

## References

- SQLite vdbe.c: `sqlite3/src/vdbe.c`
- Opcode documentation: https://sqlite.org/opcode.html
