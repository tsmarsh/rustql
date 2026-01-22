# Implement VDBE Search Count Tracking for Query Efficiency Tests

## Problem
The where.test uses a `count` proc that appends `sqlite_search_count` to query results:

```tcl
proc count sql {
  set ::sqlite_search_count 0
  return [concat [execsql $sql] $::sqlite_search_count]
}
```

Tests expect efficient index usage measured by low search counts:
- Expected: `[3 121 10 3]` (3 VDBE search operations)
- Got: `[3 121 10 0]` (search count not implemented)

## What sqlite_search_count Tracks

SQLite increments `sqlite_search_count` for these VDBE operations:
- **OP_SeekGE, OP_SeekGT, OP_SeekLE, OP_SeekLT** - Index/table seeks
- **OP_Next, OP_Prev** - Cursor movement
- **OP_MoveTo** - Direct rowid lookup

This counter proves that:
1. Index is being used (few seeks vs full scan)
2. Query is efficient (low operation count)

## Implementation

### 1. Add Global Counter
```rust
// In VDBE state
thread_local! {
    static SEARCH_COUNT: Cell<i32> = Cell::new(0);
}

pub fn reset_search_count() {
    SEARCH_COUNT.with(|c| c.set(0));
}

pub fn get_search_count() -> i32 {
    SEARCH_COUNT.with(|c| c.get())
}

fn increment_search_count() {
    SEARCH_COUNT.with(|c| c.set(c.get() + 1));
}
```

### 2. Increment in VDBE Operations
```rust
fn execute_op(&mut self, op: &VdbeOp) -> Result<()> {
    match op.opcode {
        Opcode::SeekGE | Opcode::SeekGT |
        Opcode::SeekLE | Opcode::SeekLT |
        Opcode::SeekRowid | Opcode::NotExists => {
            increment_search_count();
            // ... actual seek logic
        }
        Opcode::Next | Opcode::Prev => {
            increment_search_count();
            // ... cursor movement
        }
        // ...
    }
}
```

### 3. Expose in TCL Extension
```rust
// Add sqlite_search_count variable
unsafe extern "C" fn get_sqlite_search_count(...) {
    set_result_int(interp, get_search_count());
}

// Reset at start of each query
unsafe extern "C" fn execsql_cmd(...) {
    reset_search_count();
    // ... execute query
}
```

## Files to Modify
- `src/vdbe/execute.rs` - Add counter increments
- `src/tcl_ext.rs` - Expose sqlite_search_count variable
- `scripts/run_sqlite_test.tcl` - Ensure count proc uses the variable

## Test Command
```bash
make test-where
```

## Success Criteria
Tests like `where-1.1.1` should return correct search counts:
- With index: low count (3-10)
- Full scan: high count (~100 for 100 rows)
