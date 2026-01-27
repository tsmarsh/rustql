# Get LIKE test to 100% pass rate

## Progress

Started at 41% pass rate, now at 61% (58/95 tests passing).

### Completed

1. **nosort detection** - Implemented `check_order_by_satisfied()` in SelectCompiler to detect when an index scan satisfies ORDER BY, skipping the sorter
   - Handles `ORDER BY 1` positional references
   - Detects LIKE range scans with has_range flag
   - Correctly reports "nosort" in EXPLAIN QUERY PLAN output

2. **sqlite_like_count tracking** - Implemented global counter for LIKE function calls
   - Added counter in vdbe/engine/state.rs (similar to sort_count)
   - Increments in LIKE opcode handler
   - TCL extension exposes ::sqlite_like_count variable
   - Skip reset/update for EXPLAIN queries to preserve count from actual query

### Remaining Failures

1. **Count tests expecting 0 LIKE calls** (~5 tests)
   - Index optimization reduces rows scanned but still verifies with LIKE
   - SQLite eliminates LIKE verification entirely for certain patterns

2. **Case-insensitive LIKE tests** (~10 tests)
   - These can't be index-optimized (correct behavior)
   - Shows "sort t1 *" instead of "nosort {} i1"

3. **QPSG feature** (~3 tests)
   - Query Planner Stability Guarantee not implemented

4. **like-9.4.1 crash** (1 test)
   - Crash on hex encoding in LIKE patterns

### Files Modified

- src/executor/select/mod.rs - check_order_by_satisfied()
- src/vdbe/engine/state.rs - like_count counter
- src/vdbe/engine/mod.rs - inc_like_count() in LIKE opcode
- src/tcl_ext.rs - sqlite_like_count TCL variable
- src/api/stmt.rs - reset_like_count() on statement reset
