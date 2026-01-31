# Get LIKE test to 100% pass rate

## Progress

Started at 41% pass rate, now at **89%** (141/159 tests passing, 18 errors).

### Session Progress (2026-01-31) - continued (session 4)

- **INTEGER PRIMARY KEY implicit rowid substitution in Column opcode** - Fixed INSERT...SELECT not preserving IPK values
  - Bug: `INSERT INTO t10b SELECT * FROM t10` would auto-generate rowids (1,2,3...) instead of using values from column 0
  - Root cause: Column opcode returned NULL for IPK columns in records, but should return rowid instead
  - In SQLite, INTEGER PRIMARY KEY columns store NULL in the record but Column opcode returns rowid
  - Fix: Added IPK handling in Column opcode - when reading NULL from an IPK column, return cursor's rowid
  - Result: like-10.10 through like-10.15 and like-11.6 now pass (7 tests fixed)

- Combined result: LIKE test errors reduced from 25 to 18 (7 more tests passing)

### Session Progress (2026-01-31) - continued (session 3)

- **LIKE upper bound fix for NOCASE collation** - Fixed range bounds for mixed-case patterns
  - Bug: Pattern 'zZ%' computed upper bound 'z[' (incrementing 'Z' to '['), but under NOCASE 'z[' < 'zZ'
  - Fix: Normalize prefix to lowercase before computing upper bound for NOCASE patterns
  - Result: like-5.22 and like-5.24 now pass

- **Index entry ordering for NOCASE-equal values** - Added rowid as tiebreaker
  - Bug: Index entries 'abc' and 'ABC' (equal under NOCASE) were stored in binary order not rowid order
  - Fix: Modified `compare_records()` in btree to compare additional fields (rowid) after key fields are equal
  - Result: like-5.3, like-5.13, like-11.6 now pass

- **LIKE with non-literal pattern** - Fixed register allocation for concatenated patterns
  - Bug: Pattern `'ab' || 'c%'` used intermediate registers, breaking consecutive layout expected by Function opcode
  - Fix: Compile all arguments to temps first, then copy to consecutive registers
  - Result: like-4.5 now passes

- **GLOB function sqlite_like_count** - Added counter increment for GLOB
  - SQLite counts both LIKE and GLOB evaluations in sqlite_like_count
  - Added special GLOB handling in Function opcode that calls inc_like_count()
  - Result: like-3.18, like-3.24, like-5.8, like-5.18 now pass

- **LIKE optimization for TEXT columns only** - Disable optimization for non-TEXT columns
  - SQLite only uses LIKE index optimization for TEXT affinity columns
  - Added column_affinities tracking to TableInfo and QueryPlanner
  - Check affinity in generate_like_range_terms and skip non-TEXT columns
  - Result: like-10.1 through like-10.4, like-16.1 now pass

- Combined result: LIKE test errors reduced from 40 to 25 (15 more tests passing)

### Session Progress (2026-01-31) - continued (session 2)

- **INSERT...SELECT...ORDER BY fix** - Fixed cursor offset for OpenPseudo opcode
  - Bug: `compile_select_with_subqueries()` was adjusting cursor offsets for most opcodes but missed `OpenPseudo`
  - This caused pseudo cursor to have wrong number, leading to Column reads from non-existent cursor
  - Result: INSERT...SELECT...ORDER BY now works correctly

- **IdxGE/GT/LE/LT collation fix** - Fixed index range comparisons to use cursor's collation
  - Bug: Idx* opcodes were using P4::KeyInfo for collation, but bytecode often passes P4::Int64
  - When P4 doesn't have KeyInfo, it fell back to BINARY collation instead of cursor's key_info
  - Fix: Extract collation names from cursor's btree_cursor.key_info before mutable borrow, use as fallback
  - Result: NOCASE index scans now correctly filter using NOCASE collation

- **LIKE_OPT_COMPLETE for all prefix patterns** - Updated to set flag for NOCASE too
  - The flag is now set for all prefix% patterns regardless of case sensitivity
  - The actual skip decision happens in compile_runtime_filter_terms which checks using_index_range
  - If collation-matching index is used, range scan is sufficient (no LIKE function needed)

- Combined result: LIKE test errors reduced from 46 to 40 (6 more tests passing)

### Previous Session Progress (2026-01-31)

- **NOCASE collation handling for CREATE INDEX and LIKE optimization** - Major fix for case-insensitive LIKE with indexes
  - Fixed CREATE INDEX SQL storage to include COLLATE clause
  - Fixed schema::parse_create_index_sql to extract COLLATE from SQL
  - Fixed ParseSchema to inherit collation from table columns when creating index
  - Fixed index population during CREATE INDEX to use proper KeyInfo with collations
  - Fixed LIKE optimization to not skip LIKE function for NOCASE (range scan captures false positives)
  - Result: LIKE test errors reduced from 53 to 46 (7 more tests passing)

- **ORDER BY on INTEGER PRIMARY KEY optimization** - Added check to skip sorter when ORDER BY is on rowid column
  - Fixed like-11.1 and like-11.2 tests
  - Result: LIKE test errors reduced from 52 to 50

- **Also fixed (as bonus):**
  - **join3 test**: 100% pass rate (130/130) - enforced 64-table join limit with SQLite-compatible error
  - **func2 test**: 100% pass rate (132/132) - fixed SUBSTR function name case in error message

### Previously Completed

1. **LIKE function call elimination for prefix patterns** - When LIKE uses index range scan for patterns like `'abc%'`, skip the LIKE function verification entirely
   - Added `LIKE_OPT_COMPLETE` flag in WhereTermFlags to track when index bounds fully satisfy the LIKE
   - `like_pattern_complete()` helper checks if pattern is `prefix%` with no wildcards in prefix
   - `compile_runtime_filter_terms()` skips LIKE terms marked as complete when index range is used
   - Result: `sqlite_like_count = 0` for fully optimized LIKE queries

2. **ORDER BY index usage in EQP output** - When ORDER BY uses an index for ordering (not filtering), show it in EXPLAIN QUERY PLAN
   - Added `detect_order_by_index()` in prepare.rs to find indexes satisfying ORDER BY
   - Modified `format_plan_detail_with_order()` to show "SCAN t1 USING COVERING INDEX i1" for ORDER BY index scans
   - Handles single-column ASC ORDER BY with positional references (ORDER BY 1)

3. **nosort detection** (previous work) - `check_order_by_satisfied()` in SelectCompiler detects when an index scan satisfies ORDER BY

4. **sqlite_like_count tracking** (previous work) - Global counter for LIKE function calls exposed via TCL

### Remaining Failures (18 tests)

1. **Scan/step count instrumentation** (like-9.1, like-9.4.3, like-9.5.2, like-10.5b - 4 tests)
   - `db status step` and `db status sort` instrumentation issues
   - Some tests wrong scan counts

2. **Custom LIKE functions** (like-8.3, like-8.4 - 2 tests)
   - Tests use `db function like -argcount 2 newlike` to override built-in LIKE
   - Requires user-defined function support for LIKE

3. **QPSG feature** (like-3.3.102, like-3.3.104, like-3.3.105 - 3 tests)
   - Query Planner Stability Guarantee not implemented

4. **Missing sqlite_options** (like-14.1, like-14.2 - 2 tests)
   - Tests require `::sqlite_options(configslower)` variable

5. **EQP output differences** (like-12.13, like-12.15, like-15.101, like-15.112, like-15.121 - 5 tests)
   - SEARCH vs SCAN expected in EXPLAIN QUERY PLAN output

6. **Index selection for case_sensitive_like** (like-11.7, like-11.8 - 2 tests)
   - SQLite prefers explicit COLLATE BINARY index over inherited BINARY
   - Results are correct, just index selection differs

### Files Modified

- src/executor/insert.rs
  - Added `OpenPseudo` to list of opcodes that get cursor offset adjustment in `compile_select_with_subqueries()`

- src/executor/where_clause.rs
  - Added `LIKE_OPT_COMPLETE` flag
  - Added `like_pattern_complete()` helper
  - Modified `generate_like_range_terms()` to mark complete patterns
  - Updated is_complete logic to set flag for all prefix patterns (collation check happens at runtime)

- src/executor/select/mod.rs
  - Modified `compile_runtime_filter_terms()` to skip LIKE_OPT_COMPLETE terms
  - Added MAX_TABLES_IN_JOIN constant and check (for join3 fix)
  - Added ORDER BY on INTEGER PRIMARY KEY optimization

- src/executor/prepare.rs
  - Added `detect_order_by_index()` for ORDER BY index detection
  - Added `format_plan_detail_with_order()` for EQP output with ORDER BY index
  - Fixed CREATE INDEX SQL generation to include COLLATE clause
  - Fixed index column name extraction for Collate expressions
  - Build KeyInfo with collations for index population during CREATE INDEX

- src/schema/mod.rs
  - Fixed parse_create_index_sql to extract COLLATE from column specs

- src/vdbe/engine/mod.rs
  - OpenRead: fixed collation capture from schema indexes
  - OpenWrite: handle P4::KeyInfo for directly provided collations
  - ParseSchema: inherit collation from table columns when creating index
  - IdxGE/GT/LE/LT: added fallback to cursor's key_info for collation comparison
  - Column: added IPK implicit rowid substitution (read NULL from IPK column → return rowid)

- src/api/connection.rs
  - Fixed parse_create_index_sql to inherit collation from table columns

- src/functions/scalar.rs
  - Fixed SUBSTR function name case in error message (for func2 fix)
