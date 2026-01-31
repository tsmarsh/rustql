# Get LIKE test to 100% pass rate

## Progress

Started at 41% pass rate, now at **71%** (113/159 tests passing, 46 errors).

### Session Progress (2026-01-31) - continued

- **NOCASE collation handling for CREATE INDEX and LIKE optimization** - Major fix for case-insensitive LIKE with indexes
  - Fixed CREATE INDEX SQL storage to include COLLATE clause
  - Fixed schema::parse_create_index_sql to extract COLLATE from SQL
  - Fixed ParseSchema to inherit collation from table columns when creating index
  - Fixed index population during CREATE INDEX to use proper KeyInfo with collations
  - Fixed LIKE optimization to not skip LIKE function for NOCASE (range scan captures false positives)
  - Result: LIKE test errors reduced from 53 to 46 (7 more tests passing)

### Earlier Session Progress (2026-01-31)

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

### Remaining Failures (~46 tests)

1. **INSERT...SELECT...ORDER BY issue** (~15 tests)
   - Tests like-5.x use `INSERT INTO t2 SELECT * FROM t1 ORDER BY rowid` which fails silently
   - This is a pre-existing issue separate from LIKE optimization

2. **Custom LIKE functions** (like-8.3, like-8.4)
   - Tests use `db function like -argcount 2 newlike` to override built-in LIKE
   - Requires user-defined function support for LIKE

3. **Scan/step count instrumentation** (like-9.x, like-10.x)
   - Tests use `db status step` and `db status sort` for instrumentation
   - We don't fully support this API yet

4. **QPSG feature** (like-3.3.104, like-3.3.105)
   - Query Planner Stability Guarantee not implemented

5. **Unicode LIKE matching** (like-13.4)
   - Character comparison for non-ASCII characters

6. **Missing sqlite_options** (like-14.x)
   - Tests require `::sqlite_options(configslower)` variable

7. **EQP output differences** (like-15.x, like-12.x)
   - SEARCH vs SCAN expected in EXPLAIN QUERY PLAN output

### Files Modified

- src/executor/where_clause.rs
  - Added `LIKE_OPT_COMPLETE` flag
  - Added `like_pattern_complete()` helper
  - Modified `generate_like_range_terms()` to mark complete patterns
  - Fixed NOCASE collation: only skip LIKE function for BINARY collation (not NOCASE)

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

- src/api/connection.rs
  - Fixed parse_create_index_sql to inherit collation from table columns

- src/functions/scalar.rs
  - Fixed SUBSTR function name case in error message (for func2 fix)
