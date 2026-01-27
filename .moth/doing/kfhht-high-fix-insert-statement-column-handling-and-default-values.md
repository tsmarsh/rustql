# Fix INSERT Statement Column Handling and Default Values

## Problem
INSERT statements fail to properly handle column specifications, defaults, triggers, and ordering.

### Current Pass Rate: 65/83 (78%)

**Progress**: 59 → 64 → 65 tests (+6 total)

**Target**: 80% = 66/83 tests (need 1 more)

---

## Latest Session Notes (2026-01-27 - Session 2)

### What Was Fixed This Session

1. **UNIQUE Index Constraint Enforcement in IdxInsert** (`src/vdbe/engine/mod.rs`)
   - **Bug**: IdxInsert opcode didn't check for duplicate keys in UNIQUE indexes
   - **Root cause**: No duplicate key checking before btree insert
   - **Fix**: Added is_unique/index_name fields to VdbeCursor; implemented prefix comparison in IdxInsert to detect duplicates; proper error message generation with table.column format
   - **Impact**: Enables secondary UNIQUE index constraint checking

2. **Auto-index creation for column-level UNIQUE constraints** (`src/executor/prepare.rs`, `src/schema/mod.rs`)
   - **Bug**: `CREATE TABLE t(a, b UNIQUE)` didn't create the auto-index for column b
   - **Root cause**: Column-level UNIQUE constraints only set is_unique flag on Column, didn't create actual B-tree indexes
   - **Fix**: Modified compile_create_table to emit CreateBtree + ParseSchemaIndex + sqlite_master insert for each UNIQUE column/table constraint
   - **Impact**: Fixes insert-16.6 (+1 test), enables proper UNIQUE checking for column-level constraints

3. **Auto-index lookup in OpenRead/OpenWrite** (`src/vdbe/engine/mod.rs`)
   - **Bug**: Auto-indexes stored in table.indexes weren't found by cursor open operations
   - **Root cause**: OpenRead/OpenWrite only checked schema.indexes, not table.indexes
   - **Fix**: Added search loop to check table.indexes for auto-index names
   - **Impact**: Required for auto-index UNIQUE enforcement to work

### Remaining Issues (18 tests)

1. **INTEGER PRIMARY KEY UNIQUE in Triggers** (insert-16.4, 17.x - multiple tests)
   - Trigger tries to insert conflicting rowid, should fail with "UNIQUE constraint failed: t.a"
   - Issue: Insert opcode's rowid conflict detection doesn't work correctly in trigger context
   - The secondary UNIQUE checking works, but PRIMARY KEY (rowid) conflicts need different handling

2. **REPLACE Semantics** (insert-6.2, 6.3, 6.4)
   - REPLACE should delete existing row before inserting new one
   - Currently inserts without properly deleting conflicting rows

3. **Expression Index Parsing** (insert-13.1)
   - `CREATE INDEX t13x1 ON t13(-b=b)` fails with "syntax error"
   - Parser doesn't handle negative sign in expression indexes

4. **sqlite_temp_master** (insert-5.5)
   - Query on sqlite_temp_master for temporary tables not working

---

## Previous Session Notes (2026-01-27 - Session 1)
   - Requires: Fix type coercion in record serialization/deserialization

---

## Previous Session Notes (2026-01-26)

### What Was Fixed This Session

1. **BEGIN...END parsing for triggers** (`src/executor/prepare.rs:find_statement_tail`)
   - **Bug**: The `find_statement_tail` function split SQL on semicolons without understanding BEGIN...END blocks
   - **Symptom**: `CREATE TRIGGER ... BEGIN SELECT 1; END;` caused `END` to be parsed as a separate COMMIT statement, producing "cannot commit - no transaction is active" errors
   - **Fix**: Added BEGIN/END depth tracking; only split on semicolons at depth 0

2. **Added AFTER INSERT trigger firing** (`src/vdbe/engine/mod.rs`)
   - Added `fire_after_insert_triggers()` function (modeled on existing `fire_after_delete_triggers()`)
   - Modified Insert opcode handler to call this after successful insert
   - Triggers now compile and execute correctly

3. **Fixed INSERT opcode to pass table name** (`src/executor/insert.rs`)
   - Changed P4 from `P4::Int64(flags)` to `P4::Table(table_name)`
   - Moved conflict resolution flags to P5 (combined with OPFLAG_NCHANGE)
   - Modified VDBE engine to extract conflict mode from P5 when P4 is Table
   - Added `table_name` field to `InsertCompiler` struct

4. **Fixed error message for inconsistent VALUES** (`src/executor/insert.rs:compile_values`)
   - SQLite expects "all VALUES must have the same number of terms" when rows have different value counts
   - Added pre-check before comparing to table column count

### Uncommitted Changes

Files modified (not committed):
- `src/executor/insert.rs` - table_name field, P4::Table changes, VALUES validation
- `src/executor/prepare.rs` - BEGIN...END aware statement splitting
- `src/vdbe/engine/mod.rs` - INSERT trigger firing, conflict flag extraction from P5

### Current Test Failures (22 tests)

| Test | Expected | Got | Root Cause |
|------|----------|-----|------------|
| insert-4.3 | "no such column: t3.a" | "UNIQUE constraint failed" | Column scope checking in VALUES - t3.a should be invalid |
| insert-4.5 | {6 7} | Empty | IS NULL comparison issue after INSERT with NULL |
| insert-6.3, 6.4 | REPLACE/UPDATE count/data | Wrong | UPDATE OR REPLACE semantics |
| insert-15.1 | {4 33000} | {4 31xxx} | REPLACE with INTEGER PRIMARY KEY selecting wrong row |
| insert-16.x (4 tests) | UNIQUE errors | Success/wrong | UNIQUE index constraints not enforced in triggers |
| insert-17.x (10 tests) | UNIQUE errors/data | Success/wrong | Trigger + REPLACE + UNIQUE interaction |

### Known Issues Requiring Significant Work

1. **REPLACE (OE_REPLACE) semantics broken**
   - Location: `src/vdbe/engine/mod.rs` around line 3152
   - Current behavior: Does btree delete but doesn't properly handle INTEGER PRIMARY KEY conflicts
   - Test: `REPLACE INTO t1 SELECT a, b FROM t2` where multiple rows have same PK - should keep last
   - Symptom: First row wins instead of last, or both rows end up in table

2. **UNIQUE index constraints not enforced**
   - Location: `src/vdbe/engine/mod.rs` Opcode::IdxInsert (line ~4547)
   - Current behavior: IdxInsert just inserts without checking for duplicates
   - Required: Before insert, check if key exists (excluding rowid), fail if duplicate
   - This affects all insert-16.x and insert-17.x tests

3. **CREATE TABLE AS SELECT**
   - Test insert-11.1: `CREATE TABLE t11a AS SELECT '123456789' AS x;`
   - Table is created but no rows are inserted
   - Need to investigate CREATE TABLE AS implementation

4. **Scalar subquery results in INSERT VALUES**
   - Test insert-4.5: `INSERT INTO t3 VALUES((SELECT b FROM t3 WHERE a=0),6,7)`
   - When subquery returns no rows, should use NULL
   - Currently seems to use wrong value

### Recommended Next Steps (to reach 80%)

**Quickest wins** (2 tests needed):

1. **Fix REPLACE for INTEGER PRIMARY KEY** - Would fix insert-15.1
   - In OE_REPLACE handling, ensure proper rowid conflict detection and deletion
   - The delete happens but the check is on table.rowid, need to check IPK column

2. **Fix column scope in VALUES** - Would fix insert-4.3
   - In expression compilation for INSERT VALUES, reject table-qualified columns
   - Error should be "no such column: t3.a"

**Higher effort but high value**:

3. **UNIQUE index constraint checking in IdxInsert** - Would fix 6+ tests
   - Before inserting into UNIQUE index, check if key already exists
   - Handle based on conflict resolution mode (ABORT, REPLACE, IGNORE, etc.)

---

## Historical Context

### Previous Session Improvements
- Fixed column validation to reject invalid column names
- Fixed column position mapping to table schema
- Implemented DEFAULT value extraction and application
- Added support for unquoted identifier defaults
- Fixed numeric string comparisons in WHERE clauses
- Added ColumnMapper infrastructure for complex INSERT...SELECT
- Fixed function register allocation (nested function calls)
- Fixed IPK implicit mapping
- Fixed NULL rowid in VALUES
- Fixed SELECT * column count
- Fixed SELECT without FROM
- Fixed duplicate column handling

### Files Overview
- `src/executor/insert.rs` - Main INSERT compiler (InsertCompiler struct)
- `src/executor/column_mapping.rs` - ColumnMapper for INSERT...SELECT
- `src/executor/prepare.rs` - Statement compilation, find_statement_tail
- `src/vdbe/engine/mod.rs` - VDBE opcodes including Insert, IdxInsert, trigger firing
- `src/executor/trigger.rs` - Trigger utilities (find_matching_triggers)

### Test Command
```bash
make test-insert  # Runs sqlite3/test/insert.test via TCL
```

Results in `test-results/insert.log`

## Definition of Done
- [x] insert.test pass rate: >=50% - ACHIEVED
- [x] insert.test pass rate: >=75% - ACHIEVED: 77% (64/83)
- [ ] insert.test pass rate: >=80% (66+ of 83) - Need 2 more tests

### Path to 80%
The remaining 2 tests require implementing UNIQUE index constraint checking in IdxInsert.
See detailed notes at top of file.
