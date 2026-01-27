# Fix INSERT Statement Column Handling and Default Values

## Problem
INSERT statements fail to properly handle column specifications, defaults, triggers, and ordering.

### Current Pass Rate: 61/83 (73%)

**Progress**: 59 → 61 tests (+2 tests this session)

**Target**: 80% = 66/83 tests (need 5 more)

---

## Latest Session Notes (2026-01-27)

### What Was Fixed This Session

1. **Index affinity coercion for type comparison** (`src/executor/select/mod.rs`)
   - **Bug**: Index seeks failed when comparing text literal against integer column (e.g., `WHERE f1='111'` wouldn't match integer 111)
   - **Root cause**: The index search key was built without applying column type affinities
   - **Fix**: Added emission of `Opcode::Affinity` before `MakeRecord` when building index seek keys
   - **Impact**: Fixes insert-3.2 and insert-3.3 (SELECT after INSERT with DEFAULT values through index)

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
- [ ] insert.test pass rate: >=75% - Current: 73% (61/83)
- [ ] insert.test pass rate: >=80% (66+ of 83) - Need 5 more tests
