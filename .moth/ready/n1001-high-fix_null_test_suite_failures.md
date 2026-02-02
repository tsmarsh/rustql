# Fix NULL Test Suite Failures [high]

## Problem

The null.test suite has 11 failures (83% pass rate). These failures affect fundamental NULL handling semantics that impact correctness across the entire SQL engine.

## Failing Tests

| Test | Issue | Expected | Got |
|------|-------|----------|-----|
| null-6.5 | ORDER BY validation in UNION | Error: "1st ORDER BY term does not match any column in the result set" | Success with wrong data |
| null-6.6 | ORDER BY validation in UNION (qualified) | Same error | Success with wrong data |
| null-7.1 | UNIQUE ON CONFLICT IGNORE (column) | 3 rows inserted | UNIQUE constraint error |
| null-7.2 | UNIQUE ON CONFLICT IGNORE (table) | 3 rows inserted | UNIQUE constraint error |
| null-8.11 | Index lookup with NULL equality | Empty result | Returns row with NULL |
| null-8.13 | Index scan with less-than comparison | 1 row | 2 rows (includes NULL) |
| null-10.1 | WHERE comparison with NULL | Empty result | Returns row |

## Root Cause Analysis

### Issue 1: ORDER BY Validation in UNION Queries (null-6.5, null-6.6)

**SQL Example:**
```sql
SELECT b FROM t1 UNION SELECT c FROM t1 ORDER BY t1.a;
-- Should error: column 'a' is not in the result set
```

**Root Cause:** The function `is_valid_compound_order_by_term()` in `src/executor/select/mod.rs` (lines 8890-8915) incorrectly allows all `Expr::Column(_)` references:

```rust
// Line 8899 - WRONG:
Expr::Column(_) => true,  // Allows any column reference
```

SQLite only allows ORDER BY columns that exist in the UNION's result set.

**Fix:** Validate column references against result column names before allowing them.

### Issue 2: UNIQUE Constraint ON CONFLICT Not Propagated (null-7.1, null-7.2)

**SQL Example:**
```sql
CREATE TABLE t2(a, b UNIQUE ON CONFLICT IGNORE);
INSERT INTO t2 VALUES(1,1);
INSERT INTO t2 VALUES(4,1);  -- Should be silently ignored
```

**Root Cause:**
1. `Index` struct in `src/schema/mod.rs` has no field for `conflict_action`
2. `emit_index_inserts()` in `src/executor/insert.rs` ignores the `_conflict_action` parameter
3. IdxInsert opcode doesn't receive conflict flags

**Fix:**
- Add `conflict_action: Option<ConflictAction>` to Index struct
- Pass conflict flags to IdxInsert opcode
- Handle IGNORE in VDBE execution

### Issue 3: NULL Values in Index Lookups (null-8.11, null-8.13)

**SQL Example:**
```sql
CREATE INDEX t4i1 ON t4(y);
SELECT x FROM t4 WHERE y=NULL;   -- Should return empty (NULL=NULL is UNKNOWN)
SELECT x FROM t4 WHERE y<33;     -- Should exclude rows where y IS NULL
```

**Root Cause:** Index optimization code doesn't handle NULL semantics:
- `col = NULL` should never use the index (always returns empty)
- Comparisons like `col < 33` with NULL values should exclude NULLs

**Fix:** In index optimization code:
- Detect `col = NULL` patterns and refuse index usage
- Ensure index scans properly exclude NULLs from comparison results

### Issue 4: WHERE Clause NULL Comparison (null-10.1)

**SQL Example:**
```sql
SELECT * FROM t0 WHERE t0.c0 > NULL;  -- Should return empty
```

**Root Cause:** Comparison `0 > NULL` evaluates to TRUE instead of UNKNOWN.

**Fix:** Ensure all comparison operators return UNKNOWN when either operand is NULL.

## Files to Modify

1. **src/executor/select/mod.rs** (lines 8890-8915)
   - Fix `is_valid_compound_order_by_term()` to validate column references

2. **src/schema/mod.rs** (Index struct)
   - Add `conflict_action: Option<ConflictAction>` field

3. **src/executor/insert.rs** (lines 2771-2846)
   - Pass conflict action to IdxInsert opcode

4. **src/executor/where_clause.rs** or **src/executor/wherecode.rs**
   - Fix index optimization for NULL comparisons

5. **src/vdbe/engine/mod.rs** (comparison opcodes)
   - Ensure NULL operand handling returns UNKNOWN

## Acceptance Criteria

```bash
make test-null
# Should show: 0 errors out of 42 tests
```

**Individual test verification:**
```sql
-- null-6.5: Should error
SELECT b FROM t1 UNION SELECT c FROM t1 ORDER BY t1.a;
-- Expected: Error "1st ORDER BY term does not match any column in the result set"

-- null-7.1: Should succeed with IGNORE
CREATE TABLE t2(a, b UNIQUE ON CONFLICT IGNORE);
INSERT INTO t2 VALUES(1,1);
INSERT INTO t2 VALUES(2,NULL);
INSERT INTO t2 VALUES(3,NULL);
INSERT INTO t2 VALUES(4,1);  -- Ignored
SELECT a FROM t2;
-- Expected: 1 2 3

-- null-8.11: Should return empty
SELECT x FROM t4 WHERE y=NULL;
-- Expected: (empty)

-- null-10.1: Should return empty
SELECT * FROM t0 WHERE t0.c0 > NULL;
-- Expected: (empty)
```

## Scope

- ORDER BY validation in compound SELECT
- UNIQUE constraint conflict action propagation
- NULL semantics in index lookups
- NULL comparison evaluation
