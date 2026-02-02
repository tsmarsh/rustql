# Fix BETWEEN Test Suite Failures [high]

## Problem

The between.test suite has 17 failures. These failures affect BETWEEN operator behavior with dynamic expressions and collation sequences.

## Failing Tests

| Test | Issue | Expected | Got |
|------|-------|----------|-----|
| between-1.2.1 | Dynamic upper bound | 2 rows | 0 rows |
| between-1.3.1 | Dynamic lower bound | 2 rows | 10 rows |
| between-1.4 | Both bounds dynamic | 2 rows | 0 rows |
| between-2.1.1 | TEXT BETWEEN with type mixing | 0 | 1 |
| between-2.1.2 | COLLATE binary ignored | 0 | 1 |
| between-2.1.3 | COLLATE nocase ignored | 0 | 1 |
| between-2.1.4 | Column default COLLATE nocase | 1 | 0 |
| between-2.1.5 | Explicit COLLATE nocase | 1 | 0 |

## Root Cause Analysis

### Issue 1: Index Optimization with Non-Constant Bounds (3 failures)

**SQL Examples:**
```sql
-- Test data: t1 has columns w, x, y, z with index on w
SELECT * FROM t1 WHERE w BETWEEN 5 AND 65-y ORDER BY +w;
-- Expected: rows where w=5 and w=6 (calculated from 65-y)
-- Got: 0 rows (index optimization fails with dynamic bound)

SELECT * FROM t1 WHERE w BETWEEN 41-y AND 6 ORDER BY +w;
-- Expected: rows where w=5 and w=6
-- Got: 10 rows (wrong range calculated)
```

**Root Cause:** In `src/executor/where_clause.rs` (lines 742-773), the index optimization code assumes BETWEEN bounds are constant literals. When bounds contain expressions like `65-y` (involving table columns), the bounds are not evaluated at runtime.

**Fix:**
1. Detect when BETWEEN bounds contain non-constant expressions
2. Either evaluate expressions at runtime before index seek, OR
3. Disable index optimization and fall back to table scan with filter

### Issue 2: COLLATE Handling in BETWEEN Comparisons (5 failures)

**SQL Examples:**
```sql
-- Table: t1(x TEXT, y TEXT COLLATE nocase)
-- Row: x='0', y='abc'

SELECT x BETWEEN 1 AND '5' FROM t1;
-- Expected: 0 (TEXT '0' not between INTEGER 1 and TEXT '5')
-- Got: 1 (wrong type affinity handling)

SELECT y BETWEEN 'A' AND 'B' FROM t1;  -- y has COLLATE nocase
-- Expected: 1 ('abc' is between 'A' and 'B' case-insensitively)
-- Got: 0 (column default collation not applied)

SELECT y COLLATE nocase BETWEEN 'A' AND 'B' FROM t1;
-- Expected: 1
-- Got: 0 (explicit COLLATE wrapper not passed to comparison)
```

**Root Cause:** When BETWEEN is split into two comparisons (`expr >= low` and `expr <= high`) in `src/executor/where_clause.rs`, the collation information from COLLATE wrappers is not propagated to the Ge/Le opcodes.

In `src/vdbe/expr.rs` (lines 397-453), `compile_comparison`:
1. Compiles both operands
2. Executes comparison opcode
3. Applies Affinity opcode with collation AFTER comparison
4. But comparison opcode has no knowledge of collation

**Fix:**
1. Extract collation from Collate wrapper before comparison
2. Pass collation as P4 parameter to Ge, Le, Eq opcodes
3. Ensure column default collations from schema are applied

## Files to Modify

1. **src/executor/where_clause.rs** (lines 742-773)
   - Add constant expression validation for BETWEEN bounds
   - Handle dynamic bounds at runtime or disable index optimization

2. **src/vdbe/expr.rs** (lines 397-453)
   - Extract collation from Collate wrapper in `compile_comparison`
   - Pass collation to comparison opcodes

3. **src/vdbe/engine/mod.rs** (Ge, Le, Eq opcodes)
   - Use P4 collation parameter in string comparisons

4. **src/executor/select/mod.rs** (BETWEEN compilation)
   - Ensure collation is preserved when splitting BETWEEN

## Acceptance Criteria

```bash
make test-between
# Should show: 0 errors out of 17 tests
```

**Individual test verification:**
```sql
-- Index with dynamic bounds
CREATE TABLE t1(w INT, x, y, z);
CREATE INDEX i1w ON t1(w);
INSERT INTO t1 VALUES(5, 2, 36, 38);
INSERT INTO t1 VALUES(6, 2, 49, 51);

SELECT * FROM t1 WHERE w BETWEEN 5 AND 65-y ORDER BY +w;
-- Expected: 5 2 36 38, 6 2 49 51

-- COLLATE in BETWEEN
CREATE TABLE t2(x TEXT, y TEXT COLLATE nocase);
INSERT INTO t2 VALUES('0', 'abc');

SELECT y BETWEEN 'A' AND 'B' FROM t2;
-- Expected: 1

SELECT y COLLATE nocase BETWEEN 'A' AND 'B' FROM t2;
-- Expected: 1
```

## Scope

- BETWEEN index optimization with dynamic expressions
- COLLATE propagation in BETWEEN comparisons
- Type affinity handling in BETWEEN with mixed types
