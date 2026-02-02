# Fix LIMIT Test Suite Failures [high]

## Problem

The limit.test suite has 23 failures (81% pass rate). These failures affect LIMIT/OFFSET behavior with ORDER BY, INSERT SELECT, DISTINCT, and parameter validation.

## Failing Tests

| Test | Issue | Expected | Got |
|------|-------|----------|-----|
| limit-1.6 | Cartesian product + ORDER BY + LIMIT | Sorted first 5 rows | Wrong rows returned |
| limit-1.7 | Cartesian product + ORDER BY + LIMIT + OFFSET | Sorted rows 32-36 | Wrong rows returned |
| limit-4.1 | Complex INSERT sequence | 10240 rows | Query aborted |
| limit-4.2 | Depends on limit-4.1 | Values exist | Empty (cascade) |
| limit-4.3 | Depends on limit-4.1 | 1000 | Empty (cascade) |
| limit-5.1 | INSERT SELECT + ORDER BY + LIMIT | 2 rows inserted | Empty result |
| limit-5.2 | INSERT SELECT + ORDER BY DESC + LIMIT | 2 rows inserted | Empty result |
| limit-8.1 | DISTINCT + LIMIT | 5 distinct values | Empty |
| limit-8.2 | DISTINCT + LIMIT + OFFSET | 5 distinct values | Empty |
| limit-8.3 | DISTINCT + LIMIT + OFFSET | 5 distinct values | Empty |
| limit-10.4 | Float LIMIT parameter | Error: datatype mismatch | Success with 9 |
| limit-10.5 | String LIMIT parameter | Error: datatype mismatch | Success with all rows |

## Root Cause Analysis

### Issue 1: Cartesian Product with ORDER BY + LIMIT (limit-1.6, limit-1.7)

**SQL Example:**
```sql
SELECT * FROM t1 AS a, t1 AS b ORDER BY a.x, b.x LIMIT 5;
-- t1 has 32 rows, so Cartesian product = 1024 rows
-- Expected: First 5 rows sorted by (a.x, b.x)
-- Got: Wrong rows with incorrect column values
```

**Root Cause:** When compiling a Cartesian product with ORDER BY:
1. Rows should be inserted into a sorter with proper ORDER BY key formation
2. The sorter should then output rows with LIMIT applied
3. But the ORDER BY expressions are not being properly evaluated for multi-table joins

**Location:** `src/executor/select/mod.rs` lines 578-620 (ORDER BY setup) and 2100-2170 (output compilation)

**Fix:** Ensure ORDER BY expressions for Cartesian products correctly reference columns from both tables when building sorter keys.

### Issue 2: INSERT SELECT with ORDER BY + LIMIT (limit-5.1, limit-5.2)

**SQL Example:**
```sql
INSERT INTO t5 SELECT x-y, x+y FROM t1 WHERE x BETWEEN 10 AND 15 ORDER BY x LIMIT 2;
SELECT * FROM t5;
-- Expected: 2 rows with calculated values
-- Got: Empty (NULLs inserted)
```

**Root Cause:** When INSERT SELECT has ORDER BY/LIMIT:
1. SELECT destination is changed to a sorter (SelectDest::Sorter)
2. After sorting, rows go through `compile_order_by_output()`
3. But the final output isn't being routed to the INSERT table cursor

**Location:** `src/executor/select/mod.rs` lines 8336-8386 (compile_order_by_output)

**Fix:** Ensure `SelectDest::Table` case in ORDER BY output properly inserts sorted rows into the table cursor.

### Issue 3: Query Abort on Complex INSERT Sequences (limit-4.1, 4.2, 4.3)

**SQL Example:**
```sql
CREATE TABLE t3(x);
INSERT INTO t3 SELECT x FROM t1 ORDER BY x LIMIT 10 OFFSET 1;
INSERT INTO t3 SELECT x+(SELECT max(x) FROM t3) FROM t3;
-- Repeated 10 times (exponential growth)
-- Expected: 10240 rows
-- Got: "query aborted" error
```

**Root Cause:** The error "query aborted" suggests:
1. State not properly reset between INSERT operations
2. Register or cursor allocation exhaustion
3. Label collision or improper resolution
4. Recursive SELECT failing when source table is being modified

**Location:** Multiple areas:
- `src/executor/select/mod.rs` - Compiler state reset
- `src/executor/insert.rs` - INSERT source compilation
- `src/vdbe/engine/mod.rs` - Query execution

**Fix:** Debug the specific abort cause and fix state management between consecutive INSERT operations.

### Issue 4: DISTINCT with ORDER BY + LIMIT (limit-8.1, 8.2, 8.3)

**Note:** These tests depend on limit-4.1 which fails, so t3 is empty. However, there may be an independent bug in DISTINCT + LIMIT interaction.

**SQL Example:**
```sql
SELECT DISTINCT cast(round(x/100) as integer) FROM t3 LIMIT 5;
-- Expected: 0 1 2 3 4
-- Got: Empty (because t3 is empty from cascade failure)
```

**Fix:** First fix limit-4.1, then verify DISTINCT + LIMIT works correctly.

### Issue 5: Parameter Type Validation (limit-10.4, limit-10.5)

**SQL Example:**
```sql
-- TCL: set limit 1.5
SELECT x FROM t1 WHERE x<10 LIMIT :limit;
-- Expected: Error "datatype mismatch"
-- Got: Returns row with x=9 (1.5 used as LIMIT somehow)

-- TCL: set limit "hello world"
SELECT x FROM t1 WHERE x<10 LIMIT :limit;
-- Expected: Error "datatype mismatch"
-- Got: Returns all rows (string treated as NULL or -1?)
```

**Root Cause:** LIMIT/OFFSET parameters should be validated as integers at runtime. Non-integer values should raise "datatype mismatch" error.

**Location:**
- `src/executor/select/mod.rs` lines 8414-8431 (compile_limit)
- `src/vdbe/engine/mod.rs` (LIMIT opcode execution)

**Fix:** Add type checking when LIMIT/OFFSET values are read:
1. Check if value is integer type
2. If not, raise "datatype mismatch" error
3. Don't silently coerce or ignore non-integer values

## Files to Modify

1. **src/executor/select/mod.rs** (lines 578-620, 2100-2170)
   - Fix ORDER BY key formation for Cartesian products

2. **src/executor/select/mod.rs** (lines 8336-8386)
   - Fix `compile_order_by_output()` for INSERT SELECT destination

3. **src/executor/select/mod.rs** (lines 8414-8431)
   - Add LIMIT parameter type validation

4. **src/vdbe/engine/mod.rs** (multiple areas)
   - Add runtime type checking for LIMIT values
   - Debug query abort on complex INSERT sequences

5. **src/executor/insert.rs**
   - Ensure proper state reset between INSERT operations

## Acceptance Criteria

```bash
make test-limit
# Should show: 0 errors out of 123 tests
```

**Individual test verification:**
```sql
-- Cartesian product with ORDER BY + LIMIT
CREATE TABLE t1(x INT, y INT);
INSERT INTO t1 SELECT i, 10-i/4 FROM generate_series(0,31) AS s(i);
SELECT * FROM t1 AS a, t1 AS b ORDER BY a.x, b.x LIMIT 5;
-- Expected: (0,5,0,5), (0,5,1,5), (0,5,2,5), (0,5,3,5), (0,5,4,5)

-- INSERT SELECT with ORDER BY + LIMIT
CREATE TABLE t5(x, y);
INSERT INTO t5 SELECT x-y, x+y FROM t1 WHERE x BETWEEN 10 AND 15 ORDER BY x LIMIT 2;
SELECT * FROM t5;
-- Expected: 2 rows

-- Parameter type validation
-- TCL: set limit 1.5
SELECT x FROM t1 LIMIT :limit;
-- Expected: Error "datatype mismatch"
```

## Scope

- Cartesian product ORDER BY key formation
- INSERT SELECT with ORDER BY/LIMIT destination routing
- Query state management for complex INSERT sequences
- LIMIT parameter type validation
- DISTINCT + ORDER BY + LIMIT interaction
