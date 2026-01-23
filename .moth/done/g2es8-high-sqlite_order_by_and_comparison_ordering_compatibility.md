# SQLite ORDER BY and Comparison Ordering Compatibility

Bring all ordering behavior in line with SQLite to improve select1.test compatibility.

## Current Failures (select1.test)

### 1. ORDER BY Column Index with Unary Plus
- **Test**: select1-4.9.2
- **Query**: `SELECT * FROM t5 ORDER BY +2`
- **Expected**: `{2 9 1 10}` (sorted by column 2)
- **Got**: `{1 10 2 9}` (original order)
- **Issue**: `+2` should be treated as column index 2, not constant expression

### 2. ORDER BY with Alias Expressions
- **Tests**: select1-10.3, select1-10.4
- **Query**: `SELECT f1-23 AS x FROM test1 ORDER BY abs(x)`
- **Expected**: `{10 -12}` (ordered by abs value)
- **Got**: `{-12 10}` (wrong order)
- **Issue**: Alias `x` not resolved in ORDER BY expressions

### 3. WHERE Clause Alias Resolution
- **Test**: select1-10.6
- **Query**: `SELECT f1-22 AS x, f2-22 AS y FROM test1 WHERE x>0 AND y<50`
- **Expected**: `{11 22}` (filtered rows)
- **Got**: `{-11 0 11 22}` (no filtering)
- **Issue**: Aliases `x` and `y` not resolved in WHERE clause

### 4. Type Comparison Ordering in Aggregates
- **Tests**: select1-2.8.1, select1-2.13.1
- **Query**: `SELECT min(a) FROM t3` (t3.a has: 'abc', NULL, 11, 33)
- **Expected min**: `11` (integers sort before text)
- **Expected max**: `abc` (text sorts after integers)
- **Got**: `33` for both
- **Issue**: SQLite type affinity ordering: NULL < INTEGER/REAL < TEXT < BLOB

### 5. COLLATE Support in ORDER BY
- **Test**: select1-10.7
- **Query**: `SELECT f1 COLLATE nocase AS x FROM test1 ORDER BY x`
- **Expected**: `{11 33}`
- **Got**: `{{} {}}`
- **Issue**: COLLATE clause not supported

## Implementation Tasks

1. [ ] **Fix ORDER BY +N**: Detect unary plus on integer literal and treat as column index
2. [ ] **Alias resolution in ORDER BY**: Resolve SELECT aliases in ORDER BY expressions
3. [ ] **Alias resolution in WHERE**: Allow SELECT aliases in WHERE (SQLite extension)
4. [ ] **Type affinity comparison**: Implement SQLite's type ordering for min/max
5. [ ] **COLLATE support**: Parse and apply COLLATE clause in expressions

## Files to Modify

- `src/executor/select.rs` - ORDER BY compilation, alias resolution
- `src/vdbe/engine.rs` - Type comparison in aggregates
- `src/parser/grammar.rs` - COLLATE clause parsing (if not present)

## References

- SQLite type affinity: https://www.sqlite.org/datatype3.html
- SQLite expression evaluation: https://www.sqlite.org/lang_expr.html

## Expected Impact

Fixing these issues should improve select1.test from ~71% to ~75%+ passing.
