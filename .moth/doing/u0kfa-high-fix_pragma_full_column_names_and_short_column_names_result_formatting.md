# Fix PRAGMA full_column_names and short_column_names Result Formatting

## Problem

Previous moths (er0l3, u4lm3) claimed these PRAGMAs were implemented but **18 tests are still failing** in select1.test due to incorrect column name formatting in results.

The PRAGMAs exist but the column name generation logic is broken or incomplete.

## Failing Tests (18 total)

### Column names missing table prefix when full_column_names=ON
```
select1-6.1.1 expected: [0 {test1.f1 11 test1.f1 33}]
select1-6.1.1 got:      [0 {f1 11 f1 33}]

select1-6.9.9 expected: [test1.f1 11 test1.f2 22]
select1-6.9.9 got:      [a.f1 11 b.f2 22]

select1-6.9.10 expected: [test1.f1 11 test2.t1 abc]
select1-6.9.10 got:      [f1 11 t1 abc]

select1-6.9.11 expected: [test1.f1 11 test1.f2 22]
select1-6.9.11 got:      [a.f1 11 b.f2 22]

select1-6.9.12 expected: [test1.f1 11 test2.t1 abc]
select1-6.9.12 got:      [f1 11 t1 abc]

select1-6.9.15 expected: [test1.f1 11 test1.f1 11]
select1-6.9.15 got:      [a.f1 11 b.f1 11]

select1-6.9.16 expected: [test1.f1 11 test2.t1 abc]
select1-6.9.16 got:      [f1 11 t1 abc]
```

### Using alias prefix instead of no prefix when short_column_names=ON
```
select1-6.7 expected: [0 {f1 11 t1 abc f1 33 t1 abc}]
select1-6.7 got:      [0 {A.f1 11 t1 abc A.f1 33 t1 abc}]

select1-6.9.2 expected: [0 {f1 11 f1 11 f1 33 f1 33 ...}]
select1-6.9.2 got:      [0 {A.f1 11 B.f1 11 A.f1 11 B.f1 33 ...}]

select1-6.9.13 expected: [f1 11 f1 11]
select1-6.9.13 got:      [a.f1 11 b.f1 11]
```

### Alias prefix needed but not included
```
select1-6.9.6 expected: [a.f1 11 a.f2 22 b.f1 11 b.f2 22]
select1-6.9.6 got:      [f1 11 f2 22 f1 11 f2 22]
```

### Wrong format for column names (spaces around dot)
```
select1-6.9.3 expected: [{test1 . f1} 11 {test1 . f2} 22]
select1-6.9.3 got:      [test1.f1 11 test1.f2 22]
```

### JOIN returning wrong row count
```
select1-6.9.1 expected: [0 {11 11 11 33 33 11 33 33}]  (8 values)
select1-6.9.1 got:      [0 {11 11 11 33 33 33}]  (6 values)
```

### Ambiguous column error message format
```
select1-6.8c expected: [1 {ambiguous column name: A.f1}]
select1-6.8c got:      [1 {ambiguous column name: f1}]
```

## SQLite Behavior Reference

```sql
-- Default: short_column_names=ON, full_column_names=OFF
SELECT f1 FROM test1;           -- Column name: "f1"
SELECT test1.f1 FROM test1;     -- Column name: "f1"

-- full_column_names=ON overrides short_column_names
PRAGMA full_column_names=ON;
SELECT f1 FROM test1;           -- Column name: "test1.f1"
SELECT test1.f1 FROM test1;     -- Column name: "test1.f1"

-- With aliases, alias takes precedence
SELECT f1 AS x FROM test1;      -- Column name: "x" (always)

-- With table aliases
SELECT a.f1 FROM test1 AS a;
-- short_column_names=ON: "f1"
-- full_column_names=ON: "test1.f1" (real table name, not alias)
```

## Root Cause

The column name generation in `src/executor/select.rs` and/or `src/api/stmt.rs` is using:
1. Table aliases instead of real table names
2. Not respecting the PRAGMA settings correctly
3. Missing table prefix when full_column_names=ON

## Files to Investigate

- `src/executor/select.rs` - ResultRow column name generation
- `src/api/stmt.rs` - Column metadata
- `src/vdbe/engine.rs` - PRAGMA handling
- `src/schema/mod.rs` - Connection flags for PRAGMAs
