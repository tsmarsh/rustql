# Implement PRAGMA case_sensitive_like

The `case_sensitive_like` pragma controls whether the LIKE operator is case-sensitive.

## Current Behavior

```sql
PRAGMA case_sensitive_like=on;
SELECT x FROM t1 WHERE x LIKE 'abc';
-- Expected: [abc]
-- Got:      [ABC abc]  (still case-insensitive)
```

The pragma is being ignored - LIKE always operates case-insensitively.

## Required Implementation

1. Store `case_sensitive_like` flag in database connection state
2. `PRAGMA case_sensitive_like=on` sets flag to true
3. `PRAGMA case_sensitive_like=off` sets flag to false (default)
4. `PRAGMA case_sensitive_like` (no arg) returns current value without changing it
5. LIKE operator must check this flag when comparing characters

## Affected Tests

- like-1.5.x through like-1.10 (10+ tests)
- pragma.test also tests this pragma

## Files to Modify

- `src/pragma.rs` - handle the pragma
- `src/func.rs` or `src/vdbe.rs` - pass flag to LIKE implementation
- `src/expr.rs` - LIKE comparison logic needs to respect the flag

## SQLite Behavior

- Default: case-insensitive for ASCII A-Z/a-z
- With `case_sensitive_like=on`: exact byte comparison
- GLOB is always case-sensitive (unaffected by this pragma)
