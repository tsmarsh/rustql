# Fix Collate1 Test Suite Failures [high]

## Problem

The collate1.test suite has 73 failures (32% pass rate). Custom collation sequences registered via TCL are not being used by RustQL's comparison and sorting engine, falling back to BINARY comparison.

## Failing Tests Summary

| Category | Pass | Fail | Issue |
|----------|------|------|-------|
| HEX collation | 0 | 7 | Not registered |
| NUMERIC collation | 0 | 3 | Not registered |
| Column defaults | 0 | 5 | COLLATE clause ignored |
| RTRIM collation | 0 | 2 | Not applied from TCL |
| Multi-field ORDER BY | 2 | 8 | Collations not coordinated |
| Nested COLLATE | 1 | 2 | Last-wins partial |
| Special char collations | 0 | 4 | Quote collation not registered |
| Error handling | 1 | 3 | No undefined collation errors |

## Root Cause Analysis

### Issue 1: TCL Collation Registration Not Implemented

**TCL Command:**
```tcl
db collate HEX hex_collate
proc hex_collate {lhs rhs} {
    # Compare hex strings as integers
    set lhsx [scan $lhs %x]
    set rhsx [scan $rhs %x]
    if {$lhsx < $rhsx} {return -1}
    if {$lhsx == $rhsx} {return 0}
    return 1
}
```

**Root Cause:** In `src/tcl_ext/db.rs` (lines 555-570), the `db collate` command is a no-op stub:

```rust
"collate" | "trace" | "profile" ... => {
    // Stub these methods - accept but ignore
    TCL_OK
}
```

**Fix:** Implement `db collate` to:
1. Parse collation name and TCL proc name
2. Call `connection.create_collation(name, callback)`
3. Store reference to TCL proc for callback invocation

### Issue 2: Collation Lookups Don't Use TCL Callbacks

**Root Cause:** In `src/vdbe/mem.rs` (lines 816-842), `compare_with_collation` only recognizes hardcoded collations:

```rust
pub fn compare_with_collation(&self, other: &Mem, collation: &str) -> Ordering {
    match collation.to_uppercase().as_str() {
        "NOCASE" => sa.to_ascii_lowercase().cmp(&sb.to_ascii_lowercase()),
        "RTRIM" => sa.trim_end().cmp(sb.trim_end()),
        _ => sa.cmp(&sb),  // Falls back to BINARY for custom collations!
    }
}
```

**Fix:** Query the connection's collation registry for custom collations:
1. Look up collation by name in connection's registry
2. If found, invoke the registered callback
3. If not found, use BINARY as fallback

### Issue 3: Column-Level Collations Not Applied

**SQL Example:**
```sql
CREATE TABLE t1(a COLLATE hex);
INSERT INTO t1 VALUES('0x5'), ('1'), ('0x45');
SELECT * FROM t1 ORDER BY a;
-- Expected: NULL, 1, 0x5, 0x45 (HEX ordering)
-- Got: NULL, 0x45, 0x5, 1 (BINARY ordering)
```

**Root Cause:** The column's default collation from schema is not being passed to Compare operations during table scans.

**Fix:**
1. Store column collations in schema during CREATE TABLE
2. Pass column collations through KeyInfo to Sort/Compare operations
3. Apply column default when no explicit COLLATE specified

### Issue 4: Error Handling for Undefined Collations

**SQL Example:**
```sql
SELECT a FROM x1 ORDER BY 1 COLLATE undefined_collation;
-- Expected: Error "no such collation sequence: undefined_collation"
-- Got: Silent fallback to BINARY
```

**Fix:** Add validation during compilation:
1. Check if collation exists in registry
2. If not found, raise error with descriptive message

## Required Collations for Test Suite

The test file registers these collations:

```tcl
# HEX collation - numeric comparison for hex strings
db collate HEX hex_collate
proc hex_collate {lhs rhs} {
    set lhs_ishex [regexp {^(0x|)[1234567890abcdefABCDEF]+$} $lhs]
    set rhs_ishex [regexp {^(0x|)[1234567890abcdefABCDEF]+$} $rhs]
    if {$lhs_ishex && $rhs_ishex} {
        set lhsx [scan $lhs %x]
        set rhsx [scan $rhs %x]
        if {$lhsx < $rhsx} {return -1}
        if {$lhsx == $rhsx} {return 0}
        return 1
    }
    if {$lhs_ishex} { return -1 }
    if {$rhs_ishex} { return 1 }
    return [string compare $lhs $rhs]
}

# NUMERIC collation - numeric comparison
db collate numeric numeric_collate
proc numeric_collate {lhs rhs} {
    if {$lhs == $rhs} {return 0}
    return [expr ($lhs>$rhs)?1:-1]
}

# Special quote collation
db collate {"""} [list string compare -nocase]
```

## Files to Modify

1. **src/tcl_ext/db.rs** (lines 555-570)
   - Implement `db collate` command
   - Store TCL proc reference for callback

2. **src/api/connection.rs** (lines 914-920)
   - Ensure `create_collation` stores callback properly
   - Add method to look up collation by name

3. **src/vdbe/mem.rs** (lines 816-842)
   - Modify `compare_with_collation` to query connection registry
   - Invoke custom collation callbacks

4. **src/schema/mod.rs**
   - Ensure column collations are stored in table schema

5. **src/executor/select/mod.rs**
   - Pass column collations through KeyInfo for Sort operations

6. **src/vdbe/engine/mod.rs** (lines 6384-6422)
   - Ensure Compare opcode uses connection's collation registry

7. **src/executor/where_clause.rs** or **src/executor/select/mod.rs**
   - Add validation for undefined collations during compilation

## Acceptance Criteria

```bash
make test-collate1
# Should show: 0 errors out of 59 tests

make test-collate2
make test-collate3
# All collation tests should pass
```

**Individual test verification:**
```tcl
# Register collations
db collate HEX hex_collate
db collate numeric numeric_collate

# HEX collation test
db eval {
    CREATE TABLE t1(c1, c2);
    INSERT INTO t1 VALUES(45, '0x2D');   -- hex(45)
    INSERT INTO t1 VALUES(NULL, NULL);
    INSERT INTO t1 VALUES(281, '0x119'); -- hex(281)
    SELECT c2 FROM t1 ORDER BY 1 COLLATE hex;
}
# Expected: {} 0x2D 0x119 (NULL, then 45, then 281 in numeric order)

# Column default collation
db eval {
    CREATE TABLE t2(a COLLATE hex);
    INSERT INTO t2 VALUES('0x5'), ('1'), ('0x45');
    SELECT * FROM t2 ORDER BY a;
}
# Expected: 1 0x5 0x45 (non-hex, then 5, then 69)

# Error handling
catch {db eval {SELECT 1 ORDER BY 1 COLLATE undefined}} msg
# Expected: Error containing "no such collation sequence"
```

## Scope

- TCL `db collate` command implementation
- Custom collation callback invocation
- Column default collation propagation
- Collation lookup in comparison operations
- Error handling for undefined collations

## Notes

This moth supersedes/extends `dlzmr-high-register-tcl-test-collations-for-collate-test-suite.md` with more detailed analysis and acceptance criteria.

Fixing this will also help collate2.test and collate3.test pass, as they use the same collation infrastructure.
