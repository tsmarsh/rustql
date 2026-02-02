# Register TCL Test Collations For Collate Test Suite [high]

## Problem
The SQLite TCL test suite (collate1, collate2, collate3) uses custom test 
collations that aren't registered in the RustQL TCL extension. This causes
all collation tests to fail with incorrect sort orders.

## Failing Test Examples
From `collate1.test`:
```
Expected: {} 0x119 0x2D  (NULL-first, then numeric order)
Got:      {} 323831 3435 (wrong interpretation of hex values)
```

## Required TCL Test Collations
The test suite registers these collations via TCL:
1. `c1` - Case-insensitive text comparison
2. `c2` - Reverse of c1
3. `hex_collate` - Compares hex strings as integers
4. `numeric` - Numeric comparison for text values
5. Various collation needed callbacks

## SQLite Test Infrastructure
From `sqlite3/test/collate1.test`:
```tcl
proc c1 {a b} { string compare $a $b }
proc c2 {a b} { string compare $b $a }
db collate c1 c1
db collate c2 c2
```

## Reference
- `sqlite3/test/collate1.test` - Collation test definitions
- `sqlite3/test/tester.tcl` - Test harness collation setup
- `src/tcl_ext/stubs.rs` - TCL extension implementation

## Acceptance Criteria
```bash
make test-collate1
make test-collate2
make test-collate3
```
All collation tests should use the proper test collations.

## Scope
- `src/tcl_ext/stubs.rs` - Add `db collate` command handler
- `src/api/connection.rs` - Collation registration API
