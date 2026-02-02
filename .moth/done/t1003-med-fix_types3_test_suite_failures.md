# Fix Types3 Test Suite Failures [med]

## Problem

The types3.test suite has 19 failures. These failures relate to TCL type representation and parameter binding - primarily TCL extension infrastructure rather than core SQL semantics.

## Failing Tests

| Test | Issue | Expected | Got |
|------|-------|----------|-----|
| types3-1.2 | tcl_variable_type for int | `int integer` | `integer` |
| types3-1.3 | tcl_variable_type for wideInt | `int integer` | `integer` |
| types3-1.4 | tcl_variable_type for double | `double real` | `real` |
| types3-1.5 | tcl_variable_type for bytearray | `bytearray blob` | `text` |
| types3-1.6 | tcl_variable_type for mixed bytearray | `bytearray text` | `text` |
| types3-2.1 | Query result type (blob) | `bytearray` | `` |
| types3-2.2 | Query result type (int) | `int` | `` |
| types3-2.4.1 | Query result type (double) | `double` | `` |
| types3-2.4.2 | Query result type (double) | `double` | `` |
| types3-3.2 | add_text_type function | Success | no such function |
| types3-3.3 | add_int_type function | Success | no such function |
| types3-3.4 | add_real_type function | Success | no such function |
| types3-3.5 | add_text_type function | Success | no such function |

## Root Cause Analysis

### Issue 1: Missing tcl_variable_type Implementation (8 failures)

**TCL Command:**
```tcl
set V [expr {int(1+2)}]
tcl_variable_type V
# Expected: "int" (TCL internal type)
```

**Root Cause:** In `src/tcl_ext/stubs.rs` (lines 305-322), `tcl_variable_type_cmd` is a stub that always returns empty string:

```rust
pub unsafe extern "C" fn tcl_variable_type_cmd(...) -> c_int {
    // For compatibility, just return empty string (unknown type)
    set_result_string(interp, "");
    TCL_OK
}
```

**Fix:** Implement proper TCL object type inspection using TCL C API:
- `Tcl_GetObjType()` to get the type
- Return "int", "wideInt", "double", "bytearray", or "" based on type

### Issue 2: Missing Bytearray Detection in Parameter Binding (2 failures)

**TCL Code:**
```tcl
set V [binary format a3 abc]
# V is now a bytearray type
db eval {SELECT typeof(:V)}
# Expected: blob (because V is bytearray)
# Got: text (bytearray converted to text)
```

**Root Cause:** In `src/tcl_ext/db.rs` (lines 1001-1043), `bind_tcl_variables` only checks for int/float parsing:

```rust
if let Ok(int_val) = value_str.parse::<i64>() {
    let _ = sqlite3_bind_int64(stmt, i, int_val);
} else if let Ok(float_val) = value_str.parse::<f64>() {
    let _ = sqlite3_bind_double(stmt, i, float_val);
} else {
    let _ = sqlite3_bind_text(stmt, i, value_str);  // Bytearray becomes text!
}
```

**Fix:** Check TCL object type before binding:
- If bytearray type, use `sqlite3_bind_blob()`
- Requires using TCL C API to inspect object type

### Issue 3: Missing Type Information on Query Results (4 failures)

**TCL Code:**
```tcl
set V [db one {SELECT 123}]
tcl_variable_type V
# Expected: "int" (TCL should receive typed object)
# Got: "" (receives string-only object)
```

**Root Cause:** In `src/tcl_ext/db.rs`, result values are returned via `set_result_string()` which creates string-only TCL objects. Integer/float/blob results should use:
- `Tcl_NewIntObj()` for integers
- `Tcl_NewDoubleObj()` for floats
- `Tcl_NewByteArrayObj()` for blobs

### Issue 4: Missing Test Infrastructure Functions (5 failures)

**SQL:**
```sql
SELECT add_text_type(1);  -- Error: no such function
```

**Root Cause:** The test functions `add_text_type`, `add_int_type`, `add_real_type` from SQLite's test1.c are not registered. These functions force a value to have multiple type representations for testing type coercion.

**Fix:** Register these test functions in the TCL extension:
```rust
// add_text_type(X): calls sqlite3_value_text() then returns via sqlite3_result_value()
// add_int_type(X): calls sqlite3_value_int64() then returns via sqlite3_result_value()
// add_real_type(X): calls sqlite3_value_double() then returns via sqlite3_result_value()
```

## Files to Modify

1. **src/tcl_ext/stubs.rs** (lines 305-322)
   - Implement `tcl_variable_type_cmd` to inspect TCL object types
   - Use `Tcl_GetObjType()` from TCL C API

2. **src/tcl_ext/db.rs** (lines 1001-1043)
   - Enhance `bind_tcl_variables` to detect bytearray type
   - Bind bytearrays as BLOB instead of TEXT

3. **src/tcl_ext/db.rs** (result handling)
   - Modify `db_onecolumn`, `db_eval` to return typed TCL objects
   - Use `Tcl_NewIntObj`, `Tcl_NewDoubleObj`, `Tcl_NewByteArrayObj`

4. **src/tcl_ext/helpers.rs**
   - Add helpers for creating typed TCL objects

5. **src/tcl_ext/ffi.rs**
   - Add FFI bindings for:
     - `Tcl_GetObjType`
     - `Tcl_NewIntObj`
     - `Tcl_NewDoubleObj`
     - `Tcl_NewByteArrayObj`
     - `Tcl_GetByteArrayFromObj`

6. **src/tcl_ext/stubs.rs** (or new module)
   - Register `add_text_type`, `add_int_type`, `add_real_type` functions

## Acceptance Criteria

```bash
make test-types3
# Should show: 0 errors out of 19 tests
```

**Individual test verification:**
```tcl
# Type binding
set V [expr {int(1+2)}]
set type [tcl_variable_type V]
# Expected: int

set V [binary format a3 abc]
set type [tcl_variable_type V]
# Expected: bytearray

db eval {SELECT typeof(:V)}
# Expected: blob (when V is bytearray)

# Result types
set V [db one {SELECT 123}]
tcl_variable_type V
# Expected: int

set V [db one {SELECT x'616263'}]
tcl_variable_type V
# Expected: bytearray

# Test functions
db eval {SELECT add_text_type(1)}
# Expected: Success (returns 1 with text type added)
```

## Scope

- TCL variable type inspection
- Bytearray detection in parameter binding
- Typed TCL object creation for query results
- Test infrastructure function registration

## Notes

This is primarily TCL extension infrastructure work. The core SQL engine type handling is correct; these failures are about properly representing types in the TCL interface.
