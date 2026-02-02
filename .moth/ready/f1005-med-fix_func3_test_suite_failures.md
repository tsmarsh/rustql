# Fix Func3 Test Suite Failures [med]

## Problem

The func3.test suite has 25 failures (38% pass rate). These failures relate to user-defined function lifecycle management, likelihood function validation, and query optimizer behavior.

## Failing Tests

| Test | Issue | Expected | Got |
|------|-------|----------|-----|
| func3-1.4 | Function destructor (utf16be) | destroyed=1 | destroyed=0 |
| func3-2.2 | Function override destructor | destroyed=1 | destroyed=0 |
| func3-3.2 | DB close destructor | destroyed=1 | destroyed=0 |
| func3-4.1 | Invalid function registration | Error code 1 | Success (0) |
| func3-4.2 | Destructor after error | destroyed=1 | destroyed=0 |
| func3-5.8 | likelihood(123, 1.000001) | Error: out of range | Success: 123 |
| func3-5.9 | likelihood(123, -0.000001) | Error: out of range | Success: 123 |
| func3-5.10 | likelihood(123, 0.5+0.3) | Error: not constant | Success: 123 |
| func3-5.20 | EXPLAIN likelihood elimination | No likelihood opcode | Has likelihood opcode |
| func3-5.39 | EXPLAIN unlikely elimination | No unlikely opcode | Has unlikely opcode |
| func3-5.59 | EXPLAIN likely elimination | No likely opcode | Has likely opcode |
| func3-6.0 | sqlite3_create_function_v2 return | {} | 0 |

## Root Cause Analysis

### Issue 1: Function Destruction/Lifecycle (5 failures)

**TCL Commands:**
```tcl
# Create function with destructor
sqlite3_create_function_v2 db f1 1 utf16be -func f1 -destroy destruct
# Later: expect destructor to be called when function is replaced or DB closed
```

**Root Cause:** In `src/tcl_ext/stubs.rs` (lines 101-102), `sqlite3_create_function_v2` is just a stub:

```rust
// Registered as: test_stub_return_zero
// Always returns 0, no actual function registration or destructor tracking
```

**What's Missing:**
- Tracking of user-defined functions with their destructors
- Destructor invocation when function is overridden
- Destructor invocation when database is closed
- Validation of -step/-final vs -func conflicts (SQLITE_MISUSE)

**Fix:** Implement real `sqlite3_create_function_v2`:
1. Store function registrations with destructor callbacks
2. When overriding, call old destructor
3. On DB close, call all destructors
4. Validate parameter conflicts

### Issue 2: Likelihood Validation (3 failures)

**SQL Examples:**
```sql
SELECT likelihood(123, 1.000001);  -- Error: Y > 1.0
SELECT likelihood(123, -0.000001); -- Error: Y < 0.0
SELECT likelihood(123, 0.5+0.3);   -- Error: Y is not constant
```

**Root Cause:** In `src/functions/scalar.rs` (lines 957-969), `func_likelihood` doesn't validate:

```rust
pub fn func_likelihood(args: &[Value]) -> Result<Value> {
    // Just returns first argument, no validation of second argument
    Ok(args.first().cloned().unwrap_or(Value::Null))
}
```

**SQLite Behavior:** Validation must happen at **compile time** (during statement preparation):
1. Second argument must be a constant (literal number)
2. Value must be between 0.0 and 1.0 (inclusive)
3. Expressions like `0.5+0.3` are not allowed

**Fix:** Add compile-time validation in `src/vdbe/expr.rs` (compile_function):
```rust
if name.eq_ignore_ascii_case("likelihood") {
    // Validate second argument is constant
    // Validate value is in [0.0, 1.0]
    // Raise error if validation fails
}
```

### Issue 3: Optimizer Elimination (3 failures)

**SQL Example:**
```sql
EXPLAIN SELECT likelihood(min(1.0+'2.0',4*11), 0.5);
-- Expected bytecode: Only "Function min()" - no likelihood
-- Got bytecode: Both "Function min()" AND "Function likelihood()"
```

**SQLite Behavior:** `likelihood()`, `unlikely()`, and `likely()` are query hints that:
1. Return their first argument unchanged
2. Are completely eliminated during compilation
3. Don't appear in VDBE bytecode at all

**Root Cause:** In `src/vdbe/expr.rs` (lines 181-187), `compile_function` treats these as regular functions.

**Fix:** Add special-case handling in `compile_function`:
```rust
match name.to_lowercase().as_str() {
    "likelihood" | "unlikely" | "likely" => {
        // Only compile first argument
        // Skip generating Function opcode
        // Discard second argument (for likelihood)
        self.compile_expr(&args[0], dest)?;
        return Ok(());
    }
    // ... rest of function handling
}
```

### Issue 4: Return Type (1 failure)

**TCL Command:**
```tcl
sqlite3_create_function_v2 db nofunc 1 utf8
# Expected: {} (empty list)
# Got: 0 (integer)
```

**Root Cause:** Stub returns integer 0 via `test_stub_return_zero()` instead of empty TCL list.

**Fix:** Return empty result from the stub.

## Files to Modify

1. **src/tcl_ext/stubs.rs** (lines 101-102)
   - Replace stub with real `sqlite3_create_function_v2` implementation
   - Add function lifecycle tracking

2. **src/tcl_ext/user_func.rs** (new or existing)
   - Implement destructor callback storage
   - Implement cleanup on DB close

3. **src/vdbe/expr.rs** (lines 767-784, compile_function)
   - Add compile-time validation for likelihood
   - Add special-case elimination for likelihood/unlikely/likely

4. **src/functions/scalar.rs** (lines 957-969)
   - Remove runtime implementation (validation moves to compiler)

5. **src/api/connection.rs**
   - Add destructor tracking to function registration
   - Add destructor invocation on close

## Acceptance Criteria

```bash
make test-func3
# Should show: 0 errors out of 40 tests
```

**Individual test verification:**
```tcl
# Destructor lifecycle
set destroyed 0
proc destruct {} { set ::destroyed 1 }
sqlite3_create_function_v2 db f1 1 utf8 -func f1 -destroy destruct
sqlite3_create_function_v2 db f1 1 utf8 -func f1_v2  ;# Override
# Expected: destroyed == 1

# Likelihood validation
db eval {SELECT likelihood(123, 1.000001)}
# Expected: Error "second argument to likelihood() must be a constant between 0.0 and 1.0"

db eval {SELECT likelihood(123, 0.5+0.3)}
# Expected: Error (expression is not constant)

# Optimizer elimination
set bytecode [db eval {EXPLAIN SELECT likelihood(min(1,2), 0.5)}]
# Expected: No "likelihood" in bytecode, only "min"
```

## Scope

- sqlite3_create_function_v2 implementation with destructor support
- Function lifecycle management (override, close)
- Compile-time validation for likelihood function
- Query optimizer elimination of hint functions

## Notes

The likelihood/unlikely/likely functions are optimizer hints only - they don't affect runtime behavior, just query planning. The compiler should eliminate them entirely from the generated bytecode.
