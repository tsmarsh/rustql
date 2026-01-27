# Fix numeric type handling, overflow, and literal interpretation in EXPR tests [high]

## Problem
The `expr.test` TCL suite highlights significant discrepancies in how `rustql` handles numeric types, including parsing and evaluating integer and floating-point literals, managing arithmetic operations under potential overflow conditions, and performing type conversions. Failures manifest as `invalid integer` errors, subtle but critical mismatches in expected large numeric values (both integer and real), and incorrect behavior when negating non-numeric types. These issues point to fundamental differences in `rustql`'s type system and numeric processing compared to SQLite.

## Failing Test Details
- **TCL Test File:** `sqlite3/test/expr.test`
- **Failing Sub-tests (examples covering various `expr-1.*` and `expr-13.*` tests):**
  - `expr-1.45d`: `! expr-1.45d expected: [4611686018427387904] got: []` (Large integer handling)
  - `expr-1.45e`: `! expr-1.45e expected: [-9223372036854775808] got: []` (Large negative integer handling)
  - `expr-1.45g`: `Error: invalid integer` (Integer parsing error)
  - `expr-9.1`: `Error: cannot negate non-numeric value` (Type checking for unary operators)
  - `expr-13.2`: `! expr-13.2 expected: [9223372036854775807] got: [9223372036854776000.0]` (Precision/conversion mismatch for large numbers)
  - ... (Many more similar errors across `expr-1.*` and `expr-13.*` ranges)

## Reference
- **Relevant SQLite C Source Files:**
  - `sqlite3/src/expr.c`: Contains logic for parsing and compiling numeric literals and arithmetic expressions.
  - `sqlite3/src/vdbe.c` / `sqlite3/src/vdbeapi.c`: VDBE opcodes that perform arithmetic, type conversions (e.g., `Opcode::Cast`), and handle value representation during execution.
  - `sqlite3/src/mem.c`: Central to SQLite's type system, defining how values (integers, reals, text, blobs, nulls) are stored, coerced, and checked for overflow/underflow.
  - `sqlite3/src/util.c`: Utility functions for string-to-numeric conversions and vice versa.

## Acceptance Criteria
This moth is considered done when the sub-tests related to numeric literal parsing, arithmetic operations (especially with large or boundary values), type conversions, and negation rules consistently produce the expected values and error messages as per SQLite's behavior.
To verify, run:
```bash
make test-expr
```
