# Fix VDBE Hard Limit on UPDATE with String Concatenation

## Problem Statement

UPDATE statements with string concatenation fail with "internal error" when the table has 99+ rows, but succeed with ≤98 rows. This is a hard limit in the VDBE (Virtual Database Engine) that blocks 6 tests in insert2.test.

### Symptom

UPDATE t4 SET y='lots of data for the row where x=' || x || ' and y=' || y || ' - even more data to fill space';
Result: "Error: internal error" at 99+ rows

### Threshold Characteristics

- **Exact threshold**: 99 rows (works at ≤98, fails at ≥99)
- **Trigger condition**: 4+ operand string concatenation with long literals (>30 chars total)
- Examples: Works at ANY row count for ≤3 operands
- String length: Not the issue (tested 10-50 char strings all fail at 99 rows)

### Root Cause (Suspected)

This is NOT a Rust memory overflow (memory-safe). The error is from deliberate bounds checking. Evidence points to:

1. **Hard-coded limit (~100)** on VDBE operations or registers
2. **Fixed-size data structure** with capacity ~98 elements  
3. **Calculation-based limit** where concat operations × row count = threshold
4. **Label exhaustion** (labels allocated with next_label: -1 counting down)
5. **Register allocation overflow** (INSERT reserves 100 registers; UPDATE doesn't)

## Impact

### Blocked Tests (6 tests)
- insert2-3.4: UPDATE with string concat fails
- insert2-3.5 to 3.8: Depend on 3.4

### Overall Impact
- q4sxj moth: Blocked at 24/31 (77%)
- insert2.test: Blocked at 24/31
- Prevents reaching 80%+ pass rates

## Investigation Findings

### Code Locations to Investigate
- `src/executor/update.rs:compile_row_update_phase2()` - Phase 2 iteration
- `src/executor/update.rs:alloc_reg()` - Register allocation
- `src/executor/insert.rs:650-653` - INSERT reserves 100 registers
- `src/vdbe/expr.rs` - Expression compiler, concat handling

### Known Differences
- INSERT: `self.next_reg += 100;` (reserves registers)
- UPDATE: No such reservation (missing!)
- SELECT: No reservation (but not affected)

## Solution Strategy

### Phase 1: Instrumentation
1. Add debug logging to alloc_reg() to track register count
2. Add debug logging to alloc_label() to track label count
3. Run failing test and capture counts at failure
4. Compare with successful case (98 rows)

### Phase 2: Root Cause Identification
Identify which counter hits limit:
- Register allocation overflow?
- Label allocation overflow?
- Operation count overflow?

### Phase 3: Fix
Likely fixes:
1. If register overflow: Add `self.next_reg += 100;` to UPDATE (like INSERT)
2. If label overflow: Increase label allocation
3. If opcode overflow: Increase VDBE operation limit
4. If calculation-based: Find and fix the mathematical limit

### Phase 4: Verification
1. Test 99+ row UPDATE with string concat
2. Test 160+ rows (actual test case)
3. Run full insert2.test suite
4. Verify no regressions

## Success Criteria

- [ ] Identify exact limit (register/label/opcode/etc)
- [ ] Find the bounds check causing error
- [ ] Apply minimal fix
- [ ] insert2-3.4 passes
- [ ] insert2-3.5 to 3.8 pass
- [ ] insert2.test reaches ≥75% pass rate
- [ ] No regressions

## Key Files

- `src/executor/update.rs` - Primary file for modifications
- `src/executor/insert.rs` - Reference for register reservation pattern
- `src/vdbe/engine/mod.rs` - VDBE execution environment
- `src/vdbe/expr.rs` - Expression compilation

## Timeline

- **Discovered**: During q4sxj implementation  
- **Status**: Ready for detailed investigation
- **Blocking**: 6 test cases, prevents higher pass rates
