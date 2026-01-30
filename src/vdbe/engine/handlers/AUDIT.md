# Handler SQLite Compatibility Audit

This document tracks differences between the RustQL opcode handlers and SQLite's vdbe.c implementation.

## Fixed Issues

The following issues were identified by comparing with SQLite vdbe.c and have been fixed:

1. **IfNotZero** - Now only decrements if positive, jumps if non-zero (was always decrementing)
2. **OffsetLimit** - Now computes limit+offset (was computing limit-offset, completely wrong)
3. **MustBeInt** - NULL now correctly jumps/errors (was silently continuing)
4. **DecrJumpZero** - Added underflow guard for i64::MIN
5. **AddImm** - Now uses wrapping unsigned arithmetic like SQLite
6. **Return** - Now checks if P1 is an integer before jumping; falls through if P3 is non-zero and P1 is not an integer (used with OP_BeginSubrtn)
7. **Affinity** - Added missing affinity types '@' (NONE) and 'F' (FLEXNUM)

---

## Needs Verification

### Gosub/Yield PC Handling

SQLite stores the current instruction index and uses `pOp = &aOp[x]` followed by implicit `pOp++` in the main loop. RustQL's exec_op increments PC before execution, so `*ctx.pc` is already the next instruction.

**Status:** Likely correct since RustQL stores the already-incremented PC which points to the next instruction (the return address).

---

## Verified Correct

### Affinity Characters
- '@' (0x40) = NONE ✓
- 'A' (0x41) = BLOB ✓
- 'B' (0x42) = TEXT ✓
- 'C' (0x43) = NUMERIC ✓
- 'D' (0x44) = INTEGER ✓
- 'E' (0x45) = REAL ✓
- 'F' (0x46) = FLEXNUM ✓

### Comparison Operators (Eq, Ne, Lt, Le, Gt, Ge)
- NULL handling with NULLEQ flag ✓
- JUMPIFNULL flag ✓
- Affinity application via compare_with_affinity ✓

### Data Movement (Integer, Int64, Real, String8, Null, Copy, SCopy, Move, IntCopy)
- Verified against SQLite vdbe.c ✓

---

## Test Coverage

- 143 unit tests for handlers
- Handlers verified against SQLite vdbe.c source

## Next Steps

1. Run TCL test suite: `make test-basic`
2. Add handler for InitCoroutine (used with Yield/EndCoroutine)
