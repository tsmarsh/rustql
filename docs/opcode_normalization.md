# Opcode Normalization Rules

This document describes the normalization rules used when comparing RustQL's compiled bytecode against SQLite's EXPLAIN output.

## Purpose

RustQL aims to emit SQLite-equivalent bytecode, but exact instruction-for-instruction matching is neither practical nor necessary. This document defines which differences are acceptable and why.

## Acceptable Differences

### 1. Address Values (p2 jump targets)

Jump targets may differ due to:
- Different instruction ordering during compilation
- Optimization passes that reorder instructions
- Different handling of setup/cleanup code

**Example:**
```
SQLite: Init 0 5 0    # Jump to address 5
RustQL: Init 0 4 0    # Jump to address 4
```

This is acceptable as long as the control flow structure is equivalent.

### 2. Register Allocation (p1, p3)

Register numbers may differ because:
- Different register allocation algorithms
- Different temporary storage strategies
- Different lifetime analysis

**Example:**
```
SQLite: Integer 1 3 0    # Store in register 3
RustQL: Integer 1 1 0    # Store in register 1
```

This is acceptable as long as data flow is correct.

### 3. Trace Opcode

SQLite includes `Trace` opcodes for debugging when compiled with appropriate flags. RustQL omits these in normal compilation.

**Normalization:** Remove all `Trace` opcodes before comparison.

### 4. Explain Opcode

The `Explain` opcode appears in EXPLAIN output but is not executed during normal query processing. It carries metadata about the query plan.

**Normalization:** Remove all `Explain` opcodes before comparison.

### 5. AggStep vs AggStep1

SQLite uses `AggStep1` for single-argument aggregate functions as an optimization. RustQL may use `AggStep` with appropriate flags instead.

**Normalization:** Treat `AggStep1` as equivalent to `AggStep`.

### 6. Init Jump Target

The `Init` opcode's p2 value (jump target) varies based on the total instruction count and may differ between implementations.

**Normalization:** Compare only that `Init` is the first opcode, not its exact parameters.

### 7. Init and Goto Control Flow

SQLite uses `Init` to jump to the query setup code at the end of the bytecode, then `Goto` to jump back to the main execution. RustQL starts execution directly at the first instruction without this indirection.

**Normalization:** Remove `Init` and `Goto` opcodes when comparing sequences.

### 8. Transaction Opcode

SQLite emits `Transaction` opcodes for read locking. RustQL handles transaction management implicitly or at a different layer.

**Normalization:** Remove `Transaction` opcodes.

### 9. Close Opcode Placement

Cursor `Close` opcodes may appear at different positions in the instruction stream. SQLite may omit them when cursors are implicitly closed at statement end.

**Normalization:** Remove `Close` opcodes.

### 10. Inline Constants vs Explicit Load

SQLite may store integer constants inline in arithmetic opcodes (appearing after `Halt` in EXPLAIN output). RustQL loads constants explicitly before use.

**Example:**
```
SQLite: Init -> Add -> ResultRow -> Halt -> Integer -> Goto
RustQL: Integer -> Integer -> Add -> ResultRow -> Halt
```

Both are semantically equivalent but have different instruction orderings.

## RustQL-Specific Opcodes

These opcodes exist in RustQL but not in SQLite:

### AggStep0

Used for aggregate initialization in RustQL's compilation model.

### MaxOpcode

Sentinel value representing the maximum opcode number. Used internally for range checks.

### Unused

Placeholder opcode for unimplemented or deprecated functionality.

## Comparison Algorithm

1. Extract opcodes from both SQLite EXPLAIN and RustQL bytecode
2. Apply normalization rules to remove acceptable differences
3. Compare normalized sequences
4. Report meaningful differences

### Comparison Levels

1. **Exact Match**: Normalized sequences are identical
2. **Semantic Match**: Same opcodes with different counts/order (may be acceptable)
3. **Different**: Missing or extra opcodes that need investigation

## Pass Criteria

A query passes opcode comparison if:

1. The normalized opcode sequences match exactly, OR
2. The opcodes are semantically equivalent (same operations, potentially different order for independent operations)

## Failing Cases

These differences indicate potential bugs:

1. Missing critical opcodes (e.g., `ResultRow` for SELECT queries)
2. Different loop structures (e.g., missing `Rewind`/`Next` pairs)
3. Missing transaction opcodes for write operations
4. Different aggregate handling that affects results

## Running Tests

```bash
# Run the Rust opcode comparison tests
cargo test --test opcode_comparison

# Run the shell comparison script
./scripts/compare_opcodes.sh --verbose

# Test a specific query
./scripts/compare_opcodes.sh --query "SELECT * FROM t1 WHERE a > 5"
```

## Updating Rules

When adding new normalization rules:

1. Document the rule in this file
2. Update `normalize_opcode()` in `tests/opcode_comparison.rs`
3. Update `normalize_opcodes()` in `scripts/compare_opcodes.sh`
4. Add test cases that exercise the new rule
