# RustQL Coverage Improvement Plan

## Current Status

**SQLite TCL Test Suite Compatibility (primary metric):**
- **Weighted assertion pass rate**: 91% (330,953 / 360,948 assertions)
- **Test suites passing completely**: 224 / 1,176
- **Average per-file pass rate**: 45% across 805 test files with assertions

**Codebase size**: ~131K lines of Rust across 122 source files.

## Strengths (90%+ pass rate)

These areas are near-complete and should be maintained:

| Area | Pass Rate | Tests |
|------|-----------|-------|
| SELECT | 98-100% | select1-select9 |
| Functions | 99% | func (14,550/14,630) |
| Expressions | 89-95% | expr, e_expr |
| INSERT | 90% | insert |
| UPDATE | 93% | update |
| LIKE | 98% | like |
| BETWEEN | 100% | between |
| LIMIT | 100% | limit |
| NULL | 100% | null |
| Types | 85-100% | types, types2, types3 |
| B-tree | 99% | btree01 |
| Corruption detection | 86-99% | corrupt, corruptC, corruptF |
| DELETE | 85-92% | delete, delete2, delete4 |
| CREATE TABLE | 93% | e_createtable |

## Priority Improvement Areas

### 1. Window Functions (3-50% pass rate)

Window functions have the largest gap. The basic framework exists but many
specifications fail.

**Key test files**: window1 (30%), window2 (8%), window3 (6%), window4 (7%), window8 (3%)

**What's missing**:
- RANGE and GROUPS frame types
- EXCLUDE clauses
- Complex partition-by / order-by combinations
- Nested window function expressions

### 2. Complex Joins (5-48% pass rate)

Simple joins work well (join 80%, join3 100%, joinE 91%), but complex
multi-way joins and advanced join patterns are weak.

**Key test files**: join7 (6%), join9 (5%), joinA (7%), joinC (0%), joinB (12%)

**What's missing**:
- Large multi-way join trees
- Complex ON clause predicates
- Join reordering optimizations

### 3. Triggers (1-80% pass rate)

The trigger1 suite is at 80%, but other trigger test files are lower.

**Key test files**: triggerB (1%), triggerE (3%), trigger3 (16%), trigger4 (16%)

**What's missing**:
- RAISE() with expression arguments
- Recursive trigger support (PRAGMA recursive_triggers)
- Cross-database triggers (ATTACH)
- Complex nested trigger execution
- UPDATE triggers in more trigger suites

### 4. Collation (0-100% pass rate)

Basic collation works but advanced cases are incomplete.

**Key test files**: collate7 (0%), collate3 (36%), collate8 (39%), collate2 (42%)

**What's missing**:
- Custom collation sequences via API
- Collation in index expressions
- Unicode-aware collation

### 5. Virtual Tables and FTS (0% pass rate for most)

Virtual table infrastructure exists but runtime dispatch is incomplete.

**Key test files**: FTS tests mostly 0%, extension01 (0%), carray (0%)

**What's missing**:
- Full virtual table xFilter/xNext/xColumn dispatch
- FTS3 integration as a virtual table
- Extension loading API

### 6. ALTER TABLE (3-77% pass rate)

Some ALTER operations work, others don't.

**Key test files**: alter2 (3%), alterqf (14%), alterlegacy (18%), alter (25%)

**What's missing**:
- RENAME COLUMN
- DROP COLUMN (partial)
- Schema migration edge cases

### 7. WHERE Optimization (0-100% pass rate)

Core WHERE works well but advanced optimization paths are weak.

**Key test files**: where7 (6%), where8 (21%), whereE (0%), whereJ (29%)

**What's missing**:
- Complex OR optimization (where7 has 1,068 tests)
- Partial index utilization
- Skip-scan optimization

### 8. Bind Parameters (7% pass rate)

Parameter binding has low coverage.

**Key test files**: bind (7%), bindxfer (0%)

**What's missing**:
- Named parameter binding
- Parameter type coercion
- Bulk binding operations

## Test Categories by Impact

### High impact (many assertions, moderate pass rate)
These have the most room for total assertion gains:

- window3: 71/1,071 (6%) -- 1,000 assertions to gain
- where7: 68/1,068 (6%) -- 1,000 assertions to gain
- joinB: 66/512 (12%) -- 446 assertions to gain
- window8: 12/361 (3%) -- 349 assertions to gain
- window1: 107/348 (30%) -- 241 assertions to gain

### Medium impact (moderate size, low pass rate)
- joinC: 1/256 (0%) -- 255 assertions to gain
- triggerB: 3/202 (1%) -- 199 assertions to gain
- window4: 17/225 (7%) -- 208 assertions to gain
- join9: 8/150 (5%) -- 142 assertions to gain

## Testing Strategy

### Primary: SQLite TCL Test Suite
The TCL test suite is the ground truth. Improvements should be measured by:
```bash
make test          # Run full suite in parallel
make pass-rates    # Show per-file pass rates
make test-report   # Detailed per-file report
```

### Secondary: Rust Unit Tests
```bash
cargo test         # Run Rust unit tests
```

### Specific Area Testing
```bash
make test-select1  # Run a specific test interactively
make test-trigger1 # See individual assertion results
```

## Next Steps

1. **Window functions**: Implement RANGE/GROUPS frame types for biggest assertion gains
2. **Complex joins**: Investigate join7/join9/joinB failures for optimizer improvements
3. **Trigger completion**: Finish RAISE() expression support and recursive triggers
4. **WHERE optimization**: Handle OR-expansion (where7) and partial indexes
5. **Bind parameters**: Implement named parameters and type coercion
