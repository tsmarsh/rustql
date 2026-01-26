# Fix INSERT Statement Column Handling and Default Values

## Problem
INSERT statements fail to properly handle column specifications, defaults, and ordering.

### Current Pass Rate: 47/83 (57%)

**Progress from start of session**: 30 → 47 tests (+17 tests, +21%)

### Session Improvements
- Fixed column validation to reject invalid column names ✓
- Fixed column position mapping to table schema ✓
- Implemented DEFAULT value extraction and application ✓
- Added support for unquoted identifier defaults ✓
- Fixed numeric string comparisons in WHERE clauses ✓
- q4sxj team added ColumnMapper infrastructure for complex INSERT...SELECT ✓

### Passing Test Categories
- insert-1.x: 13 tests - All basic inserts
- insert-2.x: 7 tests - Column list variations
- insert-3.x: 6 tests - DEFAULT value handling (including numeric comparisons)
- insert-4.1, 4.7: Simple expression inserts
- insert-5.x: 4 tests - Partial column handling
- insert-6.1, 6.5, 6.6: Constraint basics
- insert-7.1, 7.2: Index creation
- insert-10.1, 14.2: Various specs
- insert-16.2, 16.3, 16.5, 16.7: Basic constraint checks
- insert-17.2, 17.4: Conflict action testing

## Root Causes
1. **Column name validation**: Invalid column names not rejected during parsing
2. **Column position mapping**: INSERT column list doesn't correctly map to table schema
3. **DEFAULT value handling**: Schema-defined defaults not being retrieved or applied
4. **VDBE compilation**: Column ordering lost during code generation

## TCL Tests That Must Pass

### From insert.test (must reach 80% pass rate)
- insert-1.0 through 1.3: Basic insert operations
- insert-1.4: Reject INSERT with invalid column name
- insert-1.5, 1.5b, 1.5c: INSERT with partial column list, correct defaults
- insert-1.6b, 1.6c: INSERT partial columns, verify defaults and order
- insert-2.0 through 2.3: Various column specifications and NULL handling
- insert-3.0 through 3.5: Column name edge cases, case sensitivity

### From insert2.test (must reach 75% pass rate)
- insert2-1.0: Basic rowid handling in INSERT
- insert2-1.1.1 through 1.1.3: ROWID function and autoincrement
- insert2-1.2.1 through 1.2.2: INSERT...SELECT with WHERE and aggregates
- insert2-1.3.1 through 1.4: INSERT...SELECT with joins and ORDER BY
- insert2-2.0 through 2.3: Column ordering and DEFAULT values
- insert2-3.0 through 3.6: Trigger-like behavior, constraints, expression eval

## Implementation Tasks

### Task 1: Column Validation (Priority: Highest)
- [ ] Parse INSERT column list and validate all names exist in table
- [ ] Reject with "no such column: X" error for invalid names
- [ ] Handle column name case sensitivity per table collation

### Task 2: Column Position Mapping (Priority: Highest)
- [ ] Map INSERT column list to table column positions
- [ ] Store mapping in prepared statement
- [ ] Verify order independent of declaration order

### Task 3: DEFAULT Value Application (Priority: High)
- [ ] Query table schema for DEFAULT values
- [ ] Apply defaults for columns not in INSERT list
- [ ] Distinguish between NULL and DEFAULT

### Task 4: INSERT...SELECT Column Mapping (Priority: High)
- [ ] Support non-simple SELECT in INSERT...SELECT
- [ ] Map SELECT result columns to INSERT columns
- [ ] Handle column name resolution in SELECT context

### Task 5: ROWID Handling (Priority: Medium)
- [ ] Track and return correct ROWID in multi-row INSERT
- [ ] Support ROWID function after INSERT
- [ ] Support autoincrement in INSERT

## Files to Modify
- src/executor/insert.rs - Main INSERT executor
- src/parser/resolve.rs - Column validation and resolution
- src/schema/mod.rs - Schema DEFAULT value access
- src/vdbe/engine.rs - VDBE INSERT operations

## Next Steps to Reach 80% Target (19 tests needed)

### High-Value Items (3-5 tests each)
1. **Expression evaluation in INSERT VALUES** (would unlock 4-5 tests)
   - Current failures: insert-4.2-4.6, 9.1-9.2, 11.1, 14.1
   - Requirements: Evaluate `(SELECT max(a) FROM t3)+1`, CASE expressions, function calls
   - Blocking: q4sxj moth and SelectCompiler integration

2. **UNIQUE constraint detection** (would unlock 2-3 tests)
   - Current failures: insert-6.2-6.4
   - Requirements: Proper UNIQUE violation detection, error messages
   - Current status: Constraints recognized but not enforced in INSERT

3. **INSERT...SELECT with rowid mapping** (would unlock 1-2 tests)
   - Current failures: insert-12.1-12.3
   - Requirements: Support explicit rowid column in INSERT...SELECT
   - Blocking: Complex column mapping in ColumnMapper

### Medium-Value Items (1-2 tests each)
- **Trigger execution with REPLACE** (insert-16.x, 17.x)
- **Transaction handling** (insert-17.x constraint violations)
- **Temporary table metadata** (insert-5.5, 5.6)

## Definition of Done
- [x] insert.test pass rate: >=50% (42+ of 83) - ACHIEVED: 47/83 (57%)
- [ ] insert.test pass rate: >=80% (66+ of 83) - Target: 19 more tests
- [ ] insert2.test pass rate: >=75% (24+ of 31)
- [ ] No regression in other test suites
