# Fix INSERT Statement Column Handling and Default Values

## Problem
INSERT statements fail to properly handle column specifications, defaults, and ordering.

### Current Failures
- insert-1.4: Column validation not rejecting invalid column names
- insert-1.5, 1.5b, 1.5c: Values inserted in wrong column positions
- insert-1.6b, 1.6c: NULL placeholders appearing instead of DEFAULT values
- insert2-2.1, 2.2, 2.3: Column order incorrect in results
- insert2-3.2: DEFAULT values not being applied

### Current Pass Rate
- insert.test: 30/83 (36%)
- insert2.test: 13/31 (42%)

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

## Definition of Done
- [ ] insert.test pass rate: >=80% (66+ of 83)
- [ ] insert2.test pass rate: >=75% (24+ of 31)
- [ ] No regression in other test suites
- [ ] All TCL tests listed above passing
