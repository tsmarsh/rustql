# Fix DELETE Statement WHERE Clause and Constraint Handling

## Problem
DELETE statements fail to properly handle WHERE clauses, constraints, and transaction behavior.

### Current Failures
- delete-1.2: DELETE with WHERE clause doesn't delete correct rows
- delete-2.3: DELETE with complex WHERE doesn't work
- delete-3.2: DELETE with ORDER BY and LIMIT not supported
- delete-4.1 through 4.5: DELETE with WHERE expressions
- delete-5.1 through 5.3: DELETE without WHERE clause edge cases
- delete-6.2 through 6.6: DELETE with table aliases and expressions
- delete-7.2: Transaction commit after DELETE fails
- delete-7.3, 7.5, 7.6: DELETE with trigger side-effects
- delete-8.1 through 8.6: Readonly database error handling
- delete-9.2 through 9.5: DELETE with JOIN-like behavior (correlated)
- delete-10.0: DELETE with subquery in WHERE
- delete-11.1: DELETE with GROUP BY constraints
- delete-12.0: DELETE row count not accurate

### Current Pass Rate
- delete.test: 42/67 (62%) ← **UP from 39/67 (58%)**

## Session Progress

### Completed (Session 4) - count_changes FIX
- **PRAGMA count_changes now working**: When enabled, INSERT/UPDATE/DELETE return affected row count
- **The Fix**: Re-enabled count_changes result return in VDBE Halt handler (was disabled due to corruption fears, but corruption was a separate issue)
- **Result**: 3 more tests passing (39→42)
- Tests now passing: delete-3.1.6.1, delete-3.1.6.2, delete-5.1.1, delete-5.2.2
- Files modified: `src/vdbe/engine/mod.rs`

### Completed (Session 3) - CORRUPTION FIX
- **ROOT CAUSE IDENTIFIED AND FIXED**: Database corruption after bulk DELETE operations
- **The Problem**: In `validate_freeblocks()` and `compute_free_space()`, the infinite loop detection check `steps > n_cell + 1` was too restrictive when n_cell=0 (empty page after DELETE)
- **The Fix**: Changed the check to use `steps > max_freeblocks` where `max_freeblocks = usable_size / 4` (page size / min freeblock size)
- **Result**: 8 more tests passing (31→39), corruption no longer occurs on bulk DELETE operations
- Files modified: `src/storage/btree/mod.rs`

### Completed (Session 2)
- Investigated count_changes handling - found NOT the root cause of corruption
- Disabled problematic count_changes Row return (causes cursor/transaction state issues)
- Isolated database corruption to minimal test case

## Current Status
- **DELETE tests: 42/67 passing (62%)** ← Up from 58%
- **CRITICAL BLOCKER RESOLVED**: Database corruption fixed!
- **count_changes FIXED**: PRAGMA count_changes now returns row counts
- **Remaining work**: Complex WHERE expressions (delete-6.5.x), subqueries, readonly database handling

### ~~CRITICAL ISSUE~~ RESOLVED: Database Corruption in Bulk Operations

**Root Cause (Session 3)**: The infinite loop detection in `validate_freeblocks()` and `compute_free_space()` used the check `steps > n_cell + 1`. When a page becomes empty after DELETE (n_cell=0), this check incorrectly triggers "corruption" errors when there are more than 1 freeblock in the chain.

**Fix Applied**: Changed the loop detection to use `steps > max_freeblocks` where `max_freeblocks = usable_size / 4`. This allows the correct number of freeblocks while still detecting actual infinite loops.

**Location**: `src/storage/btree/mod.rs` - functions `validate_freeblocks()` and `compute_free_space()`

**Previous Fixes Applied (Session 2)**:
1. ✓ Removed trunk pages from allocatable free_pages (load_freelist)
2. ✓ Removed trunk pages after they're selected in save_freelist
3. ✓ Fixed trunk page write() call ordering (was before data modification, now after)

### Other Issues (Lower Priority)
1. **delete-3.1.4 syntax error**: Parser rejects `DELETE FROM 'table1'` - single quotes not valid for identifiers
2. **Readonly database error handling**: delete-8.1-8.6 need proper readonly file setup

## Root Causes
1. **WHERE clause evaluation**: Rows not being correctly identified for deletion
2. **Row counting**: Number of deleted rows not tracked properly
3. **Constraint checking**: Foreign key constraints not verified before DELETE
4. **Transaction handling**: Commit/rollback after DELETE not working
5. **Complex expressions**: WHERE expressions with arithmetic, functions not evaluated
6. **Subqueries**: WHERE clauses with subqueries not supported
7. **Aliases**: Table aliases in WHERE not working

## TCL Tests That Must Pass

### From delete.test (required for 80% pass rate = 54+ of 67)

#### Basic DELETE Operations (delete-1.x through 3.x)
- delete-1.0: Basic DELETE without WHERE
- delete-1.1: DELETE with simple WHERE
- delete-1.2: DELETE with multiple matching rows
- delete-1.3: DELETE with no matching rows
- delete-2.0: DELETE with NULL values in WHERE
- delete-2.1: DELETE with string comparison
- delete-2.2: DELETE with numeric comparison
- delete-2.3: DELETE with complex WHERE clause
- delete-3.0: DELETE with expressions in WHERE
- delete-3.1: DELETE with AND/OR operators
- delete-3.2: DELETE with ORDER BY and LIMIT

#### Row Counting (delete-4.x through 5.x)
- delete-4.1 through 4.5: Various WHERE conditions, verify row counts
- delete-5.0: DELETE * rows count accuracy
- delete-5.1: DELETE * check remaining rows
- delete-5.2: Multi-row DELETE tracking
- delete-5.3: Partial DELETE row count

#### Complex WHERE (delete-6.x through 7.x)
- delete-6.0: DELETE with arithmetic expressions
- delete-6.1: DELETE with string functions
- delete-6.2: DELETE with BETWEEN
- delete-6.3: DELETE with IN operator
- delete-6.4: DELETE with LIKE
- delete-6.5.1 through 6.5.2: DELETE after INSERT operations
- delete-6.6: DELETE with qualified column names
- delete-6.7: DELETE with IS NULL
- delete-7.0: DELETE in transaction (COMMIT)
- delete-7.1: DELETE in transaction (ROLLBACK)
- delete-7.2: DELETE and transaction state
- delete-7.3: DELETE with subsequent SELECT
- delete-7.4: Multiple DELETEs in transaction
- delete-7.5 through 7.6: DELETE integrity checks

#### Advanced Features (delete-8.x through 12.x)
- delete-8.0 through 8.7: Error handling (readonly, invalid columns)
- delete-9.0 through 9.5: DELETE with correlated subqueries
- delete-10.0 through 10.2: DELETE with scalar subqueries in WHERE
- delete-11.0 through 11.1: DELETE with aggregate constraints
- delete-12.0: Final row count verification

## Implementation Tasks

### Task 1: WHERE Clause Evaluation (Priority: Highest)
- [ ] Properly evaluate WHERE expression for each row
- [ ] Support all comparison operators (=, !=, <, >, <=, >=)
- [ ] Support logical operators (AND, OR, NOT)
- [ ] Support BETWEEN, IN, LIKE, IS NULL
- [ ] Test: delete-1.1 through 2.3

### Task 2: Row Counting and Tracking (Priority: Highest)
- [ ] Track number of rows deleted
- [ ] Return correct count to application
- [ ] Verify remaining rows after DELETE
- [ ] Test: delete-4.1 through 5.3

### Task 3: Complex Expressions in WHERE (Priority: High)
- [ ] Support arithmetic expressions (col + 5 > 10)
- [ ] Support string functions (LENGTH, UPPER, etc.)
- [ ] Support CAST and type conversions
- [ ] Test: delete-6.0 through 6.6

### Task 4: Subqueries in WHERE (Priority: High)
- [ ] Support scalar subqueries (WHERE col IN (SELECT ...))
- [ ] Support correlated subqueries
- [ ] Support EXISTS and NOT EXISTS
- [ ] Test: delete-9.0 through 11.1

### Task 5: Transaction Handling (Priority: Medium)
- [ ] Maintain transaction state after DELETE
- [ ] Support COMMIT after DELETE
- [ ] Support ROLLBACK after DELETE
- [ ] Test: delete-7.0 through 7.6

### Task 6: Error Handling and Constraints (Priority: Medium)
- [ ] Proper error messages for readonly databases
- [ ] Foreign key constraint checking
- [ ] Invalid column detection
- [ ] Test: delete-8.0 through 8.7

## Files to Modify
- src/executor/delete.rs - Main DELETE executor
- src/parser/resolve.rs - WHERE clause validation
- src/vdbe/engine.rs - VDBE DELETE operations
- src/executor/expr.rs - WHERE expression evaluation

## Definition of Done
- [ ] delete.test pass rate: >=80% (54+ of 67)
- [ ] All WHERE clause features working
- [ ] Row counts accurate
- [ ] Transactions working correctly
- [ ] Error handling correct
- [ ] No regression in other test suites
- [ ] All TCL tests listed above passing
