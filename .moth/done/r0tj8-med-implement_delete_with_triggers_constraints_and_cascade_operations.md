# Implement DELETE with Triggers, Constraints and Cascade Operations

## Problem Statement

The DELETE statement implementation currently lacks support for:
1. **Trigger execution** - Triggers are not fired on DELETE operations (both BEFORE and AFTER)
2. **Foreign key constraints** - CASCADE, SET NULL, SET DEFAULT, RESTRICT operations not enforced
3. **Constraint enforcement** - DELETE operations that violate constraints don't raise errors
4. **Transaction semantics** - Partial failures don't rollback correctly

Current Status:
- delete.test: 28/67 tests passing (41%)
- **Failing trigger tests**: delete-8.0 through delete-12.0 (significant test block)
- **Failing constraint tests**: delete-4.0 through delete-7.6 (multi-table referential integrity)

## Root Causes

1. **Missing trigger plumbing**: The VDBE delete opcode doesn't invoke the trigger subsystem
   - File: src/vdbe/engine.rs - OpenRead, Delete opcodes
   - Missing: TriggerContext propagation, BEFORE/AFTER trigger dispatch

2. **Incomplete foreign key handling**: Constraint checking not implemented in delete executor
   - File: src/executor/delete.rs - No FK validation in execute_delete()
   - Missing: Cascade action evaluation, constraint violation detection

3. **Weak error recovery**: Errors during deletion don't restore transaction state
   - File: src/storage/btree.rs - No rollback on partial page failure
   - Missing: Savepoint/rollback integration

## TCL Tests That Must Pass

### Block 1: Basic Trigger Support (delete-8.0 to delete-8.5)
```
delete-8.0    - BEFORE DELETE trigger fires before row removal
delete-8.1    - AFTER DELETE trigger fires after row removal
delete-8.2    - NEW/OLD references in trigger context
delete-8.3    - Trigger can modify deletion behavior
delete-8.4    - Multiple triggers execute in order
delete-8.5    - Trigger raises error halts deletion
```

### Block 2: Trigger with Updates (delete-9.0 to delete-9.2)
```
delete-9.0    - BEFORE DELETE trigger can skip row via RAISE
delete-9.1    - Trigger can perform cascading operations
delete-9.2    - Nested trigger invocations work correctly
```

### Block 3: Foreign Key Cascade (delete-10.0 to delete-10.8)
```
delete-10.0   - ON DELETE CASCADE removes child rows
delete-10.1   - ON DELETE CASCADE with self-references
delete-10.2   - ON DELETE CASCADE transaction all-or-nothing
delete-10.3   - ON DELETE CASCADE with multiple children
delete-10.4   - ON DELETE CASCADE error on non-unique parent
delete-10.5   - Cascade through multiple levels
delete-10.6   - Cascade ordering is deterministic
delete-10.7   - Cascade with triggers fires correctly
delete-10.8   - Cascade respects transaction boundaries
```

### Block 4: Foreign Key Set NULL (delete-11.0 to delete-11.3)
```
delete-11.0   - ON DELETE SET NULL updates child rows
delete-11.1   - ON DELETE SET NULL with default value
delete-11.2   - ON DELETE SET NULL on non-nullable column errors
delete-11.3   - ON DELETE SET NULL with triggers
```

### Block 5: Foreign Key Set DEFAULT and RESTRICT (delete-12.0 to delete-12.4)
```
delete-12.0   - ON DELETE SET DEFAULT updates child rows
delete-12.1   - ON DELETE RESTRICT prevents parent deletion
delete-12.2   - ON DELETE RESTRICT error message
delete-12.3   - ON DELETE NO ACTION treated as RESTRICT
delete-12.4   - Constraint errors prevent transaction commit
```

## Implementation Tasks

### Task 1: Trigger Dispatch in VDBE (HIGH PRIORITY)
**Target**: delete-8.0 through delete-9.2

1. Modify src/vdbe/engine.rs - Delete opcode handler:
   - Add trigger context to execution state
   - Call trigger subsystem before deletion (BEFORE triggers)
   - Call trigger subsystem after deletion (AFTER triggers)
   - Propagate trigger errors up execution chain

2. Create src/executor/trigger_delete.rs:
   - Function: `fire_delete_triggers(ctx, table_name, row_values, when: Before|After) -> Result<bool>`
   - Return: bool indicating if deletion should proceed (false = skip)
   - Handle: NEW/OLD references, RAISE operations

3. Update src/schema/trigger.rs:
   - Add trigger firing logic
   - Implement NEW/OLD record substitution
   - Handle trigger error propagation

### Task 2: Foreign Key Constraint Checking (HIGH PRIORITY)
**Target**: delete-10.0 through delete-12.4

1. Modify src/executor/delete.rs - execute_delete():
   - Before removing rows: check foreign key references
   - Collect all child rows affected by cascade
   - Validate RESTRICT constraints before deletion
   - Build deletion plan respecting referential integrity

2. Create src/executor/fk_actions.rs:
   - Function: `get_fk_actions(parent_table, deleted_row) -> Vec<FKAction>`
   - Implement CASCADE: collect child rows to delete
   - Implement SET NULL: collect child rows to update
   - Implement SET DEFAULT: collect child rows to update
   - Implement RESTRICT: check for child rows, raise error if found

3. Update src/schema/mod.rs:
   - Add ForeignKeyAction enum: CASCADE, SetNull, SetDefault, Restrict, NoAction
   - Add method: `get_foreign_keys_referencing(table) -> Vec<ForeignKeyDef>`

### Task 3: Transaction Rollback on Constraint Failure (MEDIUM PRIORITY)
**Target**: delete-10.2, delete-12.4

1. Modify src/storage/pager.rs:
   - Add savepoint before starting delete operation
   - On constraint violation: rollback to savepoint
   - Preserve transaction consistency

2. Update src/executor/delete.rs:
   - Wrap deletion in try/catch
   - On error: trigger rollback, propagate error
   - Ensure partial deletions are rolled back

### Task 4: Cascade Deletion Ordering (MEDIUM PRIORITY)
**Target**: delete-10.6

1. Create src/executor/cascade_order.rs:
   - Function: `topological_sort(deletions: Vec<Deletion>) -> Vec<Deletion>`
   - Ensure child rows deleted before parent
   - Maintain deterministic ordering for reproducibility

## Files to Modify

**Core Files (Must Modify)**:
- [ ] src/vdbe/engine.rs - Add trigger dispatch in Delete opcode
- [ ] src/executor/delete.rs - Add FK constraint checking
- [ ] src/schema/mod.rs - Add FK action types
- [ ] src/schema/trigger.rs - Add trigger firing logic
- [ ] src/storage/pager.rs - Add savepoint/rollback for constraints

**New Files (Create)**:
- [ ] src/executor/trigger_delete.rs - Trigger dispatch logic
- [ ] src/executor/fk_actions.rs - Foreign key action evaluation
- [ ] src/executor/cascade_order.rs - Cascade ordering

**Test Files**:
- [ ] tests/delete_triggers.rs - Unit tests for trigger firing
- [ ] tests/delete_fk.rs - Unit tests for FK constraints

## Definition of Done

✅ All criteria must be met:

1. **Compilation**:
   - `cargo build --release` succeeds
   - No warnings related to this moth

2. **Test Results**:
   - delete-8.0 through delete-8.5: ALL PASS (6/6)
   - delete-9.0 through delete-9.2: ALL PASS (3/3)
   - delete-10.0 through delete-10.8: ALL PASS (9/9)
   - delete-11.0 through delete-11.3: ALL PASS (4/4)
   - delete-12.0 through delete-12.4: ALL PASS (5/5)
   - **Total**: 27/27 new tests passing
   - **No regression**: All previously passing tests still pass

3. **Code Quality**:
   - All changes reviewed and consistent with codebase style
   - Comments explain non-obvious trigger/FK logic
   - Error messages are user-friendly

4. **Documentation**:
   - Code comments explain trigger dispatch mechanism
   - FK action implementation documented
   - Examples in comments show CASCADE, SET NULL, SET DEFAULT usage

5. **Performance**:
   - Cascade deletion doesn't exceed original delete time by >20%
   - No unnecessary full-table scans for FK checking

## Verification Script

Run after implementation:
```bash
# Build and test
cargo build --release

# Run specific test blocks
make test-delete8
make test-delete9
make test-delete10
make test-delete11
make test-delete12

# Verify all delete tests still pass
make test-delete
echo "Expected: 55+/67 tests passing (was 28/67)"
```

## Notes

- This moth builds on xg8mt (DELETE WHERE clauses) - should be started after xg8mt is completed
- Trigger execution order is important for determinism; see delete-8.4
- Foreign key cascade can create complex deletion plans; topological sort is critical
- Error messages must match SQLite's for compatibility
