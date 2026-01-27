# Fix CREATE TRIGGER Parser and Execution

## Problem
The trigger1.test suite fails with syntax errors because:
1. Parser requires BEFORE/AFTER/INSTEAD OF timing keyword, but SQLite makes it optional (defaults to BEFORE)
2. Statement tail calculation doesn't handle BEGIN...END blocks, causing `END;` to be executed as COMMIT
3. Trigger execution not implemented - `generate_trigger_code()` is a placeholder

## Session Progress

### Completed (Session 1)
- **Parser fix**: Made BEFORE/AFTER/INSTEAD OF timing optional, defaults to BEFORE
  - File: `src/parser/grammar.rs`
- **Statement tail fix**: Added `Parser::remaining()` method to get unparsed SQL text
  - The old `find_statement_tail()` split at the first semicolon, breaking CREATE TRIGGER with BEGIN...END
  - Now uses parser's actual position after parsing the statement
  - File: `src/parser/grammar.rs`, `src/executor/prepare.rs`
- **Test status**: trigger1.test 14/55 (25%) passing

### Completed (Session 2) - Trigger Execution Implementation
- **TriggerBodyCompiler** in `src/executor/trigger.rs`:
  - New struct that compiles trigger body statements to VDBE bytecode
  - Handles INSERT statements in trigger body
  - Handles expressions including OLD/NEW references via Param opcode
  - Fixed `Expr::Parens` handling (was falling through to NULL)

- **SetTriggerRow opcode** in `src/vdbe/ops.rs` and `src/vdbe/engine/mod.rs`:
  - Stores OLD/NEW row values in VDBE state for Param opcode to access

- **DeleteCompiler trigger integration** in `src/executor/delete.rs`:
  - Finds matching BEFORE/AFTER DELETE triggers
  - Emits SCopy opcodes to copy row data before Delete
  - Emits SetTriggerRow to store trigger context
  - Emits Program opcode to call trigger subprogram
  - Uses label-based return address resolution

- **Program opcode fixes** in `src/vdbe/engine/mod.rs`:
  - Fixed pc to be set to 0 (not -1) when entering subprogram
  - Added return Continue to properly transition to subprogram

- **Halt opcode fix** in `src/vdbe/engine/mod.rs`:
  - Fixed return from subprogram: pc = return_pc (not return_pc - 1)
  - Because exec_op reads ops[pc] THEN increments

- **Disabled runtime trigger firing** in Delete opcode:
  - The `fire_after_delete_triggers` function was corrupting registers
  - Now triggers are compiled into bytecode at compile time

- **Test status**: trigger1.test 20/55 (36%) passing

### Passing Tests (Session 2)
- trigger1-1.10: DELETE trigger now fires correctly
- Previous passing tests still work

### Completed (Session 3) - INSERT/UPDATE Triggers & DELETE in Trigger Body

- **INSERT trigger integration** in `src/executor/insert.rs`:
  - Added trigger imports and fields to InsertCompiler
  - Looks up BEFORE/AFTER INSERT triggers at compile time
  - Added emit_before_triggers and emit_after_triggers methods
  - Integrated into compile_values, compile_select, compile_default_values

- **UPDATE trigger integration** in `src/executor/update.rs`:
  - Added trigger imports and fields to UpdateCompiler
  - Looks up BEFORE/AFTER UPDATE triggers with column filtering
  - Saves OLD values before modification for trigger access
  - Builds NEW values for BEFORE triggers
  - Fires AFTER triggers with OLD (saved) and NEW (updated) values

- **DELETE in trigger body** in `src/executor/trigger.rs`:
  - Implemented compile_delete for TriggerBodyCompiler
  - Opens table, loops through rows, evaluates WHERE clause
  - Handles OLD/NEW references via Param opcode
  - compile_delete_where handles binary ops, column refs, literals

- **Drop table triggers fix** in `src/vdbe/engine/mod.rs`:
  - DropSchema opcode now removes triggers when dropping table
  - Fixes cascading trigger cleanup

- **Test status**: trigger1.test 22/55 (40%) passing

### Passing Tests (Session 3)
- trigger1-1.10: DELETE trigger with DELETE in body
- trigger1-1.11: UPDATE trigger with DELETE in body
- trigger1-3.1: CREATE TRIGGER after DROP TABLE (trigger cleanup)

### Completed (Session 4) - INSERT Triggers & Validation

- **INSERT trigger integration** in `src/executor/insert.rs`:
  - Added std::sync::Arc, Trigger, TriggerEvent, TriggerTiming imports
  - Added find_matching_triggers, generate_trigger_code imports
  - Added before_triggers and after_triggers fields to InsertCompiler
  - Look up triggers in compile() method
  - Added emit_before_triggers and emit_after_triggers methods
  - Integrated into compile_values, compile_select, compile_default_values

- **Trigger validation** in `src/vdbe/engine/mod.rs`:
  - Check for system tables (sqlite_master etc.) - error "cannot create trigger on system table"
  - Validate INSTEAD OF triggers only allowed on views
  - Validate BEFORE/AFTER triggers only allowed on tables (not views)
  - Proper error messages for each case

- **DROP VIEW fix** in `src/vdbe/engine/mod.rs` and `src/executor/prepare.rs`:
  - DropSchema case 2 now removes from schema.views instead of tables
  - compile_drop now checks schema.views for view existence

- **Test status**: trigger1.test 26/55 (47.3%) passing

### Passing Tests (Session 4)
- trigger1-1.9: Cannot create trigger on system table
- trigger1-1.12: Cannot create INSTEAD OF trigger on table
- trigger1-1.13: Cannot create BEFORE trigger on view
- trigger1-1.14: Cannot create AFTER trigger on view

### Remaining Work

#### High Priority
- Need 2 more tests to reach 50% target (28/55)

#### Medium Priority
- **Error message format**: Some tests fail due to message differences
  - "no such table: main.t2" vs "no such table: t2"
  - Quoted trigger names in error messages
- **TEMP trigger handling**: Triggers on temp tables should go to sqlite_temp_master
- **Schema resolution**: Triggers need proper main vs temp schema resolution

## Definition of Done
- [x] Parser accepts optional timing keyword (defaults to BEFORE) - DONE
- [x] Statement tail calculation handles BEGIN...END - DONE
- [ ] trigger1.test pass rate: >=50% (currently 47.3%)
- [x] Basic AFTER DELETE triggers fire and execute body statements - DONE
- [x] BEFORE triggers working - DONE (via emit_before_triggers)
- [x] INSERT triggers working - DONE
- [x] UPDATE triggers working - DONE
- [x] DELETE in trigger body - DONE
- [x] Trigger validation (INSTEAD OF on views only, BEFORE/AFTER on tables only) - DONE
