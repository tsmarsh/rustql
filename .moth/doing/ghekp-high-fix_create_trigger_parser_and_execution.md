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

### Remaining Work

#### High Priority
- INSERT trigger execution
- UPDATE trigger execution
- BEFORE triggers (currently only AFTER DELETE works)

#### Medium Priority
- **Error message format**: Some tests fail due to message differences
  - "no such table: main.t2" vs "no such table: t2"
  - Quoted trigger names in error messages
- **Trigger validation**:
  - INSTEAD OF triggers only on views
  - BEFORE/AFTER triggers only on tables
  - Cannot create triggers on sqlite_master

## Definition of Done
- [x] Parser accepts optional timing keyword (defaults to BEFORE) - DONE
- [x] Statement tail calculation handles BEGIN...END - DONE
- [ ] trigger1.test pass rate: >=50% (currently 36%)
- [x] Basic AFTER DELETE triggers fire and execute body statements - DONE
- [ ] BEFORE triggers working
- [ ] INSERT triggers working
- [ ] UPDATE triggers working
