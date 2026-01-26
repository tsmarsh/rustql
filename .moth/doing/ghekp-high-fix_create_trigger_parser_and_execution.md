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

### Passing Tests
- trigger1-1.1.2, 1.1.3: Basic syntax error detection
- trigger1-1.2.0, 1.2.1: CREATE TRIGGER IF NOT EXISTS
- trigger1-1.3, 1.4, 1.5, 1.6.1, 1.6.2, 1.7: Various trigger creation scenarios
- trigger1-2.1, 2.2: Trigger schema queries
- trigger1-3.3: DROP TRIGGER
- trigger1-7.1: Transaction state check
- trigger1-8.3, 8.4, 8.5, 8.6: sqlite_master queries

### Remaining Work

#### High Priority
- **Trigger execution**: `generate_trigger_code()` returns empty bytecode
  - Triggers create successfully but never fire
  - Need to emit VDBE code that executes trigger body statements
  - Files: `src/executor/trigger.rs`, `src/vdbe/engine/mod.rs`

#### Medium Priority
- **Error message format**: Some tests fail due to message differences
  - "no such table: main.t2" vs "no such table: t2"
  - Quoted trigger names in error messages
- **Trigger validation**:
  - INSTEAD OF triggers only on views
  - BEFORE/AFTER triggers only on tables
  - Cannot create triggers on sqlite_master

## TCL Tests That Should Pass
After trigger execution is implemented:
- trigger1-1.10: DELETE trigger should remove rows
- trigger1-6.x: Trigger body execution tests
- trigger1-9.x: REPLACE with triggers

## Definition of Done
- [x] Parser accepts optional timing keyword (defaults to BEFORE) - DONE
- [x] Statement tail calculation handles BEGIN...END - DONE
- [ ] trigger1.test pass rate: >=50% (need trigger execution)
- [ ] Basic triggers (BEFORE/AFTER INSERT/DELETE/UPDATE) working
- [ ] Triggers fire and execute body statements
