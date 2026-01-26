# Fix CREATE TRIGGER Parser and Execution

## Problem
The trigger1.test suite fails with syntax errors because:
1. Parser requires BEFORE/AFTER/INSTEAD OF timing keyword, but SQLite makes it optional (defaults to BEFORE)
2. Trigger execution not implemented - `generate_trigger_code()` is a placeholder

### Current Test Status
- trigger1.test: Fails early with syntax errors
- Error: `near "INSERT": syntax error` when parsing `CREATE TRIGGER tr1 INSERT ON t1 BEGIN...`

## Root Causes

### 1. Parser Bug (grammar.rs:1791-1792)
```rust
} else {
    return Err(self.error("expected BEFORE, AFTER, or INSTEAD OF"));
};
```
SQLite allows omitting the timing keyword, defaulting to BEFORE. The parser should check for event keywords (INSERT/DELETE/UPDATE) and default timing to BEFORE if not specified.

### 2. Trigger Execution Not Implemented (trigger.rs:254-262)
```rust
pub fn generate_trigger_code(...) -> Result<Vec<VdbeOp>> {
    // Full implementation requires nested VDBE execution
    // which is complex and will be done incrementally
    Ok(Vec::new())
}
```
Triggers are created but never fire - the execution code returns empty bytecode.

## Implementation Tasks

### Task 1: Fix Parser (Priority: Highest)
- [ ] Make BEFORE/AFTER/INSTEAD OF optional (default to BEFORE)
- [ ] Parser should handle: `CREATE TRIGGER name [timing] event ON table...`
- File: `src/parser/grammar.rs`

### Task 2: Store triggers in sqlite_master (Priority: High)
- [ ] INSERT trigger definition into sqlite_master
- [ ] Load triggers from sqlite_master on database open
- File: `src/executor/trigger.rs`, `src/schema/mod.rs`

### Task 3: Implement trigger firing (Priority: High)
- [ ] Generate VDBE code to execute trigger body
- [ ] Support OLD/NEW pseudo-table access
- [ ] Handle WHEN condition
- [ ] Recursive trigger prevention
- Files: `src/executor/trigger.rs`, `src/vdbe/engine/mod.rs`

## TCL Tests That Should Pass
After completion:
- trigger1-1.x: Basic CREATE TRIGGER syntax
- trigger1-2.x: Trigger body execution
- trigger1-3.x: OLD/NEW row access

## Definition of Done
- [ ] trigger1.test pass rate: >=50%
- [ ] Basic triggers (BEFORE/AFTER INSERT/DELETE/UPDATE) working
- [ ] Triggers fire and execute body statements
