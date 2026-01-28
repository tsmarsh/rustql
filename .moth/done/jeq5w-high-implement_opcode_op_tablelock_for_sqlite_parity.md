# Problem
SQLite opcode OP_TableLock exists but is missing in RustQL.

# Scope
- Add OP_TableLock to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_TableLock where appropriate

# Acceptance Criteria
- OP_TableLock is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_TableLock
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_TableLock implementation)
