# Problem
SQLite opcode OP_CursorUnlock exists but is missing in RustQL.

# Scope
- Add OP_CursorUnlock to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_CursorUnlock where appropriate

# Acceptance Criteria
- OP_CursorUnlock is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_CursorUnlock
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_CursorUnlock implementation)
