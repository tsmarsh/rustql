# Problem
SQLite opcode OP_CursorHint exists but is missing in RustQL.

# Scope
- Add OP_CursorHint to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_CursorHint where appropriate

# Acceptance Criteria
- OP_CursorHint is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_CursorHint
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_CursorHint implementation)
