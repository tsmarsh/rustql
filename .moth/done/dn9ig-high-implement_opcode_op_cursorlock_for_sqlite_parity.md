# Problem
SQLite opcode OP_CursorLock exists but is missing in RustQL.

# Scope
- Add OP_CursorLock to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_CursorLock where appropriate

# Acceptance Criteria
- OP_CursorLock is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_CursorLock
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_CursorLock implementation)
