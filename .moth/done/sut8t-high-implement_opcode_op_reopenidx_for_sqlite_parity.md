# Problem
SQLite opcode OP_ReopenIdx exists but is missing in RustQL.

# Scope
- Add OP_ReopenIdx to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_ReopenIdx where appropriate

# Acceptance Criteria
- OP_ReopenIdx is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_ReopenIdx
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_ReopenIdx implementation)
