# Problem
SQLite opcode OP_FilterAdd exists but is missing in RustQL.

# Scope
- Add OP_FilterAdd to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_FilterAdd where appropriate

# Acceptance Criteria
- OP_FilterAdd is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_FilterAdd
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_FilterAdd implementation)
