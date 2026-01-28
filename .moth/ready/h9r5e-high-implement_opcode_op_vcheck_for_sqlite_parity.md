# Problem
SQLite opcode OP_VCheck exists but is missing in RustQL.

# Scope
- Add OP_VCheck to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_VCheck where appropriate

# Acceptance Criteria
- OP_VCheck is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_VCheck
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_VCheck implementation)
