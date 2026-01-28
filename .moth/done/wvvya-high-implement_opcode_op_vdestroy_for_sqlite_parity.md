# Problem
SQLite opcode OP_VDestroy exists but is missing in RustQL.

# Scope
- Add OP_VDestroy to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_VDestroy where appropriate

# Acceptance Criteria
- OP_VDestroy is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_VDestroy
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_VDestroy implementation)
