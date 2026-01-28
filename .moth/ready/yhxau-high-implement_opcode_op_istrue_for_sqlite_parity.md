# Problem
SQLite opcode OP_IsTrue exists but is missing in RustQL.

# Scope
- Add OP_IsTrue to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_IsTrue where appropriate

# Acceptance Criteria
- OP_IsTrue is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_IsTrue
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_IsTrue implementation)
