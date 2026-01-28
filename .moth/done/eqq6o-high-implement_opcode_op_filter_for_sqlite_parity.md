# Problem
SQLite opcode OP_Filter exists but is missing in RustQL.

# Scope
- Add OP_Filter to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_Filter where appropriate

# Acceptance Criteria
- OP_Filter is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_Filter
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_Filter implementation)
