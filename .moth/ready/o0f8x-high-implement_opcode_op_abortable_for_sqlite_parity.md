# Problem
SQLite opcode OP_Abortable exists but is missing in RustQL.

# Scope
- Add OP_Abortable to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_Abortable where appropriate

# Acceptance Criteria
- OP_Abortable is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_Abortable
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_Abortable implementation)
