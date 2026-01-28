# Problem
SQLite opcode OP_Offset exists but is missing in RustQL.

# Scope
- Add OP_Offset to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_Offset where appropriate

# Acceptance Criteria
- OP_Offset is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_Offset
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_Offset implementation)
