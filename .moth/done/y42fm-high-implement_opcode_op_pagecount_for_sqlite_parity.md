# Problem
SQLite opcode OP_Pagecount exists but is missing in RustQL.

# Scope
- Add OP_Pagecount to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_Pagecount where appropriate

# Acceptance Criteria
- OP_Pagecount is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_Pagecount
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_Pagecount implementation)
