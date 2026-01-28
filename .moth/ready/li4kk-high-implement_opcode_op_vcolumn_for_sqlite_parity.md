# Problem
SQLite opcode OP_VColumn exists but is missing in RustQL.

# Scope
- Add OP_VColumn to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_VColumn where appropriate

# Acceptance Criteria
- OP_VColumn is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_VColumn
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_VColumn implementation)
