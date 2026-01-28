# Problem
SQLite opcode OP_RowData exists but is missing in RustQL.

# Scope
- Add OP_RowData to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_RowData where appropriate

# Acceptance Criteria
- OP_RowData is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_RowData
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_RowData implementation)
