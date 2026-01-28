# Problem
SQLite opcode OP_RowCell exists but is missing in RustQL.

# Scope
- Add OP_RowCell to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_RowCell where appropriate

# Acceptance Criteria
- OP_RowCell is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_RowCell
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_RowCell implementation)
