# Problem
SQLite opcode OP_ColumnsUsed exists but is missing in RustQL.

# Scope
- Add OP_ColumnsUsed to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_ColumnsUsed where appropriate

# Acceptance Criteria
- OP_ColumnsUsed is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_ColumnsUsed
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_ColumnsUsed implementation)
