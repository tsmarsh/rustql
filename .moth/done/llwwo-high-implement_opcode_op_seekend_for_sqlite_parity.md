# Problem
SQLite opcode OP_SeekEnd exists but is missing in RustQL.

# Scope
- Add OP_SeekEnd to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_SeekEnd where appropriate

# Acceptance Criteria
- OP_SeekEnd is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_SeekEnd
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_SeekEnd implementation)
