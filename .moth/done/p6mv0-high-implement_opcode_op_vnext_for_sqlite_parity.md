# Problem
SQLite opcode OP_VNext exists but is missing in RustQL.

# Scope
- Add OP_VNext to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_VNext where appropriate

# Acceptance Criteria
- OP_VNext is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_VNext
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_VNext implementation)
