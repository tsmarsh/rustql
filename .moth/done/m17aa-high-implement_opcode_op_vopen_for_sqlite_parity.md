# Problem
SQLite opcode OP_VOpen exists but is missing in RustQL.

# Scope
- Add OP_VOpen to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_VOpen where appropriate

# Acceptance Criteria
- OP_VOpen is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_VOpen
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_VOpen implementation)
