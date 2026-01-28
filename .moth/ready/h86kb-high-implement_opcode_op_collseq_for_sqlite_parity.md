# Problem
SQLite opcode OP_CollSeq exists but is missing in RustQL.

# Scope
- Add OP_CollSeq to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_CollSeq where appropriate

# Acceptance Criteria
- OP_CollSeq is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_CollSeq
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_CollSeq implementation)
