# Problem
SQLite opcode OP_OpenDup exists but is missing in RustQL.

# Scope
- Add OP_OpenDup to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_OpenDup where appropriate

# Acceptance Criteria
- OP_OpenDup is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_OpenDup
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_OpenDup implementation)
