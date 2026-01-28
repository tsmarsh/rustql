# Problem
SQLite opcode OP_VRename exists but is missing in RustQL.

# Scope
- Add OP_VRename to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_VRename where appropriate

# Acceptance Criteria
- OP_VRename is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_VRename
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_VRename implementation)
