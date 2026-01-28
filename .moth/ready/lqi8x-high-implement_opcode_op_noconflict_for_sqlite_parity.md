# Problem
SQLite opcode OP_NoConflict exists but is missing in RustQL.

# Scope
- Add OP_NoConflict to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_NoConflict where appropriate

# Acceptance Criteria
- OP_NoConflict is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_NoConflict
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_NoConflict implementation)
