# Problem
SQLite opcode OP_IfNotOpen exists but is missing in RustQL.

# Scope
- Add OP_IfNotOpen to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_IfNotOpen where appropriate

# Acceptance Criteria
- OP_IfNotOpen is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_IfNotOpen
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_IfNotOpen implementation)
