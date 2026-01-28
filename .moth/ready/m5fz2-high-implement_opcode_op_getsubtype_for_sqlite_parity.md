# Problem
SQLite opcode OP_GetSubtype exists but is missing in RustQL.

# Scope
- Add OP_GetSubtype to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_GetSubtype where appropriate

# Acceptance Criteria
- OP_GetSubtype is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_GetSubtype
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_GetSubtype implementation)
