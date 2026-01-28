# Problem
SQLite opcode OP_SetSubtype exists but is missing in RustQL.

# Scope
- Add OP_SetSubtype to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_SetSubtype where appropriate

# Acceptance Criteria
- OP_SetSubtype is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_SetSubtype
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_SetSubtype implementation)
