# Problem
SQLite opcode OP_ClrSubtype exists but is missing in RustQL.

# Scope
- Add OP_ClrSubtype to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_ClrSubtype where appropriate

# Acceptance Criteria
- OP_ClrSubtype is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_ClrSubtype
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_ClrSubtype implementation)
