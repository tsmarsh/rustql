# Problem
SQLite opcode OP_SoftNull exists but is missing in RustQL.

# Scope
- Add OP_SoftNull to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_SoftNull where appropriate

# Acceptance Criteria
- OP_SoftNull is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_SoftNull
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_SoftNull implementation)
