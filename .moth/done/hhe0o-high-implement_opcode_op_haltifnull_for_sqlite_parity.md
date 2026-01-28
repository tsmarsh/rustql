# Problem
SQLite opcode OP_HaltIfNull exists but is missing in RustQL.

# Scope
- Add OP_HaltIfNull to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_HaltIfNull where appropriate

# Acceptance Criteria
- OP_HaltIfNull is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_HaltIfNull
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_HaltIfNull implementation)
