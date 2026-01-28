# Problem
SQLite opcode OP_IsType exists but is missing in RustQL.

# Scope
- Add OP_IsType to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_IsType where appropriate

# Acceptance Criteria
- OP_IsType is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_IsType
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_IsType implementation)
