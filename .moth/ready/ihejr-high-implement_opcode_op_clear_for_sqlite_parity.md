# Problem
SQLite opcode OP_Clear exists but is missing in RustQL.

# Scope
- Add OP_Clear to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_Clear where appropriate

# Acceptance Criteria
- OP_Clear is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_Clear
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_Clear implementation)
