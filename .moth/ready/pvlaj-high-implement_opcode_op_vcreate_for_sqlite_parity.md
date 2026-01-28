# Problem
SQLite opcode OP_VCreate exists but is missing in RustQL.

# Scope
- Add OP_VCreate to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_VCreate where appropriate

# Acceptance Criteria
- OP_VCreate is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_VCreate
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_VCreate implementation)
