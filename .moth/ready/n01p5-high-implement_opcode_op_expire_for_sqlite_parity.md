# Problem
SQLite opcode OP_Expire exists but is missing in RustQL.

# Scope
- Add OP_Expire to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_Expire where appropriate

# Acceptance Criteria
- OP_Expire is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_Expire
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_Expire implementation)
