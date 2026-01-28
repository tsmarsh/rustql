# Problem
SQLite opcode OP_VUpdate exists but is missing in RustQL.

# Scope
- Add OP_VUpdate to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_VUpdate where appropriate

# Acceptance Criteria
- OP_VUpdate is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_VUpdate
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_VUpdate implementation)
