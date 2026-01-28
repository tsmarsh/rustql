# Problem
SQLite opcode OP_InitCoroutine exists but is missing in RustQL.

# Scope
- Add OP_InitCoroutine to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_InitCoroutine where appropriate

# Acceptance Criteria
- OP_InitCoroutine is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_InitCoroutine
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_InitCoroutine implementation)
