# Problem
SQLite opcode OP_MaxPgcnt exists but is missing in RustQL.

# Scope
- Add OP_MaxPgcnt to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_MaxPgcnt where appropriate

# Acceptance Criteria
- OP_MaxPgcnt is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_MaxPgcnt
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_MaxPgcnt implementation)
