# Problem
SQLite opcode OP_IncrVacuum exists but is missing in RustQL.

# Scope
- Add OP_IncrVacuum to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_IncrVacuum where appropriate

# Acceptance Criteria
- OP_IncrVacuum is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_IncrVacuum
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_IncrVacuum implementation)
