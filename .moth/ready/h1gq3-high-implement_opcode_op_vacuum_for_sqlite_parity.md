# Problem
SQLite opcode OP_Vacuum exists but is missing in RustQL.

# Scope
- Add OP_Vacuum to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_Vacuum where appropriate

# Acceptance Criteria
- OP_Vacuum is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_Vacuum
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_Vacuum implementation)
