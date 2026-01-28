# Problem
SQLite opcode OP_MemMax exists but is missing in RustQL.

# Scope
- Add OP_MemMax to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_MemMax where appropriate

# Acceptance Criteria
- OP_MemMax is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_MemMax
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_MemMax implementation)
