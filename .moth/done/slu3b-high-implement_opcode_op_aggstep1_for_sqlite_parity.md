# Problem
SQLite opcode OP_AggStep1 exists but is missing in RustQL.

# Scope
- Add OP_AggStep1 to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_AggStep1 where appropriate

# Acceptance Criteria
- OP_AggStep1 is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_AggStep1
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_AggStep1 implementation)
