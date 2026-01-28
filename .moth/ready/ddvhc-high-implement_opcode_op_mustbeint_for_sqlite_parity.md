# Problem
SQLite opcode OP_MustBeInt exists but is missing in RustQL.

# Scope
- Add OP_MustBeInt to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_MustBeInt where appropriate

# Acceptance Criteria
- OP_MustBeInt is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_MustBeInt
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_MustBeInt implementation)
