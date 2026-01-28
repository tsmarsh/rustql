# Problem
SQLite opcode OP_SorterOpen exists but is missing in RustQL.

# Scope
- Add OP_SorterOpen to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_SorterOpen where appropriate

# Acceptance Criteria
- OP_SorterOpen is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_SorterOpen
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_SorterOpen implementation)
