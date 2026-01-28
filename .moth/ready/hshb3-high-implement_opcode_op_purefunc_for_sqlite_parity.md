# Problem
SQLite opcode OP_PureFunc exists but is missing in RustQL.

# Scope
- Add OP_PureFunc to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_PureFunc where appropriate

# Acceptance Criteria
- OP_PureFunc is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_PureFunc
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_PureFunc implementation)
