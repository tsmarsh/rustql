# Problem
SQLite opcode OP_ElseEq exists but is missing in RustQL.

# Scope
- Add OP_ElseEq to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_ElseEq where appropriate

# Acceptance Criteria
- OP_ElseEq is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_ElseEq
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_ElseEq implementation)
