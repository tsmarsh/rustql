# Problem
SQLite opcode OP_ZeroOrNull exists but is missing in RustQL.

# Scope
- Add OP_ZeroOrNull to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_ZeroOrNull where appropriate

# Acceptance Criteria
- OP_ZeroOrNull is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_ZeroOrNull
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_ZeroOrNull implementation)
