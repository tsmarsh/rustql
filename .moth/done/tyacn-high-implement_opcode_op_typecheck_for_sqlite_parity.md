# Problem
SQLite opcode OP_TypeCheck exists but is missing in RustQL.

# Scope
- Add OP_TypeCheck to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_TypeCheck where appropriate

# Acceptance Criteria
- OP_TypeCheck is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_TypeCheck
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_TypeCheck implementation)
