# Problem
SQLite opcode OP_IfNotZero exists but is missing in RustQL.

# Scope
- Add OP_IfNotZero to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_IfNotZero where appropriate

# Acceptance Criteria
- OP_IfNotZero is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_IfNotZero
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_IfNotZero implementation)
