# Problem
SQLite opcode OP_String exists but is missing in RustQL.

# Scope
- Add OP_String to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_String where appropriate

# Acceptance Criteria
- OP_String is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_String
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_String implementation)
