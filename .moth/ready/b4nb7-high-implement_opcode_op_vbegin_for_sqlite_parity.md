# Problem
SQLite opcode OP_VBegin exists but is missing in RustQL.

# Scope
- Add OP_VBegin to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_VBegin where appropriate

# Acceptance Criteria
- OP_VBegin is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_VBegin
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_VBegin implementation)
