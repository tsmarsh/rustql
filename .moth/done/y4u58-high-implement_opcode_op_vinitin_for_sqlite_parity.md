# Problem
SQLite opcode OP_VInitIn exists but is missing in RustQL.

# Scope
- Add OP_VInitIn to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_VInitIn where appropriate

# Acceptance Criteria
- OP_VInitIn is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_VInitIn
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_VInitIn implementation)
