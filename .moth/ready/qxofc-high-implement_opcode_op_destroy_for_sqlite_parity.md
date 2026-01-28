# Problem
SQLite opcode OP_Destroy exists but is missing in RustQL.

# Scope
- Add OP_Destroy to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_Destroy where appropriate

# Acceptance Criteria
- OP_Destroy is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_Destroy
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_Destroy implementation)
