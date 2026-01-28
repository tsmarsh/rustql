# Problem
SQLite opcode OP_ResetCount exists but is missing in RustQL.

# Scope
- Add OP_ResetCount to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_ResetCount where appropriate

# Acceptance Criteria
- OP_ResetCount is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_ResetCount
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_ResetCount implementation)
