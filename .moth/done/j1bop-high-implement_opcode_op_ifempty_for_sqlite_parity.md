# Problem
SQLite opcode OP_IfEmpty exists but is missing in RustQL.

# Scope
- Add OP_IfEmpty to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_IfEmpty where appropriate

# Acceptance Criteria
- OP_IfEmpty is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_IfEmpty
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_IfEmpty implementation)
