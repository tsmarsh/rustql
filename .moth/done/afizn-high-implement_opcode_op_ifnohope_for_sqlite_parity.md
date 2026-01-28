# Problem
SQLite opcode OP_IfNoHope exists but is missing in RustQL.

# Scope
- Add OP_IfNoHope to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_IfNoHope where appropriate

# Acceptance Criteria
- OP_IfNoHope is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_IfNoHope
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_IfNoHope implementation)
