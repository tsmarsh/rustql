# Problem
SQLite opcode OP_IntCopy exists but is missing in RustQL.

# Scope
- Add OP_IntCopy to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_IntCopy where appropriate

# Acceptance Criteria
- OP_IntCopy is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_IntCopy
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_IntCopy implementation)
