# Problem
SQLite opcode OP_DropIndex exists but is missing in RustQL.

# Scope
- Add OP_DropIndex to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_DropIndex where appropriate

# Acceptance Criteria
- OP_DropIndex is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_DropIndex
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_DropIndex implementation)
