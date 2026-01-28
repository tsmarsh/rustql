# Problem
SQLite opcode OP_Sort exists but is missing in RustQL.

# Scope
- Add OP_Sort to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_Sort where appropriate

# Acceptance Criteria
- OP_Sort is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_Sort
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_Sort implementation)
