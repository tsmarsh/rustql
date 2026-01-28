# Problem
SQLite opcode OP_Permutation exists but is missing in RustQL.

# Scope
- Add OP_Permutation to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_Permutation where appropriate

# Acceptance Criteria
- OP_Permutation is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_Permutation
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_Permutation implementation)
