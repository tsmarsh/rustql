# Problem
SQLite opcode OP_LoadAnalysis exists but is missing in RustQL.

# Scope
- Add OP_LoadAnalysis to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_LoadAnalysis where appropriate

# Acceptance Criteria
- OP_LoadAnalysis is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_LoadAnalysis
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_LoadAnalysis implementation)
