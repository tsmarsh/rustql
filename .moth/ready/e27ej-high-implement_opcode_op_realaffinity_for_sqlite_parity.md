# Problem
SQLite opcode OP_RealAffinity exists but is missing in RustQL.

# Scope
- Add OP_RealAffinity to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_RealAffinity where appropriate

# Acceptance Criteria
- OP_RealAffinity is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_RealAffinity
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_RealAffinity implementation)
