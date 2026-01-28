# Problem
SQLite opcode OP_ReleaseReg exists but is missing in RustQL.

# Scope
- Add OP_ReleaseReg to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_ReleaseReg where appropriate

# Acceptance Criteria
- OP_ReleaseReg is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_ReleaseReg
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_ReleaseReg implementation)
