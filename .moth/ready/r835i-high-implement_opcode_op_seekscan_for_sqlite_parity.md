# Problem
SQLite opcode OP_SeekScan exists but is missing in RustQL.

# Scope
- Add OP_SeekScan to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_SeekScan where appropriate

# Acceptance Criteria
- OP_SeekScan is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_SeekScan
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_SeekScan implementation)
