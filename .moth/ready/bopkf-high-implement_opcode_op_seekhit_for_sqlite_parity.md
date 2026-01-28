# Problem
SQLite opcode OP_SeekHit exists but is missing in RustQL.

# Scope
- Add OP_SeekHit to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_SeekHit where appropriate

# Acceptance Criteria
- OP_SeekHit is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_SeekHit
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_SeekHit implementation)
