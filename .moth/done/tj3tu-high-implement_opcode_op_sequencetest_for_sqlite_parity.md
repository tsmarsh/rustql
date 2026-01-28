# Problem
SQLite opcode OP_SequenceTest exists but is missing in RustQL.

# Scope
- Add OP_SequenceTest to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_SequenceTest where appropriate

# Acceptance Criteria
- OP_SequenceTest is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_SequenceTest
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_SequenceTest implementation)
