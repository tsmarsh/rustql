# Problem
SQLite opcode OP_JournalMode exists but is missing in RustQL.

# Scope
- Add OP_JournalMode to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_JournalMode where appropriate

# Acceptance Criteria
- OP_JournalMode is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_JournalMode
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_JournalMode implementation)
