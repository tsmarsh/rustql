# Problem
SQLite opcode OP_DropTable exists but is missing in RustQL.

# Scope
- Add OP_DropTable to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_DropTable where appropriate

# Acceptance Criteria
- OP_DropTable is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_DropTable
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_DropTable implementation)
