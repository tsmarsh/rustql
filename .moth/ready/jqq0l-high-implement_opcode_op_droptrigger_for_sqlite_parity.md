# Problem
SQLite opcode OP_DropTrigger exists but is missing in RustQL.

# Scope
- Add OP_DropTrigger to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_DropTrigger where appropriate

# Acceptance Criteria
- OP_DropTrigger is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_DropTrigger
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_DropTrigger implementation)
