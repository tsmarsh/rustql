# Problem
SQLite opcode OP_IfSizeBetween exists but is missing in RustQL.

# Scope
- Add OP_IfSizeBetween to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_IfSizeBetween where appropriate

# Acceptance Criteria
- OP_IfSizeBetween is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_IfSizeBetween
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_IfSizeBetween implementation)
