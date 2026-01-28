# Problem
SQLite opcode OP_IntegrityCk exists but is missing in RustQL.

# Scope
- Add OP_IntegrityCk to src/vdbe/ops.rs if absent
- Implement semantics in src/vdbe/engine/mod.rs
- Ensure planner/executor emits OP_IntegrityCk where appropriate

# Acceptance Criteria
- OP_IntegrityCk is present in opcode enum and executed in VDBE
- Behavior matches SQLite for tests that exercise OP_IntegrityCk
- docs/vdbe.md updated if opcode catalog changes

# References
- sqlite3/src/vdbe.c (OP_IntegrityCk implementation)
