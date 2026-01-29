# Problem
Opcode TriggerTest exists in RustQL but has no SQLite equivalent (per vdbe.c).

# Scope
- Determine if TriggerTest is a rename of a SQLite opcode (align name/semantics)
- Otherwise remove it and update compiler/engine usage

# Acceptance Criteria
- Opcode list matches SQLite naming and semantics
- No references to TriggerTest remain (or it is mapped to SQLite name)

# References
- sqlite3/src/vdbe.c (opcode cases)
