# Problem
Opcode AggStep0 exists in RustQL but has no SQLite equivalent (per vdbe.c).

# Scope
- Determine if AggStep0 is a rename of a SQLite opcode (align name/semantics)
- Otherwise remove it and update compiler/engine usage

# Acceptance Criteria
- Opcode list matches SQLite naming and semantics
- No references to AggStep0 remain (or it is mapped to SQLite name)

# References
- sqlite3/src/vdbe.c (opcode cases)
