# Problem
Opcode Noop exists in RustQL but has no SQLite equivalent (per vdbe.c).

# Scope
- Determine if Noop is a rename of a SQLite opcode (align name/semantics)
- Otherwise remove it and update compiler/engine usage

# Acceptance Criteria
- Opcode list matches SQLite naming and semantics
- No references to Noop remain (or it is mapped to SQLite name)

# References
- sqlite3/src/vdbe.c (opcode cases)
