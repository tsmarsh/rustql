
## Summary
Implement OP_OpenPseudo using SQLite's logic as the source of truth.

## Source Reference
- sqlite3/src/vdbe.c (opcode implementation)
- sqlite3/src/vdbeaux.c and sqlite3/src/btree.c as needed

## Design Fidelity
- Preserve SQLite observable behavior, error semantics, and control flow.
- Prefer mechanical translation over refactors.

## Acceptance Criteria
- Implement opcode semantics matching SQLite (including edge cases and NULL handling).
- Add unit tests:
  - Add pseudo-cursor unit tests (ephemeral row materialization)

- SQLite Tcl tests that should pass before completion:
  - sqlite3/test/select1.test
  - sqlite3/test/select3.test

