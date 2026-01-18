# Implement VDBE KeyInfo/Collation Comparisons

## Problem
Current VDBE comparisons use simple `Mem::compare` and ignore KeyInfo/collations/affinity rules, which diverges from SQLite behavior for `Compare`, `IdxGE/IdxGT/IdxLE/IdxLT`, and `SorterCompare`.

## SQLite References
- `sqlite3/src/vdbe.c`: `OP_Compare`, `OP_IdxGE`, `OP_IdxGT`, `OP_IdxLE`, `OP_IdxLT`
- `sqlite3/src/vdbeaux.c`: `sqlite3VdbeRecordCompare`, `sqlite3VdbeRecordCompareWithSkip`
- `sqlite3/src/vdbe.c`: `sqlite3VdbeIdxKeyCompare` inline section

## RustQL Targets
- `src/vdbe/engine.rs`: `Opcode::Compare`, `Opcode::IdxGE`, `Opcode::IdxGT`, `Opcode::IdxLE`, `Opcode::IdxLT`, `Opcode::SorterCompare`
- `src/vdbe/ops.rs`: `KeyInfo` already exists; ensure it carries enough collation/sort metadata
- `src/vdbe/sort.rs`: record comparison helpers

## Requirements
- Use `KeyInfo` (collations, sort orders, number of key fields) for all index and sorter comparisons.
- Implement NULL ordering and comparison rules consistent with SQLite for index keys.
- Ensure `Compare` uses collations when provided by `KeyInfo` (or by P4 if used).
- Match SQLite behavior for `default_rc` and skip-of-PK fields where applicable.

## Tests
- Add unit tests for `Compare` with collation variations (binary vs nocase).
- Add tests for `IdxGE/IdxGT/IdxLE/IdxLT` with multi-column keys and NULLs.
- Add tests for `SorterCompare` with DESC sort orders and NULL keys.
- Tcl coverage to target:
  - `sqlite3/test/where*.test` (index comparisons)
  - `sqlite3/test/orderby*.test` (sorter order)
  - `sqlite3/test/expr*.test` (collation/compare)

## Notes
- Align control flow with SQLite (avoid refactors). Use C code as primary reference.
