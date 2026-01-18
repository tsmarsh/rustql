# Implement External Sorter Semantics

## Problem
Sorter opcodes currently use in-memory vectors and simplified key handling. SQLite uses an external sorter with PMAs and multi-way merge that preserves ORDER BY semantics and memory limits.

## SQLite References
- `sqlite3/src/vdbesort.c`: sorter implementation (PMAs, merges, temp files)
- `sqlite3/src/vdbe.c`: `OP_SorterOpen`, `OP_SorterInsert`, `OP_SorterSort`, `OP_SorterNext`, `OP_SorterData`, `OP_SorterCompare`, `OP_SortKey`, `OP_ResetSorter`
- `sqlite3/src/select.c`: ORDER BY and sorter usage

## RustQL Targets
- `src/vdbe/engine.rs`: all `Sorter*` opcodes and `SortKey`
- `src/vdbe/sort.rs`: wire to the sorter module instead of `Vec<Vec<u8>>`
- `src/vdbe/ops.rs`: confirm opcodes/p4 semantics match SQLite

## Requirements
- Implement external sorter pipeline with memory limits and temp files.
- `SorterCompare` and `SortKey` must use sorter key prefix rules (KeyInfo + number of ORDER BY columns).
- `ResetSorter` should reset sorter state without leaking temp files.
- Ensure result order and stability match SQLite.

## Tests
- Unit tests for `SorterSort`, `SorterCompare`, `SortKey`, `SorterData` with multi-column keys.
- Tcl coverage to target:
  - `sqlite3/test/orderby*.test`
  - `sqlite3/test/sort*.test`
  - `sqlite3/test/distinct.test`

## Notes
- Preserve SQLite flow and error handling; avoid refactors.
