# Epic: VDBE Opcode Parity & Semantics

## Intent
Complete opcode coverage and align semantics with SQLite's VDBE, ensuring
control flow, register behavior, and error propagation are indistinguishable
from SQLite for supported features.

## Architectural Alignment
- VDBE is the single execution engine for SQL.
- Opcode semantics mirror SQLite's behavior and edge cases.

## Scope
- Implement missing opcodes and full operand semantics.
- Align memory/register handling, affinity rules, and comparisons.
- Implement missing control paths (coroutines, triggers, FKs, savepoints).

## Out of Scope
- Non-SQLite opcodes or alternate execution engines.

## Acceptance Criteria (How We Assert This Is Done)
- Opcode catalog in `src/vdbe/ops.rs` matches SQLite for supported features.
- No opcodes in the execution path are stubs or placeholders; any remaining
  unsupported opcodes are explicitly called out in `docs/differences.md`.
- SQLite Tcl tests listed in this epic pass in RustQL, with no untracked
  exclusions.
- Error codes and messages match SQLite for relevant test cases.

## Test Targets (SQLite Tcl)
Run each with `testfixture test/<name>.test`:
- `sqlite3/test/expr*.test`
- `sqlite3/test/select*.test`
- `sqlite3/test/where*.test`
- `sqlite3/test/join*.test`
- `sqlite3/test/trigger*.test`
- `sqlite3/test/fkey*.test`
- `sqlite3/test/e_expr.test`
- `sqlite3/test/e_select.test`
- `sqlite3/test/e_select2.test`
- `sqlite3/test/e_fkey.test`
- `sqlite3/test/e_insert.test`
- `sqlite3/test/e_update.test`
- `sqlite3/test/e_delete.test`

## Verification Steps
- Run opcode-heavy Tcl suites and compare against SQLite outputs.
- Audit `src/vdbe/engine/` for TODO/stub paths tied to opcodes.
- Ensure `docs/vdbe.md` reflects the final opcode set and semantics.
