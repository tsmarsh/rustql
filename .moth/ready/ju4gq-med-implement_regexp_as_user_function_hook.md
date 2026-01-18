# Implement REGEXP As User Function Hook

## Problem
`OP_Regexp` is currently a small built-in matcher. SQLite treats REGEXP as a user-defined function/extension (usually via `ext/misc/regexp.c`). This diverges from SQLite behavior and configurability.

## SQLite References
- `sqlite3/ext/misc/regexp.c`: example REGEXP function implementation
- `sqlite3/src/func.c`: function registration and dispatch
- `sqlite3/src/vdbe.c`: `OP_Regexp` behavior

## RustQL Targets
- `src/vdbe/engine.rs`: `Opcode::Regexp` should call the function registry, not a built-in matcher
- `src/functions/scalar.rs`: add REGEXP function registration hook (or extension mechanism)
- Optional: add feature-gated regex crate or plug-in mechanism

## Requirements
- `OP_Regexp` should delegate to the registered REGEXP function and preserve NULL semantics.
- Provide a default implementation compatible with SQLite’s `regexp.c` or expose an extension hook.
- Ensure case sensitivity and anchoring follow the selected engine.

## Tests
- Unit tests for `OP_Regexp` with custom registration/hook.
- Tcl coverage to target:
  - `sqlite3/test/regexp1.test` (if present)
  - `sqlite3/test/e_expr.test` (REGEXP expressions)

## Notes
- Keep behavior consistent with SQLite; no hard-coded minimal regex unless feature-gated and documented.
