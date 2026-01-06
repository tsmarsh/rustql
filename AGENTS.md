# Repository Guidelines

## Project Structure & Module Organization
- `sqlite3/` is the SQLite source tree. Core C sources live in `sqlite3/src/`.
- Tests are primarily Tcl scripts under `sqlite3/test/` (plus `src/test*.c` helpers).
- Build/test tooling and generators are under `sqlite3/tool/` and `sqlite3/autoconf/`.
- Extensions are in `sqlite3/ext/`; internal docs are in `sqlite3/doc/`.
- Issue tracking for this workspace uses `.moth/` (see “Workflow & Issues”).

## Build, Test, and Development Commands
Run commands from a separate build directory when possible:
- `../sqlite3/configure` and `make sqlite3` to build the CLI.
- `make sqlite3.c` to build the amalgamation.
- `make testfixture` then `testfixture test/main.test` for a single test file.
- `make devtest` for developer tests; `make releasetest` for the full suite.
- Windows (MSVC): `nmake /f Makefile.msc sqlite3.exe` and `nmake /f Makefile.msc devtest`.

## Coding Style & Naming Conventions
- Follow existing C conventions in nearby files; avoid wholesale reformatting.
- Keep block comments with the `**` prefix style as used across `sqlite3/src/`.
- Tests in C typically use `test*.c`; Tcl tests use `*.test` under `sqlite3/test/`.
- Avoid editing generated artifacts (e.g., `sqlite3.c`, `sqlite3.h`); regenerate via `make target_source` or the scripts in `sqlite3/tool/`.

## Generated Code & Parser Notes
- `sqlite3/src/parse.y` is a Lemon grammar (not yacc/bison). Rust cannot consume Lemon directly.
- If translating the parser, prefer a Rust-native generator like `lalrpop` or a hand-written parser; keep `parse.y` as the source of truth.
- Keep any Rust code generation behind `build.rs` or `tools/` scripts and document inputs/outputs.

## Design Fidelity (SQLite Philosophy)
- SQLite’s “Why C?” rationale (`https://sqlite.org/whyc.html`) is core context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior, performance intent, and architectural shape unless a change is explicitly agreed.
- Favor mechanical translations over refactors; keep control flow and error handling closely aligned to upstream C.

## Testing Guidelines
- The primary framework is Tcl via the `testfixture` binary.
- Use `testfixture test/<name>.test` for focused coverage and `make devtest` for broader coverage.
- Preserve coverage-related special comments such as `NO_TEST` or `OPTIMIZATION-IF-TRUE` in `sqlite3/src/`.

## Commit & Pull Request Guidelines
- This tree mirrors SQLite, whose upstream uses Fossil and generally does not accept pull requests because the code is public domain.
- There is no Git history in this workspace; if you commit locally, prefer issue-linked messages like `[abc12] Fix X` (see “Workflow & Issues”).

## Workflow & Issues
- Issues live under `.moth/` and are managed with the `moth` CLI.
- Typical flow: `moth ls`, `moth show`, `moth start <id>`, `moth done <id>`.
