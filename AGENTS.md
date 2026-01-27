# Repository Guidelines

## Agent Role (Architect)
- Own system-level design and cross-module integration for RustQL.
- Review changes that span parser/executor/VDBE/storage to prevent stovepipes.
- Keep architecture docs current and actionable (`docs/architecture.md`, `docs/vdbe.md`, `docs/btree.md`, `docs/differences.md`).
- Drive end-to-end test coverage strategy and ensure gaps become moths.

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
- For FTS and other extensions, **do not** implement placeholder or approximate behavior. Port directly from the SQLite C sources in `sqlite3/ext/` and keep semantics aligned.

## Testing Guidelines
- The primary framework is Tcl via the `testfixture` binary.
- Use `testfixture test/<name>.test` for focused coverage and `make devtest` for broader coverage.
- Preserve coverage-related special comments such as `NO_TEST` or `OPTIMIZATION-IF-TRUE` in `sqlite3/src/`.

## Commit & Pull Request Guidelines
- This tree mirrors SQLite, whose upstream uses Fossil and generally does not accept pull requests because the code is public domain.
- There is no Git history in this workspace; if you commit locally, prefer issue-linked messages like `[abc12] Fix X` (see “Workflow & Issues”).

## Workflow & Issues (Team Collaboration)

This is a team project with multiple agents. **Always coordinate via git and moth.**

### The Golden Rules
1. **Always `git pull` before starting work** - sync with team
2. **Always `moth start` before writing code** - claim the issue
3. **Always push after `moth start`** - teammates see you're working on it
4. **Always push after `moth done`** - signal completion
5. **Always `git pull` after completing work** - stay in sync

### Before Starting ANY Implementation

```bash
# 1. Sync with remote
git fetch origin && git pull origin main

# 2. Check for work in progress
moth ls -t doing

# 3. If nothing in progress, pick an issue (priority order: crit > high > med > low)
moth ls -t ready -s crit
moth ls -t ready

# 4. Start the moth BEFORE writing any code
moth start {id}

# 5. Push immediately so teammates know you claimed it
git add .moth/ && git commit -m "[{id}] Started work on: {title}"
git push origin main
```

### During Development

```bash
# Commit frequently with issue ID prefix
git add src/
git commit -m "[{id}] Implement feature X"
git push origin main
```

### Completing Work

```bash
# 1. Mark done
moth done

# 2. Push completion status
git add .moth/ && git commit -m "[{id}] Completed: {title}"
git push origin main

# 3. Sync and pick up next issue
git fetch origin && git pull origin main
moth ls -t ready
```

### Resuming After Context Loss

```bash
git fetch origin && git pull origin main
moth ls -t doing              # Check if you have work in progress
moth show                     # View current issue details
# If nothing in doing, start a new issue per "Before Starting" workflow
```

### Quick Reference

| Command | Purpose |
|---------|---------|
| `moth ls` | List active issues |
| `moth ls -t ready` | List available issues |
| `moth ls -t doing` | See what's being worked on |
| `moth show` | Show current issue |
| `moth start {id}` | Claim an issue |
| `moth done` | Complete current issue |

**Why this matters:** If you don't push moth status changes, another agent may start the same work, causing merge conflicts and wasted effort.
