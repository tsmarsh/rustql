# RustQL

RustQL is a memory-safe, from-scratch rewrite of SQLite in Rust. It keeps SQLite’s architecture, control flow, and observable behavior as the source of truth while making the codebase easier to evolve.

## Goals And Compatibility

- **SQLite compatibility first**: every change is measured against upstream SQLite behavior.
- **Mechanical translation**: prefer direct, readable ports over refactors.
- **Operational parity**: preserve performance intent and error semantics.

When RustQL diverges, the difference is documented in `docs/differences.md`. The upstream SQLite C tree lives in `sqlite3/` and is used as the reference implementation.

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
rustql = { git = "https://github.com/tsmarsh/rustql.git" }
```

Open a database and run a statement:

```rust
use rustql::{
    sqlite3_open, sqlite3_prepare_v2, sqlite3_step, sqlite3_finalize, sqlite3_close,
};

let mut conn = sqlite3_open(":memory:")?;
let (mut stmt, _) = sqlite3_prepare_v2(&mut conn, "CREATE TABLE t(a INTEGER)")?;
sqlite3_step(&mut stmt)?;
sqlite3_finalize(stmt)?;
sqlite3_close(conn)?;
```

Query rows:

```rust
use rustql::{
    sqlite3_prepare_v2, sqlite3_step, sqlite3_finalize, sqlite3_column_int,
    sqlite3_column_text, StepResult,
};

let (mut stmt, _) = sqlite3_prepare_v2(&mut conn, "SELECT id, name FROM users")?;
while let StepResult::Row = sqlite3_step(&mut stmt)? {
    let id = sqlite3_column_int(&stmt, 0);
    let name = sqlite3_column_text(&stmt, 1);
    println!("User {}: {}", id, name);
}
sqlite3_finalize(stmt)?;
```

## What's Implemented

RustQL passes **91% of SQLite's individual test assertions** across 1,176 TCL test files (330,953 / 360,948 assertions). 224 test suites pass completely. The codebase is ~131K lines of Rust across 122 source files.

Major subsystems:

- **SQL compiler**: tokenizer, parser, AST, resolver, and code generator
- **VDBE**: bytecode compiler and interpreter with 150+ opcodes
- **Storage**: B-tree (table and index), pager, WAL, page cache
- **Query planner**: WHERE clause optimization, index selection, cost estimation
- **DML**: INSERT, UPDATE, DELETE with conflict resolution (OR REPLACE, etc.)
- **Triggers**: BEFORE/AFTER/INSTEAD OF, WHEN clauses, NEW/OLD pseudo-tables, nested execution
- **Foreign keys**: cascade, restrict, set null, deferred checks
- **Functions**: scalar, aggregate, and window functions (14,550/14,630 func tests pass)
- **Expressions**: full expression evaluation (95% of e_expr tests pass)
- **Views**: CREATE VIEW, DROP VIEW, queryable views
- **Collation**: custom collation sequences, NOCASE, BINARY, RTRIM
- **ATTACH/DETACH**: multiple database files in a single connection
- **Transactions**: BEGIN, COMMIT, ROLLBACK, SAVEPOINT, nested savepoints
- **VFS**: Unix and Windows file system abstraction
- **Session/changeset**: change tracking and application
- **R\*Tree**: spatial indexing
- **FTS3**: full-text search (scaffold, integration in progress)

### Test Suite Highlights

| Area | Pass Rate | Notes |
|------|-----------|-------|
| SELECT | 98-100% | select1 through select9 |
| INSERT | 90% | Core insert operations |
| UPDATE | 93% | Core update operations |
| DELETE | 85-92% | Core delete operations |
| Expressions | 89-95% | expr, e_expr |
| Functions | 99-100% | func, func2, func3 |
| Types | 85-100% | types, types2, types3 |
| LIKE/BETWEEN | 98-100% | Pattern matching |
| LIMIT | 100% | Row limiting |
| NULL handling | 100% | NULL semantics |
| JOINs | 80-100% | Core join operations |
| Subqueries | 72-83% | Correlated and uncorrelated |
| Triggers | 80% | trigger1 (primary suite) |
| Foreign keys | 79% | fkey2 (primary suite) |
| B-tree | 99% | btree01 |
| Corruption detection | 86-99% | corrupt, corruptC, corruptF |

Feature completeness varies; the authoritative signal is the test suite.

## Feature Flags

Feature flags are used to gate optional subsystems:

```toml
[features]
default = ["fts5", "rtree", "session"]
fts3 = []
fts5 = []
rtree = []
session = []
json = []
tui = ["crossterm"]
```

`fts3` enables the in-tree FTS3 implementation. `fts5` is reserved for a future port. `rtree`, `session`, and `json` compile their respective modules when enabled. `tui` enables the interactive terminal database browser (see below).

## Architecture

RustQL mirrors SQLite’s internal layers:

```
┌─────────────────────────────────────────────────────────────┐
│                         API Layer                           │
│  (sqlite3_open, sqlite3_prepare, sqlite3_step, etc.)         │
├─────────────────────────────────────────────────────────────┤
│                      SQL Compiler                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐   │
│  │ Tokenizer│→ │  Parser  │→ │ Resolver │→ │ Code Gen   │   │
│  └──────────┘  └──────────┘  └──────────┘  └────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                Virtual Database Engine (VDBE)               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              Bytecode Interpreter                     │   │
│  │  (OpenRead, Column, Insert, ... opcodes)              │   │
│  └──────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                      B-Tree Layer                           │
│  ┌─────────────────┐  ┌─────────────────────────────────┐   │
│  │  Table B-Trees  │  │       Index B-Trees             │   │
│  └─────────────────┘  └─────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                       Pager Layer                           │
│  ┌──────────┐  ┌───────────┐  ┌─────────────────────────┐   │
│  │  Cache   │  │    WAL    │  │     Page Management     │   │
│  └──────────┘  └───────────┘  └─────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                    OS Abstraction (VFS)                     │
│  ┌──────────────────────┐  ┌────────────────────────────┐   │
│  │    Unix (libc)       │  │    Windows (windows-sys)   │   │
│  └──────────────────────┘  └────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

More detailed docs:

- `docs/architecture.md`
- `docs/vdbe.md`
- `docs/btree.md`
- `docs/differences.md`
- [`docs/tui.md`](docs/tui.md) — TUI database browser

## Module Map

| Module | Description |
|--------|-------------|
| `api/` | C-style API surface |
| `parser/` | Tokenizer, grammar, AST |
| `executor/` | Planning, code generation |
| `vdbe/` | Bytecode VM and ops |
| `storage/` | B-tree, pager, WAL, cache |
| `functions/` | Scalar/aggregate/date-time functions |
| `schema/` | Tables, indexes, metadata |
| `mem/` | Memory allocation/tracking |
| `os/` | VFS implementation |
| `util/` | Varints, hashing, bitvecs |

## Build And Test

```bash
# Build
cargo build

# Run tests
cargo test

# Feature-gated builds
cargo build --features "fts3,rtree,session,json"

# Release build
cargo build --release
```

## TUI Database Browser

RustQL includes an interactive terminal UI for browsing tables and running queries, built with crossterm. Build with the `tui` feature flag:

```bash
cargo build --features tui
```

Launch from the REPL with `.browse`:

```
$ rustql mydb.sqlite
rustql> .browse
```

Navigate tables with `j`/`k`, select with `Enter`, scroll data with arrow keys, type SQL with `:`, and exit with `q`. See [`docs/tui.md`](docs/tui.md) for the full usage guide and key bindings.

## SQLite Test Suite

RustQL runs SQLite's full TCL test suite (1,176 test files) using a TCL extension that loads RustQL as the `sqlite3` implementation.

### Using the Makefile

```bash
# Run the full test suite in parallel
make test

# Run a specific SQLite test (output to terminal)
make test-select1
make test-trigger1

# Show pass rates from existing results
make pass-rates

# Show pass/fail summary
make test-summary

# Show detailed per-file report
make test-report

# List all available test targets
make list-tests
```

Test results are stored in `test-results/`:
- `<test>.result` - PASSED, FAILED, or SKIPPED (suite-level)
- `<test>.log` - Full test output with individual assertion results

### Building the TCL Extension

```bash
# Via Makefile (recommended, includes all feature flags)
make tcl-extension

# Or directly with cargo
cargo build --release --features tcl,fts3,fts5,rtree,session,json --lib
```

This produces `target/release/librustql.so` (Linux) or `librustql.dylib` (macOS).

### Running Tests Manually

For interactive testing or debugging:

```tcl
load ./target/release/librustql.so
sqlite3 db :memory:
db eval {CREATE TABLE t(x); INSERT INTO t VALUES(1),(2),(3)}
db eval {SELECT * FROM t}  ;# Returns: 1 2 3
db close
```

### Test Wrapper Script

For running individual tests with proper setup:

```bash
tclsh scripts/run_sqlite_test.tcl select1
tclsh scripts/run_sqlite_test.tcl insert
```

## Contributing And Workflow

This project uses [moth](https://github.com/tsmarsh/moth) to coordinate work.

```bash
moth ls -t ready
moth start {id}
moth done
```

See `AGENTS.md` for the required workflow steps before starting any implementation.

### Pre-commit Hook

RustQL uses a Git pre-commit hook to enforce formatting, clippy, and unit tests
before each commit.

Install the hook:

```bash
./scripts/install-hooks.sh
```

## License

Licensed under either of:

- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT license (`LICENSE-MIT`)

## Acknowledgments

RustQL is a translation of SQLite, which is public domain. SQLite’s design documents and source tree are the primary references for behavior and architecture.
