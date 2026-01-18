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

## What’s Implemented

RustQL already includes major SQLite subsystems:

- SQL tokenizer, parser, AST, and resolver
- VDBE bytecode compiler and interpreter
- B-tree storage, pager, and WAL
- Query planning and WHERE clause optimization
- Triggers and foreign keys
- Scalar and aggregate functions
- Window functions
- VFS for Unix and Windows
- Session/change tracking
- R*Tree indexing
- Virtual table plumbing for FTS3 (work in progress)

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
```

`fts3` enables the in-tree FTS3 implementation. `fts5` is reserved for a future port. `rtree`, `session`, and `json` compile their respective modules when enabled.

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

## SQLite Test Suite

RustQL can run SQLite's TCL test suite using the TCL extension. The canonical tests live in `sqlite3/test/`.

### Building the TCL Extension

```bash
cargo build --release --features tcl
```

This produces `target/release/librustql.so` (Linux) or `librustql.dylib` (macOS).

### Running SQLite TCL Tests

Load the extension into `tclsh` and run tests against RustQL instead of SQLite:

```bash
cd sqlite3/test
tclsh
```

```tcl
# Load RustQL as the sqlite3 implementation
load ../../target/release/librustql.so

# Source the test infrastructure
source tester.tcl

# Run a specific test file
source select1.test
```

Or run individual tests interactively:

```tcl
load ../../target/release/librustql.so
sqlite3 db :memory:
db eval {CREATE TABLE t(x); INSERT INTO t VALUES(1),(2),(3)}
db eval {SELECT * FROM t}  ;# Returns: 1 2 3
db close
```

### Quick Smoke Tests

```bash
# Run basic TCL extension tests
cargo build --features tcl
tclsh tests/run_tcl_test.tcl tests/basic_tcl.test
```

## Contributing And Workflow

This project uses [moth](https://github.com/tsmarsh/moth) to coordinate work.

```bash
moth ls -t ready
moth start {id}
moth done
```

See `AGENTS.md` for the required workflow steps before starting any implementation.

## License

Licensed under either of:

- Apache License, Version 2.0 (`LICENSE-APACHE`)
- MIT license (`LICENSE-MIT`)

## Acknowledgments

RustQL is a translation of SQLite, which is public domain. SQLite’s design documents and source tree are the primary references for behavior and architecture.
