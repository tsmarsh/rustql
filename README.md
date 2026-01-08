# RustQL

A pure Rust implementation of the SQLite database engine.

## Overview

RustQL is a from-scratch translation of SQLite3 into Rust, preserving SQLite's architecture, behavior, and API semantics while leveraging Rust's safety guarantees. The project aims to provide a fully compatible SQLite implementation that can be used as a drop-in replacement in Rust applications.

## Status

**Work in Progress** - This project is under active development. Core functionality is implemented but not all SQLite features are complete.

- 658 tests passing
- ~84,000 lines of Rust code
- Cross-platform support (Unix/Windows)

## Features

### Implemented

- **SQL Parser** - Full SQL parsing with support for SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, and more
- **VDBE (Virtual Database Engine)** - Bytecode compiler and execution engine
- **B-Tree Storage** - Page-based storage with B-tree indexes
- **WAL (Write-Ahead Logging)** - Journal-based transaction support
- **Query Optimizer** - WHERE clause optimization with index selection
- **Aggregate Functions** - SUM, COUNT, AVG, MIN, MAX, GROUP_CONCAT, etc.
- **Scalar Functions** - 70+ built-in functions (string, math, date/time, JSON)
- **Window Functions** - ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, etc.
- **Triggers** - BEFORE/AFTER triggers for INSERT, UPDATE, DELETE
- **Foreign Keys** - Constraint enforcement with CASCADE, SET NULL, etc.
- **Collations** - BINARY, NOCASE, RTRIM, and custom collation support
- **Session Extension** - Change tracking, changesets, and conflict resolution
- **Backup API** - Online database backup
- **Blob I/O** - Incremental blob read/write
- **R*Tree** - Spatial indexing extension

### Optional Features (Cargo features)

```toml
[features]
default = ["fts5", "rtree", "session"]
fts3 = []      # Full-text search (FTS3)
fts5 = []      # Full-text search (FTS5)
rtree = []     # R*Tree spatial indexes
session = []   # Change tracking sessions
json = []      # JSON functions
```

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
rustql = { git = "https://github.com/tsmarsh/rustql.git" }
```

## Usage

### Opening a Database

```rust
use rustql::{sqlite3_open, sqlite3_close};

let conn = sqlite3_open(":memory:")?;
// ... use the connection
sqlite3_close(conn)?;
```

### Executing Queries

```rust
use rustql::{
    sqlite3_open, sqlite3_prepare_v2, sqlite3_step, sqlite3_finalize,
    sqlite3_column_int, sqlite3_column_text,
};

let conn = sqlite3_open("test.db")?;

// Create a table
let (stmt, _) = sqlite3_prepare_v2(&mut conn, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;
sqlite3_step(&mut stmt)?;
sqlite3_finalize(stmt)?;

// Insert data
let (stmt, _) = sqlite3_prepare_v2(&mut conn, "INSERT INTO users (name) VALUES ('Alice')")?;
sqlite3_step(&mut stmt)?;
sqlite3_finalize(stmt)?;

// Query data
let (mut stmt, _) = sqlite3_prepare_v2(&mut conn, "SELECT id, name FROM users")?;
while let StepResult::Row = sqlite3_step(&mut stmt)? {
    let id = sqlite3_column_int(&stmt, 0);
    let name = sqlite3_column_text(&stmt, 1);
    println!("User {}: {}", id, name);
}
sqlite3_finalize(stmt)?;
```

### Using Parameters

```rust
use rustql::{sqlite3_prepare_v2, sqlite3_bind_text, sqlite3_bind_int};

let (mut stmt, _) = sqlite3_prepare_v2(&mut conn, "INSERT INTO users (id, name) VALUES (?, ?)")?;
sqlite3_bind_int(&mut stmt, 1, 42)?;
sqlite3_bind_text(&mut stmt, 2, "Bob")?;
sqlite3_step(&mut stmt)?;
```

### Session Extension (Change Tracking)

```rust
use rustql::{
    sqlite3session_create, sqlite3session_attach, sqlite3session_changeset,
    sqlite3changeset_apply,
};

// Create a session to track changes
let mut session = sqlite3session_create(&mut conn, "main")?;
sqlite3session_attach(&mut session, Some("users"))?;

// Make changes...
// INSERT INTO users (name) VALUES ('Charlie');

// Get the changeset
let changeset = sqlite3session_changeset(&session)?;

// Apply changeset to another database
sqlite3changeset_apply(&mut other_conn, &changeset, None, None)?;
```

## Architecture

RustQL mirrors SQLite's internal architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                         API Layer                           │
│  (sqlite3_open, sqlite3_prepare, sqlite3_step, etc.)       │
├─────────────────────────────────────────────────────────────┤
│                      SQL Compiler                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐  │
│  │ Tokenizer│→ │  Parser  │→ │ Resolver │→ │ Code Gen   │  │
│  └──────────┘  └──────────┘  └──────────┘  └────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                Virtual Database Engine (VDBE)               │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              Bytecode Interpreter                     │  │
│  │  (100+ opcodes: OpenRead, Column, Insert, etc.)      │  │
│  └──────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                      B-Tree Layer                           │
│  ┌─────────────────┐  ┌─────────────────────────────────┐  │
│  │  Table B-Trees  │  │       Index B-Trees             │  │
│  └─────────────────┘  └─────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                       Pager Layer                           │
│  ┌──────────┐  ┌───────────┐  ┌─────────────────────────┐  │
│  │  Cache   │  │    WAL    │  │     Page Management     │  │
│  └──────────┘  └───────────┘  └─────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                    OS Abstraction (VFS)                     │
│  ┌──────────────────────┐  ┌────────────────────────────┐  │
│  │    Unix (libc)       │  │    Windows (windows-sys)   │  │
│  └──────────────────────┘  └────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Module Structure

| Module | Description |
|--------|-------------|
| `api/` | Public API functions (C API translation) |
| `parser/` | SQL tokenizer, AST, and grammar |
| `executor/` | Query planning, WHERE optimization, code generation |
| `vdbe/` | Virtual machine, bytecode, expression evaluation |
| `storage/` | B-tree, pager, WAL, page cache |
| `functions/` | Scalar, aggregate, date/time, JSON functions |
| `schema/` | Table, column, index metadata |
| `mem/` | Memory allocation and tracking |
| `os/` | Platform abstraction (Unix/Windows VFS) |
| `util/` | Utilities (varint, hash, bitvec) |

## Building

```bash
# Build the library
cargo build

# Run tests
cargo test

# Build with specific features
cargo build --features "fts5,rtree,session,json"

# Build release
cargo build --release
```

## Testing

```bash
# Run all tests
cargo test

# Run tests for a specific module
cargo test session

# Run with output
cargo test -- --nocapture
```

## Contributing

This project uses [moth](https://github.com/tsmarsh/moth) for issue tracking. Issues are stored in `.moth/` as markdown files.

```bash
# List available issues
moth ls -t ready

# Start working on an issue
moth start {id}

# Mark issue as done
moth done
```

See [AGENTS.md](AGENTS.md) for team workflow guidelines.

## Design Philosophy

RustQL follows these principles from SQLite:

1. **Behavioral Compatibility** - Match SQLite's observable behavior
2. **Architectural Fidelity** - Preserve SQLite's internal structure
3. **Mechanical Translation** - Favor direct translation over refactoring
4. **Performance Intent** - Maintain SQLite's performance characteristics

While we use Rust instead of C, we preserve:
- Control flow patterns
- Error handling semantics
- API signatures (adapted to Rust idioms)
- Data structures and algorithms

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Acknowledgments

This project is a translation of [SQLite](https://sqlite.org/), which is in the public domain. We gratefully acknowledge the SQLite team's decades of work on this exceptional piece of software.
