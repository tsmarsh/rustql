# Setup Cargo.toml and Project Structure

## Overview
Initialize the Rust project with proper Cargo configuration and module structure to support the SQLite3 translation.

## Source Reference
- New project (no C source file)

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Tasks

### 1. Create Cargo.toml
```toml
[package]
name = "rustql"
version = "0.1.0"
edition = "2021"
description = "SQLite3 database engine rewritten in Rust"
license = "MIT OR Apache-2.0"

[features]
default = ["fts5", "rtree", "session"]
fts3 = []
fts5 = []
rtree = []
session = []
json = []

[dependencies]
bitflags = "2"

[dev-dependencies]
```

### 2. Create Module Structure
```
src/
├── lib.rs              # Library root, re-exports public API
├── error.rs            # Error types (separate issue)
├── types.rs            # Core type aliases (separate issue)
├── storage/
│   ├── mod.rs
│   ├── btree.rs        # B-tree implementation
│   ├── pager.rs        # Page cache management
│   ├── wal.rs          # Write-ahead logging
│   └── pcache.rs       # Page cache
├── vdbe/
│   ├── mod.rs
│   ├── engine.rs       # VDBE core execution
│   ├── ops.rs          # Opcodes and instructions
│   ├── mem.rs          # Value/memory handling
│   └── sort.rs         # Sorting operations
├── parser/
│   ├── mod.rs
│   ├── tokenizer.rs    # SQL tokenization
│   ├── ast.rs          # Abstract syntax tree
│   └── grammar.rs      # SQL grammar/parser
├── executor/
│   ├── mod.rs
│   ├── select.rs       # SELECT processing
│   ├── insert.rs       # INSERT processing
│   ├── update.rs       # UPDATE processing
│   ├── delete.rs       # DELETE processing
│   └── planner.rs      # WHERE clause / query planning
├── functions/
│   ├── mod.rs
│   ├── scalar.rs       # Scalar functions
│   ├── aggregate.rs    # Aggregate functions
│   ├── datetime.rs     # Date/time functions
│   └── json.rs         # JSON functions
├── os/
│   ├── mod.rs
│   ├── vfs.rs          # Virtual filesystem trait
│   ├── unix.rs         # Unix implementation
│   └── windows.rs      # Windows implementation
└── util/
    ├── mod.rs
    ├── hash.rs         # Hash table
    └── bitvec.rs       # Bit vector
```

### 3. Create stub lib.rs
```rust
//! RustQL - SQLite3 rewritten in Rust

pub mod error;
pub mod types;
pub mod storage;
pub mod vdbe;
pub mod parser;
pub mod executor;
pub mod functions;
pub mod os;
pub mod util;

// Re-export main public types
pub use error::{Error, Result};
```

## Acceptance Criteria
- [ ] Cargo.toml created with appropriate metadata and features
- [ ] All module directories created with mod.rs stubs
- [ ] Project compiles with `cargo check`
- [ ] Basic lib.rs exports the module structure
