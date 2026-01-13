# Architecture Overview

This document summarizes the RustQL architecture and its current state, with
focus on module layout and execution flow. It also highlights where SQLite-like
features (such as virtual tables and FTS) are partially implemented.

## High-level Map

```mermaid
flowchart TD
  UserSQL["SQL Input"] --> Parser["Parser + AST"]
  Parser --> Compiler["Statement Compiler"]
  Compiler --> VDBE["VDBE Bytecode"]
  VDBE --> Engine["VDBE Engine"]
  Engine --> Storage["Storage + Btree + Pager"]
  Engine --> Schema["Schema Registry"]
  Engine --> Functions["Scalar/Aggr Functions"]
  Engine --> Ext["Extensions (FTS, RTree, etc.)"]

  Schema --> Parser
  Storage --> Schema
```

## Module Layout

```mermaid
flowchart LR
  subgraph src
    api["api/ (public SQLite-style API)"]
    parser["parser/ (tokenizer + grammar + AST)"]
    executor["executor/ (SQL -> VDBE compiler)"]
    vdbe["vdbe/ (ops + engine + mem + auxdata)"]
    storage["storage/ (pager + btree + wal)"]
    schema["schema/ (schema registry + DDL modeling)"]
    functions["functions/ (built-in SQL functions)"]
    util["util/ (helpers)"]
    rtree["rtree.rs (RTree extension)"]
    fts3["fts3/ (FTS3 scaffolding)"]
  end

  api --> parser
  api --> executor
  executor --> vdbe
  vdbe --> storage
  vdbe --> schema
  vdbe --> functions
```

## Query Execution Path

```mermaid
sequenceDiagram
  autonumber
  participant Client as Client
  participant API as api::SqliteConnection
  participant Parser as parser::grammar
  participant Comp as executor::prepare
  participant VDBE as vdbe::engine
  participant Store as storage::btree/pager

  Client->>API: sqlite3_prepare_v2(sql)
  API->>Parser: parse_stmt(sql)
  Parser-->>API: AST
  API->>Comp: compile(AST)
  Comp-->>API: VDBE ops
  API->>VDBE: sqlite3_step()
  VDBE->>Store: read/write pages
  Store-->>VDBE: rows/records
  VDBE-->>API: row or done
  API-->>Client: row values
```

## DDL and Schema Registration

SQLite-style DDL statements are compiled to VDBE ops that call `ParseSchema`.
The VDBE engine parses the CREATE SQL and updates the in-memory schema.

```mermaid
flowchart TD
  CreateSQL["CREATE TABLE / CREATE VIRTUAL TABLE"]
  CreateSQL --> Compiler["executor::prepare"]
  Compiler --> ParseSchemaOp["VDBE Opcode::ParseSchema"]
  ParseSchemaOp --> VDBEEngine["vdbe::engine"]
  VDBEEngine --> Schema["Schema Registry"]
```

Notes:
- `CREATE VIRTUAL TABLE` is parsed and registered in schema as a virtual table.
- The runtime virtual-table opcodes (xFilter/xNext/xColumn/xRowid) are not yet
  implemented, so vtab queries are not fully dispatched.

## Storage Subsystem

```mermaid
flowchart TD
  VDBEEngine["vdbe::engine"] --> Btree["storage::btree"]
  Btree --> Pager["storage::pager"]
  Pager --> VFS["os/ (VFS abstractions)"]
  Pager --> WAL["storage::wal"]
```

## FTS3 Scaffold (Current State)

The FTS3 code is partially translated and lives behind the `fts3` feature.
It currently provides in-memory segment construction and term lookup but is
not yet integrated as a virtual table module.

```mermaid
flowchart TD
  Tokens["Tokenize input"] --> Pending["PendingTerms"]
  Pending --> Leaf["LeafNode encode"]
  Leaf --> Segments["In-memory segments"]
  Segments --> Lookup["Term lookup"]
  Lookup --> Doclist["Doclist merge"]
```

## Virtual Tables (Planned)

This is the intended shape based on SQLite's pattern. Only the schema part is
implemented today.

```mermaid
flowchart TD
  CreateVTab["CREATE VIRTUAL TABLE"] --> Schema["Schema Registry"]
  Query["VDBE Query"] --> VTabOps["Virtual Table Opcodes"]
  VTabOps --> Module["Module Registry"]
  Module --> XFilter["xFilter/xNext/xColumn/xRowid"]
  Module --> Storage["Module storage/indexes"]
```

## Extension Points

```mermaid
flowchart LR
  VDBE["VDBE"] --> Functions["Functions"]
  VDBE --> Extensions["Extensions"]
  Extensions --> rtree["rtree"]
  Extensions --> fts3["fts3"]
```

## Open Gaps vs SQLite

- Virtual-table execution opcodes and module registry are missing.
- FTS3 is present as an internal module, but not yet attached to a vtab.
- DDL schema parsing is simplified and does not track all SQLite metadata.
