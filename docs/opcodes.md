# VDBE Opcode Architecture (SQLite-Shape)

This document explains how VDBE opcodes fit together in RustQL, using SQLite’s
execution model as the source of truth. It is intended to guide opcode parity
work and prevent bespoke opcode paths.

## Core Model

SQL statements compile to a linear program of opcodes executed by the VDBE
engine. Opcodes operate on:

- **Registers**: temporary values (Mem cells)
- **Cursors**: table/index handles over the btree or virtual tables
- **P1..P5 / P4**: operands (integers + typed payload)

The VDBE program is a single-threaded interpreter. Control flow is explicit
(Goto/If/Return), and all data access is mediated by cursors.

## Execution Phases (Typical Shape)

1. **Init**: set up registers, program state
2. **Transaction**: open/verify transaction and schema cookie
3. **Open**: create cursors for tables/indexes/temp
4. **Scan**: Rewind/Next (or Seek*) to visit rows
5. **Eval**: Column/Rowid + expression ops into registers
6. **Filter**: If/Compare/Jump to include/exclude rows
7. **Output / DML**: ResultRow or Insert/Update/Delete
8. **Halt**: finalize statement

## Opcode Families (How They Fit Together)

### 1) Control Flow
- `Init`, `Goto`, `If`, `IfNot`, `Return`, `Gosub`, `Yield`, `Halt`
- Used to structure loops, subroutines, and row iteration.

### 2) Cursor Open/Close
- `OpenRead`, `OpenWrite`, `OpenEphemeral`, `OpenAutoindex`, `Close`
- Establishes cursors that the scan/eval ops depend on.

### 3) Cursor Movement / Seek
- `Rewind`, `Next`, `Prev`, `SeekRowid`, `SeekGE`, `SeekGT`, `SeekLE`, `SeekLT`
- These define table/index traversal. They must honor cursor stability
  semantics (SQLite’s “cursor moved” model).

### 4) Row Access
- `Column`, `Rowid`, `RowData` (SQLite), `MakeRecord`, `DecodeRecord`
- Read a row into registers or build a record for write operations.

### 5) Expression Evaluation
- `Integer`, `Int64`, `Real`, `String`, `Null`, `Copy`, `Move`
- Arithmetic/logic ops (`Add`, `Subtract`, `And`, `Or`, `Between`, etc.)
- Comparisons (`Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge`, `Compare`, `Jump`)

### 6) DML (Table/Index Writes)
- `Insert`, `InsertInt`, `Delete`, `NewRowid`, `IdxInsert`, `IdxDelete`
- Must update cursor staleness and follow SQLite’s btree write contracts.

### 7) Sorting / Distinct
- `SorterOpen`, `SorterInsert`, `SorterSort`, `SorterNext`, `SorterData`
- `Sort`, `SortKey`, `RowSet*` (SQLite) for DISTINCT and ORDER BY

### 8) Schema / Metadata
- `ParseSchema`, `CreateBtree`, `DropTable`, `DropIndex`, `ReadCookie`,
  `SetCookie`, `VerifyCookie`

### 9) Transactions / Savepoints
- `Transaction`, `AutoCommit`, `Savepoint`, `ReadCookie`, `SetCookie`
- Must match SQLite’s locking and durability semantics.

### 10) Triggers / FKs
- `Program`, `Param`, `TriggerProlog`, `TriggerTest`, `FkCheck`, `FkCounter`
- Driven by compiler; VDBE execution must preserve register state.

### 11) Virtual Tables
- `VOpen`, `VFilter`, `VNext`, `VColumn`, `VUpdate`, `VCreate`, `VDestroy`
- All vtabs should use these opcodes and module registry dispatch.

### 12) Pragmas / Maintenance
- `JournalMode`, `Pagecount`, `IntegrityCk`, `Vacuum`, `IncrVacuum`
- Required for control-plane parity with SQLite.

## SQLite Parity Expectations

- **No bespoke opcode semantics**: any opcode must map to SQLite’s behavior
  or be renamed/removed.
- **Cursor movement model** must follow SQLite: btree marks cursor moved,
  VDBE restores or returns NULL (not global data-version hacks).
- **Record comparison** uses SQLite’s KeyInfo/collation rules.

## References
- SQLite opcode implementations: `sqlite3/src/vdbe.c`, `sqlite3/src/vdbeaux.c`
- RustQL opcode enum: `src/vdbe/ops.rs`
- RustQL VDBE engine: `src/vdbe/engine/mod.rs`
