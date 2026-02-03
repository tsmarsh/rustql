# SQLite vs RustQL (Differences Overview)

This document summarizes how RustQL differs from SQLite today. It focuses on
architecture, feature coverage, and current gaps.

## High-level Comparison

- **SQLite**: production-grade, public-domain, decades of optimizations. ~150K lines of C.
- **RustQL**: Rust translation with behavior parity as the goal. ~131K lines of Rust. Passes 91% of SQLite's test assertions.

Both systems follow the same pipeline:

1) Parse SQL into an AST.
2) Compile to bytecode (VDBE opcodes).
3) Execute via a bytecode interpreter.
4) Store data in B-tree pages via a pager with WAL.

## Feature Status

### Core SQL (Parsing + DDL + DML)

| Feature | SQLite | RustQL | Notes |
|---------|--------|--------|-------|
| SELECT | Complete | 98-100% | select1-select9 nearly all passing |
| INSERT | Complete | 90% | Including OR REPLACE, ON CONFLICT |
| UPDATE | Complete | 93% | Including complex WHERE clauses |
| DELETE | Complete | 85-92% | Including cascading deletes |
| CREATE TABLE | Complete | 93% | e_createtable tests |
| Expressions | Complete | 89-95% | expr and e_expr suites |
| Type affinity | Complete | 85-100% | types, types2, types3 |
| LIKE / GLOB | Complete | 98% | Pattern matching |
| BETWEEN | Complete | 100% | Range checks |
| LIMIT / OFFSET | Complete | 100% | Row limiting |
| NULL handling | Complete | 100% | NULL semantics |
| CAST | Complete | 67% | Type conversion |
| Subqueries | Complete | 72-83% | Correlated and uncorrelated |

### Functions

| Feature | SQLite | RustQL | Notes |
|---------|--------|--------|-------|
| Scalar functions | Complete | 99% | 14,550/14,630 assertions pass |
| Aggregate functions | Complete | ~52% | GROUP BY, HAVING work; some edge cases |
| Window functions | Complete | 6-50% | Basic window support; complex cases incomplete |
| Date/time functions | Complete | 76-97% | Core date ops work; edge cases vary |
| JSON functions | Complete | Partial | json feature flag enabled |

### Joins

| Feature | SQLite | RustQL | Notes |
|---------|--------|--------|-------|
| INNER JOIN | Complete | 80-100% | Core joins solid |
| LEFT JOIN | Complete | 80%+ | Works for common patterns |
| CROSS JOIN | Complete | Working | |
| Complex multi-way | Complete | 5-48% | Large join trees need work |

### Schema and DDL

| Feature | SQLite | RustQL | Notes |
|---------|--------|--------|-------|
| Views | Complete | 56% | CREATE/DROP/query work; edge cases remain |
| Indexes | Complete | 74-86% | B-tree indexes, partial indexes |
| Collation | Complete | 42-100% | NOCASE, BINARY work; custom collations partial |
| ALTER TABLE | Complete | 25-77% | ADD COLUMN works; some operations limited |
| VACUUM | Complete | 66-76% | Basic vacuum works |
| ANALYZE | Complete | 46-66% | Statistics collection partial |
| ATTACH/DETACH | Complete | 31-79% | Core attach works; cross-db triggers limited |
| AUTO_INCREMENT | Complete | 3% | Needs work |

### Triggers

| Feature | SQLite | RustQL | Notes |
|---------|--------|--------|-------|
| BEFORE/AFTER INSERT | Complete | Working | NEW pseudo-table access |
| BEFORE/AFTER UPDATE | Complete | Working | OLD/NEW pseudo-table access |
| BEFORE/AFTER DELETE | Complete | Working | OLD pseudo-table access |
| INSTEAD OF (views) | Complete | Working | |
| WHEN clause | Complete | Working | Conditional trigger execution |
| TEMP triggers | Complete | Working | Schema isolation |
| RAISE() | Complete | Partial | ABORT works; expression args incomplete |
| Nested triggers | Complete | Partial | Basic nesting; complex recursion limited |
| Trigger on DROP TABLE | Complete | Working | Cascade cleanup |
| trigger1 suite | 89 tests | 80% | 71/88 assertions pass |

### Foreign Keys

| Feature | SQLite | RustQL | Notes |
|---------|--------|--------|-------|
| CASCADE | Complete | Working | ON DELETE/UPDATE CASCADE |
| RESTRICT | Complete | Working | |
| SET NULL | Complete | Working | |
| Deferred checks | Complete | Working | |
| fkey2 suite | 882 tests | 79% | Primary FK test suite |

### Storage and Transactions

| Feature | SQLite | RustQL | Notes |
|---------|--------|--------|-------|
| B-tree (table) | Complete | 99% | btree01 suite |
| B-tree (index) | Complete | Working | |
| Pager | Complete | Working | Page cache, journal |
| WAL | Complete | Working | Write-ahead logging |
| Transactions | Complete | Working | BEGIN/COMMIT/ROLLBACK |
| Savepoints | Complete | Working | Nested savepoints |
| Corruption detection | Complete | 86-99% | corrupt test suites |
| PRAGMA | Complete | 55% | Many pragmas implemented |

### Virtual Tables and Extensions

| Feature | SQLite | RustQL | Notes |
|---------|--------|--------|-------|
| Virtual table API | Complete | Partial | Registration and basic dispatch |
| R\*Tree | Complete | Partial | Spatial indexing scaffold |
| FTS3/FTS4 | Complete | Scaffold | In-tree code, not fully integrated |
| FTS5 | Complete | Reserved | Feature flag only |
| Session/changeset | Complete | Partial | Change tracking basics |

### Query Planner

| Feature | SQLite | RustQL | Notes |
|---------|--------|--------|-------|
| Index selection | Complete | Working | Cost-based selection |
| WHERE optimization | Complete | 54-100% | Core WHERE solid; complex cases vary |
| Automatic indexes | Complete | 62-79% | autoindex4, autoindex5 |
| Cover index scans | Complete | 66% | coveridxscan |

## Known Behavioral Gaps

- **Window functions**: basic support works but complex window specifications (RANGE, GROUPS, EXCLUDE) are incomplete.
- **Complex joins**: large multi-way joins with complex predicates have lower pass rates.
- **AUTO_INCREMENT**: not yet fully implemented.
- **Some ALTER TABLE operations**: RENAME COLUMN, DROP COLUMN partial.
- **Virtual table execution**: schema registration present, full runtime dispatch incomplete.
- **FTS modules**: scaffold exists but not wired into the SQL layer as virtual tables.
- **RAISE() expressions**: ABORT works; expression arguments (e.g., `RAISE(ABORT, expr)`) not yet supported.
- **Recursive triggers**: `PRAGMA recursive_triggers` partially honored.

## Why This Matters

SQLite is the reference behavior; RustQL is intentionally following SQLite's
shape. Differences should be considered **temporary unless explicitly noted**.

## Where to Look

- Architecture: `docs/architecture.md`
- VDBE details: `docs/vdbe.md`
- Storage/B-tree: `docs/btree.md`
