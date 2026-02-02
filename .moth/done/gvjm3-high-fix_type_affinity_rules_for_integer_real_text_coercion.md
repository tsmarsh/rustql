# Fix Type Affinity Rules For Integer Real Text Coercion [high]

## Problem
RustQL incorrectly determines column type affinity in query results. SQLite has
specific rules about when values should be INTEGER vs REAL vs TEXT, and RustQL
diverges from these rules.

## Failing Test Examples
From `types.test`:
```
Expected: integer integer text real
Got:      integer real text real

Expected: blob blob blob blob  
Got:      blob blob text blob
```

## SQLite Type Affinity Rules
SQLite determines type affinity based on:
1. Column declared type (INTEGER, REAL, TEXT, BLOB, NUMERIC)
2. Expression context (comparisons, arithmetic, concatenation)
3. Storage class of the value itself

Key rules RustQL violates:
- Integer literals should stay INTEGER type, not become REAL
- BLOB affinity should remain BLOB in expressions
- NUMERIC affinity should prefer INTEGER over REAL when possible

## Reference
- `sqlite3/src/vdbemem.c`: `sqlite3VdbeMemNumerify()`, type coercion logic
- `sqlite3/src/expr.c`: `sqlite3ExprAffinity()`, affinity determination
- `sqlite3/doc/datatype3.html`: Official type affinity documentation

## Acceptance Criteria
```bash
make test-types
make test-types2  
make test-types3
```
All type affinity tests should match SQLite's reported types exactly.

## Scope
- `src/types/` - Value type representation
- `src/vdbe/engine/` - Opcode handlers for type coercion
- `src/executor/` - Expression compilation with affinity hints
