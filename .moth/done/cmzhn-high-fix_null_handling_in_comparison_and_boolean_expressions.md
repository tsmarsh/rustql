# Fix NULL Handling In Comparison And Boolean Expressions [high]

## Problem
RustQL returns incorrect values when NULL is involved in comparisons and boolean
expressions. SQLite has specific three-valued logic rules for NULL that RustQL
doesn't properly implement.

## Failing Test Examples
From `expr.test`:
```
expr-1.44b expected: {} (NULL)
           got:      -1

expr-1.94 expected: 0
          got:      {} (NULL)

expr-1.111b expected: 0
            got:      {} (NULL)

expr-1.116b expected: yes
            got:      no
```

## SQLite NULL Semantics
- `NULL = NULL` → NULL (not TRUE or FALSE)
- `NULL AND FALSE` → FALSE  
- `NULL AND TRUE` → NULL
- `NULL OR TRUE` → TRUE
- `NULL OR FALSE` → NULL
- `NOT NULL` → NULL
- Comparison with NULL → NULL (except IS NULL, IS NOT NULL)
- CASE WHEN NULL THEN ... → skips to next WHEN/ELSE

## Key Issues
1. Comparisons returning -1 instead of NULL
2. Boolean expressions returning NULL when they should return 0 or 1
3. CASE expressions not properly short-circuiting on NULL conditions
4. IS NULL / IS NOT NULL possibly returning wrong values

## Reference
- `sqlite3/src/vdbe.c`: `OP_Eq`, `OP_Ne`, `OP_Lt`, `OP_Le`, `OP_Gt`, `OP_Ge` handlers
- `sqlite3/src/vdbe.c`: `OP_And`, `OP_Or`, `OP_Not` handlers
- `sqlite3/src/expr.c`: NULL propagation in expression trees

## Acceptance Criteria
```bash
make test-expr
```
All expr-1.* tests involving NULL comparisons should pass.

## Scope
- `src/vdbe/engine/mod.rs` - Comparison opcode handlers
- `src/vdbe/engine/handlers/` - Boolean logic opcodes
- `src/executor/select/` - CASE expression compilation
