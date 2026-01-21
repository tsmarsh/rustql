# Implement Common Table Expressions (WITH clause)

Support for SQL WITH clause (CTEs).

## Syntax
```sql
WITH cte_name AS (
    SELECT ...
)
SELECT * FROM cte_name;

-- Recursive CTE
WITH RECURSIVE cte_name AS (
    SELECT ...  -- base case
    UNION ALL
    SELECT ... FROM cte_name  -- recursive case
)
SELECT * FROM cte_name;
```

## Current Status
- `near "WITH": syntax error` in tests
- Parser does not recognize WITH keyword

## Implementation Steps
1. Add WITH token to lexer
2. Add CTE AST nodes to parser
3. Implement non-recursive CTE execution (materialized subquery)
4. Implement recursive CTE execution (iterative evaluation)

## Files to Modify
- `src/parser/lexer.rs` - Add WITH token
- `src/parser/parser.rs` - Parse CTE syntax
- `src/parser/ast.rs` - Add Cte struct
- `src/executor/select.rs` - Execute CTEs

## Test Files
- with1.test, with2.test, with3.test in SQLite test suite

## Notes
Recursive CTEs are more complex and could be a separate issue.
Start with non-recursive CTEs which are essentially named subqueries.
