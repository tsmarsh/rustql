# Fix Ambiguous Column Name Resolution in Queries

## Problem
Queries fail with "ambiguous column name" when they shouldn't:
```
Error: ambiguous column name: y
```

This occurs in where-1.0 setup which creates valid tables and should work.

## When Columns Are Ambiguous

A column is ambiguous when:
1. Multiple tables in FROM clause have the same column name
2. No table qualifier is used (e.g., `y` instead of `t1.y`)

A column is NOT ambiguous when:
1. Only one table has that column name
2. Table qualifier is used (e.g., `t1.y`)
3. Column is aliased in SELECT (e.g., `y AS col1`)

## SQLite Column Resolution Algorithm

### Resolution Order
1. Check if column has table qualifier → resolve directly
2. Search all tables in FROM clause for matching column
3. If found in exactly one table → use it
4. If found in multiple tables → error "ambiguous column name"
5. If not found → error "no such column"

### Special Cases
- **Self-join**: Same table aliased twice - must use alias
- **Subqueries**: Inner columns shadow outer with same name
- **Natural JOIN**: Joined columns are not ambiguous
- **USING clause**: Joined columns are not ambiguous

## Implementation

```rust
fn resolve_column(
    &self,
    col_name: &str,
    table_name: Option<&str>,
    from_clause: &FromClause,
) -> Result<ResolvedColumn> {
    // If table specified, resolve directly
    if let Some(tbl) = table_name {
        return self.resolve_qualified_column(tbl, col_name, from_clause);
    }

    // Search all tables for unqualified column
    let mut matches = Vec::new();

    for table_ref in from_clause.tables() {
        if let Some(col_info) = self.find_column_in_table(table_ref, col_name)? {
            matches.push((table_ref, col_info));
        }
    }

    match matches.len() {
        0 => Err(Error::no_such_column(col_name)),
        1 => Ok(matches.into_iter().next().unwrap()),
        _ => Err(Error::ambiguous_column(col_name)),
    }
}
```

## Debugging the Bug

The error appears during table setup, not query execution:
```tcl
do_test where-1.0 {
  execsql {
    CREATE TABLE t1(w int, x int, y int);
    CREATE TABLE t2(p int, q int, r int, s int);
  }
  ...
}
```

Possible causes:
1. Column resolution running during CREATE TABLE (shouldn't)
2. Previous test leaving stale state
3. Subquery in INSERT incorrectly resolving columns

## Files to Modify
- `src/executor/select.rs` - Column resolution logic
- `src/parser/expr.rs` - Column reference parsing

## Test Command
```bash
make test-where
```

## Success Criteria
- where-1.0 should pass without "ambiguous column name" error
- Legitimate ambiguous references should still error correctly
