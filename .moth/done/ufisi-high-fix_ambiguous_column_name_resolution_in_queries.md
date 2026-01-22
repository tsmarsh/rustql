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

## Regression Tests (Required)

Add these Rust unit tests to prevent regression:

### 1. `src/executor/tests/column_resolution_tests.rs`
```rust
#[cfg(test)]
mod column_resolution_tests {
    use super::*;

    #[test]
    fn test_unambiguous_single_table() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT, b INT, c INT)").unwrap();
        db.execute("INSERT INTO t1 VALUES(1, 2, 3)").unwrap();

        // Unqualified column in single table - should work
        let result: (i32,) = db.query_row("SELECT b FROM t1").unwrap();
        assert_eq!(result, (2,));
    }

    #[test]
    fn test_unambiguous_different_columns() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT, b INT)").unwrap();
        db.execute("CREATE TABLE t2(c INT, d INT)").unwrap();
        db.execute("INSERT INTO t1 VALUES(1, 2)").unwrap();
        db.execute("INSERT INTO t2 VALUES(3, 4)").unwrap();

        // Different column names - no ambiguity
        let result: (i32, i32) = db.query_row(
            "SELECT a, c FROM t1, t2"
        ).unwrap();
        assert_eq!(result, (1, 3));
    }

    #[test]
    fn test_ambiguous_same_column_name_errors() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(x INT)").unwrap();
        db.execute("CREATE TABLE t2(x INT)").unwrap();

        // Same column name in both tables without qualifier - should error
        let result = db.execute("SELECT x FROM t1, t2");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ambiguous"));
    }

    #[test]
    fn test_qualified_column_not_ambiguous() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(x INT)").unwrap();
        db.execute("CREATE TABLE t2(x INT)").unwrap();
        db.execute("INSERT INTO t1 VALUES(1)").unwrap();
        db.execute("INSERT INTO t2 VALUES(2)").unwrap();

        // Qualified column names - no ambiguity
        let result: (i32, i32) = db.query_row(
            "SELECT t1.x, t2.x FROM t1, t2"
        ).unwrap();
        assert_eq!(result, (1, 2));
    }

    #[test]
    fn test_alias_resolves_ambiguity() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(x INT)").unwrap();
        db.execute("INSERT INTO t1 VALUES(1)").unwrap();

        // Self-join with aliases
        let result: (i32, i32) = db.query_row(
            "SELECT a.x, b.x FROM t1 AS a, t1 AS b"
        ).unwrap();
        assert_eq!(result, (1, 1));
    }

    #[test]
    fn test_subquery_column_shadowing() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(x INT)").unwrap();
        db.execute("INSERT INTO t1 VALUES(1)").unwrap();

        // Inner x shadows outer x
        let result: (i32,) = db.query_row(
            "SELECT (SELECT x FROM t1 LIMIT 1) FROM t1"
        ).unwrap();
        assert_eq!(result, (1,));
    }

    #[test]
    fn test_natural_join_not_ambiguous() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(id INT, a INT)").unwrap();
        db.execute("CREATE TABLE t2(id INT, b INT)").unwrap();
        db.execute("INSERT INTO t1 VALUES(1, 10)").unwrap();
        db.execute("INSERT INTO t2 VALUES(1, 20)").unwrap();

        // NATURAL JOIN - 'id' is joined column, not ambiguous
        let result: (i32, i32, i32) = db.query_row(
            "SELECT id, a, b FROM t1 NATURAL JOIN t2"
        ).unwrap();
        assert_eq!(result, (1, 10, 20));
    }

    #[test]
    fn test_join_using_not_ambiguous() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(id INT, a INT)").unwrap();
        db.execute("CREATE TABLE t2(id INT, b INT)").unwrap();
        db.execute("INSERT INTO t1 VALUES(1, 10)").unwrap();
        db.execute("INSERT INTO t2 VALUES(1, 20)").unwrap();

        // JOIN USING - 'id' is joined column, not ambiguous
        let result: (i32,) = db.query_row(
            "SELECT id FROM t1 JOIN t2 USING(id)"
        ).unwrap();
        assert_eq!(result, (1,));
    }
}
```

### Acceptance Criteria
- [ ] All tests in `column_resolution_tests.rs` pass
- [ ] where-1.0 passes without "ambiguous column name" error
- [ ] Legitimate ambiguous column errors still occur
- [ ] No regression in other test suites
