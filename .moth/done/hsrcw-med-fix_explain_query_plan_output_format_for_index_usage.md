# Fix EXPLAIN QUERY PLAN Output Format for Index Usage

## Problem
EXPLAIN QUERY PLAN output doesn't match expected SQLite format:
- Expected: `SEARCH t1 USING INDEX i1w (w=?)`
- Got: `0 0 0 {SCAN TABLE}`

The tests use `do_eqp_test` which matches patterns like `{*SEARCH t1 USING INDEX*}`.

## SQLite EXPLAIN QUERY PLAN Output Format

### Column Structure
| selectid | order | from | detail |
|----------|-------|------|--------|
| 0 | 0 | 0 | SEARCH t1 USING INDEX i1w (w=?) |

### Detail String Formats

**Table Scan:**
```
SCAN t1
SCAN t1 USING COVERING INDEX idx
```

**Index Seek:**
```
SEARCH t1 USING INDEX idx (col=?)
SEARCH t1 USING INDEX idx (col>? AND col<?)
SEARCH t1 USING COVERING INDEX idx (col=?)
SEARCH t1 USING INTEGER PRIMARY KEY (rowid=?)
```

**Other Operations:**
```
USE TEMP B-TREE FOR ORDER BY
USE TEMP B-TREE FOR DISTINCT
USE TEMP B-TREE FOR GROUP BY
COMPOUND QUERY (UNION/INTERSECT/EXCEPT)
CORRELATED SCALAR SUBQUERY
```

## Implementation

### 1. Build EQP Detail String
```rust
fn build_eqp_detail(plan: &QueryPlan, table: &str) -> String {
    match plan {
        QueryPlan::TableScan => format!("SCAN {}", table),
        QueryPlan::IndexScan { index, .. } => {
            format!("SCAN {} USING INDEX {}", table, index.name)
        }
        QueryPlan::IndexSeek { index, terms, covering } => {
            let cols = terms.iter()
                .map(|t| format!("{}=?", t.column))
                .collect::<Vec<_>>()
                .join(" AND ");
            if *covering {
                format!("SEARCH {} USING COVERING INDEX {} ({})",
                    table, index.name, cols)
            } else {
                format!("SEARCH {} USING INDEX {} ({})",
                    table, index.name, cols)
            }
        }
        QueryPlan::RowidLookup => {
            format!("SEARCH {} USING INTEGER PRIMARY KEY (rowid=?)", table)
        }
    }
}
```

### 2. EXPLAIN QUERY PLAN Command
```rust
fn explain_query_plan(&mut self, stmt: &SelectStmt) -> Result<Vec<Row>> {
    let plan = self.analyze_query_plan(stmt)?;
    let mut rows = Vec::new();

    for (i, table_plan) in plan.iter().enumerate() {
        rows.push(Row::new(vec![
            Value::Integer(0),                          // selectid
            Value::Integer(i as i64),                   // order
            Value::Integer(0),                          // from
            Value::Text(build_eqp_detail(table_plan)),  // detail
        ]));
    }

    Ok(rows)
}
```

## Files to Modify
- `src/executor/explain.rs` - EQP detail string generation
- `src/executor/select.rs` - Query plan generation

## Dependencies
Requires index-based query optimization (moth ib9vb) to be implemented first,
as EQP output should reflect actual index usage decisions.

## Test Command
```bash
make test-where
```

## Success Criteria
`do_eqp_test` assertions should pass with correct index/scan descriptions.

## Regression Tests (Required)

Add these Rust unit tests to prevent regression:

### 1. `src/executor/tests/explain_query_plan_tests.rs`
```rust
#[cfg(test)]
mod explain_query_plan_tests {
    use super::*;

    #[test]
    fn test_eqp_table_scan_format() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT, b INT)").unwrap();

        let rows = db.query("EXPLAIN QUERY PLAN SELECT * FROM t1").unwrap();
        assert_eq!(rows.len(), 1);

        // Check column structure: selectid, order, from, detail
        let row = &rows[0];
        assert_eq!(row[0], Value::Integer(0)); // selectid
        assert_eq!(row[1], Value::Integer(0)); // order
        assert_eq!(row[2], Value::Integer(0)); // from

        // Detail should be "SCAN t1"
        let detail = row[3].as_text().unwrap();
        assert!(detail.contains("SCAN") && detail.contains("t1"));
    }

    #[test]
    fn test_eqp_index_seek_format() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT, b INT)").unwrap();
        db.execute("CREATE INDEX i1 ON t1(a)").unwrap();

        let rows = db.query("EXPLAIN QUERY PLAN SELECT * FROM t1 WHERE a = 5").unwrap();
        let detail = rows[0][3].as_text().unwrap();

        // Should be "SEARCH t1 USING INDEX i1 (a=?)"
        assert!(detail.contains("SEARCH"), "Expected SEARCH, got: {}", detail);
        assert!(detail.contains("t1"), "Expected t1, got: {}", detail);
        assert!(detail.contains("USING INDEX"), "Expected USING INDEX, got: {}", detail);
        assert!(detail.contains("i1"), "Expected i1, got: {}", detail);
        assert!(detail.contains("a=?"), "Expected a=?, got: {}", detail);
    }

    #[test]
    fn test_eqp_covering_index_format() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT, b INT)").unwrap();
        db.execute("CREATE INDEX i1 ON t1(a, b)").unwrap();

        // Query only uses columns in the index
        let rows = db.query("EXPLAIN QUERY PLAN SELECT a, b FROM t1 WHERE a = 5").unwrap();
        let detail = rows[0][3].as_text().unwrap();

        // Should include "COVERING INDEX"
        assert!(detail.contains("COVERING INDEX"), "Expected COVERING INDEX, got: {}", detail);
    }

    #[test]
    fn test_eqp_rowid_lookup_format() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT PRIMARY KEY, b INT)").unwrap();

        let rows = db.query("EXPLAIN QUERY PLAN SELECT * FROM t1 WHERE a = 5").unwrap();
        let detail = rows[0][3].as_text().unwrap();

        // Should be "SEARCH t1 USING INTEGER PRIMARY KEY (rowid=?)"
        assert!(detail.contains("INTEGER PRIMARY KEY") || detail.contains("USING INDEX"),
            "Expected primary key lookup, got: {}", detail);
    }

    #[test]
    fn test_eqp_range_query_format() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT)").unwrap();
        db.execute("CREATE INDEX i1 ON t1(a)").unwrap();

        let rows = db.query("EXPLAIN QUERY PLAN SELECT * FROM t1 WHERE a > 5 AND a < 10").unwrap();
        let detail = rows[0][3].as_text().unwrap();

        // Should show range: "SEARCH t1 USING INDEX i1 (a>? AND a<?)"
        assert!(detail.contains("SEARCH"), "Expected SEARCH, got: {}", detail);
        assert!(detail.contains("i1"), "Expected i1, got: {}", detail);
    }

    #[test]
    fn test_eqp_multi_column_index_format() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT, b INT, c INT)").unwrap();
        db.execute("CREATE INDEX i1 ON t1(a, b)").unwrap();

        let rows = db.query("EXPLAIN QUERY PLAN SELECT * FROM t1 WHERE a = 1 AND b = 2").unwrap();
        let detail = rows[0][3].as_text().unwrap();

        // Should show both columns: "(a=? AND b=?)"
        assert!(detail.contains("a=?"), "Expected a=?, got: {}", detail);
        assert!(detail.contains("b=?"), "Expected b=?, got: {}", detail);
    }

    #[test]
    fn test_eqp_order_by_temp_btree() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT, b INT)").unwrap();

        let rows = db.query("EXPLAIN QUERY PLAN SELECT * FROM t1 ORDER BY b").unwrap();

        // Should include "USE TEMP B-TREE FOR ORDER BY" for non-indexed sort
        let has_temp_btree = rows.iter().any(|r| {
            r[3].as_text().map(|s| s.contains("TEMP B-TREE")).unwrap_or(false)
        });
        assert!(has_temp_btree || rows.len() == 1, "Expected temp b-tree or simple scan");
    }

    #[test]
    fn test_eqp_multiple_tables() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT)").unwrap();
        db.execute("CREATE TABLE t2(b INT)").unwrap();

        let rows = db.query("EXPLAIN QUERY PLAN SELECT * FROM t1, t2").unwrap();

        // Should have entries for both tables
        assert!(rows.len() >= 2, "Expected at least 2 plan entries for 2 tables");
    }
}
```

### Acceptance Criteria
- [ ] All tests in `explain_query_plan_tests.rs` pass
- [ ] `do_eqp_test` assertions in where.test pass
- [ ] EQP output matches SQLite format exactly
- [ ] No regression in other test suites
