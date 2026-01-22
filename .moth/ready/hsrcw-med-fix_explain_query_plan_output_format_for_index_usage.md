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
