# Implement Index-Based Query Optimization in WHERE Clause

## Problem
The query optimizer always uses table scans instead of available indexes. Tests show:
- Expected: `SEARCH t1 USING INDEX i1w (w=?)`
- Got: `SCAN TABLE`

## Affected Tests
- where.test: 93/316 passing (29%)
- Many tests expect EXPLAIN to show index usage

## Algorithm: Cost-Based Index Selection

SQLite uses a cost-based optimizer. The algorithm works as follows:

### 1. Cost Estimation Model

For a table with `nRow` rows:

```
Full table scan cost:     nRow * 3.0
Covering index scan:      nRow * K           (K ≈ 1.0 for index-only)
Non-covering index scan:  nRow * (K + 3.0)   (must fetch from table too)

Index seek cost:
  Covering:      nSeek * (log2(nRow) + K * nVisit)
  Non-covering:  nSeek * (log2(nRow) + (K + 3.0) * nVisit)
```

Where:
- `nSeek` = number of index seeks (usually 1 for equality, 2 for range)
- `nVisit` = estimated rows matching the condition
- `K` = index column access cost (varies by column count)

### 2. WHERE Clause Analysis

1. Parse WHERE into a list of terms (AND-separated conditions)
2. For each term, identify:
   - Column references
   - Operator type (=, <, >, <=, >=, BETWEEN, IN, LIKE)
   - Whether it's indexable (column OP constant or column OP column)

### 3. Index Matching Algorithm

For each available index on the table:
1. Check if leftmost index column matches a WHERE term with `=`
2. Continue matching subsequent columns while terms exist
3. For the last matched column, allow range operators (<, >, etc.)
4. Calculate estimated rows using index statistics or heuristics:
   - Equality: `nRow / nDistinct` (or `nRow / 10` if unknown)
   - Range: `nRow / 3` (rough estimate)
   - LIKE 'prefix%': `nRow / 10`

### 4. Plan Selection

1. Generate candidate plans: full scan + each usable index
2. Calculate cost for each plan
3. Select plan with lowest cost
4. For covering indexes (all SELECT columns in index), prefer over non-covering

### 5. Implementation Pseudo-code

```rust
fn select_best_index(table: &Table, where_clause: &[Term]) -> QueryPlan {
    let mut best_plan = QueryPlan::TableScan { cost: estimate_scan_cost(table) };

    for index in table.indexes() {
        if let Some(usable_terms) = match_index_to_where(index, where_clause) {
            let cost = estimate_index_cost(index, &usable_terms, table.row_count());
            if cost < best_plan.cost {
                best_plan = QueryPlan::IndexSeek {
                    index: index.clone(),
                    terms: usable_terms,
                    cost,
                };
            }
        }
    }

    best_plan
}

fn match_index_to_where(index: &Index, terms: &[Term]) -> Option<Vec<Term>> {
    let mut matched = Vec::new();

    for (i, index_col) in index.columns.iter().enumerate() {
        // Find equality term for this column
        if let Some(term) = terms.iter().find(|t| t.column == *index_col && t.op == Eq) {
            matched.push(term.clone());
        } else if i == matched.len() {
            // Try range term for last position only
            if let Some(term) = terms.iter().find(|t| t.column == *index_col && t.op.is_range()) {
                matched.push(term.clone());
            }
            break;
        } else {
            break;
        }
    }

    if matched.is_empty() { None } else { Some(matched) }
}
```

## Key Files to Modify
- `src/executor/select.rs` - Add index selection before query execution
- `src/vdbe/` - Add opcodes for index seek operations
- `src/executor/where_clause.rs` (new) - WHERE clause analysis

## EXPLAIN Output Format
Must output: `SEARCH table USING INDEX index_name (column=?)`

## Test Command
```bash
make test-where
```

## Regression Tests (Required)

Add these Rust unit tests to prevent regression:

### 1. `src/executor/tests/index_selection_tests.rs`
```rust
#[cfg(test)]
mod index_selection_tests {
    use super::*;

    #[test]
    fn test_single_column_equality_uses_index() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT, b INT)").unwrap();
        db.execute("CREATE INDEX i1 ON t1(a)").unwrap();

        let plan = db.explain_query_plan("SELECT * FROM t1 WHERE a = 5").unwrap();
        assert!(plan.contains("SEARCH") && plan.contains("USING INDEX i1"));
    }

    #[test]
    fn test_multi_column_index_partial_match() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT, b INT, c INT)").unwrap();
        db.execute("CREATE INDEX i1 ON t1(a, b)").unwrap();

        // Only first column matched - should still use index
        let plan = db.explain_query_plan("SELECT * FROM t1 WHERE a = 5").unwrap();
        assert!(plan.contains("USING INDEX i1"));
    }

    #[test]
    fn test_no_index_when_no_match() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT, b INT)").unwrap();
        db.execute("CREATE INDEX i1 ON t1(a)").unwrap();

        // Index on 'a' but WHERE uses 'b' - should scan
        let plan = db.explain_query_plan("SELECT * FROM t1 WHERE b = 5").unwrap();
        assert!(plan.contains("SCAN"));
    }

    #[test]
    fn test_covering_index_preferred() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT, b INT, c INT)").unwrap();
        db.execute("CREATE INDEX i1 ON t1(a, b)").unwrap();

        let plan = db.explain_query_plan("SELECT a, b FROM t1 WHERE a = 5").unwrap();
        assert!(plan.contains("COVERING INDEX"));
    }

    #[test]
    fn test_range_query_uses_index() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT)").unwrap();
        db.execute("CREATE INDEX i1 ON t1(a)").unwrap();

        let plan = db.explain_query_plan("SELECT * FROM t1 WHERE a > 5").unwrap();
        assert!(plan.contains("USING INDEX i1"));
    }

    #[test]
    fn test_index_with_multiple_conditions() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t1(a INT, b INT)").unwrap();
        db.execute("CREATE INDEX i1 ON t1(a, b)").unwrap();

        let plan = db.explain_query_plan("SELECT * FROM t1 WHERE a = 5 AND b = 10").unwrap();
        assert!(plan.contains("USING INDEX i1") && plan.contains("a=? AND b=?"));
    }
}
```

### 2. `src/executor/tests/query_results_tests.rs`
```rust
#[test]
fn test_index_query_returns_correct_results() {
    let db = setup_test_db();
    db.execute("CREATE TABLE t1(w INT, x INT, y INT)").unwrap();
    db.execute("CREATE INDEX i1w ON t1(w)").unwrap();

    for i in 1..=100 {
        db.execute(&format!("INSERT INTO t1 VALUES({}, {}, {})",
            i, (i as f64).log2() as i32, i*i + 2*i + 1)).unwrap();
    }

    let rows: Vec<_> = db.query("SELECT x, y, w FROM t1 WHERE w = 10").unwrap();
    assert_eq!(rows, vec![(3, 121, 10)]);
}
```

### Acceptance Criteria
- [ ] All tests in `index_selection_tests.rs` pass
- [ ] `make test-where` shows improvement from 29% baseline
- [ ] No regression in other test suites
