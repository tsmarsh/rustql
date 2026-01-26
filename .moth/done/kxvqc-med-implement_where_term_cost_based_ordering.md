# Implement WHERE Term Cost-Based Ordering

## Problem

When evaluating WHERE clauses with multiple conditions, the order of evaluation matters for performance. Currently, we evaluate terms in the order they appear in the SQL. SQLite reorders terms to minimize cost.

### Example

```sql
SELECT * FROM t1 WHERE expensive_function(x) > 0 AND w = 5
```

**Current behavior**: Evaluate `expensive_function(x)` for every row, then check `w = 5`

**Optimal behavior**: Check `w = 5` first (cheap, uses index), only call `expensive_function(x)` for matching rows

### Cost Factors

SQLite considers these when ordering WHERE terms:

1. **Index availability**: Terms that can use an index are cheapest
2. **Column location**: Index-covered columns vs table columns
3. **Operation type**: Simple comparisons < function calls < subqueries
4. **Selectivity**: More selective terms should come first (fail fast)

## Technical Details

### Cost Categories (lowest to highest)

| Category | Example | Relative Cost |
|----------|---------|---------------|
| Index seek term | `w = 5` (w indexed) | 0 (handled by seek) |
| Index-covered column | `w > 3` (w in index) | 1 |
| Table column comparison | `x = 10` | 10 |
| Simple function | `ABS(x) > 5` | 20 |
| String function | `LENGTH(name) > 10` | 30 |
| Subquery | `x IN (SELECT ...)` | 100 |
| Correlated subquery | `EXISTS (SELECT ... WHERE t2.a = t1.a)` | 1000 |

### Required Changes

1. **Add cost estimation to WhereTerm**:
   ```rust
   pub struct WhereTerm {
       // ... existing fields ...
       pub eval_cost: i32,  // Estimated cost to evaluate this term
   }
   ```

2. **Implement cost calculation**:
   ```rust
   fn estimate_term_cost(term: &WhereTerm, index_columns: &HashSet<i32>) -> i32 {
       // Check if term uses index-covered column
       if let Some((_, col_idx)) = term.left_col {
           if index_columns.contains(&col_idx) {
               return 1;  // Cheap: in index
           }
           return 10;  // Medium: table lookup needed
       }
       // Check for functions, subqueries, etc.
       estimate_expr_cost(&term.expr)
   }
   ```

3. **Sort terms before code generation**:
   ```rust
   fn compile_where_condition(&mut self, where_clause: &Expr) -> Result<()> {
       let mut terms = self.extract_and_terms(where_clause);

       // Sort by cost (cheapest first)
       terms.sort_by_key(|t| t.eval_cost);

       // Generate short-circuit evaluation code
       for term in terms {
           self.compile_term_with_jump(term, skip_label)?;
       }
   }
   ```

### Interaction with Short-Circuit Evaluation

This optimization works best WITH short-circuit evaluation (moth ooqhg):
- Short-circuit ensures we stop at first failing term
- Cost ordering ensures we evaluate cheapest terms first
- Together: maximum performance

### Files to Modify

- `src/executor/where_clause.rs` - WhereTerm cost field, cost estimation
- `src/executor/select/mod.rs` - Term sorting before compilation

### Test Cases

Queries with mixed-cost conditions should:
1. Evaluate index terms first
2. Evaluate simple comparisons before functions
3. Show improved search_count (fewer unnecessary evaluations)

### Definition of Done

- [ ] WhereTerm has cost estimate
- [ ] Terms sorted by cost before compilation
- [ ] Index-covered terms evaluated first
- [ ] Function calls deferred until necessary
- [ ] Works in conjunction with short-circuit evaluation
- [ ] No regression in existing tests
