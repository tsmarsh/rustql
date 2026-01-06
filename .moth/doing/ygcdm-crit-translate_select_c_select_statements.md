# Translate select.c - SELECT Statements

## Overview
Translate SELECT statement code generation including joins, subqueries, aggregates, and compound selects.

## Source Reference
- `sqlite3/src/select.c` - 9,060 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Functions

### SELECT Compilation
- `sqlite3Select()` - Main SELECT compiler
- `selectInnerLoop()` - Inner loop code generation
- `generateSortTail()` - ORDER BY final output
- `flattenSubquery()` - Subquery flattening optimization
- `multiSelect()` - UNION/INTERSECT/EXCEPT handling

### Join Processing
- Compute FROM clause join order
- Generate nested loop join code
- Handle LEFT/RIGHT/CROSS joins
- Optimize join order based on indexes

### Aggregate Processing
- `groupBySort()` - GROUP BY with sorting
- `groupByHash()` - GROUP BY with hash table
- Aggregate function accumulation
- HAVING clause filtering

### DISTINCT Processing
- Sort-based distinct
- Hash-based distinct
- Result deduplication

## Key Code Generation

```rust
impl<'a> Parse<'a> {
    /// Compile a SELECT statement
    pub fn compile_select(&mut self, select: &SelectStmt) -> Result<()> {
        // Handle compound selects (UNION, etc.)
        if let Some(compound) = &select.compound {
            return self.compile_compound_select(select, compound);
        }

        // Resolve names
        self.resolve_select_names(select)?;

        // Generate code
        let dest = self.select_dest();
        self.generate_select(select, &dest)?;

        Ok(())
    }

    fn generate_select(&mut self, select: &SelectStmt, dest: &SelectDest) -> Result<()> {
        // Open cursors for FROM tables
        let cursors = self.open_from_cursors(&select.from)?;

        // Generate WHERE loop
        let where_info = self.generate_where_loop(select, &cursors)?;

        // Inner loop: evaluate result columns
        self.select_inner_loop(select, dest, &where_info)?;

        // Handle ORDER BY
        if select.order_by.is_some() {
            self.generate_sort_tail(dest)?;
        }

        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] Basic SELECT with column list
- [ ] FROM clause with multiple tables
- [ ] JOIN (INNER, LEFT, RIGHT, CROSS)
- [ ] WHERE clause filtering
- [ ] GROUP BY with aggregates
- [ ] HAVING clause
- [ ] ORDER BY with ASC/DESC
- [ ] LIMIT and OFFSET
- [ ] DISTINCT
- [ ] UNION/INTERSECT/EXCEPT
- [ ] Subqueries (correlated and uncorrelated)
- [ ] Scalar subqueries in SELECT list
