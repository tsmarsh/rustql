# Translate walker.c - AST Walker

## Overview
Translate the AST walking utilities for traversing and transforming expression and select trees.

## Source Reference
- `sqlite3/src/walker.c` - 261 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Types

### Walk Result
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalkResult {
    /// Continue walking
    Continue,

    /// Prune this branch (don't visit children)
    Prune,

    /// Abort walk entirely
    Abort,
}
```

### Walker Trait
```rust
/// Callback for expression walking
pub trait ExprWalker {
    /// Called for each expression node
    fn walk_expr(&mut self, expr: &mut Expr) -> WalkResult;

    /// Called for each select (if any)
    fn walk_select(&mut self, _select: &mut SelectStmt) -> WalkResult {
        WalkResult::Continue
    }
}
```

## Walker Implementation

### Expression Walking

```rust
/// Walk an expression tree
pub fn walk_expr<F>(expr: &mut Expr, callback: F) -> WalkResult
where
    F: FnMut(&mut Expr) -> WalkResult,
{
    walk_expr_impl(expr, &mut callback)
}

fn walk_expr_impl<F>(expr: &mut Expr, callback: &mut F) -> WalkResult
where
    F: FnMut(&mut Expr) -> WalkResult,
{
    // Pre-order: visit node first
    match callback(expr) {
        WalkResult::Abort => return WalkResult::Abort,
        WalkResult::Prune => return WalkResult::Continue,
        WalkResult::Continue => {}
    }

    // Visit children
    match expr {
        Expr::Null | Expr::Integer(_) | Expr::Float(_) |
        Expr::String(_) | Expr::Blob(_) | Expr::Bool(_) |
        Expr::Column(_) | Expr::Variable(_) => {
            // Leaf nodes - no children
        }

        Expr::Unary { expr: child, .. } => {
            if walk_expr_impl(child, callback) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }

        Expr::Binary { left, right, .. } => {
            if walk_expr_impl(left, callback) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if walk_expr_impl(right, callback) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }

        Expr::Between { expr, low, high, .. } => {
            if walk_expr_impl(expr, callback) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if walk_expr_impl(low, callback) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if walk_expr_impl(high, callback) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }

        Expr::In { expr, list, .. } => {
            if walk_expr_impl(expr, callback) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            match list {
                InList::Values(values) => {
                    for v in values {
                        if walk_expr_impl(v, callback) == WalkResult::Abort {
                            return WalkResult::Abort;
                        }
                    }
                }
                InList::Select(select) => {
                    // Walk select separately if needed
                }
            }
        }

        Expr::Case { operand, when_clauses, else_clause } => {
            if let Some(op) = operand {
                if walk_expr_impl(op, callback) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
            for (when_expr, then_expr) in when_clauses {
                if walk_expr_impl(when_expr, callback) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
                if walk_expr_impl(then_expr, callback) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
            if let Some(else_expr) = else_clause {
                if walk_expr_impl(else_expr, callback) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
        }

        Expr::Function { args, filter, over, .. } => {
            match args {
                FunctionArgs::Args(exprs) => {
                    for e in exprs {
                        if walk_expr_impl(e, callback) == WalkResult::Abort {
                            return WalkResult::Abort;
                        }
                    }
                }
                FunctionArgs::Star => {}
                FunctionArgs::Distinct(e) => {
                    if walk_expr_impl(e, callback) == WalkResult::Abort {
                        return WalkResult::Abort;
                    }
                }
            }
            if let Some(f) = filter {
                if walk_expr_impl(f, callback) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
            // Window spec expressions if needed
        }

        Expr::Cast { expr: child, .. } |
        Expr::Collate { expr: child, .. } => {
            if walk_expr_impl(child, callback) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }

        Expr::Like { expr, pattern, escape, .. } => {
            if walk_expr_impl(expr, callback) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if walk_expr_impl(pattern, callback) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if let Some(esc) = escape {
                if walk_expr_impl(esc, callback) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
        }

        Expr::IsNull { expr: child, .. } |
        Expr::Exists { subquery: child, .. } => {
            // Handle subquery if needed
        }

        Expr::Subquery(select) => {
            // Walk select if needed
        }
    }

    WalkResult::Continue
}
```

### Select Walking

```rust
/// Walk a SELECT statement
pub fn walk_select<F>(select: &mut SelectStmt, callback: &mut F) -> WalkResult
where
    F: ExprWalker,
{
    // Result columns
    for col in &mut select.columns {
        if let ResultColumn::Expr(expr, _) = col {
            if walk_expr_impl(expr, &mut |e| callback.walk_expr(e)) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }

    // FROM clause
    if let Some(ref mut from) = select.from {
        for table_ref in &mut from.tables {
            if walk_table_ref(table_ref, callback) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }

    // WHERE clause
    if let Some(ref mut where_expr) = select.where_clause {
        if walk_expr_impl(where_expr, &mut |e| callback.walk_expr(e)) == WalkResult::Abort {
            return WalkResult::Abort;
        }
    }

    // GROUP BY
    if let Some(ref mut group_by) = select.group_by {
        for expr in group_by {
            if walk_expr_impl(expr, &mut |e| callback.walk_expr(e)) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }

    // HAVING
    if let Some(ref mut having) = select.having {
        if walk_expr_impl(having, &mut |e| callback.walk_expr(e)) == WalkResult::Abort {
            return WalkResult::Abort;
        }
    }

    // ORDER BY
    if let Some(ref mut order_by) = select.order_by {
        for term in order_by {
            if walk_expr_impl(&mut term.expr, &mut |e| callback.walk_expr(e)) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }

    // LIMIT/OFFSET
    if let Some(ref mut limit) = select.limit {
        if walk_expr_impl(&mut limit.count, &mut |e| callback.walk_expr(e)) == WalkResult::Abort {
            return WalkResult::Abort;
        }
        if let Some(ref mut offset) = limit.offset {
            if walk_expr_impl(offset, &mut |e| callback.walk_expr(e)) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }

    // Compound select
    if let Some(ref mut compound) = select.compound {
        if walk_select(&mut compound.select, callback) == WalkResult::Abort {
            return WalkResult::Abort;
        }
    }

    WalkResult::Continue
}

fn walk_table_ref<F>(table_ref: &mut TableRef, callback: &mut F) -> WalkResult
where
    F: ExprWalker,
{
    match table_ref {
        TableRef::Table { .. } => {}
        TableRef::Subquery { query, .. } => {
            if callback.walk_select(query) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if walk_select(query, callback) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
        TableRef::Join { left, right, constraint, .. } => {
            if walk_table_ref(left, callback) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if walk_table_ref(right, callback) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if let Some(JoinConstraint::On(expr)) = constraint {
                if walk_expr_impl(expr, &mut |e| callback.walk_expr(e)) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
        }
        TableRef::TableFunction { args, .. } => {
            for arg in args {
                if walk_expr_impl(arg, &mut |e| callback.walk_expr(e)) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
        }
    }
    WalkResult::Continue
}
```

## Common Walker Implementations

### Count Aggregates
```rust
/// Count aggregate functions in expression
pub fn count_aggregates(expr: &Expr) -> i32 {
    let mut count = 0;
    walk_expr(&mut expr.clone(), |e| {
        if let Expr::Function { name, .. } = e {
            if is_aggregate_function(name) {
                count += 1;
            }
        }
        WalkResult::Continue
    });
    count
}
```

### Find Column References
```rust
/// Find all column references in expression
pub fn find_columns(expr: &Expr) -> Vec<ColumnRef> {
    let mut columns = Vec::new();
    walk_expr(&mut expr.clone(), |e| {
        if let Expr::Column(col) = e {
            columns.push(col.clone());
        }
        WalkResult::Continue
    });
    columns
}
```

### Expression Depth
```rust
/// Calculate maximum depth of expression tree
pub fn expr_depth(expr: &Expr) -> i32 {
    let mut max_depth = 0;
    let mut current_depth = 0;

    walk_expr(&mut expr.clone(), |_| {
        current_depth += 1;
        max_depth = max_depth.max(current_depth);
        WalkResult::Continue
    });

    max_depth
}
```

## Acceptance Criteria
- [ ] WalkResult enum (Continue, Prune, Abort)
- [ ] walk_expr() for expression trees
- [ ] walk_select() for select statements
- [ ] Handle all expression types
- [ ] Handle subqueries
- [ ] Support pre-order traversal
- [ ] Support early termination (Abort)
- [ ] Support pruning (Prune)
- [ ] Utility functions (count_aggregates, find_columns, etc.)
