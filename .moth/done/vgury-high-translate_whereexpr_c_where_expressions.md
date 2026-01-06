# Translate whereexpr.c - WHERE Expression Analysis

## Overview
Translate WHERE clause expression analysis which breaks down WHERE into usable terms.

## Source Reference
- `sqlite3/src/whereexpr.c` - 2,094 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Functions

### Term Analysis
- `exprAnalyze()` - Analyze a single expression
- `exprAnalyzeAll()` - Analyze entire WHERE clause
- `termCanDriveIndex()` - Check if term can use index
- `whereOrSplit()` - Split OR into separate terms

### Term Classification
```rust
/// Classify a WHERE term for optimization
pub fn classify_term(expr: &Expr, tables: &[SrcItem]) -> WhereTerm {
    match expr {
        // column = constant
        Expr::Binary { op: BinaryOp::Eq, left, right } => {
            if let (Some(col), true) = (extract_column(left), is_constant(right)) {
                WhereTerm {
                    expr: Box::new(expr.clone()),
                    prereq: table_mask(col, tables),
                    flags: WhereTermFlags::INDEXED,
                    left_col: Some(col.col_idx),
                    equiv: None,
                    idx: 0,
                }
            } else {
                WhereTerm::unindexed(expr)
            }
        }
        // column IN (...)
        Expr::In { expr: col_expr, list, .. } => {
            if let Some(col) = extract_column(col_expr) {
                WhereTerm {
                    flags: WhereTermFlags::INDEXED | WhereTermFlags::IN,
                    left_col: Some(col.col_idx),
                    ..WhereTerm::new(expr, tables)
                }
            } else {
                WhereTerm::unindexed(expr)
            }
        }
        // column LIKE 'prefix%'
        Expr::Like { expr: col_expr, pattern, .. } => {
            if let Some(col) = extract_column(col_expr) {
                if has_prefix_pattern(pattern) {
                    WhereTerm {
                        flags: WhereTermFlags::LIKE_PREFIX,
                        left_col: Some(col.col_idx),
                        ..WhereTerm::new(expr, tables)
                    }
                } else {
                    WhereTerm::unindexed(expr)
                }
            } else {
                WhereTerm::unindexed(expr)
            }
        }
        _ => WhereTerm::unindexed(expr),
    }
}
```

### OR Optimization
```rust
/// Split OR clause into indexable parts
pub fn split_or_clause(expr: &Expr) -> Vec<WhereTerm> {
    let mut terms = Vec::new();

    if let Expr::Binary { op: BinaryOp::Or, left, right } = expr {
        terms.extend(split_or_clause(left));
        terms.extend(split_or_clause(right));
    } else {
        terms.push(classify_term(expr));
    }

    terms
}
```

## Acceptance Criteria
- [ ] Equality term detection (column = value)
- [ ] Range term detection (column > value)
- [ ] IN clause analysis
- [ ] LIKE prefix analysis
- [ ] BETWEEN analysis
- [ ] OR clause splitting
- [ ] AND clause processing
- [ ] Table mask computation
- [ ] Index usability flags
