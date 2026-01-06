# Translate where.c - WHERE Optimization

## Overview
Translate the query planner/optimizer which analyzes WHERE clauses and chooses optimal join orders and index usage.

## Source Reference
- `sqlite3/src/where.c` - 7,735 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### WhereInfo
```rust
pub struct WhereInfo {
    /// Parse context
    parse: *mut Parse,
    /// FROM clause tables
    tab_list: Vec<SrcItem>,
    /// WHERE clause terms
    terms: Vec<WhereTerm>,
    /// Number of loops
    n_level: i32,
    /// Loop details
    levels: Vec<WhereLevel>,
    /// Estimated rows
    n_row_out: f64,
    /// Saved VDBE address for later fixup
    top: i32,
    cont: i32,
    end: i32,
}
```

### WhereTerm
```rust
pub struct WhereTerm {
    /// The expression
    expr: Box<Expr>,
    /// Table mask (which tables referenced)
    prereq: u64,
    /// Index of WHERE this came from
    idx: i32,
    /// Term type flags
    flags: WhereTermFlags,
    /// Left column for equality
    left_col: Option<i32>,
    /// Equivalent term (for OR optimization)
    equiv: Option<Box<WhereTerm>>,
}
```

### WhereLevel
```rust
pub struct WhereLevel {
    /// Which table
    from_idx: i32,
    /// Flags
    flags: WhereLevelFlags,
    /// Plan for this level
    plan: WherePlan,
    /// Loop top address
    addr_first: i32,
    addr_cont: i32,
}

pub enum WherePlan {
    FullScan,
    IndexScan { index: Arc<Index>, eq_cols: i32 },
    PrimaryKey { eq_cols: i32 },
    RowidEq,
    RowidRange { start: Option<Box<Expr>>, end: Option<Box<Expr>> },
}
```

## Key Functions

### Query Planning
- `sqlite3WhereBegin()` - Initialize query planning
- `sqlite3WhereEnd()` - Finalize WHERE clause code
- `whereLoopAddAll()` - Find all possible access paths
- `wherePathSolver()` - Choose optimal path

### Cost Estimation
- `whereLoopCost()` - Estimate cost of a loop
- `whereRangeScanEst()` - Range scan row estimate
- `whereStat4Lookup()` - Use sqlite_stat4 data

## Cost Model

```rust
/// Estimate cost of a query plan
pub fn estimate_cost(plan: &WherePlan, table: &Table, term_count: i32) -> f64 {
    match plan {
        WherePlan::FullScan => {
            // Full scan: read all rows
            table.estimated_rows as f64
        }
        WherePlan::IndexScan { index, eq_cols } => {
            // Index scan: use selectivity
            let selectivity = 0.1f64.powi(*eq_cols);
            table.estimated_rows as f64 * selectivity
        }
        WherePlan::PrimaryKey { .. } => {
            // Primary key lookup: ~1 row
            1.0
        }
        WherePlan::RowidEq => 1.0,
        WherePlan::RowidRange { .. } => {
            table.estimated_rows as f64 * 0.33
        }
    }
}
```

## Acceptance Criteria
- [ ] WhereInfo, WhereTerm, WhereLevel structures
- [ ] Term analysis and classification
- [ ] Index usability detection
- [ ] Join order optimization
- [ ] Cost-based plan selection
- [ ] OR clause optimization
- [ ] IN clause optimization
- [ ] Range constraint handling
- [ ] BETWEEN optimization
- [ ] LIKE prefix optimization
