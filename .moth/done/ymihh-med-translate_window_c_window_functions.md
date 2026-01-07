# Translate window.c - Window Functions

## Overview
Translate window function implementation including OVER clauses, frame specifications, and window aggregates.

## Source Reference
- `sqlite3/src/window.c` - 3,114 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### WindowSpec
```rust
pub struct WindowSpec {
    /// Window name (for WINDOW clause)
    pub name: Option<String>,
    /// Base window (for "OVER window_name")
    pub base: Option<String>,
    /// PARTITION BY expressions
    pub partition_by: Option<Vec<Expr>>,
    /// ORDER BY terms
    pub order_by: Option<Vec<OrderingTerm>>,
    /// Frame specification
    pub frame: Option<WindowFrame>,
}

pub struct WindowFrame {
    /// ROWS, RANGE, or GROUPS
    pub mode: FrameMode,
    /// Start bound
    pub start: FrameBound,
    /// End bound (defaults to CURRENT ROW)
    pub end: FrameBound,
    /// EXCLUDE clause
    pub exclude: FrameExclude,
}

#[derive(Debug, Clone, Copy)]
pub enum FrameMode {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(Box<Expr>),
    CurrentRow,
    Following(Box<Expr>),
    UnboundedFollowing,
}

#[derive(Debug, Clone, Copy)]
pub enum FrameExclude {
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}
```

## Key Functions

### Window Function Analysis
```rust
impl<'a> Parse<'a> {
    /// Analyze window functions in SELECT
    pub fn analyze_windows(&mut self, select: &mut SelectStmt) -> Result<Vec<WindowInfo>> {
        let mut windows = Vec::new();

        // Find all window function calls
        for col in &select.columns {
            if let ResultColumn::Expr(expr, _) = col {
                self.collect_window_funcs(expr, &mut windows)?;
            }
        }

        // Group by window specification
        let grouped = self.group_by_window(windows)?;

        Ok(grouped)
    }

    fn collect_window_funcs(&self, expr: &Expr, windows: &mut Vec<WindowFunc>) -> Result<()> {
        walk_expr(expr, |e| {
            if let Expr::Function { name, args, over: Some(over), .. } = e {
                windows.push(WindowFunc {
                    name: name.clone(),
                    args: args.clone(),
                    over: over.clone(),
                });
            }
            WalkResult::Continue
        });
        Ok(())
    }
}
```

### Window Function Code Generation
```rust
impl<'a> Parse<'a> {
    /// Generate code for window functions
    pub fn compile_window_functions(
        &mut self,
        select: &SelectStmt,
        windows: &[WindowInfo],
    ) -> Result<()> {
        for window in windows {
            // 1. Sort rows by PARTITION BY + ORDER BY
            let sort_cursor = self.setup_window_sort(window)?;

            // 2. Process each partition
            let part_start = self.make_label();
            let part_end = self.make_label();

            self.add_op(Opcode::Rewind, sort_cursor, part_end, 0);
            self.resolve_label(part_start);

            // 3. Initialize window frame
            self.init_window_frame(window)?;

            // 4. Process each row in partition
            let row_start = self.make_label();
            self.resolve_label(row_start);

            // 5. Update frame bounds
            self.update_window_frame(window)?;

            // 6. Compute window function result
            for func in &window.functions {
                self.compute_window_func(func)?;
            }

            // 7. Output row
            self.output_window_row(window)?;

            // 8. Next row
            self.add_op(Opcode::Next, sort_cursor, row_start, 0);

            self.resolve_label(part_end);
        }

        Ok(())
    }

    fn compute_window_func(&mut self, func: &WindowFunc) -> Result<()> {
        match func.name.to_uppercase().as_str() {
            "ROW_NUMBER" => {
                // Just increment counter
            }
            "RANK" => {
                // Reset on order change
            }
            "DENSE_RANK" => {
                // Increment only on order change
            }
            "NTILE" => {
                // Distribute rows into N buckets
            }
            "LAG" => {
                // Value from N rows back
            }
            "LEAD" => {
                // Value from N rows forward
            }
            "FIRST_VALUE" => {
                // First value in frame
            }
            "LAST_VALUE" => {
                // Last value in frame
            }
            "NTH_VALUE" => {
                // Nth value in frame
            }
            _ => {
                // Regular aggregate over frame
                self.compute_frame_aggregate(func)?;
            }
        }
        Ok(())
    }
}
```

## Window Functions

### Ranking Functions
- `ROW_NUMBER()` - Sequential row number
- `RANK()` - Rank with gaps
- `DENSE_RANK()` - Rank without gaps
- `NTILE(n)` - Divide into n buckets
- `PERCENT_RANK()` - Relative rank (0-1)
- `CUME_DIST()` - Cumulative distribution

### Value Functions
- `LAG(expr, offset, default)` - Previous row value
- `LEAD(expr, offset, default)` - Next row value
- `FIRST_VALUE(expr)` - First in frame
- `LAST_VALUE(expr)` - Last in frame
- `NTH_VALUE(expr, n)` - Nth in frame

### Aggregate Functions
Any aggregate can be used as window function:
- `SUM(expr) OVER (...)`
- `AVG(expr) OVER (...)`
- `COUNT(expr) OVER (...)`
- `MIN(expr) OVER (...)`
- `MAX(expr) OVER (...)`

## Acceptance Criteria
- [ ] OVER clause parsing
- [ ] PARTITION BY handling
- [ ] ORDER BY within window
- [ ] Frame specification (ROWS/RANGE/GROUPS)
- [ ] Frame bounds (PRECEDING/FOLLOWING/CURRENT ROW)
- [ ] EXCLUDE clause
- [ ] ROW_NUMBER, RANK, DENSE_RANK
- [ ] LAG, LEAD
- [ ] FIRST_VALUE, LAST_VALUE, NTH_VALUE
- [ ] Aggregate functions over windows
- [ ] Named windows (WINDOW clause)

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `window1.test` - Basic window function syntax
- `window2.test` - PARTITION BY and ORDER BY
- `window3.test` - Frame specifications (ROWS/RANGE/GROUPS)
- `window4.test` - Frame bounds (PRECEDING/FOLLOWING)
- `window5.test` - EXCLUDE clause
- `window6.test` - Ranking functions (ROW_NUMBER, RANK, DENSE_RANK)
- `window7.test` - Navigation functions (LAG, LEAD)
- `window8.test` - Value functions (FIRST_VALUE, LAST_VALUE, NTH_VALUE)
- `window9.test` - Aggregate functions as window functions
- `windowA.test` - Named windows (WINDOW clause)
- `windowB.test` - Window function edge cases
- `windowfault.test` - Window function error handling
