# Translate resolve.c - Name Resolution

## Overview
Translate the name resolution code which resolves table, column, and function names during SQL compilation.

## Source Reference
- `sqlite3/src/resolve.c` - 2,317 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Functions

### Expression Resolution

```rust
impl<'a> Parse<'a> {
    /// Resolve names in an expression tree
    pub fn resolve_expr_names(
        &mut self,
        nc: &mut NameContext,
        expr: &mut Expr,
    ) -> Result<()> {
        self.walk_expr(expr, |e| self.resolve_expr_name(nc, e))
    }

    /// Resolve a single expression node
    fn resolve_expr_name(
        &mut self,
        nc: &mut NameContext,
        expr: &mut Expr,
    ) -> Result<WalkResult> {
        match expr {
            Expr::Column(ref mut col_ref) => {
                self.resolve_column_ref(nc, col_ref)?;
            }
            Expr::Variable(ref mut var) => {
                self.resolve_variable(var)?;
            }
            Expr::Function { ref name, ref mut args, .. } => {
                self.resolve_function(nc, name, args)?;
            }
            _ => {}
        }
        Ok(WalkResult::Continue)
    }
}
```

### Column Resolution

```rust
impl<'a> Parse<'a> {
    /// Resolve a column reference (table.column or just column)
    fn resolve_column_ref(
        &mut self,
        nc: &NameContext,
        col_ref: &mut ColumnRef,
    ) -> Result<()> {
        let src_list = nc.src_list.as_ref()
            .ok_or_else(|| Error::with_message(
                ErrorCode::Error,
                "no tables in FROM clause"
            ))?;

        // If table specified, find it
        if let Some(ref table_name) = col_ref.table {
            for (i, src) in src_list.iter().enumerate() {
                if src.matches_name(table_name) {
                    let col_idx = src.find_column(&col_ref.column)?;
                    col_ref.resolved = Some(ResolvedColumn {
                        src_idx: i as i32,
                        col_idx,
                        table: src.table.clone(),
                    });
                    return Ok(());
                }
            }
            return Err(Error::with_message(
                ErrorCode::Error,
                format!("no such table: {}", table_name)
            ));
        }

        // No table specified - search all sources
        let mut found = None;
        for (i, src) in src_list.iter().enumerate() {
            if let Ok(col_idx) = src.find_column(&col_ref.column) {
                if found.is_some() {
                    return Err(Error::with_message(
                        ErrorCode::Error,
                        format!("ambiguous column name: {}", col_ref.column)
                    ));
                }
                found = Some((i, col_idx, src.table.clone()));
            }
        }

        if let Some((src_idx, col_idx, table)) = found {
            col_ref.resolved = Some(ResolvedColumn {
                src_idx: src_idx as i32,
                col_idx,
                table,
            });
            Ok(())
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                format!("no such column: {}", col_ref.column)
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedColumn {
    /// Index into FROM clause sources
    pub src_idx: i32,

    /// Column index in table
    pub col_idx: i32,

    /// Table reference
    pub table: Option<Arc<Table>>,
}
```

### Variable Resolution

```rust
impl<'a> Parse<'a> {
    /// Resolve a parameter variable
    fn resolve_variable(&mut self, var: &mut Variable) -> Result<()> {
        match var {
            Variable::Anonymous => {
                // ?
                self.n_var += 1;
                *var = Variable::Numbered(self.n_var);
            }
            Variable::Numbered(n) => {
                // ?NNN
                if *n > self.n_var {
                    self.n_var = *n;
                }
            }
            Variable::Named(name) => {
                // :name, @name, $name
                // Check if we've seen this name
                for (i, existing) in self.var_names.iter().enumerate() {
                    if let Some(n) = existing {
                        if n == name {
                            // Reuse existing index
                            return Ok(());
                        }
                    }
                }
                // New named variable
                self.n_var += 1;
                self.var_names.push(Some(name.clone()));
            }
        }
        Ok(())
    }
}
```

### Function Resolution

```rust
impl<'a> Parse<'a> {
    /// Resolve a function call
    fn resolve_function(
        &mut self,
        nc: &mut NameContext,
        name: &str,
        args: &mut FunctionArgs,
    ) -> Result<()> {
        // Look up function
        let func = self.db.find_function(name, args.len())?;

        // Check for aggregate
        if func.is_aggregate() {
            if !nc.nc_flags.contains(NcFlags::ALLOW_AGG) {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("misuse of aggregate function {}()", name)
                ));
            }
            nc.nc_flags.insert(NcFlags::HAS_AGG);
        }

        // Check for window function
        if func.is_window() {
            if !nc.nc_flags.contains(NcFlags::ALLOW_WIN) {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("misuse of window function {}()", name)
                ));
            }
            nc.nc_flags.insert(NcFlags::HAS_WIN);
        }

        Ok(())
    }
}
```

### SELECT Resolution

```rust
impl<'a> Parse<'a> {
    /// Resolve names in a SELECT statement
    pub fn resolve_select_names(&mut self, select: &mut SelectStmt) -> Result<()> {
        // Build source list from FROM clause
        let src_list = if let Some(ref from) = select.from {
            self.build_src_list(from)?
        } else {
            Vec::new()
        };

        // Create name context
        let mut nc = NameContext {
            parse: self as *mut _ as *mut Parse<'static>,
            src_list: Some(src_list),
            expr: None,
            src_mask: 0,
            n_err: 0,
            nc_flags: NcFlags::ALLOW_AGG | NcFlags::ALLOW_WIN,
        };

        // Resolve result columns
        for col in &mut select.columns {
            self.resolve_result_column(&mut nc, col)?;
        }

        // Resolve WHERE
        if let Some(ref mut where_expr) = select.where_clause {
            // No aggregates allowed in WHERE
            nc.nc_flags.remove(NcFlags::ALLOW_AGG);
            self.resolve_expr_names(&mut nc, where_expr)?;
            nc.nc_flags.insert(NcFlags::ALLOW_AGG);
        }

        // Resolve GROUP BY
        if let Some(ref mut group_by) = select.group_by {
            for expr in group_by {
                self.resolve_expr_names(&mut nc, expr)?;
            }
        }

        // Resolve HAVING
        if let Some(ref mut having) = select.having {
            nc.nc_flags.insert(NcFlags::IN_HAVING);
            self.resolve_expr_names(&mut nc, having)?;
            nc.nc_flags.remove(NcFlags::IN_HAVING);
        }

        // Resolve ORDER BY
        if let Some(ref mut order_by) = select.order_by {
            nc.nc_flags.insert(NcFlags::IN_ORDER_BY);
            for term in order_by {
                self.resolve_expr_names(&mut nc, &mut term.expr)?;
            }
            nc.nc_flags.remove(NcFlags::IN_ORDER_BY);
        }

        Ok(())
    }

    /// Build source list from FROM clause
    fn build_src_list(&mut self, from: &FromClause) -> Result<Vec<SrcItem>> {
        let mut items = Vec::new();

        for table_ref in &from.tables {
            self.add_src_item(&mut items, table_ref)?;
        }

        Ok(items)
    }
}

#[derive(Debug)]
pub struct SrcItem {
    /// Table name or alias
    pub name: String,

    /// Actual table (if not subquery)
    pub table: Option<Arc<Table>>,

    /// Subquery (if not table)
    pub subquery: Option<Box<SelectStmt>>,

    /// Cursor number
    pub cursor: i32,

    /// Columns available
    pub columns: Vec<String>,
}
```

### Star Expansion

```rust
impl<'a> Parse<'a> {
    /// Expand * in result columns
    fn resolve_result_column(
        &mut self,
        nc: &mut NameContext,
        col: &mut ResultColumn,
    ) -> Result<()> {
        match col {
            ResultColumn::Star => {
                // Expand to all columns from all tables
                let src_list = nc.src_list.as_ref().unwrap();
                let mut expanded = Vec::new();

                for src in src_list {
                    for col_name in &src.columns {
                        expanded.push(ResultColumn::Expr(
                            Expr::Column(ColumnRef {
                                database: None,
                                table: Some(src.name.clone()),
                                column: col_name.clone(),
                                resolved: None,
                            }),
                            None,
                        ));
                    }
                }

                // Replace star with expanded columns
                // (handled by caller)
            }
            ResultColumn::TableStar(table) => {
                // Expand table.* to all columns from that table
                let src_list = nc.src_list.as_ref().unwrap();
                let src = src_list.iter()
                    .find(|s| s.matches_name(table))
                    .ok_or_else(|| Error::with_message(
                        ErrorCode::Error,
                        format!("no such table: {}", table)
                    ))?;

                // Expand columns...
            }
            ResultColumn::Expr(expr, _alias) => {
                self.resolve_expr_names(nc, expr)?;
            }
        }
        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] NameContext struct with flags
- [ ] Column reference resolution
- [ ] Ambiguous column detection
- [ ] Variable resolution (?NNN, :name, etc.)
- [ ] Function resolution and validation
- [ ] Aggregate function context checking
- [ ] Window function context checking
- [ ] SELECT name resolution
- [ ] Star expansion (* and table.*)
- [ ] Subquery name scoping
- [ ] Error messages with locations
