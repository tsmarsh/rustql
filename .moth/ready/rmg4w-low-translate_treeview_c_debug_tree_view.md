# Translate treeview.c - Debug Tree View

## Overview
Translate the debug utilities for displaying AST structures as text trees. Used for debugging the parser and query planner.

## Source Reference
- `sqlite3/src/treeview.c` - 1,322 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Functions

### Tree View Structure

```rust
/// Tree view context for generating output
pub struct TreeView {
    /// Output buffer
    output: String,

    /// Current indentation level
    indent: usize,

    /// Flags for each level (true = more siblings)
    more: Vec<bool>,
}

impl TreeView {
    pub fn new() -> Self {
        TreeView {
            output: String::new(),
            indent: 0,
            more: Vec::new(),
        }
    }

    /// Get the output string
    pub fn finish(self) -> String {
        self.output
    }
}
```

### Indentation Helpers

```rust
impl TreeView {
    /// Push a new indentation level
    fn push(&mut self, has_more: bool) {
        self.more.push(has_more);
        self.indent += 1;
    }

    /// Pop indentation level
    fn pop(&mut self) {
        self.more.pop();
        self.indent -= 1;
    }

    /// Write the current line prefix
    fn write_prefix(&mut self) {
        for (i, &more) in self.more.iter().enumerate() {
            if i == self.more.len() - 1 {
                // Last level - show branch
                if more {
                    self.output.push_str("├── ");
                } else {
                    self.output.push_str("└── ");
                }
            } else {
                // Higher level - show continuation or space
                if more {
                    self.output.push_str("│   ");
                } else {
                    self.output.push_str("    ");
                }
            }
        }
    }

    /// Write a line with current prefix
    fn line(&mut self, text: &str) {
        self.write_prefix();
        self.output.push_str(text);
        self.output.push('\n');
    }
}
```

### Expression Display

```rust
impl TreeView {
    /// Display an expression tree
    pub fn show_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Null => self.line("NULL"),
            Expr::Integer(i) => self.line(&format!("INTEGER {}", i)),
            Expr::Float(f) => self.line(&format!("FLOAT {}", f)),
            Expr::String(s) => self.line(&format!("STRING '{}'", s)),
            Expr::Blob(b) => self.line(&format!("BLOB x'{}'", hex::encode(b))),
            Expr::Bool(b) => self.line(&format!("BOOL {}", b)),

            Expr::Column(col) => {
                let name = match (&col.database, &col.table) {
                    (Some(db), Some(tbl)) => format!("{}.{}.{}", db, tbl, col.column),
                    (None, Some(tbl)) => format!("{}.{}", tbl, col.column),
                    _ => col.column.clone(),
                };
                self.line(&format!("COLUMN {}", name));
            }

            Expr::Variable(var) => {
                let name = match var {
                    Variable::Anonymous => "?".to_string(),
                    Variable::Numbered(n) => format!("?{}", n),
                    Variable::Named(s) => s.clone(),
                };
                self.line(&format!("VARIABLE {}", name));
            }

            Expr::Unary { op, expr } => {
                self.line(&format!("{:?}", op));
                self.push(false);
                self.show_expr(expr);
                self.pop();
            }

            Expr::Binary { op, left, right } => {
                self.line(&format!("{:?}", op));
                self.push(true);
                self.show_expr(left);
                self.pop();
                self.push(false);
                self.show_expr(right);
                self.pop();
            }

            Expr::Function { name, args, .. } => {
                self.line(&format!("FUNCTION {}()", name));
                match args {
                    FunctionArgs::Star => {
                        self.push(false);
                        self.line("*");
                        self.pop();
                    }
                    FunctionArgs::Args(exprs) => {
                        for (i, e) in exprs.iter().enumerate() {
                            self.push(i < exprs.len() - 1);
                            self.show_expr(e);
                            self.pop();
                        }
                    }
                    FunctionArgs::Distinct(e) => {
                        self.push(false);
                        self.line("DISTINCT");
                        self.push(false);
                        self.show_expr(e);
                        self.pop();
                        self.pop();
                    }
                }
            }

            Expr::Case { operand, when_clauses, else_clause } => {
                self.line("CASE");
                if let Some(op) = operand {
                    self.push(true);
                    self.line("operand:");
                    self.push(false);
                    self.show_expr(op);
                    self.pop();
                    self.pop();
                }
                for (i, (when_expr, then_expr)) in when_clauses.iter().enumerate() {
                    let has_more = i < when_clauses.len() - 1 || else_clause.is_some();
                    self.push(has_more);
                    self.line("WHEN");
                    self.push(true);
                    self.show_expr(when_expr);
                    self.pop();
                    self.push(false);
                    self.line("THEN");
                    self.push(false);
                    self.show_expr(then_expr);
                    self.pop();
                    self.pop();
                    self.pop();
                }
                if let Some(else_expr) = else_clause {
                    self.push(false);
                    self.line("ELSE");
                    self.push(false);
                    self.show_expr(else_expr);
                    self.pop();
                    self.pop();
                }
            }

            // ... other expression types
            _ => self.line(&format!("{:?}", expr)),
        }
    }
}
```

### SELECT Display

```rust
impl TreeView {
    /// Display a SELECT statement tree
    pub fn show_select(&mut self, select: &SelectStmt) {
        self.line("SELECT");

        // Distinct
        self.push(true);
        match select.distinct {
            Distinct::All => self.line("ALL"),
            Distinct::Distinct => self.line("DISTINCT"),
            Distinct::None => {}
        }
        self.pop();

        // Columns
        self.push(true);
        self.line("columns:");
        for (i, col) in select.columns.iter().enumerate() {
            self.push(i < select.columns.len() - 1);
            self.show_result_column(col);
            self.pop();
        }
        self.pop();

        // FROM
        if let Some(ref from) = select.from {
            self.push(true);
            self.line("FROM:");
            self.show_from(from);
            self.pop();
        }

        // WHERE
        if let Some(ref where_expr) = select.where_clause {
            self.push(true);
            self.line("WHERE:");
            self.push(false);
            self.show_expr(where_expr);
            self.pop();
            self.pop();
        }

        // GROUP BY
        if let Some(ref group_by) = select.group_by {
            self.push(true);
            self.line("GROUP BY:");
            for (i, expr) in group_by.iter().enumerate() {
                self.push(i < group_by.len() - 1);
                self.show_expr(expr);
                self.pop();
            }
            self.pop();
        }

        // HAVING
        if let Some(ref having) = select.having {
            self.push(true);
            self.line("HAVING:");
            self.push(false);
            self.show_expr(having);
            self.pop();
            self.pop();
        }

        // ORDER BY
        if let Some(ref order_by) = select.order_by {
            self.push(select.limit.is_some());
            self.line("ORDER BY:");
            for (i, term) in order_by.iter().enumerate() {
                self.push(i < order_by.len() - 1);
                self.show_expr(&term.expr);
                self.pop();
            }
            self.pop();
        }

        // LIMIT
        if let Some(ref limit) = select.limit {
            self.push(false);
            self.line("LIMIT:");
            self.push(limit.offset.is_some());
            self.show_expr(&limit.count);
            self.pop();
            if let Some(ref offset) = limit.offset {
                self.push(false);
                self.line("OFFSET:");
                self.push(false);
                self.show_expr(offset);
                self.pop();
                self.pop();
            }
            self.pop();
        }
    }

    fn show_result_column(&mut self, col: &ResultColumn) {
        match col {
            ResultColumn::Star => self.line("*"),
            ResultColumn::TableStar(t) => self.line(&format!("{}.*", t)),
            ResultColumn::Expr(expr, alias) => {
                if let Some(a) = alias {
                    self.line(&format!("AS {}", a));
                    self.push(false);
                }
                self.show_expr(expr);
                if alias.is_some() {
                    self.pop();
                }
            }
        }
    }

    fn show_from(&mut self, from: &FromClause) {
        for (i, table_ref) in from.tables.iter().enumerate() {
            self.push(i < from.tables.len() - 1);
            self.show_table_ref(table_ref);
            self.pop();
        }
    }

    fn show_table_ref(&mut self, table_ref: &TableRef) {
        match table_ref {
            TableRef::Table { name, alias, .. } => {
                let text = if let Some(a) = alias {
                    format!("TABLE {} AS {}", name, a)
                } else {
                    format!("TABLE {}", name)
                };
                self.line(&text);
            }
            TableRef::Subquery { alias, query } => {
                let text = if let Some(a) = alias {
                    format!("SUBQUERY AS {}", a)
                } else {
                    "SUBQUERY".to_string()
                };
                self.line(&text);
                self.push(false);
                self.show_select(query);
                self.pop();
            }
            TableRef::Join { left, join_type, right, constraint } => {
                self.line(&format!("{:?} JOIN", join_type));
                self.push(true);
                self.show_table_ref(left);
                self.pop();
                self.push(constraint.is_some());
                self.show_table_ref(right);
                self.pop();
                if let Some(JoinConstraint::On(expr)) = constraint {
                    self.push(false);
                    self.line("ON");
                    self.push(false);
                    self.show_expr(expr);
                    self.pop();
                    self.pop();
                }
            }
            _ => self.line(&format!("{:?}", table_ref)),
        }
    }
}
```

### Public API

```rust
/// Generate tree view of expression
pub fn expr_tree(expr: &Expr) -> String {
    let mut tv = TreeView::new();
    tv.show_expr(expr);
    tv.finish()
}

/// Generate tree view of SELECT statement
pub fn select_tree(select: &SelectStmt) -> String {
    let mut tv = TreeView::new();
    tv.show_select(select);
    tv.finish()
}

/// Generate tree view of any statement
pub fn stmt_tree(stmt: &Stmt) -> String {
    let mut tv = TreeView::new();
    tv.show_stmt(stmt);
    tv.finish()
}
```

### Example Output

```
SELECT
├── columns:
│   ├── COLUMN users.name
│   └── COLUMN users.email
├── FROM:
│   └── TABLE users
├── WHERE:
│   └── EQ
│       ├── COLUMN users.active
│       └── INTEGER 1
└── ORDER BY:
    └── COLUMN users.name
```

## Acceptance Criteria
- [ ] TreeView struct with indentation tracking
- [ ] Expression tree display
- [ ] SELECT statement display
- [ ] All statement types supported
- [ ] Proper tree structure characters (├, └, │)
- [ ] Handles nested subqueries
- [ ] Public API functions
- [ ] Clean output formatting
