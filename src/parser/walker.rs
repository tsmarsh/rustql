//! AST walking utilities for expressions and SELECT statements.

use crate::functions::is_aggregate_function;
use crate::parser::ast::{
    Expr, FunctionArgs, FunctionCall, InList, JoinConstraint, OrderingTerm, Over, ResultColumn,
    SelectBody, SelectCore, SelectStmt, TableRef, WhenClause, WindowDef, WindowFrame,
    WindowFrameBound, WindowSpec,
};

/// Result of a walker callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalkResult {
    /// Continue walking.
    Continue,
    /// Prune this branch.
    Prune,
    /// Abort the walk.
    Abort,
}

/// Trait for walking expressions and SELECT statements.
pub trait ExprWalker {
    /// Called for each expression node.
    fn walk_expr(&mut self, expr: &mut Expr) -> WalkResult;

    /// Called for each SELECT statement before walking children.
    fn walk_select(&mut self, _select: &mut SelectStmt) -> WalkResult {
        WalkResult::Continue
    }
}

/// Walk an expression tree in pre-order.
pub fn walk_expr<W: ExprWalker>(walker: &mut W, expr: &mut Expr) -> WalkResult {
    walk_expr_impl(walker, expr)
}

fn walk_expr_impl<W: ExprWalker>(walker: &mut W, expr: &mut Expr) -> WalkResult {
    match walker.walk_expr(expr) {
        WalkResult::Abort => return WalkResult::Abort,
        WalkResult::Prune => return WalkResult::Continue,
        WalkResult::Continue => {}
    }

    match expr {
        Expr::Literal(_) | Expr::Column(_) | Expr::Variable(_) | Expr::Raise { .. } => {}
        Expr::Unary { expr: child, .. }
        | Expr::Cast { expr: child, .. }
        | Expr::Collate { expr: child, .. }
        | Expr::IsNull { expr: child, .. }
        | Expr::Parens(child) => {
            if walk_expr_impl(walker, child) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
        Expr::Binary { left, right, .. } | Expr::IsDistinct { left, right, .. } => {
            if walk_expr_impl(walker, left) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if walk_expr_impl(walker, right) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            if walk_expr_impl(walker, expr) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if walk_expr_impl(walker, low) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if walk_expr_impl(walker, high) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
        Expr::In { expr, list, .. } => {
            if walk_expr_impl(walker, expr) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if walk_in_list(walker, list) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            if walk_expr_impl(walker, expr) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if walk_expr_impl(walker, pattern) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if let Some(escape) = escape {
                if walk_expr_impl(walker, escape) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                if walk_expr_impl(walker, operand) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
            for WhenClause { when, then } in when_clauses {
                if walk_expr_impl(walker, when) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
                if walk_expr_impl(walker, then) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
            if let Some(else_clause) = else_clause {
                if walk_expr_impl(walker, else_clause) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
        }
        Expr::Function(FunctionCall {
            args, filter, over, ..
        }) => {
            if walk_function_args(walker, args) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if let Some(filter) = filter {
                if walk_expr_impl(walker, filter) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
            if let Some(over) = over {
                if walk_over(walker, over) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
        }
        Expr::Subquery(select)
        | Expr::Exists {
            subquery: select, ..
        } => {
            if walk_select_impl(walker, select) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }

    WalkResult::Continue
}

/// Walk a SELECT statement in pre-order.
pub fn walk_select<W: ExprWalker>(walker: &mut W, select: &mut SelectStmt) -> WalkResult {
    walk_select_impl(walker, select)
}

fn walk_select_impl<W: ExprWalker>(walker: &mut W, select: &mut SelectStmt) -> WalkResult {
    match walker.walk_select(select) {
        WalkResult::Abort => return WalkResult::Abort,
        WalkResult::Prune => return WalkResult::Continue,
        WalkResult::Continue => {}
    }

    if let Some(with) = &mut select.with {
        for cte in &mut with.ctes {
            if walk_select_impl(walker, &mut cte.query) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }

    if walk_select_body(walker, &mut select.body) == WalkResult::Abort {
        return WalkResult::Abort;
    }

    if let Some(order_by) = &mut select.order_by {
        for term in order_by {
            if walk_ordering_term(walker, term) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }

    if let Some(limit) = &mut select.limit {
        if walk_expr_impl(walker, &mut limit.limit) == WalkResult::Abort {
            return WalkResult::Abort;
        }
        if let Some(offset) = &mut limit.offset {
            if walk_expr_impl(walker, offset) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }

    WalkResult::Continue
}

fn walk_select_body<W: ExprWalker>(walker: &mut W, body: &mut SelectBody) -> WalkResult {
    match body {
        SelectBody::Select(core) => walk_select_core(walker, core),
        SelectBody::Compound { left, right, .. } => {
            if walk_select_body(walker, left) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if walk_select_body(walker, right) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            WalkResult::Continue
        }
    }
}

fn walk_select_core<W: ExprWalker>(walker: &mut W, core: &mut SelectCore) -> WalkResult {
    for col in &mut core.columns {
        if let ResultColumn::Expr { expr, .. } = col {
            if walk_expr_impl(walker, expr) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }

    if let Some(from) = &mut core.from {
        for table in &mut from.tables {
            if walk_table_ref(walker, table) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }

    if let Some(where_clause) = &mut core.where_clause {
        if walk_expr_impl(walker, where_clause) == WalkResult::Abort {
            return WalkResult::Abort;
        }
    }

    if let Some(group_by) = &mut core.group_by {
        for expr in group_by {
            if walk_expr_impl(walker, expr) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }

    if let Some(having) = &mut core.having {
        if walk_expr_impl(walker, having) == WalkResult::Abort {
            return WalkResult::Abort;
        }
    }

    if let Some(window_defs) = &mut core.window {
        for window in window_defs {
            if walk_window_def(walker, window) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }

    WalkResult::Continue
}

fn walk_table_ref<W: ExprWalker>(walker: &mut W, table: &mut TableRef) -> WalkResult {
    match table {
        TableRef::Table { .. } => {}
        TableRef::Subquery { query, .. } => {
            if walk_select_impl(walker, query) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
        TableRef::Join {
            left,
            right,
            constraint,
            ..
        } => {
            if walk_table_ref(walker, left) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if walk_table_ref(walker, right) == WalkResult::Abort {
                return WalkResult::Abort;
            }
            if let Some(JoinConstraint::On(expr)) = constraint {
                if walk_expr_impl(walker, expr) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
        }
        TableRef::TableFunction { args, .. } => {
            for arg in args {
                if walk_expr_impl(walker, arg) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
        }
        TableRef::Parens(inner) => {
            if walk_table_ref(walker, inner) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }
    WalkResult::Continue
}

fn walk_in_list<W: ExprWalker>(walker: &mut W, list: &mut InList) -> WalkResult {
    match list {
        InList::Values(values) => {
            for value in values {
                if walk_expr_impl(walker, value) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
        }
        InList::Subquery(select) => {
            if walk_select_impl(walker, select) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
        InList::Table(_) => {}
    }
    WalkResult::Continue
}

fn walk_function_args<W: ExprWalker>(walker: &mut W, args: &mut FunctionArgs) -> WalkResult {
    match args {
        FunctionArgs::Star => WalkResult::Continue,
        FunctionArgs::Exprs(exprs) => {
            for expr in exprs {
                if walk_expr_impl(walker, expr) == WalkResult::Abort {
                    return WalkResult::Abort;
                }
            }
            WalkResult::Continue
        }
    }
}

fn walk_over<W: ExprWalker>(walker: &mut W, over: &mut Over) -> WalkResult {
    match over {
        Over::Window(_) => WalkResult::Continue,
        Over::Spec(spec) => walk_window_spec(walker, spec),
    }
}

fn walk_ordering_term<W: ExprWalker>(walker: &mut W, term: &mut OrderingTerm) -> WalkResult {
    walk_expr_impl(walker, &mut term.expr)
}

fn walk_window_def<W: ExprWalker>(walker: &mut W, window: &mut WindowDef) -> WalkResult {
    walk_window_spec(walker, &mut window.spec)
}

fn walk_window_spec<W: ExprWalker>(walker: &mut W, spec: &mut WindowSpec) -> WalkResult {
    if let Some(partition_by) = &mut spec.partition_by {
        for expr in partition_by {
            if walk_expr_impl(walker, expr) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }
    if let Some(order_by) = &mut spec.order_by {
        for term in order_by {
            if walk_ordering_term(walker, term) == WalkResult::Abort {
                return WalkResult::Abort;
            }
        }
    }
    if let Some(frame) = &mut spec.frame {
        if walk_window_frame(walker, frame) == WalkResult::Abort {
            return WalkResult::Abort;
        }
    }
    WalkResult::Continue
}

fn walk_window_frame<W: ExprWalker>(walker: &mut W, frame: &mut WindowFrame) -> WalkResult {
    if walk_window_bound(walker, &mut frame.start) == WalkResult::Abort {
        return WalkResult::Abort;
    }
    if let Some(end) = &mut frame.end {
        if walk_window_bound(walker, end) == WalkResult::Abort {
            return WalkResult::Abort;
        }
    }
    WalkResult::Continue
}

fn walk_window_bound<W: ExprWalker>(walker: &mut W, bound: &mut WindowFrameBound) -> WalkResult {
    match bound {
        WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr) => {
            walk_expr_impl(walker, expr)
        }
        WindowFrameBound::CurrentRow
        | WindowFrameBound::UnboundedPreceding
        | WindowFrameBound::UnboundedFollowing => WalkResult::Continue,
    }
}

/// Count aggregate functions in an expression.
pub fn count_aggregates(expr: &Expr) -> i32 {
    struct Counter {
        count: i32,
    }
    impl ExprWalker for Counter {
        fn walk_expr(&mut self, expr: &mut Expr) -> WalkResult {
            if let Expr::Function(FunctionCall { name, .. }) = expr {
                if is_aggregate_function(name) {
                    self.count += 1;
                }
            }
            WalkResult::Continue
        }
    }

    let mut counter = Counter { count: 0 };
    let mut expr = expr.clone();
    let _ = walk_expr(&mut counter, &mut expr);
    counter.count
}

/// Find all column references in an expression.
pub fn find_columns(expr: &Expr) -> Vec<crate::parser::ast::ColumnRef> {
    struct Finder {
        columns: Vec<crate::parser::ast::ColumnRef>,
    }
    impl ExprWalker for Finder {
        fn walk_expr(&mut self, expr: &mut Expr) -> WalkResult {
            if let Expr::Column(col) = expr {
                self.columns.push(col.clone());
            }
            WalkResult::Continue
        }
    }

    let mut finder = Finder {
        columns: Vec::new(),
    };
    let mut expr = expr.clone();
    let _ = walk_expr(&mut finder, &mut expr);
    finder.columns
}

/// Calculate maximum depth of an expression tree.
pub fn expr_depth(expr: &Expr) -> i32 {
    fn depth(expr: &Expr) -> i32 {
        match expr {
            Expr::Literal(_) | Expr::Column(_) | Expr::Variable(_) | Expr::Raise { .. } => 1,
            Expr::Unary { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::Collate { expr, .. }
            | Expr::IsNull { expr, .. }
            | Expr::Parens(expr) => 1 + depth(expr),
            Expr::Binary { left, right, .. } | Expr::IsDistinct { left, right, .. } => {
                1 + depth(left).max(depth(right))
            }
            Expr::Between {
                expr, low, high, ..
            } => 1 + depth(expr).max(depth(low)).max(depth(high)),
            Expr::In { expr, list, .. } => {
                let list_depth = match list {
                    InList::Values(values) => values.iter().map(depth).max().unwrap_or(0),
                    InList::Subquery(_) | InList::Table(_) => 0,
                };
                1 + depth(expr).max(list_depth)
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                let escape_depth = escape.as_ref().map(|e| depth(e)).unwrap_or(0);
                1 + depth(expr).max(depth(pattern)).max(escape_depth)
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                let mut max_depth = operand.as_ref().map(|e| depth(e)).unwrap_or(0);
                for WhenClause { when, then } in when_clauses {
                    max_depth = max_depth.max(depth(when)).max(depth(then));
                }
                if let Some(else_clause) = else_clause {
                    max_depth = max_depth.max(depth(else_clause));
                }
                1 + max_depth
            }
            Expr::Function(FunctionCall {
                args, filter, over, ..
            }) => {
                let args_depth = match args {
                    FunctionArgs::Star => 0,
                    FunctionArgs::Exprs(exprs) => exprs.iter().map(depth).max().unwrap_or(0),
                };
                let filter_depth = filter.as_ref().map(|e| depth(e)).unwrap_or(0);
                let over_depth = match over {
                    Some(Over::Spec(spec)) => window_spec_depth(spec),
                    _ => 0,
                };
                1 + args_depth.max(filter_depth).max(over_depth)
            }
            Expr::Subquery(_) | Expr::Exists { .. } => 1,
        }
    }

    fn window_spec_depth(spec: &WindowSpec) -> i32 {
        let mut max_depth = 0;
        if let Some(partition_by) = &spec.partition_by {
            max_depth = max_depth.max(partition_by.iter().map(depth).max().unwrap_or(0));
        }
        if let Some(order_by) = &spec.order_by {
            max_depth = max_depth.max(
                order_by
                    .iter()
                    .map(|term| depth(&term.expr))
                    .max()
                    .unwrap_or(0),
            );
        }
        if let Some(frame) = &spec.frame {
            max_depth = max_depth.max(window_frame_depth(frame));
        }
        max_depth
    }

    fn window_frame_depth(frame: &WindowFrame) -> i32 {
        let start_depth = window_bound_depth(&frame.start);
        let end_depth = frame.end.as_ref().map(window_bound_depth).unwrap_or(0);
        start_depth.max(end_depth)
    }

    fn window_bound_depth(bound: &WindowFrameBound) -> i32 {
        match bound {
            WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr) => depth(expr),
            WindowFrameBound::CurrentRow
            | WindowFrameBound::UnboundedPreceding
            | WindowFrameBound::UnboundedFollowing => 0,
        }
    }

    depth(expr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{BinaryOp, Expr};

    #[test]
    fn test_walk_expr_prune() {
        let mut expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::int(1)),
            right: Box::new(Expr::int(2)),
        };

        struct Pruner {
            visited: usize,
        }
        impl ExprWalker for Pruner {
            fn walk_expr(&mut self, _expr: &mut Expr) -> WalkResult {
                self.visited += 1;
                WalkResult::Prune
            }
        }

        let mut pruner = Pruner { visited: 0 };
        let _ = walk_expr(&mut pruner, &mut expr);
        assert_eq!(pruner.visited, 1);
    }

    #[test]
    fn test_walk_expr_abort() {
        let mut expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::int(1)),
            right: Box::new(Expr::int(2)),
        };

        struct Aborter {
            visited: usize,
        }
        impl ExprWalker for Aborter {
            fn walk_expr(&mut self, _expr: &mut Expr) -> WalkResult {
                self.visited += 1;
                WalkResult::Abort
            }
        }

        let mut aborter = Aborter { visited: 0 };
        let _ = walk_expr(&mut aborter, &mut expr);
        assert_eq!(aborter.visited, 1);
    }

    #[test]
    fn test_expr_depth() {
        let expr = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Expr::int(1)),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Mul,
                left: Box::new(Expr::int(2)),
                right: Box::new(Expr::int(3)),
            }),
        };
        assert_eq!(expr_depth(&expr), 3);
    }
}
