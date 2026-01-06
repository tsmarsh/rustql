//! WHERE expression analysis helpers
//!
//! This module provides a small subset of the expression analysis utilities
//! found in SQLite's whereexpr.c. It focuses on operator classification,
//! commutation, and expression table-usage tracking for query planning.

use bitflags::bitflags;

use crate::parser::ast::{
    BinaryOp, Expr, FunctionArgs, FunctionCall, InList, JoinConstraint, OrderingTerm, Over,
    ResultColumn, SelectBody, SelectCore, SelectStmt, TableRef, WindowFrame, WindowFrameBound,
    WindowSpec,
};

bitflags! {
    /// Bitmask describing a comparison operator used in WHERE terms.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct OperatorMask: u16 {
        const EQ = 0x0001;
        const LT = 0x0002;
        const LE = 0x0004;
        const GT = 0x0008;
        const GE = 0x0010;
        const IN = 0x0020;
        const IS = 0x0040;
        const ISNULL = 0x0080;
    }
}

/// Return true if the expression operator is allowed for indexable WHERE terms.
pub fn allowed_expr_op(expr: &Expr) -> bool {
    operator_mask(expr).is_some()
}

/// Map an expression operator to an OperatorMask entry.
pub fn operator_mask(expr: &Expr) -> Option<OperatorMask> {
    match expr {
        Expr::Binary { op, .. } => match op {
            BinaryOp::Eq => Some(OperatorMask::EQ),
            BinaryOp::Lt => Some(OperatorMask::LT),
            BinaryOp::Le => Some(OperatorMask::LE),
            BinaryOp::Gt => Some(OperatorMask::GT),
            BinaryOp::Ge => Some(OperatorMask::GE),
            BinaryOp::Is => Some(OperatorMask::IS),
            _ => None,
        },
        Expr::In { .. } => Some(OperatorMask::IN),
        Expr::IsNull { .. } => Some(OperatorMask::ISNULL),
        _ => None,
    }
}

/// Commute a comparison operator by swapping its operands.
pub fn commute_comparison(expr: &mut Expr) -> bool {
    let (op, left, right) = match expr {
        Expr::Binary { op, left, right } => (op, left, right),
        _ => return false,
    };

    let new_op = match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::Le => BinaryOp::Ge,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::Ge => BinaryOp::Le,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Is | BinaryOp::IsNot => *op,
        _ => return false,
    };

    std::mem::swap(left, right);
    *op = new_op;
    true
}

/// Split an OR expression into its component terms.
pub fn split_or_clause(expr: &Expr) -> Vec<Expr> {
    let mut terms = Vec::new();
    split_or_clause_inner(expr, &mut terms);
    terms
}

/// Compute a table-usage mask for the given expression.
pub fn expr_usage(expr: &Expr, tables: &[(String, Option<String>, u64)]) -> u64 {
    match expr {
        Expr::Literal(_) | Expr::Variable(_) | Expr::Raise { .. } => 0,
        Expr::Column(col) => column_usage(col, tables),
        Expr::Unary { expr, .. } => expr_usage(expr, tables),
        Expr::Binary { left, right, .. } => expr_usage(left, tables) | expr_usage(right, tables),
        Expr::Between {
            expr, low, high, ..
        } => expr_usage(expr, tables) | expr_usage(low, tables) | expr_usage(high, tables),
        Expr::In { expr, list, .. } => expr_usage(expr, tables) | in_list_usage(list, tables),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            let mut mask = expr_usage(expr, tables) | expr_usage(pattern, tables);
            if let Some(escape) = escape {
                mask |= expr_usage(escape, tables);
            }
            mask
        }
        Expr::IsNull { expr, .. } => expr_usage(expr, tables),
        Expr::IsDistinct { left, right, .. } => {
            expr_usage(left, tables) | expr_usage(right, tables)
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            let mut mask = 0;
            if let Some(operand) = operand {
                mask |= expr_usage(operand, tables);
            }
            for clause in when_clauses {
                mask |= expr_usage(&clause.when, tables);
                mask |= expr_usage(&clause.then, tables);
            }
            if let Some(else_clause) = else_clause {
                mask |= expr_usage(else_clause, tables);
            }
            mask
        }
        Expr::Cast { expr, .. } | Expr::Collate { expr, .. } | Expr::Parens(expr) => {
            expr_usage(expr, tables)
        }
        Expr::Function(call) => function_usage(call, tables),
        Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => select_usage(subquery, tables),
    }
}

fn split_or_clause_inner(expr: &Expr, terms: &mut Vec<Expr>) {
    match expr {
        Expr::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } => {
            split_or_clause_inner(left, terms);
            split_or_clause_inner(right, terms);
        }
        _ => terms.push(expr.clone()),
    }
}

/// Compute a table-usage mask for an expression list.
pub fn expr_list_usage(exprs: &[Expr], tables: &[(String, Option<String>, u64)]) -> u64 {
    exprs
        .iter()
        .fold(0, |mask, expr| mask | expr_usage(expr, tables))
}

/// Compute a table-usage mask for a SELECT statement.
pub fn select_usage(select: &SelectStmt, tables: &[(String, Option<String>, u64)]) -> u64 {
    let mut mask = 0;

    if let Some(with) = &select.with {
        for cte in &with.ctes {
            mask |= select_usage(&cte.query, tables);
        }
    }

    match &select.body {
        SelectBody::Select(core) => {
            mask |= select_core_usage(core, tables);
        }
        SelectBody::Compound { left, right, .. } => {
            mask |= select_body_usage(left, tables);
            mask |= select_body_usage(right, tables);
        }
    }

    if let Some(order_by) = &select.order_by {
        mask |= ordering_usage(order_by, tables);
    }

    if let Some(limit) = &select.limit {
        mask |= expr_usage(&limit.limit, tables);
        if let Some(offset) = &limit.offset {
            mask |= expr_usage(offset, tables);
        }
    }

    mask
}

fn select_body_usage(select: &SelectBody, tables: &[(String, Option<String>, u64)]) -> u64 {
    match select {
        SelectBody::Select(core) => select_core_usage(core, tables),
        SelectBody::Compound { left, right, .. } => {
            select_body_usage(left, tables) | select_body_usage(right, tables)
        }
    }
}

fn select_core_usage(core: &SelectCore, tables: &[(String, Option<String>, u64)]) -> u64 {
    let mut mask = 0;

    for column in &core.columns {
        mask |= result_column_usage(column, tables);
    }
    if let Some(from) = &core.from {
        for table in &from.tables {
            mask |= table_ref_usage(table, tables);
        }
    }
    if let Some(where_clause) = &core.where_clause {
        mask |= expr_usage(where_clause, tables);
    }
    if let Some(group_by) = &core.group_by {
        mask |= expr_list_usage(group_by, tables);
    }
    if let Some(having) = &core.having {
        mask |= expr_usage(having, tables);
    }
    if let Some(window) = &core.window {
        for def in window {
            mask |= window_spec_usage(&def.spec, tables);
        }
    }

    mask
}

fn result_column_usage(column: &ResultColumn, tables: &[(String, Option<String>, u64)]) -> u64 {
    match column {
        ResultColumn::Star => 0,
        ResultColumn::TableStar(name) => table_name_mask(name, tables),
        ResultColumn::Expr { expr, .. } => expr_usage(expr, tables),
    }
}

fn table_ref_usage(table: &TableRef, tables: &[(String, Option<String>, u64)]) -> u64 {
    match table {
        TableRef::Table { .. } => 0,
        TableRef::Subquery { query, .. } => select_usage(query, tables),
        TableRef::Join {
            left,
            right,
            constraint,
            ..
        } => {
            let mut mask = table_ref_usage(left, tables) | table_ref_usage(right, tables);
            if let Some(constraint) = constraint {
                if let JoinConstraint::On(expr) = constraint {
                    mask |= expr_usage(expr, tables);
                }
            }
            mask
        }
        TableRef::TableFunction { args, .. } => expr_list_usage(args, tables),
        TableRef::Parens(inner) => table_ref_usage(inner, tables),
    }
}

fn in_list_usage(list: &InList, tables: &[(String, Option<String>, u64)]) -> u64 {
    match list {
        InList::Values(exprs) => expr_list_usage(exprs, tables),
        InList::Subquery(query) => select_usage(query, tables),
        InList::Table(_) => 0,
    }
}

fn function_usage(call: &FunctionCall, tables: &[(String, Option<String>, u64)]) -> u64 {
    let mut mask = 0;
    if let FunctionArgs::Exprs(exprs) = &call.args {
        mask |= expr_list_usage(exprs, tables);
    }
    if let Some(filter) = &call.filter {
        mask |= expr_usage(filter, tables);
    }
    if let Some(over) = &call.over {
        mask |= over_usage(over, tables);
    }
    mask
}

fn over_usage(over: &Over, tables: &[(String, Option<String>, u64)]) -> u64 {
    match over {
        Over::Window(_) => 0,
        Over::Spec(spec) => window_spec_usage(spec, tables),
    }
}

fn window_spec_usage(spec: &WindowSpec, tables: &[(String, Option<String>, u64)]) -> u64 {
    let mut mask = 0;
    if let Some(partition_by) = &spec.partition_by {
        mask |= expr_list_usage(partition_by, tables);
    }
    if let Some(order_by) = &spec.order_by {
        mask |= ordering_usage(order_by, tables);
    }
    if let Some(frame) = &spec.frame {
        mask |= window_frame_usage(frame, tables);
    }
    mask
}

fn window_frame_usage(frame: &WindowFrame, tables: &[(String, Option<String>, u64)]) -> u64 {
    let mut mask = window_frame_bound_usage(&frame.start, tables);
    if let Some(end) = &frame.end {
        mask |= window_frame_bound_usage(end, tables);
    }
    mask
}

fn window_frame_bound_usage(
    bound: &WindowFrameBound,
    tables: &[(String, Option<String>, u64)],
) -> u64 {
    match bound {
        WindowFrameBound::CurrentRow
        | WindowFrameBound::UnboundedPreceding
        | WindowFrameBound::UnboundedFollowing => 0,
        WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr) => {
            expr_usage(expr, tables)
        }
    }
}

fn ordering_usage(order_by: &[OrderingTerm], tables: &[(String, Option<String>, u64)]) -> u64 {
    order_by
        .iter()
        .fold(0, |mask, term| mask | expr_usage(&term.expr, tables))
}

fn column_usage(
    col: &crate::parser::ast::ColumnRef,
    tables: &[(String, Option<String>, u64)],
) -> u64 {
    if let Some(table) = &col.table {
        return table_name_mask(table, tables);
    }

    tables
        .iter()
        .fold(0, |mask, (_, _, table_mask)| mask | table_mask)
}

fn table_name_mask(name: &str, tables: &[(String, Option<String>, u64)]) -> u64 {
    for (table_name, alias, mask) in tables {
        let matches = match alias {
            Some(alias) => alias == name || table_name == name,
            None => table_name == name,
        };
        if matches {
            return *mask;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{BinaryOp, ColumnRef, Expr, Literal};

    fn table_map() -> Vec<(String, Option<String>, u64)> {
        vec![
            ("t1".to_string(), None, 1u64 << 0),
            ("t2".to_string(), Some("alias".to_string()), 1u64 << 1),
        ]
    }

    #[test]
    fn test_allowed_operator() {
        let eq = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::column("a")),
            right: Box::new(Expr::column("b")),
        };
        let like = Expr::Like {
            expr: Box::new(Expr::column("a")),
            pattern: Box::new(Expr::string("x%")),
            escape: None,
            op: crate::parser::ast::LikeOp::Like,
            negated: false,
        };
        let is_null = Expr::IsNull {
            expr: Box::new(Expr::column("a")),
            negated: false,
        };

        assert!(allowed_expr_op(&eq));
        assert!(allowed_expr_op(&is_null));
        assert!(!allowed_expr_op(&like));
    }

    #[test]
    fn test_operator_mask_mapping() {
        let expr = Expr::Binary {
            op: BinaryOp::Ge,
            left: Box::new(Expr::column("a")),
            right: Box::new(Expr::column("b")),
        };
        assert_eq!(operator_mask(&expr), Some(OperatorMask::GE));
    }

    #[test]
    fn test_commute_comparison() {
        let mut expr = Expr::Binary {
            op: BinaryOp::Lt,
            left: Box::new(Expr::column("a")),
            right: Box::new(Expr::column("b")),
        };
        assert!(commute_comparison(&mut expr));
        if let Expr::Binary { op, left, right } = expr {
            assert_eq!(op, BinaryOp::Gt);
            assert!(matches!(*left, Expr::Column(ColumnRef { column, .. }) if column == "b"));
            assert!(matches!(*right, Expr::Column(ColumnRef { column, .. }) if column == "a"));
        } else {
            panic!("expected binary expression");
        }
    }

    #[test]
    fn test_expr_usage_columns() {
        let tables = table_map();
        let expr = Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column(ColumnRef {
                database: None,
                table: Some("t1".to_string()),
                column: "a".to_string(),
                column_index: None,
            })),
            right: Box::new(Expr::Column(ColumnRef {
                database: None,
                table: None,
                column: "b".to_string(),
                column_index: None,
            })),
        };
        let mask = expr_usage(&expr, &tables);
        assert_eq!(mask, (1u64 << 0) | (1u64 << 1));
    }

    #[test]
    fn test_expr_usage_function() {
        let tables = table_map();
        let expr = Expr::Function(FunctionCall {
            name: "sum".to_string(),
            args: FunctionArgs::Exprs(vec![Expr::Column(ColumnRef {
                database: None,
                table: Some("t2".to_string()),
                column: "x".to_string(),
                column_index: None,
            })]),
            distinct: false,
            filter: Some(Box::new(Expr::Literal(Literal::Integer(1)))),
            over: None,
        });
        let mask = expr_usage(&expr, &tables);
        assert_eq!(mask, 1u64 << 1);
    }

    #[test]
    fn test_split_or_clause() {
        let expr = Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::column("a")),
                right: Box::new(Expr::Literal(Literal::Integer(1))),
            }),
            right: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::column("b")),
                right: Box::new(Expr::Literal(Literal::Integer(2))),
            }),
        };
        let terms = split_or_clause(&expr);
        assert_eq!(terms.len(), 2);
    }
}
