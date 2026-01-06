//! Name resolution (resolve.c translation).

use std::collections::HashMap;

use crate::error::{Error, ErrorCode, Result};
use crate::parser::ast::*;
use crate::schema::{Schema, Table};

#[derive(Debug, Clone)]
struct SourceInfo {
    name: String,
    table_name: String,
    columns: Vec<String>,
    has_rowid: bool,
}

impl SourceInfo {
    fn find_column(&self, column: &str) -> Option<i32> {
        if self.has_rowid && is_rowid_alias(column) {
            return Some(-1);
        }
        self.columns
            .iter()
            .position(|col| col.eq_ignore_ascii_case(column))
            .map(|idx| idx as i32)
    }
}

/// Resolver for column names and aliases.
pub struct Resolver<'a> {
    schema: &'a Schema,
}

impl<'a> Resolver<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self { schema }
    }

    pub fn resolve_stmt(&self, stmt: &mut Stmt) -> Result<()> {
        match stmt {
            Stmt::Select(select) => self.resolve_select(select),
            Stmt::Insert(insert) => self.resolve_insert(insert),
            Stmt::Update(update) => self.resolve_update(update),
            Stmt::Delete(delete) => self.resolve_delete(delete),
            Stmt::CreateView(create) => self.resolve_select(&mut create.query),
            _ => Ok(()),
        }
    }

    pub fn resolve_select(&self, select: &mut SelectStmt) -> Result<()> {
        let mut ctes: HashMap<String, &CommonTableExpr> = HashMap::new();
        if let Some(with) = &mut select.with {
            for cte in &mut with.ctes {
                self.resolve_select(&mut cte.query)?;
            }
            for cte in &with.ctes {
                ctes.insert(cte.name.to_lowercase(), cte);
            }
        }

        self.resolve_select_body(&mut select.body, &ctes)?;

        let sources = match &select.body {
            SelectBody::Select(core) => {
                if let Some(from) = &core.from {
                    self.collect_sources(from, &ctes)?
                } else {
                    Vec::new()
                }
            }
            SelectBody::Compound { left, .. } => self.collect_sources_from_body(left, &ctes)?,
        };

        if let Some(order_by) = &mut select.order_by {
            let result_columns = result_columns_from_body(&select.body);
            self.resolve_order_by(order_by, &result_columns, &sources)?;
        }

        if let Some(limit) = &mut select.limit {
            self.resolve_expr(&mut limit.limit, &sources)?;
            if let Some(offset) = &mut limit.offset {
                self.resolve_expr(offset, &sources)?;
            }
        }

        Ok(())
    }

    fn resolve_select_body(
        &self,
        body: &mut SelectBody,
        ctes: &HashMap<String, &CommonTableExpr>,
    ) -> Result<()> {
        match body {
            SelectBody::Select(core) => self.resolve_select_core(core, ctes),
            SelectBody::Compound { left, right, .. } => {
                self.resolve_select_body(left, ctes)?;
                self.resolve_select_body(right, ctes)
            }
        }
    }

    fn resolve_select_core(
        &self,
        core: &mut SelectCore,
        ctes: &HashMap<String, &CommonTableExpr>,
    ) -> Result<()> {
        let sources = if let Some(from) = &core.from {
            self.collect_sources(from, ctes)?
        } else {
            Vec::new()
        };

        if let Some(from) = &mut core.from {
            for table_ref in &mut from.tables {
                self.resolve_table_ref(table_ref, &sources, ctes)?;
            }
        }

        for col in &mut core.columns {
            if let ResultColumn::Expr { expr, .. } = col {
                self.resolve_expr(expr, &sources)?;
            }
        }

        if let Some(where_clause) = &mut core.where_clause {
            self.resolve_expr(where_clause, &sources)?;
        }

        if let Some(group_by) = &mut core.group_by {
            for expr in group_by {
                self.resolve_expr(expr, &sources)?;
            }
        }

        if let Some(having) = &mut core.having {
            self.resolve_expr(having, &sources)?;
        }

        if let Some(window_defs) = &mut core.window {
            for window_def in window_defs {
                self.resolve_window_spec(&mut window_def.spec, &sources)?;
            }
        }

        Ok(())
    }

    fn resolve_insert(&self, insert: &mut InsertStmt) -> Result<()> {
        if let Some(with) = &mut insert.with {
            for cte in &mut with.ctes {
                self.resolve_select(&mut cte.query)?;
            }
        }

        match &mut insert.source {
            InsertSource::Values(rows) => {
                for row in rows {
                    for expr in row {
                        self.resolve_expr(expr, &[])?;
                    }
                }
            }
            InsertSource::Select(select) => {
                self.resolve_select(select)?;
            }
            InsertSource::DefaultValues => {}
        }

        if let Some(conflict) = &mut insert.on_conflict {
            if let Some(where_clause) = conflict
                .target
                .as_mut()
                .and_then(|t| t.where_clause.as_mut())
            {
                self.resolve_expr(where_clause, &[])?;
            }
            if let ConflictResolution::Update {
                assignments,
                where_clause,
            } = &mut conflict.action
            {
                for assignment in assignments {
                    self.resolve_expr(&mut assignment.expr, &[])?;
                }
                if let Some(expr) = where_clause {
                    self.resolve_expr(expr, &[])?;
                }
            }
        }

        if let Some(returning) = &mut insert.returning {
            for col in returning {
                if let ResultColumn::Expr { expr, .. } = col {
                    self.resolve_expr(expr, &[])?;
                }
            }
        }

        Ok(())
    }

    fn resolve_update(&self, update: &mut UpdateStmt) -> Result<()> {
        if let Some(with) = &mut update.with {
            for cte in &mut with.ctes {
                self.resolve_select(&mut cte.query)?;
            }
        }

        let sources = self.collect_update_sources(update)?;

        for assignment in &mut update.assignments {
            self.resolve_expr(&mut assignment.expr, &sources)?;
        }

        if let Some(where_clause) = &mut update.where_clause {
            self.resolve_expr(where_clause, &sources)?;
        }

        if let Some(returning) = &mut update.returning {
            for col in returning {
                if let ResultColumn::Expr { expr, .. } = col {
                    self.resolve_expr(expr, &sources)?;
                }
            }
        }

        if let Some(order_by) = &mut update.order_by {
            let result_columns = Vec::new();
            self.resolve_order_by(order_by, &result_columns, &sources)?;
        }

        if let Some(limit) = &mut update.limit {
            self.resolve_expr(&mut limit.limit, &sources)?;
            if let Some(offset) = &mut limit.offset {
                self.resolve_expr(offset, &sources)?;
            }
        }

        Ok(())
    }

    fn resolve_delete(&self, delete: &mut DeleteStmt) -> Result<()> {
        if let Some(with) = &mut delete.with {
            for cte in &mut with.ctes {
                self.resolve_select(&mut cte.query)?;
            }
        }

        let sources = self.collect_delete_sources(delete)?;

        if let Some(where_clause) = &mut delete.where_clause {
            self.resolve_expr(where_clause, &sources)?;
        }

        if let Some(returning) = &mut delete.returning {
            for col in returning {
                if let ResultColumn::Expr { expr, .. } = col {
                    self.resolve_expr(expr, &sources)?;
                }
            }
        }

        if let Some(order_by) = &mut delete.order_by {
            let result_columns = Vec::new();
            self.resolve_order_by(order_by, &result_columns, &sources)?;
        }

        if let Some(limit) = &mut delete.limit {
            self.resolve_expr(&mut limit.limit, &sources)?;
            if let Some(offset) = &mut limit.offset {
                self.resolve_expr(offset, &sources)?;
            }
        }

        Ok(())
    }

    fn collect_update_sources(&self, update: &UpdateStmt) -> Result<Vec<SourceInfo>> {
        let mut sources = Vec::new();
        let (primary_name, primary_table) = self.lookup_table(&update.table.name)?;
        sources.push(SourceInfo {
            name: update.alias.clone().unwrap_or_else(|| primary_name.clone()),
            table_name: primary_name,
            columns: primary_table
                .columns
                .iter()
                .map(|col| col.name.clone())
                .collect(),
            has_rowid: !primary_table.without_rowid,
        });

        if let Some(from) = &update.from {
            sources.extend(self.collect_sources(from, &HashMap::new())?);
        }

        Ok(sources)
    }

    fn collect_delete_sources(&self, delete: &DeleteStmt) -> Result<Vec<SourceInfo>> {
        let (table_name, table) = self.lookup_table(&delete.table.name)?;
        let source_name = delete.alias.clone().unwrap_or_else(|| table_name.clone());
        Ok(vec![SourceInfo {
            name: source_name,
            table_name,
            columns: table.columns.iter().map(|col| col.name.clone()).collect(),
            has_rowid: !table.without_rowid,
        }])
    }

    fn collect_sources(
        &self,
        from: &FromClause,
        ctes: &HashMap<String, &CommonTableExpr>,
    ) -> Result<Vec<SourceInfo>> {
        let mut sources = Vec::new();
        for table_ref in &from.tables {
            self.collect_table_ref(table_ref, &mut sources, ctes)?;
        }
        Ok(sources)
    }

    fn collect_sources_from_body(
        &self,
        body: &SelectBody,
        ctes: &HashMap<String, &CommonTableExpr>,
    ) -> Result<Vec<SourceInfo>> {
        match body {
            SelectBody::Select(core) => {
                if let Some(from) = &core.from {
                    self.collect_sources(from, ctes)
                } else {
                    Ok(Vec::new())
                }
            }
            SelectBody::Compound { left, .. } => self.collect_sources_from_body(left, ctes),
        }
    }

    fn collect_table_ref(
        &self,
        table_ref: &TableRef,
        sources: &mut Vec<SourceInfo>,
        ctes: &HashMap<String, &CommonTableExpr>,
    ) -> Result<()> {
        match table_ref {
            TableRef::Table { name, alias, .. } => {
                if let Some(cte) = ctes.get(&name.name.to_lowercase()) {
                    let columns = cte
                        .columns
                        .clone()
                        .unwrap_or_else(|| result_column_names(&cte.query.body));
                    sources.push(SourceInfo {
                        name: alias.clone().unwrap_or_else(|| name.name.clone()),
                        table_name: name.name.clone(),
                        columns,
                        has_rowid: false,
                    });
                    return Ok(());
                }

                let (table_name, table) = self.lookup_table(&name.name)?;
                sources.push(SourceInfo {
                    name: alias.clone().unwrap_or_else(|| table_name.clone()),
                    table_name,
                    columns: table.columns.iter().map(|col| col.name.clone()).collect(),
                    has_rowid: !table.without_rowid,
                });
            }
            TableRef::Subquery { query, alias } => {
                let columns = result_column_names(&query.body);
                sources.push(SourceInfo {
                    name: alias.clone().unwrap_or_else(|| "subquery".to_string()),
                    table_name: String::new(),
                    columns,
                    has_rowid: false,
                });
            }
            TableRef::Join { left, right, .. } => {
                self.collect_table_ref(left, sources, ctes)?;
                self.collect_table_ref(right, sources, ctes)?;
            }
            TableRef::Parens(inner) => {
                self.collect_table_ref(inner, sources, ctes)?;
            }
            TableRef::TableFunction { name, .. } => {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("table-valued function {} not supported", name),
                ));
            }
        }

        Ok(())
    }

    fn resolve_table_ref(
        &self,
        table_ref: &mut TableRef,
        sources: &[SourceInfo],
        ctes: &HashMap<String, &CommonTableExpr>,
    ) -> Result<()> {
        match table_ref {
            TableRef::Join {
                left,
                right,
                constraint,
                ..
            } => {
                self.resolve_table_ref(left, sources, ctes)?;
                self.resolve_table_ref(right, sources, ctes)?;
                if let Some(JoinConstraint::On(expr)) = constraint {
                    self.resolve_expr(expr, sources)?;
                }
            }
            TableRef::Subquery { query, .. } => {
                self.resolve_select(query)?;
            }
            TableRef::Parens(inner) => {
                self.resolve_table_ref(inner, sources, ctes)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn resolve_expr(&self, expr: &mut Expr, sources: &[SourceInfo]) -> Result<()> {
        match expr {
            Expr::Column(col_ref) => self.resolve_column_ref(col_ref, sources),
            Expr::Unary { expr, .. } => self.resolve_expr(expr, sources),
            Expr::Binary { left, right, .. } => {
                self.resolve_expr(left, sources)?;
                self.resolve_expr(right, sources)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.resolve_expr(expr, sources)?;
                self.resolve_expr(low, sources)?;
                self.resolve_expr(high, sources)
            }
            Expr::In { expr, list, .. } => {
                self.resolve_expr(expr, sources)?;
                match list {
                    InList::Values(values) => {
                        for value in values {
                            self.resolve_expr(value, sources)?;
                        }
                    }
                    InList::Subquery(select) => {
                        self.resolve_select(select)?;
                    }
                    InList::Table(name) => {
                        let _ = self.lookup_table(&name.name)?;
                    }
                }
                Ok(())
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.resolve_expr(expr, sources)?;
                self.resolve_expr(pattern, sources)?;
                if let Some(escape) = escape {
                    self.resolve_expr(escape, sources)?;
                }
                Ok(())
            }
            Expr::IsNull { expr, .. } => self.resolve_expr(expr, sources),
            Expr::IsDistinct { left, right, .. } => {
                self.resolve_expr(left, sources)?;
                self.resolve_expr(right, sources)
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(operand) = operand {
                    self.resolve_expr(operand, sources)?;
                }
                for clause in when_clauses {
                    self.resolve_expr(&mut clause.when, sources)?;
                    self.resolve_expr(&mut clause.then, sources)?;
                }
                if let Some(else_clause) = else_clause {
                    self.resolve_expr(else_clause, sources)?;
                }
                Ok(())
            }
            Expr::Cast { expr, .. } => self.resolve_expr(expr, sources),
            Expr::Collate { expr, .. } => self.resolve_expr(expr, sources),
            Expr::Function(func) => {
                if let FunctionArgs::Exprs(args) = &mut func.args {
                    for arg in args {
                        self.resolve_expr(arg, sources)?;
                    }
                }
                Ok(())
            }
            Expr::Subquery(select) => self.resolve_select(select),
            Expr::Exists { subquery, .. } => self.resolve_select(subquery),
            Expr::Parens(inner) => self.resolve_expr(inner, sources),
            Expr::Raise { .. } => Ok(()),
            Expr::Literal(_) | Expr::Variable(_) => Ok(()),
        }
    }

    fn resolve_column_ref(&self, col_ref: &mut ColumnRef, sources: &[SourceInfo]) -> Result<()> {
        let column = col_ref.column.clone();
        if let Some(table_name) = &col_ref.table {
            let source = sources
                .iter()
                .find(|source| {
                    source.name.eq_ignore_ascii_case(table_name)
                        || source.table_name.eq_ignore_ascii_case(table_name)
                })
                .ok_or_else(|| {
                    Error::with_message(ErrorCode::Error, format!("no such table: {}", table_name))
                })?;
            let idx = source.find_column(&column).ok_or_else(|| {
                Error::with_message(
                    ErrorCode::Error,
                    format!("no such column: {}.{}", table_name, column),
                )
            })?;
            col_ref.table = Some(source.name.clone());
            col_ref.column_index = Some(idx);
            return Ok(());
        }

        let mut matches = Vec::new();
        for source in sources {
            if let Some(idx) = source.find_column(&column) {
                matches.push((source, idx));
            }
        }

        match matches.len() {
            0 => Err(Error::with_message(
                ErrorCode::Error,
                format!("no such column: {}", column),
            )),
            1 => {
                let (source, idx) = matches.remove(0);
                col_ref.table = Some(source.name.clone());
                col_ref.column_index = Some(idx);
                Ok(())
            }
            _ => Err(Error::with_message(
                ErrorCode::Error,
                format!("ambiguous column name: {}", column),
            )),
        }
    }

    fn resolve_order_by(
        &self,
        order_by: &mut [OrderingTerm],
        result_columns: &[ResultColumn],
        sources: &[SourceInfo],
    ) -> Result<()> {
        let entries = result_column_entries(result_columns);

        for term in order_by {
            let mut replacement = None;
            match &term.expr {
                Expr::Literal(Literal::Integer(idx)) => {
                    if *idx > 0 {
                        replacement = result_column_expr(result_columns, *idx as usize - 1);
                    }
                }
                Expr::Column(col_ref) => {
                    if col_ref.table.is_none() {
                        if let Some((_, expr)) = entries
                            .iter()
                            .find(|(name, _)| name.eq_ignore_ascii_case(&col_ref.column))
                        {
                            replacement = Some(expr.clone());
                        }
                    }
                }
                _ => {}
            }

            if let Some(expr) = replacement {
                term.expr = expr;
            }
            self.resolve_expr(&mut term.expr, sources)?;
        }

        Ok(())
    }

    fn resolve_window_spec(&self, spec: &mut WindowSpec, sources: &[SourceInfo]) -> Result<()> {
        if let Some(partition_by) = &mut spec.partition_by {
            for expr in partition_by {
                self.resolve_expr(expr, sources)?;
            }
        }
        if let Some(order_by) = &mut spec.order_by {
            let result_columns = Vec::new();
            self.resolve_order_by(order_by, &result_columns, sources)?;
        }
        if let Some(frame) = &mut spec.frame {
            self.resolve_window_frame(frame, sources)?;
        }
        Ok(())
    }

    fn resolve_window_frame(&self, frame: &mut WindowFrame, sources: &[SourceInfo]) -> Result<()> {
        self.resolve_window_bound(&mut frame.start, sources)?;
        if let Some(end) = &mut frame.end {
            self.resolve_window_bound(end, sources)?;
        }
        Ok(())
    }

    fn resolve_window_bound(
        &self,
        bound: &mut WindowFrameBound,
        sources: &[SourceInfo],
    ) -> Result<()> {
        match bound {
            WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr) => {
                self.resolve_expr(expr, sources)
            }
            WindowFrameBound::CurrentRow
            | WindowFrameBound::UnboundedPreceding
            | WindowFrameBound::UnboundedFollowing => Ok(()),
        }
    }

    fn lookup_table(&self, name: &str) -> Result<(String, Table)> {
        let table = self.schema.table(name).ok_or_else(|| {
            Error::with_message(ErrorCode::Error, format!("no such table: {}", name))
        })?;
        Ok((table.name.clone(), (*table).clone()))
    }
}

fn result_column_entries(columns: &[ResultColumn]) -> Vec<(String, Expr)> {
    columns
        .iter()
        .enumerate()
        .filter_map(|(idx, col)| {
            if let ResultColumn::Expr { expr, alias } = col {
                let name = alias.clone().unwrap_or_else(|| expr_name(expr, idx));
                Some((name, expr.clone()))
            } else {
                None
            }
        })
        .collect()
}

fn result_column_expr(columns: &[ResultColumn], idx: usize) -> Option<Expr> {
    columns.get(idx).and_then(|col| match col {
        ResultColumn::Expr { expr, .. } => Some(expr.clone()),
        _ => None,
    })
}

fn result_column_names(body: &SelectBody) -> Vec<String> {
    result_columns_from_body(body)
        .iter()
        .enumerate()
        .map(|(idx, col)| match col {
            ResultColumn::Expr { expr, alias } => {
                alias.clone().unwrap_or_else(|| expr_name(expr, idx))
            }
            ResultColumn::Star => format!("column{}", idx + 1),
            ResultColumn::TableStar(table) => format!("{}.*", table),
        })
        .collect()
}

fn result_columns_from_body(body: &SelectBody) -> Vec<ResultColumn> {
    match body {
        SelectBody::Select(core) => core.columns.clone(),
        SelectBody::Compound { left, .. } => result_columns_from_body(left),
    }
}

fn expr_name(expr: &Expr, index: usize) -> String {
    match expr {
        Expr::Column(col) => col.column.clone(),
        Expr::Literal(lit) => format!("{:?}", lit),
        Expr::Function(func) => func.name.clone(),
        _ => format!("column{}", index + 1),
    }
}

fn is_rowid_alias(name: &str) -> bool {
    name.eq_ignore_ascii_case("rowid")
        || name.eq_ignore_ascii_case("_rowid_")
        || name.eq_ignore_ascii_case("oid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, Schema, Table};
    use std::sync::Arc;

    fn schema_with_table(name: &str, columns: &[&str]) -> Schema {
        let mut schema = Schema::new();
        let mut table = Table::new(name);
        table.columns = columns.iter().map(|col| Column::new(*col)).collect();
        schema.tables.insert(name.to_lowercase(), Arc::new(table));
        schema
    }

    #[test]
    fn resolve_simple_column() {
        let schema = schema_with_table("t1", &["id", "name"]);
        let mut select = SelectStmt {
            with: None,
            body: SelectBody::Select(SelectCore {
                distinct: Distinct::All,
                columns: vec![ResultColumn::Expr {
                    expr: Expr::Column(ColumnRef::new("id")),
                    alias: None,
                }],
                from: Some(FromClause {
                    tables: vec![TableRef::Table {
                        name: QualifiedName::new("t1"),
                        alias: None,
                        indexed_by: None,
                    }],
                }),
                where_clause: None,
                group_by: None,
                having: None,
                window: None,
            }),
            order_by: None,
            limit: None,
        };

        let resolver = Resolver::new(&schema);
        resolver.resolve_select(&mut select).unwrap();

        if let SelectBody::Select(core) = &select.body {
            if let ResultColumn::Expr { expr, .. } = &core.columns[0] {
                if let Expr::Column(col_ref) = expr {
                    assert_eq!(col_ref.table.as_deref(), Some("t1"));
                    assert_eq!(col_ref.column_index, Some(0));
                } else {
                    panic!("expected column ref");
                }
            }
        }
    }

    #[test]
    fn resolve_ambiguous_column() {
        let mut schema = schema_with_table("t1", &["id"]);
        let mut table = Table::new("t2");
        table.columns = vec![Column::new("id")];
        schema.tables.insert("t2".to_string(), Arc::new(table));

        let mut select = SelectStmt {
            with: None,
            body: SelectBody::Select(SelectCore {
                distinct: Distinct::All,
                columns: vec![ResultColumn::Expr {
                    expr: Expr::Column(ColumnRef::new("id")),
                    alias: None,
                }],
                from: Some(FromClause {
                    tables: vec![
                        TableRef::Table {
                            name: QualifiedName::new("t1"),
                            alias: None,
                            indexed_by: None,
                        },
                        TableRef::Table {
                            name: QualifiedName::new("t2"),
                            alias: None,
                            indexed_by: None,
                        },
                    ],
                }),
                where_clause: None,
                group_by: None,
                having: None,
                window: None,
            }),
            order_by: None,
            limit: None,
        };

        let resolver = Resolver::new(&schema);
        let result = resolver.resolve_select(&mut select);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_order_by_alias() {
        let schema = schema_with_table("t1", &["id"]);
        let mut select = SelectStmt {
            with: None,
            body: SelectBody::Select(SelectCore {
                distinct: Distinct::All,
                columns: vec![ResultColumn::Expr {
                    expr: Expr::Column(ColumnRef::new("id")),
                    alias: Some("ident".to_string()),
                }],
                from: Some(FromClause {
                    tables: vec![TableRef::Table {
                        name: QualifiedName::new("t1"),
                        alias: None,
                        indexed_by: None,
                    }],
                }),
                where_clause: None,
                group_by: None,
                having: None,
                window: None,
            }),
            order_by: Some(vec![OrderingTerm {
                expr: Expr::Column(ColumnRef::new("ident")),
                order: SortOrder::Asc,
                nulls: NullsOrder::Default,
            }]),
            limit: None,
        };

        let resolver = Resolver::new(&schema);
        resolver.resolve_select(&mut select).unwrap();

        let order_by = select.order_by.as_ref().unwrap();
        if let Expr::Column(col_ref) = &order_by[0].expr {
            assert_eq!(col_ref.table.as_deref(), Some("t1"));
            assert_eq!(col_ref.column, "id");
        } else {
            panic!("expected column ref");
        }
    }
}
