//! Statement Preparation
//!
//! This module handles SQL parsing and compilation to VDBE bytecode.
//! Corresponds to SQLite's prepare.c - the interface between the parser
//! and the code generator.

use std::collections::HashSet;

use crate::error::Result;
use crate::parser::ast::*;
use crate::parser::grammar::Parser;
use crate::types::ColumnType;
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

use super::delete::compile_delete;
use super::insert::compile_insert;
use super::select::{SelectCompiler, SelectDest};
use super::update::compile_update;

// ============================================================================
// Compiled Statement Info
// ============================================================================

/// Information about a compiled statement
#[derive(Debug, Clone)]
pub struct CompiledStmt {
    /// VDBE bytecode
    pub ops: Vec<VdbeOp>,
    /// Column names (for SELECT)
    pub column_names: Vec<String>,
    /// Column types (declared or inferred)
    pub column_types: Vec<ColumnType>,
    /// Parameter count
    pub param_count: i32,
    /// Parameter names (1-indexed, None for positional)
    pub param_names: Vec<Option<String>>,
    /// Is this a read-only statement?
    pub read_only: bool,
    /// Statement type
    pub stmt_type: StmtType,
}

/// Statement type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StmtType {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    CreateIndex,
    CreateView,
    CreateTrigger,
    DropTable,
    DropIndex,
    DropView,
    DropTrigger,
    AlterTable,
    Begin,
    Commit,
    Rollback,
    Savepoint,
    Release,
    Pragma,
    Vacuum,
    Analyze,
    Reindex,
    Attach,
    Detach,
    Explain,
    ExplainQueryPlan,
}

impl StmtType {
    /// Check if statement is read-only
    pub fn is_read_only(&self) -> bool {
        matches!(
            self,
            StmtType::Select
                | StmtType::Explain
                | StmtType::ExplainQueryPlan
                | StmtType::Begin
                | StmtType::Commit
                | StmtType::Rollback
                | StmtType::Pragma
        )
    }
}

// ============================================================================
// Statement Compiler
// ============================================================================

/// Compiles SQL statements to VDBE bytecode
pub struct StatementCompiler {
    /// Parameter counter for extraction
    param_count: i32,
    /// Parameter names found
    param_names: Vec<Option<String>>,
    /// Named parameters seen (for deduplication)
    named_params: HashSet<String>,
}

impl StatementCompiler {
    /// Create a new statement compiler
    pub fn new() -> Self {
        Self {
            param_count: 0,
            param_names: Vec::new(),
            named_params: HashSet::new(),
        }
    }

    /// Compile a SQL string to VDBE bytecode
    ///
    /// Returns the compiled statement and any remaining SQL (tail).
    pub fn compile<'a>(&mut self, sql: &'a str) -> Result<(CompiledStmt, &'a str)> {
        // Parse the SQL
        let mut parser = Parser::new(sql)?;
        let stmt = parser.parse_stmt()?;

        // Extract parameters from the AST
        self.extract_parameters(&stmt);

        // Compile based on statement type
        let (ops, stmt_type, column_names, column_types) = self.compile_stmt(&stmt)?;

        let compiled = CompiledStmt {
            ops,
            column_names,
            column_types,
            param_count: self.param_count,
            param_names: self.param_names.clone(),
            read_only: stmt_type.is_read_only(),
            stmt_type,
        };

        // Calculate the tail (remaining SQL after first statement)
        let tail = find_statement_tail(sql);

        Ok((compiled, tail))
    }

    /// Compile a parsed statement
    fn compile_stmt(
        &mut self,
        stmt: &Stmt,
    ) -> Result<(Vec<VdbeOp>, StmtType, Vec<String>, Vec<ColumnType>)> {
        match stmt {
            Stmt::Select(select) => {
                let mut compiler = SelectCompiler::new();
                let ops = compiler.compile(select, &SelectDest::Output)?;
                let (names, types) = self.extract_select_columns(select);
                Ok((ops, StmtType::Select, names, types))
            }

            Stmt::Insert(insert) => {
                let ops = compile_insert(insert)?;
                Ok((ops, StmtType::Insert, Vec::new(), Vec::new()))
            }

            Stmt::Update(update) => {
                let ops = compile_update(update)?;
                Ok((ops, StmtType::Update, Vec::new(), Vec::new()))
            }

            Stmt::Delete(delete) => {
                let ops = compile_delete(delete)?;
                Ok((ops, StmtType::Delete, Vec::new(), Vec::new()))
            }

            Stmt::CreateTable(create) => {
                let ops = self.compile_create_table(create)?;
                Ok((ops, StmtType::CreateTable, Vec::new(), Vec::new()))
            }

            Stmt::CreateIndex(create) => {
                let ops = self.compile_create_index(create)?;
                Ok((ops, StmtType::CreateIndex, Vec::new(), Vec::new()))
            }

            Stmt::CreateView(create) => {
                let ops = self.compile_create_view(create)?;
                Ok((ops, StmtType::CreateView, Vec::new(), Vec::new()))
            }

            Stmt::CreateTrigger(create) => {
                let ops = self.compile_create_trigger(create)?;
                Ok((ops, StmtType::CreateTrigger, Vec::new(), Vec::new()))
            }

            Stmt::DropTable(drop) => {
                let ops = self.compile_drop(drop, "table")?;
                Ok((ops, StmtType::DropTable, Vec::new(), Vec::new()))
            }

            Stmt::DropIndex(drop) => {
                let ops = self.compile_drop(drop, "index")?;
                Ok((ops, StmtType::DropIndex, Vec::new(), Vec::new()))
            }

            Stmt::DropView(drop) => {
                let ops = self.compile_drop(drop, "view")?;
                Ok((ops, StmtType::DropView, Vec::new(), Vec::new()))
            }

            Stmt::DropTrigger(drop) => {
                let ops = self.compile_drop(drop, "trigger")?;
                Ok((ops, StmtType::DropTrigger, Vec::new(), Vec::new()))
            }

            Stmt::AlterTable(alter) => {
                let ops = self.compile_alter_table(alter)?;
                Ok((ops, StmtType::AlterTable, Vec::new(), Vec::new()))
            }

            Stmt::Begin(begin) => {
                let ops = self.compile_begin(begin)?;
                Ok((ops, StmtType::Begin, Vec::new(), Vec::new()))
            }

            Stmt::Commit => {
                let ops = self.compile_commit()?;
                Ok((ops, StmtType::Commit, Vec::new(), Vec::new()))
            }

            Stmt::Rollback(rollback) => {
                let ops = self.compile_rollback(rollback)?;
                Ok((ops, StmtType::Rollback, Vec::new(), Vec::new()))
            }

            Stmt::Savepoint(name) => {
                let ops = self.compile_savepoint(name)?;
                Ok((ops, StmtType::Savepoint, Vec::new(), Vec::new()))
            }

            Stmt::Release(name) => {
                let ops = self.compile_release(name)?;
                Ok((ops, StmtType::Release, Vec::new(), Vec::new()))
            }

            Stmt::Pragma(pragma) => {
                let (ops, names, types) = self.compile_pragma(pragma)?;
                Ok((ops, StmtType::Pragma, names, types))
            }

            Stmt::Vacuum(vacuum) => {
                let ops = self.compile_vacuum(vacuum)?;
                Ok((ops, StmtType::Vacuum, Vec::new(), Vec::new()))
            }

            Stmt::Analyze(table) => {
                let ops = self.compile_analyze(table.as_ref())?;
                Ok((ops, StmtType::Analyze, Vec::new(), Vec::new()))
            }

            Stmt::Reindex(table) => {
                let ops = self.compile_reindex(table.as_ref())?;
                Ok((ops, StmtType::Reindex, Vec::new(), Vec::new()))
            }

            Stmt::Attach(attach) => {
                let ops = self.compile_attach(attach)?;
                Ok((ops, StmtType::Attach, Vec::new(), Vec::new()))
            }

            Stmt::Detach(name) => {
                let ops = self.compile_detach(name)?;
                Ok((ops, StmtType::Detach, Vec::new(), Vec::new()))
            }

            Stmt::Explain(inner) => {
                // Compile inner statement and wrap with explain
                let (inner_ops, _, _, _) = self.compile_stmt(inner)?;
                let ops = self.wrap_explain(inner_ops)?;
                let names = vec![
                    "addr".to_string(),
                    "opcode".to_string(),
                    "p1".to_string(),
                    "p2".to_string(),
                    "p3".to_string(),
                    "p4".to_string(),
                    "p5".to_string(),
                    "comment".to_string(),
                ];
                let types = vec![ColumnType::Integer; 8];
                Ok((ops, StmtType::Explain, names, types))
            }

            Stmt::ExplainQueryPlan(inner) => {
                let (_inner_ops, _, _, _) = self.compile_stmt(inner)?;
                let ops = self.compile_explain_query_plan()?;
                let names = vec![
                    "id".to_string(),
                    "parent".to_string(),
                    "notused".to_string(),
                    "detail".to_string(),
                ];
                let types = vec![
                    ColumnType::Integer,
                    ColumnType::Integer,
                    ColumnType::Integer,
                    ColumnType::Text,
                ];
                Ok((ops, StmtType::ExplainQueryPlan, names, types))
            }
        }
    }

    // ========================================================================
    // Parameter Extraction
    // ========================================================================

    /// Extract parameters from a statement
    fn extract_parameters(&mut self, stmt: &Stmt) {
        match stmt {
            Stmt::Select(s) => self.extract_params_select(s),
            Stmt::Insert(i) => self.extract_params_insert(i),
            Stmt::Update(u) => self.extract_params_update(u),
            Stmt::Delete(d) => self.extract_params_delete(d),
            Stmt::Explain(inner) | Stmt::ExplainQueryPlan(inner) => {
                self.extract_parameters(inner);
            }
            // Other statements typically don't have parameters
            _ => {}
        }
    }

    fn extract_params_select(&mut self, select: &SelectStmt) {
        // Process body
        self.extract_params_select_body(&select.body);

        // ORDER BY
        if let Some(order_by) = &select.order_by {
            for term in order_by {
                self.extract_params_expr(&term.expr);
            }
        }

        // LIMIT
        if let Some(limit) = &select.limit {
            self.extract_params_expr(&limit.limit);
            if let Some(offset) = &limit.offset {
                self.extract_params_expr(offset);
            }
        }
    }

    fn extract_params_select_body(&mut self, body: &SelectBody) {
        match body {
            SelectBody::Select(core) => self.extract_params_select_core(core),
            SelectBody::Compound { left, right, .. } => {
                self.extract_params_select_body(left);
                self.extract_params_select_body(right);
            }
        }
    }

    fn extract_params_select_core(&mut self, core: &SelectCore) {
        // Result columns
        for col in &core.columns {
            if let ResultColumn::Expr { expr, .. } = col {
                self.extract_params_expr(expr);
            }
        }

        // FROM clause
        if let Some(from) = &core.from {
            self.extract_params_from(from);
        }

        // WHERE clause
        if let Some(where_clause) = &core.where_clause {
            self.extract_params_expr(where_clause);
        }

        // GROUP BY
        if let Some(group_by) = &core.group_by {
            for expr in group_by {
                self.extract_params_expr(expr);
            }
        }

        // HAVING
        if let Some(having) = &core.having {
            self.extract_params_expr(having);
        }
    }

    fn extract_params_from(&mut self, from: &FromClause) {
        for table_ref in &from.tables {
            self.extract_params_table_ref(table_ref);
        }
    }

    fn extract_params_table_ref(&mut self, table_ref: &TableRef) {
        match table_ref {
            TableRef::Subquery { query, .. } => {
                self.extract_params_select(query);
            }
            TableRef::Join {
                left,
                right,
                constraint,
                ..
            } => {
                self.extract_params_table_ref(left);
                self.extract_params_table_ref(right);
                if let Some(JoinConstraint::On(on_expr)) = constraint {
                    self.extract_params_expr(on_expr);
                }
            }
            TableRef::TableFunction { args, .. } => {
                for arg in args {
                    self.extract_params_expr(arg);
                }
            }
            TableRef::Parens(inner) => {
                self.extract_params_table_ref(inner);
            }
            _ => {}
        }
    }

    fn extract_params_insert(&mut self, insert: &InsertStmt) {
        match &insert.source {
            InsertSource::Values(rows) => {
                for row in rows {
                    for expr in row {
                        self.extract_params_expr(expr);
                    }
                }
            }
            InsertSource::Select(select) => {
                self.extract_params_select(select);
            }
            InsertSource::DefaultValues => {}
        }

        // ON CONFLICT DO UPDATE
        if let Some(on_conflict) = &insert.on_conflict {
            if let ConflictResolution::Update {
                assignments,
                where_clause,
            } = &on_conflict.action
            {
                for assign in assignments {
                    self.extract_params_expr(&assign.expr);
                }
                if let Some(where_expr) = where_clause {
                    self.extract_params_expr(where_expr);
                }
            }
        }

        // RETURNING
        if let Some(returning) = &insert.returning {
            for col in returning {
                if let ResultColumn::Expr { expr, .. } = col {
                    self.extract_params_expr(expr);
                }
            }
        }
    }

    fn extract_params_update(&mut self, update: &UpdateStmt) {
        for assign in &update.assignments {
            self.extract_params_expr(&assign.expr);
        }

        if let Some(where_clause) = &update.where_clause {
            self.extract_params_expr(where_clause);
        }

        if let Some(returning) = &update.returning {
            for col in returning {
                if let ResultColumn::Expr { expr, .. } = col {
                    self.extract_params_expr(expr);
                }
            }
        }
    }

    fn extract_params_delete(&mut self, delete: &DeleteStmt) {
        if let Some(where_clause) = &delete.where_clause {
            self.extract_params_expr(where_clause);
        }

        if let Some(returning) = &delete.returning {
            for col in returning {
                if let ResultColumn::Expr { expr, .. } = col {
                    self.extract_params_expr(expr);
                }
            }
        }
    }

    fn extract_params_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Variable(var) => {
                match var {
                    Variable::Numbered(num) => {
                        if let Some(idx) = num {
                            // Numbered parameter like ?1
                            while self.param_count < *idx {
                                self.param_count += 1;
                                self.param_names.push(None);
                            }
                        } else {
                            // Simple ? parameter
                            self.param_count += 1;
                            self.param_names.push(None);
                        }
                    }
                    Variable::Named { prefix, name } => {
                        let full_name = format!("{}{}", prefix, name);
                        if !self.named_params.contains(&full_name) {
                            self.named_params.insert(full_name.clone());
                            self.param_count += 1;
                            self.param_names.push(Some(full_name));
                        }
                    }
                }
            }

            Expr::Binary { left, right, .. } => {
                self.extract_params_expr(left);
                self.extract_params_expr(right);
            }

            Expr::Unary { expr, .. } => {
                self.extract_params_expr(expr);
            }

            Expr::Between {
                expr, low, high, ..
            } => {
                self.extract_params_expr(expr);
                self.extract_params_expr(low);
                self.extract_params_expr(high);
            }

            Expr::In { expr, list, .. } => {
                self.extract_params_expr(expr);
                match list {
                    InList::Values(values) => {
                        for v in values {
                            self.extract_params_expr(v);
                        }
                    }
                    InList::Subquery(select) => {
                        self.extract_params_select(select);
                    }
                    _ => {}
                }
            }

            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.extract_params_expr(expr);
                self.extract_params_expr(pattern);
                if let Some(escape) = escape {
                    self.extract_params_expr(escape);
                }
            }

            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    self.extract_params_expr(op);
                }
                for when_clause in when_clauses {
                    self.extract_params_expr(&when_clause.when);
                    self.extract_params_expr(&when_clause.then);
                }
                if let Some(else_expr) = else_clause {
                    self.extract_params_expr(else_expr);
                }
            }

            Expr::Function(func) => {
                if let FunctionArgs::Exprs(exprs) = &func.args {
                    for arg in exprs {
                        self.extract_params_expr(arg);
                    }
                }
                if let Some(filter) = &func.filter {
                    self.extract_params_expr(filter);
                }
                if let Some(Over::Spec(spec)) = &func.over {
                    if let Some(partition) = &spec.partition_by {
                        for p in partition {
                            self.extract_params_expr(p);
                        }
                    }
                    if let Some(order) = &spec.order_by {
                        for o in order {
                            self.extract_params_expr(&o.expr);
                        }
                    }
                }
            }

            Expr::Subquery(select) => {
                self.extract_params_select(select);
            }

            Expr::Exists { subquery, .. } => {
                self.extract_params_select(subquery);
            }

            Expr::Cast { expr, .. } => {
                self.extract_params_expr(expr);
            }

            Expr::Collate { expr, .. } => {
                self.extract_params_expr(expr);
            }

            Expr::IsNull { expr, .. } => {
                self.extract_params_expr(expr);
            }

            Expr::IsDistinct { left, right, .. } => {
                self.extract_params_expr(left);
                self.extract_params_expr(right);
            }

            Expr::Parens(inner) => {
                self.extract_params_expr(inner);
            }

            // Literals and column refs have no parameters
            _ => {}
        }
    }

    // ========================================================================
    // Column Extraction
    // ========================================================================

    /// Extract column names and types from a SELECT statement
    fn extract_select_columns(&self, select: &SelectStmt) -> (Vec<String>, Vec<ColumnType>) {
        let mut names = Vec::new();
        let mut types = Vec::new();

        if let SelectBody::Select(core) = &select.body {
            for (i, col) in core.columns.iter().enumerate() {
                match col {
                    ResultColumn::Star => {
                        names.push(format!("column{}", i));
                        types.push(ColumnType::Null);
                    }
                    ResultColumn::TableStar(table) => {
                        names.push(format!("{}.*", table));
                        types.push(ColumnType::Null);
                    }
                    ResultColumn::Expr { expr, alias } => {
                        let name = if let Some(alias) = alias {
                            alias.clone()
                        } else {
                            self.expr_name(expr, i)
                        };
                        names.push(name);
                        types.push(self.infer_type(expr));
                    }
                }
            }
        }

        (names, types)
    }

    /// Get a name for an expression
    fn expr_name(&self, expr: &Expr, index: usize) -> String {
        match expr {
            Expr::Column(col) => col.column.clone(),
            Expr::Literal(lit) => format!("{:?}", lit),
            Expr::Function(func) => func.name.clone(),
            _ => format!("column{}", index),
        }
    }

    /// Infer the type of an expression
    fn infer_type(&self, expr: &Expr) -> ColumnType {
        match expr {
            Expr::Literal(lit) => match lit {
                Literal::Integer(_) => ColumnType::Integer,
                Literal::Float(_) => ColumnType::Float,
                Literal::String(_) => ColumnType::Text,
                Literal::Blob(_) => ColumnType::Blob,
                Literal::Null => ColumnType::Null,
                Literal::Bool(_) => ColumnType::Integer,
                Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp => {
                    ColumnType::Text
                }
            },
            Expr::Function(func) => match func.name.to_uppercase().as_str() {
                "COUNT" | "LENGTH" | "INSTR" | "UNICODE" => ColumnType::Integer,
                "SUM" | "AVG" | "TOTAL" | "ABS" | "ROUND" => ColumnType::Float,
                "UPPER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "REPLACE" | "SUBSTR"
                | "TYPEOF" | "HEX" | "QUOTE" | "GROUP_CONCAT" => ColumnType::Text,
                "ZEROBLOB" | "RANDOMBLOB" => ColumnType::Blob,
                _ => ColumnType::Null,
            },
            Expr::Cast { type_name, .. } => match type_name.name.to_uppercase().as_str() {
                "INTEGER" | "INT" => ColumnType::Integer,
                "REAL" | "FLOAT" | "DOUBLE" => ColumnType::Float,
                "TEXT" | "VARCHAR" | "CHAR" => ColumnType::Text,
                "BLOB" => ColumnType::Blob,
                _ => ColumnType::Null,
            },
            _ => ColumnType::Null,
        }
    }

    // ========================================================================
    // Helper for creating VdbeOp
    // ========================================================================

    fn make_op(opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: P4) -> VdbeOp {
        VdbeOp {
            opcode,
            p1,
            p2,
            p3,
            p4,
            p5: 0,
            comment: None,
        }
    }

    // ========================================================================
    // Schema Statement Compilation
    // ========================================================================

    fn compile_create_table(&mut self, create: &CreateTableStmt) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
        ops.push(Self::make_op(
            Opcode::Noop,
            0,
            0,
            0,
            P4::Text(format!("CREATE TABLE {}", create.name)),
        ));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    fn compile_create_index(&mut self, create: &CreateIndexStmt) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
        ops.push(Self::make_op(
            Opcode::Noop,
            0,
            0,
            0,
            P4::Text(format!("CREATE INDEX {}", create.name)),
        ));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    fn compile_create_view(&mut self, create: &CreateViewStmt) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
        ops.push(Self::make_op(
            Opcode::Noop,
            0,
            0,
            0,
            P4::Text(format!("CREATE VIEW {}", create.name)),
        ));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    fn compile_create_trigger(&mut self, create: &CreateTriggerStmt) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
        ops.push(Self::make_op(
            Opcode::Noop,
            0,
            0,
            0,
            P4::Text(format!("CREATE TRIGGER {}", create.name)),
        ));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    fn compile_drop(&mut self, drop: &DropStmt, kind: &str) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
        ops.push(Self::make_op(
            Opcode::Noop,
            0,
            0,
            0,
            P4::Text(format!("DROP {} {}", kind.to_uppercase(), drop.name)),
        ));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    fn compile_alter_table(&mut self, alter: &AlterTableStmt) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
        ops.push(Self::make_op(
            Opcode::Noop,
            0,
            0,
            0,
            P4::Text(format!("ALTER TABLE {}", alter.table)),
        ));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    // ========================================================================
    // Transaction Statement Compilation
    // ========================================================================

    fn compile_begin(&mut self, begin: &BeginStmt) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));

        let p2 = match begin.mode {
            Some(TransactionMode::Deferred) | None => 0,
            Some(TransactionMode::Immediate) => 1,
            Some(TransactionMode::Exclusive) => 2,
        };

        ops.push(Self::make_op(Opcode::Transaction, 0, p2, 0, P4::Unused));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    fn compile_commit(&mut self) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
        ops.push(Self::make_op(Opcode::AutoCommit, 1, 0, 0, P4::Unused));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    fn compile_rollback(&mut self, rollback: &RollbackStmt) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));

        if let Some(savepoint) = &rollback.savepoint {
            ops.push(Self::make_op(
                Opcode::Savepoint,
                2,
                0,
                0,
                P4::Text(savepoint.clone()),
            ));
        } else {
            ops.push(Self::make_op(Opcode::AutoCommit, 2, 0, 0, P4::Unused));
        }

        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    fn compile_savepoint(&mut self, name: &str) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
        ops.push(Self::make_op(
            Opcode::Savepoint,
            0,
            0,
            0,
            P4::Text(name.to_string()),
        ));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    fn compile_release(&mut self, name: &str) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
        ops.push(Self::make_op(
            Opcode::Savepoint,
            1,
            0,
            0,
            P4::Text(name.to_string()),
        ));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    // ========================================================================
    // PRAGMA Compilation
    // ========================================================================

    fn compile_pragma(
        &mut self,
        pragma: &PragmaStmt,
    ) -> Result<(Vec<VdbeOp>, Vec<String>, Vec<ColumnType>)> {
        let mut ops = Vec::new();
        let mut names = Vec::new();
        let mut types = Vec::new();

        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));

        let value_str = pragma.value.as_ref().map(|v| match v {
            PragmaValue::Set(_) => "=...".to_string(),
            PragmaValue::Call(_) => "(...)".to_string(),
        });

        ops.push(Self::make_op(
            Opcode::Noop,
            0,
            0,
            0,
            P4::Text(format!(
                "PRAGMA {}{}",
                pragma.name,
                value_str.map(|v| format!(" = {}", v)).unwrap_or_default()
            )),
        ));

        if pragma.value.is_none() {
            names.push(pragma.name.clone());
            types.push(ColumnType::Text);
        }

        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok((ops, names, types))
    }

    // ========================================================================
    // Utility Statement Compilation
    // ========================================================================

    fn compile_vacuum(&mut self, vacuum: &VacuumStmt) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
        ops.push(Self::make_op(
            Opcode::Noop,
            0,
            0,
            0,
            P4::Text(format!(
                "VACUUM{}",
                vacuum
                    .schema
                    .as_ref()
                    .map(|s| format!(" {}", s))
                    .unwrap_or_default()
            )),
        ));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    fn compile_analyze(&mut self, table: Option<&QualifiedName>) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
        ops.push(Self::make_op(
            Opcode::Noop,
            0,
            0,
            0,
            P4::Text(format!(
                "ANALYZE{}",
                table.map(|t| format!(" {}", t)).unwrap_or_default()
            )),
        ));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    fn compile_reindex(&mut self, table: Option<&QualifiedName>) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
        ops.push(Self::make_op(
            Opcode::Noop,
            0,
            0,
            0,
            P4::Text(format!(
                "REINDEX{}",
                table.map(|t| format!(" {}", t)).unwrap_or_default()
            )),
        ));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    fn compile_attach(&mut self, attach: &AttachStmt) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
        ops.push(Self::make_op(
            Opcode::Noop,
            0,
            0,
            0,
            P4::Text(format!("ATTACH ... AS {}", attach.schema)),
        ));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    fn compile_detach(&mut self, name: &str) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
        ops.push(Self::make_op(
            Opcode::Noop,
            0,
            0,
            0,
            P4::Text(format!("DETACH {}", name)),
        ));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    // ========================================================================
    // EXPLAIN Compilation
    // ========================================================================

    fn wrap_explain(&mut self, inner_ops: Vec<VdbeOp>) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 2, 0, P4::Unused));

        let base_reg = 1;
        for (i, op) in inner_ops.iter().enumerate() {
            // addr
            ops.push(Self::make_op(
                Opcode::Integer,
                i as i32,
                base_reg,
                0,
                P4::Unused,
            ));
            // opcode name
            ops.push(Self::make_op(
                Opcode::String8,
                0,
                base_reg + 1,
                0,
                P4::Text(format!("{:?}", op.opcode)),
            ));
            // p1, p2, p3
            ops.push(Self::make_op(
                Opcode::Integer,
                op.p1,
                base_reg + 2,
                0,
                P4::Unused,
            ));
            ops.push(Self::make_op(
                Opcode::Integer,
                op.p2,
                base_reg + 3,
                0,
                P4::Unused,
            ));
            ops.push(Self::make_op(
                Opcode::Integer,
                op.p3,
                base_reg + 4,
                0,
                P4::Unused,
            ));
            // p4
            ops.push(Self::make_op(
                Opcode::String8,
                0,
                base_reg + 5,
                0,
                P4::Text(format!("{:?}", op.p4)),
            ));
            // p5
            ops.push(Self::make_op(
                Opcode::Integer,
                op.p5 as i32,
                base_reg + 6,
                0,
                P4::Unused,
            ));
            // comment
            ops.push(Self::make_op(
                Opcode::String8,
                0,
                base_reg + 7,
                0,
                P4::Text(op.comment.clone().unwrap_or_default()),
            ));
            // Result row
            ops.push(Self::make_op(Opcode::ResultRow, base_reg, 8, 0, P4::Unused));
        }

        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    fn compile_explain_query_plan(&mut self) -> Result<Vec<VdbeOp>> {
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));

        let base_reg = 1;
        ops.push(Self::make_op(Opcode::Integer, 0, base_reg, 0, P4::Unused));
        ops.push(Self::make_op(
            Opcode::Integer,
            0,
            base_reg + 1,
            0,
            P4::Unused,
        ));
        ops.push(Self::make_op(
            Opcode::Integer,
            0,
            base_reg + 2,
            0,
            P4::Unused,
        ));
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            base_reg + 3,
            0,
            P4::Text("SCAN TABLE".to_string()),
        ));
        ops.push(Self::make_op(Opcode::ResultRow, base_reg, 4, 0, P4::Unused));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));

        Ok(ops)
    }
}

impl Default for StatementCompiler {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Find the remaining SQL after the first statement
fn find_statement_tail(sql: &str) -> &str {
    let bytes = sql.as_bytes();
    let mut in_string = false;
    let mut string_char = b'\0';

    for (i, &c) in bytes.iter().enumerate() {
        if in_string {
            if c == string_char {
                in_string = false;
            }
        } else {
            match c {
                b'\'' | b'"' => {
                    in_string = true;
                    string_char = c;
                }
                b';' => {
                    // Found statement end - return everything after
                    return &sql[i + 1..];
                }
                _ => {}
            }
        }
    }

    // No semicolon found
    ""
}

/// Compile SQL to VDBE bytecode
///
/// Returns the compiled statement and any remaining SQL (tail).
pub fn compile_sql<'a>(sql: &'a str) -> Result<(CompiledStmt, &'a str)> {
    let mut compiler = StatementCompiler::new();
    compiler.compile(sql)
}

/// Parse SQL without compiling (for validation)
pub fn parse_sql(sql: &str) -> Result<Stmt> {
    let mut parser = Parser::new(sql)?;
    parser.parse_stmt()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile_select() {
        let (compiled, tail) = compile_sql("SELECT 1").unwrap();
        assert!(tail.is_empty());
        assert_eq!(compiled.stmt_type, StmtType::Select);
        assert!(compiled.read_only);
        assert!(!compiled.ops.is_empty());
    }

    #[test]
    fn test_compile_insert() {
        let (compiled, _) = compile_sql("INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(compiled.stmt_type, StmtType::Insert);
        assert!(!compiled.read_only);
    }

    #[test]
    fn test_compile_update() {
        let (compiled, _) = compile_sql("UPDATE t SET x = 1").unwrap();
        assert_eq!(compiled.stmt_type, StmtType::Update);
        assert!(!compiled.read_only);
    }

    #[test]
    fn test_compile_delete() {
        let (compiled, _) = compile_sql("DELETE FROM t").unwrap();
        assert_eq!(compiled.stmt_type, StmtType::Delete);
        assert!(!compiled.read_only);
    }

    #[test]
    fn test_parameter_extraction() {
        let (compiled, _) = compile_sql("SELECT ? WHERE x = ?").unwrap();
        assert_eq!(compiled.param_count, 2);
    }

    #[test]
    fn test_named_parameters() {
        let (compiled, _) = compile_sql("SELECT :name WHERE x = :value").unwrap();
        assert_eq!(compiled.param_count, 2);
        assert!(compiled
            .param_names
            .iter()
            .any(|n| n.as_deref() == Some(":name")));
        assert!(compiled
            .param_names
            .iter()
            .any(|n| n.as_deref() == Some(":value")));
    }

    #[test]
    fn test_compile_begin() {
        let (compiled, _) = compile_sql("BEGIN").unwrap();
        assert_eq!(compiled.stmt_type, StmtType::Begin);
        assert!(compiled.read_only);
    }

    #[test]
    fn test_compile_commit() {
        let (compiled, _) = compile_sql("COMMIT").unwrap();
        assert_eq!(compiled.stmt_type, StmtType::Commit);
    }

    #[test]
    fn test_compile_rollback() {
        let (compiled, _) = compile_sql("ROLLBACK").unwrap();
        assert_eq!(compiled.stmt_type, StmtType::Rollback);
    }

    #[test]
    fn test_compile_explain() {
        let (compiled, _) = compile_sql("EXPLAIN SELECT 1").unwrap();
        assert_eq!(compiled.stmt_type, StmtType::Explain);
        assert_eq!(compiled.column_names.len(), 8);
    }

    #[test]
    fn test_compile_explain_query_plan() {
        let (compiled, _) = compile_sql("EXPLAIN QUERY PLAN SELECT 1").unwrap();
        assert_eq!(compiled.stmt_type, StmtType::ExplainQueryPlan);
        assert_eq!(compiled.column_names.len(), 4);
    }

    #[test]
    fn test_column_extraction() {
        let (compiled, _) = compile_sql("SELECT a, b AS alias, 1 + 2").unwrap();
        assert!(compiled.column_names.len() >= 1);
    }
}
