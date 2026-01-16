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
use super::insert::{compile_insert, compile_insert_with_schema};
use super::select::{SelectCompiler, SelectDest};
use super::update::{compile_update, compile_update_with_schema};

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
    CreateVirtualTable,
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
pub struct StatementCompiler<'s> {
    /// Parameter counter for extraction
    param_count: i32,
    /// Parameter names found
    param_names: Vec<Option<String>>,
    /// Named parameters seen (for deduplication)
    named_params: HashSet<String>,
    /// Schema for name resolution (optional)
    schema: Option<&'s crate::schema::Schema>,
}

impl<'s> StatementCompiler<'s> {
    /// Create a new statement compiler
    pub fn new() -> Self {
        Self {
            param_count: 0,
            param_names: Vec::new(),
            named_params: HashSet::new(),
            schema: None,
        }
    }

    /// Create a new statement compiler with schema access
    pub fn with_schema(schema: &'s crate::schema::Schema) -> Self {
        Self {
            param_count: 0,
            param_names: Vec::new(),
            named_params: HashSet::new(),
            schema: Some(schema),
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
                let mut compiler = if let Some(schema) = self.schema {
                    SelectCompiler::with_schema(schema)
                } else {
                    SelectCompiler::new()
                };
                let ops = compiler.compile(select, &SelectDest::Output)?;
                // Use column names from compiler (properly expanded for Star)
                let names = if compiler.column_names().is_empty() {
                    // Fallback to extracting from AST if compiler didn't populate names
                    self.extract_select_columns(select).0
                } else {
                    compiler.column_names().to_vec()
                };
                let (_, types) = self.extract_select_columns(select);
                Ok((ops, StmtType::Select, names, types))
            }

            Stmt::Insert(insert) => {
                let ops = if let Some(schema) = self.schema {
                    compile_insert_with_schema(insert, schema)?
                } else {
                    compile_insert(insert)?
                };
                Ok((ops, StmtType::Insert, Vec::new(), Vec::new()))
            }

            Stmt::Update(update) => {
                let ops = if let Some(schema) = self.schema {
                    compile_update_with_schema(update, schema)?
                } else {
                    compile_update(update)?
                };
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

            Stmt::CreateVirtualTable(create) => {
                let ops = self.compile_create_virtual_table(create)?;
                Ok((ops, StmtType::CreateVirtualTable, Vec::new(), Vec::new()))
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
            Stmt::Attach(a) => self.extract_params_expr(&a.expr),
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
        use crate::storage::btree::BTREE_INTKEY;

        let mut ops = Vec::new();

        // Register allocation
        let reg_root_page = 1; // root page number for new table

        // 0: Init - jump to start of program
        ops.push(Self::make_op(Opcode::Init, 0, 2, 0, P4::Unused));

        // 1: Halt - end of program
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));

        // 2: CreateBtree - create the table's root page
        // P1=0 (main database), P2=register for root page, P3=BTREE_INTKEY for table
        ops.push(Self::make_op(
            Opcode::CreateBtree,
            0,
            reg_root_page,
            BTREE_INTKEY as i32,
            P4::Unused,
        ));

        // Build the CREATE TABLE SQL for the schema
        let create_sql = self.build_create_table_sql(create);

        // 3: ParseSchema - parse the CREATE statement and add to schema
        // P4 contains the SQL text
        ops.push(Self::make_op(
            Opcode::ParseSchema,
            0,
            reg_root_page, // root page register
            0,
            P4::Text(create_sql.clone()),
        ));

        let cursor_id = 0;
        self.append_sqlite_master_open(&mut ops, cursor_id);
        self.append_sqlite_master_insert(
            &mut ops,
            cursor_id,
            &create.name.name,
            reg_root_page,
            &create_sql,
        );
        self.append_sqlite_master_close(&mut ops, cursor_id);

        // 4: Goto end
        ops.push(Self::make_op(Opcode::Goto, 0, 1, 0, P4::Unused));

        Ok(ops)
    }

    fn compile_create_virtual_table(
        &mut self,
        create: &CreateVirtualTableStmt,
    ) -> Result<Vec<VdbeOp>> {
        use crate::storage::btree::BTREE_INTKEY;

        let mut ops = Vec::new();

        let reg_root_page = 1;
        ops.push(Self::make_op(Opcode::Init, 0, 2, 0, P4::Unused));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        let sqlite_master_cursor = 0;
        self.append_sqlite_master_open(&mut ops, sqlite_master_cursor);

        if create.module.eq_ignore_ascii_case("fts3") {
            let shadow_tables = self.build_fts3_shadow_tables(create);
            for (table_name, sql) in shadow_tables {
                ops.push(Self::make_op(
                    Opcode::CreateBtree,
                    0,
                    reg_root_page,
                    BTREE_INTKEY as i32,
                    P4::Unused,
                ));
                ops.push(Self::make_op(
                    Opcode::ParseSchema,
                    0,
                    reg_root_page,
                    0,
                    P4::Text(sql.clone()),
                ));
                self.append_sqlite_master_insert(
                    &mut ops,
                    sqlite_master_cursor,
                    &table_name,
                    reg_root_page,
                    &sql,
                );
            }
        }
        if create.module.eq_ignore_ascii_case("fts5") {
            let shadow_tables = self.build_fts5_shadow_tables(create);
            for (table_name, sql) in shadow_tables {
                ops.push(Self::make_op(
                    Opcode::CreateBtree,
                    0,
                    reg_root_page,
                    BTREE_INTKEY as i32,
                    P4::Unused,
                ));
                ops.push(Self::make_op(
                    Opcode::ParseSchema,
                    0,
                    reg_root_page,
                    0,
                    P4::Text(sql.clone()),
                ));
                self.append_sqlite_master_insert(
                    &mut ops,
                    sqlite_master_cursor,
                    &table_name,
                    reg_root_page,
                    &sql,
                );
            }
        }

        ops.push(Self::make_op(
            Opcode::Integer,
            0,
            reg_root_page,
            0,
            P4::Unused,
        ));

        let create_sql = self.build_create_virtual_table_sql(create);
        ops.push(Self::make_op(
            Opcode::ParseSchema,
            0,
            reg_root_page,
            0,
            P4::Text(create_sql.clone()),
        ));
        self.append_sqlite_master_insert(
            &mut ops,
            sqlite_master_cursor,
            &create.name.name,
            reg_root_page,
            &create_sql,
        );
        self.append_sqlite_master_close(&mut ops, sqlite_master_cursor);
        ops.push(Self::make_op(Opcode::Goto, 0, 1, 0, P4::Unused));

        Ok(ops)
    }

    /// Build CREATE TABLE SQL from AST for storage in schema
    fn build_create_table_sql(&self, create: &CreateTableStmt) -> String {
        use crate::parser::ast::TableDefinition;

        let mut sql = String::from("CREATE TABLE ");
        if create.if_not_exists {
            sql.push_str("IF NOT EXISTS ");
        }
        sql.push_str(&create.name.name);
        sql.push_str(" (");

        if let TableDefinition::Columns { columns, .. } = &create.definition {
            let col_defs: Vec<String> = columns
                .iter()
                .map(|col| {
                    let mut col_sql = col.name.clone();
                    if let Some(ref type_name) = col.type_name {
                        col_sql.push(' ');
                        col_sql.push_str(&type_name.name);
                    }
                    col_sql
                })
                .collect();
            sql.push_str(&col_defs.join(", "));
        }
        sql.push(')');
        sql
    }

    fn build_create_virtual_table_sql(&self, create: &CreateVirtualTableStmt) -> String {
        let mut sql = String::from("CREATE VIRTUAL TABLE ");
        if create.if_not_exists {
            sql.push_str("IF NOT EXISTS ");
        }
        sql.push_str(&create.name.name);
        sql.push_str(" USING ");
        sql.push_str(&create.module);
        if !create.args.is_empty() {
            sql.push('(');
            sql.push_str(&create.args.join(", "));
            sql.push(')');
        }
        sql
    }

    fn build_fts3_shadow_tables(&self, create: &CreateVirtualTableStmt) -> Vec<(String, String)> {
        let (columns, has_content, internal_content) =
            self.parse_fts3_virtual_columns(&create.args);
        let mut tables = Vec::new();
        let name = &create.name.name;

        if has_content && internal_content {
            let table_name = format!("{}_content", name);
            let mut sql = format!("CREATE TABLE {} (docid INTEGER PRIMARY KEY", table_name);
            for column in &columns {
                sql.push_str(", ");
                sql.push_str(column);
            }
            sql.push(')');
            tables.push((table_name, sql));
        }

        let segments_name = format!("{}_segments", name);
        tables.push((
            segments_name.clone(),
            format!(
                "CREATE TABLE {} (blockid INTEGER PRIMARY KEY, block BLOB)",
                segments_name
            ),
        ));
        let segdir_name = format!("{}_segdir", name);
        tables.push((
            segdir_name.clone(),
            format!(
                "CREATE TABLE {} (level INTEGER, idx INTEGER, start_block INTEGER, leaves_end_block INTEGER, end_block INTEGER, root BLOB)",
                segdir_name
            ),
        ));
        let stat_name = format!("{}_stat", name);
        tables.push((
            stat_name.clone(),
            format!(
                "CREATE TABLE {} (id INTEGER PRIMARY KEY, value BLOB)",
                stat_name
            ),
        ));

        tables
    }

    fn build_fts5_shadow_tables(&self, create: &CreateVirtualTableStmt) -> Vec<(String, String)> {
        let (columns, has_content, internal_content) =
            self.parse_fts5_virtual_columns(&create.args);
        let mut tables = Vec::new();
        let name = &create.name.name;

        if has_content && internal_content {
            let table_name = format!("{}_content", name);
            let mut sql = format!("CREATE TABLE {} (id INTEGER PRIMARY KEY", table_name);
            for (idx, _) in columns.iter().enumerate() {
                sql.push_str(", c");
                sql.push_str(&idx.to_string());
            }
            sql.push(')');
            tables.push((table_name, sql));
        }

        let data_name = format!("{}_data", name);
        tables.push((
            data_name.clone(),
            format!(
                "CREATE TABLE {} (id INTEGER PRIMARY KEY, block BLOB)",
                data_name
            ),
        ));
        let idx_name = format!("{}_idx", name);
        tables.push((
            idx_name.clone(),
            format!("CREATE TABLE {} (segid, term, pgno)", idx_name),
        ));
        let docsize_name = format!("{}_docsize", name);
        tables.push((
            docsize_name.clone(),
            format!(
                "CREATE TABLE {} (id INTEGER PRIMARY KEY, sz BLOB)",
                docsize_name
            ),
        ));
        let config_name = format!("{}_config", name);
        tables.push((
            config_name.clone(),
            format!("CREATE TABLE {} (k PRIMARY KEY, v)", config_name),
        ));

        tables
    }

    fn parse_fts3_virtual_columns(&self, args: &[String]) -> (Vec<String>, bool, bool) {
        let mut columns = Vec::new();
        let mut has_content = true;
        let mut internal_content = true;
        let mut pending_prefix = false;

        for arg in args {
            let trimmed = arg.trim();
            if let Some(value) = trimmed.strip_prefix("content=") {
                let value = value.trim();
                if value.eq_ignore_ascii_case("none") {
                    has_content = false;
                    internal_content = false;
                } else {
                    has_content = true;
                    internal_content = false;
                }
            } else if let Some(value) = trimmed.strip_prefix("CONTENT=") {
                let value = value.trim();
                if value.eq_ignore_ascii_case("none") {
                    has_content = false;
                    internal_content = false;
                } else {
                    has_content = true;
                    internal_content = false;
                }
            } else if trimmed.starts_with("prefix=") || trimmed.starts_with("PREFIX=") {
                pending_prefix = true;
            } else if trimmed.starts_with("tokenize=") || trimmed.starts_with("TOKENIZE=") {
                continue;
            } else if pending_prefix {
                if trimmed.parse::<i32>().is_ok() {
                    continue;
                }
                pending_prefix = false;
                if !trimmed.contains('=') {
                    columns.push(trimmed.to_string());
                }
            } else if !trimmed.contains('=') {
                columns.push(trimmed.to_string());
            }
        }

        (columns, has_content, internal_content)
    }

    fn parse_fts5_virtual_columns(&self, args: &[String]) -> (Vec<String>, bool, bool) {
        let mut columns = Vec::new();
        let mut has_content = true;
        let mut internal_content = true;
        let mut pending_prefix = false;

        for arg in args {
            let trimmed = arg.trim();
            if let Some(value) = trimmed.strip_prefix("content=") {
                let value = value.trim();
                if value.eq_ignore_ascii_case("none") {
                    has_content = false;
                    internal_content = false;
                } else {
                    has_content = true;
                    internal_content = false;
                }
            } else if let Some(value) = trimmed.strip_prefix("CONTENT=") {
                let value = value.trim();
                if value.eq_ignore_ascii_case("none") {
                    has_content = false;
                    internal_content = false;
                } else {
                    has_content = true;
                    internal_content = false;
                }
            } else if trimmed.starts_with("prefix=") || trimmed.starts_with("PREFIX=") {
                pending_prefix = true;
            } else if trimmed.starts_with("tokenize=") || trimmed.starts_with("TOKENIZE=") {
                continue;
            } else if pending_prefix {
                if trimmed.parse::<i32>().is_ok() {
                    continue;
                }
                pending_prefix = false;
                if !trimmed.contains('=') {
                    columns.push(trimmed.to_string());
                }
            } else if !trimmed.contains('=') {
                columns.push(trimmed.to_string());
            }
        }

        (columns, has_content, internal_content)
    }

    fn append_sqlite_master_open(&self, ops: &mut Vec<VdbeOp>, cursor_id: i32) {
        ops.push(Self::make_op(
            Opcode::OpenWrite,
            cursor_id,
            1,
            5,
            P4::Text("sqlite_master".to_string()),
        ));
    }

    fn append_sqlite_master_close(&self, ops: &mut Vec<VdbeOp>, cursor_id: i32) {
        ops.push(Self::make_op(Opcode::Close, cursor_id, 0, 0, P4::Unused));
    }

    fn append_sqlite_master_insert(
        &self,
        ops: &mut Vec<VdbeOp>,
        cursor_id: i32,
        table_name: &str,
        reg_root_page: i32,
        create_sql: &str,
    ) {
        let reg_type = 2;
        let reg_name = 3;
        let reg_tbl = 4;
        let reg_root = 5;
        let reg_sql = 6;
        let reg_record = 7;
        let reg_rowid = 8;
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_type,
            0,
            P4::Text("table".to_string()),
        ));
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_name,
            0,
            P4::Text(table_name.to_string()),
        ));
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_tbl,
            0,
            P4::Text(table_name.to_string()),
        ));
        ops.push(Self::make_op(
            Opcode::Copy,
            reg_root_page,
            reg_root,
            0,
            P4::Unused,
        ));
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_sql,
            0,
            P4::Text(create_sql.to_string()),
        ));
        ops.push(Self::make_op(
            Opcode::MakeRecord,
            reg_type,
            5,
            reg_record,
            P4::Unused,
        ));
        ops.push(Self::make_op(
            Opcode::NewRowid,
            cursor_id,
            reg_rowid,
            0,
            P4::Unused,
        ));
        ops.push(Self::make_op(
            Opcode::Insert,
            cursor_id,
            reg_record,
            reg_rowid,
            P4::Text("sqlite_master".to_string()),
        ));
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
        let table_name = &drop.name.name;
        let table_name_lower = table_name.to_lowercase();

        // Check for reserved names (sqlite_master, etc.) - cannot be dropped
        if table_name_lower.starts_with("sqlite_") {
            return Err(crate::error::Error::with_message(
                crate::error::ErrorCode::Error,
                format!("table {} may not be dropped", table_name),
            ));
        }

        // Check if table exists in schema
        if let Some(schema) = self.schema {
            if !schema.tables.contains_key(&table_name_lower) {
                if !drop.if_exists {
                    return Err(crate::error::Error::with_message(
                        crate::error::ErrorCode::Error,
                        format!("no such {}: {}", kind, table_name),
                    ));
                }
                // IF EXISTS specified and table doesn't exist - return no-op
                let mut ops = Vec::new();
                ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
                ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
                return Ok(ops);
            }
        }

        // Generate bytecode to drop the table
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 2, 0, P4::Unused));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        // DropSchema opcode to remove from schema
        ops.push(Self::make_op(
            Opcode::DropSchema,
            0,
            0,
            0,
            P4::Text(table_name.clone()),
        ));
        ops.push(Self::make_op(Opcode::Goto, 0, 1, 0, P4::Unused));
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
        ops.push(Self::make_op(Opcode::AutoCommit, 0, 0, 0, P4::Unused));
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
            ops.push(Self::make_op(Opcode::AutoCommit, 1, 1, 0, P4::Unused));
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

impl Default for StatementCompiler<'_> {
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
pub fn compile_sql(sql: &str) -> Result<(CompiledStmt, &str)> {
    let mut compiler = StatementCompiler::new();
    compiler.compile(sql)
}

/// Compile SQL to VDBE bytecode with schema access
///
/// Returns the compiled statement and any remaining SQL (tail).
/// The schema is used for name resolution (e.g., expanding SELECT *).
pub fn compile_sql_with_schema<'a>(
    sql: &'a str,
    schema: &crate::schema::Schema,
) -> Result<(CompiledStmt, &'a str)> {
    let mut compiler = StatementCompiler::with_schema(schema);
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
