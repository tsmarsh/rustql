//! Statement Preparation
//!
//! This module handles SQL parsing and compilation to VDBE bytecode.
//! Corresponds to SQLite's prepare.c - the interface between the parser
//! and the code generator.

use std::collections::{HashMap, HashSet};

use crate::error::Result;
use crate::parser::ast::*;
use crate::parser::grammar::Parser;
use crate::types::ColumnType;
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

use super::select::{SelectCompiler, SelectDest};
use super::where_clause::{IndexInfo, QueryPlanner, WherePlan, WhereTerm};

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

#[derive(Debug, Clone)]
struct ExplainTableInfo {
    name: String,
    alias: Option<String>,
    display_name: String,
    columns: Vec<String>,
    estimated_rows: i64,
    has_rowid: bool,
    indexed_by: Option<IndexedBy>,
    indexes: Vec<IndexInfo>,
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
    /// PRAGMA short_column_names (default ON)
    short_column_names: bool,
    /// PRAGMA full_column_names (default OFF)
    full_column_names: bool,
    /// LIKE case sensitivity (for LIKE index optimization)
    case_sensitive_like: bool,
}

impl<'s> StatementCompiler<'s> {
    /// Create a new statement compiler
    pub fn new() -> Self {
        Self {
            param_count: 0,
            param_names: Vec::new(),
            named_params: HashSet::new(),
            schema: None,
            short_column_names: true,
            full_column_names: false,
            case_sensitive_like: false,
        }
    }

    /// Create a new statement compiler with schema access
    pub fn with_schema(schema: &'s crate::schema::Schema) -> Self {
        Self {
            param_count: 0,
            param_names: Vec::new(),
            named_params: HashSet::new(),
            schema: Some(schema),
            short_column_names: true,
            full_column_names: false,
            case_sensitive_like: false,
        }
    }

    /// Set column naming flags from PRAGMA settings
    pub fn set_column_name_flags(&mut self, short_column_names: bool, full_column_names: bool) {
        self.short_column_names = short_column_names;
        self.full_column_names = full_column_names;
    }

    /// Set LIKE case sensitivity for index optimization
    pub fn set_case_sensitive_like(&mut self, value: bool) {
        self.case_sensitive_like = value;
    }

    /// Compile a SQL string to VDBE bytecode
    ///
    /// Returns the compiled statement and any remaining SQL (tail).
    pub fn compile<'a>(&mut self, sql: &'a str) -> Result<(CompiledStmt, &'a str)> {
        // Parse the SQL
        let mut parser = Parser::new(sql)?;
        let stmt = parser.parse_stmt()?;

        // Get the tail (remaining unparsed SQL) from the parser
        // This correctly handles complex statements like CREATE TRIGGER with BEGIN...END
        let tail = parser.remaining();

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
                // Pass column naming flags from PRAGMA settings
                compiler.set_column_name_flags(self.short_column_names, self.full_column_names);
                // Pass parameter names for Variable compilation
                compiler.set_param_names(self.param_names.clone());
                // Pass LIKE case sensitivity for index optimization
                compiler.set_case_sensitive_like(self.case_sensitive_like);
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
                let mut compiler = if let Some(schema) = self.schema {
                    super::insert::InsertCompiler::with_schema(schema)
                } else {
                    super::insert::InsertCompiler::new()
                };
                // Pass parameter names for Variable compilation
                compiler.set_param_names(self.param_names.clone());
                let ops = compiler.compile(insert)?;
                Ok((ops, StmtType::Insert, Vec::new(), Vec::new()))
            }

            Stmt::Update(update) => {
                let mut compiler = if let Some(schema) = self.schema {
                    super::update::UpdateCompiler::with_schema(schema)
                } else {
                    super::update::UpdateCompiler::new()
                };
                // Pass parameter names for Variable compilation
                compiler.set_param_names(self.param_names.clone());
                let ops = compiler.compile(update)?;
                Ok((ops, StmtType::Update, Vec::new(), Vec::new()))
            }

            Stmt::Delete(delete) => {
                let mut compiler = if let Some(schema) = self.schema {
                    super::delete::DeleteCompiler::with_schema(schema)
                } else {
                    super::delete::DeleteCompiler::new()
                };
                // Pass parameter names for Variable compilation
                compiler.set_param_names(self.param_names.clone());
                let ops = compiler.compile(delete)?;
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
                let ops = self.compile_explain_query_plan(inner)?;
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

    fn make_op_with_p5(opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: P4, p5: u16) -> VdbeOp {
        VdbeOp {
            opcode,
            p1,
            p2,
            p3,
            p4,
            p5,
            comment: None,
        }
    }

    // ========================================================================
    // Schema Statement Compilation
    // ========================================================================

    fn compile_create_table(&mut self, create: &CreateTableStmt) -> Result<Vec<VdbeOp>> {
        use crate::parser::ast::TableDefinition;
        use crate::storage::btree::BTREE_INTKEY;

        let mut ops = Vec::new();

        // Register allocation
        let reg_root_page = 1; // root page number for new table

        // 0: Init - jump to start of program
        ops.push(Self::make_op(Opcode::Init, 0, 2, 0, P4::Unused));

        // 1: Halt - end of program (placeholder, will be patched later if AsSelect)
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

        // Handle AsSelect case - need to also insert rows from SELECT
        if let TableDefinition::AsSelect(select) = &create.definition {
            // Get column count from the SELECT
            let select_cols = self.resolve_select_columns_for_create(select);
            let num_cols = select_cols.len() as i32;

            // Open the new table for writing (by name, it was just created)
            // Use cursor 1 since cursor 0 is used for sqlite_master
            let target_cursor = 1;
            ops.push(Self::make_op(
                Opcode::OpenWrite,
                target_cursor,
                0, // root page 0 = look up by name
                num_cols,
                P4::Text(create.name.name.clone()),
            ));

            // Compile the SELECT using SelectCompiler with Table destination
            let mut select_compiler = if let Some(schema) = self.schema {
                SelectCompiler::with_schema(schema)
            } else {
                SelectCompiler::new()
            };

            // Compile the SELECT with Table destination to insert directly
            let dest = SelectDest::Table {
                cursor: target_cursor,
            };
            let select_ops = select_compiler.compile(select, &dest)?;

            // Append select ops, adjusting jump targets
            let offset = ops.len() as i32;
            for op in select_ops {
                let mut new_op = op;
                // Patch jump targets to account for the offset
                // But skip Init since it's already at the start
                if new_op.opcode.is_jump() && new_op.p2 > 0 {
                    new_op.p2 += offset;
                }
                if new_op.opcode == Opcode::Init && new_op.p2 > 0 {
                    new_op.p2 += offset;
                }
                // Convert Halt to Close + Goto to our Halt at position 1
                if new_op.opcode == Opcode::Halt {
                    ops.push(Self::make_op(
                        Opcode::Close,
                        target_cursor,
                        0,
                        0,
                        P4::Unused,
                    ));
                    ops.push(Self::make_op(Opcode::Goto, 0, 1, 0, P4::Unused));
                    continue;
                }
                ops.push(new_op);
            }
        } else {
            // Regular CREATE TABLE - just Goto to the Halt
            ops.push(Self::make_op(Opcode::Goto, 0, 1, 0, P4::Unused));
        }

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

    /// Resolve column names from a SELECT statement for CREATE TABLE AS SELECT
    /// This handles star expansion using schema information
    fn resolve_select_columns_for_create(&self, select: &SelectStmt) -> Vec<(String, String)> {
        let mut columns = Vec::new();

        if let SelectBody::Select(core) = &select.body {
            // Get source table info for star expansion
            let from_tables: Vec<String> = if let Some(from) = &core.from {
                from.tables
                    .iter()
                    .filter_map(|t| match t {
                        TableRef::Table { name, .. } => Some(name.name.clone()),
                        _ => None,
                    })
                    .collect()
            } else {
                Vec::new()
            };

            for (i, col) in core.columns.iter().enumerate() {
                match col {
                    ResultColumn::Star => {
                        // Expand * using schema if available
                        if let Some(schema) = self.schema {
                            for table_name in &from_tables {
                                if let Some(table) = schema.table(table_name) {
                                    for col_def in &table.columns {
                                        let type_str =
                                            col_def.type_name.clone().unwrap_or_default();
                                        columns.push((col_def.name.clone(), type_str));
                                    }
                                }
                            }
                        }
                        if columns.is_empty() {
                            // Fallback if no schema
                            columns.push((format!("column{}", i), String::new()));
                        }
                    }
                    ResultColumn::TableStar(table) => {
                        // Expand table.* using schema
                        if let Some(schema) = self.schema {
                            if let Some(schema_table) = schema.table(table) {
                                for col_def in &schema_table.columns {
                                    let type_str = col_def.type_name.clone().unwrap_or_default();
                                    columns.push((col_def.name.clone(), type_str));
                                }
                            }
                        }
                        if columns.is_empty() {
                            columns.push((format!("{}_{}", table, i), String::new()));
                        }
                    }
                    ResultColumn::Expr { expr, alias } => {
                        let name = if let Some(alias) = alias {
                            alias.clone()
                        } else {
                            self.expr_name(expr, i)
                        };
                        let type_name = match self.infer_type(expr) {
                            ColumnType::Integer => "INTEGER",
                            ColumnType::Float => "REAL",
                            ColumnType::Text => "TEXT",
                            ColumnType::Blob => "BLOB",
                            _ => "",
                        };
                        columns.push((name, type_name.to_string()));
                    }
                }
            }
        }

        columns
    }

    /// Build CREATE TABLE SQL from AST for storage in schema
    fn build_create_table_sql(&self, create: &CreateTableStmt) -> String {
        use crate::parser::ast::{ColumnConstraintKind, TableConstraintKind, TableDefinition};

        let mut sql = String::from("CREATE TABLE ");
        if create.if_not_exists {
            sql.push_str("IF NOT EXISTS ");
        }
        sql.push_str(&create.name.name);
        sql.push_str(" (");

        match &create.definition {
            TableDefinition::Columns {
                columns,
                constraints,
            } => {
                let col_defs: Vec<String> = columns
                    .iter()
                    .map(|col| {
                        let mut col_sql = col.name.clone();
                        if let Some(ref type_name) = col.type_name {
                            col_sql.push(' ');
                            col_sql.push_str(&type_name.name);
                        }
                        // Add column constraints
                        for constraint in &col.constraints {
                            match &constraint.kind {
                                ColumnConstraintKind::PrimaryKey { autoincrement, .. } => {
                                    col_sql.push_str(" PRIMARY KEY");
                                    if *autoincrement {
                                        col_sql.push_str(" AUTOINCREMENT");
                                    }
                                }
                                ColumnConstraintKind::NotNull { .. } => {
                                    col_sql.push_str(" NOT NULL");
                                }
                                ColumnConstraintKind::Unique { .. } => {
                                    col_sql.push_str(" UNIQUE");
                                }
                                ColumnConstraintKind::Default(val) => {
                                    col_sql.push_str(" DEFAULT ");
                                    match val {
                                        crate::parser::ast::DefaultValue::Literal(lit) => match lit
                                        {
                                            crate::parser::ast::Literal::Null => {
                                                col_sql.push_str("NULL");
                                            }
                                            crate::parser::ast::Literal::Integer(n) => {
                                                col_sql.push_str(&n.to_string());
                                            }
                                            crate::parser::ast::Literal::Float(f) => {
                                                col_sql.push_str(&f.to_string());
                                            }
                                            crate::parser::ast::Literal::String(s) => {
                                                col_sql.push('\'');
                                                col_sql.push_str(&s.replace("'", "''"));
                                                col_sql.push('\'');
                                            }
                                            crate::parser::ast::Literal::Blob(_) => {
                                                col_sql.push_str("X''");
                                            }
                                            crate::parser::ast::Literal::Bool(b) => {
                                                col_sql.push_str(if *b { "1" } else { "0" });
                                            }
                                            crate::parser::ast::Literal::CurrentTime => {
                                                col_sql.push_str("current_time");
                                            }
                                            crate::parser::ast::Literal::CurrentDate => {
                                                col_sql.push_str("current_date");
                                            }
                                            crate::parser::ast::Literal::CurrentTimestamp => {
                                                col_sql.push_str("current_timestamp");
                                            }
                                        },
                                        crate::parser::ast::DefaultValue::Expr(_) => {
                                            col_sql.push_str("(expression)");
                                        }
                                        crate::parser::ast::DefaultValue::CurrentTime => {
                                            col_sql.push_str("current_time");
                                        }
                                        crate::parser::ast::DefaultValue::CurrentDate => {
                                            col_sql.push_str("current_date");
                                        }
                                        crate::parser::ast::DefaultValue::CurrentTimestamp => {
                                            col_sql.push_str("current_timestamp");
                                        }
                                    }
                                }
                                ColumnConstraintKind::Collate(name) => {
                                    col_sql.push_str(" COLLATE ");
                                    col_sql.push_str(name);
                                }
                                _ => {}
                            }
                        }
                        col_sql
                    })
                    .collect();
                sql.push_str(&col_defs.join(", "));

                // Add table-level constraints
                for constraint in constraints {
                    sql.push_str(", ");
                    if let Some(name) = &constraint.name {
                        sql.push_str("CONSTRAINT ");
                        sql.push_str(name);
                        sql.push(' ');
                    }
                    match &constraint.kind {
                        TableConstraintKind::PrimaryKey { columns, .. } => {
                            sql.push_str("PRIMARY KEY (");
                            let col_names: Vec<String> = columns
                                .iter()
                                .filter_map(|c| {
                                    if let crate::parser::ast::IndexedColumnKind::Name(name) =
                                        &c.column
                                    {
                                        Some(name.clone())
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            sql.push_str(&col_names.join(", "));
                            sql.push(')');
                        }
                        TableConstraintKind::Unique { columns, .. } => {
                            sql.push_str("UNIQUE (");
                            let col_names: Vec<String> = columns
                                .iter()
                                .filter_map(|c| {
                                    if let crate::parser::ast::IndexedColumnKind::Name(name) =
                                        &c.column
                                    {
                                        Some(name.clone())
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            sql.push_str(&col_names.join(", "));
                            sql.push(')');
                        }
                        TableConstraintKind::Check(expr) => {
                            sql.push_str("CHECK (");
                            sql.push_str(&format!("{:?}", expr));
                            sql.push(')');
                        }
                        TableConstraintKind::ForeignKey {
                            columns, clause, ..
                        } => {
                            sql.push_str("FOREIGN KEY (");
                            sql.push_str(&columns.join(", "));
                            sql.push_str(") REFERENCES ");
                            sql.push_str(&clause.table);
                            if let Some(ref_cols) = &clause.columns {
                                sql.push_str(" (");
                                sql.push_str(&ref_cols.join(", "));
                                sql.push(')');
                            }
                        }
                    }
                }
            }
            TableDefinition::AsSelect(select) => {
                // For CREATE TABLE AS SELECT, derive columns from the SELECT
                let cols = self.resolve_select_columns_for_create(select);
                let col_defs: Vec<String> = cols
                    .iter()
                    .map(|(name, type_name)| {
                        if type_name.is_empty() {
                            name.clone()
                        } else {
                            format!("{} {}", name, type_name)
                        }
                    })
                    .collect();
                sql.push_str(&col_defs.join(", "));
            }
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
        use crate::storage::btree::BTREE_BLOBKEY;

        let index_name = &create.name.name;
        let index_name_lower = index_name.to_lowercase();
        let table_name = &create.table;
        let table_name_lower = table_name.to_lowercase();

        // Check if index already exists
        if let Some(schema) = self.schema {
            if schema.indexes.contains_key(&index_name_lower) {
                if create.if_not_exists {
                    // Return no-op
                    let mut ops = Vec::new();
                    ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
                    ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
                    return Ok(ops);
                }
                return Err(crate::error::Error::with_message(
                    crate::error::ErrorCode::Error,
                    format!("index {} already exists", index_name),
                ));
            }

            // Check if target table exists
            if !schema.tables.contains_key(&table_name_lower) {
                return Err(crate::error::Error::with_message(
                    crate::error::ErrorCode::Error,
                    format!("no such table: {}", table_name),
                ));
            }
        }

        // Build CREATE INDEX SQL for ParseSchema
        let unique_str = if create.unique { "UNIQUE " } else { "" };
        let if_not_exists_str = if create.if_not_exists {
            "IF NOT EXISTS "
        } else {
            ""
        };
        let columns_str: Vec<String> = create
            .columns
            .iter()
            .map(|c| {
                let name = match &c.column {
                    crate::parser::ast::IndexedColumnKind::Name(n) => n.clone(),
                    crate::parser::ast::IndexedColumnKind::Expr(_) => "expr".to_string(),
                };
                match c.order {
                    Some(crate::parser::ast::SortOrder::Asc) => format!("{} ASC", name),
                    Some(crate::parser::ast::SortOrder::Desc) => format!("{} DESC", name),
                    None => name,
                }
            })
            .collect();
        let sql = format!(
            "CREATE {}INDEX {}{} ON {}({})",
            unique_str,
            if_not_exists_str,
            index_name,
            table_name,
            columns_str.join(", ")
        );

        let mut ops = Vec::new();
        let reg_root_page = 1;

        // Init - jump to start of program
        ops.push(Self::make_op(Opcode::Init, 0, 2, 0, P4::Unused));
        // Halt - end of program
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));

        // CreateBtree - create the index's btree page (BLOBKEY for index)
        ops.push(Self::make_op(
            Opcode::CreateBtree,
            0,
            reg_root_page,
            BTREE_BLOBKEY as i32,
            P4::Unused,
        ));

        // ParseSchemaIndex to register the index in schema cache
        ops.push(Self::make_op(
            Opcode::ParseSchemaIndex,
            if create.unique { 1 } else { 0 },
            reg_root_page,
            0,
            P4::Text(sql.clone()),
        ));

        // Populate the index with existing table data
        // Get column indices for the indexed columns
        let indexed_col_indices: Vec<usize> = if let Some(schema) = self.schema {
            if let Some(table) = schema.tables.get(&table_name_lower) {
                create
                    .columns
                    .iter()
                    .filter_map(|c| {
                        let col_name = match &c.column {
                            crate::parser::ast::IndexedColumnKind::Name(n) => n.clone(),
                            crate::parser::ast::IndexedColumnKind::Expr(_) => return None,
                        };
                        table
                            .columns
                            .iter()
                            .position(|tc| tc.name.eq_ignore_ascii_case(&col_name))
                    })
                    .collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        if !indexed_col_indices.is_empty() {
            let table_cursor = 1;
            let index_cursor = 2;
            let num_key_cols = indexed_col_indices.len();
            let reg_col_base = 10;
            let reg_rowid = reg_col_base + num_key_cols as i32;
            let reg_record = reg_rowid + 1;
            let loop_start = ops.len() as i32 + 1;
            let loop_end_label = ops.len() as i32 + 10 + (num_key_cols * 2) as i32;

            // OpenRead table cursor
            ops.push(Self::make_op(
                Opcode::OpenRead,
                table_cursor,
                0,
                0,
                P4::Text(table_name.to_string()),
            ));

            // OpenWrite index cursor using root page from register
            ops.push(Self::make_op_with_p5(
                Opcode::OpenWrite,
                index_cursor,
                reg_root_page,
                (num_key_cols + 1) as i32, // +1 for rowid
                P4::Unused,
                0x02, // P2 is register
            ));

            // Rewind table cursor
            let after_loop = loop_end_label + 2;
            ops.push(Self::make_op(
                Opcode::Rewind,
                table_cursor,
                after_loop,
                0,
                P4::Unused,
            ));

            let loop_body_start = ops.len() as i32;

            // For each indexed column, read from table cursor
            for (i, col_idx) in indexed_col_indices.iter().enumerate() {
                ops.push(Self::make_op(
                    Opcode::Column,
                    table_cursor,
                    *col_idx as i32,
                    reg_col_base + i as i32,
                    P4::Unused,
                ));
            }

            // Get rowid
            ops.push(Self::make_op(
                Opcode::Rowid,
                table_cursor,
                reg_rowid,
                0,
                P4::Unused,
            ));

            // MakeRecord for index (columns + rowid)
            ops.push(Self::make_op(
                Opcode::MakeRecord,
                reg_col_base,
                (num_key_cols + 1) as i32,
                reg_record,
                P4::Unused,
            ));

            // IdxInsert into index cursor
            ops.push(Self::make_op(
                Opcode::IdxInsert,
                index_cursor,
                reg_record,
                0,
                P4::Unused,
            ));

            // Next table cursor, loop back
            ops.push(Self::make_op(
                Opcode::Next,
                table_cursor,
                loop_body_start,
                0,
                P4::Unused,
            ));

            // Close cursors
            ops.push(Self::make_op(Opcode::Close, index_cursor, 0, 0, P4::Unused));
            ops.push(Self::make_op(Opcode::Close, table_cursor, 0, 0, P4::Unused));
        }

        // Insert into sqlite_master
        let cursor_id = 0;
        self.append_sqlite_master_open(&mut ops, cursor_id);
        self.append_sqlite_master_insert_index(
            &mut ops,
            cursor_id,
            index_name,
            table_name,
            reg_root_page,
            &sql,
        );
        self.append_sqlite_master_close(&mut ops, cursor_id);

        // Goto end
        ops.push(Self::make_op(Opcode::Goto, 0, 1, 0, P4::Unused));
        Ok(ops)
    }

    /// Insert an index entry into sqlite_master
    fn append_sqlite_master_insert_index(
        &self,
        ops: &mut Vec<VdbeOp>,
        cursor_id: i32,
        index_name: &str,
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

        // type = 'index'
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_type,
            0,
            P4::Text("index".to_string()),
        ));
        // name = index_name
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_name,
            0,
            P4::Text(index_name.to_string()),
        ));
        // tbl_name = table_name (the table this index is on)
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_tbl,
            0,
            P4::Text(table_name.to_string()),
        ));
        // rootpage = from register
        ops.push(Self::make_op(
            Opcode::Copy,
            reg_root_page,
            reg_root,
            0,
            P4::Unused,
        ));
        // sql = CREATE INDEX statement
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_sql,
            0,
            P4::Text(create_sql.to_string()),
        ));
        // Make record from columns
        ops.push(Self::make_op(
            Opcode::MakeRecord,
            reg_type,
            5,
            reg_record,
            P4::Unused,
        ));
        // Get new rowid
        ops.push(Self::make_op(
            Opcode::NewRowid,
            cursor_id,
            reg_rowid,
            0,
            P4::Unused,
        ));
        // Insert into sqlite_master
        ops.push(Self::make_op(
            Opcode::Insert,
            cursor_id,
            reg_record,
            reg_rowid,
            P4::Text("sqlite_master".to_string()),
        ));
    }

    fn compile_create_view(&mut self, create: &CreateViewStmt) -> Result<Vec<VdbeOp>> {
        // Reconstruct the CREATE VIEW SQL for storage
        let sql = self.reconstruct_create_view_sql(create);

        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 2, 0, P4::Unused));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));

        // Insert into sqlite_master
        let cursor_id = 0;
        self.append_sqlite_master_open(&mut ops, cursor_id);
        self.append_sqlite_master_insert_view(&mut ops, cursor_id, &create.name.name, &sql);
        self.append_sqlite_master_close(&mut ops, cursor_id);

        // Use ParseSchema to register the view in the schema at runtime
        // P2=0 (views don't need a root page), P4=SQL text
        ops.push(Self::make_op(
            Opcode::ParseSchema,
            0,
            0,
            0,
            P4::Text(sql.clone()),
        ));
        ops.push(Self::make_op(Opcode::Goto, 0, 1, 0, P4::Unused));

        Ok(ops)
    }

    /// Insert a view entry into sqlite_master
    fn append_sqlite_master_insert_view(
        &self,
        ops: &mut Vec<VdbeOp>,
        cursor_id: i32,
        view_name: &str,
        sql: &str,
    ) {
        // sqlite_master columns: type, name, tbl_name, rootpage, sql
        // Views have type='view', tbl_name=view_name, rootpage=0
        let reg_type = 2;
        let reg_name = 3;
        let reg_tbl_name = 4;
        let reg_rootpage = 5;
        let reg_sql = 6;
        let reg_record = 7;
        let reg_rowid = 8;

        // type = 'view'
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_type,
            0,
            P4::Text("view".to_string()),
        ));
        // name = view_name
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_name,
            0,
            P4::Text(view_name.to_string()),
        ));
        // tbl_name = view_name (same as name for views)
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_tbl_name,
            0,
            P4::Text(view_name.to_string()),
        ));
        // rootpage = 0 (views don't have a root page)
        ops.push(Self::make_op(
            Opcode::Integer,
            0,
            reg_rootpage,
            0,
            P4::Unused,
        ));
        // sql = CREATE VIEW statement
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_sql,
            0,
            P4::Text(sql.to_string()),
        ));
        // MakeRecord: create record from columns
        ops.push(Self::make_op(
            Opcode::MakeRecord,
            reg_type,
            5,
            reg_record,
            P4::Unused,
        ));
        // NewRowid
        ops.push(Self::make_op(
            Opcode::NewRowid,
            cursor_id,
            reg_rowid,
            0,
            P4::Unused,
        ));
        // Insert into sqlite_master
        ops.push(Self::make_op(
            Opcode::Insert,
            cursor_id,
            reg_record,
            reg_rowid,
            P4::Text("sqlite_master".to_string()),
        ));
    }

    /// Reconstruct CREATE VIEW SQL from the AST
    fn reconstruct_create_view_sql(&self, create: &CreateViewStmt) -> String {
        let mut sql = String::from("CREATE ");
        if create.temporary {
            sql.push_str("TEMP ");
        }
        sql.push_str("VIEW ");
        if create.if_not_exists {
            sql.push_str("IF NOT EXISTS ");
        }
        sql.push_str(&create.name.to_string());

        // Column names (optional)
        if let Some(ref columns) = create.columns {
            sql.push('(');
            sql.push_str(&columns.join(", "));
            sql.push(')');
        }

        sql.push_str(" AS ");
        sql.push_str(&self.select_to_sql(&create.query));

        sql
    }

    fn compile_create_trigger(&mut self, create: &CreateTriggerStmt) -> Result<Vec<VdbeOp>> {
        // Reconstruct the CREATE TRIGGER SQL for storage
        // This preserves the original SQL text for later parsing
        let sql = self.reconstruct_create_trigger_sql(create);

        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 2, 0, P4::Unused));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        // Use ParseSchema to register the trigger in the schema at runtime
        // P2=0 (triggers don't need a root page), P4=SQL text
        ops.push(Self::make_op(Opcode::ParseSchema, 0, 0, 0, P4::Text(sql)));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        Ok(ops)
    }

    /// Reconstruct CREATE TRIGGER SQL from the AST
    fn reconstruct_create_trigger_sql(&self, create: &CreateTriggerStmt) -> String {
        let mut sql = String::from("CREATE ");
        if create.temporary {
            sql.push_str("TEMPORARY ");
        }
        sql.push_str("TRIGGER ");
        if create.if_not_exists {
            sql.push_str("IF NOT EXISTS ");
        }
        sql.push_str(&create.name.to_string());
        sql.push(' ');

        // Timing
        match create.time {
            TriggerTime::Before => sql.push_str("BEFORE "),
            TriggerTime::After => sql.push_str("AFTER "),
            TriggerTime::InsteadOf => sql.push_str("INSTEAD OF "),
        }

        // Event
        match &create.event {
            TriggerEvent::Delete => sql.push_str("DELETE "),
            TriggerEvent::Insert => sql.push_str("INSERT "),
            TriggerEvent::Update(cols) => {
                sql.push_str("UPDATE ");
                if let Some(cols) = cols {
                    sql.push_str("OF ");
                    sql.push_str(&cols.join(", "));
                    sql.push(' ');
                }
            }
        }

        sql.push_str("ON ");
        sql.push_str(&create.table);
        sql.push(' ');

        if create.for_each_row {
            sql.push_str("FOR EACH ROW ");
        }

        // WHEN clause
        if let Some(ref when) = create.when {
            sql.push_str("WHEN ");
            sql.push_str(&self.expr_to_sql(when));
            sql.push(' ');
        }

        sql.push_str("BEGIN ");
        for stmt in &create.body {
            sql.push_str(&self.stmt_to_sql(stmt));
            sql.push_str("; ");
        }
        sql.push_str("END");

        sql
    }

    /// Convert expression to SQL (for trigger reconstruction)
    fn expr_to_sql(&self, expr: &Expr) -> String {
        match expr {
            Expr::Literal(lit) => match lit {
                Literal::Null => "NULL".to_string(),
                Literal::Integer(n) => n.to_string(),
                Literal::Float(n) => n.to_string(),
                Literal::String(s) => format!("'{}'", s.replace('\'', "''")),
                Literal::Blob(b) => format!("X'{}'", hex::encode(b)),
                Literal::Bool(b) => if *b { "1" } else { "0" }.to_string(),
                Literal::CurrentTime => "CURRENT_TIME".to_string(),
                Literal::CurrentDate => "CURRENT_DATE".to_string(),
                Literal::CurrentTimestamp => "CURRENT_TIMESTAMP".to_string(),
            },
            Expr::Column(col) => {
                if let Some(ref table) = col.table {
                    format!("{}.{}", table, col.column)
                } else {
                    col.column.clone()
                }
            }
            Expr::Binary { op, left, right } => {
                let op_str = match op {
                    BinaryOp::Add => "+",
                    BinaryOp::Sub => "-",
                    BinaryOp::Mul => "*",
                    BinaryOp::Div => "/",
                    BinaryOp::Mod => "%",
                    BinaryOp::Eq => "=",
                    BinaryOp::Ne => "!=",
                    BinaryOp::Lt => "<",
                    BinaryOp::Le => "<=",
                    BinaryOp::Gt => ">",
                    BinaryOp::Ge => ">=",
                    BinaryOp::And => "AND",
                    BinaryOp::Or => "OR",
                    BinaryOp::Concat => "||",
                    BinaryOp::BitAnd => "&",
                    BinaryOp::BitOr => "|",
                    BinaryOp::ShiftLeft => "<<",
                    BinaryOp::ShiftRight => ">>",
                    BinaryOp::Is => "IS",
                    BinaryOp::IsNot => "IS NOT",
                };
                format!(
                    "({} {} {})",
                    self.expr_to_sql(left),
                    op_str,
                    self.expr_to_sql(right)
                )
            }
            Expr::Unary { op, expr: inner } => {
                let op_str = match op {
                    UnaryOp::Not => "NOT ",
                    UnaryOp::Neg => "-",
                    UnaryOp::Pos => "+",
                    UnaryOp::BitNot => "~",
                };
                format!("{}{}", op_str, self.expr_to_sql(inner))
            }
            Expr::Function(func) => {
                let args_str = match &func.args {
                    FunctionArgs::Star => "*".to_string(),
                    FunctionArgs::Exprs(args) => args
                        .iter()
                        .map(|a| self.expr_to_sql(a))
                        .collect::<Vec<_>>()
                        .join(", "),
                };
                format!("{}({})", func.name, args_str)
            }
            Expr::Variable(var) => match var {
                Variable::Named { prefix, name } => format!("{}{}", prefix, name),
                Variable::Numbered(Some(idx)) => format!("?{}", idx),
                Variable::Numbered(None) => "?".to_string(),
            },
            Expr::Parens(inner) => format!("({})", self.expr_to_sql(inner)),
            Expr::Like {
                expr,
                pattern,
                escape,
                op,
                negated,
            } => {
                let op_str = match op {
                    LikeOp::Like => "LIKE",
                    LikeOp::Glob => "GLOB",
                    LikeOp::Regexp => "REGEXP",
                    LikeOp::Match => "MATCH",
                };
                let neg = if *negated { "NOT " } else { "" };
                let esc = if let Some(esc_expr) = escape {
                    format!(" ESCAPE {}", self.expr_to_sql(esc_expr))
                } else {
                    String::new()
                };
                format!(
                    "({} {}{} {}{})",
                    self.expr_to_sql(expr),
                    neg,
                    op_str,
                    self.expr_to_sql(pattern),
                    esc
                )
            }
            _ => "?".to_string(), // Fallback for complex expressions
        }
    }

    /// Convert statement to SQL (for trigger body reconstruction)
    fn stmt_to_sql(&self, stmt: &Stmt) -> String {
        match stmt {
            Stmt::Update(update) => {
                let mut sql = format!("UPDATE {} SET ", update.table.name);
                let assignments: Vec<String> = update
                    .assignments
                    .iter()
                    .map(|a| {
                        let cols = a.columns.join(", ");
                        format!("{} = {}", cols, self.expr_to_sql(&a.expr))
                    })
                    .collect();
                sql.push_str(&assignments.join(", "));
                if let Some(ref where_clause) = update.where_clause {
                    sql.push_str(" WHERE ");
                    sql.push_str(&self.expr_to_sql(where_clause));
                }
                sql
            }
            Stmt::Insert(insert) => {
                let mut sql = format!("INSERT INTO {}", insert.table.name);
                if let Some(ref cols) = insert.columns {
                    sql.push_str(" (");
                    sql.push_str(&cols.join(", "));
                    sql.push(')');
                }
                match &insert.source {
                    InsertSource::Values(rows) => {
                        sql.push_str(" VALUES ");
                        let value_lists: Vec<String> = rows
                            .iter()
                            .map(|row| {
                                let exprs: Vec<String> =
                                    row.iter().map(|e| self.expr_to_sql(e)).collect();
                                format!("({})", exprs.join(", "))
                            })
                            .collect();
                        sql.push_str(&value_lists.join(", "));
                    }
                    InsertSource::Select(select) => {
                        sql.push(' ');
                        sql.push_str(&self.select_to_sql(select));
                    }
                    InsertSource::DefaultValues => {
                        sql.push_str(" DEFAULT VALUES");
                    }
                }
                sql
            }
            Stmt::Delete(delete) => {
                let mut sql = format!("DELETE FROM {}", delete.table.name);
                if let Some(ref where_clause) = delete.where_clause {
                    sql.push_str(" WHERE ");
                    sql.push_str(&self.expr_to_sql(where_clause));
                }
                sql
            }
            Stmt::Select(select) => self.select_to_sql(select),
            _ => String::new(),
        }
    }

    /// Convert SELECT to SQL including ORDER BY and LIMIT
    fn select_to_sql(&self, select: &SelectStmt) -> String {
        use crate::parser::ast::{NullsOrder, SortOrder};
        let mut sql = self.select_body_to_sql(&select.body);

        // Add ORDER BY if present
        if let Some(ref order_by) = select.order_by {
            sql.push_str(" ORDER BY ");
            let terms: Vec<String> = order_by
                .iter()
                .map(|term| {
                    let mut s = self.expr_to_sql(&term.expr);
                    if term.order == SortOrder::Desc {
                        s.push_str(" DESC");
                    }
                    if term.nulls == NullsOrder::First {
                        s.push_str(" NULLS FIRST");
                    } else if term.nulls == NullsOrder::Last {
                        s.push_str(" NULLS LAST");
                    }
                    s
                })
                .collect();
            sql.push_str(&terms.join(", "));
        }

        // Add LIMIT if present
        if let Some(ref limit) = select.limit {
            sql.push_str(" LIMIT ");
            sql.push_str(&self.expr_to_sql(&limit.limit));
            if let Some(ref offset) = limit.offset {
                sql.push_str(" OFFSET ");
                sql.push_str(&self.expr_to_sql(offset));
            }
        }

        sql
    }

    /// Convert SelectBody to SQL
    fn select_body_to_sql(&self, body: &SelectBody) -> String {
        match body {
            SelectBody::Select(core) => self.select_core_to_sql(core),
            SelectBody::Compound { op, left, right } => {
                let left_sql = self.select_body_to_sql(left);
                let right_sql = self.select_body_to_sql(right);
                let op_str = match op {
                    CompoundOp::Union => "UNION",
                    CompoundOp::UnionAll => "UNION ALL",
                    CompoundOp::Intersect => "INTERSECT",
                    CompoundOp::Except => "EXCEPT",
                };
                format!("{} {} {}", left_sql, op_str, right_sql)
            }
        }
    }

    /// Convert SelectCore to SQL
    fn select_core_to_sql(&self, core: &SelectCore) -> String {
        let mut sql = String::from("SELECT ");
        let cols: Vec<String> = core
            .columns
            .iter()
            .map(|col| match col {
                ResultColumn::Star => "*".to_string(),
                ResultColumn::TableStar(t) => format!("{}.*", t),
                ResultColumn::Expr { expr, alias } => {
                    let e = self.expr_to_sql(expr);
                    if let Some(a) = alias {
                        format!("{} AS {}", e, a)
                    } else {
                        e
                    }
                }
            })
            .collect();
        sql.push_str(&cols.join(", "));

        if let Some(ref from) = core.from {
            sql.push_str(" FROM ");
            sql.push_str(&self.from_clause_to_sql(from));
        }

        if let Some(ref where_clause) = core.where_clause {
            sql.push_str(" WHERE ");
            sql.push_str(&self.expr_to_sql(where_clause));
        }

        sql
    }

    /// Convert FROM clause to SQL
    fn from_clause_to_sql(&self, from: &FromClause) -> String {
        let parts: Vec<String> = from
            .tables
            .iter()
            .map(|t| self.table_ref_to_sql(t))
            .collect();
        parts.join(", ")
    }

    /// Convert TableRef to SQL
    fn table_ref_to_sql(&self, table_ref: &TableRef) -> String {
        match table_ref {
            TableRef::Table { name, alias, .. } => {
                if let Some(a) = alias {
                    format!("{} AS {}", name.name, a)
                } else {
                    name.name.clone()
                }
            }
            TableRef::Join {
                left,
                join_type,
                right,
                constraint,
            } => {
                let left_sql = self.table_ref_to_sql(left);
                let right_sql = self.table_ref_to_sql(right);
                let join_str = if join_type.contains(JoinFlags::LEFT) {
                    "LEFT JOIN"
                } else if join_type.contains(JoinFlags::RIGHT) {
                    "RIGHT JOIN"
                } else if join_type.contains(JoinFlags::CROSS) {
                    "CROSS JOIN"
                } else {
                    "JOIN"
                };
                let mut sql = format!("{} {} {}", left_sql, join_str, right_sql);
                if let Some(ref c) = constraint {
                    match c {
                        JoinConstraint::On(expr) => {
                            sql.push_str(" ON ");
                            sql.push_str(&self.expr_to_sql(expr));
                        }
                        JoinConstraint::Using(cols) => {
                            sql.push_str(" USING (");
                            sql.push_str(&cols.join(", "));
                            sql.push(')');
                        }
                    }
                }
                sql
            }
            TableRef::Subquery { query, alias } => {
                let subq = self.select_to_sql(query);
                if let Some(a) = alias {
                    format!("({}) AS {}", subq, a)
                } else {
                    format!("({})", subq)
                }
            }
            TableRef::TableFunction { name, args, alias } => {
                let args_sql: Vec<String> = args.iter().map(|a| self.expr_to_sql(a)).collect();
                let func_sql = format!("{}({})", name, args_sql.join(", "));
                if let Some(a) = alias {
                    format!("{} AS {}", func_sql, a)
                } else {
                    func_sql
                }
            }
            TableRef::Parens(inner) => {
                format!("({})", self.table_ref_to_sql(inner))
            }
        }
    }

    fn compile_drop(&mut self, drop: &DropStmt, kind: &str) -> Result<Vec<VdbeOp>> {
        let name = &drop.name.name;
        let name_lower = name.to_lowercase();

        // Check for reserved names (sqlite_master, etc.) - cannot be dropped
        if name_lower.starts_with("sqlite_") {
            return Err(crate::error::Error::with_message(
                crate::error::ErrorCode::Error,
                format!("{} {} may not be dropped", kind, name),
            ));
        }

        // Check if the object exists in schema based on kind
        if let Some(schema) = self.schema {
            let exists = match kind {
                "table" => schema.tables.contains_key(&name_lower),
                "index" => schema.indexes.contains_key(&name_lower),
                "view" => schema.views.contains_key(&name_lower),
                "trigger" => schema.triggers.contains_key(&name_lower),
                _ => true, // Unknown kind - let it through
            };

            if !exists {
                if !drop.if_exists {
                    return Err(crate::error::Error::with_message(
                        crate::error::ErrorCode::Error,
                        format!("no such {}: {}", kind, name),
                    ));
                }
                // IF EXISTS specified and object doesn't exist - return no-op
                let mut ops = Vec::new();
                ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
                ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
                return Ok(ops);
            }
        }

        // Generate bytecode to drop the object
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 2, 0, P4::Unused));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        // DropSchema opcode to remove from schema
        // P1 encodes the type: 0=table, 1=index, 2=view, 3=trigger
        let type_code = match kind {
            "table" => 0,
            "index" => 1,
            "view" => 2,
            "trigger" => 3,
            _ => 0,
        };
        ops.push(Self::make_op(
            Opcode::DropSchema,
            type_code,
            0,
            0,
            P4::Text(name.clone()),
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

    fn compile_explain_query_plan(&mut self, stmt: &Stmt) -> Result<Vec<VdbeOp>> {
        let details = self.explain_query_plan_details(stmt);
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));

        let base_reg = 1;
        for (i, detail) in details.iter().enumerate() {
            ops.push(Self::make_op(
                Opcode::Integer,
                i as i32,
                base_reg,
                0,
                P4::Unused,
            ));
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
                P4::Text(detail.clone()),
            ));
            ops.push(Self::make_op(Opcode::ResultRow, base_reg, 4, 0, P4::Unused));
        }

        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));

        Ok(ops)
    }

    fn explain_query_plan_details(&self, stmt: &Stmt) -> Vec<String> {
        match stmt {
            Stmt::Select(select) => self.explain_select_query_plan(select),
            _ => vec!["SCAN TABLE".to_string()],
        }
    }

    fn explain_select_query_plan(&self, select: &SelectStmt) -> Vec<String> {
        let schema = match self.schema {
            Some(schema) => schema,
            None => return vec!["SCAN TABLE".to_string()],
        };

        let core = match &select.body {
            SelectBody::Select(core) => core,
            _ => return vec!["SCAN TABLE".to_string()],
        };

        let from = match &core.from {
            Some(from) => from,
            None => return vec!["SCAN CONSTANT ROW".to_string()],
        };

        let src_list = from.to_src_list();
        let mut table_infos = Vec::new();

        for item in &src_list.items {
            match &item.source {
                TableSource::Table(name) => {
                    let table_name = name.name.clone();
                    let display_name = item.alias.clone().unwrap_or_else(|| table_name.clone());
                    let schema_table = schema.table(&table_name);
                    let (columns, estimated_rows, has_rowid) = match schema_table.as_ref() {
                        Some(table) => (
                            table.columns.iter().map(|c| c.name.clone()).collect(),
                            if table.row_estimate > 0 {
                                table.row_estimate
                            } else {
                                1000
                            },
                            !table.without_rowid,
                        ),
                        None => (Vec::new(), 1000, true),
                    };

                    table_infos.push(ExplainTableInfo {
                        name: table_name,
                        alias: item.alias.clone(),
                        display_name,
                        columns,
                        estimated_rows,
                        has_rowid,
                        indexed_by: item.indexed_by.clone(),
                        indexes: Vec::new(),
                    });
                }
                TableSource::Subquery(_) => {
                    let display_name = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| "(subquery)".to_string());
                    table_infos.push(ExplainTableInfo {
                        name: display_name.clone(),
                        alias: item.alias.clone(),
                        display_name,
                        columns: Vec::new(),
                        estimated_rows: 1000,
                        has_rowid: true,
                        indexed_by: item.indexed_by.clone(),
                        indexes: Vec::new(),
                    });
                }
                TableSource::TableFunction { name, .. } => {
                    table_infos.push(ExplainTableInfo {
                        name: name.clone(),
                        alias: item.alias.clone(),
                        display_name: name.clone(),
                        columns: Vec::new(),
                        estimated_rows: 1000,
                        has_rowid: true,
                        indexed_by: item.indexed_by.clone(),
                        indexes: Vec::new(),
                    });
                }
            }
        }

        let (required_columns, requires_all_cols) =
            self.collect_required_columns(core, &table_infos);

        for (idx, table_info) in table_infos.iter_mut().enumerate() {
            let schema_table = schema.table(&table_info.name);
            let Some(table) = schema_table.as_ref() else {
                continue;
            };

            let index_filter = table_info.indexed_by.as_ref();
            let mut indexes: Vec<IndexInfo> = Vec::new();

            for index in &table.indexes {
                if let Some(IndexedBy::NotIndexed) = index_filter {
                    break;
                }
                if let Some(IndexedBy::Index(forced)) = index_filter {
                    if !index.name.eq_ignore_ascii_case(forced) {
                        continue;
                    }
                }

                let mut columns = Vec::new();
                for col in &index.columns {
                    let col_idx = if col.column_idx >= 0 {
                        Some(col.column_idx)
                    } else {
                        match col.expr.as_ref() {
                            Some(crate::schema::Expr::Column { column, .. }) => table
                                .columns
                                .iter()
                                .position(|c| c.name.eq_ignore_ascii_case(column))
                                .map(|idx| idx as i32),
                            _ => None,
                        }
                    };
                    if let Some(idx) = col_idx {
                        columns.push(idx);
                    }
                }
                if columns.is_empty() {
                    continue;
                }

                let is_covering = !requires_all_cols[idx]
                    && required_columns[idx].iter().all(|col| {
                        columns.iter().any(|cidx| {
                            table
                                .columns
                                .get(*cidx as usize)
                                .map(|c| c.name.eq_ignore_ascii_case(col))
                                .unwrap_or(false)
                        })
                    });

                indexes.push(IndexInfo {
                    name: index.name.clone(),
                    columns,
                    is_primary: index.is_primary_key,
                    is_unique: index.unique,
                    is_covering,
                    stats: index.stats.clone(),
                });
            }

            table_info.indexes = indexes;
        }

        let mut planner = QueryPlanner::new();
        planner.set_case_sensitive_like(self.case_sensitive_like);
        for table in &table_infos {
            planner.add_table(
                table.name.clone(),
                table.alias.clone(),
                table.estimated_rows,
            );
        }
        for (idx, table) in table_infos.iter().enumerate() {
            planner.set_table_columns(idx, table.columns.clone());
            planner.set_table_rowid(idx, table.has_rowid);

            // Set INTEGER PRIMARY KEY column (rowid alias) if present
            if table.has_rowid {
                if let Some(schema_table) = schema.table(&table.name) {
                    if let Some(ref pk_cols) = schema_table.primary_key {
                        if pk_cols.len() == 1 {
                            let pk_col_idx = pk_cols[0];
                            if pk_col_idx < schema_table.columns.len() {
                                use crate::schema::Affinity;
                                let col = &schema_table.columns[pk_col_idx];
                                if col.affinity == Affinity::Integer {
                                    planner.set_table_ipk(idx, pk_col_idx as i32);
                                }
                            }
                        }
                    }
                }
            }

            for index in &table.indexes {
                planner.add_index(idx, index.clone());
            }
        }

        // Build alias map and resolve aliases in WHERE clause for proper index detection
        let aliases = Self::build_alias_map(core);
        let resolved_where = core.where_clause.as_ref().map(|w| {
            if aliases.is_empty() {
                (**w).clone()
            } else {
                Self::resolve_aliases_in_expr(&aliases, w)
            }
        });

        if planner.analyze_where(resolved_where.as_ref()).is_err() {
            // Return SCAN for each table when WHERE analysis fails
            return table_infos
                .iter()
                .map(|t| format!("SCAN {}", t.display_name))
                .collect();
        }
        let plan = match planner.find_best_plan() {
            Ok(plan) => plan,
            Err(_) => {
                // Return SCAN for each table when planning fails
                return table_infos
                    .iter()
                    .map(|t| format!("SCAN {}", t.display_name))
                    .collect();
            }
        };

        if plan.levels.is_empty() {
            // Return SCAN for each table when plan has no levels
            return table_infos
                .iter()
                .map(|t| format!("SCAN {}", t.display_name))
                .collect();
        }

        plan.levels
            .iter()
            .map(|level| self.format_plan_detail(level, &plan.terms, &table_infos))
            .collect()
    }

    fn format_plan_detail(
        &self,
        level: &super::where_clause::WhereLevel,
        terms: &[WhereTerm],
        table_infos: &[ExplainTableInfo],
    ) -> String {
        let table_idx = level.from_idx as usize;
        let table_info = table_infos.get(table_idx);
        let display_name = table_info
            .map(|t| t.display_name.as_str())
            .unwrap_or("table");
        match &level.plan {
            WherePlan::FullScan => format!("SCAN {}", display_name),
            WherePlan::IndexScan {
                index_name,
                covering,
                ..
            } => {
                let index_info = table_info.and_then(|t| {
                    t.indexes
                        .iter()
                        .find(|idx| idx.name.eq_ignore_ascii_case(index_name))
                });
                let constraints = match (table_info, index_info) {
                    (Some(table), Some(index)) => {
                        self.index_constraints(index, table_idx, &table.columns, terms)
                    }
                    _ => Vec::new(),
                };
                if constraints.is_empty() {
                    if *covering {
                        format!(
                            "SEARCH {} USING COVERING INDEX {}",
                            display_name, index_name
                        )
                    } else {
                        format!("SEARCH {} USING INDEX {}", display_name, index_name)
                    }
                } else {
                    if *covering {
                        format!(
                            "SEARCH {} USING COVERING INDEX {} ({})",
                            display_name,
                            index_name,
                            constraints.join(" AND ")
                        )
                    } else {
                        format!(
                            "SEARCH {} USING INDEX {} ({})",
                            display_name,
                            index_name,
                            constraints.join(" AND ")
                        )
                    }
                }
            }
            WherePlan::RowidEq => {
                format!(
                    "SEARCH {} USING INTEGER PRIMARY KEY (rowid=?)",
                    display_name
                )
            }
            WherePlan::RowidRange { has_start, has_end } => {
                let mut parts = Vec::new();
                if *has_start {
                    parts.push("rowid>?");
                }
                if *has_end {
                    parts.push("rowid<?");
                }
                if parts.is_empty() {
                    format!("SEARCH {} USING INTEGER PRIMARY KEY", display_name)
                } else {
                    format!(
                        "SEARCH {} USING INTEGER PRIMARY KEY ({})",
                        display_name,
                        parts.join(" AND ")
                    )
                }
            }
            WherePlan::PrimaryKey { .. } => {
                format!("SEARCH {} USING INTEGER PRIMARY KEY", display_name)
            }
            WherePlan::RowidIn { .. } => {
                format!(
                    "SEARCH {} USING INTEGER PRIMARY KEY (rowid=?)",
                    display_name
                )
            }
        }
    }

    fn index_constraints(
        &self,
        index: &IndexInfo,
        table_idx: usize,
        columns: &[String],
        terms: &[WhereTerm],
    ) -> Vec<String> {
        let mut constraints = Vec::new();
        for col_idx in &index.columns {
            // Check both left_col and right_col for equality (for join conditions like s=y)
            let eq_term = terms.iter().find(|term| {
                let left_matches = term
                    .left_col
                    .is_some_and(|(ti, ci)| ti == table_idx as i32 && ci == *col_idx);
                let right_matches = term
                    .right_col
                    .is_some_and(|(ti, ci)| ti == table_idx as i32 && ci == *col_idx);
                (left_matches || right_matches) && term.is_equality()
            });
            if eq_term.is_some() {
                constraints.push(format!("{}=?", self.column_name(columns, *col_idx)));
                continue;
            }

            let range_terms: Vec<&WhereTerm> = terms
                .iter()
                .filter(|term| {
                    term.left_col
                        .is_some_and(|(ti, ci)| ti == table_idx as i32 && ci == *col_idx)
                        && term.is_range()
                })
                .collect();
            if range_terms.is_empty() {
                break;
            }

            for term in range_terms {
                if let Some(op) = term.op {
                    constraints.push(format!(
                        "{}{}?",
                        self.column_name(columns, *col_idx),
                        self.term_op_string(op)
                    ));
                }
            }
            break;
        }
        constraints
    }

    fn column_name(&self, columns: &[String], col_idx: i32) -> String {
        if col_idx < 0 {
            return "rowid".to_string();
        }
        columns
            .get(col_idx as usize)
            .cloned()
            .unwrap_or_else(|| format!("column{}", col_idx))
    }

    fn term_op_string(&self, op: super::where_clause::TermOp) -> &'static str {
        match op {
            super::where_clause::TermOp::Eq | super::where_clause::TermOp::Is => "=",
            super::where_clause::TermOp::Lt => "<",
            super::where_clause::TermOp::Le => "<=",
            super::where_clause::TermOp::Gt => ">",
            super::where_clause::TermOp::Ge => ">=",
            super::where_clause::TermOp::In => " IN ",
            _ => "",
        }
    }

    /// Resolve column aliases in a WHERE expression.
    /// Replaces unqualified column references with their aliased expressions.
    fn resolve_aliases_in_expr(aliases: &HashMap<String, Expr>, expr: &Expr) -> Expr {
        match expr {
            Expr::Column(col_ref) if col_ref.table.is_none() => {
                let col_lower = col_ref.column.to_lowercase();
                if let Some(resolved) = aliases.get(&col_lower) {
                    resolved.clone()
                } else {
                    expr.clone()
                }
            }
            Expr::Binary { op, left, right } => Expr::Binary {
                op: *op,
                left: Box::new(Self::resolve_aliases_in_expr(aliases, left)),
                right: Box::new(Self::resolve_aliases_in_expr(aliases, right)),
            },
            Expr::Unary { op, expr: inner } => Expr::Unary {
                op: *op,
                expr: Box::new(Self::resolve_aliases_in_expr(aliases, inner)),
            },
            Expr::Parens(inner) => {
                Expr::Parens(Box::new(Self::resolve_aliases_in_expr(aliases, inner)))
            }
            Expr::In {
                expr: inner,
                list,
                negated,
            } => {
                let resolved_list = match list {
                    InList::Values(exprs) => InList::Values(
                        exprs
                            .iter()
                            .map(|e| Self::resolve_aliases_in_expr(aliases, e))
                            .collect(),
                    ),
                    other => other.clone(),
                };
                Expr::In {
                    expr: Box::new(Self::resolve_aliases_in_expr(aliases, inner)),
                    list: resolved_list,
                    negated: *negated,
                }
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                negated,
            } => Expr::Between {
                expr: Box::new(Self::resolve_aliases_in_expr(aliases, inner)),
                low: Box::new(Self::resolve_aliases_in_expr(aliases, low)),
                high: Box::new(Self::resolve_aliases_in_expr(aliases, high)),
                negated: *negated,
            },
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => Expr::Case {
                operand: operand
                    .as_ref()
                    .map(|e| Box::new(Self::resolve_aliases_in_expr(aliases, e))),
                when_clauses: when_clauses
                    .iter()
                    .map(|wc| WhenClause {
                        when: Box::new(Self::resolve_aliases_in_expr(aliases, &wc.when)),
                        then: Box::new(Self::resolve_aliases_in_expr(aliases, &wc.then)),
                    })
                    .collect(),
                else_clause: else_clause
                    .as_ref()
                    .map(|e| Box::new(Self::resolve_aliases_in_expr(aliases, e))),
            },
            Expr::Function(func) => {
                let args = match &func.args {
                    FunctionArgs::Exprs(exprs) => FunctionArgs::Exprs(
                        exprs
                            .iter()
                            .map(|e| Self::resolve_aliases_in_expr(aliases, e))
                            .collect(),
                    ),
                    other => other.clone(),
                };
                Expr::Function(FunctionCall {
                    name: func.name.clone(),
                    args,
                    distinct: func.distinct,
                    filter: func.filter.clone(),
                    over: func.over.clone(),
                })
            }
            _ => expr.clone(),
        }
    }

    /// Build alias map from select result columns
    fn build_alias_map(core: &SelectCore) -> HashMap<String, Expr> {
        let mut aliases = HashMap::new();
        for col in &core.columns {
            if let ResultColumn::Expr {
                expr,
                alias: Some(alias),
            } = col
            {
                aliases.insert(alias.to_lowercase(), expr.clone());
            }
        }
        aliases
    }

    fn collect_required_columns(
        &self,
        core: &SelectCore,
        table_infos: &[ExplainTableInfo],
    ) -> (Vec<std::collections::HashSet<String>>, Vec<bool>) {
        let mut required = vec![std::collections::HashSet::new(); table_infos.len()];
        let mut requires_all_cols = vec![false; table_infos.len()];

        for col in &core.columns {
            match col {
                ResultColumn::Star => {
                    for flag in &mut requires_all_cols {
                        *flag = true;
                    }
                }
                ResultColumn::TableStar(name) => {
                    for (idx, table) in table_infos.iter().enumerate() {
                        if table.name.eq_ignore_ascii_case(name)
                            || table
                                .alias
                                .as_ref()
                                .is_some_and(|alias| alias.eq_ignore_ascii_case(name))
                        {
                            requires_all_cols[idx] = true;
                        }
                    }
                }
                ResultColumn::Expr { expr, .. } => {
                    let mut refs = Vec::new();
                    self.collect_expr_columns(expr, &mut refs);
                    for col_ref in refs {
                        if let Some(idx) = self.table_index_for_column(&col_ref, table_infos) {
                            required[idx].insert(col_ref.column);
                        }
                    }
                }
            }
        }

        (required, requires_all_cols)
    }

    fn table_index_for_column(
        &self,
        col_ref: &ColumnRef,
        table_infos: &[ExplainTableInfo],
    ) -> Option<usize> {
        if let Some(table) = &col_ref.table {
            for (idx, info) in table_infos.iter().enumerate() {
                if table.eq_ignore_ascii_case(&info.name)
                    || info
                        .alias
                        .as_ref()
                        .is_some_and(|alias| table.eq_ignore_ascii_case(alias))
                {
                    return Some(idx);
                }
            }
            return None;
        }

        if table_infos.len() == 1 {
            return Some(0);
        }

        None
    }

    fn collect_expr_columns(&self, expr: &Expr, refs: &mut Vec<ColumnRef>) {
        match expr {
            Expr::Column(col) => refs.push(col.clone()),
            Expr::Unary { expr, .. } => self.collect_expr_columns(expr, refs),
            Expr::Binary { left, right, .. } => {
                self.collect_expr_columns(left, refs);
                self.collect_expr_columns(right, refs);
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.collect_expr_columns(expr, refs);
                self.collect_expr_columns(low, refs);
                self.collect_expr_columns(high, refs);
            }
            Expr::In { expr, list, .. } => {
                self.collect_expr_columns(expr, refs);
                match list {
                    InList::Values(values) => {
                        for value in values {
                            self.collect_expr_columns(value, refs);
                        }
                    }
                    InList::Subquery(_) | InList::Table(_) => {}
                }
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                self.collect_expr_columns(expr, refs);
                self.collect_expr_columns(pattern, refs);
                if let Some(escape) = escape {
                    self.collect_expr_columns(escape, refs);
                }
            }
            Expr::IsNull { expr, .. } => self.collect_expr_columns(expr, refs),
            Expr::IsDistinct { left, right, .. } => {
                self.collect_expr_columns(left, refs);
                self.collect_expr_columns(right, refs);
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(operand) = operand {
                    self.collect_expr_columns(operand, refs);
                }
                for clause in when_clauses {
                    self.collect_expr_columns(&clause.when, refs);
                    self.collect_expr_columns(&clause.then, refs);
                }
                if let Some(else_clause) = else_clause {
                    self.collect_expr_columns(else_clause, refs);
                }
            }
            Expr::Cast { expr, .. } | Expr::Collate { expr, .. } | Expr::Parens(expr) => {
                self.collect_expr_columns(expr, refs);
            }
            Expr::Function(call) => {
                if let FunctionArgs::Exprs(exprs) = &call.args {
                    for expr in exprs {
                        self.collect_expr_columns(expr, refs);
                    }
                }
                if let Some(filter) = &call.filter {
                    self.collect_expr_columns(filter, refs);
                }
            }
            Expr::Exists { .. } | Expr::Subquery(_) => {}
            Expr::Literal(_) | Expr::Variable(_) | Expr::Raise { .. } => {}
        }
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

/// Compile SQL to VDBE bytecode with schema and column naming configuration
///
/// Returns the compiled statement and any remaining SQL (tail).
/// The schema is used for name resolution (e.g., expanding SELECT *).
/// The column naming flags control result column name formatting.
pub fn compile_sql_with_config<'a>(
    sql: &'a str,
    schema: &crate::schema::Schema,
    short_column_names: bool,
    full_column_names: bool,
    case_sensitive_like: bool,
) -> Result<(CompiledStmt, &'a str)> {
    let mut compiler = StatementCompiler::with_schema(schema);
    compiler.set_column_name_flags(short_column_names, full_column_names);
    compiler.set_case_sensitive_like(case_sensitive_like);
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
