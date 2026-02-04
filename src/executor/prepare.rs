//! Statement Preparation
//!
//! This module handles SQL parsing and compilation to VDBE bytecode.
//! Corresponds to SQLite's prepare.c - the interface between the parser
//! and the code generator.

use std::collections::{HashMap, HashSet};

use crate::error::{Error, ErrorCode, Result};
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
    /// LIKE patterns with variable parameters that may need reprepare
    /// Contains (param_index, is_case_sensitive) for each LIKE with a variable pattern
    pub like_reprepare_info: Vec<(i32, bool)>,
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
    /// Temp schema for TEMP tables/views (optional)
    temp_schema: Option<&'s crate::schema::Schema>,
    /// PRAGMA short_column_names (default ON)
    short_column_names: bool,
    /// PRAGMA full_column_names (default OFF)
    full_column_names: bool,
    /// LIKE case sensitivity (for LIKE index optimization)
    case_sensitive_like: bool,
    /// Enable view access (from db config)
    enable_view: bool,
    /// Virtual table registry for xBestIndex calls
    vtab_registry: Option<std::sync::Arc<crate::vtab::VtabRegistry>>,
    /// Attached database schemas (name, schema) in attach order
    attached_schemas: Vec<(String, &'s crate::schema::Schema)>,
    /// Enable double-quoted string literals in DML (SQLITE_DBCONFIG_DQS_DML)
    dqs_dml: bool,
}

impl<'s> StatementCompiler<'s> {
    /// Create a new statement compiler
    pub fn new() -> Self {
        Self {
            param_count: 0,
            param_names: Vec::new(),
            named_params: HashSet::new(),
            schema: None,
            temp_schema: None,
            short_column_names: true,
            full_column_names: false,
            case_sensitive_like: false,
            enable_view: true,
            vtab_registry: None,
            attached_schemas: Vec::new(),
            dqs_dml: true, // Default: enabled for backward compatibility
        }
    }

    /// Create a new statement compiler with schema access
    pub fn with_schema(schema: &'s crate::schema::Schema) -> Self {
        Self {
            param_count: 0,
            param_names: Vec::new(),
            named_params: HashSet::new(),
            schema: Some(schema),
            temp_schema: None,
            short_column_names: true,
            full_column_names: false,
            case_sensitive_like: false,
            enable_view: true,
            vtab_registry: None,
            attached_schemas: Vec::new(),
            dqs_dml: true, // Default: enabled for backward compatibility
        }
    }

    pub fn set_attached_schemas(&mut self, schemas: Vec<(String, &'s crate::schema::Schema)>) {
        self.attached_schemas = schemas;
    }

    /// Set the DQS_DML flag (double-quoted string literals in DML)
    pub fn set_dqs_dml(&mut self, enabled: bool) {
        self.dqs_dml = enabled;
    }

    fn resolve_db_idx(&self, name: &QualifiedName, temporary: bool) -> Result<i32> {
        if temporary {
            return Ok(1);
        }
        match name.schema.as_deref() {
            None | Some("main") => Ok(0),
            Some("temp") => Ok(1),
            Some(schema) => {
                let pos = self
                    .attached_schemas
                    .iter()
                    .position(|(db, _)| db.eq_ignore_ascii_case(schema));
                if let Some(idx) = pos {
                    Ok((idx + 2) as i32)
                } else {
                    Err(Error::with_message(
                        ErrorCode::Error,
                        format!("unknown database {}", schema),
                    ))
                }
            }
        }
    }

    fn db_name_for_idx(&self, db_idx: i32) -> Option<&str> {
        match db_idx {
            0 => Some("main"),
            1 => Some("temp"),
            idx if idx > 1 => self
                .attached_schemas
                .get((idx - 2) as usize)
                .map(|(name, _)| name.as_str()),
            _ => None,
        }
    }

    /// Get the schema for a given database index
    /// 0 = main, 1 = temp, 2+ = attached databases
    fn schema_for_db_idx(&self, db_idx: i32) -> Option<&'s crate::schema::Schema> {
        match db_idx {
            0 => self.schema,
            1 => self.temp_schema,
            idx if idx > 1 => self
                .attached_schemas
                .get((idx - 2) as usize)
                .map(|(_, schema)| *schema),
            _ => None,
        }
    }

    /// Look up a table in the schema for the given database index,
    /// or search all schemas if db_idx is -1 (unqualified name)
    fn lookup_table_in_db(
        &self,
        table_name: &str,
        db_idx: i32,
    ) -> Option<std::sync::Arc<crate::schema::Table>> {
        let table_name_lower = table_name.to_lowercase();
        if db_idx >= 0 {
            // Look up in specific database schema
            self.schema_for_db_idx(db_idx)
                .and_then(|schema| schema.tables.get(&table_name_lower).cloned())
        } else {
            // Unqualified name - search all schemas (main first, then temp, then attached)
            self.schema
                .and_then(|s| s.tables.get(&table_name_lower).cloned())
                .or_else(|| {
                    self.temp_schema
                        .and_then(|s| s.tables.get(&table_name_lower).cloned())
                })
                .or_else(|| {
                    self.attached_schemas
                        .iter()
                        .find_map(|(_, schema)| schema.tables.get(&table_name_lower).cloned())
                })
        }
    }

    /// Set temp schema for TEMP tables/views
    pub fn set_temp_schema(&mut self, temp_schema: &'s crate::schema::Schema) {
        self.temp_schema = Some(temp_schema);
    }

    /// Set column naming flags from PRAGMA settings
    pub fn set_column_name_flags(&mut self, short_column_names: bool, full_column_names: bool) {
        self.short_column_names = short_column_names;
        self.full_column_names = full_column_names;
    }

    /// Set enable_view flag (from db config)
    pub fn set_enable_view(&mut self, enable: bool) {
        self.enable_view = enable;
    }

    /// Set LIKE case sensitivity for index optimization
    pub fn set_case_sensitive_like(&mut self, value: bool) {
        self.case_sensitive_like = value;
    }

    /// Set virtual table registry for xBestIndex calls
    pub fn set_vtab_registry(&mut self, registry: std::sync::Arc<crate::vtab::VtabRegistry>) {
        self.vtab_registry = Some(registry);
    }

    /// Check if a table is a virtual table with an unregistered module.
    /// Returns Err with "no such module" if the module is not registered.
    fn check_vtab_module(
        &self,
        table_name: &str,
        schema: Option<&crate::schema::Schema>,
    ) -> Result<()> {
        let schema = match schema.or(self.schema) {
            Some(s) => s,
            None => return Ok(()), // No schema to check
        };

        let table_name_lower = table_name.to_lowercase();
        let table = match schema.tables.get(&table_name_lower) {
            Some(t) => t,
            None => return Ok(()), // Table not found - let other error handling deal with it
        };

        // Only check virtual tables
        if !table.is_virtual {
            return Ok(());
        }

        // Get the module name
        let module_name = match &table.virtual_module {
            Some(m) => m,
            None => return Ok(()), // No module specified
        };

        // Check if it's a built-in module that's always available
        let is_builtin = module_name.eq_ignore_ascii_case("fts3")
            || module_name.eq_ignore_ascii_case("fts3tokenize")
            || module_name.eq_ignore_ascii_case("fts5")
            || module_name.eq_ignore_ascii_case("rtree");

        if is_builtin {
            return Ok(());
        }

        // Check if the module is registered in vtab_registry
        if let Some(ref registry) = self.vtab_registry {
            if registry.has_module(module_name) {
                return Ok(());
            }
        }

        // Module is not registered
        Err(Error::with_message(
            ErrorCode::Error,
            format!("no such module: {}", module_name),
        ))
    }

    fn make_select_compiler(&self) -> SelectCompiler<'s> {
        let mut compiler = if let Some(schema) = self.schema {
            SelectCompiler::with_schema(schema)
        } else {
            SelectCompiler::new()
        };
        if let Some(temp_schema) = self.temp_schema {
            compiler.set_temp_schema(temp_schema);
        }
        if !self.attached_schemas.is_empty() {
            compiler.set_attached_schemas(self.attached_schemas.clone());
        }
        compiler
    }

    /// Detect LIKE patterns with variable parameters that may benefit from reprepare
    /// Returns list of (param_index, is_case_sensitive) for LIKE expressions where
    /// the pattern is a variable parameter
    fn detect_like_variable_patterns(&self, stmt: &Stmt) -> Vec<(i32, bool)> {
        let mut results = Vec::new();
        self.collect_like_variables(stmt, &mut results);
        results
    }

    /// Recursively collect LIKE expressions with variable patterns
    fn collect_like_variables(&self, stmt: &Stmt, results: &mut Vec<(i32, bool)>) {
        match stmt {
            Stmt::Select(select) => {
                self.collect_like_variables_in_select(select, results);
            }
            Stmt::Update(update) => {
                if let Some(ref where_clause) = update.where_clause {
                    self.collect_like_variables_in_expr(where_clause, results);
                }
            }
            Stmt::Delete(delete) => {
                if let Some(ref where_clause) = delete.where_clause {
                    self.collect_like_variables_in_expr(where_clause, results);
                }
            }
            _ => {}
        }
    }

    fn collect_like_variables_in_select(
        &self,
        select: &SelectStmt,
        results: &mut Vec<(i32, bool)>,
    ) {
        // Process all SelectCores in the body (handles compound queries)
        self.collect_like_variables_in_body(&select.body, results);
    }

    fn collect_like_variables_in_body(&self, body: &SelectBody, results: &mut Vec<(i32, bool)>) {
        match body {
            SelectBody::Select(core) => {
                // Check WHERE clause
                if let Some(ref where_clause) = core.where_clause {
                    self.collect_like_variables_in_expr(where_clause, results);
                }

                // Check HAVING clause
                if let Some(ref having) = core.having {
                    self.collect_like_variables_in_expr(having, results);
                }
            }
            SelectBody::Compound { left, right, .. } => {
                self.collect_like_variables_in_body(left, results);
                self.collect_like_variables_in_body(right, results);
            }
        }
    }

    fn collect_like_variables_in_expr(&self, expr: &Expr, results: &mut Vec<(i32, bool)>) {
        match expr {
            Expr::Like {
                pattern,
                op,
                negated: false,
                ..
            } => {
                // Check if the pattern is a variable
                if let Some(param_idx) = self.extract_variable_index(pattern) {
                    // Determine if this is case-sensitive
                    // GLOB is always case-sensitive; LIKE depends on pragma
                    let is_case_sensitive = matches!(op, LikeOp::Glob) || self.case_sensitive_like;
                    results.push((param_idx, is_case_sensitive));
                }
            }
            // Recurse into subexpressions
            Expr::Binary { left, right, .. } => {
                self.collect_like_variables_in_expr(left, results);
                self.collect_like_variables_in_expr(right, results);
            }
            Expr::Unary { expr: inner, .. } => {
                self.collect_like_variables_in_expr(inner, results);
            }
            Expr::Between {
                expr: operand,
                low,
                high,
                ..
            } => {
                self.collect_like_variables_in_expr(operand, results);
                self.collect_like_variables_in_expr(low, results);
                self.collect_like_variables_in_expr(high, results);
            }
            Expr::In {
                expr: operand,
                list,
                ..
            } => {
                self.collect_like_variables_in_expr(operand, results);
                if let InList::Values(exprs) = list {
                    for e in exprs {
                        self.collect_like_variables_in_expr(e, results);
                    }
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
                ..
            } => {
                if let Some(op) = operand {
                    self.collect_like_variables_in_expr(op, results);
                }
                for when_clause in when_clauses {
                    self.collect_like_variables_in_expr(&when_clause.when, results);
                    self.collect_like_variables_in_expr(&when_clause.then, results);
                }
                if let Some(else_e) = else_clause {
                    self.collect_like_variables_in_expr(else_e, results);
                }
            }
            Expr::Collate { expr, .. } => {
                self.collect_like_variables_in_expr(expr, results);
            }
            Expr::Cast { expr, .. } => {
                self.collect_like_variables_in_expr(expr, results);
            }
            Expr::Function(func) => {
                if let FunctionArgs::Exprs(args) = &func.args {
                    for arg in args {
                        self.collect_like_variables_in_expr(arg, results);
                    }
                }
            }
            Expr::Subquery(select) => {
                self.collect_like_variables_in_select(select, results);
            }
            Expr::Exists { subquery, .. } => {
                self.collect_like_variables_in_select(subquery, results);
            }
            _ => {}
        }
    }

    /// Extract the parameter index if the expression is a variable
    fn extract_variable_index(&self, expr: &Expr) -> Option<i32> {
        // Unwrap Collate wrapper if present
        let inner = match expr {
            Expr::Collate { expr, .. } => expr.as_ref(),
            other => other,
        };

        match inner {
            Expr::Variable(var) => {
                // Get the parameter index from our stored names
                match var {
                    Variable::Numbered(Some(idx)) => Some(*idx),
                    Variable::Numbered(None) => {
                        // Positional parameter - find its position
                        // This is typically ?1, ?2, etc.
                        Some(1)
                    }
                    Variable::Named { prefix, name } => {
                        // Find this parameter in our list
                        // prefix is ':', '@', or '$'
                        let full_name = format!("{}{}", prefix, name);
                        for (i, param_name) in self.param_names.iter().enumerate() {
                            if let Some(pname) = param_name {
                                if pname == &full_name || pname == name {
                                    return Some((i + 1) as i32);
                                }
                            }
                        }
                        None
                    }
                }
            }
            _ => None,
        }
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

        // Detect LIKE patterns with variable parameters for potential reprepare
        let like_reprepare_info = self.detect_like_variable_patterns(&stmt);

        let compiled = CompiledStmt {
            ops,
            column_names,
            column_types,
            param_count: self.param_count,
            param_names: self.param_names.clone(),
            read_only: stmt_type.is_read_only(),
            stmt_type,
            like_reprepare_info,
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
                let mut compiler = self.make_select_compiler();
                // Pass column naming flags from PRAGMA settings
                compiler.set_column_name_flags(self.short_column_names, self.full_column_names);
                // Pass parameter names for Variable compilation
                compiler.set_param_names(self.param_names.clone());
                // Pass LIKE case sensitivity for index optimization
                compiler.set_case_sensitive_like(self.case_sensitive_like);
                // Pass enable_view flag from db config
                compiler.set_enable_view(self.enable_view);
                // Pass vtab_registry for xBestIndex calls
                if let Some(ref registry) = self.vtab_registry {
                    compiler.set_vtab_registry(registry.clone());
                }
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
                // Resolve schema: check for schema prefix (attached database)
                let target_schema: Option<&crate::schema::Schema> = if let Some(ref schema_name) =
                    insert.table.schema
                {
                    // Look up in attached_schemas
                    self.attached_schemas
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case(schema_name))
                        .map(|(_, schema)| *schema)
                        .or(self.schema) // Fallback to main if not found
                } else {
                    // Unqualified table name: search main first, then attached schemas
                    let table_name_lower = insert.table.name.to_lowercase();
                    self.schema
                        .filter(|s| s.tables.contains_key(&table_name_lower))
                        .or_else(|| {
                            self.attached_schemas
                                .iter()
                                .find(|(_, schema)| schema.tables.contains_key(&table_name_lower))
                                .map(|(_, schema)| *schema)
                        })
                        .or(self.schema) // Fallback to main if not found anywhere
                };

                // Check if target is a virtual table with unregistered module
                self.check_vtab_module(&insert.table.name, target_schema)?;

                let mut compiler = if let Some(schema) = target_schema {
                    super::insert::InsertCompiler::with_schema(schema)
                } else {
                    super::insert::InsertCompiler::new()
                };
                // Pass parameter names for Variable compilation
                compiler.set_param_names(self.param_names.clone());
                // Pass DQS_DML flag for double-quoted string handling
                compiler.set_dqs_dml(self.dqs_dml);
                // Pass temp schema for temp trigger lookup
                if let Some(temp_schema) = self.temp_schema {
                    compiler.set_temp_schema(temp_schema);
                }
                let ops = compiler.compile(insert)?;
                Ok((ops, StmtType::Insert, Vec::new(), Vec::new()))
            }

            Stmt::Update(update) => {
                // Resolve schema: check for schema prefix (attached database)
                let target_schema: Option<&crate::schema::Schema> = if let Some(ref schema_name) =
                    update.table.schema
                {
                    self.attached_schemas
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case(schema_name))
                        .map(|(_, schema)| *schema)
                        .or(self.schema)
                } else {
                    // Unqualified table name: search main first, then attached schemas
                    let table_name_lower = update.table.name.to_lowercase();
                    self.schema
                        .filter(|s| s.tables.contains_key(&table_name_lower))
                        .or_else(|| {
                            self.attached_schemas
                                .iter()
                                .find(|(_, schema)| schema.tables.contains_key(&table_name_lower))
                                .map(|(_, schema)| *schema)
                        })
                        .or(self.schema) // Fallback to main if not found anywhere
                };

                // Check if target is a virtual table with unregistered module
                self.check_vtab_module(&update.table.name, target_schema)?;

                let mut compiler = if let Some(schema) = target_schema {
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
                // Resolve schema: check for schema prefix (attached database)
                let target_schema: Option<&crate::schema::Schema> = if let Some(ref schema_name) =
                    delete.table.schema
                {
                    self.attached_schemas
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case(schema_name))
                        .map(|(_, schema)| *schema)
                        .or(self.schema)
                } else {
                    // Unqualified table name: search main first, then attached schemas
                    let table_name_lower = delete.table.name.to_lowercase();
                    self.schema
                        .filter(|s| s.tables.contains_key(&table_name_lower))
                        .or_else(|| {
                            self.attached_schemas
                                .iter()
                                .find(|(_, schema)| schema.tables.contains_key(&table_name_lower))
                                .map(|(_, schema)| *schema)
                        })
                        .or(self.schema) // Fallback to main if not found anywhere
                };

                // Check if target is a virtual table with unregistered module
                self.check_vtab_module(&delete.table.name, target_schema)?;

                let mut compiler = if let Some(schema) = target_schema {
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
                // Check if target is a virtual table with unregistered module
                self.check_vtab_module(&drop.name.name, self.schema)?;
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
    /// For compound queries, uses the leftmost SELECT for column info
    fn extract_select_columns(&self, select: &SelectStmt) -> (Vec<String>, Vec<ColumnType>) {
        let mut names = Vec::new();
        let mut types = Vec::new();

        let core = select.body.leftmost_core();
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
        // P1 = database index (0 for main, 1 for temp, 2+ attached)
        // P2 = register for root page, P3 = BTREE_INTKEY for table
        let db_idx = self.resolve_db_idx(&create.name, create.temporary)?;
        ops.push(Self::make_op(
            Opcode::CreateBtree,
            db_idx,
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
            db_idx,
            reg_root_page, // root page register
            0,
            P4::Text(create_sql.clone()),
        ));

        let cursor_id = 0;
        self.append_sqlite_master_open(&mut ops, cursor_id, db_idx);
        self.append_sqlite_master_insert(
            &mut ops,
            cursor_id,
            &create.name.name,
            reg_root_page,
            &create_sql,
        );

        // Create auto-indexes for UNIQUE column constraints
        let reg_index_page = 2; // Use a separate register for index root pages
        if let TableDefinition::Columns {
            columns,
            constraints,
        } = &create.definition
        {
            use crate::storage::btree::BTREE_BLOBKEY;

            let mut auto_idx_num = 0;

            // Create indexes for column-level UNIQUE and PRIMARY KEY constraints
            for col_def in columns {
                // Check column type for INTEGER PRIMARY KEY (rowid alias)
                let col_type_upper = col_def
                    .type_name
                    .as_ref()
                    .map(|t| t.name.to_uppercase())
                    .unwrap_or_default();
                let is_integer_type = col_type_upper == "INTEGER";

                for constraint in &col_def.constraints {
                    // Create index for UNIQUE constraint
                    let needs_index = match &constraint.kind {
                        crate::parser::ast::ColumnConstraintKind::Unique { .. } => true,
                        crate::parser::ast::ColumnConstraintKind::PrimaryKey { .. } => {
                            // PRIMARY KEY needs index unless it's INTEGER PRIMARY KEY (rowid alias)
                            !is_integer_type
                        }
                        _ => false,
                    };

                    if needs_index {
                        auto_idx_num += 1;
                        let index_name =
                            format!("sqlite_autoindex_{}_{}", create.name.name, auto_idx_num);
                        let index_sql = format!(
                            "CREATE UNIQUE INDEX {} ON {}({})",
                            index_name, create.name.name, col_def.name
                        );

                        // CreateBtree for the index (BLOBKEY for index btrees)
                        // Use same db_idx as the table
                        ops.push(Self::make_op(
                            Opcode::CreateBtree,
                            db_idx,
                            reg_index_page,
                            BTREE_BLOBKEY as i32,
                            P4::Unused,
                        ));

                        // ParseSchema to register the index in schema cache
                        ops.push(Self::make_op(
                            Opcode::ParseSchema,
                            db_idx,
                            reg_index_page,
                            0,
                            P4::Text(index_sql.clone()),
                        ));

                        // Insert into sqlite_master (auto-index has NULL SQL)
                        self.append_sqlite_master_insert_index(
                            &mut ops,
                            cursor_id,
                            &index_name,
                            &create.name.name,
                            reg_index_page,
                            None, // Auto-indexes have NULL SQL field
                        );
                    }
                }
            }

            // Create indexes for table-level UNIQUE and PRIMARY KEY constraints
            for constraint in constraints {
                // Get column names from constraint
                let (idx_cols, is_pk) = match &constraint.kind {
                    crate::parser::ast::TableConstraintKind::Unique { columns: cols, .. } => {
                        (cols.clone(), false)
                    }
                    crate::parser::ast::TableConstraintKind::PrimaryKey {
                        columns: cols, ..
                    } => {
                        // Skip if it's a single INTEGER PRIMARY KEY (rowid alias)
                        let col_names: Vec<String> = cols
                            .iter()
                            .filter_map(|c| {
                                if let crate::parser::ast::IndexedColumnKind::Name(name) = &c.column
                                {
                                    Some(name.clone())
                                } else {
                                    None
                                }
                            })
                            .collect();
                        if col_names.len() == 1 {
                            // Check if the single column is INTEGER type
                            let col_name = &col_names[0];
                            let is_integer_pk = columns.iter().any(|cd| {
                                cd.name.eq_ignore_ascii_case(col_name)
                                    && cd
                                        .type_name
                                        .as_ref()
                                        .map(|t| t.name.eq_ignore_ascii_case("INTEGER"))
                                        .unwrap_or(false)
                            });
                            if is_integer_pk {
                                continue; // Skip, rowid alias doesn't need separate index
                            }
                        }
                        (cols.clone(), true)
                    }
                    _ => continue,
                };

                auto_idx_num += 1;
                let index_name = format!("sqlite_autoindex_{}_{}", create.name.name, auto_idx_num);
                let col_names: Vec<String> = idx_cols
                    .iter()
                    .filter_map(|c| {
                        if let crate::parser::ast::IndexedColumnKind::Name(name) = &c.column {
                            Some(name.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                // Build index SQL with collation from column definitions
                let col_specs: Vec<String> = col_names
                    .iter()
                    .map(|col_name| {
                        // Find column definition to get collation
                        // SQLite uses the LAST COLLATE clause when multiple are specified
                        let collation = columns.iter().find_map(|cd| {
                            if cd.name.eq_ignore_ascii_case(col_name) {
                                // Check for COLLATE constraint - use rfind to get last one
                                cd.constraints.iter().rev().find_map(|c| {
                                    if let crate::parser::ast::ColumnConstraintKind::Collate(seq) =
                                        &c.kind
                                    {
                                        Some(seq.clone())
                                    } else {
                                        None
                                    }
                                })
                            } else {
                                None
                            }
                        });
                        if let Some(coll) = collation {
                            format!("{} COLLATE {}", col_name, coll)
                        } else {
                            col_name.clone()
                        }
                    })
                    .collect();
                let index_sql = format!(
                    "CREATE UNIQUE INDEX {} ON {}({})",
                    index_name,
                    create.name.name,
                    col_specs.join(", ")
                );

                // CreateBtree for the index (use same db_idx as the table)
                ops.push(Self::make_op(
                    Opcode::CreateBtree,
                    db_idx,
                    reg_index_page,
                    BTREE_BLOBKEY as i32,
                    P4::Unused,
                ));

                // ParseSchema to register the index
                ops.push(Self::make_op(
                    Opcode::ParseSchema,
                    db_idx,
                    reg_index_page,
                    0,
                    P4::Text(index_sql.clone()),
                ));

                // Insert into sqlite_master (auto-index has NULL SQL)
                self.append_sqlite_master_insert_index(
                    &mut ops,
                    cursor_id,
                    &index_name,
                    &create.name.name,
                    reg_index_page,
                    None, // Auto-indexes have NULL SQL field
                );

                // Mark as primary key index if it's a PK constraint
                let _ = is_pk; // Used in the match above to filter INTEGER PK
            }
        }

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
            // Start cursor allocation from 2 to avoid conflicts:
            // cursor 0 = sqlite_master, cursor 1 = target table
            select_compiler.set_next_cursor(2);

            // Compile the SELECT with Table destination to insert directly
            let dest = SelectDest::Table {
                cursor: target_cursor,
            };
            let select_ops = select_compiler.compile(select, &dest)?;

            // The SELECT compiler generates: Init(0) -> jump to body, ..., Halt
            // We need to:
            // 1. Skip the Init instruction (we already have program flow)
            // 2. Adjust all jump targets by (offset - 1) since we're skipping Init
            // 3. Replace Halt with Close + Goto to our Halt at position 1
            let offset = ops.len() as i32;
            let adjust = offset - 1; // -1 because we skip Init

            for (i, op) in select_ops.into_iter().enumerate() {
                // Skip the Init instruction at position 0
                if i == 0 && op.opcode == Opcode::Init {
                    continue;
                }

                let mut new_op = op;

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

                // Patch jump targets to account for the offset
                // All P2 jump targets need adjustment
                if new_op.opcode.is_jump() && new_op.p2 > 0 {
                    new_op.p2 += adjust;
                }
                // Some opcodes use P2 for jumps even if not classified as "jump"
                // (like Rewind, Next, etc. which jump on condition)
                if matches!(
                    new_op.opcode,
                    Opcode::Rewind
                        | Opcode::Next
                        | Opcode::Prev
                        | Opcode::IfNot
                        | Opcode::If
                        | Opcode::IfNullRow
                        | Opcode::IsNull
                        | Opcode::NotNull
                        | Opcode::Once
                        | Opcode::SorterNext
                        | Opcode::VNext
                ) && new_op.p2 > 0
                {
                    // Already handled by is_jump() check above for most,
                    // but ensure these are caught
                    if !new_op.opcode.is_jump() {
                        new_op.p2 += adjust;
                    }
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

        // Validate that the module exists
        // Built-in modules: fts3, fts5, rtree are always available
        // Other modules must be registered in the vtab_registry
        let module_name = create.module.to_lowercase();
        let is_builtin = matches!(module_name.as_str(), "fts3" | "fts5" | "rtree");

        if !is_builtin {
            // Check if module is registered
            let module_exists = self
                .vtab_registry
                .as_ref()
                .map(|registry| registry.has_module(&module_name))
                .unwrap_or(false);

            if !module_exists {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("no such module: {}", create.module),
                ));
            }
        }

        let mut ops = Vec::new();

        let reg_root_page = 1;
        ops.push(Self::make_op(Opcode::Init, 0, 2, 0, P4::Unused));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
        let db_idx = self.resolve_db_idx(&create.name, false)?;
        let sqlite_master_cursor = 0;
        self.append_sqlite_master_open(&mut ops, sqlite_master_cursor, db_idx);

        if create.module.eq_ignore_ascii_case("fts3") {
            let shadow_tables = self.build_fts3_shadow_tables(create);
            for (table_name, sql) in shadow_tables {
                ops.push(Self::make_op(
                    Opcode::CreateBtree,
                    db_idx,
                    reg_root_page,
                    BTREE_INTKEY as i32,
                    P4::Unused,
                ));
                ops.push(Self::make_op(
                    Opcode::ParseSchema,
                    db_idx,
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
                    db_idx,
                    reg_root_page,
                    BTREE_INTKEY as i32,
                    P4::Unused,
                ));
                ops.push(Self::make_op(
                    Opcode::ParseSchema,
                    db_idx,
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
        if create.module.eq_ignore_ascii_case("rtree") {
            let shadow_tables = self.build_rtree_shadow_tables(create);
            for (table_name, sql) in shadow_tables {
                ops.push(Self::make_op(
                    Opcode::CreateBtree,
                    db_idx,
                    reg_root_page,
                    BTREE_INTKEY as i32,
                    P4::Unused,
                ));
                ops.push(Self::make_op(
                    Opcode::ParseSchema,
                    db_idx,
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

        // First, create the table entry in schema via ParseSchema
        // This must happen BEFORE VCreate so VCreate can update the columns
        let create_sql = self.build_create_virtual_table_sql(create);
        ops.push(Self::make_op(
            Opcode::ParseSchema,
            db_idx,
            reg_root_page,
            0,
            P4::Text(create_sql.clone()),
        ));

        // For custom modules (not built-in), call VCreate to register the module instance
        // and update schema columns. Skip this for built-in modules that handle their own
        // initialization (fts3, fts5, rtree, etc.)
        if !is_builtin {
            let vtab_info = crate::vdbe::ops::VtabCreateInfo {
                module_name: create.module.clone(),
                table_name: create.name.name.clone(),
                db_idx,
                args: create.args.clone(),
            };
            ops.push(Self::make_op(
                Opcode::VCreate,
                db_idx,
                0,
                0,
                P4::VtabCreate(vtab_info),
            ));
        }

        // Finally, insert into sqlite_master
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
    /// For compound queries (UNION, etc.), column names come from the leftmost SELECT
    fn resolve_select_columns_for_create(&self, select: &SelectStmt) -> Vec<(String, String)> {
        let mut columns = Vec::new();

        // Use leftmost_core() to handle both simple and compound queries
        let core = select.body.leftmost_core();

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
                                    let type_str = col_def.type_name.clone().unwrap_or_default();
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

        columns
    }

    /// Build CREATE TABLE SQL from AST for storage in schema
    fn build_create_table_sql(&self, create: &CreateTableStmt) -> String {
        use crate::parser::ast::{ColumnConstraintKind, TableConstraintKind, TableDefinition};

        let mut sql = String::from("CREATE ");
        if create.temporary {
            sql.push_str("TEMP ");
        }
        sql.push_str("TABLE ");
        if create.if_not_exists {
            sql.push_str("IF NOT EXISTS ");
        }
        sql.push_str(&create.name.name);
        sql.push('(');

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
                                ColumnConstraintKind::Unique { conflict } => {
                                    col_sql.push_str(" UNIQUE");
                                    if let Some(action) = conflict {
                                        col_sql.push_str(" ON CONFLICT ");
                                        col_sql.push_str(match action {
                                            crate::parser::ast::ConflictAction::Abort => "ABORT",
                                            crate::parser::ast::ConflictAction::Rollback => {
                                                "ROLLBACK"
                                            }
                                            crate::parser::ast::ConflictAction::Fail => "FAIL",
                                            crate::parser::ast::ConflictAction::Ignore => "IGNORE",
                                            crate::parser::ast::ConflictAction::Replace => {
                                                "REPLACE"
                                            }
                                        });
                                    }
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
                                ColumnConstraintKind::Null => {
                                    col_sql.push_str(" NULL");
                                }
                                ColumnConstraintKind::Generated { expr, storage } => {
                                    col_sql.push_str(" AS(");
                                    col_sql.push_str(&self.expr_to_sql(expr));
                                    col_sql.push(')');
                                    match storage {
                                        crate::parser::ast::GeneratedStorage::Stored => {
                                            col_sql.push_str(" STORED");
                                        }
                                        crate::parser::ast::GeneratedStorage::Virtual => {
                                            // VIRTUAL is the default, can be omitted
                                        }
                                    }
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
                        TableConstraintKind::Unique { columns, conflict } => {
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
                            if let Some(action) = conflict {
                                sql.push_str(" ON CONFLICT ");
                                sql.push_str(match action {
                                    crate::parser::ast::ConflictAction::Abort => "ABORT",
                                    crate::parser::ast::ConflictAction::Rollback => "ROLLBACK",
                                    crate::parser::ast::ConflictAction::Fail => "FAIL",
                                    crate::parser::ast::ConflictAction::Ignore => "IGNORE",
                                    crate::parser::ast::ConflictAction::Replace => "REPLACE",
                                });
                            }
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

    /// Build R-tree shadow tables for persistence
    ///
    /// SQLite R-tree uses three shadow tables:
    /// - `{name}_node`: stores R-tree node data blobs
    /// - `{name}_rowid`: maps user rowids to leaf node numbers
    /// - `{name}_parent`: maps child nodes to parent nodes
    fn build_rtree_shadow_tables(&self, create: &CreateVirtualTableStmt) -> Vec<(String, String)> {
        let mut tables = Vec::new();
        let name = &create.name.name;

        // _node: stores R-tree node data blobs
        // Root node is always nodeno=1
        let node_name = format!("{}_node", name);
        tables.push((
            node_name.clone(),
            format!(
                "CREATE TABLE {} (nodeno INTEGER PRIMARY KEY, data BLOB)",
                node_name
            ),
        ));

        // _rowid: maps user rowids to the leaf node containing them
        let rowid_name = format!("{}_rowid", name);
        tables.push((
            rowid_name.clone(),
            format!(
                "CREATE TABLE {} (rowid INTEGER PRIMARY KEY, nodeno INTEGER)",
                rowid_name
            ),
        ));

        // _parent: maps each node to its parent node (root has parent=NULL or 0)
        let parent_name = format!("{}_parent", name);
        tables.push((
            parent_name.clone(),
            format!(
                "CREATE TABLE {} (nodeno INTEGER PRIMARY KEY, parentnode INTEGER)",
                parent_name
            ),
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

    fn append_sqlite_master_open(&self, ops: &mut Vec<VdbeOp>, cursor_id: i32, db_idx: i32) {
        let table_name = match self.db_name_for_idx(db_idx) {
            Some("temp") => "sqlite_temp_master".to_string(),
            Some(db_name) if db_idx > 1 => format!("{}.sqlite_master", db_name),
            _ => "sqlite_master".to_string(),
        };
        ops.push(Self::make_op(
            Opcode::OpenWrite,
            cursor_id,
            1,
            5,
            P4::Text(table_name),
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
        // Use high register numbers to avoid conflicts with caller's registers
        // (e.g., reg_root_page=1, reg_index_page=2 in compile_create_table)
        let reg_type = 20;
        let reg_name = 21;
        let reg_tbl = 22;
        let reg_root = 23;
        let reg_sql = 24;
        let reg_record = 25;
        let reg_rowid = 26;
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

    /// Delete entries from sqlite_master where name matches
    fn append_sqlite_master_delete(
        &self,
        ops: &mut Vec<VdbeOp>,
        cursor_id: i32,
        name: &str,
        kind: &str,
    ) {
        // sqlite_master columns: type, name, tbl_name, rootpage, sql
        // We need to scan and find rows where name (column 1) matches AND type (column 0) matches

        // Use high register numbers to avoid conflicts
        let reg_type_col = 30;
        let reg_name_col = 31;
        let reg_target_name = 32;
        let reg_target_type = 33;

        // Store the target name and type to match
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_target_name,
            0,
            P4::Text(name.to_string()),
        ));
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_target_type,
            0,
            P4::Text(kind.to_string()),
        ));

        // Rewind to start of sqlite_master
        let rewind_addr = ops.len();
        let end_label = 0; // Will be fixed later
        ops.push(Self::make_op(
            Opcode::Rewind,
            cursor_id,
            end_label,
            0,
            P4::Unused,
        ));

        // Loop: read type column (column 0) and name column (column 1)
        let loop_start = ops.len();
        ops.push(Self::make_op(
            Opcode::Column,
            cursor_id,
            0,
            reg_type_col,
            P4::Unused,
        ));
        ops.push(Self::make_op(
            Opcode::Column,
            cursor_id,
            1,
            reg_name_col,
            P4::Unused,
        ));

        // Compare type first - skip if type doesn't match
        // next_label points to the Next instruction: current + 3 ops (Ne, Ne, Delete)
        let next_label = ops.len() + 3;
        ops.push(Self::make_op(
            Opcode::Ne,
            reg_type_col,
            next_label as i32,
            reg_target_type,
            P4::Unused,
        ));

        // Compare with target name - skip if name doesn't match
        // Still jump to the same Next instruction
        let next_label2 = ops.len() + 2; // current + 2 ops (Ne, Delete)
        ops.push(Self::make_op(
            Opcode::Ne,
            reg_name_col,
            next_label2 as i32,
            reg_target_name,
            P4::Unused,
        ));

        // Delete the current row
        ops.push(Self::make_op(Opcode::Delete, cursor_id, 0, 0, P4::Unused));

        // Next row
        ops.push(Self::make_op(
            Opcode::Next,
            cursor_id,
            loop_start as i32,
            0,
            P4::Unused,
        ));

        // End of loop - fix the Rewind jump address
        let end_addr = ops.len();
        ops[rewind_addr].p2 = end_addr as i32;
    }

    /// Delete entries from sqlite_master where tbl_name matches (for cascading drops)
    /// This is used when dropping a table to also drop its indexes and triggers
    fn append_sqlite_master_delete_by_tbl_name(
        &self,
        ops: &mut Vec<VdbeOp>,
        cursor_id: i32,
        tbl_name: &str,
    ) {
        // sqlite_master columns: type, name, tbl_name, rootpage, sql
        // We need to scan and find rows where tbl_name (column 2) matches
        // and type is 'trigger' or 'index'

        // Use high register numbers to avoid conflicts
        let reg_type_col = 32;
        let reg_tbl_name_col = 33;
        let reg_target = 34;
        let reg_trigger_type = 35;
        let reg_index_type = 36;

        // Store the target tbl_name to match
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_target,
            0,
            P4::Text(tbl_name.to_string()),
        ));

        // Store "trigger" and "index" for type comparison
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_trigger_type,
            0,
            P4::Text("trigger".to_string()),
        ));
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_index_type,
            0,
            P4::Text("index".to_string()),
        ));

        // Rewind to start of sqlite_master
        let rewind_addr = ops.len();
        let end_label = 0; // Will be fixed later
        ops.push(Self::make_op(
            Opcode::Rewind,
            cursor_id,
            end_label,
            0,
            P4::Unused,
        ));

        // Loop: read type column (column 0) and tbl_name column (column 2)
        let loop_start = ops.len();
        ops.push(Self::make_op(
            Opcode::Column,
            cursor_id,
            0,
            reg_type_col,
            P4::Unused,
        ));
        ops.push(Self::make_op(
            Opcode::Column,
            cursor_id,
            2,
            reg_tbl_name_col,
            P4::Unused,
        ));

        // Skip if tbl_name doesn't match - jump to Next instruction
        // After this Ne, we have: Ne(trigger), Delete, Ne(index), Delete, Next
        // That's 5 more ops, so Next is at current + 5
        let next_label = ops.len() + 5;
        ops.push(Self::make_op(
            Opcode::Ne,
            reg_tbl_name_col,
            next_label as i32,
            reg_target,
            P4::Unused,
        ));

        // Check if type is 'trigger' - if so, delete
        let check_index_label = ops.len() + 2;
        ops.push(Self::make_op(
            Opcode::Ne,
            reg_type_col,
            check_index_label as i32,
            reg_trigger_type,
            P4::Unused,
        ));

        // Delete the trigger row
        ops.push(Self::make_op(Opcode::Delete, cursor_id, 0, 0, P4::Unused));

        // Check if type is 'index' - if so, delete
        let next_label2 = ops.len() + 2;
        ops.push(Self::make_op(
            Opcode::Ne,
            reg_type_col,
            next_label2 as i32,
            reg_index_type,
            P4::Unused,
        ));

        // Delete the index row
        ops.push(Self::make_op(Opcode::Delete, cursor_id, 0, 0, P4::Unused));

        // Next row
        ops.push(Self::make_op(
            Opcode::Next,
            cursor_id,
            loop_start as i32,
            0,
            P4::Unused,
        ));

        // End of loop - fix the Rewind jump address
        let end_addr = ops.len();
        ops[rewind_addr].p2 = end_addr as i32;
    }

    fn compile_create_index(&mut self, create: &CreateIndexStmt) -> Result<Vec<VdbeOp>> {
        use crate::storage::btree::BTREE_BLOBKEY;

        let index_name = &create.name.name;
        let index_name_lower = index_name.to_lowercase();
        let table_name = &create.table;
        let table_name_lower = table_name.to_lowercase();
        let db_idx = self.resolve_db_idx(&create.name, false)?;

        // Get the schema for the target database
        let target_schema = self.schema_for_db_idx(db_idx);

        // Check if index already exists (in the target schema)
        if let Some(schema) = target_schema {
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

            // Check if target is a view - cannot create index on views
            if schema.views.contains_key(&table_name_lower) {
                return Err(crate::error::Error::with_message(
                    crate::error::ErrorCode::Error,
                    "views may not be indexed".to_string(),
                ));
            }

            // Check if target table exists in the target schema
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
                // Extract column name and collation, handling both Name and Expr cases
                // When the parser sees "x COLLATE NOCASE", it may parse it as
                // Expr::Collate { expr: Column("x"), collation: "NOCASE" }
                let (name, collation) = match &c.column {
                    crate::parser::ast::IndexedColumnKind::Name(n) => {
                        (n.clone(), c.collation.clone())
                    }
                    crate::parser::ast::IndexedColumnKind::Expr(e) => {
                        // Check if it's a Collate expression wrapping a column
                        if let Expr::Collate { expr, collation } = e.as_ref() {
                            if let Expr::Column(col) = expr.as_ref() {
                                // Column with collation parsed as expression
                                (col.column.clone(), Some(collation.clone()))
                            } else {
                                // Complex expression with collation
                                (
                                    format!("{} COLLATE {}", self.expr_to_sql(expr), collation),
                                    None,
                                )
                            }
                        } else {
                            (self.expr_to_sql(e), c.collation.clone())
                        }
                    }
                };
                // Build column spec with optional COLLATE and ORDER
                let with_collate = if let Some(ref coll) = collation {
                    format!("{} COLLATE {}", name, coll)
                } else {
                    name
                };
                match c.order {
                    Some(crate::parser::ast::SortOrder::Asc) => format!("{} ASC", with_collate),
                    Some(crate::parser::ast::SortOrder::Desc) => format!("{} DESC", with_collate),
                    None => with_collate,
                }
            })
            .collect();
        // Build WHERE clause for partial indexes
        let where_str = if let Some(ref where_clause) = create.where_clause {
            format!(" WHERE {}", self.expr_to_sql(where_clause))
        } else {
            String::new()
        };
        let sql = format!(
            "CREATE {}INDEX {}{} ON {}({}){}",
            unique_str,
            if_not_exists_str,
            index_name,
            table_name,
            columns_str.join(", "),
            where_str
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
            db_idx,
            reg_root_page,
            BTREE_BLOBKEY as i32,
            P4::Unused,
        ));

        // ParseSchema to register the index in schema cache
        ops.push(Self::make_op(
            Opcode::ParseSchema,
            db_idx,
            reg_root_page,
            0,
            P4::Text(sql.clone()),
        ));

        // Populate the index with existing table data
        // Get column indices and collations for the indexed columns
        let (indexed_col_indices, index_collations): (Vec<usize>, Vec<String>) =
            if let Some(schema) = self.schema {
                if let Some(table) = schema.tables.get(&table_name_lower) {
                    create
                        .columns
                        .iter()
                        .filter_map(|c| {
                            // Extract column name and explicit collation, handling both Name and Expr cases
                            let (col_name, explicit_collation) = match &c.column {
                                crate::parser::ast::IndexedColumnKind::Name(n) => {
                                    (n.clone(), c.collation.clone())
                                }
                                crate::parser::ast::IndexedColumnKind::Expr(e) => {
                                    // Handle Collate expression wrapping a column
                                    if let Expr::Collate { expr, collation } = e.as_ref() {
                                        if let Expr::Column(col) = expr.as_ref() {
                                            (col.column.clone(), Some(collation.clone()))
                                        } else {
                                            return None;
                                        }
                                    } else if let Expr::Column(col) = e.as_ref() {
                                        (col.column.clone(), c.collation.clone())
                                    } else {
                                        return None;
                                    }
                                }
                            };
                            let col_idx = table
                                .columns
                                .iter()
                                .position(|tc| tc.name.eq_ignore_ascii_case(&col_name))?;
                            // Determine collation: explicit COLLATE > table column > BINARY
                            let collation = explicit_collation.unwrap_or_else(|| {
                                table
                                    .columns
                                    .get(col_idx)
                                    .map(|c| c.collation.to_uppercase())
                                    .unwrap_or_else(|| "BINARY".to_string())
                            });
                            Some((col_idx, collation))
                        })
                        .unzip()
                } else {
                    (Vec::new(), Vec::new())
                }
            } else {
                (Vec::new(), Vec::new())
            };

        if !indexed_col_indices.is_empty() {
            let table_cursor = 1;
            let index_cursor = 2;
            let num_key_cols = indexed_col_indices.len();
            let reg_col_base = 10;
            let reg_rowid = reg_col_base + num_key_cols as i32;
            let reg_record = reg_rowid + 1;

            // Calculate after_loop: where to jump if table is empty
            // Opcodes in the loop section:
            //   OpenRead(1) + OpenWrite(1) + Rewind(1) + Column*n(n) +
            //   Rowid(1) + MakeRecord(1) + IdxInsert(1) + Next(1) + Close(1) + Close(1)
            // = 9 + num_key_cols
            let after_loop = ops.len() as i32 + 9 + num_key_cols as i32;

            // OpenRead table cursor
            ops.push(Self::make_op(
                Opcode::OpenRead,
                table_cursor,
                0,
                0,
                P4::Text(table_name.to_string()),
            ));

            // Build KeyInfo with collations for the index
            use crate::vdbe::ops::KeyInfo;
            let key_info = KeyInfo {
                collations: index_collations.clone(),
                sort_orders: vec![false; index_collations.len()],
                bignull: vec![false; index_collations.len()],
                n_key_field: index_collations.len() as u16,
            };

            // OpenWrite index cursor using root page from register
            let keyinfo_p5 = 0x02 | ((db_idx as u16) << 8);
            ops.push(Self::make_op_with_p5(
                Opcode::OpenWrite,
                index_cursor,
                reg_root_page,
                (num_key_cols + 1) as i32, // +1 for rowid
                P4::KeyInfo(std::sync::Arc::new(key_info)),
                keyinfo_p5, // P2 is register; high bits carry db_idx
            ));

            // Rewind table cursor - jump to after_loop if table is empty
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

        // Insert into sqlite_master (explicit CREATE INDEX has SQL)
        let cursor_id = 0;
        self.append_sqlite_master_open(&mut ops, cursor_id, db_idx);
        self.append_sqlite_master_insert_index(
            &mut ops,
            cursor_id,
            index_name,
            table_name,
            reg_root_page,
            Some(&sql), // Explicit indexes have their CREATE INDEX SQL
        );
        self.append_sqlite_master_close(&mut ops, cursor_id);

        // Goto end
        ops.push(Self::make_op(Opcode::Goto, 0, 1, 0, P4::Unused));
        Ok(ops)
    }

    /// Insert an index entry into sqlite_master
    /// If `create_sql` is None, this is an auto-index and SQL field should be NULL
    fn append_sqlite_master_insert_index(
        &self,
        ops: &mut Vec<VdbeOp>,
        cursor_id: i32,
        index_name: &str,
        table_name: &str,
        reg_root_page: i32,
        create_sql: Option<&str>,
    ) {
        // Use high register numbers to avoid conflicts with caller's registers
        // (e.g., reg_root_page=1, reg_index_page=2 in compile_create_table)
        let reg_type = 20;
        let reg_name = 21;
        let reg_tbl = 22;
        let reg_root = 23;
        let reg_sql = 24;
        let reg_record = 25;
        let reg_rowid = 26;

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
        // sql = CREATE INDEX statement (or NULL for auto-indexes)
        if let Some(sql) = create_sql {
            ops.push(Self::make_op(
                Opcode::String8,
                0,
                reg_sql,
                0,
                P4::Text(sql.to_string()),
            ));
        } else {
            // Auto-indexes have NULL SQL field
            ops.push(Self::make_op(Opcode::Null, 0, reg_sql, 0, P4::Unused));
        }
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
        // Check for parameters in the view definition - not allowed
        if Self::select_has_parameters(&create.query) {
            return Err(crate::error::Error::with_message(
                crate::error::ErrorCode::Error,
                "parameters are not allowed in views".to_string(),
            ));
        }

        // Check for references to objects in other databases (not allowed for non-temp views)
        // Views in main cannot reference tables in attached databases
        if !create.temporary {
            if let Some(db_name) = Self::select_references_other_database(&create.query) {
                return Err(crate::error::Error::with_message(
                    crate::error::ErrorCode::Error,
                    format!(
                        "view {} cannot reference objects in database {}",
                        create.name.name, db_name
                    ),
                ));
            }
        }

        // Reconstruct the CREATE VIEW SQL for storage
        // For non-temp views, this includes all keywords
        // For temp views, we strip the TEMP keyword since it's stored in temp schema
        let sql = self.reconstruct_create_view_sql(create);

        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 2, 0, P4::Unused));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));

        // Only insert into sqlite_master for non-temp views
        // Temp views are transient and only exist in the in-memory schema
        let db_idx = self.resolve_db_idx(&create.name, create.temporary)?;
        if !create.temporary {
            let cursor_id = 0;
            self.append_sqlite_master_open(&mut ops, cursor_id, db_idx);
            self.append_sqlite_master_insert_view(&mut ops, cursor_id, &create.name.name, &sql);
            self.append_sqlite_master_close(&mut ops, cursor_id);
        }

        // Use ParseSchema to register the view in the schema at runtime
        // P1=db_idx (0 for main, 1 for temp)
        // P2=0 (views don't need a root page), P4=SQL text
        ops.push(Self::make_op(
            Opcode::ParseSchema,
            db_idx,
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
        // Use high register numbers to avoid conflicts with caller's registers
        let reg_type = 20;
        let reg_name = 21;
        let reg_tbl_name = 22;
        let reg_rootpage = 23;
        let reg_sql = 24;
        let reg_record = 25;
        let reg_rowid = 26;

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
        // Determine the database for the trigger
        let trigger_db_idx = self.resolve_db_idx(&create.name, create.temporary)?;

        // Check if the trigger's target table has a schema prefix that differs from the trigger's database
        // SQLite rule: Triggers can only reference tables in the same database as the trigger
        // (TEMP triggers are an exception - they can reference any database, but we validate below)
        if let Some(ref table_schema) = create.table_schema {
            // Get the database name for the trigger
            let trigger_db_name = self.db_name_for_idx(trigger_db_idx);

            // Check if the table's schema matches the trigger's database
            let table_schema_lower = table_schema.to_lowercase();
            let matches = match trigger_db_name {
                Some(name) => name.eq_ignore_ascii_case(table_schema),
                None => false,
            };

            if !matches {
                // The table is in a different database - this is not allowed
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!(
                        "trigger {} cannot reference objects in database {}",
                        create.name.name, table_schema
                    ),
                ));
            }
        }

        // Also validate the trigger body for cross-database references
        // Triggers cannot reference objects in other databases (except TEMP triggers)
        if !create.temporary {
            if let Err(e) = self.validate_trigger_body_references(create, trigger_db_idx) {
                return Err(e);
            }
        }

        // Check if the target table is in the temp schema
        // Triggers on temp tables are implicitly temp triggers
        let table_lower = create.table.to_lowercase();
        let table_in_temp = self
            .temp_schema
            .map(|s| s.tables.contains_key(&table_lower) || s.views.contains_key(&table_lower))
            .unwrap_or(false);
        let is_temp_trigger = create.temporary || table_in_temp;

        // Adjust db_idx if target table is in temp schema
        let actual_db_idx = if table_in_temp { 1 } else { trigger_db_idx };

        // Reconstruct the CREATE TRIGGER SQL for storage
        // This preserves the original SQL text for later parsing
        let sql = self.reconstruct_create_trigger_sql(create);

        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 2, 0, P4::Unused));
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));

        // Only insert into sqlite_master for non-temp triggers
        // Temp triggers are transient and only exist in the in-memory schema
        if !is_temp_trigger {
            let cursor_id = 0;
            self.append_sqlite_master_open(&mut ops, cursor_id, actual_db_idx);
            self.append_sqlite_master_insert_trigger(
                &mut ops,
                cursor_id,
                &create.name.name,
                &create.table,
                &sql,
            );
            self.append_sqlite_master_close(&mut ops, cursor_id);
        }

        // Use ParseSchema to register the trigger in the schema at runtime
        // P2=0 (triggers don't need a root page), P4=SQL text
        ops.push(Self::make_op(
            Opcode::ParseSchema,
            actual_db_idx,
            0,
            0,
            P4::Text(sql.clone()),
        ));
        ops.push(Self::make_op(Opcode::Goto, 0, 1, 0, P4::Unused));

        Ok(ops)
    }

    /// Insert a trigger entry into sqlite_master
    fn append_sqlite_master_insert_trigger(
        &self,
        ops: &mut Vec<VdbeOp>,
        cursor_id: i32,
        trigger_name: &str,
        table_name: &str,
        sql: &str,
    ) {
        // sqlite_master columns: type, name, tbl_name, rootpage, sql
        // Triggers have type='trigger', tbl_name=table the trigger is on, rootpage=0
        // Use high register numbers to avoid conflicts with caller's registers
        let reg_type = 20;
        let reg_name = 21;
        let reg_tbl_name = 22;
        let reg_rootpage = 23;
        let reg_sql = 24;
        let reg_record = 25;
        let reg_rowid = 26;

        // type = 'trigger'
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_type,
            0,
            P4::Text("trigger".to_string()),
        ));
        // name = trigger_name
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_name,
            0,
            P4::Text(trigger_name.to_string()),
        ));
        // tbl_name = table the trigger is on
        ops.push(Self::make_op(
            Opcode::String8,
            0,
            reg_tbl_name,
            0,
            P4::Text(table_name.to_string()),
        ));
        // rootpage = 0 (triggers don't have a root page)
        ops.push(Self::make_op(
            Opcode::Integer,
            0,
            reg_rootpage,
            0,
            P4::Unused,
        ));
        // sql = CREATE TRIGGER statement
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

    /// Validate that trigger body doesn't reference objects in other databases
    fn validate_trigger_body_references(
        &self,
        create: &CreateTriggerStmt,
        trigger_db_idx: i32,
    ) -> Result<()> {
        let trigger_db_name = self.db_name_for_idx(trigger_db_idx);

        for stmt in &create.body {
            if let Some(invalid_db) = self.find_invalid_db_reference_in_stmt(stmt, trigger_db_name)
            {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!(
                        "trigger {} cannot reference objects in database {}",
                        create.name.name, invalid_db
                    ),
                ));
            }
        }

        // Also check the WHEN clause
        if let Some(ref when_expr) = create.when {
            if let Some(invalid_db) =
                self.find_invalid_db_reference_in_expr(when_expr, trigger_db_name)
            {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!(
                        "trigger {} cannot reference objects in database {}",
                        create.name.name, invalid_db
                    ),
                ));
            }
        }

        Ok(())
    }

    /// Find an invalid database reference in a statement
    fn find_invalid_db_reference_in_stmt(
        &self,
        stmt: &crate::parser::ast::Stmt,
        trigger_db_name: Option<&str>,
    ) -> Option<String> {
        use crate::parser::ast::Stmt;

        match stmt {
            Stmt::Select(select) => self.find_invalid_db_in_select(select, trigger_db_name),
            Stmt::Insert(insert) => {
                // Check table reference
                if let Some(ref schema) = insert.table.schema {
                    if !self.db_name_matches(trigger_db_name, schema) {
                        return Some(schema.clone());
                    }
                }
                // Check source
                if let crate::parser::ast::InsertSource::Select(select) = &insert.source {
                    return self.find_invalid_db_in_select(select, trigger_db_name);
                }
                if let crate::parser::ast::InsertSource::Values(rows) = &insert.source {
                    for row in rows {
                        for expr in row {
                            if let Some(db) =
                                self.find_invalid_db_reference_in_expr(expr, trigger_db_name)
                            {
                                return Some(db);
                            }
                        }
                    }
                }
                None
            }
            Stmt::Update(update) => {
                // Check table reference
                if let Some(ref schema) = update.table.schema {
                    if !self.db_name_matches(trigger_db_name, schema) {
                        return Some(schema.clone());
                    }
                }
                // Check WHERE clause
                if let Some(ref where_expr) = update.where_clause {
                    if let Some(db) =
                        self.find_invalid_db_reference_in_expr(where_expr, trigger_db_name)
                    {
                        return Some(db);
                    }
                }
                // Check assignments
                for assign in &update.assignments {
                    if let Some(db) =
                        self.find_invalid_db_reference_in_expr(&assign.expr, trigger_db_name)
                    {
                        return Some(db);
                    }
                }
                None
            }
            Stmt::Delete(delete) => {
                // Check table reference
                if let Some(ref schema) = delete.table.schema {
                    if !self.db_name_matches(trigger_db_name, schema) {
                        return Some(schema.clone());
                    }
                }
                // Check WHERE clause
                if let Some(ref where_expr) = delete.where_clause {
                    if let Some(db) =
                        self.find_invalid_db_reference_in_expr(where_expr, trigger_db_name)
                    {
                        return Some(db);
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Find an invalid database reference in a SELECT statement
    fn find_invalid_db_in_select(
        &self,
        select: &crate::parser::ast::SelectStmt,
        trigger_db_name: Option<&str>,
    ) -> Option<String> {
        // Check all SELECT cores
        for core in select.body.all_cores() {
            // Check FROM clause
            if let Some(ref from) = core.from {
                if let Some(db) = self.find_invalid_db_in_from(from, trigger_db_name) {
                    return Some(db);
                }
            }

            // Check result columns for expressions
            for col in &core.columns {
                if let crate::parser::ast::ResultColumn::Expr { expr, .. } = col {
                    if let Some(db) = self.find_invalid_db_reference_in_expr(expr, trigger_db_name)
                    {
                        return Some(db);
                    }
                }
            }

            // Check WHERE clause
            if let Some(ref where_expr) = core.where_clause {
                if let Some(db) =
                    self.find_invalid_db_reference_in_expr(where_expr, trigger_db_name)
                {
                    return Some(db);
                }
            }

            // Check GROUP BY
            if let Some(ref group_by) = core.group_by {
                for expr in group_by {
                    if let Some(db) = self.find_invalid_db_reference_in_expr(expr, trigger_db_name)
                    {
                        return Some(db);
                    }
                }
            }

            // Check HAVING
            if let Some(ref having) = core.having {
                if let Some(db) = self.find_invalid_db_reference_in_expr(having, trigger_db_name) {
                    return Some(db);
                }
            }
        }

        None
    }

    /// Find an invalid database reference in a FROM clause
    fn find_invalid_db_in_from(
        &self,
        from: &crate::parser::ast::FromClause,
        trigger_db_name: Option<&str>,
    ) -> Option<String> {
        for table_ref in &from.tables {
            if let Some(db) = self.find_invalid_db_in_table_ref(table_ref, trigger_db_name) {
                return Some(db);
            }
        }
        None
    }

    /// Find an invalid database reference in a table reference
    fn find_invalid_db_in_table_ref(
        &self,
        table_ref: &crate::parser::ast::TableRef,
        trigger_db_name: Option<&str>,
    ) -> Option<String> {
        use crate::parser::ast::TableRef;

        match table_ref {
            TableRef::Table { name, .. } => {
                if let Some(ref schema) = name.schema {
                    if !self.db_name_matches(trigger_db_name, schema) {
                        return Some(schema.clone());
                    }
                }
                None
            }
            TableRef::Subquery { query, .. } => {
                self.find_invalid_db_in_select(query, trigger_db_name)
            }
            TableRef::Join {
                left,
                right,
                constraint,
                ..
            } => {
                if let Some(db) = self.find_invalid_db_in_table_ref(left, trigger_db_name) {
                    return Some(db);
                }
                if let Some(db) = self.find_invalid_db_in_table_ref(right, trigger_db_name) {
                    return Some(db);
                }
                if let Some(crate::parser::ast::JoinConstraint::On(expr)) = constraint {
                    if let Some(db) = self.find_invalid_db_reference_in_expr(expr, trigger_db_name)
                    {
                        return Some(db);
                    }
                }
                None
            }
            TableRef::TableFunction { .. } => None,
            TableRef::Parens(inner) => self.find_invalid_db_in_table_ref(inner, trigger_db_name),
        }
    }

    /// Find an invalid database reference in an expression
    fn find_invalid_db_reference_in_expr(
        &self,
        expr: &crate::parser::ast::Expr,
        trigger_db_name: Option<&str>,
    ) -> Option<String> {
        use crate::parser::ast::{Expr, FunctionArgs, InList};

        match expr {
            Expr::Column(col_ref) => {
                // Check if column has a database prefix that differs from trigger's database
                if let Some(ref database) = col_ref.database {
                    if !self.db_name_matches(trigger_db_name, database) {
                        return Some(database.clone());
                    }
                }
                None
            }
            Expr::Binary { left, right, .. } => {
                if let Some(db) = self.find_invalid_db_reference_in_expr(left, trigger_db_name) {
                    return Some(db);
                }
                self.find_invalid_db_reference_in_expr(right, trigger_db_name)
            }
            Expr::Unary { expr: inner, .. } => {
                self.find_invalid_db_reference_in_expr(inner, trigger_db_name)
            }
            Expr::Parens(inner) => self.find_invalid_db_reference_in_expr(inner, trigger_db_name),
            Expr::Function(func_call) => {
                if let FunctionArgs::Exprs(args) = &func_call.args {
                    for arg in args {
                        if let Some(db) =
                            self.find_invalid_db_reference_in_expr(arg, trigger_db_name)
                        {
                            return Some(db);
                        }
                    }
                }
                // Check filter clause
                if let Some(ref filter) = func_call.filter {
                    if let Some(db) =
                        self.find_invalid_db_reference_in_expr(filter, trigger_db_name)
                    {
                        return Some(db);
                    }
                }
                None
            }
            Expr::Subquery(select) => self.find_invalid_db_in_select(select, trigger_db_name),
            Expr::In { expr, list, .. } => {
                if let Some(db) = self.find_invalid_db_reference_in_expr(expr, trigger_db_name) {
                    return Some(db);
                }
                match list {
                    InList::Values(exprs) => {
                        for e in exprs {
                            if let Some(db) =
                                self.find_invalid_db_reference_in_expr(e, trigger_db_name)
                            {
                                return Some(db);
                            }
                        }
                    }
                    InList::Subquery(select) => {
                        return self.find_invalid_db_in_select(select, trigger_db_name);
                    }
                    InList::Table(qname) => {
                        if let Some(ref schema) = qname.schema {
                            if !self.db_name_matches(trigger_db_name, schema) {
                                return Some(schema.clone());
                            }
                        }
                    }
                }
                None
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                if let Some(db) = self.find_invalid_db_reference_in_expr(expr, trigger_db_name) {
                    return Some(db);
                }
                if let Some(db) = self.find_invalid_db_reference_in_expr(low, trigger_db_name) {
                    return Some(db);
                }
                self.find_invalid_db_reference_in_expr(high, trigger_db_name)
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(ref op) = operand {
                    if let Some(db) = self.find_invalid_db_reference_in_expr(op, trigger_db_name) {
                        return Some(db);
                    }
                }
                for when_clause in when_clauses {
                    if let Some(db) =
                        self.find_invalid_db_reference_in_expr(&when_clause.when, trigger_db_name)
                    {
                        return Some(db);
                    }
                    if let Some(db) =
                        self.find_invalid_db_reference_in_expr(&when_clause.then, trigger_db_name)
                    {
                        return Some(db);
                    }
                }
                if let Some(ref else_expr) = else_clause {
                    return self.find_invalid_db_reference_in_expr(else_expr, trigger_db_name);
                }
                None
            }
            Expr::Cast { expr: inner, .. } => {
                self.find_invalid_db_reference_in_expr(inner, trigger_db_name)
            }
            Expr::Collate { expr: inner, .. } => {
                self.find_invalid_db_reference_in_expr(inner, trigger_db_name)
            }
            Expr::Exists { subquery, .. } => {
                self.find_invalid_db_in_select(subquery, trigger_db_name)
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                if let Some(db) = self.find_invalid_db_reference_in_expr(expr, trigger_db_name) {
                    return Some(db);
                }
                if let Some(db) = self.find_invalid_db_reference_in_expr(pattern, trigger_db_name) {
                    return Some(db);
                }
                if let Some(ref esc) = escape {
                    return self.find_invalid_db_reference_in_expr(esc, trigger_db_name);
                }
                None
            }
            Expr::IsNull { expr, .. } => {
                self.find_invalid_db_reference_in_expr(expr, trigger_db_name)
            }
            Expr::IsDistinct { left, right, .. } => {
                if let Some(db) = self.find_invalid_db_reference_in_expr(left, trigger_db_name) {
                    return Some(db);
                }
                self.find_invalid_db_reference_in_expr(right, trigger_db_name)
            }
            _ => None,
        }
    }

    /// Check if a database name matches
    fn db_name_matches(&self, trigger_db_name: Option<&str>, schema: &str) -> bool {
        // "temp" references are always invalid unless the trigger is also in temp
        if schema.eq_ignore_ascii_case("temp") {
            return trigger_db_name == Some("temp");
        }

        match trigger_db_name {
            Some(name) => name.eq_ignore_ascii_case(schema),
            None => false,
        }
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
        sql.push_str(create.name.display_name());
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
        if let Some(ref schema) = create.table_schema {
            sql.push_str(schema);
            sql.push('.');
        }
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

    /// Check if a SELECT statement contains any parameters (? or :name)
    fn select_has_parameters(select: &SelectStmt) -> bool {
        // Check the select body
        if Self::select_body_has_parameters(&select.body) {
            return true;
        }

        // Check ORDER BY
        if let Some(ref order_by) = select.order_by {
            for ord in order_by {
                if Self::expr_has_parameters(&ord.expr) {
                    return true;
                }
            }
        }

        // Check LIMIT/OFFSET
        if let Some(ref limit_clause) = select.limit {
            if Self::expr_has_parameters(&limit_clause.limit) {
                return true;
            }
            if let Some(ref offset) = limit_clause.offset {
                if Self::expr_has_parameters(offset) {
                    return true;
                }
            }
        }

        false
    }

    /// Check if a SelectBody contains parameters
    fn select_body_has_parameters(body: &crate::parser::ast::SelectBody) -> bool {
        match body {
            crate::parser::ast::SelectBody::Select(core) => Self::select_core_has_parameters(core),
            crate::parser::ast::SelectBody::Compound { left, right, .. } => {
                Self::select_body_has_parameters(left) || Self::select_body_has_parameters(right)
            }
        }
    }

    /// Check if a SelectCore contains parameters
    fn select_core_has_parameters(core: &crate::parser::ast::SelectCore) -> bool {
        // Check result columns
        for col in &core.columns {
            match col {
                crate::parser::ast::ResultColumn::Expr { expr, .. } => {
                    if Self::expr_has_parameters(expr) {
                        return true;
                    }
                }
                crate::parser::ast::ResultColumn::Star
                | crate::parser::ast::ResultColumn::TableStar(_) => {}
            }
        }

        // Check FROM clause
        if let Some(ref from) = core.from {
            if Self::from_has_parameters(from) {
                return true;
            }
        }

        // Check WHERE clause
        if let Some(ref where_expr) = core.where_clause {
            if Self::expr_has_parameters(where_expr) {
                return true;
            }
        }

        // Check GROUP BY
        if let Some(ref group_by) = core.group_by {
            for expr in group_by {
                if Self::expr_has_parameters(expr) {
                    return true;
                }
            }
        }

        // Check HAVING
        if let Some(ref having) = core.having {
            if Self::expr_has_parameters(having) {
                return true;
            }
        }

        false
    }

    /// Check if a FROM clause contains parameters (in subqueries)
    fn from_has_parameters(from: &crate::parser::ast::FromClause) -> bool {
        for table_ref in &from.tables {
            if Self::table_ref_has_parameters(table_ref) {
                return true;
            }
        }
        false
    }

    /// Check if a TableRef contains parameters
    fn table_ref_has_parameters(table_ref: &crate::parser::ast::TableRef) -> bool {
        match table_ref {
            crate::parser::ast::TableRef::Table { .. } => false,
            crate::parser::ast::TableRef::Subquery { query, .. } => {
                Self::select_has_parameters(query)
            }
            crate::parser::ast::TableRef::Join {
                left,
                right,
                constraint,
                ..
            } => {
                Self::table_ref_has_parameters(left)
                    || Self::table_ref_has_parameters(right)
                    || constraint
                        .as_ref()
                        .map(|c| Self::join_constraint_has_parameters(c))
                        .unwrap_or(false)
            }
            crate::parser::ast::TableRef::TableFunction { args, .. } => {
                args.iter().any(|e| Self::expr_has_parameters(e))
            }
            crate::parser::ast::TableRef::Parens(inner) => Self::table_ref_has_parameters(inner),
        }
    }

    /// Check if a JoinConstraint contains parameters
    fn join_constraint_has_parameters(constraint: &crate::parser::ast::JoinConstraint) -> bool {
        match constraint {
            crate::parser::ast::JoinConstraint::On(expr) => Self::expr_has_parameters(expr),
            crate::parser::ast::JoinConstraint::Using(_) => false,
        }
    }

    /// Check if an expression contains any parameters
    fn expr_has_parameters(expr: &Expr) -> bool {
        match expr {
            Expr::Variable(_) => true,
            Expr::Literal(_) | Expr::Column(_) => false,
            Expr::Binary { left, right, .. } => {
                Self::expr_has_parameters(left) || Self::expr_has_parameters(right)
            }
            Expr::Unary { expr, .. } => Self::expr_has_parameters(expr),
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::expr_has_parameters(expr)
                    || Self::expr_has_parameters(low)
                    || Self::expr_has_parameters(high)
            }
            Expr::In { expr, list, .. } => {
                Self::expr_has_parameters(expr) || Self::in_list_has_parameters(list)
            }
            Expr::Like {
                expr,
                pattern,
                escape,
                ..
            } => {
                Self::expr_has_parameters(expr)
                    || Self::expr_has_parameters(pattern)
                    || escape
                        .as_ref()
                        .map(|e| Self::expr_has_parameters(e))
                        .unwrap_or(false)
            }
            Expr::IsNull { expr, .. } => Self::expr_has_parameters(expr),
            Expr::IsDistinct { left, right, .. } => {
                Self::expr_has_parameters(left) || Self::expr_has_parameters(right)
            }
            Expr::Function(func) => Self::function_call_has_parameters(func),
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                operand
                    .as_ref()
                    .map(|e| Self::expr_has_parameters(e))
                    .unwrap_or(false)
                    || when_clauses.iter().any(|wc| {
                        Self::expr_has_parameters(&wc.when) || Self::expr_has_parameters(&wc.then)
                    })
                    || else_clause
                        .as_ref()
                        .map(|e| Self::expr_has_parameters(e))
                        .unwrap_or(false)
            }
            Expr::Cast { expr, .. } => Self::expr_has_parameters(expr),
            Expr::Collate { expr, .. } => Self::expr_has_parameters(expr),
            Expr::Subquery(q) => Self::select_has_parameters(q),
            Expr::Exists { subquery, .. } => Self::select_has_parameters(subquery),
            Expr::Parens(e) => Self::expr_has_parameters(e),
            Expr::Vector(exprs) => exprs.iter().any(|e| Self::expr_has_parameters(e)),
            Expr::Raise { .. } => false,
        }
    }

    /// Check if an InList contains parameters
    fn in_list_has_parameters(list: &crate::parser::ast::InList) -> bool {
        match list {
            crate::parser::ast::InList::Values(exprs) => {
                exprs.iter().any(|e| Self::expr_has_parameters(e))
            }
            crate::parser::ast::InList::Subquery(q) => Self::select_has_parameters(q),
            crate::parser::ast::InList::Table(_) => false,
        }
    }

    /// Check if a FunctionCall contains parameters
    fn function_call_has_parameters(func: &crate::parser::ast::FunctionCall) -> bool {
        // Check arguments
        match &func.args {
            crate::parser::ast::FunctionArgs::Star => {}
            crate::parser::ast::FunctionArgs::Exprs(exprs) => {
                for expr in exprs {
                    if Self::expr_has_parameters(expr) {
                        return true;
                    }
                }
            }
        }

        // Check filter
        if let Some(ref filter) = func.filter {
            if Self::expr_has_parameters(filter) {
                return true;
            }
        }

        // Check over clause
        if let Some(ref over) = func.over {
            if Self::over_has_parameters(over) {
                return true;
            }
        }

        false
    }

    /// Check if an Over clause contains parameters
    fn over_has_parameters(over: &crate::parser::ast::Over) -> bool {
        match over {
            crate::parser::ast::Over::Window(_) => false, // Named window, check defined elsewhere
            crate::parser::ast::Over::Spec(spec) => Self::window_spec_has_parameters(spec),
        }
    }

    /// Check if a WindowSpec contains parameters
    fn window_spec_has_parameters(spec: &crate::parser::ast::WindowSpec) -> bool {
        // Check partition_by
        if let Some(ref partition_by) = spec.partition_by {
            for expr in partition_by {
                if Self::expr_has_parameters(expr) {
                    return true;
                }
            }
        }

        // Check order_by
        if let Some(ref order_by) = spec.order_by {
            for ord in order_by {
                if Self::expr_has_parameters(&ord.expr) {
                    return true;
                }
            }
        }

        false
    }

    /// Check if a SELECT references tables in databases other than main/temp
    /// Returns the database name if found, None otherwise
    fn select_references_other_database(select: &SelectStmt) -> Option<String> {
        Self::select_body_references_other_database(&select.body)
    }

    fn select_body_references_other_database(
        body: &crate::parser::ast::SelectBody,
    ) -> Option<String> {
        match body {
            crate::parser::ast::SelectBody::Select(core) => {
                Self::select_core_references_other_database(core)
            }
            crate::parser::ast::SelectBody::Compound { left, right, .. } => {
                Self::select_body_references_other_database(left)
                    .or_else(|| Self::select_body_references_other_database(right))
            }
        }
    }

    fn select_core_references_other_database(
        core: &crate::parser::ast::SelectCore,
    ) -> Option<String> {
        if let Some(ref from) = core.from {
            if let Some(db) = Self::from_references_other_database(from) {
                return Some(db);
            }
        }
        None
    }

    fn from_references_other_database(from: &crate::parser::ast::FromClause) -> Option<String> {
        for table_ref in &from.tables {
            if let Some(db) = Self::table_ref_references_other_database(table_ref) {
                return Some(db);
            }
        }
        None
    }

    fn table_ref_references_other_database(
        table_ref: &crate::parser::ast::TableRef,
    ) -> Option<String> {
        match table_ref {
            crate::parser::ast::TableRef::Table { name, .. } => {
                if let Some(ref schema) = name.schema {
                    let schema_lower = schema.to_lowercase();
                    // main and temp are allowed
                    if schema_lower != "main" && schema_lower != "temp" {
                        return Some(schema.clone());
                    }
                }
                None
            }
            crate::parser::ast::TableRef::Subquery { query, .. } => {
                Self::select_references_other_database(query)
            }
            crate::parser::ast::TableRef::Join {
                left,
                right,
                constraint,
                ..
            } => Self::table_ref_references_other_database(left)
                .or_else(|| Self::table_ref_references_other_database(right))
                .or_else(|| {
                    // Also check subqueries in ON clause
                    if let Some(crate::parser::ast::JoinConstraint::On(expr)) = constraint {
                        Self::expr_references_other_database(expr)
                    } else {
                        None
                    }
                }),
            crate::parser::ast::TableRef::TableFunction { .. } => None,
            crate::parser::ast::TableRef::Parens(inner) => {
                Self::table_ref_references_other_database(inner)
            }
        }
    }

    fn expr_references_other_database(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Subquery(q) => Self::select_references_other_database(q),
            Expr::Exists { subquery, .. } => Self::select_references_other_database(subquery),
            Expr::In { list, .. } => {
                if let crate::parser::ast::InList::Subquery(q) = list {
                    Self::select_references_other_database(q)
                } else {
                    None
                }
            }
            // Other expressions don't contain table references directly
            _ => None,
        }
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
                    BinaryOp::JsonExtract => "->",
                    BinaryOp::JsonExtractText => "->>",
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
            Expr::Raise { action, message } => {
                let action_str = match action {
                    RaiseAction::Ignore => "IGNORE",
                    RaiseAction::Rollback => "ROLLBACK",
                    RaiseAction::Abort => "ABORT",
                    RaiseAction::Fail => "FAIL",
                };
                if let Some(msg) = message {
                    match msg {
                        crate::parser::ast::RaiseMessage::Literal(s) => {
                            format!("RAISE({}, '{}')", action_str, s.replace('\'', "''"))
                        }
                        crate::parser::ast::RaiseMessage::Expr(e) => {
                            format!("RAISE({}, {})", action_str, self.expr_to_sql(e))
                        }
                    }
                } else {
                    format!("RAISE({})", action_str)
                }
            }
            Expr::Subquery(select) => {
                // Scalar subquery - wrap SELECT in parentheses
                format!("({})", self.select_to_sql(select))
            }
            Expr::Exists { subquery, negated } => {
                let prefix = if *negated { "NOT EXISTS" } else { "EXISTS" };
                format!("{} ({})", prefix, self.select_to_sql(subquery))
            }
            Expr::In {
                expr,
                list,
                negated,
            } => {
                let neg = if *negated { " NOT" } else { "" };
                let list_str = match list {
                    InList::Values(vals) => vals
                        .iter()
                        .map(|v| self.expr_to_sql(v))
                        .collect::<Vec<_>>()
                        .join(", "),
                    InList::Subquery(sel) => self.select_to_sql(sel),
                    InList::Table(name) => name.to_string(),
                };
                format!("({}{}IN ({}))", self.expr_to_sql(expr), neg, list_str)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let neg = if *negated { " NOT" } else { "" };
                format!(
                    "({}{}BETWEEN {} AND {})",
                    self.expr_to_sql(expr),
                    neg,
                    self.expr_to_sql(low),
                    self.expr_to_sql(high)
                )
            }
            Expr::IsNull { expr, negated } => {
                if *negated {
                    format!("({} IS NOT NULL)", self.expr_to_sql(expr))
                } else {
                    format!("({} IS NULL)", self.expr_to_sql(expr))
                }
            }
            Expr::IsDistinct {
                left,
                right,
                negated,
            } => {
                if *negated {
                    format!(
                        "({} IS NOT DISTINCT FROM {})",
                        self.expr_to_sql(left),
                        self.expr_to_sql(right)
                    )
                } else {
                    format!(
                        "({} IS DISTINCT FROM {})",
                        self.expr_to_sql(left),
                        self.expr_to_sql(right)
                    )
                }
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                let mut sql = String::from("CASE");
                if let Some(op) = operand {
                    sql.push(' ');
                    sql.push_str(&self.expr_to_sql(op));
                }
                for when in when_clauses {
                    sql.push_str(" WHEN ");
                    sql.push_str(&self.expr_to_sql(&when.when));
                    sql.push_str(" THEN ");
                    sql.push_str(&self.expr_to_sql(&when.then));
                }
                if let Some(else_expr) = else_clause {
                    sql.push_str(" ELSE ");
                    sql.push_str(&self.expr_to_sql(else_expr));
                }
                sql.push_str(" END");
                sql
            }
            Expr::Cast { expr, type_name } => {
                format!("CAST({} AS {})", self.expr_to_sql(expr), type_name.name)
            }
            Expr::Collate { expr, collation } => {
                format!("{} COLLATE {}", self.expr_to_sql(expr), collation)
            }
            Expr::Vector(exprs) => {
                let inner: Vec<String> = exprs.iter().map(|e| self.expr_to_sql(e)).collect();
                format!("({})", inner.join(", "))
            }
            _ => "?".to_string(), // Fallback for any remaining expressions
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

    /// Convert SELECT to SQL including WITH, ORDER BY and LIMIT
    fn select_to_sql(&self, select: &SelectStmt) -> String {
        use crate::parser::ast::{NullsOrder, SortOrder};
        let mut sql = String::new();

        // Add WITH clause if present
        if let Some(ref with) = select.with {
            sql.push_str(&self.with_clause_to_sql(with));
            sql.push(' ');
        }

        sql.push_str(&self.select_body_to_sql(&select.body));

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

    /// Convert WITH clause to SQL
    fn with_clause_to_sql(&self, with: &crate::parser::ast::WithClause) -> String {
        let mut sql = String::from("WITH ");
        if with.recursive {
            sql.push_str("RECURSIVE ");
        }
        let ctes: Vec<String> = with
            .ctes
            .iter()
            .map(|cte| {
                let mut s = cte.name.clone();
                if let Some(ref cols) = cte.columns {
                    s.push('(');
                    s.push_str(&cols.join(", "));
                    s.push(')');
                }
                s.push_str(" AS (");
                s.push_str(&self.select_to_sql(&cte.query));
                s.push(')');
                s
            })
            .collect();
        sql.push_str(&ctes.join(", "));
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
        let db_idx = self.resolve_db_idx(&drop.name, false)?;

        // Build qualified name for error messages (include schema if specified)
        let display_name = if let Some(ref schema) = drop.name.schema {
            format!("{}.{}", schema, name)
        } else {
            name.clone()
        };

        // Check for reserved names (sqlite_master, etc.) - cannot be dropped
        if name_lower.starts_with("sqlite_") {
            return Err(crate::error::Error::with_message(
                crate::error::ErrorCode::Error,
                format!("{} {} may not be dropped", kind, name),
            ));
        }

        // Check if the object exists in schema based on kind
        let check_exists = |schema: &crate::schema::Schema| -> bool {
            match kind {
                "table" => schema.tables.contains_key(&name_lower),
                "index" => schema.indexes.contains_key(&name_lower),
                "view" => schema.views.contains_key(&name_lower),
                "trigger" => schema.triggers.contains_key(&name_lower),
                _ => true, // Unknown kind - let it through
            }
        };

        // Check the appropriate schema based on database qualifier
        // Returns (exists, actual_db_idx) - actual_db_idx is where the object was found
        let (exists, actual_db_idx) = if db_idx == 1 {
            // temp database specified - only check temp schema
            let found = self.temp_schema.map(|s| check_exists(s)).unwrap_or(false);
            (found, 1)
        } else {
            // main or no qualifier - check main schema first, then temp
            // SQLite resolves unqualified names by checking temp first, then main
            let in_temp = self.temp_schema.map(|s| check_exists(s)).unwrap_or(false);
            let in_main = self.schema.map(|s| check_exists(s)).unwrap_or(false);
            if in_temp {
                (true, 1) // Found in temp - use temp db_idx
            } else if in_main {
                (true, 0) // Found in main - use main db_idx
            } else {
                (false, db_idx) // Not found - use original db_idx for error message
            }
        };

        if !exists {
            if !drop.if_exists {
                // Check if the name exists as a different type and give helpful error
                let check_wrong_type = |schema: &crate::schema::Schema| -> Option<&'static str> {
                    match kind {
                        "table" => {
                            if schema.views.contains_key(&name_lower) {
                                Some("view")
                            } else {
                                None
                            }
                        }
                        "view" => {
                            if schema.tables.contains_key(&name_lower) {
                                Some("table")
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                };

                // Check both schemas for wrong type
                let wrong_type = self
                    .temp_schema
                    .and_then(check_wrong_type)
                    .or_else(|| self.schema.and_then(check_wrong_type));

                if let Some(actual_type) = wrong_type {
                    return Err(crate::error::Error::with_message(
                        crate::error::ErrorCode::Error,
                        format!(
                            "use DROP {} to delete {} {}",
                            actual_type.to_uppercase(),
                            actual_type,
                            display_name
                        ),
                    ));
                }

                return Err(crate::error::Error::with_message(
                    crate::error::ErrorCode::Error,
                    format!("no such {}: {}", kind, display_name),
                ));
            }
            // IF EXISTS specified and object doesn't exist - return no-op
            let mut ops = Vec::new();
            ops.push(Self::make_op(Opcode::Init, 0, 1, 0, P4::Unused));
            ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
            return Ok(ops);
        }

        // Generate bytecode to drop the object
        // P1 = db_idx (0=main, 1=temp)
        // Control flow: Init -> main code -> Goto -> Halt
        let mut ops = Vec::new();
        ops.push(Self::make_op(Opcode::Init, 0, 2, 0, P4::Unused)); // Jump to main code at 2
        ops.push(Self::make_op(Opcode::Halt, 0, 0, 0, P4::Unused)); // Error halt at 1

        // Main code starts at index 2
        // For non-temp objects, we need to delete from sqlite_master
        // Temp objects are only in memory and don't persist to sqlite_master
        if actual_db_idx != 1 {
            // Delete from sqlite_master
            let cursor_id = 0;
            self.append_sqlite_master_open(&mut ops, cursor_id, actual_db_idx);
            self.append_sqlite_master_delete(&mut ops, cursor_id, name, kind);

            // When dropping a table, also delete associated triggers and indexes
            if kind == "table" {
                self.append_sqlite_master_delete_by_tbl_name(&mut ops, cursor_id, name);
            }

            self.append_sqlite_master_close(&mut ops, cursor_id);
        }

        // Use appropriate Drop opcode based on type (SQLite: OP_DropTable/Index/Trigger)
        let drop_opcode = match kind {
            "table" | "view" => Opcode::DropTable, // SQLite uses DropTable for views too
            "index" => Opcode::DropIndex,
            "trigger" => Opcode::DropTrigger,
            _ => Opcode::DropTable,
        };
        ops.push(Self::make_op(
            drop_opcode,
            actual_db_idx,
            0,
            0,
            P4::Text(name.clone()),
        ));

        // End with Goto back to Halt
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
                let mut collations = Vec::new();
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
                        // Get collation from index column, or fall back to table column collation
                        // If the index has an explicit collation (non-empty), use it
                        // Otherwise, inherit from the table column's collation
                        let collation = if !col.collation.is_empty() {
                            col.collation.to_uppercase()
                        } else if let Some(table_col) = table.columns.get(idx as usize) {
                            if !table_col.collation.is_empty() {
                                table_col.collation.to_uppercase()
                            } else {
                                "BINARY".to_string()
                            }
                        } else {
                            "BINARY".to_string()
                        };
                        collations.push(collation);
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
                    collations,
                    is_primary: index.is_primary_key,
                    is_unique: index.unique,
                    is_covering,
                    stats: index.stats.clone(),
                });
            }

            // Also look at indexes from schema.indexes (separately created indexes)
            let mut added_indexes: std::collections::HashSet<String> =
                indexes.iter().map(|i| i.name.to_lowercase()).collect();

            for (_name, index) in schema.indexes.iter() {
                // Only consider indexes for this table
                if !index.table.eq_ignore_ascii_case(&table_info.name) {
                    continue;
                }
                // Skip if already added from table.indexes
                if added_indexes.contains(&index.name.to_lowercase()) {
                    continue;
                }
                // Apply index filter if specified
                if let Some(IndexedBy::NotIndexed) = index_filter {
                    break;
                }
                if let Some(IndexedBy::Index(forced)) = index_filter {
                    if !index.name.eq_ignore_ascii_case(forced) {
                        continue;
                    }
                }

                let mut columns = Vec::new();
                let mut collations = Vec::new();
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
                    if let Some(cidx) = col_idx {
                        columns.push(cidx);
                        // Get collation from index column, or fall back to table column collation
                        // If the index has an explicit collation (non-empty), use it
                        // Otherwise, inherit from the table column's collation
                        let collation = if !col.collation.is_empty() {
                            col.collation.to_uppercase()
                        } else if let Some(table_col) = table.columns.get(cidx as usize) {
                            if !table_col.collation.is_empty() {
                                table_col.collation.to_uppercase()
                            } else {
                                "BINARY".to_string()
                            }
                        } else {
                            "BINARY".to_string()
                        };
                        collations.push(collation);
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
                    collations,
                    is_primary: index.is_primary_key,
                    is_unique: index.unique,
                    is_covering,
                    stats: index.stats.clone(),
                });
                added_indexes.insert(index.name.to_lowercase());
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

            // Set column affinities for LIKE optimization check
            // LIKE index optimization is only valid for TEXT columns
            if let Some(schema_table) = schema.table(&table.name) {
                let affinities: Vec<String> = schema_table
                    .columns
                    .iter()
                    .map(|c| format!("{:?}", c.affinity))
                    .collect();
                planner.set_table_column_affinities(idx, affinities);
            }

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

        // Check for ORDER BY index satisfaction
        // If a table has FullScan but there's an ORDER BY that can use an index,
        // update the output to show the index being used for scanning
        let order_by_index = self.detect_order_by_index(select, schema, &table_infos);

        plan.levels
            .iter()
            .map(|level| {
                self.format_plan_detail_with_order(
                    level,
                    &plan.terms,
                    &table_infos,
                    &order_by_index,
                )
            })
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
            WherePlan::MultiIndexOr { branches, .. } => {
                let names: Vec<&str> = branches
                    .iter()
                    .filter_map(|b| b.index_name.as_deref())
                    .collect();
                if names.is_empty() {
                    format!("MULTI-INDEX OR on {}", display_name)
                } else {
                    format!("MULTI-INDEX OR ({}) on {}", names.join(", "), display_name)
                }
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

    /// Detect if ORDER BY can be satisfied by an index.
    /// Returns a map from table index to (index_name, is_covering).
    fn detect_order_by_index(
        &self,
        select: &SelectStmt,
        schema: &crate::schema::Schema,
        table_infos: &[ExplainTableInfo],
    ) -> HashMap<usize, (String, bool)> {
        let mut result = HashMap::new();

        // Get ORDER BY clause
        let order_by = match &select.order_by {
            Some(order_by) if !order_by.is_empty() => order_by,
            _ => return result,
        };

        // Only handle simple cases: single ORDER BY column, ASC order
        if order_by.len() != 1 {
            return result;
        }
        let order_term = &order_by[0];
        if order_term.order == SortOrder::Desc {
            return result;
        }

        // Get the ORDER BY column
        let core = match &select.body {
            SelectBody::Select(core) => core,
            _ => return result,
        };

        let order_col = match &order_term.expr {
            Expr::Column(col_ref) => col_ref.column.to_lowercase(),
            Expr::Literal(Literal::Integer(n)) => {
                // ORDER BY 1 means first column in SELECT list
                let idx = (*n as usize).saturating_sub(1);
                if idx < core.columns.len() {
                    match &core.columns[idx] {
                        ResultColumn::Expr { expr, .. } => match expr {
                            Expr::Column(col_ref) => col_ref.column.to_lowercase(),
                            _ => return result,
                        },
                        _ => return result,
                    }
                } else {
                    return result;
                }
            }
            _ => return result,
        };

        // For each table, check if there's an index that can satisfy ORDER BY
        for (table_idx, table_info) in table_infos.iter().enumerate() {
            // Check if table has the ORDER BY column
            let has_col = table_info
                .columns
                .iter()
                .any(|c| c.to_lowercase() == order_col);
            if !has_col {
                continue;
            }

            // Check each index to see if its first column matches ORDER BY
            for index in &table_info.indexes {
                if let Some(&first_col_idx) = index.columns.first() {
                    if first_col_idx >= 0 && (first_col_idx as usize) < table_info.columns.len() {
                        let idx_col_name = &table_info.columns[first_col_idx as usize];
                        if idx_col_name.to_lowercase() == order_col {
                            // Found matching index
                            result.insert(table_idx, (index.name.clone(), index.is_covering));
                            break;
                        }
                    }
                }
            }

            // Also check schema's global indexes if not found in table_info.indexes
            if !result.contains_key(&table_idx) {
                if let Some(schema_table) = schema.table(&table_info.name) {
                    for index in &schema_table.indexes {
                        if let Some(first_col) = index.columns.first() {
                            let col_idx = first_col.column_idx;
                            if col_idx >= 0 && (col_idx as usize) < schema_table.columns.len() {
                                let idx_col_name = &schema_table.columns[col_idx as usize].name;
                                if idx_col_name.to_lowercase() == order_col {
                                    // Check if covering
                                    let is_covering = table_info.indexes.iter().any(|i| {
                                        i.name.eq_ignore_ascii_case(&index.name) && i.is_covering
                                    });
                                    result.insert(table_idx, (index.name.clone(), is_covering));
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        result
    }

    /// Format plan detail with ORDER BY index consideration.
    fn format_plan_detail_with_order(
        &self,
        level: &super::where_clause::WhereLevel,
        terms: &[WhereTerm],
        table_infos: &[ExplainTableInfo],
        order_by_index: &HashMap<usize, (String, bool)>,
    ) -> String {
        let table_idx = level.from_idx as usize;
        let table_info = table_infos.get(table_idx);
        let display_name = table_info
            .map(|t| t.display_name.as_str())
            .unwrap_or("table");

        match &level.plan {
            WherePlan::FullScan => {
                // Check if ORDER BY uses an index for this table
                if let Some((index_name, is_covering)) = order_by_index.get(&table_idx) {
                    if *is_covering {
                        format!("SCAN {} USING COVERING INDEX {}", display_name, index_name)
                    } else {
                        format!("SCAN {} USING INDEX {}", display_name, index_name)
                    }
                } else {
                    format!("SCAN {}", display_name)
                }
            }
            // For other plans, delegate to the original format_plan_detail
            _ => self.format_plan_detail(level, terms, table_infos),
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
            Expr::Vector(exprs) => {
                for e in exprs {
                    self.collect_expr_columns(e, refs);
                }
            }
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
///
/// This function handles:
/// - String literals (single and double quoted)
/// - BEGIN...END blocks (for triggers and compound statements)
/// - Nested BEGIN...END blocks
fn find_statement_tail(sql: &str) -> &str {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut in_string = false;
    let mut string_char = b'\0';
    let mut begin_depth = 0;

    while i < len {
        let c = bytes[i];

        if in_string {
            if c == string_char {
                // Check for escaped quote (doubled)
                if i + 1 < len && bytes[i + 1] == string_char {
                    i += 2;
                    continue;
                }
                in_string = false;
            }
            i += 1;
            continue;
        }

        match c {
            b'\'' | b'"' => {
                in_string = true;
                string_char = c;
                i += 1;
            }
            b'B' | b'b' => {
                // Check for BEGIN keyword
                if i + 4 < len {
                    let word = &sql[i..i + 5];
                    if word.eq_ignore_ascii_case("BEGIN") {
                        // Make sure it's a word boundary (not part of larger identifier)
                        let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
                        let after_ok = i + 5 >= len
                            || !bytes[i + 5].is_ascii_alphanumeric() && bytes[i + 5] != b'_';
                        if before_ok && after_ok {
                            begin_depth += 1;
                            i += 5;
                            continue;
                        }
                    }
                }
                i += 1;
            }
            b'E' | b'e' => {
                // Check for END keyword
                if begin_depth > 0 && i + 2 < len {
                    let word = &sql[i..i + 3];
                    if word.eq_ignore_ascii_case("END") {
                        // Make sure it's a word boundary
                        let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
                        let after_ok = i + 3 >= len
                            || !bytes[i + 3].is_ascii_alphanumeric() && bytes[i + 3] != b'_';
                        if before_ok && after_ok {
                            begin_depth -= 1;
                            i += 3;
                            continue;
                        }
                    }
                }
                i += 1;
            }
            b';' => {
                if begin_depth == 0 {
                    // Found statement end - return everything after
                    return &sql[i + 1..];
                }
                // Inside BEGIN...END block, semicolon is not statement separator
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    // No statement-ending semicolon found
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

/// Compile SQL to VDBE bytecode with main and temp schema access
///
/// Returns the compiled statement and any remaining SQL (tail).
/// Both main and temp schemas are used for name resolution.
pub fn compile_sql_with_full_config<'a>(
    sql: &'a str,
    schema: &crate::schema::Schema,
    temp_schema: Option<&crate::schema::Schema>,
    attached_schemas: Vec<(String, &crate::schema::Schema)>,
    short_column_names: bool,
    full_column_names: bool,
    case_sensitive_like: bool,
    enable_view: bool,
    vtab_registry: Option<std::sync::Arc<crate::vtab::VtabRegistry>>,
    dqs_dml: bool,
) -> Result<(CompiledStmt, &'a str)> {
    let mut compiler = StatementCompiler::with_schema(schema);
    if let Some(ts) = temp_schema {
        compiler.set_temp_schema(ts);
    }
    compiler.set_attached_schemas(attached_schemas);
    compiler.set_column_name_flags(short_column_names, full_column_names);
    compiler.set_case_sensitive_like(case_sensitive_like);
    compiler.set_enable_view(enable_view);
    compiler.set_dqs_dml(dqs_dml);
    if let Some(registry) = vtab_registry {
        compiler.set_vtab_registry(registry);
    }
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
