//! INSERT statement compilation
//!
//! This module compiles INSERT statements to VDBE bytecode.
//! Corresponds to insert.c in SQLite.

use std::collections::HashMap;

use crate::error::Result;
use crate::parser::ast::{
    ConflictAction, Expr, InsertSource, InsertStmt, ResultColumn, SelectBody, SelectStmt, TableRef,
};
use crate::schema::Schema;
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

use super::column_mapping::{ColumnMapper, ColumnSource, RowidMapping};
use super::select::{SelectCompiler, SelectDest};

fn is_rowid_alias(name: &str) -> bool {
    name.eq_ignore_ascii_case("rowid")
        || name.eq_ignore_ascii_case("_rowid_")
        || name.eq_ignore_ascii_case("oid")
}

/// Flag to indicate that this operation should update the change counter
const OPFLAG_NCHANGE: u16 = 0x01;

#[derive(Debug, Clone, Copy)]
enum InsertColumnTarget {
    Rowid,
    Column(usize),
}

// ============================================================================
// InsertCompiler
// ============================================================================

/// Index cursor info for index maintenance
struct IndexCursor {
    /// Cursor number
    cursor: i32,
    /// Column indices in the index (in order)
    columns: Vec<i32>,
    /// Index name
    name: String,
}

/// Compiles INSERT statements to VDBE opcodes
pub struct InsertCompiler<'a> {
    /// Generated VDBE operations
    ops: Vec<VdbeOp>,

    /// Next register to allocate
    next_reg: i32,

    /// Next cursor to allocate
    next_cursor: i32,

    /// Next label
    next_label: i32,

    /// Labels pending resolution
    labels: HashMap<i32, Option<i32>>,

    /// Table cursor
    table_cursor: i32,

    /// Number of columns in target table
    num_columns: usize,

    /// Column name to index mapping
    column_map: HashMap<String, usize>,

    /// Optional schema for validation
    schema: Option<&'a Schema>,

    /// Index cursors for maintenance
    index_cursors: Vec<IndexCursor>,

    /// Parameter names for bound parameter lookup
    param_names: Vec<Option<String>>,

    /// Next unnamed parameter index (1-based)
    next_unnamed_param: i32,
}

impl<'a> InsertCompiler<'a> {
    /// Create a new INSERT compiler
    pub fn new() -> Self {
        InsertCompiler {
            ops: Vec::new(),
            next_reg: 1,
            next_cursor: 0,
            next_label: -1,
            labels: HashMap::new(),
            table_cursor: 0,
            num_columns: 0,
            column_map: HashMap::new(),
            schema: None,
            index_cursors: Vec::new(),
            param_names: Vec::new(),
            next_unnamed_param: 1,
        }
    }

    /// Create a new INSERT compiler with schema
    pub fn with_schema(schema: &'a Schema) -> Self {
        InsertCompiler {
            ops: Vec::new(),
            next_reg: 1,
            next_cursor: 0,
            next_label: -1,
            labels: HashMap::new(),
            table_cursor: 0,
            num_columns: 0,
            column_map: HashMap::new(),
            schema: Some(schema),
            index_cursors: Vec::new(),
            param_names: Vec::new(),
            next_unnamed_param: 1,
        }
    }

    /// Set parameter names for Variable compilation
    pub fn set_param_names(&mut self, param_names: Vec<Option<String>>) {
        self.param_names = param_names;
    }

    /// Compile an INSERT statement
    pub fn compile(&mut self, insert: &InsertStmt) -> Result<Vec<VdbeOp>> {
        // Check for system tables that cannot be modified
        let table_name_lower = insert.table.name.to_lowercase();
        if table_name_lower == "sqlite_master"
            || table_name_lower == "sqlite_schema"
            || table_name_lower == "sqlite_temp_master"
            || table_name_lower == "sqlite_temp_schema"
        {
            return Err(crate::error::Error::with_message(
                crate::error::ErrorCode::Error,
                format!("table {} may not be modified", insert.table.name),
            ));
        }

        // Initialize
        self.emit(Opcode::Init, 0, 0, 0, P4::Unused);

        // Open table for writing
        self.table_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenWrite,
            self.table_cursor,
            0, // Root page (would come from schema)
            0,
            P4::Text(insert.table.name.clone()),
        );

        self.num_columns = self.infer_num_columns(insert);

        // Open indexes for writing
        self.open_indexes_for_write(&insert.table.name)?;

        // Handle conflict action
        let conflict_action = insert.or_action.unwrap_or(ConflictAction::Abort);

        // Compile based on source type
        match &insert.source {
            InsertSource::Values(rows) => {
                self.compile_values(insert, rows, conflict_action)?;
            }
            InsertSource::Select(select) => {
                // Validate ORDER BY doesn't contain aggregates without GROUP BY
                self.validate_select_order_by(select)?;
                self.compile_select(insert, select, conflict_action)?;
            }
            InsertSource::DefaultValues => {
                self.compile_default_values(insert, conflict_action)?;
            }
        }

        // Handle RETURNING clause
        if let Some(returning) = &insert.returning {
            self.compile_returning(returning)?;
        }

        // Close index cursors
        let index_cursor_ids: Vec<i32> = self.index_cursors.iter().map(|ic| ic.cursor).collect();
        for cursor in index_cursor_ids {
            self.emit(Opcode::Close, cursor, 0, 0, P4::Unused);
        }

        // Close table cursor
        self.emit(Opcode::Close, self.table_cursor, 0, 0, P4::Unused);

        // Halt
        self.emit(Opcode::Halt, 0, 0, 0, P4::Unused);

        // Resolve labels
        self.resolve_labels()?;

        Ok(std::mem::take(&mut self.ops))
    }

    /// Compile INSERT...VALUES
    fn compile_values(
        &mut self,
        insert: &InsertStmt,
        rows: &[Vec<Expr>],
        conflict_action: ConflictAction,
    ) -> Result<()> {
        // Build column index map if columns specified
        let col_targets = self.build_column_map(&insert.table.name, &insert.columns)?;

        // Validate column count for each row
        let expected_cols = col_targets.len();
        for row in rows {
            if row.len() != expected_cols {
                if insert.columns.is_some() {
                    // Column list specified: "N values for M columns"
                    return Err(crate::error::Error::with_message(
                        crate::error::ErrorCode::Error,
                        format!("{} values for {} columns", row.len(), expected_cols),
                    ));
                } else {
                    // No column list: "table X has N columns but M values were supplied"
                    return Err(crate::error::Error::with_message(
                        crate::error::ErrorCode::Error,
                        format!(
                            "table {} has {} columns but {} values were supplied",
                            insert.table.name,
                            self.num_columns,
                            row.len()
                        ),
                    ));
                }
            }
        }

        for row in rows {
            // Allocate rowid register
            let rowid_reg = self.alloc_reg();

            // Generate new rowid (autoincrement)
            self.emit(
                Opcode::NewRowid,
                self.table_cursor,
                rowid_reg,
                0,
                P4::Unused,
            );

            // Allocate registers for column values
            let data_base = self.next_reg;
            let _data_regs = self.alloc_regs(self.num_columns);

            // Evaluate each value and store in appropriate register
            let mut present = vec![false; self.num_columns];
            for (i, target) in col_targets.iter().enumerate() {
                if i < row.len() {
                    match *target {
                        InsertColumnTarget::Rowid => {
                            self.compile_expr(&row[i], rowid_reg)?;
                        }
                        InsertColumnTarget::Column(col_idx) => {
                            let dest_reg = data_base + col_idx as i32;
                            self.compile_expr(&row[i], dest_reg)?;
                            if col_idx < present.len() {
                                present[col_idx] = true;
                            }
                        }
                    }
                }
            }

            // Fill in defaults or NULL for unspecified columns
            let table_lower = insert.table.name.to_lowercase();
            let table_opt = self.schema.and_then(|s| s.tables.get(&table_lower));

            for (col_idx, seen) in present.iter().enumerate() {
                if !*seen {
                    let reg = data_base + col_idx as i32;

                    // Try to get default value from schema
                    let has_default = if let Some(table) = table_opt {
                        if col_idx < table.columns.len() {
                            let column = &table.columns[col_idx];
                            if let Some(default) = &column.default_value {
                                // Emit code to apply the default value
                                self.emit_default_value(default, reg)?;
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    // If no default found, use NULL
                    if !has_default {
                        self.emit(Opcode::Null, 0, reg, 0, P4::Unused);
                    }
                }
            }

            // Handle conflict action
            self.emit_conflict_check(conflict_action)?;

            // Make record
            let record_reg = self.alloc_reg();
            self.emit(
                Opcode::MakeRecord,
                data_base,
                self.num_columns as i32,
                record_reg,
                P4::Unused,
            );

            // Insert the record
            let flags = self.conflict_flags(conflict_action);
            self.emit_with_p5(
                Opcode::Insert,
                self.table_cursor,
                record_reg,
                rowid_reg,
                P4::Int64(flags),
                OPFLAG_NCHANGE,
            );

            // Insert into indexes
            self.emit_index_inserts(data_base, rowid_reg);
        }

        Ok(())
    }

    fn infer_num_columns(&self, insert: &InsertStmt) -> usize {
        // Always try to get actual table column count from schema
        if let Some(schema) = self.schema {
            let table_name_lower = insert.table.name.to_lowercase();
            if let Some(table) = schema.tables.get(&table_name_lower) {
                return table.columns.len();
            }
        }

        // Fallback: if column list is specified, use that count
        if let Some(cols) = &insert.columns {
            if !cols.is_empty() {
                return cols.iter().filter(|col| !is_rowid_alias(col)).count();
            }
        }

        // Final fallback: infer from source (less accurate)
        match &insert.source {
            InsertSource::Values(rows) => rows.first().map(|row| row.len()).unwrap_or(0),
            InsertSource::Select(select) => self.count_select_columns(select),
            InsertSource::DefaultValues => 1,
        }
    }

    /// Count columns in SELECT result
    fn count_select_columns(&self, select: &SelectStmt) -> usize {
        if let SelectBody::Select(core) = &select.body {
            let mut count = 0;
            for col in &core.columns {
                match col {
                    ResultColumn::Star => {
                        // For *, we don't know the count without schema
                        // Use a reasonable default
                        return 10;
                    }
                    ResultColumn::TableStar(_) => return 10,
                    ResultColumn::Expr { .. } => count += 1,
                }
            }
            return count.max(1);
        }
        10 // Default fallback
    }

    /// Compile INSERT...SELECT
    ///
    /// To handle self-referential queries (INSERT INTO t SELECT ... FROM t),
    /// we materialize the SELECT results into an ephemeral table first, then
    /// insert from the ephemeral table. This prevents infinite loops where
    /// newly inserted rows would be visible to the SELECT cursor.
    ///
    /// Steps:
    /// 1. Open source table for reading
    /// 2. Open ephemeral table to buffer rows
    /// 3. First loop: Read from source, evaluate expressions, insert into ephemeral table
    /// 4. Close source cursor
    /// 5. Second loop: Read from ephemeral table, insert into target
    /// 6. Close ephemeral cursor
    fn compile_select(
        &mut self,
        insert: &InsertStmt,
        select: &SelectStmt,
        conflict_action: ConflictAction,
    ) -> Result<()> {
        // Build column index map
        let col_targets = self.build_column_map(&insert.table.name, &insert.columns)?;

        // Extract SELECT expressions to check for complexity
        let select_exprs = self.get_select_expressions(select);
        let select_col_count = select_exprs.len();

        // Check if SELECT is complex and needs the full SelectCompiler
        // Complex cases include: UNION, ORDER BY, GROUP BY, multiple tables, etc.
        if Self::has_subquery(&select_exprs) || self.is_complex_select(select) {
            return self.compile_select_with_subqueries(
                insert,
                select,
                conflict_action,
                &col_targets,
            );
        }

        // Simple path: single table SELECT with no complex clauses

        // Extract source table from SELECT
        // For now, we support simple "SELECT * FROM table" or "SELECT cols FROM table"
        let source_table = self.get_source_table(select)?;

        // Build column name to index map for the source table
        let source_col_map = self.build_source_column_map(&source_table);

        // Open source table for reading
        let source_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenRead,
            source_cursor,
            0, // Root page 0 = look up by name
            self.num_columns as i32,
            P4::Text(source_table.clone()),
        );

        // Open ephemeral table to buffer the SELECT results
        // This is critical for self-referential queries to avoid infinite loops
        let eph_cursor = self.alloc_cursor();
        self.emit(
            Opcode::OpenEphemeral,
            eph_cursor,
            select_col_count as i32,
            0,
            P4::Unused,
        );

        // ========================================================================
        // Phase 1: Read all source rows into ephemeral table
        // ========================================================================
        let read_loop_start = self.alloc_label();
        let read_loop_end = self.alloc_label();

        // Rewind to start of source table
        self.emit(Opcode::Rewind, source_cursor, read_loop_end, 0, P4::Unused);
        self.resolve_label(read_loop_start, self.current_addr() as i32);

        // Compile each SELECT expression and store in registers
        let temp_base = self.next_reg;
        let _temp_regs = self.alloc_regs(select_col_count);

        for (i, expr) in select_exprs.iter().enumerate() {
            let dest_reg = temp_base + i as i32;
            self.compile_select_expr(expr, dest_reg, source_cursor, &source_col_map)?;
        }

        // Allocate a rowid for the ephemeral table
        let eph_rowid_reg = self.alloc_reg();
        self.emit(Opcode::NewRowid, eph_cursor, eph_rowid_reg, 0, P4::Unused);

        // Make record for ephemeral table
        let eph_record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            temp_base,
            select_col_count as i32,
            eph_record_reg,
            P4::Unused,
        );

        // Insert into ephemeral table
        self.emit(
            Opcode::Insert,
            eph_cursor,
            eph_record_reg,
            eph_rowid_reg,
            P4::Int64(0), // No conflict handling for ephemeral
        );

        // Next row in source table
        self.emit(Opcode::Next, source_cursor, read_loop_start, 0, P4::Unused);
        self.resolve_label(read_loop_end, self.current_addr() as i32);

        // Close source cursor - we're done reading
        self.emit(Opcode::Close, source_cursor, 0, 0, P4::Unused);

        // ========================================================================
        // Phase 2: Insert from ephemeral table into target
        // ========================================================================
        let insert_loop_start = self.alloc_label();
        let insert_loop_end = self.alloc_label();

        // Rewind ephemeral table
        self.emit(Opcode::Rewind, eph_cursor, insert_loop_end, 0, P4::Unused);
        self.resolve_label(insert_loop_start, self.current_addr() as i32);

        // Use ColumnMapper to build proper column mapping including DEFAULTs
        let explicit_cols = insert
            .columns
            .as_ref()
            .map(|cols| cols.iter().map(|c| c.clone()).collect::<Vec<_>>());
        let mapper = ColumnMapper::new(
            &insert.table.name,
            explicit_cols.as_deref(),
            select_col_count,
            self.schema,
        )?;

        // Allocate rowid register for target table
        let rowid_reg = self.alloc_reg();

        // Check if rowid comes from explicit column or should be auto-generated
        match mapper.rowid_mapping() {
            RowidMapping::SourceIndex(src_idx) => {
                // Read rowid from SELECT result
                self.emit(
                    Opcode::Column,
                    eph_cursor,
                    src_idx as i32,
                    rowid_reg,
                    P4::Unused,
                );
                // If rowid is NULL, generate a new rowid instead
                let skip_newrowid = self.alloc_label();
                self.emit(Opcode::NotNull, rowid_reg, skip_newrowid, 0, P4::Unused);
                self.emit(
                    Opcode::NewRowid,
                    self.table_cursor,
                    rowid_reg,
                    0,
                    P4::Unused,
                );
                self.resolve_label(skip_newrowid, self.current_addr() as i32);
            }
            RowidMapping::Auto => {
                // Generate new rowid
                self.emit(
                    Opcode::NewRowid,
                    self.table_cursor,
                    rowid_reg,
                    0,
                    P4::Unused,
                );
            }
        }

        // Read columns from ephemeral row and map to target columns
        let data_base = self.next_reg;
        let _data_regs = self.alloc_regs(self.num_columns);

        for (target_idx, column_source) in mapper.mapping().iter().enumerate() {
            let dest_reg = data_base + target_idx as i32;

            match column_source {
                ColumnSource::SourceIndex(src_idx) => {
                    // Read from SELECT result
                    self.emit(
                        Opcode::Column,
                        eph_cursor,
                        *src_idx as i32,
                        dest_reg,
                        P4::Unused,
                    );
                }
                ColumnSource::DefaultValue => {
                    // Use column's DEFAULT value
                    if let Some(col) = mapper.get_column(target_idx) {
                        if let Some(default) = &col.default_value {
                            self.emit_default_value(default, dest_reg)?;
                        } else {
                            // Shouldn't happen (DEFAULT would be mapped by ColumnMapper),
                            // but be safe
                            self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                        }
                    } else {
                        self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                    }
                }
                ColumnSource::Null => {
                    // Use NULL
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                }
            }
        }

        // Handle conflict
        self.emit_conflict_check(conflict_action)?;

        // Make and insert record
        let record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            data_base,
            self.num_columns as i32,
            record_reg,
            P4::Unused,
        );

        let flags = self.conflict_flags(conflict_action);
        self.emit_with_p5(
            Opcode::Insert,
            self.table_cursor,
            record_reg,
            rowid_reg,
            P4::Int64(flags),
            OPFLAG_NCHANGE,
        );

        // Insert into indexes
        self.emit_index_inserts(data_base, rowid_reg);

        // Next row in ephemeral table
        self.emit(Opcode::Next, eph_cursor, insert_loop_start, 0, P4::Unused);
        self.resolve_label(insert_loop_end, self.current_addr() as i32);

        // Close ephemeral cursor
        self.emit(Opcode::Close, eph_cursor, 0, 0, P4::Unused);

        Ok(())
    }

    /// Compile INSERT...SELECT when the SELECT contains subqueries
    /// Uses SelectCompiler for the SELECT portion to handle complex expressions
    fn compile_select_with_subqueries(
        &mut self,
        insert: &InsertStmt,
        select: &SelectStmt,
        conflict_action: ConflictAction,
        col_targets: &[InsertColumnTarget],
    ) -> Result<()> {
        // Get the number of columns in the SELECT result
        let select_col_count = self.get_select_column_count(select);

        // Use SelectCompiler to compile the SELECT first
        // We'll use a high cursor number for the ephemeral table to avoid conflicts
        // with cursors allocated by SelectCompiler for nested subqueries
        let mut sub_compiler = if let Some(schema) = self.schema {
            SelectCompiler::with_schema(schema)
        } else {
            SelectCompiler::new()
        };

        // Use a high cursor number that won't conflict with SelectCompiler's allocations
        // SelectCompiler starts at cursor 0, so we use 100 to be safe
        let eph_cursor = 100;
        let sub_dest = SelectDest::EphemTable { cursor: eph_cursor };
        let sub_ops = sub_compiler.compile(select, &sub_dest)?;

        // Now open the ephemeral table with the high cursor number
        self.emit(
            Opcode::OpenEphemeral,
            eph_cursor,
            select_col_count as i32,
            0,
            P4::Unused,
        );

        // Get the cursor offset for adjusting SelectCompiler's cursor numbers
        let cursor_offset = self.next_cursor;

        // Filter out Init and Halt from the subquery ops and inline them
        // Adjust jump addresses and cursor numbers
        let base_addr = self.ops.len() as i32;
        for mut op in sub_ops {
            if op.opcode == Opcode::Init || op.opcode == Opcode::Halt {
                continue;
            }
            // Adjust jump addresses
            if op.opcode.is_jump() && op.p2 > 0 {
                op.p2 += base_addr;
            }
            // Adjust cursor numbers - p1 is usually the cursor for table operations
            // But we need to be careful: eph_cursor should NOT be adjusted since we
            // already have it at the right value
            if op.opcode == Opcode::OpenRead || op.opcode == Opcode::OpenWrite {
                op.p1 += cursor_offset;
            } else if matches!(
                op.opcode,
                Opcode::Rewind
                    | Opcode::Next
                    | Opcode::Column
                    | Opcode::Close
                    | Opcode::Insert
                    | Opcode::NewRowid
                    | Opcode::SeekGE
                    | Opcode::SeekGT
                    | Opcode::SeekLE
                    | Opcode::SeekLT
                    | Opcode::SeekRowid
                    | Opcode::IdxGE
                    | Opcode::IdxGT
                    | Opcode::IdxLE
                    | Opcode::IdxLT
                    | Opcode::Found
                    | Opcode::NotFound
                    | Opcode::SorterInsert
                    | Opcode::SorterSort
                    | Opcode::SorterNext
                    | Opcode::SorterData
                    | Opcode::OpenEphemeral
                    | Opcode::OpenAutoindex
            ) {
                // Don't adjust if it's our ephemeral cursor
                if op.p1 != eph_cursor {
                    op.p1 += cursor_offset;
                }
            }
            self.ops.push(op);
        }

        // Update cursor count to account for cursors used by SelectCompiler
        self.next_cursor += 10; // Reserve space for SelectCompiler cursors

        // Reserve registers used by SelectCompiler
        self.next_reg += 100;

        // ========================================================================
        // Phase 2: Insert from ephemeral table into target
        // ========================================================================
        let insert_loop_start = self.alloc_label();
        let insert_loop_end = self.alloc_label();

        // Rewind ephemeral table
        self.emit(Opcode::Rewind, eph_cursor, insert_loop_end, 0, P4::Unused);
        self.resolve_label(insert_loop_start, self.current_addr() as i32);

        // Use ColumnMapper to build proper column mapping including DEFAULTs
        let explicit_cols = insert
            .columns
            .as_ref()
            .map(|cols| cols.iter().map(|c| c.clone()).collect::<Vec<_>>());
        let mapper = ColumnMapper::new(
            &insert.table.name,
            explicit_cols.as_deref(),
            select_col_count,
            self.schema,
        )?;

        // Allocate rowid register for target table
        let rowid_reg = self.alloc_reg();

        // Check if rowid comes from explicit column or should be auto-generated
        match mapper.rowid_mapping() {
            RowidMapping::SourceIndex(src_idx) => {
                // Read rowid from SELECT result
                self.emit(
                    Opcode::Column,
                    eph_cursor,
                    src_idx as i32,
                    rowid_reg,
                    P4::Unused,
                );
                // If rowid is NULL, generate a new rowid instead
                let skip_newrowid = self.alloc_label();
                self.emit(Opcode::NotNull, rowid_reg, skip_newrowid, 0, P4::Unused);
                self.emit(
                    Opcode::NewRowid,
                    self.table_cursor,
                    rowid_reg,
                    0,
                    P4::Unused,
                );
                self.resolve_label(skip_newrowid, self.current_addr() as i32);
            }
            RowidMapping::Auto => {
                // Generate new rowid
                self.emit(
                    Opcode::NewRowid,
                    self.table_cursor,
                    rowid_reg,
                    0,
                    P4::Unused,
                );
            }
        }

        // Read columns from ephemeral row and map to target columns
        let data_base = self.next_reg;
        let _data_regs = self.alloc_regs(self.num_columns);

        for (target_idx, column_source) in mapper.mapping().iter().enumerate() {
            let dest_reg = data_base + target_idx as i32;

            match column_source {
                ColumnSource::SourceIndex(src_idx) => {
                    // Read from SELECT result
                    self.emit(
                        Opcode::Column,
                        eph_cursor,
                        *src_idx as i32,
                        dest_reg,
                        P4::Unused,
                    );
                }
                ColumnSource::DefaultValue => {
                    // Use column's DEFAULT value
                    if let Some(col) = mapper.get_column(target_idx) {
                        if let Some(default) = &col.default_value {
                            self.emit_default_value(default, dest_reg)?;
                        } else {
                            // Shouldn't happen (DEFAULT would be mapped by ColumnMapper),
                            // but be safe
                            self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                        }
                    } else {
                        self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                    }
                }
                ColumnSource::Null => {
                    // Use NULL
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                }
            }
        }

        // Handle conflict
        self.emit_conflict_check(conflict_action)?;

        // Make and insert record
        let record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            data_base,
            self.num_columns as i32,
            record_reg,
            P4::Unused,
        );

        let flags = self.conflict_flags(conflict_action);
        self.emit_with_p5(
            Opcode::Insert,
            self.table_cursor,
            record_reg,
            rowid_reg,
            P4::Int64(flags),
            OPFLAG_NCHANGE,
        );

        // Insert into indexes
        self.emit_index_inserts(data_base, rowid_reg);

        // Next row in ephemeral table
        self.emit(Opcode::Next, eph_cursor, insert_loop_start, 0, P4::Unused);
        self.resolve_label(insert_loop_end, self.current_addr() as i32);

        // Close ephemeral cursor
        self.emit(Opcode::Close, eph_cursor, 0, 0, P4::Unused);

        Ok(())
    }

    /// Extract source table name from SELECT for simple cases
    fn get_source_table(&self, select: &SelectStmt) -> Result<String> {
        // Handle SELECT...FROM table
        if let SelectBody::Select(core) = &select.body {
            if let Some(from) = &core.from {
                if let Some(table_ref) = from.tables.first() {
                    if let TableRef::Table { name, .. } = table_ref {
                        return Ok(name.name.clone());
                    }
                }
            }
        }
        Err(crate::error::Error::with_message(
            crate::error::ErrorCode::Error,
            "INSERT...SELECT requires a simple SELECT from a table".to_string(),
        ))
    }

    /// Get number of columns in SELECT result
    fn get_select_column_count(&self, select: &SelectStmt) -> usize {
        if let SelectBody::Select(core) = &select.body {
            // For SELECT *, return all columns from target (num_columns)
            // For explicit columns, count them
            let mut count = 0;
            for col in &core.columns {
                match col {
                    ResultColumn::Star => return self.num_columns,
                    ResultColumn::TableStar(_) => return self.num_columns,
                    ResultColumn::Expr { .. } => count += 1,
                }
            }
            return count.max(1);
        }
        self.num_columns
    }

    /// Extract expressions from SELECT clause
    fn get_select_expressions(&self, select: &SelectStmt) -> Vec<Expr> {
        if let SelectBody::Select(core) = &select.body {
            let mut exprs = Vec::new();
            for col in &core.columns {
                match col {
                    ResultColumn::Star | ResultColumn::TableStar(_) => {
                        // For SELECT *, generate column references for all columns
                        for i in 0..self.num_columns {
                            exprs.push(Expr::Column(crate::parser::ast::ColumnRef {
                                database: None,
                                table: None,
                                column: format!("col{}", i),
                                column_index: Some(i as i32),
                            }));
                        }
                    }
                    ResultColumn::Expr { expr, .. } => {
                        exprs.push(expr.clone());
                    }
                }
            }
            if exprs.is_empty() {
                // Fallback: return a single column reference
                exprs.push(Expr::Column(crate::parser::ast::ColumnRef {
                    database: None,
                    table: None,
                    column: "col0".to_string(),
                    column_index: Some(0),
                }));
            }
            return exprs;
        }
        // Fallback for compound selects
        vec![Expr::Column(crate::parser::ast::ColumnRef {
            database: None,
            table: None,
            column: "col0".to_string(),
            column_index: Some(0),
        })]
    }

    /// Build column name to index map for source table
    fn build_source_column_map(&self, table_name: &str) -> HashMap<String, usize> {
        let mut map = HashMap::new();
        if let Some(schema) = self.schema {
            let table_lower = table_name.to_lowercase();
            if let Some(table) = schema.tables.get(&table_lower) {
                for (i, col) in table.columns.iter().enumerate() {
                    map.insert(col.name.to_lowercase(), i);
                }
            }
        }
        map
    }

    /// Check if any expression contains a subquery
    fn has_subquery(exprs: &[Expr]) -> bool {
        for expr in exprs {
            if Self::expr_has_subquery(expr) {
                return true;
            }
        }
        false
    }

    /// Check if an expression contains a subquery
    fn expr_has_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::Subquery(_) => true,
            Expr::Binary { left, right, .. } => {
                Self::expr_has_subquery(left) || Self::expr_has_subquery(right)
            }
            Expr::Unary { expr, .. } => Self::expr_has_subquery(expr),
            Expr::Function(func) => {
                if let crate::parser::ast::FunctionArgs::Exprs(args) = &func.args {
                    for arg in args {
                        if Self::expr_has_subquery(arg) {
                            return true;
                        }
                    }
                }
                false
            }
            Expr::In { expr, list, .. } => {
                if Self::expr_has_subquery(expr) {
                    return true;
                }
                if let crate::parser::ast::InList::Subquery(_) = list {
                    return true;
                }
                if let crate::parser::ast::InList::Values(vals) = list {
                    for v in vals {
                        if Self::expr_has_subquery(v) {
                            return true;
                        }
                    }
                }
                false
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
                ..
            } => {
                if let Some(op) = operand {
                    if Self::expr_has_subquery(op) {
                        return true;
                    }
                }
                for clause in when_clauses {
                    if Self::expr_has_subquery(&clause.when)
                        || Self::expr_has_subquery(&clause.then)
                    {
                        return true;
                    }
                }
                if let Some(else_expr) = else_clause {
                    if Self::expr_has_subquery(else_expr) {
                        return true;
                    }
                }
                false
            }
            Expr::Exists { .. } => true,
            _ => false,
        }
    }

    /// Check if a SELECT statement requires the full SelectCompiler
    /// (i.e., it's not a simple "SELECT cols FROM table" query)
    fn is_complex_select(&self, select: &SelectStmt) -> bool {
        // Check for UNION, EXCEPT, INTERSECT (compound selects)
        if !matches!(select.body, SelectBody::Select(_)) {
            return true;
        }

        if let SelectBody::Select(core) = &select.body {
            // Has ORDER BY or LIMIT
            if select.order_by.is_some() || select.limit.is_some() {
                return true;
            }

            // Has GROUP BY or HAVING
            if core.group_by.is_some() || core.having.is_some() {
                return true;
            }

            // Check for multiple tables (JOINs) - more complex than simple table
            if let Some(from) = &core.from {
                if from.tables.len() > 1 {
                    return true;
                }
                // Check if table is a JOIN
                if let Some(table_ref) = from.tables.first() {
                    if matches!(table_ref, crate::parser::ast::TableRef::Join { .. }) {
                        return true;
                    }
                }
            }

            // For now, treat any WHERE clause as potentially complex
            // (The simple path doesn't handle complex WHERE conditions well)
            // This is conservative but safe - simpler queries without WHERE will still use fast path
            if core.where_clause.is_some() {
                return true;
            }
        }

        false
    }

    /// Compile a SELECT expression with proper column resolution
    fn compile_select_expr(
        &mut self,
        expr: &Expr,
        dest_reg: i32,
        source_cursor: i32,
        col_map: &HashMap<String, usize>,
    ) -> Result<()> {
        match expr {
            Expr::Literal(lit) => match lit {
                crate::parser::ast::Literal::Null => {
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                }
                crate::parser::ast::Literal::Integer(n) => {
                    if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                        self.emit(Opcode::Integer, *n as i32, dest_reg, 0, P4::Unused);
                    } else {
                        self.emit(Opcode::Int64, 0, dest_reg, 0, P4::Int64(*n));
                    }
                }
                crate::parser::ast::Literal::Float(f) => {
                    self.emit(Opcode::Real, 0, dest_reg, 0, P4::Real(*f));
                }
                crate::parser::ast::Literal::String(s) => {
                    self.emit(Opcode::String8, 0, dest_reg, 0, P4::Text(s.clone()));
                }
                crate::parser::ast::Literal::Blob(b) => {
                    self.emit(
                        Opcode::Blob,
                        b.len() as i32,
                        dest_reg,
                        0,
                        P4::Blob(b.clone()),
                    );
                }
                crate::parser::ast::Literal::Bool(b) => {
                    self.emit(
                        Opcode::Integer,
                        if *b { 1 } else { 0 },
                        dest_reg,
                        0,
                        P4::Unused,
                    );
                }
                _ => {
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                }
            },
            Expr::Column(col_ref) => {
                // Use explicit column_index if set (for SELECT *), otherwise look up by name
                let col_idx = if let Some(idx) = col_ref.column_index {
                    idx as usize
                } else {
                    let col_name = col_ref.column.to_lowercase();
                    col_map.get(&col_name).copied().unwrap_or(0)
                };
                self.emit(
                    Opcode::Column,
                    source_cursor,
                    col_idx as i32,
                    dest_reg,
                    P4::Unused,
                );
            }
            Expr::Binary { op, left, right } => {
                let left_reg = self.alloc_reg();
                let right_reg = self.alloc_reg();
                self.compile_select_expr(left, left_reg, source_cursor, col_map)?;
                self.compile_select_expr(right, right_reg, source_cursor, col_map)?;

                let opcode = match op {
                    crate::parser::ast::BinaryOp::Add => Opcode::Add,
                    crate::parser::ast::BinaryOp::Sub => Opcode::Subtract,
                    crate::parser::ast::BinaryOp::Mul => Opcode::Multiply,
                    crate::parser::ast::BinaryOp::Div => Opcode::Divide,
                    crate::parser::ast::BinaryOp::Concat => Opcode::Concat,
                    crate::parser::ast::BinaryOp::Mod => Opcode::Remainder,
                    _ => Opcode::Add,
                };

                // Arithmetic opcodes: P1=right operand, P2=left operand, P3=dest
                // Add/Sub/Mul/Div compute r[P2] op r[P1] and store in r[P3]
                self.emit(opcode, right_reg, left_reg, dest_reg, P4::Unused);
            }
            Expr::Unary { op, expr: inner } => {
                self.compile_select_expr(inner, dest_reg, source_cursor, col_map)?;
                match op {
                    crate::parser::ast::UnaryOp::Neg => {
                        self.emit(Opcode::Negative, dest_reg, dest_reg, 0, P4::Unused);
                    }
                    crate::parser::ast::UnaryOp::Not => {
                        self.emit(Opcode::Not, dest_reg, dest_reg, 0, P4::Unused);
                    }
                    _ => {}
                }
            }
            Expr::Function(func_call) => {
                // Compile function arguments
                let arg_base = self.next_reg;
                let argc = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => {
                        for arg in exprs {
                            let reg = self.alloc_reg();
                            self.compile_select_expr(arg, reg, source_cursor, col_map)?;
                        }
                        exprs.len()
                    }
                    crate::parser::ast::FunctionArgs::Star => 0,
                };

                self.emit(
                    Opcode::Function,
                    argc as i32,
                    arg_base,
                    dest_reg,
                    P4::Text(func_call.name.clone()),
                );
            }
            Expr::Subquery(_select) => {
                // Scalar subqueries in INSERT...SELECT require special handling.
                // For now, fall back to NULL - this is a known limitation.
                // TODO: Use SelectCompiler to evaluate subqueries properly.
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
            }
            _ => {
                // Default to NULL for unsupported expressions
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
            }
        }
        Ok(())
    }

    /// Validate ORDER BY in SELECT doesn't contain aggregates without GROUP BY
    fn validate_select_order_by(&self, select: &SelectStmt) -> Result<()> {
        if let Some(order_by) = &select.order_by {
            let has_group_by = match &select.body {
                SelectBody::Select(core) => core.group_by.is_some(),
                SelectBody::Compound { .. } => false,
            };
            if !has_group_by {
                for term in order_by {
                    if let Some(agg_name) = self.find_aggregate_in_expr(&term.expr) {
                        return Err(crate::error::Error::with_message(
                            crate::error::ErrorCode::Error,
                            format!("misuse of aggregate: {}()", agg_name),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Find if an expression contains an aggregate function
    fn find_aggregate_in_expr(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Function(func_call) => {
                let name_upper = func_call.name.to_uppercase();
                let arg_count = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => exprs.len(),
                    crate::parser::ast::FunctionArgs::Star => 0,
                };
                let is_aggregate = if matches!(name_upper.as_str(), "MIN" | "MAX") && arg_count > 1
                {
                    false
                } else {
                    matches!(
                        name_upper.as_str(),
                        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
                    )
                };
                if is_aggregate {
                    return Some(func_call.name.clone());
                }
                // Check arguments
                if let crate::parser::ast::FunctionArgs::Exprs(exprs) = &func_call.args {
                    for arg in exprs {
                        if let Some(found) = self.find_aggregate_in_expr(arg) {
                            return Some(found);
                        }
                    }
                }
                None
            }
            Expr::Binary { left, right, .. } => self
                .find_aggregate_in_expr(left)
                .or_else(|| self.find_aggregate_in_expr(right)),
            Expr::Unary { expr, .. } => self.find_aggregate_in_expr(expr),
            _ => None,
        }
    }

    /// Compile INSERT...DEFAULT VALUES
    fn compile_default_values(
        &mut self,
        _insert: &InsertStmt,
        conflict_action: ConflictAction,
    ) -> Result<()> {
        // Allocate rowid
        let rowid_reg = self.alloc_reg();
        self.emit(
            Opcode::NewRowid,
            self.table_cursor,
            rowid_reg,
            0,
            P4::Unused,
        );

        // All columns get default values (NULL if no default specified)
        let data_base = self.next_reg;
        let _data_regs = self.alloc_regs(self.num_columns);

        for i in 0..self.num_columns {
            let reg = data_base + i as i32;
            // In real implementation, would evaluate column default
            self.emit(Opcode::Null, 0, reg, 0, P4::Unused);
        }

        // Handle conflict
        self.emit_conflict_check(conflict_action)?;

        // Make and insert record
        let record_reg = self.alloc_reg();
        self.emit(
            Opcode::MakeRecord,
            data_base,
            self.num_columns as i32,
            record_reg,
            P4::Unused,
        );

        let flags = self.conflict_flags(conflict_action);
        self.emit_with_p5(
            Opcode::Insert,
            self.table_cursor,
            record_reg,
            rowid_reg,
            P4::Int64(flags),
            OPFLAG_NCHANGE,
        );

        // Insert into indexes
        self.emit_index_inserts(data_base, rowid_reg);

        Ok(())
    }

    /// Compile RETURNING clause
    fn compile_returning(&mut self, returning: &[ResultColumn]) -> Result<()> {
        let base_reg = self.next_reg;

        for (i, col) in returning.iter().enumerate() {
            let reg = self.alloc_reg();
            match col {
                ResultColumn::Star => {
                    // Return all columns
                    self.emit(Opcode::Column, self.table_cursor, i as i32, reg, P4::Unused);
                }
                ResultColumn::TableStar(_) => {
                    self.emit(Opcode::Column, self.table_cursor, i as i32, reg, P4::Unused);
                }
                ResultColumn::Expr { expr, .. } => {
                    self.compile_expr(expr, reg)?;
                }
            }
        }

        // Output the row
        self.emit(
            Opcode::ResultRow,
            base_reg,
            returning.len() as i32,
            0,
            P4::Unused,
        );

        Ok(())
    }

    /// Build column index map from column list
    fn build_column_map(
        &self,
        table_name: &str,
        columns: &Option<Vec<String>>,
    ) -> Result<Vec<InsertColumnTarget>> {
        match columns {
            Some(cols) => {
                // Map specified columns to their actual table indices
                let mut targets = Vec::with_capacity(cols.len());

                // Get the table schema for column validation and mapping
                let table_lower = table_name.to_lowercase();
                let table_opt = self.schema.and_then(|s| s.tables.get(&table_lower));

                // Validate each column name and map to actual index
                for col in cols {
                    if is_rowid_alias(col) {
                        targets.push(InsertColumnTarget::Rowid);
                    } else {
                        // Try to find the column in the schema
                        if let Some(table) = table_opt {
                            let col_lower = col.to_lowercase();
                            let col_index = table
                                .columns
                                .iter()
                                .position(|c| c.name.to_lowercase() == col_lower);

                            match col_index {
                                Some(idx) => {
                                    // Check if this is an INTEGER PRIMARY KEY (rowid alias)
                                    let col_def = &table.columns[idx];
                                    if col_def.is_primary_key
                                        && col_def.affinity == crate::schema::Affinity::Integer
                                    {
                                        targets.push(InsertColumnTarget::Rowid);
                                    } else {
                                        targets.push(InsertColumnTarget::Column(idx));
                                    }
                                }
                                None => {
                                    // Column doesn't exist in table
                                    return Err(crate::error::Error::with_message(
                                        crate::error::ErrorCode::Error,
                                        format!("table {} has no column named {}", table_name, col),
                                    ));
                                }
                            }
                        } else {
                            // No schema available - we'll accept the column names and validate at runtime
                            targets.push(InsertColumnTarget::Column(0));
                        }
                    }
                }
                Ok(targets)
            }
            None => {
                // All columns in order - but check for INTEGER PRIMARY KEY (rowid alias)
                let table_lower = table_name.to_lowercase();
                let table_opt = self.schema.and_then(|s| s.tables.get(&table_lower));

                Ok((0..self.num_columns)
                    .map(|i| {
                        // Check if this column is an INTEGER PRIMARY KEY (rowid alias)
                        if let Some(table) = table_opt {
                            if i < table.columns.len() {
                                let col = &table.columns[i];
                                if col.is_primary_key
                                    && col.affinity == crate::schema::Affinity::Integer
                                {
                                    return InsertColumnTarget::Rowid;
                                }
                            }
                        }
                        InsertColumnTarget::Column(i)
                    })
                    .collect())
            }
        }
    }

    /// Emit conflict checking code
    fn emit_conflict_check(&mut self, action: ConflictAction) -> Result<()> {
        match action {
            ConflictAction::Abort => {
                // Default behavior - abort on constraint violation
            }
            ConflictAction::Rollback => {
                // Will be handled by the Insert opcode flags
            }
            ConflictAction::Fail => {
                // Will be handled by the Insert opcode flags
            }
            ConflictAction::Ignore => {
                // Skip row on conflict - needs special handling
                // In a real implementation, would emit constraint checks
                // and jump past Insert if violated
            }
            ConflictAction::Replace => {
                // Delete existing row with same key
                // In a real implementation, would emit:
                // 1. Check for existing row with same unique key
                // 2. Delete if found
            }
        }
        Ok(())
    }

    /// Get Insert opcode flags for conflict action
    /// Must match OE_* constants in vdbe/engine/state.rs
    fn conflict_flags(&self, action: ConflictAction) -> i64 {
        // OE_NONE=0, OE_ROLLBACK=1, OE_ABORT=2, OE_FAIL=3, OE_IGNORE=4, OE_REPLACE=5
        match action {
            ConflictAction::Abort => 2,    // OE_ABORT
            ConflictAction::Rollback => 1, // OE_ROLLBACK
            ConflictAction::Fail => 3,     // OE_FAIL
            ConflictAction::Ignore => 4,   // OE_IGNORE
            ConflictAction::Replace => 5,  // OE_REPLACE
        }
    }

    /// Emit code for a DEFAULT value
    fn emit_default_value(
        &mut self,
        default: &crate::schema::DefaultValue,
        dest_reg: i32,
    ) -> Result<()> {
        use crate::schema::DefaultValue;

        match default {
            DefaultValue::Null => {
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
            }
            DefaultValue::Integer(n) => {
                if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                    self.emit(Opcode::Integer, *n as i32, dest_reg, 0, P4::Unused);
                } else {
                    self.emit(Opcode::Int64, 0, dest_reg, 0, P4::Int64(*n));
                }
            }
            DefaultValue::Float(f) => {
                self.emit(Opcode::Real, 0, dest_reg, 0, P4::Real(*f));
            }
            DefaultValue::String(s) => {
                self.emit(Opcode::String8, 0, dest_reg, 0, P4::Text(s.clone()));
            }
            DefaultValue::Blob(b) => {
                self.emit(Opcode::Blob, 0, dest_reg, 0, P4::Blob(b.clone()));
            }
            DefaultValue::Expr(_expr) => {
                // For expression defaults, emit NULL as fallback for now
                // Full expression evaluation would require more infrastructure
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
            }
            DefaultValue::CurrentTime => {
                self.emit(
                    Opcode::String8,
                    0,
                    dest_reg,
                    0,
                    P4::Text("current_time".to_string()),
                );
            }
            DefaultValue::CurrentDate => {
                self.emit(
                    Opcode::String8,
                    0,
                    dest_reg,
                    0,
                    P4::Text("current_date".to_string()),
                );
            }
            DefaultValue::CurrentTimestamp => {
                self.emit(
                    Opcode::String8,
                    0,
                    dest_reg,
                    0,
                    P4::Text("current_timestamp".to_string()),
                );
            }
        }
        Ok(())
    }

    /// Compile an expression
    fn compile_expr(&mut self, expr: &Expr, dest_reg: i32) -> Result<()> {
        match expr {
            Expr::Literal(lit) => match lit {
                crate::parser::ast::Literal::Null => {
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                }
                crate::parser::ast::Literal::Integer(n) => {
                    if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                        self.emit(Opcode::Integer, *n as i32, dest_reg, 0, P4::Unused);
                    } else {
                        self.emit(Opcode::Int64, 0, dest_reg, 0, P4::Int64(*n));
                    }
                }
                crate::parser::ast::Literal::Float(f) => {
                    self.emit(Opcode::Real, 0, dest_reg, 0, P4::Real(*f));
                }
                crate::parser::ast::Literal::String(s) => {
                    self.emit(Opcode::String8, 0, dest_reg, 0, P4::Text(s.clone()));
                }
                crate::parser::ast::Literal::Blob(b) => {
                    self.emit(
                        Opcode::Blob,
                        b.len() as i32,
                        dest_reg,
                        0,
                        P4::Blob(b.clone()),
                    );
                }
                crate::parser::ast::Literal::Bool(b) => {
                    self.emit(
                        Opcode::Integer,
                        if *b { 1 } else { 0 },
                        dest_reg,
                        0,
                        P4::Unused,
                    );
                }
                _ => {
                    self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
                }
            },
            Expr::Column(col_ref) => {
                // Column reference - would need to resolve from schema
                self.emit(
                    Opcode::Column,
                    0,
                    0,
                    dest_reg,
                    P4::Text(col_ref.column.clone()),
                );
            }
            Expr::Binary { op, left, right } => {
                let left_reg = self.alloc_reg();
                let right_reg = self.alloc_reg();
                self.compile_expr(left, left_reg)?;
                self.compile_expr(right, right_reg)?;

                let opcode = match op {
                    crate::parser::ast::BinaryOp::Add => Opcode::Add,
                    crate::parser::ast::BinaryOp::Sub => Opcode::Subtract,
                    crate::parser::ast::BinaryOp::Mul => Opcode::Multiply,
                    crate::parser::ast::BinaryOp::Div => Opcode::Divide,
                    crate::parser::ast::BinaryOp::Concat => Opcode::Concat,
                    _ => Opcode::Add,
                };

                self.emit(opcode, left_reg, right_reg, dest_reg, P4::Unused);
            }
            Expr::Unary { op, expr: inner } => {
                self.compile_expr(inner, dest_reg)?;
                match op {
                    crate::parser::ast::UnaryOp::Neg => {
                        self.emit(Opcode::Negative, dest_reg, dest_reg, 0, P4::Unused);
                    }
                    crate::parser::ast::UnaryOp::Not => {
                        self.emit(Opcode::Not, dest_reg, dest_reg, 0, P4::Unused);
                    }
                    _ => {}
                }
            }
            Expr::Function(func_call) => {
                // Compile function arguments
                let arg_base = self.next_reg;
                let argc = match &func_call.args {
                    crate::parser::ast::FunctionArgs::Exprs(exprs) => {
                        for arg in exprs {
                            let reg = self.alloc_reg();
                            self.compile_expr(arg, reg)?;
                        }
                        exprs.len()
                    }
                    crate::parser::ast::FunctionArgs::Star => 0,
                };

                self.emit(
                    Opcode::Function,
                    argc as i32,
                    arg_base,
                    dest_reg,
                    P4::Text(func_call.name.clone()),
                );
            }
            Expr::Variable(var) => {
                // Emit Variable opcode to read bound parameter
                let param_idx = match var {
                    crate::parser::ast::Variable::Numbered(Some(idx)) => *idx,
                    crate::parser::ast::Variable::Numbered(None) => {
                        // Unnamed parameter - use next sequential index
                        let idx = self.next_unnamed_param;
                        self.next_unnamed_param += 1;
                        idx
                    }
                    crate::parser::ast::Variable::Named { prefix, name } => {
                        // Look up named parameter in param_names
                        let full_name = format!("{}{}", prefix, name);
                        self.param_names
                            .iter()
                            .position(|n| n.as_deref() == Some(&full_name))
                            .map(|i| (i + 1) as i32) // 1-based index
                            .unwrap_or(1) // Default to 1 if not found
                    }
                };
                self.emit(Opcode::Variable, param_idx, dest_reg, 0, P4::Unused);
            }
            _ => {
                // Default to NULL for unsupported expressions
                self.emit(Opcode::Null, 0, dest_reg, 0, P4::Unused);
            }
        }
        Ok(())
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    fn alloc_reg(&mut self) -> i32 {
        let reg = self.next_reg;
        self.next_reg += 1;
        reg
    }

    fn alloc_regs(&mut self, n: usize) -> i32 {
        let base = self.next_reg;
        self.next_reg += n as i32;
        base
    }

    fn alloc_cursor(&mut self) -> i32 {
        let cursor = self.next_cursor;
        self.next_cursor += 1;
        cursor
    }

    /// Open indexes for write access
    fn open_indexes_for_write(&mut self, table_name: &str) -> Result<()> {
        // Get indexes from schema
        if let Some(schema) = self.schema {
            let table_name_lower = table_name.to_lowercase();

            // First check schema.indexes for indexes on this table
            for (_name, idx) in schema.indexes.iter() {
                if idx.table.eq_ignore_ascii_case(&table_name_lower) {
                    let cursor = self.alloc_cursor();
                    self.emit(
                        Opcode::OpenWrite,
                        cursor,
                        0, // Root page comes from schema lookup at runtime
                        0,
                        P4::Text(idx.name.clone()),
                    );

                    let columns: Vec<i32> = idx.columns.iter().map(|c| c.column_idx).collect();
                    self.index_cursors.push(IndexCursor {
                        cursor,
                        columns,
                        name: idx.name.clone(),
                    });
                }
            }

            // Also check table.indexes
            if let Some(table) = schema.tables.get(&table_name_lower) {
                for idx in &table.indexes {
                    // Skip if already added
                    if self
                        .index_cursors
                        .iter()
                        .any(|ic| ic.name.eq_ignore_ascii_case(&idx.name))
                    {
                        continue;
                    }

                    let cursor = self.alloc_cursor();
                    self.emit(Opcode::OpenWrite, cursor, 0, 0, P4::Text(idx.name.clone()));

                    let columns: Vec<i32> = idx.columns.iter().map(|c| c.column_idx).collect();
                    self.index_cursors.push(IndexCursor {
                        cursor,
                        columns,
                        name: idx.name.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Emit index insert operations after inserting a row
    /// data_base is the register containing the first column value
    /// rowid_reg is the register containing the rowid
    fn emit_index_inserts(&mut self, data_base: i32, rowid_reg: i32) {
        // Clone index_cursors to avoid borrow issues
        let index_cursors: Vec<_> = self
            .index_cursors
            .iter()
            .map(|ic| (ic.cursor, ic.columns.clone()))
            .collect();

        for (cursor, columns) in index_cursors {
            // Build index key: indexed columns + rowid
            let key_base = self.alloc_regs(columns.len() + 1);

            // Copy indexed columns to key registers
            for (i, col_idx) in columns.iter().enumerate() {
                if *col_idx >= 0 {
                    // Copy column value from data registers
                    self.emit(
                        Opcode::Copy,
                        data_base + *col_idx,
                        key_base + i as i32,
                        0,
                        P4::Unused,
                    );
                } else {
                    // Expression index - not supported yet, use null
                    self.emit(Opcode::Null, 0, key_base + i as i32, 0, P4::Unused);
                }
            }

            // Copy rowid as the last key component
            let rowid_pos = key_base + columns.len() as i32;
            self.emit(Opcode::Copy, rowid_reg, rowid_pos, 0, P4::Unused);

            // Make the index record
            let record_reg = self.alloc_reg();
            self.emit(
                Opcode::MakeRecord,
                key_base,
                (columns.len() + 1) as i32,
                record_reg,
                P4::Unused,
            );

            // Insert into index
            self.emit(Opcode::IdxInsert, cursor, record_reg, 0, P4::Unused);
        }
    }

    fn alloc_label(&mut self) -> i32 {
        let label = self.next_label;
        self.next_label -= 1;
        self.labels.insert(label, None);
        label
    }

    fn resolve_label(&mut self, label: i32, addr: i32) {
        self.labels.insert(label, Some(addr));
    }

    fn current_addr(&self) -> usize {
        self.ops.len()
    }

    fn emit(&mut self, opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: P4) {
        self.ops.push(VdbeOp::with_p4(opcode, p1, p2, p3, p4));
    }

    fn emit_with_p5(&mut self, opcode: Opcode, p1: i32, p2: i32, p3: i32, p4: P4, p5: u16) {
        self.ops
            .push(VdbeOp::with_p4(opcode, p1, p2, p3, p4).with_p5(p5));
    }

    fn resolve_labels(&mut self) -> Result<()> {
        for op in &mut self.ops {
            if op.opcode.is_jump() && op.p2 < 0 {
                if let Some(Some(addr)) = self.labels.get(&op.p2) {
                    op.p2 = *addr;
                }
            }
        }
        Ok(())
    }
}

impl<'a> Default for InsertCompiler<'a> {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Compile an INSERT statement to VDBE opcodes
pub fn compile_insert(insert: &InsertStmt) -> Result<Vec<VdbeOp>> {
    let mut compiler = InsertCompiler::new();
    compiler.compile(insert)
}

/// Compile an INSERT statement with schema for proper column count validation
pub fn compile_insert_with_schema(insert: &InsertStmt, schema: &Schema) -> Result<Vec<VdbeOp>> {
    let mut compiler = InsertCompiler::with_schema(schema);
    compiler.compile(insert)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{Literal, QualifiedName};

    #[test]
    fn test_insert_compiler_new() {
        let compiler = InsertCompiler::new();
        assert!(compiler.ops.is_empty());
        assert_eq!(compiler.next_reg, 1);
    }

    #[test]
    fn test_compile_simple_insert() {
        let insert = InsertStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("users"),
            alias: None,
            columns: None,
            source: InsertSource::Values(vec![vec![
                Expr::Literal(Literal::Integer(1)),
                Expr::Literal(Literal::String("Alice".to_string())),
                Expr::Literal(Literal::Integer(30)),
            ]]),
            on_conflict: None,
            returning: None,
        };

        let ops = compile_insert(&insert).unwrap();
        assert!(!ops.is_empty());

        // Should have Init, OpenWrite, value ops, MakeRecord, Insert, Close, Halt
        assert!(ops.iter().any(|op| op.opcode == Opcode::Init));
        assert!(ops.iter().any(|op| op.opcode == Opcode::OpenWrite));
        assert!(ops.iter().any(|op| op.opcode == Opcode::NewRowid));
        assert!(ops.iter().any(|op| op.opcode == Opcode::MakeRecord));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Insert));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Close));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Halt));
    }

    #[test]
    fn test_compile_insert_with_columns() {
        let insert = InsertStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("users"),
            alias: None,
            columns: Some(vec!["name".to_string(), "age".to_string()]),
            source: InsertSource::Values(vec![vec![
                Expr::Literal(Literal::String("Bob".to_string())),
                Expr::Literal(Literal::Integer(25)),
            ]]),
            on_conflict: None,
            returning: None,
        };

        let ops = compile_insert(&insert).unwrap();
        assert!(!ops.is_empty());
    }

    #[test]
    fn test_compile_insert_default_values() {
        let insert = InsertStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("users"),
            alias: None,
            // Provide explicit columns so compiler knows how many Null ops to emit
            columns: Some(vec![
                "id".to_string(),
                "name".to_string(),
                "age".to_string(),
            ]),
            source: InsertSource::DefaultValues,
            on_conflict: None,
            returning: None,
        };

        let ops = compile_insert(&insert).unwrap();
        assert!(!ops.is_empty());

        // Should have Null opcodes for default values
        assert!(ops.iter().any(|op| op.opcode == Opcode::Null));
    }

    #[test]
    fn test_compile_insert_or_replace() {
        let insert = InsertStmt {
            with: None,
            or_action: Some(ConflictAction::Replace),
            table: QualifiedName::new("users"),
            alias: None,
            columns: None,
            source: InsertSource::Values(vec![vec![Expr::Literal(Literal::Integer(1))]]),
            on_conflict: None,
            returning: None,
        };

        let ops = compile_insert(&insert).unwrap();
        assert!(!ops.is_empty());

        // Check that Insert has the right conflict flags
        let insert_op = ops.iter().find(|op| op.opcode == Opcode::Insert);
        assert!(insert_op.is_some());
    }

    #[test]
    fn test_compile_insert_multiple_rows() {
        let insert = InsertStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("users"),
            alias: None,
            columns: None,
            source: InsertSource::Values(vec![
                vec![Expr::Literal(Literal::Integer(1))],
                vec![Expr::Literal(Literal::Integer(2))],
                vec![Expr::Literal(Literal::Integer(3))],
            ]),
            on_conflict: None,
            returning: None,
        };

        let ops = compile_insert(&insert).unwrap();

        // Should have multiple Insert opcodes
        let insert_count = ops.iter().filter(|op| op.opcode == Opcode::Insert).count();
        assert_eq!(insert_count, 3);
    }

    #[test]
    fn test_conflict_flags() {
        let compiler = InsertCompiler::new();
        // Must match OE_* constants: OE_NONE=0, OE_ROLLBACK=1, OE_ABORT=2, OE_FAIL=3, OE_IGNORE=4, OE_REPLACE=5
        assert_eq!(compiler.conflict_flags(ConflictAction::Abort), 2); // OE_ABORT
        assert_eq!(compiler.conflict_flags(ConflictAction::Rollback), 1); // OE_ROLLBACK
        assert_eq!(compiler.conflict_flags(ConflictAction::Fail), 3); // OE_FAIL
        assert_eq!(compiler.conflict_flags(ConflictAction::Ignore), 4); // OE_IGNORE
        assert_eq!(compiler.conflict_flags(ConflictAction::Replace), 5); // OE_REPLACE
    }

    #[test]
    fn test_compile_insert_select_with_expression() {
        use crate::parser::ast::{
            BinaryOp, ColumnRef, Distinct, FromClause, SelectBody, SelectCore, SelectStmt,
        };

        // INSERT INTO t2 SELECT a+10 FROM t1
        let select = SelectStmt {
            with: None,
            body: SelectBody::Select(SelectCore {
                distinct: Distinct::All,
                columns: vec![ResultColumn::Expr {
                    expr: Expr::Binary {
                        op: BinaryOp::Add,
                        left: Box::new(Expr::Column(ColumnRef {
                            database: None,
                            table: None,
                            column: "a".to_string(),
                            column_index: None,
                        })),
                        right: Box::new(Expr::Literal(Literal::Integer(10))),
                    },
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

        let insert = InsertStmt {
            with: None,
            or_action: None,
            table: QualifiedName::new("t2"),
            alias: None,
            columns: None,
            source: InsertSource::Select(Box::new(select)),
            on_conflict: None,
            returning: None,
        };

        let ops = compile_insert(&insert).unwrap();
        assert!(!ops.is_empty());

        // Should have OpenRead for source table
        assert!(ops.iter().any(|op| op.opcode == Opcode::OpenRead));

        // Should have OpenEphemeral for buffering (critical for self-referential queries)
        assert!(ops.iter().any(|op| op.opcode == Opcode::OpenEphemeral));

        // Should have Add opcode for the a+10 expression
        assert!(
            ops.iter().any(|op| op.opcode == Opcode::Add),
            "Should compile binary Add expression"
        );

        // Should have proper loop structure with Rewind and Next
        assert!(ops.iter().any(|op| op.opcode == Opcode::Rewind));
        assert!(ops.iter().any(|op| op.opcode == Opcode::Next));
    }

    #[test]
    fn test_get_select_expressions_extracts_binary_expr() {
        use crate::parser::ast::{
            BinaryOp, ColumnRef, Distinct, SelectBody, SelectCore, SelectStmt,
        };

        let compiler = InsertCompiler::new();

        let select = SelectStmt {
            with: None,
            body: SelectBody::Select(SelectCore {
                distinct: Distinct::All,
                columns: vec![ResultColumn::Expr {
                    expr: Expr::Binary {
                        op: BinaryOp::Add,
                        left: Box::new(Expr::Column(ColumnRef {
                            database: None,
                            table: None,
                            column: "x".to_string(),
                            column_index: None,
                        })),
                        right: Box::new(Expr::Literal(Literal::Integer(100))),
                    },
                    alias: None,
                }],
                from: None,
                where_clause: None,
                group_by: None,
                having: None,
                window: None,
            }),
            order_by: None,
            limit: None,
        };

        let exprs = compiler.get_select_expressions(&select);
        assert_eq!(exprs.len(), 1);

        // Verify it's a Binary expression
        match &exprs[0] {
            Expr::Binary { op, .. } => {
                assert!(matches!(op, BinaryOp::Add));
            }
            _ => panic!("Expected Binary expression, got {:?}", exprs[0]),
        }
    }

    #[test]
    fn test_compile_select_expr_handles_column_reference() {
        let mut compiler = InsertCompiler::new();
        compiler.next_cursor = 1; // Simulate having allocated a cursor

        let mut col_map = HashMap::new();
        col_map.insert("a".to_string(), 0usize);
        col_map.insert("b".to_string(), 1usize);

        let expr = Expr::Column(crate::parser::ast::ColumnRef {
            database: None,
            table: None,
            column: "a".to_string(),
            column_index: None,
        });

        let dest_reg = 5;
        let source_cursor = 0;
        compiler
            .compile_select_expr(&expr, dest_reg, source_cursor, &col_map)
            .unwrap();

        // Should emit Column opcode with correct cursor and column index
        assert_eq!(compiler.ops.len(), 1);
        let op = &compiler.ops[0];
        assert_eq!(op.opcode, Opcode::Column);
        assert_eq!(op.p1, source_cursor); // cursor
        assert_eq!(op.p2, 0); // column index for "a"
        assert_eq!(op.p3, dest_reg); // destination register
    }

    #[test]
    fn test_compile_select_expr_handles_binary_add() {
        let mut compiler = InsertCompiler::new();

        let col_map = HashMap::new(); // Empty map - literals don't need column resolution

        // Expression: 5 + 10
        let expr = Expr::Binary {
            op: crate::parser::ast::BinaryOp::Add,
            left: Box::new(Expr::Literal(Literal::Integer(5))),
            right: Box::new(Expr::Literal(Literal::Integer(10))),
        };

        let dest_reg = 1;
        compiler
            .compile_select_expr(&expr, dest_reg, 0, &col_map)
            .unwrap();

        // Should have: Integer(5), Integer(10), Add
        assert!(compiler.ops.iter().any(|op| op.opcode == Opcode::Integer));
        assert!(compiler.ops.iter().any(|op| op.opcode == Opcode::Add));

        // The Add should write to dest_reg
        let add_op = compiler
            .ops
            .iter()
            .find(|op| op.opcode == Opcode::Add)
            .unwrap();
        assert_eq!(add_op.p3, dest_reg);
    }

    #[test]
    fn test_build_source_column_map_with_schema() {
        use crate::schema::{Column, Schema, Table};
        use std::sync::Arc;

        let mut schema = Schema::new();
        let mut table = Table::new("mytable");
        table.columns = vec![Column::new("id"), Column::new("value")];
        schema.tables.insert("mytable".to_string(), Arc::new(table));

        let compiler = InsertCompiler::with_schema(&schema);
        let col_map = compiler.build_source_column_map("mytable");

        assert_eq!(col_map.get("id"), Some(&0));
        assert_eq!(col_map.get("value"), Some(&1));
    }
}
