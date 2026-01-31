//! Virtual table adapter for R-tree spatial indexes
//!
//! This module wraps the existing R-tree implementation with the VtabModule,
//! VtabTable, and VtabCursor traits to integrate with the virtual table registry.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::error::{Error, ErrorCode, Result};
use crate::rtree::{RtreeBbox, RtreeConstraint, RtreeResult, RtreeTable};
use crate::schema::Affinity;
use crate::vdbe::mem::Mem;
use crate::vtab::{
    ConstraintValue, IndexInfo, VtabCursor, VtabModule, VtabTable, SQLITE_INDEX_CONSTRAINT_EQ,
    SQLITE_INDEX_CONSTRAINT_GE, SQLITE_INDEX_CONSTRAINT_GT, SQLITE_INDEX_CONSTRAINT_LE,
    SQLITE_INDEX_CONSTRAINT_LT,
};

// ============================================================================
// Global R-tree Registry
// ============================================================================

lazy_static::lazy_static! {
    static ref RTREE_REGISTRY: RwLock<HashMap<String, Arc<Mutex<RtreeTable>>>> =
        RwLock::new(HashMap::new());
}

/// Register an R-tree table in the global registry
pub fn register_table(name: &str, table: RtreeTable) -> Arc<Mutex<RtreeTable>> {
    let table_arc = Arc::new(Mutex::new(table));
    if let Ok(mut registry) = RTREE_REGISTRY.write() {
        registry.insert(name.to_lowercase(), table_arc.clone());
    }
    table_arc
}

/// Get an R-tree table from the global registry
pub fn get_table(name: &str) -> Option<Arc<Mutex<RtreeTable>>> {
    RTREE_REGISTRY
        .read()
        .ok()
        .and_then(|registry| registry.get(&name.to_lowercase()).cloned())
}

// ============================================================================
// R-tree Virtual Table Module
// ============================================================================

/// R-tree virtual table module
///
/// This module creates R-tree spatial index tables.
///
/// Syntax: CREATE VIRTUAL TABLE t USING rtree(id, minX, maxX, minY, maxY, ...)
pub struct RtreeVtabModule;

impl VtabModule for RtreeVtabModule {
    fn name(&self) -> &str {
        "rtree"
    }

    fn create(
        &self,
        db_name: &str,
        table_name: &str,
        args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        // Parse R-tree arguments to get dimensions
        // R-tree syntax: rtree(id, minX, maxX, minY, maxY, ...)
        // First column is always the id (INTEGER PRIMARY KEY)
        // Remaining columns are coordinate pairs (min, max) for each dimension

        let (id_column, coord_columns) = parse_rtree_args(args)?;
        let n_dim = coord_columns.len() / 2;

        if n_dim == 0 {
            return Err(Error::with_message(
                ErrorCode::Error,
                "rtree requires at least one dimension (two coordinate columns)",
            ));
        }

        // Build schema string for sqlite_master
        let mut schema_parts = vec![format!("{} INTEGER PRIMARY KEY", id_column)];
        for col in &coord_columns {
            schema_parts.push(format!("{} REAL", col));
        }
        let schema = format!("({})", schema_parts.join(", "));

        // Create the R-tree table
        let rtree_table = RtreeTable::new(n_dim, 0)?;

        // Register in the global registry
        let full_name = format!("{}.{}", db_name, table_name);
        let table_arc = register_table(&full_name, rtree_table);

        // Create the adapter
        let adapter = RtreeVtabTableAdapter {
            name: table_name.to_string(),
            db_name: db_name.to_string(),
            id_column,
            coord_columns,
            n_dim,
            table: table_arc,
        };

        Ok((schema, Arc::new(adapter)))
    }

    fn connect(
        &self,
        db_name: &str,
        table_name: &str,
        args: &[String],
    ) -> Result<(String, Arc<dyn VtabTable>)> {
        // For R-tree, connect is the same as create (in-memory implementation)
        self.create(db_name, table_name, args)
    }

    fn destroy(&self, _table: Arc<dyn VtabTable>) -> Result<()> {
        Ok(())
    }

    fn uses_shadow_tables(&self) -> bool {
        true
    }

    fn shadow_table_suffixes(&self) -> Vec<&'static str> {
        vec!["_node", "_rowid", "_parent"]
    }
}

// ============================================================================
// R-tree Virtual Table Adapter
// ============================================================================

/// R-tree virtual table instance adapter
pub struct RtreeVtabTableAdapter {
    name: String,
    db_name: String,
    id_column: String,
    coord_columns: Vec<String>,
    n_dim: usize,
    table: Arc<Mutex<RtreeTable>>,
}

impl VtabTable for RtreeVtabTableAdapter {
    fn table_name(&self) -> &str {
        &self.name
    }

    fn db_name(&self) -> &str {
        &self.db_name
    }

    fn column_count(&self) -> usize {
        // id column + coordinate columns
        1 + self.coord_columns.len()
    }

    fn column_name(&self, col_idx: usize) -> &str {
        if col_idx == 0 {
            &self.id_column
        } else if col_idx <= self.coord_columns.len() {
            &self.coord_columns[col_idx - 1]
        } else {
            ""
        }
    }

    fn column_affinity(&self, col_idx: usize) -> Affinity {
        if col_idx == 0 {
            Affinity::Integer
        } else {
            Affinity::Real
        }
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // R-tree supports range queries on coordinates
        // Look for constraints on coordinate columns

        // First pass: analyze constraints (immutable borrow)
        let mut id_eq_idx: Option<usize> = None;
        let mut spatial_constraints: Vec<usize> = Vec::new();

        for (idx, constraint) in info.constraints.iter().enumerate() {
            if !constraint.usable {
                continue;
            }

            let col = constraint.col_idx as usize;
            if col == 0 && constraint.op == SQLITE_INDEX_CONSTRAINT_EQ {
                // id column - EQ constraint is fast
                id_eq_idx = Some(idx);
                break; // ID lookup is the best, stop looking
            } else if col <= self.coord_columns.len() {
                // Coordinate column - range constraints
                match constraint.op {
                    SQLITE_INDEX_CONSTRAINT_EQ
                    | SQLITE_INDEX_CONSTRAINT_LT
                    | SQLITE_INDEX_CONSTRAINT_LE
                    | SQLITE_INDEX_CONSTRAINT_GT
                    | SQLITE_INDEX_CONSTRAINT_GE => {
                        spatial_constraints.push(idx);
                    }
                    _ => {}
                }
            }
        }

        // Second pass: mark constraints as used (mutable borrow)
        if let Some(idx) = id_eq_idx {
            info.use_constraint(idx, 1, true);
            info.idx_num = 1; // 1 = id lookup
            info.estimated_cost = 1.0;
            info.estimated_rows = 1;
        } else if !spatial_constraints.is_empty() {
            let mut constraint_mask = 0u32;
            for (arg_idx, &constraint_idx) in spatial_constraints.iter().enumerate() {
                info.use_constraint(constraint_idx, (arg_idx + 1) as i32, false);
                constraint_mask |= 1 << constraint_idx;
            }
            info.idx_num = 2 | ((constraint_mask as i32) << 8); // 2 = spatial query
            info.estimated_cost = 100.0;
            info.estimated_rows = 100;
        } else {
            info.idx_num = 0; // 0 = full scan
            info.estimated_cost = 1_000_000.0;
            info.estimated_rows = 1_000_000;
        }

        Ok(())
    }

    fn open_cursor(&self) -> Result<Box<dyn VtabCursor>> {
        Ok(Box::new(RtreeVtabCursorAdapter {
            table: self.table.clone(),
            n_dim: self.n_dim,
            results: Vec::new(),
            position: 0,
        }))
    }

    fn update(
        &self,
        rowid: Option<i64>,
        new_rowid: Option<i64>,
        columns: &[Option<Mem>],
    ) -> Result<i64> {
        let mut table = self
            .table
            .lock()
            .map_err(|_| Error::with_message(ErrorCode::Internal, "failed to lock R-tree table"))?;

        if rowid.is_none() && new_rowid.is_some() {
            // INSERT
            let rowid = new_rowid.unwrap();
            let coords = extract_coords(columns, self.n_dim)?;
            table.insert(rowid, &coords)?;
            Ok(rowid)
        } else if let Some(old_rowid) = rowid {
            if columns.is_empty() {
                // DELETE
                table.delete(old_rowid)?;
                Ok(old_rowid)
            } else {
                // UPDATE = DELETE + INSERT
                table.delete(old_rowid)?;
                let new_rid = new_rowid.unwrap_or(old_rowid);
                let coords = extract_coords(columns, self.n_dim)?;
                table.insert(new_rid, &coords)?;
                Ok(new_rid)
            }
        } else {
            Err(Error::with_message(
                ErrorCode::Error,
                "invalid update parameters",
            ))
        }
    }
}

// ============================================================================
// R-tree Cursor Adapter
// ============================================================================

/// R-tree cursor adapter
pub struct RtreeVtabCursorAdapter {
    table: Arc<Mutex<RtreeTable>>,
    n_dim: usize,
    results: Vec<RtreeResult>,
    position: usize,
}

impl VtabCursor for RtreeVtabCursorAdapter {
    fn filter(
        &mut self,
        index_num: i32,
        _index_str: Option<&str>,
        constraints: &[ConstraintValue],
    ) -> Result<()> {
        let table = self
            .table
            .lock()
            .map_err(|_| Error::with_message(ErrorCode::Internal, "failed to lock R-tree table"))?;

        let idx_type = index_num & 0xFF;

        if idx_type == 1 && !constraints.is_empty() {
            // ID lookup
            let id = constraints[0].as_int();
            // Do a full scan and filter by ID
            let all_results = table.query(RtreeConstraint::Overlap(RtreeBbox::new(self.n_dim)?));
            self.results = all_results.into_iter().filter(|r| r.rowid == id).collect();
        } else if idx_type == 2 {
            // Spatial query - build bounding box from constraints
            let mut min_coords = vec![f64::NEG_INFINITY; self.n_dim];
            let mut max_coords = vec![f64::INFINITY; self.n_dim];

            // Parse constraints to build query box
            // This is a simplified implementation
            for (i, cv) in constraints.iter().enumerate() {
                let dim = i / 2;
                let is_max = i % 2 == 1;
                if dim < self.n_dim {
                    let val = cv.as_float();
                    if is_max {
                        max_coords[dim] = max_coords[dim].min(val);
                    } else {
                        min_coords[dim] = min_coords[dim].max(val);
                    }
                }
            }

            // Build query bbox
            let mut coords = Vec::with_capacity(self.n_dim * 2);
            for i in 0..self.n_dim {
                coords.push(min_coords[i]);
                coords.push(max_coords[i]);
            }

            if let Ok(bbox) = RtreeBbox::from_coords(&coords) {
                self.results = table.query(RtreeConstraint::Overlap(bbox));
            } else {
                self.results = Vec::new();
            }
        } else {
            // Full scan - get all entries
            // Query with infinite bounding box
            let mut coords = Vec::with_capacity(self.n_dim * 2);
            for _ in 0..self.n_dim {
                coords.push(f64::NEG_INFINITY);
                coords.push(f64::INFINITY);
            }

            if let Ok(bbox) = RtreeBbox::from_coords(&coords) {
                self.results = table.query(RtreeConstraint::Overlap(bbox));
            } else {
                self.results = Vec::new();
            }
        }

        self.position = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.position += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.position >= self.results.len()
    }

    fn rowid(&self) -> Result<i64> {
        if self.position < self.results.len() {
            Ok(self.results[self.position].rowid)
        } else {
            Ok(0)
        }
    }

    fn column(&self, col_idx: usize) -> Result<Mem> {
        if self.position >= self.results.len() {
            return Ok(Mem::new());
        }

        let result = &self.results[self.position];

        if col_idx == 0 {
            // id column
            Ok(Mem::from_int(result.rowid))
        } else {
            // coordinate columns
            let coord_idx = col_idx - 1;
            let dim = coord_idx / 2;
            let is_max = coord_idx % 2 == 1;

            if dim < result.bbox.min.len() {
                let val = if is_max {
                    result.bbox.max[dim]
                } else {
                    result.bbox.min[dim]
                };
                Ok(Mem::from_real(val))
            } else {
                Ok(Mem::new())
            }
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Parse R-tree arguments from CREATE VIRTUAL TABLE
///
/// Expected format: rtree(id, minX, maxX, minY, maxY, ...)
fn parse_rtree_args(args: &[String]) -> Result<(String, Vec<String>)> {
    if args.is_empty() {
        return Err(Error::with_message(
            ErrorCode::Error,
            "rtree requires column definitions",
        ));
    }

    let mut columns: Vec<String> = Vec::new();

    for arg in args {
        let arg = arg.trim();
        if arg.is_empty() {
            continue;
        }

        // Parse column definition (strip type annotations)
        let col_name = arg.split_whitespace().next().unwrap_or(arg);
        columns.push(col_name.to_string());
    }

    if columns.is_empty() {
        return Err(Error::with_message(
            ErrorCode::Error,
            "rtree requires at least an id column",
        ));
    }

    // First column is the id
    let id_column = columns.remove(0);

    // Remaining columns are coordinates (must be even number for min/max pairs)
    if columns.len() % 2 != 0 {
        return Err(Error::with_message(
            ErrorCode::Error,
            "rtree coordinate columns must come in min/max pairs",
        ));
    }

    Ok((id_column, columns))
}

/// Extract coordinates from column values
fn extract_coords(columns: &[Option<Mem>], n_dim: usize) -> Result<Vec<f64>> {
    let mut coords = Vec::with_capacity(n_dim * 2);

    // Skip first column (id), extract coordinate values
    for i in 1..columns.len() {
        if let Some(mem) = &columns[i] {
            coords.push(mem.to_real());
        } else {
            coords.push(0.0);
        }
    }

    // Pad with zeros if needed
    while coords.len() < n_dim * 2 {
        coords.push(0.0);
    }

    Ok(coords)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rtree_args() {
        let args = vec![
            "id".to_string(),
            "minX".to_string(),
            "maxX".to_string(),
            "minY".to_string(),
            "maxY".to_string(),
        ];
        let (id, coords) = parse_rtree_args(&args).unwrap();
        assert_eq!(id, "id");
        assert_eq!(coords, vec!["minX", "maxX", "minY", "maxY"]);
    }

    #[test]
    fn test_parse_rtree_args_with_types() {
        let args = vec![
            "id INTEGER".to_string(),
            "minX REAL".to_string(),
            "maxX REAL".to_string(),
        ];
        let (id, coords) = parse_rtree_args(&args).unwrap();
        assert_eq!(id, "id");
        assert_eq!(coords, vec!["minX", "maxX"]);
    }

    #[test]
    fn test_rtree_module_create() {
        let module = RtreeVtabModule;
        let (schema, table) = module
            .create(
                "main",
                "test_rtree",
                &[
                    "id".to_string(),
                    "minX".to_string(),
                    "maxX".to_string(),
                    "minY".to_string(),
                    "maxY".to_string(),
                ],
            )
            .unwrap();

        assert!(schema.contains("id INTEGER PRIMARY KEY"));
        assert!(schema.contains("minX REAL"));
        assert!(schema.contains("maxY REAL"));
        assert_eq!(table.table_name(), "test_rtree");
        assert_eq!(table.column_count(), 5);
    }

    #[test]
    fn test_rtree_odd_coords_error() {
        let module = RtreeVtabModule;
        let result = module.create(
            "main",
            "test_rtree",
            &[
                "id".to_string(),
                "minX".to_string(),
                "maxX".to_string(),
                "minY".to_string(), // Missing maxY
            ],
        );
        assert!(result.is_err());
    }
}
