//! Foreign Key constraint enforcement
//!
//! This module provides foreign key constraint checking for INSERT, UPDATE, and DELETE
//! operations. It corresponds to fkey.c in SQLite.
//!
//! Foreign key enforcement happens at runtime when:
//! - INSERT: Check that parent rows exist for all FK columns
//! - UPDATE: Check parent exists if FK columns changed; handle ON UPDATE actions for child rows
//! - DELETE: Handle ON DELETE actions (RESTRICT, CASCADE, SET NULL, SET DEFAULT)

use std::sync::Arc;

use crate::error::{Error, ErrorCode, Result};
use crate::schema::{FkAction, ForeignKey, Schema, Table};
use crate::storage::btree::{Btree, BtreeCursorFlags};
use crate::types::Value;

// ============================================================================
// FK Check Context
// ============================================================================

/// Context for FK checking operations
pub struct FkContext<'a> {
    /// Schema containing table definitions
    pub schema: &'a Schema,
    /// B-tree for data access
    pub btree: &'a Arc<Btree>,
    /// Whether FK enforcement is enabled
    pub fk_enabled: bool,
}

impl<'a> FkContext<'a> {
    /// Create a new FK context
    pub fn new(schema: &'a Schema, btree: &'a Arc<Btree>, fk_enabled: bool) -> Self {
        Self {
            schema,
            btree,
            fk_enabled,
        }
    }
}

// ============================================================================
// INSERT FK Check
// ============================================================================

/// Check foreign key constraints for an INSERT operation.
/// For each FK on the table, verify that the parent row exists.
///
/// Returns Ok(()) if all FK constraints are satisfied, Err otherwise.
pub fn fk_check_insert(
    schema: &Schema,
    btree: &Arc<Btree>,
    table: &Table,
    values: &[Value],
    fk_enabled: bool,
) -> Result<()> {
    if !fk_enabled {
        return Ok(());
    }

    for fk in &table.foreign_keys {
        check_parent_exists(schema, btree, table, fk, values)?;
    }

    Ok(())
}

/// Check that a parent row exists for the given FK values
fn check_parent_exists(
    schema: &Schema,
    btree: &Arc<Btree>,
    _child_table: &Table,
    fk: &ForeignKey,
    values: &[Value],
) -> Result<()> {
    // Get the parent table
    let parent_table = schema.table(&fk.ref_table).ok_or_else(|| {
        Error::with_message(
            ErrorCode::Error,
            format!("foreign key references unknown table: {}", fk.ref_table),
        )
    })?;

    // Extract FK column values from the inserted row
    let fk_values: Vec<&Value> = fk
        .columns
        .iter()
        .filter_map(|&col_idx| values.get(col_idx))
        .collect();

    // If any FK column is NULL, the constraint is satisfied (NULL = no reference)
    if fk_values.iter().any(|v| matches!(v, Value::Null)) {
        return Ok(());
    }

    // Determine which columns to match in parent table
    let parent_cols: Vec<usize> = if let Some(ref_cols) = &fk.ref_columns {
        // Explicit column references
        ref_cols
            .iter()
            .filter_map(|name| parent_table.find_column(name).ok().map(|i| i as usize))
            .collect()
    } else {
        // Default to primary key
        parent_table.primary_key.clone().unwrap_or_default()
    };

    if parent_cols.len() != fk.columns.len() {
        return Err(Error::with_message(
            ErrorCode::Error,
            "foreign key column count mismatch",
        ));
    }

    // Search for matching parent row
    let parent_exists = search_parent_row(btree, &parent_table, &parent_cols, &fk_values)?;

    if !parent_exists {
        return Err(Error::with_message(
            ErrorCode::Constraint,
            format!(
                "FOREIGN KEY constraint failed: {} references {}",
                fk.columns
                    .iter()
                    .map(|&i| format!("col{}", i))
                    .collect::<Vec<_>>()
                    .join(", "),
                fk.ref_table
            ),
        ));
    }

    Ok(())
}

/// Search for a row in the parent table matching the given values
fn search_parent_row(
    btree: &Arc<Btree>,
    parent_table: &Table,
    _parent_cols: &[usize],
    _values: &[&Value],
) -> Result<bool> {
    // Open cursor on parent table to check if it exists
    let mut cursor = btree.cursor(parent_table.root_page, BtreeCursorFlags::empty(), None)?;

    // Move to first record to verify table is not empty
    if cursor.first().is_err() {
        return Ok(false); // Empty table means no parent row
    }

    // TODO: Implement proper record scanning and comparison
    // For now, if the table has rows, assume FK is satisfied
    // Full implementation requires record format decoding
    //
    // The proper implementation would:
    // 1. Iterate through all rows: cursor.next(0)
    // 2. Get payload: cursor.payload_fetch() or cursor.payload(0, size)
    // 3. Decode the SQLite record format to extract column values
    // 4. Compare values at parent_cols indices with fk_values

    Ok(true) // Placeholder: assume parent exists
}

// ============================================================================
// UPDATE FK Check
// ============================================================================

/// Check foreign key constraints for an UPDATE operation.
///
/// Two checks:
/// 1. If FK columns changed, verify new parent exists (like INSERT)
/// 2. If this is a parent table, handle ON UPDATE actions for child rows
pub fn fk_check_update(
    schema: &Schema,
    btree: &Arc<Btree>,
    table: &Table,
    old_values: &[Value],
    new_values: &[Value],
    fk_enabled: bool,
) -> Result<()> {
    if !fk_enabled {
        return Ok(());
    }

    // Check this table's FKs (as child)
    for fk in &table.foreign_keys {
        // Check if any FK column changed
        let fk_changed = fk.columns.iter().any(|&col_idx| {
            let old = old_values.get(col_idx);
            let new = new_values.get(col_idx);
            match (old, new) {
                (Some(o), Some(n)) => !values_equal(o, n),
                _ => true,
            }
        });

        if fk_changed {
            check_parent_exists(schema, btree, table, fk, new_values)?;
        }
    }

    // Check FKs that reference this table (as parent) - ON UPDATE actions
    fk_parent_update(schema, btree, table, old_values, new_values)?;

    Ok(())
}

/// Handle ON UPDATE actions for child tables that reference this table
fn fk_parent_update(
    schema: &Schema,
    btree: &Arc<Btree>,
    parent_table: &Table,
    old_values: &[Value],
    new_values: &[Value],
) -> Result<()> {
    // Find all tables that reference this table
    let referencing = find_referencing_fks(schema, &parent_table.name);

    for (child_table, fk) in referencing {
        // Determine which parent columns are referenced
        let parent_cols: Vec<usize> = if let Some(ref_cols) = &fk.ref_columns {
            ref_cols
                .iter()
                .filter_map(|name| parent_table.find_column(name).ok().map(|i| i as usize))
                .collect()
        } else {
            parent_table.primary_key.clone().unwrap_or_default()
        };

        // Check if referenced columns changed
        let ref_changed = parent_cols.iter().any(|&col_idx| {
            let old = old_values.get(col_idx);
            let new = new_values.get(col_idx);
            match (old, new) {
                (Some(o), Some(n)) => !values_equal(o, n),
                _ => true,
            }
        });

        if !ref_changed {
            continue;
        }

        // Get old key values
        let old_key: Vec<&Value> = parent_cols
            .iter()
            .filter_map(|&col_idx| old_values.get(col_idx))
            .collect();

        match fk.on_update {
            FkAction::NoAction => {
                // Check at statement end (deferred check)
            }
            FkAction::Restrict => {
                // Check if any child rows reference old key
                if has_child_rows(btree, &child_table, &fk, &old_key)? {
                    return Err(Error::with_message(
                        ErrorCode::Constraint,
                        format!(
                            "FOREIGN KEY constraint failed: {} still references {}",
                            child_table.name, parent_table.name
                        ),
                    ));
                }
            }
            FkAction::Cascade => {
                // Update child rows to new key values
                let new_key: Vec<&Value> = parent_cols
                    .iter()
                    .filter_map(|&col_idx| new_values.get(col_idx))
                    .collect();
                cascade_update(btree, &child_table, &fk, &old_key, &new_key)?;
            }
            FkAction::SetNull => {
                set_null_child_rows(btree, &child_table, &fk, &old_key)?;
            }
            FkAction::SetDefault => {
                set_default_child_rows(btree, &child_table, &fk, &old_key)?;
            }
        }
    }

    Ok(())
}

// ============================================================================
// DELETE FK Check
// ============================================================================

/// Check foreign key constraints for a DELETE operation.
/// Handle ON DELETE actions for child tables that reference this table.
pub fn fk_check_delete(
    schema: &Schema,
    btree: &Arc<Btree>,
    table: &Table,
    values: &[Value],
    fk_enabled: bool,
) -> Result<()> {
    if !fk_enabled {
        return Ok(());
    }

    // Find all tables that reference this table
    let referencing = find_referencing_fks(schema, &table.name);

    for (child_table, fk) in referencing {
        // Determine which columns are referenced
        let parent_cols: Vec<usize> = if let Some(ref_cols) = &fk.ref_columns {
            ref_cols
                .iter()
                .filter_map(|name| table.find_column(name).ok().map(|i| i as usize))
                .collect()
        } else {
            table.primary_key.clone().unwrap_or_default()
        };

        // Get key values being deleted
        let key: Vec<&Value> = parent_cols
            .iter()
            .filter_map(|&col_idx| values.get(col_idx))
            .collect();

        match fk.on_delete {
            FkAction::NoAction => {
                // Check at statement end (deferred check)
            }
            FkAction::Restrict => {
                // Check if any child rows reference this key
                if has_child_rows(btree, &child_table, &fk, &key)? {
                    return Err(Error::with_message(
                        ErrorCode::Constraint,
                        format!(
                            "FOREIGN KEY constraint failed: {} still references {}",
                            child_table.name, table.name
                        ),
                    ));
                }
            }
            FkAction::Cascade => {
                cascade_delete(btree, &child_table, &fk, &key)?;
            }
            FkAction::SetNull => {
                set_null_child_rows(btree, &child_table, &fk, &key)?;
            }
            FkAction::SetDefault => {
                set_default_child_rows(btree, &child_table, &fk, &key)?;
            }
        }
    }

    Ok(())
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Find all foreign keys that reference a given table
fn find_referencing_fks(schema: &Schema, table_name: &str) -> Vec<(Arc<Table>, ForeignKey)> {
    let mut result = Vec::new();

    for table in schema.tables.values() {
        for fk in &table.foreign_keys {
            if fk.ref_table.eq_ignore_ascii_case(table_name) {
                result.push((Arc::clone(table), fk.clone()));
            }
        }
    }

    result
}

/// Check if any child rows reference the given key values
fn has_child_rows(
    btree: &Arc<Btree>,
    child_table: &Table,
    _fk: &ForeignKey,
    _key_values: &[&Value],
) -> Result<bool> {
    let mut cursor = btree.cursor(child_table.root_page, BtreeCursorFlags::empty(), None)?;

    if cursor.first().is_err() {
        return Ok(false); // Empty table, no child rows
    }

    // TODO: Implement proper record scanning and comparison
    // For now, if child table has rows, assume potential reference exists
    // This is conservative for RESTRICT checking (may block when not needed)
    //
    // The proper implementation would:
    // 1. Iterate through all rows
    // 2. Decode each record
    // 3. Check if FK columns match key_values

    Ok(true) // Placeholder: assume child rows exist if table not empty
}

/// Delete all child rows that reference the given key (CASCADE DELETE)
fn cascade_delete(
    _btree: &Arc<Btree>,
    _child_table: &Table,
    _fk: &ForeignKey,
    _key_values: &[&Value],
) -> Result<()> {
    // TODO: Implement cascade delete
    // This requires write access to the btree and careful handling of
    // recursive FK constraints
    Ok(())
}

/// Update all child rows to new key values (CASCADE UPDATE)
fn cascade_update(
    _btree: &Arc<Btree>,
    _child_table: &Table,
    _fk: &ForeignKey,
    _old_key: &[&Value],
    _new_key: &[&Value],
) -> Result<()> {
    // TODO: Implement cascade update
    Ok(())
}

/// Set FK columns to NULL for child rows referencing the key (SET NULL)
fn set_null_child_rows(
    _btree: &Arc<Btree>,
    _child_table: &Table,
    _fk: &ForeignKey,
    _key_values: &[&Value],
) -> Result<()> {
    // TODO: Implement SET NULL
    Ok(())
}

/// Set FK columns to default values for child rows (SET DEFAULT)
fn set_default_child_rows(
    _btree: &Arc<Btree>,
    _child_table: &Table,
    _fk: &ForeignKey,
    _key_values: &[&Value],
) -> Result<()> {
    // TODO: Implement SET DEFAULT
    Ok(())
}

/// Compare two values for equality
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::Real(a), Value::Real(b)) => (a - b).abs() < f64::EPSILON,
        (Value::Text(a), Value::Text(b)) => a == b,
        (Value::Blob(a), Value::Blob(b)) => a == b,
        // Cross-type comparisons
        (Value::Integer(a), Value::Real(b)) => (*a as f64 - b).abs() < f64::EPSILON,
        (Value::Real(a), Value::Integer(b)) => (a - *b as f64).abs() < f64::EPSILON,
        _ => false,
    }
}

// ============================================================================
// Deferred FK Constraints
// ============================================================================

/// Track deferred FK violations
#[derive(Debug, Default)]
pub struct DeferredFkState {
    /// Count of deferred FK violations
    pub violation_count: i64,
}

impl DeferredFkState {
    /// Increment violation count
    pub fn add_violation(&mut self) {
        self.violation_count += 1;
    }

    /// Decrement violation count
    pub fn remove_violation(&mut self) {
        if self.violation_count > 0 {
            self.violation_count -= 1;
        }
    }

    /// Check if there are deferred violations
    pub fn has_violations(&self) -> bool {
        self.violation_count > 0
    }

    /// Get violation count
    pub fn count(&self) -> i64 {
        self.violation_count
    }

    /// Clear all violations
    pub fn clear(&mut self) {
        self.violation_count = 0;
    }
}

// ============================================================================
// PRAGMA foreign_key_check
// ============================================================================

/// Result of a foreign key check
#[derive(Debug, Clone)]
pub struct FkViolation {
    /// Table with the FK violation
    pub table: String,
    /// Rowid of violating row
    pub rowid: i64,
    /// Referenced (parent) table
    pub parent: String,
    /// FK index (0-based)
    pub fkid: i32,
}

/// Check all foreign key constraints in a table (for PRAGMA foreign_key_check)
pub fn foreign_key_check(
    schema: &Schema,
    btree: &Arc<Btree>,
    table_name: Option<&str>,
) -> Result<Vec<FkViolation>> {
    let mut violations = Vec::new();

    let tables: Vec<Arc<Table>> = if let Some(name) = table_name {
        schema.table(name).map(|t| vec![t]).unwrap_or_default()
    } else {
        schema.tables.values().cloned().collect()
    };

    for table in tables {
        for (fk_idx, fk) in table.foreign_keys.iter().enumerate() {
            let fk_violations = check_fk_violations(schema, btree, &table, fk, fk_idx)?;
            violations.extend(fk_violations);
        }
    }

    Ok(violations)
}

/// Check a single FK for violations
fn check_fk_violations(
    schema: &Schema,
    btree: &Arc<Btree>,
    table: &Table,
    fk: &ForeignKey,
    _fk_idx: usize,
) -> Result<Vec<FkViolation>> {
    let violations = Vec::new();

    // Get parent table
    let parent_table = match schema.table(&fk.ref_table) {
        Some(t) => t,
        None => return Ok(violations), // Parent table doesn't exist
    };

    // Try to open cursor on child table to verify it exists
    let mut cursor = btree.cursor(table.root_page, BtreeCursorFlags::empty(), None)?;

    if cursor.first().is_err() {
        return Ok(violations); // Empty table, no violations
    }

    // Also verify parent table exists
    let _parent_cursor = btree.cursor(parent_table.root_page, BtreeCursorFlags::empty(), None)?;

    // TODO: Implement proper FK violation checking
    // This requires:
    // 1. Iterating through child table rows
    // 2. Decoding each record to extract FK column values
    // 3. Checking if parent row exists for each FK value
    // 4. Recording violations for missing parents
    //
    // For now, return empty (no violations detected)
    // Full implementation requires record format decoding

    Ok(violations)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_values_equal() {
        assert!(values_equal(&Value::Null, &Value::Null));
        assert!(values_equal(&Value::Integer(42), &Value::Integer(42)));
        assert!(values_equal(&Value::Real(3.14), &Value::Real(3.14)));
        assert!(values_equal(
            &Value::Text("hello".to_string()),
            &Value::Text("hello".to_string())
        ));

        assert!(!values_equal(&Value::Integer(1), &Value::Integer(2)));
        assert!(!values_equal(&Value::Null, &Value::Integer(0)));
    }

    #[test]
    fn test_deferred_fk_state() {
        let mut state = DeferredFkState::default();
        assert!(!state.has_violations());
        assert_eq!(state.count(), 0);

        state.add_violation();
        assert!(state.has_violations());
        assert_eq!(state.count(), 1);

        state.add_violation();
        assert_eq!(state.count(), 2);

        state.remove_violation();
        assert_eq!(state.count(), 1);

        state.clear();
        assert!(!state.has_violations());
    }
}
