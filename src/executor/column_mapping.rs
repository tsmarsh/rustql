//! Unified column mapping for INSERT, UPDATE, DELETE, and SELECT statements
//!
//! Provides a common solution for mapping between source and target columns,
//! handling DEFAULT values, NULL fills, and schema validation.
//!
//! This module is used by all statement executors to consistently handle:
//! - Explicit vs implicit column specification
//! - DEFAULT value substitution
//! - NULL filling for unmapped columns
//! - Case-insensitive column name matching
//! - Table qualification validation

use std::collections::HashMap;

use crate::error::Result;
use crate::schema::{Column, Schema};

#[cfg(test)]
use crate::schema::DefaultValue;

/// How a target column should be filled
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnSource {
    /// Read from source at this index (e.g., SELECT column)
    SourceIndex(usize),
    /// Use the column's DEFAULT value
    DefaultValue,
    /// Use NULL
    Null,
}

/// Maps source values to target table columns
pub struct ColumnMapper {
    table_name: String,
    target_columns: Vec<Column>,
    mapping: Vec<ColumnSource>,
}

impl ColumnMapper {
    /// Create a new column mapper
    ///
    /// # Arguments
    /// * `table_name` - Target table name
    /// * `explicit_cols` - Explicitly specified column names (None = all columns)
    /// * `source_count` - Number of columns from source (SELECT/VALUES)
    /// * `schema` - Database schema for validation and defaults
    pub fn new(
        table_name: &str,
        explicit_cols: Option<&[String]>,
        source_count: usize,
        schema: Option<&Schema>,
    ) -> Result<Self> {
        // Get table definition from schema
        let target_columns = if let Some(schema) = schema {
            let table_lower = table_name.to_lowercase();
            schema
                .tables
                .get(&table_lower)
                .map(|t| t.columns.clone())
                .unwrap_or_default()
        } else {
            // No schema - create placeholder columns
            (0..source_count)
                .map(|i| {
                    let mut col = Column::default();
                    col.name = format!("col{}", i);
                    col
                })
                .collect()
        };

        // Build the mapping
        let mapping = if let Some(explicit_cols) = explicit_cols {
            Self::build_explicit_mapping(&target_columns, explicit_cols, source_count)?
        } else {
            Self::build_implicit_mapping(&target_columns, source_count)?
        };

        Ok(ColumnMapper {
            table_name: table_name.to_string(),
            target_columns,
            mapping,
        })
    }

    /// Build mapping for explicit column list (INSERT INTO t(a, c) SELECT ...)
    fn build_explicit_mapping(
        target_columns: &[Column],
        explicit_cols: &[String],
        source_count: usize,
    ) -> Result<Vec<ColumnSource>> {
        let mut mapping = vec![ColumnSource::Null; target_columns.len()];

        for (src_idx, col_name) in explicit_cols.iter().enumerate() {
            if src_idx >= source_count {
                break; // More target cols than source cols
            }

            // Find this column in target table
            let col_lower = col_name.to_lowercase();
            let found = target_columns
                .iter()
                .position(|c| c.name.to_lowercase() == col_lower);

            match found {
                Some(target_idx) => {
                    // Check if this column already has a source
                    // (shouldn't happen with valid SQL, but be safe)
                    mapping[target_idx] = ColumnSource::SourceIndex(src_idx);
                }
                None => {
                    return Err(crate::error::Error::with_message(
                        crate::error::ErrorCode::Error,
                        format!("no such column: {}", col_name),
                    ));
                }
            }
        }

        // Fill in DEFAULTs for unmapped columns
        for (target_idx, source) in mapping.iter_mut().enumerate() {
            if matches!(source, ColumnSource::Null) {
                if target_columns[target_idx].default_value.is_some() {
                    *source = ColumnSource::DefaultValue;
                }
            }
        }

        Ok(mapping)
    }

    /// Build mapping for implicit column list (INSERT INTO t SELECT ...)
    fn build_implicit_mapping(
        target_columns: &[Column],
        source_count: usize,
    ) -> Result<Vec<ColumnSource>> {
        let mut mapping = Vec::new();

        for (target_idx, _col) in target_columns.iter().enumerate() {
            if target_idx < source_count {
                // Source has a value for this column
                mapping.push(ColumnSource::SourceIndex(target_idx));
            } else if target_columns[target_idx].default_value.is_some() {
                // Source doesn't have a value, use DEFAULT
                mapping.push(ColumnSource::DefaultValue);
            } else {
                // Source doesn't have a value and no DEFAULT
                mapping.push(ColumnSource::Null);
            }
        }

        Ok(mapping)
    }

    /// Get the mapping for all target columns
    pub fn mapping(&self) -> &[ColumnSource] {
        &self.mapping
    }

    /// Get target column count
    pub fn target_count(&self) -> usize {
        self.target_columns.len()
    }

    /// Get a specific target column
    pub fn get_column(&self, index: usize) -> Option<&Column> {
        self.target_columns.get(index)
    }

    /// Get column by name (case-insensitive)
    pub fn get_column_by_name(&self, name: &str) -> Option<(usize, &Column)> {
        let name_lower = name.to_lowercase();
        self.target_columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.name.to_lowercase() == name_lower)
            .map(|(idx, col)| (idx, col))
    }

    /// Get all target columns
    pub fn columns(&self) -> &[Column] {
        &self.target_columns
    }

    /// Build a column name to index map (useful for SELECT column resolution)
    pub fn build_name_map(&self) -> HashMap<String, usize> {
        let mut map = HashMap::new();
        for (i, col) in self.target_columns.iter().enumerate() {
            map.insert(col.name.to_lowercase(), i);
        }
        map
    }

    /// Validate that a column exists (used in WHERE clauses, etc.)
    pub fn validate_column(&self, col_name: &str) -> Result<usize> {
        let col_lower = col_name.to_lowercase();
        self.target_columns
            .iter()
            .position(|c| c.name.to_lowercase() == col_lower)
            .ok_or_else(|| {
                crate::error::Error::with_message(
                    crate::error::ErrorCode::Error,
                    format!("no such column: {}", col_name),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_implicit_mapping_all_cols() {
        let mut col_a = Column::default();
        col_a.name = "a".to_string();

        let mut col_b = Column::default();
        col_b.name = "b".to_string();

        let columns = vec![col_a, col_b];

        let mapping = ColumnMapper::build_implicit_mapping(&columns, 2).unwrap();
        assert_eq!(mapping.len(), 2);
        assert_eq!(mapping[0], ColumnSource::SourceIndex(0));
        assert_eq!(mapping[1], ColumnSource::SourceIndex(1));
    }

    #[test]
    fn test_implicit_mapping_with_default() {
        let mut col_a = Column::default();
        col_a.name = "a".to_string();

        let mut col_b = Column::default();
        col_b.name = "b".to_string();
        col_b.default_value = Some(DefaultValue::String("default_b".to_string()));

        let columns = vec![col_a, col_b];

        let mapping = ColumnMapper::build_implicit_mapping(&columns, 1).unwrap();
        assert_eq!(mapping.len(), 2);
        assert_eq!(mapping[0], ColumnSource::SourceIndex(0));
        assert_eq!(mapping[1], ColumnSource::DefaultValue);
    }

    #[test]
    fn test_explicit_mapping() {
        let mut col_a = Column::default();
        col_a.name = "a".to_string();

        let mut col_b = Column::default();
        col_b.name = "b".to_string();
        col_b.default_value = Some(DefaultValue::String("default_b".to_string()));

        let columns = vec![col_a, col_b];
        let explicit = vec!["a".to_string()];

        let mapping = ColumnMapper::build_explicit_mapping(&columns, &explicit, 1).unwrap();
        assert_eq!(mapping.len(), 2);
        assert_eq!(mapping[0], ColumnSource::SourceIndex(0));
        assert_eq!(mapping[1], ColumnSource::DefaultValue);
    }

    #[test]
    fn test_explicit_mapping_partial() {
        let mut col_a = Column::default();
        col_a.name = "a".to_string();

        let mut col_b = Column::default();
        col_b.name = "b".to_string();

        let mut col_c = Column::default();
        col_c.name = "c".to_string();
        col_c.default_value = Some(DefaultValue::Integer(42));

        let columns = vec![col_a, col_b, col_c];
        let explicit = vec!["b".to_string(), "a".to_string()];

        let mapping = ColumnMapper::build_explicit_mapping(&columns, &explicit, 2).unwrap();
        assert_eq!(mapping.len(), 3);
        assert_eq!(mapping[0], ColumnSource::SourceIndex(1)); // a gets source index 1
        assert_eq!(mapping[1], ColumnSource::SourceIndex(0)); // b gets source index 0
        assert_eq!(mapping[2], ColumnSource::DefaultValue); // c gets DEFAULT
    }
}
