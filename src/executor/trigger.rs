//! Trigger Compilation and Execution
//!
//! This module handles:
//! - CREATE TRIGGER statement compilation
//! - DROP TRIGGER statement compilation
//! - Trigger firing infrastructure
//! - OLD/NEW row pseudo-table access

use std::sync::Arc;

use crate::error::{Error, ErrorCode, Result};
use crate::parser::ast::{CreateTriggerStmt, TriggerEvent as AstTriggerEvent, TriggerTime};
use crate::schema::{Schema, Trigger, TriggerEvent, TriggerTiming};
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

// ============================================================================
// Trigger Compilation
// ============================================================================

/// Compile a CREATE TRIGGER statement to VDBE bytecode
pub fn compile_create_trigger(
    schema: &mut Schema,
    create: &CreateTriggerStmt,
    sql: &str,
) -> Result<Vec<VdbeOp>> {
    // Validate table exists
    let table_name = &create.table;
    let _table = schema.table(table_name).ok_or_else(|| {
        Error::with_message(ErrorCode::Error, format!("no such table: {}", table_name))
    })?;

    // Check for INSTEAD OF on regular table (table.is_virtual is false for views)
    // In SQLite, views are stored in sqlite_master with type='view', not as regular tables
    // For now, we'll skip this validation since we'd need to track views separately

    // Convert AST types to schema types
    let timing = match create.time {
        TriggerTime::Before => TriggerTiming::Before,
        TriggerTime::After => TriggerTiming::After,
        TriggerTime::InsteadOf => TriggerTiming::InsteadOf,
    };

    let (event, update_columns) = match &create.event {
        AstTriggerEvent::Delete => (TriggerEvent::Delete, None),
        AstTriggerEvent::Insert => (TriggerEvent::Insert, None),
        AstTriggerEvent::Update(cols) => (TriggerEvent::Update, cols.clone()),
    };

    // Create trigger definition
    // Note: We store the raw SQL and re-parse body when needed
    // This avoids complex type conversions between AST and schema types
    let trigger = Trigger {
        name: create.name.name.clone(),
        table: table_name.clone(),
        timing,
        event,
        for_each_row: create.for_each_row,
        update_columns,
        when_clause: None, // Parsed from SQL when needed
        body: Vec::new(),  // Parsed from SQL when needed
        sql: Some(sql.to_string()),
    };

    // Check for duplicate trigger
    let trigger_name_lower = trigger.name.to_lowercase();
    if schema.triggers.contains_key(&trigger_name_lower) {
        if create.if_not_exists {
            // Return success without doing anything
            let mut ops = Vec::new();
            ops.push(make_op(Opcode::Init, 0, 1, 0, P4::Unused));
            ops.push(make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
            return Ok(ops);
        }
        return Err(Error::with_message(
            ErrorCode::Error,
            format!("trigger {} already exists", trigger.name),
        ));
    }

    // Store in schema
    let trigger_name = trigger.name.clone();
    schema
        .triggers
        .insert(trigger_name_lower, Arc::new(trigger));

    // Generate bytecode to record in sqlite_master
    let mut ops = Vec::new();
    ops.push(make_op(Opcode::Init, 0, 1, 0, P4::Unused));

    // In a full implementation, we'd insert into sqlite_master here
    // For now, the trigger is stored in memory
    ops.push(make_op(
        Opcode::Noop,
        0,
        0,
        0,
        P4::Text(format!("CREATE TRIGGER {}", trigger_name)),
    ));

    ops.push(make_op(Opcode::Halt, 0, 0, 0, P4::Unused));

    Ok(ops)
}

/// Compile a DROP TRIGGER statement to VDBE bytecode
pub fn compile_drop_trigger(
    schema: &mut Schema,
    name: &str,
    if_exists: bool,
) -> Result<Vec<VdbeOp>> {
    let name_lower = name.to_lowercase();

    if !schema.triggers.contains_key(&name_lower) {
        if if_exists {
            let mut ops = Vec::new();
            ops.push(make_op(Opcode::Init, 0, 1, 0, P4::Unused));
            ops.push(make_op(Opcode::Halt, 0, 0, 0, P4::Unused));
            return Ok(ops);
        }
        return Err(Error::with_message(
            ErrorCode::Error,
            format!("no such trigger: {}", name),
        ));
    }

    // Remove from schema
    schema.triggers.remove(&name_lower);

    let mut ops = Vec::new();
    ops.push(make_op(Opcode::Init, 0, 1, 0, P4::Unused));
    ops.push(make_op(
        Opcode::Noop,
        0,
        0,
        0,
        P4::Text(format!("DROP TRIGGER {}", name)),
    ));
    ops.push(make_op(Opcode::Halt, 0, 0, 0, P4::Unused));

    Ok(ops)
}

// ============================================================================
// Trigger Firing
// ============================================================================

/// Trigger timing/event bits for efficient matching
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TriggerMask(u8);

impl TriggerMask {
    /// Before timing
    pub const BEFORE: u8 = 0x01;
    /// After timing
    pub const AFTER: u8 = 0x02;
    /// Instead of timing
    pub const INSTEAD_OF: u8 = 0x04;
    /// Insert event
    pub const INSERT: u8 = 0x08;
    /// Delete event
    pub const DELETE: u8 = 0x10;
    /// Update event
    pub const UPDATE: u8 = 0x20;

    pub fn new(timing: TriggerTiming, event: TriggerEvent) -> Self {
        let mut bits = 0u8;

        bits |= match timing {
            TriggerTiming::Before => Self::BEFORE,
            TriggerTiming::After => Self::AFTER,
            TriggerTiming::InsteadOf => Self::INSTEAD_OF,
        };

        bits |= match event {
            TriggerEvent::Insert => Self::INSERT,
            TriggerEvent::Delete => Self::DELETE,
            TriggerEvent::Update => Self::UPDATE,
        };

        Self(bits)
    }

    pub fn matches(&self, other: &TriggerMask) -> bool {
        // Check timing matches
        let timing_match = (self.0 & 0x07) == (other.0 & 0x07);
        // Check event matches
        let event_match = (self.0 & 0x38) == (other.0 & 0x38);
        timing_match && event_match
    }

    pub fn bits(&self) -> u8 {
        self.0
    }
}

/// Find triggers that match a given operation
pub fn find_matching_triggers(
    schema: &Schema,
    table_name: &str,
    timing: TriggerTiming,
    event: TriggerEvent,
    update_columns: Option<&[String]>,
) -> Vec<Arc<Trigger>> {
    let target_mask = TriggerMask::new(timing, event);

    schema
        .triggers
        .values()
        .filter(|trigger| {
            // Check table matches
            if trigger.table.to_lowercase() != table_name.to_lowercase() {
                return false;
            }

            // Check timing/event matches
            let trigger_mask = TriggerMask::new(trigger.timing, trigger.event);
            if !trigger_mask.matches(&target_mask) {
                return false;
            }

            // For UPDATE triggers, check column list
            if event == TriggerEvent::Update {
                if let (Some(trigger_cols), Some(update_cols)) =
                    (&trigger.update_columns, update_columns)
                {
                    // Trigger only fires if at least one of its columns is being updated
                    let trigger_cols_lower: Vec<_> =
                        trigger_cols.iter().map(|c| c.to_lowercase()).collect();
                    let update_cols_lower: Vec<_> =
                        update_cols.iter().map(|c| c.to_lowercase()).collect();

                    let has_match = trigger_cols_lower
                        .iter()
                        .any(|c| update_cols_lower.contains(c));
                    if !has_match {
                        return false;
                    }
                }
            }

            true
        })
        .cloned()
        .collect()
}

/// Generate code to fire triggers for an operation
///
/// This is a placeholder that returns empty bytecode.
/// A full implementation would:
/// 1. Check each matching trigger's WHEN condition
/// 2. Execute the trigger body statements
/// 3. Handle recursive trigger prevention
pub fn generate_trigger_code(
    _triggers: &[Arc<Trigger>],
    _old_reg: Option<i32>,
    _new_reg: Option<i32>,
    _rowid_reg: i32,
) -> Result<Vec<VdbeOp>> {
    // Full implementation requires nested VDBE execution
    // which is complex and will be done incrementally
    Ok(Vec::new())
}

/// Context for trigger execution
#[derive(Debug, Clone)]
pub struct TriggerContext {
    /// OLD row values (for DELETE/UPDATE triggers)
    pub old_values: Option<Vec<crate::types::Value>>,
    /// NEW row values (for INSERT/UPDATE triggers)
    pub new_values: Option<Vec<crate::types::Value>>,
    /// Rowid of affected row
    pub rowid: i64,
    /// Trigger recursion depth (for detecting infinite loops)
    pub depth: u32,
    /// Maximum recursion depth (default 1000, like SQLite)
    pub max_depth: u32,
}

impl TriggerContext {
    pub fn new() -> Self {
        Self {
            old_values: None,
            new_values: None,
            rowid: 0,
            depth: 0,
            max_depth: 1000,
        }
    }

    /// Check if we've exceeded the recursion limit
    pub fn check_recursion(&self) -> Result<()> {
        if self.depth >= self.max_depth {
            return Err(Error::with_message(
                ErrorCode::Constraint,
                "too many levels of trigger recursion",
            ));
        }
        Ok(())
    }

    /// Enter a trigger (increment depth)
    pub fn enter(&mut self) -> Result<()> {
        self.check_recursion()?;
        self.depth += 1;
        Ok(())
    }

    /// Exit a trigger (decrement depth)
    pub fn exit(&mut self) {
        if self.depth > 0 {
            self.depth -= 1;
        }
    }
}

impl Default for TriggerContext {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trigger_mask() {
        let before_insert = TriggerMask::new(TriggerTiming::Before, TriggerEvent::Insert);
        let after_insert = TriggerMask::new(TriggerTiming::After, TriggerEvent::Insert);
        let before_delete = TriggerMask::new(TriggerTiming::Before, TriggerEvent::Delete);

        // Same timing and event should match
        let same = TriggerMask::new(TriggerTiming::Before, TriggerEvent::Insert);
        assert!(before_insert.matches(&same));

        // Different timing should not match
        assert!(!before_insert.matches(&after_insert));

        // Different event should not match
        assert!(!before_insert.matches(&before_delete));
    }

    #[test]
    fn test_trigger_context() {
        let ctx = TriggerContext::new();
        assert!(ctx.old_values.is_none());
        assert!(ctx.new_values.is_none());
        assert_eq!(ctx.rowid, 0);
        assert_eq!(ctx.depth, 0);
        assert_eq!(ctx.max_depth, 1000);
    }

    #[test]
    fn test_trigger_context_recursion() {
        let mut ctx = TriggerContext::new();
        ctx.max_depth = 3;

        // Should succeed up to max_depth
        assert!(ctx.enter().is_ok());
        assert_eq!(ctx.depth, 1);
        assert!(ctx.enter().is_ok());
        assert_eq!(ctx.depth, 2);
        assert!(ctx.enter().is_ok());
        assert_eq!(ctx.depth, 3);

        // Should fail at max_depth
        assert!(ctx.enter().is_err());
        assert_eq!(ctx.depth, 3); // depth unchanged on error

        // Exit should work
        ctx.exit();
        assert_eq!(ctx.depth, 2);
    }

    #[test]
    fn test_find_matching_triggers() {
        // This requires a schema setup, so just test the empty case
        let schema = Schema::default();
        let triggers = find_matching_triggers(
            &schema,
            "test",
            TriggerTiming::Before,
            TriggerEvent::Insert,
            None,
        );
        assert!(triggers.is_empty());
    }
}
