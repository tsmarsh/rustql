//! VDBE Tracing Facilities
//!
//! This module provides SQL execution tracing for debugging and profiling.
//! It corresponds to SQLite's vdbetrace.c.
//!
//! Tracing allows observing SQL execution with bound parameter values expanded.

use std::sync::Arc;

// ============================================================================
// Trace Flags
// ============================================================================

bitflags::bitflags! {
    /// Flags to control which events are traced
    ///
    /// These correspond to SQLite's SQLITE_TRACE_* constants.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TraceFlags: u32 {
        /// Trace SQL statement execution
        const STMT = 0x01;
        /// Profile statement timing
        const PROFILE = 0x02;
        /// Trace each result row
        const ROW = 0x04;
        /// Trace statement close/finalize
        const CLOSE = 0x08;
    }
}

impl Default for TraceFlags {
    fn default() -> Self {
        TraceFlags::empty()
    }
}

// ============================================================================
// Trace Event
// ============================================================================

/// Type of trace event
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceEvent {
    /// SQL statement is being executed
    Stmt,
    /// Statement execution completed with timing
    Profile,
    /// A result row was produced
    Row,
    /// Statement was closed/finalized
    Close,
}

impl TraceEvent {
    /// Get the flag corresponding to this event
    pub fn flag(&self) -> TraceFlags {
        match self {
            TraceEvent::Stmt => TraceFlags::STMT,
            TraceEvent::Profile => TraceFlags::PROFILE,
            TraceEvent::Row => TraceFlags::ROW,
            TraceEvent::Close => TraceFlags::CLOSE,
        }
    }
}

// ============================================================================
// Trace Info
// ============================================================================

/// Information passed to trace callbacks
#[derive(Debug, Clone)]
pub struct TraceInfo {
    /// Type of event
    pub event: TraceEvent,
    /// Expanded SQL (with bound parameters substituted)
    pub sql: String,
    /// Execution time in nanoseconds (for Profile events)
    pub elapsed_ns: Option<u64>,
    /// Row count (for Row events)
    pub row_count: Option<u64>,
}

impl TraceInfo {
    /// Create a STMT trace event
    pub fn stmt(sql: String) -> Self {
        Self {
            event: TraceEvent::Stmt,
            sql,
            elapsed_ns: None,
            row_count: None,
        }
    }

    /// Create a PROFILE trace event
    pub fn profile(sql: String, elapsed_ns: u64) -> Self {
        Self {
            event: TraceEvent::Profile,
            sql,
            elapsed_ns: Some(elapsed_ns),
            row_count: None,
        }
    }

    /// Create a ROW trace event
    pub fn row(sql: String, row_count: u64) -> Self {
        Self {
            event: TraceEvent::Row,
            sql,
            elapsed_ns: None,
            row_count: Some(row_count),
        }
    }

    /// Create a CLOSE trace event
    pub fn close(sql: String) -> Self {
        Self {
            event: TraceEvent::Close,
            sql,
            elapsed_ns: None,
            row_count: None,
        }
    }

    /// Format as a human-readable string
    pub fn to_string(&self) -> String {
        match self.event {
            TraceEvent::Stmt => format!("STMT: {}", self.sql),
            TraceEvent::Profile => format!(
                "PROFILE: {} -- {} ns",
                self.sql,
                self.elapsed_ns.unwrap_or(0)
            ),
            TraceEvent::Row => format!("ROW: {} (row {})", self.sql, self.row_count.unwrap_or(0)),
            TraceEvent::Close => format!("CLOSE: {}", self.sql),
        }
    }
}

// ============================================================================
// Trace Callback
// ============================================================================

/// Callback function for trace events
pub type TraceCallback = Arc<dyn Fn(&TraceInfo) + Send + Sync>;

// ============================================================================
// Tracer
// ============================================================================

/// Tracer configuration for a connection
#[derive(Clone)]
pub struct Tracer {
    /// Callback function (if any)
    callback: Option<TraceCallback>,
    /// Which events to trace
    mask: TraceFlags,
}

impl Default for Tracer {
    fn default() -> Self {
        Self::new()
    }
}

impl Tracer {
    /// Create a new tracer with no callback
    pub fn new() -> Self {
        Self {
            callback: None,
            mask: TraceFlags::empty(),
        }
    }

    /// Set the trace callback and mask
    pub fn set(&mut self, callback: Option<TraceCallback>, mask: TraceFlags) {
        self.callback = callback;
        self.mask = mask;
    }

    /// Check if a particular event type should be traced
    pub fn should_trace(&self, event: TraceEvent) -> bool {
        self.callback.is_some() && self.mask.contains(event.flag())
    }

    /// Emit a trace event
    pub fn trace(&self, info: &TraceInfo) {
        if let Some(ref cb) = self.callback {
            if self.mask.contains(info.event.flag()) {
                cb(info);
            }
        }
    }

    /// Emit a STMT event
    pub fn trace_stmt(&self, sql: &str) {
        if self.should_trace(TraceEvent::Stmt) {
            self.trace(&TraceInfo::stmt(sql.to_string()));
        }
    }

    /// Emit a PROFILE event
    pub fn trace_profile(&self, sql: &str, elapsed_ns: u64) {
        if self.should_trace(TraceEvent::Profile) {
            self.trace(&TraceInfo::profile(sql.to_string(), elapsed_ns));
        }
    }

    /// Emit a ROW event
    pub fn trace_row(&self, sql: &str, row_count: u64) {
        if self.should_trace(TraceEvent::Row) {
            self.trace(&TraceInfo::row(sql.to_string(), row_count));
        }
    }

    /// Emit a CLOSE event
    pub fn trace_close(&self, sql: &str) {
        if self.should_trace(TraceEvent::Close) {
            self.trace(&TraceInfo::close(sql.to_string()));
        }
    }
}

impl std::fmt::Debug for Tracer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tracer")
            .field("has_callback", &self.callback.is_some())
            .field("mask", &self.mask)
            .finish()
    }
}

// ============================================================================
// SQL Parameter Expansion
// ============================================================================

/// Expand SQL with bound parameter values
///
/// Takes SQL with parameter placeholders (?, ?1, :name, $name, @name)
/// and substitutes actual bound values.
pub fn expand_sql(sql: &str, params: &[String], param_names: &[Option<String>]) -> String {
    if sql.is_empty() {
        return String::new();
    }

    let mut result = String::with_capacity(sql.len() * 2);
    let mut positional_idx = 0;
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '?' => {
                // Positional parameter: ? or ?NNN
                let mut num_str = String::new();
                while let Some(&d) = chars.peek() {
                    if d.is_ascii_digit() {
                        num_str.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }

                let idx = if num_str.is_empty() {
                    positional_idx += 1;
                    positional_idx
                } else {
                    num_str.parse().unwrap_or(0)
                };

                // Get parameter value
                if idx > 0 && idx <= params.len() {
                    result.push_str(&params[idx - 1]);
                } else {
                    result.push('?');
                    result.push_str(&num_str);
                }
            }
            '$' | '@' | ':' => {
                // Named parameter
                let mut name = String::new();
                name.push(c);
                while let Some(&d) = chars.peek() {
                    if d.is_alphanumeric() || d == '_' {
                        name.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }

                // Look up named parameter
                if let Some(idx) = find_param_index(&name, param_names) {
                    if idx <= params.len() {
                        result.push_str(&params[idx - 1]);
                    } else {
                        result.push_str(&name);
                    }
                } else {
                    result.push_str(&name);
                }
            }
            '\'' => {
                // String literal - copy as-is including contents
                result.push(c);
                while let Some(d) = chars.next() {
                    result.push(d);
                    if d == '\'' {
                        // Check for escaped quote
                        if chars.peek() == Some(&'\'') {
                            result.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                }
            }
            '"' => {
                // Identifier - copy as-is
                result.push(c);
                while let Some(d) = chars.next() {
                    result.push(d);
                    if d == '"' {
                        break;
                    }
                }
            }
            '-' if chars.peek() == Some(&'-') => {
                // Line comment - copy to end of line
                result.push(c);
                while let Some(d) = chars.next() {
                    result.push(d);
                    if d == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                // Block comment - copy to end
                result.push(c);
                result.push(chars.next().unwrap()); // *
                while let Some(d) = chars.next() {
                    result.push(d);
                    if d == '*' && chars.peek() == Some(&'/') {
                        result.push(chars.next().unwrap());
                        break;
                    }
                }
            }
            _ => {
                result.push(c);
            }
        }
    }

    result
}

/// Find the index of a named parameter
fn find_param_index(name: &str, param_names: &[Option<String>]) -> Option<usize> {
    for (i, param_name) in param_names.iter().enumerate() {
        if let Some(n) = param_name {
            if n == name {
                return Some(i + 1);
            }
        }
    }
    None
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn test_trace_flags() {
        let flags = TraceFlags::STMT | TraceFlags::PROFILE;
        assert!(flags.contains(TraceFlags::STMT));
        assert!(flags.contains(TraceFlags::PROFILE));
        assert!(!flags.contains(TraceFlags::ROW));
    }

    #[test]
    fn test_trace_info_formatting() {
        let info = TraceInfo::stmt("SELECT 1".to_string());
        assert_eq!(info.to_string(), "STMT: SELECT 1");

        let info = TraceInfo::profile("SELECT 1".to_string(), 12345);
        assert_eq!(info.to_string(), "PROFILE: SELECT 1 -- 12345 ns");

        let info = TraceInfo::row("SELECT 1".to_string(), 5);
        assert_eq!(info.to_string(), "ROW: SELECT 1 (row 5)");
    }

    #[test]
    fn test_tracer_callback() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let events_clone = Arc::clone(&events);

        let mut tracer = Tracer::new();
        tracer.set(
            Some(Arc::new(move |info: &TraceInfo| {
                events_clone.lock().unwrap().push(info.clone());
            })),
            TraceFlags::STMT | TraceFlags::PROFILE,
        );

        tracer.trace_stmt("SELECT 1");
        tracer.trace_profile("SELECT 1", 1000);
        tracer.trace_row("SELECT 1", 1); // Should not be captured (ROW not in mask)

        let captured = events.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].event, TraceEvent::Stmt);
        assert_eq!(captured[1].event, TraceEvent::Profile);
    }

    #[test]
    fn test_expand_sql_positional() {
        let params = vec!["1".to_string(), "'hello'".to_string()];
        let names: Vec<Option<String>> = vec![];

        let sql = "SELECT * FROM t WHERE id = ? AND name = ?";
        let expanded = expand_sql(sql, &params, &names);
        assert_eq!(expanded, "SELECT * FROM t WHERE id = 1 AND name = 'hello'");
    }

    #[test]
    fn test_expand_sql_numbered() {
        let params = vec!["1".to_string(), "'hello'".to_string()];
        let names: Vec<Option<String>> = vec![];

        let sql = "SELECT * FROM t WHERE id = ?1 AND name = ?2";
        let expanded = expand_sql(sql, &params, &names);
        assert_eq!(expanded, "SELECT * FROM t WHERE id = 1 AND name = 'hello'");

        // Reversed order
        let sql = "SELECT * FROM t WHERE name = ?2 AND id = ?1";
        let expanded = expand_sql(sql, &params, &names);
        assert_eq!(expanded, "SELECT * FROM t WHERE name = 'hello' AND id = 1");
    }

    #[test]
    fn test_expand_sql_named() {
        let params = vec!["1".to_string(), "'hello'".to_string()];
        let names = vec![Some(":id".to_string()), Some(":name".to_string())];

        let sql = "SELECT * FROM t WHERE id = :id AND name = :name";
        let expanded = expand_sql(sql, &params, &names);
        assert_eq!(expanded, "SELECT * FROM t WHERE id = 1 AND name = 'hello'");
    }

    #[test]
    fn test_expand_sql_string_literal() {
        let params = vec!["1".to_string()];
        let names: Vec<Option<String>> = vec![];

        // Question mark in string should not be replaced
        let sql = "SELECT '?' WHERE id = ?";
        let expanded = expand_sql(sql, &params, &names);
        assert_eq!(expanded, "SELECT '?' WHERE id = 1");
    }

    #[test]
    fn test_expand_sql_comment() {
        let params = vec!["1".to_string()];
        let names: Vec<Option<String>> = vec![];

        // Question mark in comment should not be replaced
        let sql = "SELECT * -- comment with ?\nFROM t WHERE id = ?";
        let expanded = expand_sql(sql, &params, &names);
        assert_eq!(expanded, "SELECT * -- comment with ?\nFROM t WHERE id = 1");
    }

    #[test]
    fn test_expand_sql_block_comment() {
        let params = vec!["1".to_string()];
        let names: Vec<Option<String>> = vec![];

        let sql = "SELECT /* ? */ * FROM t WHERE id = ?";
        let expanded = expand_sql(sql, &params, &names);
        assert_eq!(expanded, "SELECT /* ? */ * FROM t WHERE id = 1");
    }
}
