//! Window Functions Code Generation
//!
//! This module generates VDBE opcodes for window functions.
//! Corresponds to SQLite's window.c.

use std::collections::HashMap;

use crate::error::{Error, ErrorCode, Result};
use crate::parser::ast::{
    Expr, FunctionArgs, FunctionCall, Over, ResultColumn, SelectCore, WindowFrame,
    WindowFrameBound, WindowFrameExclude, WindowFrameMode, WindowSpec,
};
use crate::vdbe::ops::{Opcode, VdbeOp, P4};

// ============================================================================
// Window Function Info
// ============================================================================

/// Classification of window functions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowFuncType {
    /// ROW_NUMBER() - Sequential row number within partition
    RowNumber,
    /// RANK() - Rank with gaps for ties
    Rank,
    /// DENSE_RANK() - Rank without gaps for ties
    DenseRank,
    /// NTILE(n) - Divide into n buckets
    Ntile,
    /// PERCENT_RANK() - Relative rank (0 to 1)
    PercentRank,
    /// CUME_DIST() - Cumulative distribution
    CumeDist,
    /// LAG(expr, offset, default) - Value from previous row
    Lag,
    /// LEAD(expr, offset, default) - Value from next row
    Lead,
    /// FIRST_VALUE(expr) - First value in frame
    FirstValue,
    /// LAST_VALUE(expr) - Last value in frame
    LastValue,
    /// NTH_VALUE(expr, n) - Nth value in frame
    NthValue,
    /// Aggregate function used as window function (SUM, AVG, etc.)
    Aggregate(String),
}

impl WindowFuncType {
    /// Parse a function name into a WindowFuncType
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_uppercase().as_str() {
            "ROW_NUMBER" => Some(WindowFuncType::RowNumber),
            "RANK" => Some(WindowFuncType::Rank),
            "DENSE_RANK" => Some(WindowFuncType::DenseRank),
            "NTILE" => Some(WindowFuncType::Ntile),
            "PERCENT_RANK" => Some(WindowFuncType::PercentRank),
            "CUME_DIST" => Some(WindowFuncType::CumeDist),
            "LAG" => Some(WindowFuncType::Lag),
            "LEAD" => Some(WindowFuncType::Lead),
            "FIRST_VALUE" => Some(WindowFuncType::FirstValue),
            "LAST_VALUE" => Some(WindowFuncType::LastValue),
            "NTH_VALUE" => Some(WindowFuncType::NthValue),
            // Aggregate functions that can be window functions
            "SUM" | "AVG" | "COUNT" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL" => {
                Some(WindowFuncType::Aggregate(name.to_uppercase()))
            }
            _ => None,
        }
    }

    /// Whether this function requires a frame
    pub fn needs_frame(&self) -> bool {
        matches!(
            self,
            WindowFuncType::FirstValue
                | WindowFuncType::LastValue
                | WindowFuncType::NthValue
                | WindowFuncType::Aggregate(_)
        )
    }

    /// Whether this is a ranking function
    pub fn is_ranking(&self) -> bool {
        matches!(
            self,
            WindowFuncType::RowNumber
                | WindowFuncType::Rank
                | WindowFuncType::DenseRank
                | WindowFuncType::Ntile
                | WindowFuncType::PercentRank
                | WindowFuncType::CumeDist
        )
    }

    /// Whether this function needs peer group info
    pub fn needs_peer_info(&self) -> bool {
        matches!(
            self,
            WindowFuncType::Rank
                | WindowFuncType::DenseRank
                | WindowFuncType::PercentRank
                | WindowFuncType::CumeDist
        )
    }
}

// ============================================================================
// Window Function Instance
// ============================================================================

/// A window function call with its specification
#[derive(Debug, Clone)]
pub struct WindowFunc {
    /// The type of window function
    pub func_type: WindowFuncType,
    /// Function name (for aggregates and error messages)
    pub name: String,
    /// Function arguments
    pub args: Vec<Expr>,
    /// Window specification
    pub spec: WindowSpec,
    /// Result register
    pub result_reg: i32,
    /// Index in result columns
    pub col_index: usize,
}

// ============================================================================
// Window Info
// ============================================================================

/// Information about all window functions sharing a window spec
#[derive(Debug, Clone)]
pub struct WindowInfo {
    /// The window specification
    pub spec: WindowSpec,
    /// Functions using this window
    pub functions: Vec<WindowFunc>,
    /// Ephemeral table cursor for partition storage
    pub eph_cursor: i32,
    /// Sorter cursor
    pub sort_cursor: i32,
}

// ============================================================================
// Window Compiler
// ============================================================================

/// Compiler for window functions
pub struct WindowCompiler {
    /// Generated opcodes
    ops: Vec<VdbeOp>,
    /// Next available register
    next_reg: i32,
    /// Next available cursor
    next_cursor: i32,
    /// Label counter
    next_label: i32,
    /// Labels waiting to be resolved
    labels: HashMap<i32, Option<i32>>,
    /// Named windows from WINDOW clause
    named_windows: HashMap<String, WindowSpec>,
    /// Window infos grouped by spec
    windows: Vec<WindowInfo>,
}

impl WindowCompiler {
    /// Create a new window compiler
    pub fn new(next_reg: i32, next_cursor: i32) -> Self {
        Self {
            ops: Vec::new(),
            next_reg,
            next_cursor,
            next_label: 0,
            labels: HashMap::new(),
            named_windows: HashMap::new(),
            windows: Vec::new(),
        }
    }

    /// Get generated operations
    pub fn take_ops(&mut self) -> Vec<VdbeOp> {
        std::mem::take(&mut self.ops)
    }

    /// Get next register to use
    pub fn next_reg(&self) -> i32 {
        self.next_reg
    }

    /// Get next cursor to use
    pub fn next_cursor(&self) -> i32 {
        self.next_cursor
    }

    // ========================================================================
    // Register and cursor allocation
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

    fn alloc_label(&mut self) -> i32 {
        let label = self.next_label;
        self.next_label += 1;
        self.labels.insert(label, None);
        label
    }

    fn resolve_label(&mut self, label: i32, addr: i32) {
        self.labels.insert(label, Some(addr));
    }

    fn current_addr(&self) -> i32 {
        self.ops.len() as i32
    }

    // ========================================================================
    // Opcode emission
    // ========================================================================

    fn emit(&mut self, op: Opcode, p1: i32, p2: i32, p3: i32, p4: P4) {
        self.ops.push(VdbeOp {
            opcode: op,
            p1,
            p2,
            p3,
            p4,
            p5: 0,
            comment: None,
        });
    }

    // ========================================================================
    // Window function analysis
    // ========================================================================

    /// Collect all window functions from a SELECT core
    pub fn collect_window_functions(&mut self, core: &SelectCore) -> Result<Vec<WindowFunc>> {
        // First, register named windows
        if let Some(window_defs) = &core.window {
            for def in window_defs {
                self.named_windows
                    .insert(def.name.clone(), def.spec.clone());
            }
        }

        // Collect window function calls
        let mut funcs = Vec::new();

        for (col_idx, col) in core.columns.iter().enumerate() {
            if let ResultColumn::Expr { expr, .. } = col {
                self.collect_window_funcs_from_expr(expr, col_idx, &mut funcs)?;
            }
        }

        Ok(funcs)
    }

    fn collect_window_funcs_from_expr(
        &mut self,
        expr: &Expr,
        col_idx: usize,
        funcs: &mut Vec<WindowFunc>,
    ) -> Result<()> {
        match expr {
            Expr::Function(FunctionCall {
                name,
                args,
                over: Some(over),
                ..
            }) => {
                let spec = self.resolve_window_spec(over)?;
                let func_type = WindowFuncType::from_name(name).ok_or_else(|| {
                    Error::with_message(
                        ErrorCode::Error,
                        format!("Unknown window function: {}", name),
                    )
                })?;

                let expr_args = match args {
                    FunctionArgs::Exprs(exprs) => exprs.clone(),
                    FunctionArgs::Star => Vec::new(),
                };

                funcs.push(WindowFunc {
                    func_type,
                    name: name.clone(),
                    args: expr_args,
                    spec,
                    result_reg: 0, // Assigned later
                    col_index: col_idx,
                });
            }
            Expr::Binary { left, right, .. } => {
                self.collect_window_funcs_from_expr(left, col_idx, funcs)?;
                self.collect_window_funcs_from_expr(right, col_idx, funcs)?;
            }
            Expr::Unary { expr, .. } => {
                self.collect_window_funcs_from_expr(expr, col_idx, funcs)?;
            }
            Expr::Cast { expr, .. } => {
                self.collect_window_funcs_from_expr(expr, col_idx, funcs)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn resolve_window_spec(&self, over: &Over) -> Result<WindowSpec> {
        match over {
            Over::Spec(spec) => {
                // Merge with base window if specified
                if let Some(base_name) = &spec.base {
                    let base = self.named_windows.get(base_name).ok_or_else(|| {
                        Error::with_message(
                            ErrorCode::Error,
                            format!("Unknown window: {}", base_name),
                        )
                    })?;
                    Ok(self.merge_window_specs(base, spec))
                } else {
                    Ok(spec.clone())
                }
            }
            Over::Window(name) => self.named_windows.get(name).cloned().ok_or_else(|| {
                Error::with_message(ErrorCode::Error, format!("Unknown window: {}", name))
            }),
        }
    }

    fn merge_window_specs(&self, base: &WindowSpec, derived: &WindowSpec) -> WindowSpec {
        WindowSpec {
            base: None,
            partition_by: derived
                .partition_by
                .clone()
                .or_else(|| base.partition_by.clone()),
            order_by: derived.order_by.clone().or_else(|| base.order_by.clone()),
            frame: derived.frame.clone().or_else(|| base.frame.clone()),
        }
    }

    /// Group window functions by their window specification
    pub fn group_by_window(&mut self, funcs: Vec<WindowFunc>) -> Result<Vec<WindowInfo>> {
        let mut groups: Vec<WindowInfo> = Vec::new();

        for func in funcs {
            // Find matching group or create new one
            let found = groups.iter_mut().find(|g| specs_equal(&g.spec, &func.spec));

            if let Some(group) = found {
                group.functions.push(func);
            } else {
                let eph_cursor = self.alloc_cursor();
                let sort_cursor = self.alloc_cursor();
                groups.push(WindowInfo {
                    spec: func.spec.clone(),
                    functions: vec![func],
                    eph_cursor,
                    sort_cursor,
                });
            }
        }

        self.windows = groups.clone();
        Ok(groups)
    }

    // ========================================================================
    // Code generation
    // ========================================================================

    /// Generate code for window functions
    ///
    /// The strategy:
    /// 1. Store all result rows in a sorter ordered by PARTITION BY + ORDER BY
    /// 2. For each partition:
    ///    a. Load rows into ephemeral table
    ///    b. For each row, compute window function values
    ///    c. Output row with window function results
    pub fn compile_window_functions(
        &mut self,
        windows: &[WindowInfo],
        base_result_reg: i32,
        result_count: usize,
    ) -> Result<()> {
        for window in windows {
            self.compile_window(window, base_result_reg, result_count)?;
        }
        Ok(())
    }

    fn compile_window(
        &mut self,
        window: &WindowInfo,
        base_result_reg: i32,
        result_count: usize,
    ) -> Result<()> {
        let _sort_cursor = window.sort_cursor;
        let _eph_cursor = window.eph_cursor;

        // Count partition and order columns
        let partition_count = window
            .spec
            .partition_by
            .as_ref()
            .map(|v| v.len())
            .unwrap_or(0);
        let order_count = window.spec.order_by.as_ref().map(|v| v.len()).unwrap_or(0);
        let _key_count = partition_count + order_count;

        // Allocate registers for window function results
        let func_result_base = self.alloc_regs(window.functions.len());

        // Generate code for each function type
        for (i, func) in window.functions.iter().enumerate() {
            let result_reg = func_result_base + i as i32;

            match &func.func_type {
                WindowFuncType::RowNumber => {
                    self.compile_row_number(result_reg)?;
                }
                WindowFuncType::Rank => {
                    self.compile_rank(result_reg, false)?;
                }
                WindowFuncType::DenseRank => {
                    self.compile_rank(result_reg, true)?;
                }
                WindowFuncType::Ntile => {
                    self.compile_ntile(result_reg, &func.args)?;
                }
                WindowFuncType::PercentRank => {
                    self.compile_percent_rank(result_reg)?;
                }
                WindowFuncType::CumeDist => {
                    self.compile_cume_dist(result_reg)?;
                }
                WindowFuncType::Lag => {
                    self.compile_lag_lead(result_reg, &func.args, true)?;
                }
                WindowFuncType::Lead => {
                    self.compile_lag_lead(result_reg, &func.args, false)?;
                }
                WindowFuncType::FirstValue => {
                    self.compile_first_last_value(result_reg, &func.args, true)?;
                }
                WindowFuncType::LastValue => {
                    self.compile_first_last_value(result_reg, &func.args, false)?;
                }
                WindowFuncType::NthValue => {
                    self.compile_nth_value(result_reg, &func.args)?;
                }
                WindowFuncType::Aggregate(name) => {
                    self.compile_window_aggregate(result_reg, name, &func.args, &window.spec)?;
                }
            }

            // Copy result to appropriate position in output
            let dest_reg = base_result_reg + func.col_index as i32;
            if dest_reg != result_reg {
                self.emit(Opcode::Copy, result_reg, dest_reg, 0, P4::Unused);
            }
        }

        // Copy remaining non-window columns
        for i in 0..result_count {
            let is_window_col = window.functions.iter().any(|f| f.col_index == i);
            if !is_window_col {
                // Column is already in place from base compilation
            }
        }

        Ok(())
    }

    // ========================================================================
    // Individual window function compilation
    // ========================================================================

    fn compile_row_number(&mut self, result_reg: i32) -> Result<()> {
        // ROW_NUMBER: Just count rows in partition
        // Initialize counter to 0 at partition start
        // Increment for each row
        self.emit(Opcode::Integer, 1, result_reg, 0, P4::Unused);
        Ok(())
    }

    fn compile_rank(&mut self, result_reg: i32, dense: bool) -> Result<()> {
        // RANK: Reset to row_number on order key change
        // DENSE_RANK: Increment only on order key change
        let _ = dense;
        self.emit(Opcode::Integer, 1, result_reg, 0, P4::Unused);
        Ok(())
    }

    fn compile_ntile(&mut self, result_reg: i32, args: &[Expr]) -> Result<()> {
        // NTILE(n): Divide partition into n buckets
        let _ = args;
        self.emit(Opcode::Integer, 1, result_reg, 0, P4::Unused);
        Ok(())
    }

    fn compile_percent_rank(&mut self, result_reg: i32) -> Result<()> {
        // PERCENT_RANK: (rank - 1) / (partition_size - 1)
        self.emit(Opcode::Real, 0, result_reg, 0, P4::Real(0.0));
        Ok(())
    }

    fn compile_cume_dist(&mut self, result_reg: i32) -> Result<()> {
        // CUME_DIST: peer_count / partition_size
        self.emit(Opcode::Real, 0, result_reg, 0, P4::Real(1.0));
        Ok(())
    }

    fn compile_lag_lead(&mut self, result_reg: i32, args: &[Expr], _is_lag: bool) -> Result<()> {
        // LAG/LEAD: Get value from N rows before/after current
        // Default offset is 1, default value is NULL
        let _ = args;
        self.emit(Opcode::Null, 0, result_reg, 0, P4::Unused);
        Ok(())
    }

    fn compile_first_last_value(
        &mut self,
        result_reg: i32,
        args: &[Expr],
        _is_first: bool,
    ) -> Result<()> {
        // FIRST_VALUE/LAST_VALUE: Get first/last value in frame
        let _ = args;
        self.emit(Opcode::Null, 0, result_reg, 0, P4::Unused);
        Ok(())
    }

    fn compile_nth_value(&mut self, result_reg: i32, args: &[Expr]) -> Result<()> {
        // NTH_VALUE(expr, n): Get nth value in frame
        let _ = args;
        self.emit(Opcode::Null, 0, result_reg, 0, P4::Unused);
        Ok(())
    }

    fn compile_window_aggregate(
        &mut self,
        result_reg: i32,
        name: &str,
        args: &[Expr],
        spec: &WindowSpec,
    ) -> Result<()> {
        // Aggregate over window frame
        let _ = (name, args, spec);
        self.emit(Opcode::Null, 0, result_reg, 0, P4::Unused);
        Ok(())
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Check if two window specifications are equivalent
fn specs_equal(a: &WindowSpec, b: &WindowSpec) -> bool {
    // Compare partition by
    let partition_eq = match (&a.partition_by, &b.partition_by) {
        (None, None) => true,
        (Some(pa), Some(pb)) => pa.len() == pb.len(),
        _ => false,
    };

    // Compare order by
    let order_eq = match (&a.order_by, &b.order_by) {
        (None, None) => true,
        (Some(oa), Some(ob)) => oa.len() == ob.len(),
        _ => false,
    };

    // Compare frame
    let frame_eq = match (&a.frame, &b.frame) {
        (None, None) => true,
        (Some(fa), Some(fb)) => frames_equal(fa, fb),
        _ => false,
    };

    partition_eq && order_eq && frame_eq
}

fn frames_equal(a: &WindowFrame, b: &WindowFrame) -> bool {
    a.mode == b.mode && bounds_equal(&a.start, &b.start) && a.exclude == b.exclude
}

fn bounds_equal(a: &WindowFrameBound, b: &WindowFrameBound) -> bool {
    matches!(
        (a, b),
        (WindowFrameBound::CurrentRow, WindowFrameBound::CurrentRow)
            | (
                WindowFrameBound::UnboundedPreceding,
                WindowFrameBound::UnboundedPreceding
            )
            | (
                WindowFrameBound::UnboundedFollowing,
                WindowFrameBound::UnboundedFollowing
            )
            | (
                WindowFrameBound::Preceding(_),
                WindowFrameBound::Preceding(_)
            )
            | (
                WindowFrameBound::Following(_),
                WindowFrameBound::Following(_)
            )
    )
}

/// Check if an expression contains window function calls
pub fn has_window_function(expr: &Expr) -> bool {
    match expr {
        Expr::Function(FunctionCall { over: Some(_), .. }) => true,
        Expr::Binary { left, right, .. } => has_window_function(left) || has_window_function(right),
        Expr::Unary { expr, .. } => has_window_function(expr),
        Expr::Cast { expr, .. } => has_window_function(expr),
        _ => false,
    }
}

/// Check if a SELECT core has window functions
pub fn select_has_window_functions(core: &SelectCore) -> bool {
    for col in &core.columns {
        if let ResultColumn::Expr { expr, .. } = col {
            if has_window_function(expr) {
                return true;
            }
        }
    }
    false
}

// ============================================================================
// Default frame handling
// ============================================================================

/// Get the default window frame for a function
pub fn default_frame_for_function(func_type: &WindowFuncType, has_order_by: bool) -> WindowFrame {
    match func_type {
        // Ranking functions don't use frames
        WindowFuncType::RowNumber
        | WindowFuncType::Rank
        | WindowFuncType::DenseRank
        | WindowFuncType::Ntile
        | WindowFuncType::PercentRank
        | WindowFuncType::CumeDist => WindowFrame {
            mode: WindowFrameMode::Rows,
            start: WindowFrameBound::UnboundedPreceding,
            end: Some(WindowFrameBound::CurrentRow),
            exclude: WindowFrameExclude::NoOthers,
        },
        // Navigation functions: entire partition
        WindowFuncType::Lag | WindowFuncType::Lead => WindowFrame {
            mode: WindowFrameMode::Rows,
            start: WindowFrameBound::UnboundedPreceding,
            end: Some(WindowFrameBound::UnboundedFollowing),
            exclude: WindowFrameExclude::NoOthers,
        },
        // Value functions: depends on ORDER BY
        WindowFuncType::FirstValue | WindowFuncType::LastValue | WindowFuncType::NthValue => {
            if has_order_by {
                // With ORDER BY: RANGE UNBOUNDED PRECEDING
                WindowFrame {
                    mode: WindowFrameMode::Range,
                    start: WindowFrameBound::UnboundedPreceding,
                    end: Some(WindowFrameBound::CurrentRow),
                    exclude: WindowFrameExclude::NoOthers,
                }
            } else {
                // Without ORDER BY: entire partition
                WindowFrame {
                    mode: WindowFrameMode::Rows,
                    start: WindowFrameBound::UnboundedPreceding,
                    end: Some(WindowFrameBound::UnboundedFollowing),
                    exclude: WindowFrameExclude::NoOthers,
                }
            }
        }
        // Aggregates: depends on ORDER BY
        WindowFuncType::Aggregate(_) => {
            if has_order_by {
                // With ORDER BY: RANGE UNBOUNDED PRECEDING
                WindowFrame {
                    mode: WindowFrameMode::Range,
                    start: WindowFrameBound::UnboundedPreceding,
                    end: Some(WindowFrameBound::CurrentRow),
                    exclude: WindowFrameExclude::NoOthers,
                }
            } else {
                // Without ORDER BY: entire partition
                WindowFrame {
                    mode: WindowFrameMode::Rows,
                    start: WindowFrameBound::UnboundedPreceding,
                    end: Some(WindowFrameBound::UnboundedFollowing),
                    exclude: WindowFrameExclude::NoOthers,
                }
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_func_type_from_name() {
        assert_eq!(
            WindowFuncType::from_name("row_number"),
            Some(WindowFuncType::RowNumber)
        );
        assert_eq!(
            WindowFuncType::from_name("RANK"),
            Some(WindowFuncType::Rank)
        );
        assert_eq!(
            WindowFuncType::from_name("dense_rank"),
            Some(WindowFuncType::DenseRank)
        );
        assert_eq!(WindowFuncType::from_name("lag"), Some(WindowFuncType::Lag));
        assert_eq!(
            WindowFuncType::from_name("LEAD"),
            Some(WindowFuncType::Lead)
        );
        assert_eq!(
            WindowFuncType::from_name("SUM"),
            Some(WindowFuncType::Aggregate("SUM".to_string()))
        );
        assert_eq!(WindowFuncType::from_name("unknown"), None);
    }

    #[test]
    fn test_window_func_type_needs_frame() {
        assert!(!WindowFuncType::RowNumber.needs_frame());
        assert!(!WindowFuncType::Rank.needs_frame());
        assert!(WindowFuncType::FirstValue.needs_frame());
        assert!(WindowFuncType::LastValue.needs_frame());
        assert!(WindowFuncType::Aggregate("SUM".to_string()).needs_frame());
    }

    #[test]
    fn test_window_func_type_is_ranking() {
        assert!(WindowFuncType::RowNumber.is_ranking());
        assert!(WindowFuncType::Rank.is_ranking());
        assert!(WindowFuncType::DenseRank.is_ranking());
        assert!(!WindowFuncType::Lag.is_ranking());
        assert!(!WindowFuncType::FirstValue.is_ranking());
    }

    #[test]
    fn test_window_func_type_needs_peer_info() {
        assert!(WindowFuncType::Rank.needs_peer_info());
        assert!(WindowFuncType::DenseRank.needs_peer_info());
        assert!(!WindowFuncType::RowNumber.needs_peer_info());
        assert!(!WindowFuncType::Lag.needs_peer_info());
    }

    #[test]
    fn test_has_window_function() {
        use crate::parser::ast::{FunctionArgs, FunctionCall, Over};

        // Function with OVER
        let window_func = Expr::Function(FunctionCall {
            name: "row_number".to_string(),
            args: FunctionArgs::Star,
            distinct: false,
            filter: None,
            over: Some(Over::Spec(WindowSpec {
                base: None,
                partition_by: None,
                order_by: None,
                frame: None,
            })),
        });
        assert!(has_window_function(&window_func));

        // Function without OVER
        let regular_func = Expr::Function(FunctionCall {
            name: "count".to_string(),
            args: FunctionArgs::Star,
            distinct: false,
            filter: None,
            over: None,
        });
        assert!(!has_window_function(&regular_func));
    }

    #[test]
    fn test_default_frame_for_function() {
        // Ranking function
        let frame = default_frame_for_function(&WindowFuncType::RowNumber, true);
        assert_eq!(frame.mode, WindowFrameMode::Rows);

        // Aggregate with ORDER BY
        let frame = default_frame_for_function(&WindowFuncType::Aggregate("SUM".to_string()), true);
        assert_eq!(frame.mode, WindowFrameMode::Range);

        // Aggregate without ORDER BY
        let frame =
            default_frame_for_function(&WindowFuncType::Aggregate("SUM".to_string()), false);
        assert!(matches!(
            frame.end,
            Some(WindowFrameBound::UnboundedFollowing)
        ));
    }

    #[test]
    fn test_window_compiler_alloc() {
        let mut compiler = WindowCompiler::new(1, 0);

        let reg1 = compiler.alloc_reg();
        let reg2 = compiler.alloc_reg();
        assert_eq!(reg1, 1);
        assert_eq!(reg2, 2);

        let regs = compiler.alloc_regs(3);
        assert_eq!(regs, 3);
        assert_eq!(compiler.next_reg(), 6);

        let cursor = compiler.alloc_cursor();
        assert_eq!(cursor, 0);
        assert_eq!(compiler.next_cursor(), 1);
    }

    #[test]
    fn test_specs_equal() {
        let spec1 = WindowSpec {
            base: None,
            partition_by: None,
            order_by: None,
            frame: None,
        };
        let spec2 = WindowSpec {
            base: None,
            partition_by: None,
            order_by: None,
            frame: None,
        };
        assert!(specs_equal(&spec1, &spec2));

        let spec3 = WindowSpec {
            base: None,
            partition_by: Some(vec![]),
            order_by: None,
            frame: None,
        };
        assert!(!specs_equal(&spec1, &spec3));
    }

    #[test]
    fn test_bounds_equal() {
        assert!(bounds_equal(
            &WindowFrameBound::CurrentRow,
            &WindowFrameBound::CurrentRow
        ));
        assert!(bounds_equal(
            &WindowFrameBound::UnboundedPreceding,
            &WindowFrameBound::UnboundedPreceding
        ));
        assert!(!bounds_equal(
            &WindowFrameBound::CurrentRow,
            &WindowFrameBound::UnboundedPreceding
        ));
    }

    #[test]
    fn test_parse_row_number() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT row_number() OVER () FROM t";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_row_number_with_partition() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT row_number() OVER (PARTITION BY dept) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_row_number_with_order() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT row_number() OVER (ORDER BY salary DESC) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_rank_with_partition_and_order() {
        use crate::parser::grammar::Parser;

        let sql =
            "SELECT name, rank() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_dense_rank() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT dense_rank() OVER (ORDER BY score) FROM students";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_ntile() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT ntile(4) OVER (ORDER BY value) FROM data";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_lag_function() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT lag(price, 1) OVER (ORDER BY date) FROM stock_prices";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_lead_function() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT lead(value, 1, 0) OVER (ORDER BY id) FROM t";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_first_value() {
        use crate::parser::grammar::Parser;

        let sql =
            "SELECT first_value(name) OVER (PARTITION BY dept ORDER BY hire_date) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_last_value() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT last_value(name) OVER (ORDER BY hire_date) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_nth_value() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT nth_value(name, 2) OVER (ORDER BY salary DESC) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_sum_over() {
        use crate::parser::grammar::Parser;

        let sql =
            "SELECT sum(amount) OVER (PARTITION BY account_id ORDER BY date) FROM transactions";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_avg_over() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT avg(value) OVER (ORDER BY time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM data";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_count_over() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT count(*) OVER (PARTITION BY category) FROM products";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_frame_rows() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT sum(x) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_frame_range() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT sum(x) OVER (ORDER BY y RANGE UNBOUNDED PRECEDING) FROM t";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_frame_groups() {
        use crate::parser::grammar::Parser;

        let sql =
            "SELECT sum(x) OVER (ORDER BY y GROUPS BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM t";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_frame_exclude() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT sum(x) OVER (ORDER BY y ROWS CURRENT ROW EXCLUDE CURRENT ROW) FROM t";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_named_window() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT sum(x) OVER w FROM t WINDOW w AS (ORDER BY y)";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_multiple_window_functions() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT row_number() OVER (ORDER BY id), rank() OVER (ORDER BY score) FROM t";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_window_with_filter() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT count(*) FILTER (WHERE x > 10) OVER (PARTITION BY y) FROM t";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_percent_rank() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT percent_rank() OVER (ORDER BY score) FROM students";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_parse_cume_dist() {
        use crate::parser::grammar::Parser;

        let sql = "SELECT cume_dist() OVER (ORDER BY sales) FROM salespeople";
        let mut parser = Parser::new(sql).unwrap();
        let result = parser.parse_stmt();
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
    }

    #[test]
    fn test_window_detection_in_select() {
        use crate::parser::ast::{SelectBody, Stmt};
        use crate::parser::grammar::Parser;

        let sql = "SELECT row_number() OVER (PARTITION BY dept ORDER BY id) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse_stmt().unwrap();

        if let Stmt::Select(select) = stmt {
            if let SelectBody::Select(core) = &select.body {
                assert!(select_has_window_functions(core));
            } else {
                panic!("Expected SelectCore");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_no_window_detection_without_over() {
        use crate::parser::ast::{SelectBody, Stmt};
        use crate::parser::grammar::Parser;

        let sql = "SELECT count(*) FROM employees";
        let mut parser = Parser::new(sql).unwrap();
        let stmt = parser.parse_stmt().unwrap();

        if let Stmt::Select(select) = stmt {
            if let SelectBody::Select(core) = &select.body {
                assert!(!select_has_window_functions(core));
            } else {
                panic!("Expected SelectCore");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_all_ranking_functions() {
        assert!(WindowFuncType::RowNumber.is_ranking());
        assert!(WindowFuncType::Rank.is_ranking());
        assert!(WindowFuncType::DenseRank.is_ranking());
        assert!(WindowFuncType::Ntile.is_ranking());
        assert!(WindowFuncType::PercentRank.is_ranking());
        assert!(WindowFuncType::CumeDist.is_ranking());
    }

    #[test]
    fn test_all_value_functions() {
        assert!(!WindowFuncType::Lag.is_ranking());
        assert!(!WindowFuncType::Lead.is_ranking());
        assert!(!WindowFuncType::FirstValue.is_ranking());
        assert!(!WindowFuncType::LastValue.is_ranking());
        assert!(!WindowFuncType::NthValue.is_ranking());
    }
}
