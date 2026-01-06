# Translate vdbeapi.c - VDBE API

## Overview
Translate the public API for VDBE operations including statement execution, parameter binding, and result column access.

## Source Reference
- `sqlite3/src/vdbeapi.c` - 2,602 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Public API Functions

### Statement Execution

```rust
/// Execute one step of the prepared statement
/// sqlite3_step()
pub fn step(stmt: &mut Statement) -> Result<StepResult> {
    let vdbe = stmt.vdbe_mut();

    // Check magic number
    if vdbe.magic != VDBE_MAGIC_RUN && vdbe.magic != VDBE_MAGIC_RESET {
        return Err(Error::new(ErrorCode::Misuse));
    }

    // Prepare if needed
    if vdbe.magic == VDBE_MAGIC_RESET {
        vdbe.magic = VDBE_MAGIC_RUN;
        vdbe.pc = 0;
    }

    // Execute
    match vdbe.exec()? {
        ExecResult::Row => Ok(StepResult::Row),
        ExecResult::Done => {
            vdbe.is_done = true;
            Ok(StepResult::Done)
        }
    }
}

/// Reset statement for re-execution
/// sqlite3_reset()
pub fn reset(stmt: &mut Statement) -> Result<()> {
    let vdbe = stmt.vdbe_mut();
    vdbe.reset()?;
    Ok(())
}

/// Finalize statement and free resources
/// sqlite3_finalize()
pub fn finalize(stmt: Statement) -> Result<()> {
    stmt.vdbe.finalize()?;
    Ok(())
}
```

### Parameter Binding

```rust
impl Statement {
    /// Bind NULL to parameter
    /// sqlite3_bind_null()
    pub fn bind_null(&mut self, idx: i32) -> Result<()> {
        self.check_param_index(idx)?;
        self.vdbe.vars[(idx - 1) as usize] = Mem::null();
        Ok(())
    }

    /// Bind integer value
    /// sqlite3_bind_int64()
    pub fn bind_i64(&mut self, idx: i32, value: i64) -> Result<()> {
        self.check_param_index(idx)?;
        self.vdbe.vars[(idx - 1) as usize] = Mem::int(value);
        Ok(())
    }

    /// Bind double value
    /// sqlite3_bind_double()
    pub fn bind_f64(&mut self, idx: i32, value: f64) -> Result<()> {
        self.check_param_index(idx)?;
        self.vdbe.vars[(idx - 1) as usize] = Mem::real(value);
        Ok(())
    }

    /// Bind text value
    /// sqlite3_bind_text()
    pub fn bind_text(&mut self, idx: i32, value: &str) -> Result<()> {
        self.check_param_index(idx)?;
        self.vdbe.vars[(idx - 1) as usize] = Mem::text(value.to_string());
        Ok(())
    }

    /// Bind blob value
    /// sqlite3_bind_blob()
    pub fn bind_blob(&mut self, idx: i32, value: &[u8]) -> Result<()> {
        self.check_param_index(idx)?;
        self.vdbe.vars[(idx - 1) as usize] = Mem::blob(value.to_vec());
        Ok(())
    }

    /// Bind zeroblob (n bytes of zeros)
    /// sqlite3_bind_zeroblob()
    pub fn bind_zeroblob(&mut self, idx: i32, n: i32) -> Result<()> {
        self.check_param_index(idx)?;
        self.vdbe.vars[(idx - 1) as usize] = Mem::zeroblob(n);
        Ok(())
    }

    /// Bind value from another Mem
    /// sqlite3_bind_value()
    pub fn bind_value(&mut self, idx: i32, value: &Mem) -> Result<()> {
        self.check_param_index(idx)?;
        self.vdbe.vars[(idx - 1) as usize] = value.clone();
        Ok(())
    }

    /// Clear all bindings
    /// sqlite3_clear_bindings()
    pub fn clear_bindings(&mut self) -> Result<()> {
        for var in &mut self.vdbe.vars {
            *var = Mem::null();
        }
        Ok(())
    }

    /// Get parameter count
    /// sqlite3_bind_parameter_count()
    pub fn bind_parameter_count(&self) -> i32 {
        self.vdbe.vars.len() as i32
    }

    /// Get parameter name
    /// sqlite3_bind_parameter_name()
    pub fn bind_parameter_name(&self, idx: i32) -> Option<&str> {
        if idx < 1 || idx > self.vdbe.var_names.len() as i32 {
            return None;
        }
        self.vdbe.var_names[(idx - 1) as usize].as_deref()
    }

    /// Get parameter index by name
    /// sqlite3_bind_parameter_index()
    pub fn bind_parameter_index(&self, name: &str) -> i32 {
        for (i, var_name) in self.vdbe.var_names.iter().enumerate() {
            if let Some(n) = var_name {
                if n == name {
                    return (i + 1) as i32;
                }
            }
        }
        0
    }

    fn check_param_index(&self, idx: i32) -> Result<()> {
        if idx < 1 || idx > self.vdbe.vars.len() as i32 {
            return Err(Error::new(ErrorCode::Range));
        }
        Ok(())
    }
}
```

### Column Access

```rust
impl Statement {
    /// Get number of columns in result
    /// sqlite3_column_count()
    pub fn column_count(&self) -> i32 {
        self.vdbe.n_result_col
    }

    /// Get column name
    /// sqlite3_column_name()
    pub fn column_name(&self, idx: i32) -> Option<&str> {
        if idx < 0 || idx >= self.column_count() {
            return None;
        }
        self.vdbe.col_names.get(idx as usize).map(|s| s.as_str())
    }

    /// Get column type
    /// sqlite3_column_type()
    pub fn column_type(&self, idx: i32) -> ColumnType {
        match self.get_column_mem(idx) {
            Some(mem) => match &mem.value {
                MemValue::Null => ColumnType::Null,
                MemValue::Int(_) => ColumnType::Integer,
                MemValue::Real(_) => ColumnType::Float,
                MemValue::Str { .. } => ColumnType::Text,
                MemValue::Blob(_) | MemValue::ZeroBlob(_) => ColumnType::Blob,
                MemValue::Ptr { .. } => ColumnType::Null,
            },
            None => ColumnType::Null,
        }
    }

    /// Get column as integer
    /// sqlite3_column_int64()
    pub fn column_i64(&self, idx: i32) -> i64 {
        match self.get_column_mem(idx) {
            Some(mem) => mem.to_i64(),
            None => 0,
        }
    }

    /// Get column as double
    /// sqlite3_column_double()
    pub fn column_f64(&self, idx: i32) -> f64 {
        match self.get_column_mem(idx) {
            Some(mem) => mem.to_f64(),
            None => 0.0,
        }
    }

    /// Get column as text
    /// sqlite3_column_text()
    pub fn column_text(&self, idx: i32) -> &str {
        match self.get_column_mem(idx) {
            Some(mem) => mem.to_text(),
            None => "",
        }
    }

    /// Get column as blob
    /// sqlite3_column_blob()
    pub fn column_blob(&self, idx: i32) -> &[u8] {
        match self.get_column_mem(idx) {
            Some(mem) => mem.to_blob(),
            None => &[],
        }
    }

    /// Get column byte count
    /// sqlite3_column_bytes()
    pub fn column_bytes(&self, idx: i32) -> i32 {
        match self.get_column_mem(idx) {
            Some(mem) => mem.n,
            None => 0,
        }
    }

    /// Get column as generic Value
    /// sqlite3_column_value()
    pub fn column_value(&self, idx: i32) -> Value {
        match self.get_column_mem(idx) {
            Some(mem) => mem.to_value(),
            None => Value::Null,
        }
    }

    fn get_column_mem(&self, idx: i32) -> Option<&Mem> {
        if idx < 0 || idx >= self.column_count() {
            return None;
        }
        // Result columns stored starting at mem[1]
        self.vdbe.mem.get((idx + 1) as usize)
    }
}
```

### Statement Information

```rust
impl Statement {
    /// Get SQL text
    /// sqlite3_sql()
    pub fn sql(&self) -> &str {
        &self.sql_text
    }

    /// Get expanded SQL (with bound values)
    /// sqlite3_expanded_sql()
    pub fn expanded_sql(&self) -> String {
        // Replace ? with actual bound values
        let mut result = String::new();
        let mut param_idx = 0;

        for c in self.sql_text.chars() {
            if c == '?' {
                param_idx += 1;
                if let Some(mem) = self.vdbe.vars.get(param_idx - 1) {
                    result.push_str(&mem.to_sql_literal());
                } else {
                    result.push('?');
                }
            } else {
                result.push(c);
            }
        }

        result
    }

    /// Check if statement is read-only
    /// sqlite3_stmt_readonly()
    pub fn is_readonly(&self) -> bool {
        self.vdbe.is_readonly
    }

    /// Check if statement is EXPLAIN
    /// sqlite3_stmt_isexplain()
    pub fn is_explain(&self) -> ExplainMode {
        self.vdbe.explain_mode
    }

    /// Get status counters
    /// sqlite3_stmt_status()
    pub fn status(&self, op: StmtStatusOp, reset: bool) -> i32 {
        let value = match op {
            StmtStatusOp::FullScanStep => self.vdbe.n_scan,
            StmtStatusOp::Sort => self.vdbe.n_sort,
            StmtStatusOp::AutoIndex => self.vdbe.n_auto_index,
            StmtStatusOp::VmStep => self.vdbe.n_vm_step,
            StmtStatusOp::Reprepare => self.vdbe.n_reprepare,
            StmtStatusOp::Run => self.vdbe.n_run,
            StmtStatusOp::MemUsed => self.vdbe.mem_used,
        };

        if reset {
            // Reset counter
        }

        value
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StmtStatusOp {
    FullScanStep,
    Sort,
    AutoIndex,
    VmStep,
    Reprepare,
    Run,
    MemUsed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainMode {
    None = 0,
    Explain = 1,
    ExplainQueryPlan = 2,
}
```

### Data Change Information

```rust
impl Statement {
    /// Get database connection
    /// sqlite3_db_handle()
    pub fn db_handle(&self) -> &Connection {
        &self.db
    }
}

impl Connection {
    /// Get last insert rowid
    /// sqlite3_last_insert_rowid()
    pub fn last_insert_rowid(&self) -> i64 {
        self.last_rowid
    }

    /// Get rows changed by last statement
    /// sqlite3_changes()
    pub fn changes(&self) -> i32 {
        self.n_change as i32
    }

    /// Get total rows changed
    /// sqlite3_total_changes()
    pub fn total_changes(&self) -> i64 {
        self.n_total_change
    }
}
```

## Acceptance Criteria
- [ ] step() executes statement
- [ ] reset() prepares for re-execution
- [ ] finalize() releases resources
- [ ] All bind functions (null, i64, f64, text, blob, zeroblob, value)
- [ ] clear_bindings() clears all parameters
- [ ] Parameter name/index lookup
- [ ] All column accessors (type, i64, f64, text, blob, bytes, value)
- [ ] column_count() and column_name()
- [ ] sql() and expanded_sql()
- [ ] Statement status counters
- [ ] Connection change tracking
