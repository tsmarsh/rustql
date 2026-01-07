# Translate sqlite3rbu.c - Resumable Bulk Update

## Overview
Translate RBU (Resumable Bulk Update) extension for efficient bulk data updates.

## Source Reference
- `sqlite3/ext/rbu/sqlite3rbu.c` - RBU implementation (5,447 lines)

## Design Fidelity
- SQLite's "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite's observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### RBU Handle
```rust
/// RBU update handle
pub struct Rbu {
    /// Target database connection
    target_db: Connection,
    /// RBU database connection
    rbu_db: Connection,
    /// Current state
    state: RbuState,
    /// Target database path
    target_path: String,
    /// RBU database path
    rbu_path: String,
    /// Error message if any
    error: Option<String>,
    /// Statistics
    stats: RbuStats,
}

/// RBU operation state
#[derive(Debug, Clone)]
pub struct RbuState {
    /// Current stage
    stage: RbuStage,
    /// Current table being processed
    current_table: Option<String>,
    /// Current index
    current_index: Option<String>,
    /// Rows processed in current table
    rows_done: i64,
    /// Total rows in current table
    rows_total: i64,
    /// Checksum for resumability verification
    checksum: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RbuStage {
    /// Processing OAL (Off-line AL) - data changes
    Oal,
    /// Moving WAL to target
    Move,
    /// Checkpoint
    Checkpoint,
    /// Complete
    Done,
}

/// RBU statistics
#[derive(Debug, Default)]
pub struct RbuStats {
    /// Pages read
    pages_read: i64,
    /// Pages written
    pages_written: i64,
    /// Steps executed
    steps: i64,
}
```

### RBU Object Iterator
```rust
/// Iterator over RBU objects (tables to update)
struct RbuObjIter {
    /// Tables to process
    tables: Vec<RbuTable>,
    /// Current table index
    current: usize,
    /// Prepared statements
    stmts: RbuStmts,
}

struct RbuTable {
    /// Table name
    name: String,
    /// Is this an FTS table
    is_fts: bool,
    /// Is this a virtual table
    is_virtual: bool,
    /// Column count
    n_col: i32,
    /// Primary key columns
    pk_cols: Vec<bool>,
    /// Has INTEGER PRIMARY KEY
    has_ipk: bool,
}

struct RbuStmts {
    /// Select from data_xxx table
    select_data: Option<PreparedStmt>,
    /// Insert into target
    insert: Option<PreparedStmt>,
    /// Delete from target
    delete: Option<PreparedStmt>,
    /// Update target
    update: Option<PreparedStmt>,
    /// Select from target (for conflict check)
    select_target: Option<PreparedStmt>,
}
```

### RBU Data Table Schema
```rust
/// RBU data table follows pattern: data_<tablename>
/// Columns: rbu_control, <original columns>
///
/// rbu_control values:
///   0 = INSERT
///   1 = DELETE
///   2 = UPDATE
///   '.' in position = don't modify that column
///   'x' in position = set to value
///   'd' in position = set to column value + rbu_delta value

/// RBU control value interpretation
#[derive(Debug, Clone)]
pub enum RbuControl {
    Insert,
    Delete,
    Update(Vec<RbuColControl>),
}

#[derive(Debug, Clone, Copy)]
pub enum RbuColControl {
    /// Don't modify (.)
    NoChange,
    /// Set to value (x)
    SetValue,
    /// Add delta (d) - for integer columns
    AddDelta,
}

impl RbuControl {
    pub fn parse(control: &str) -> Result<Self> {
        if control == "0" {
            return Ok(RbuControl::Insert);
        }
        if control == "1" {
            return Ok(RbuControl::Delete);
        }

        // Parse update control string
        let mut cols = Vec::new();
        for c in control.chars() {
            match c {
                '.' => cols.push(RbuColControl::NoChange),
                'x' => cols.push(RbuColControl::SetValue),
                'd' => cols.push(RbuColControl::AddDelta),
                _ => return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("invalid rbu_control character: {}", c),
                )),
            }
        }

        Ok(RbuControl::Update(cols))
    }
}
```

## RBU Operations

### Open and Initialize
```rust
impl Rbu {
    /// Open RBU for update
    pub fn open(target: &str, rbu: &str, state_db: Option<&str>) -> Result<Self> {
        let target_db = Connection::open(target)?;
        let rbu_db = Connection::open(rbu)?;

        // Load or create state
        let state = if let Some(state_path) = state_db {
            RbuState::load(state_path)?
        } else {
            RbuState::new()
        };

        let mut rbu = Self {
            target_db,
            rbu_db,
            state,
            target_path: target.to_string(),
            rbu_path: rbu.to_string(),
            error: None,
            stats: RbuStats::default(),
        };

        // Verify databases are compatible
        rbu.verify_schema()?;

        Ok(rbu)
    }

    fn verify_schema(&self) -> Result<()> {
        // Check that RBU database has valid data_xxx tables
        // Verify columns match target schema
        let tables = self.list_rbu_tables()?;

        for table in tables {
            let rbu_cols = self.get_rbu_columns(&table)?;
            let target_cols = self.get_target_columns(&table)?;

            // First column must be rbu_control
            if rbu_cols.first().map(|s| s.as_str()) != Some("rbu_control") {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("data_{} missing rbu_control column", table),
                ));
            }

            // Remaining columns must match
            if rbu_cols[1..] != target_cols[..] {
                return Err(Error::with_message(
                    ErrorCode::Error,
                    format!("data_{} columns don't match target", table),
                ));
            }
        }

        Ok(())
    }

    fn list_rbu_tables(&self) -> Result<Vec<String>> {
        let mut tables = Vec::new();
        let mut stmt = self.rbu_db.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'data_%'"
        )?;

        while stmt.step()? == StepResult::Row {
            let name = stmt.column_text(0)?;
            tables.push(name[5..].to_string()); // Remove "data_" prefix
        }

        Ok(tables)
    }
}
```

### Step Execution
```rust
impl Rbu {
    /// Execute one step of RBU processing
    pub fn step(&mut self) -> Result<RbuStepResult> {
        if self.error.is_some() {
            return Ok(RbuStepResult::Error);
        }

        match self.state.stage {
            RbuStage::Oal => self.step_oal(),
            RbuStage::Move => self.step_move(),
            RbuStage::Checkpoint => self.step_checkpoint(),
            RbuStage::Done => Ok(RbuStepResult::Done),
        }
    }

    fn step_oal(&mut self) -> Result<RbuStepResult> {
        // Process one row from current table
        if let Some(table) = &self.state.current_table {
            if let Some(row) = self.read_next_row(table)? {
                self.apply_row(table, &row)?;
                self.state.rows_done += 1;
                self.stats.steps += 1;
                return Ok(RbuStepResult::Ok);
            } else {
                // Table complete, move to next
                self.state.current_table = self.next_table()?;
            }
        }

        if self.state.current_table.is_none() {
            // All tables complete
            self.state.stage = RbuStage::Move;
        }

        Ok(RbuStepResult::Ok)
    }

    fn apply_row(&mut self, table: &str, row: &RbuRow) -> Result<()> {
        match &row.control {
            RbuControl::Insert => {
                let sql = self.build_insert_sql(table, row)?;
                self.target_db.execute(&sql)?;
            }
            RbuControl::Delete => {
                let sql = self.build_delete_sql(table, row)?;
                self.target_db.execute(&sql)?;
            }
            RbuControl::Update(cols) => {
                let sql = self.build_update_sql(table, row, cols)?;
                self.target_db.execute(&sql)?;
            }
        }

        Ok(())
    }

    fn step_move(&mut self) -> Result<RbuStepResult> {
        // Move WAL from RBU to target
        // This makes the changes visible
        self.move_wal()?;
        self.state.stage = RbuStage::Checkpoint;
        Ok(RbuStepResult::Ok)
    }

    fn step_checkpoint(&mut self) -> Result<RbuStepResult> {
        // Checkpoint target database
        self.target_db.checkpoint(CheckpointMode::Truncate)?;
        self.state.stage = RbuStage::Done;
        Ok(RbuStepResult::Done)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RbuStepResult {
    Ok,
    Done,
    Error,
}

struct RbuRow {
    control: RbuControl,
    values: Vec<Value>,
}
```

### State Management
```rust
impl RbuState {
    pub fn new() -> Self {
        Self {
            stage: RbuStage::Oal,
            current_table: None,
            current_index: None,
            rows_done: 0,
            rows_total: 0,
            checksum: 0,
        }
    }

    pub fn load(path: &str) -> Result<Self> {
        let db = Connection::open(path)?;
        let mut stmt = db.prepare(
            "SELECT stage, current_table, current_index, rows_done, checksum FROM rbu_state"
        )?;

        if stmt.step()? == StepResult::Row {
            Ok(Self {
                stage: match stmt.column_int(0)? {
                    0 => RbuStage::Oal,
                    1 => RbuStage::Move,
                    2 => RbuStage::Checkpoint,
                    _ => RbuStage::Done,
                },
                current_table: stmt.column_text(1).ok(),
                current_index: stmt.column_text(2).ok(),
                rows_done: stmt.column_int64(3)?,
                rows_total: 0,
                checksum: stmt.column_int64(4)? as u64,
            })
        } else {
            Ok(Self::new())
        }
    }

    pub fn save(&self, path: &str) -> Result<()> {
        let db = Connection::open(path)?;

        db.execute(
            "CREATE TABLE IF NOT EXISTS rbu_state(
                stage INTEGER,
                current_table TEXT,
                current_index TEXT,
                rows_done INTEGER,
                checksum INTEGER
            )"
        )?;

        db.execute("DELETE FROM rbu_state")?;

        let mut stmt = db.prepare(
            "INSERT INTO rbu_state VALUES (?, ?, ?, ?, ?)"
        )?;

        stmt.bind_int(1, self.stage as i32)?;
        if let Some(t) = &self.current_table {
            stmt.bind_text(2, t)?;
        } else {
            stmt.bind_null(2)?;
        }
        if let Some(i) = &self.current_index {
            stmt.bind_text(3, i)?;
        } else {
            stmt.bind_null(3)?;
        }
        stmt.bind_int64(4, self.rows_done)?;
        stmt.bind_int64(5, self.checksum as i64)?;
        stmt.step()?;

        Ok(())
    }
}
```

### Progress and Close
```rust
impl Rbu {
    /// Get current progress
    pub fn progress(&self) -> (i64, i64) {
        (self.state.rows_done, self.state.rows_total)
    }

    /// Get current stage
    pub fn stage(&self) -> RbuStage {
        self.state.stage
    }

    /// Save state and close
    pub fn close(mut self, state_db: Option<&str>) -> Result<()> {
        if let Some(path) = state_db {
            self.state.save(path)?;
        }

        // Close connections
        drop(self.target_db);
        drop(self.rbu_db);

        Ok(())
    }

    /// Get error message if any
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Get statistics
    pub fn stats(&self) -> &RbuStats {
        &self.stats
    }
}
```

### Vacuum Mode
```rust
impl Rbu {
    /// Open RBU in vacuum mode (rebuild database)
    pub fn vacuum(target: &str, state_db: Option<&str>) -> Result<Self> {
        // Create temporary RBU database
        let rbu_path = format!("{}-vacuum", target);

        // Copy schema to RBU database
        let rbu_db = Connection::open(&rbu_path)?;
        rbu_db.execute(&format!("ATTACH '{}' AS target", target))?;

        // Copy table definitions
        let target_db = Connection::open(target)?;
        let mut stmt = target_db.prepare(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )?;

        while stmt.step()? == StepResult::Row {
            let sql = stmt.column_text(0)?;
            // Convert to data_xxx table
            let data_sql = sql.replace("CREATE TABLE ", "CREATE TABLE data_");
            rbu_db.execute(&data_sql)?;
        }

        Self::open(target, &rbu_path, state_db)
    }
}
```

## Acceptance Criteria
- [ ] RBU database schema validation
- [ ] INSERT operation processing
- [ ] DELETE operation processing
- [ ] UPDATE operation processing
- [ ] Delta updates (d control character)
- [ ] State persistence for resumability
- [ ] OAL stage processing
- [ ] WAL move stage
- [ ] Checkpoint stage
- [ ] Progress reporting
- [ ] Statistics tracking
- [ ] Error handling and recovery
- [ ] Vacuum mode support
- [ ] FTS table support

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `rbu.test` - Core RBU functionality
- `rbu2.test` - Additional RBU tests
- `rbu3.test` - RBU edge cases
- `rbufault.test` - RBU error handling
- `rbuvacuum.test` - RBU vacuum mode
