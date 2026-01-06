# Translate analyze.c - ANALYZE Command

## Overview
Translate ANALYZE statement for collecting table and index statistics used by the query optimizer.

## Source Reference
- `sqlite3/src/analyze.c` - 2,012 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Statistics Tables
```rust
/// sqlite_stat1 - basic table/index statistics
pub struct Stat1Row {
    /// Table name
    pub tbl: String,
    /// Index name (or NULL for table)
    pub idx: Option<String>,
    /// Space-separated statistics
    pub stat: String,
}

/// sqlite_stat4 - sample rows for better estimation
pub struct Stat4Row {
    /// Table name
    pub tbl: String,
    /// Index name
    pub idx: String,
    /// Sample count
    pub nlt: Vec<i64>,
    /// Number of distinct values <= sample
    pub ndlt: Vec<i64>,
    /// Number of rows with same prefix
    pub neq: Vec<i64>,
    /// Sample row
    pub sample: Vec<u8>,
}
```

### Index Statistics
```rust
/// Statistics for an index
#[derive(Debug, Clone)]
pub struct IndexStats {
    /// Total row count
    pub row_count: i64,
    /// Average rows per distinct value for each prefix
    pub avg_eq: Vec<f64>,
    /// Number of distinct values for each prefix
    pub n_distinct: Vec<i64>,
    /// Sample rows (for stat4)
    pub samples: Vec<IndexSample>,
}

#[derive(Debug, Clone)]
pub struct IndexSample {
    /// Number of equal entries
    pub n_eq: Vec<i64>,
    /// Number less than
    pub n_lt: Vec<i64>,
    /// Number of distinct less than
    pub n_d_lt: Vec<i64>,
    /// Sample key
    pub key: Vec<u8>,
}
```

## ANALYZE Compilation

### Main Handler
```rust
impl<'a> Parse<'a> {
    pub fn compile_analyze(&mut self, analyze: &AnalyzeStmt) -> Result<()> {
        match (&analyze.schema, &analyze.table) {
            (None, None) => {
                // ANALYZE - analyze all tables in all databases
                for db in &self.conn.dbs {
                    self.analyze_database(&db.name)?;
                }
            }
            (Some(schema), None) => {
                // ANALYZE schema - analyze all tables in schema
                self.analyze_database(schema)?;
            }
            (schema, Some(table)) => {
                // ANALYZE table - analyze specific table
                let schema = schema.as_deref().unwrap_or("main");
                self.analyze_table(schema, table)?;
            }
        }

        Ok(())
    }

    fn analyze_database(&mut self, schema: &str) -> Result<()> {
        let tables: Vec<String> = {
            let db_schema = self.conn.schema.read().unwrap();
            db_schema.tables.keys().cloned().collect()
        };

        for table in tables {
            self.analyze_table(schema, &table)?;
        }

        Ok(())
    }

    fn analyze_table(&mut self, schema: &str, table_name: &str) -> Result<()> {
        let (table, indexes) = {
            let db_schema = self.conn.schema.read().unwrap();
            let table = db_schema.tables.get(table_name)
                .cloned()
                .ok_or_else(|| Error::with_message(
                    ErrorCode::Error,
                    format!("no such table: {}", table_name)
                ))?;
            let indexes = table.indexes.clone();
            (table, indexes)
        };

        // Create sqlite_stat1 if needed
        self.ensure_stat_tables(schema)?;

        // Delete old stats for this table
        self.delete_old_stats(schema, table_name)?;

        // Collect and insert new stats
        self.collect_table_stats(schema, &table)?;

        for index in &indexes {
            self.collect_index_stats(schema, table_name, index)?;
        }

        Ok(())
    }
}
```

### Statistics Collection
```rust
impl<'a> Parse<'a> {
    fn collect_table_stats(&mut self, schema: &str, table: &Table) -> Result<()> {
        // Count total rows
        let cursor = self.alloc_cursor();
        self.add_op(Opcode::OpenRead, cursor, table.root_page as i32, 0);

        let count_reg = self.alloc_mem();
        self.add_op(Opcode::Integer, 0, count_reg, 0);

        let loop_start = self.make_label();
        let loop_end = self.make_label();

        self.add_op(Opcode::Rewind, cursor, loop_end, 0);
        self.resolve_label(loop_start);

        // count++
        self.add_op(Opcode::AddImm, count_reg, 1, 0);

        self.add_op(Opcode::Next, cursor, loop_start, 0);
        self.resolve_label(loop_end);

        // Insert into sqlite_stat1
        self.insert_stat1(schema, &table.name, None, count_reg)?;

        self.add_op(Opcode::Close, cursor, 0, 0);

        Ok(())
    }

    fn collect_index_stats(&mut self, schema: &str, table_name: &str, index: &Index) -> Result<()> {
        let num_cols = index.columns.len();

        // Registers for statistics
        let count_reg = self.alloc_mem();  // Total rows
        let distinct_regs = self.alloc_mem_n(num_cols as i32);  // Distinct counts per prefix
        let prev_regs = self.alloc_mem_n(num_cols as i32);  // Previous row values

        // Initialize
        self.add_op(Opcode::Integer, 0, count_reg, 0);
        for i in 0..num_cols {
            self.add_op(Opcode::Integer, 0, distinct_regs + i as i32, 0);
            self.add_op(Opcode::Null, 0, prev_regs + i as i32, 0);
        }

        // Open index cursor
        let cursor = self.alloc_cursor();
        self.add_op(Opcode::OpenRead, cursor, index.root_page as i32, num_cols as i32);

        let loop_start = self.make_label();
        let loop_end = self.make_label();

        self.add_op(Opcode::Rewind, cursor, loop_end, 0);
        self.resolve_label(loop_start);

        // count++
        self.add_op(Opcode::AddImm, count_reg, 1, 0);

        // For each column prefix, check if distinct
        for i in 0..num_cols {
            let col_reg = self.alloc_mem();
            self.add_op(Opcode::Column, cursor, i as i32, col_reg);

            // Compare with previous
            let same_label = self.make_label();
            self.add_op(Opcode::Eq, col_reg, same_label, prev_regs + i as i32);

            // Different - increment distinct count
            self.add_op(Opcode::AddImm, distinct_regs + i as i32, 1, 0);

            self.resolve_label(same_label);

            // Update previous
            self.add_op(Opcode::Copy, col_reg, prev_regs + i as i32, 0);
        }

        self.add_op(Opcode::Next, cursor, loop_start, 0);
        self.resolve_label(loop_end);

        // Build stat string: "total avg1 avg2 ..."
        let stat_reg = self.build_stat_string(count_reg, distinct_regs, num_cols)?;

        // Insert into sqlite_stat1
        self.insert_stat1(schema, table_name, Some(&index.name), stat_reg)?;

        self.add_op(Opcode::Close, cursor, 0, 0);

        Ok(())
    }

    fn build_stat_string(&mut self, count_reg: i32, distinct_regs: i32, num_cols: usize) -> Result<i32> {
        let result_reg = self.alloc_mem();

        // Start with count
        self.add_op(Opcode::Cast, count_reg, 0, 0);  // To text
        self.add_op(Opcode::Copy, count_reg, result_reg, 0);

        // Add avg for each column
        for i in 0..num_cols {
            let avg_reg = self.alloc_mem();

            // avg = count / distinct (or 1 if distinct is 0)
            let distinct_reg = distinct_regs + i as i32;
            let zero_label = self.make_label();
            let done_label = self.make_label();

            self.add_op(Opcode::IfZero, distinct_reg, zero_label, 0);
            self.add_op(Opcode::Divide, distinct_reg, count_reg, avg_reg);
            self.add_op(Opcode::Goto, 0, done_label, 0);

            self.resolve_label(zero_label);
            self.add_op(Opcode::Integer, 1, avg_reg, 0);

            self.resolve_label(done_label);

            // Concatenate
            self.add_op(Opcode::Concat, result_reg, avg_reg, result_reg);
        }

        Ok(result_reg)
    }
}
```

### Loading Statistics
```rust
impl Schema {
    /// Load statistics from sqlite_stat1
    pub fn load_statistics(&mut self, db: &Connection) -> Result<()> {
        // Query sqlite_stat1
        let sql = "SELECT tbl, idx, stat FROM sqlite_stat1";

        let mut stmt = db.prepare(sql)?;
        while stmt.step()? == StepResult::Row {
            let table_name = stmt.column_text(0)?;
            let index_name = stmt.column_text(1);
            let stat_str = stmt.column_text(2)?;

            // Parse stat string
            let stats = self.parse_stat_string(stat_str)?;

            // Apply to table/index
            if let Some(index_name) = index_name {
                if let Some(table) = self.tables.get_mut(&table_name) {
                    let table = Arc::make_mut(table);
                    if let Some(index) = table.indexes.iter_mut()
                        .find(|i| i.name == index_name)
                    {
                        index.stats = Some(stats);
                    }
                }
            } else {
                // Table-level stats
                if let Some(table) = self.tables.get_mut(&table_name) {
                    let table = Arc::make_mut(table);
                    table.row_estimate = stats.row_count;
                }
            }
        }

        // Also load sqlite_stat4 if exists
        if self.tables.contains_key("sqlite_stat4") {
            self.load_stat4(db)?;
        }

        Ok(())
    }

    fn parse_stat_string(&self, stat: &str) -> Result<IndexStats> {
        let parts: Vec<&str> = stat.split_whitespace().collect();

        if parts.is_empty() {
            return Err(Error::with_message(ErrorCode::Corrupt, "empty stat string"));
        }

        let row_count: i64 = parts[0].parse()
            .map_err(|_| Error::with_message(ErrorCode::Corrupt, "invalid row count"))?;

        let mut avg_eq = Vec::new();
        let mut n_distinct = Vec::new();

        for i in 1..parts.len() {
            let avg: f64 = parts[i].parse()
                .map_err(|_| Error::with_message(ErrorCode::Corrupt, "invalid avg"))?;
            avg_eq.push(avg);

            // distinct = row_count / avg
            let distinct = if avg > 0.0 {
                (row_count as f64 / avg) as i64
            } else {
                row_count
            };
            n_distinct.push(distinct);
        }

        Ok(IndexStats {
            row_count,
            avg_eq,
            n_distinct,
            samples: Vec::new(),
        })
    }
}
```

### Using Statistics in Query Planning
```rust
impl<'a> WhereLoop<'a> {
    /// Estimate rows returned using statistics
    pub fn estimate_rows(&self, index: &Index) -> f64 {
        let stats = match &index.stats {
            Some(s) => s,
            None => return index.default_row_estimate(),
        };

        let mut row_est = stats.row_count as f64;

        // For each constraint on indexed columns
        for (i, term) in self.equality_terms().enumerate() {
            if i < stats.avg_eq.len() {
                // Rows matching equality = avg rows per distinct value
                row_est = row_est.min(stats.avg_eq[i]);
            }
        }

        // Apply range constraints
        if let Some(range_est) = self.range_estimate(stats) {
            row_est *= range_est;
        }

        row_est.max(1.0)
    }
}
```

## Acceptance Criteria
- [ ] ANALYZE without arguments (all tables)
- [ ] ANALYZE schema
- [ ] ANALYZE table
- [ ] Create sqlite_stat1 table
- [ ] Create sqlite_stat4 table (if enabled)
- [ ] Collect row counts
- [ ] Collect distinct value counts per prefix
- [ ] Build stat string format
- [ ] Load statistics on schema read
- [ ] Use statistics in query planner
- [ ] Delete old statistics before reanalyze
- [ ] Handle empty tables
- [ ] Handle indexes with NULL values
