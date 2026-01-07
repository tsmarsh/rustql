# Translate sqldiff.c - SQL Diff Tool

## Overview
Translate sqldiff command-line tool for comparing SQLite databases.

## Source Reference
- `sqlite3/tool/sqldiff.c` - SQL diff implementation (2,050 lines)

## Design Fidelity
- SQLite's "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite's observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Diff Configuration
```rust
/// SQL diff configuration
#[derive(Debug, Clone)]
pub struct SqlDiffConfig {
    /// Source database path
    pub source: String,
    /// Target database path
    pub target: String,
    /// Output mode
    pub output_mode: OutputMode,
    /// Tables to diff (empty = all)
    pub tables: Vec<String>,
    /// Ignore tables
    pub exclude_tables: Vec<String>,
    /// Schema changes only
    pub schema_only: bool,
    /// Primary key mode for tables without ROWID
    pub pk_mode: PkMode,
    /// Transaction wrapper
    pub transaction: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum OutputMode {
    /// Output SQL statements
    Sql,
    /// Output as summary
    Summary,
    /// Output JSON
    Json,
}

#[derive(Debug, Clone, Copy)]
pub enum PkMode {
    /// Require explicit primary key
    Explicit,
    /// Use all columns as key
    AllColumns,
    /// Use rowid if available
    Rowid,
}

impl Default for SqlDiffConfig {
    fn default() -> Self {
        Self {
            source: String::new(),
            target: String::new(),
            output_mode: OutputMode::Sql,
            tables: Vec::new(),
            exclude_tables: Vec::new(),
            schema_only: false,
            pk_mode: PkMode::Rowid,
            transaction: true,
        }
    }
}
```

### Diff Result
```rust
/// Diff result for a table
#[derive(Debug)]
pub struct TableDiff {
    /// Table name
    pub name: String,
    /// Schema change type
    pub schema_change: Option<SchemaChange>,
    /// Rows only in source (deleted)
    pub deleted_rows: Vec<Row>,
    /// Rows only in target (inserted)
    pub inserted_rows: Vec<Row>,
    /// Rows that changed
    pub changed_rows: Vec<RowChange>,
}

#[derive(Debug)]
pub enum SchemaChange {
    /// Table added in target
    Created(String),
    /// Table removed in target
    Dropped(String),
    /// Table modified
    Altered {
        old_schema: String,
        new_schema: String,
    },
}

#[derive(Debug)]
pub struct Row {
    /// Row values
    pub values: Vec<Value>,
}

#[derive(Debug)]
pub struct RowChange {
    /// Primary key values
    pub pk: Vec<Value>,
    /// Old values
    pub old: Vec<Value>,
    /// New values
    pub new: Vec<Value>,
    /// Changed column indices
    pub changed_cols: Vec<usize>,
}
```

## Diff Operations

### Schema Comparison
```rust
/// Compare database schemas
pub fn diff_schema(source: &Connection, target: &Connection) -> Result<Vec<SchemaChange>> {
    let mut changes = Vec::new();

    // Get tables from both databases
    let source_tables = get_tables(source)?;
    let target_tables = get_tables(target)?;

    // Find added tables
    for name in &target_tables {
        if !source_tables.contains(name) {
            let sql = get_create_sql(target, name)?;
            changes.push(SchemaChange::Created(sql));
        }
    }

    // Find dropped tables
    for name in &source_tables {
        if !target_tables.contains(name) {
            changes.push(SchemaChange::Dropped(name.clone()));
        }
    }

    // Find altered tables
    for name in &source_tables {
        if target_tables.contains(name) {
            let source_sql = get_create_sql(source, name)?;
            let target_sql = get_create_sql(target, name)?;

            if source_sql != target_sql {
                changes.push(SchemaChange::Altered {
                    old_schema: source_sql,
                    new_schema: target_sql,
                });
            }
        }
    }

    Ok(changes)
}

fn get_tables(db: &Connection) -> Result<Vec<String>> {
    let mut tables = Vec::new();
    let mut stmt = db.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )?;

    while stmt.step()? == StepResult::Row {
        tables.push(stmt.column_text(0)?);
    }

    Ok(tables)
}

fn get_create_sql(db: &Connection, table: &str) -> Result<String> {
    let mut stmt = db.prepare(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?"
    )?;
    stmt.bind_text(1, table)?;

    if stmt.step()? == StepResult::Row {
        Ok(stmt.column_text(0)?)
    } else {
        Err(Error::with_message(ErrorCode::Error, "table not found"))
    }
}
```

### Data Comparison
```rust
/// Compare table data
pub fn diff_table(
    source: &Connection,
    target: &Connection,
    table: &str,
    config: &SqlDiffConfig,
) -> Result<TableDiff> {
    let mut diff = TableDiff {
        name: table.to_string(),
        schema_change: None,
        deleted_rows: Vec::new(),
        inserted_rows: Vec::new(),
        changed_rows: Vec::new(),
    };

    // Get primary key columns
    let pk_cols = get_pk_columns(source, table, config.pk_mode)?;
    if pk_cols.is_empty() {
        return Err(Error::with_message(
            ErrorCode::Error,
            format!("no primary key for table {}", table),
        ));
    }

    // Get all columns
    let columns = get_columns(source, table)?;

    // Build comparison query
    let pk_expr = pk_cols.iter()
        .map(|c| format!("source.\"{}\" = target.\"{}\"", c, c))
        .collect::<Vec<_>>()
        .join(" AND ");

    let order_by = pk_cols.iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    // Find rows only in source (deleted)
    let sql = format!(
        "SELECT {} FROM main.\"{}\" AS source
         WHERE NOT EXISTS (SELECT 1 FROM aux.\"{}\" AS target WHERE {})
         ORDER BY {}",
        columns.iter().map(|c| format!("source.\"{}\"", c)).collect::<Vec<_>>().join(", "),
        table, table, pk_expr, order_by
    );

    // Attach target database
    source.execute(&format!("ATTACH '{}' AS aux", config.target))?;

    let mut stmt = source.prepare(&sql)?;
    while stmt.step()? == StepResult::Row {
        let values = (0..columns.len())
            .map(|i| stmt.column_value(i as i32))
            .collect::<Result<Vec<_>>>()?;
        diff.deleted_rows.push(Row { values });
    }

    // Find rows only in target (inserted)
    let sql = format!(
        "SELECT {} FROM aux.\"{}\" AS target
         WHERE NOT EXISTS (SELECT 1 FROM main.\"{}\" AS source WHERE {})
         ORDER BY {}",
        columns.iter().map(|c| format!("target.\"{}\"", c)).collect::<Vec<_>>().join(", "),
        table, table, pk_expr, order_by
    );

    let mut stmt = source.prepare(&sql)?;
    while stmt.step()? == StepResult::Row {
        let values = (0..columns.len())
            .map(|i| stmt.column_value(i as i32))
            .collect::<Result<Vec<_>>>()?;
        diff.inserted_rows.push(Row { values });
    }

    // Find changed rows
    let col_compare = columns.iter()
        .filter(|c| !pk_cols.contains(c))
        .map(|c| format!("source.\"{}\" IS NOT target.\"{}\"", c, c))
        .collect::<Vec<_>>()
        .join(" OR ");

    if !col_compare.is_empty() {
        let sql = format!(
            "SELECT {} FROM main.\"{}\" AS source
             JOIN aux.\"{}\" AS target ON {}
             WHERE {}
             ORDER BY {}",
            columns.iter()
                .flat_map(|c| vec![
                    format!("source.\"{}\"", c),
                    format!("target.\"{}\"", c)
                ])
                .collect::<Vec<_>>()
                .join(", "),
            table, table, pk_expr, col_compare, order_by
        );

        let mut stmt = source.prepare(&sql)?;
        while stmt.step()? == StepResult::Row {
            let mut old = Vec::new();
            let mut new = Vec::new();
            let mut pk = Vec::new();
            let mut changed_cols = Vec::new();

            for (i, col) in columns.iter().enumerate() {
                let source_val = stmt.column_value((i * 2) as i32)?;
                let target_val = stmt.column_value((i * 2 + 1) as i32)?;

                old.push(source_val.clone());
                new.push(target_val.clone());

                if pk_cols.contains(col) {
                    pk.push(source_val);
                } else if !values_equal(&source_val, &target_val) {
                    changed_cols.push(i);
                }
            }

            diff.changed_rows.push(RowChange {
                pk,
                old,
                new,
                changed_cols,
            });
        }
    }

    source.execute("DETACH aux")?;

    Ok(diff)
}

fn get_pk_columns(db: &Connection, table: &str, mode: PkMode) -> Result<Vec<String>> {
    let mut pk_cols = Vec::new();

    let mut stmt = db.prepare(&format!("PRAGMA table_info(\"{}\")", table))?;
    while stmt.step()? == StepResult::Row {
        let is_pk = stmt.column_int(5)? > 0;
        if is_pk {
            pk_cols.push(stmt.column_text(1)?);
        }
    }

    if pk_cols.is_empty() && mode == PkMode::AllColumns {
        // Use all columns as key
        pk_cols = get_columns(db, table)?;
    }

    Ok(pk_cols)
}

fn get_columns(db: &Connection, table: &str) -> Result<Vec<String>> {
    let mut columns = Vec::new();
    let mut stmt = db.prepare(&format!("PRAGMA table_info(\"{}\")", table))?;

    while stmt.step()? == StepResult::Row {
        columns.push(stmt.column_text(1)?);
    }

    Ok(columns)
}
```

### Output Generation
```rust
impl TableDiff {
    /// Generate SQL statements for diff
    pub fn to_sql(&self) -> String {
        let mut sql = String::new();

        // Schema changes
        if let Some(change) = &self.schema_change {
            match change {
                SchemaChange::Created(create_sql) => {
                    sql.push_str(create_sql);
                    sql.push_str(";\n");
                }
                SchemaChange::Dropped(name) => {
                    sql.push_str(&format!("DROP TABLE \"{}\";\n", name));
                }
                SchemaChange::Altered { old_schema: _, new_schema } => {
                    // Need to recreate table
                    sql.push_str(&format!("-- Table {} schema changed\n", self.name));
                    sql.push_str(&format!("-- Old and new schemas differ\n"));
                    sql.push_str(new_schema);
                    sql.push_str(";\n");
                }
            }
        }

        // Deleted rows
        for row in &self.deleted_rows {
            sql.push_str(&self.generate_delete(&row.values));
            sql.push('\n');
        }

        // Inserted rows
        for row in &self.inserted_rows {
            sql.push_str(&self.generate_insert(&row.values));
            sql.push('\n');
        }

        // Changed rows
        for change in &self.changed_rows {
            sql.push_str(&self.generate_update(change));
            sql.push('\n');
        }

        sql
    }

    fn generate_delete(&self, pk_values: &[Value]) -> String {
        let where_clause = pk_values.iter()
            .enumerate()
            .map(|(i, v)| format!("c{} = {}", i, quote_value(v)))
            .collect::<Vec<_>>()
            .join(" AND ");

        format!("DELETE FROM \"{}\" WHERE {};", self.name, where_clause)
    }

    fn generate_insert(&self, values: &[Value]) -> String {
        let vals = values.iter()
            .map(|v| quote_value(v))
            .collect::<Vec<_>>()
            .join(", ");

        format!("INSERT INTO \"{}\" VALUES ({});", self.name, vals)
    }

    fn generate_update(&self, change: &RowChange) -> String {
        let set_clause = change.changed_cols.iter()
            .map(|&i| format!("c{} = {}", i, quote_value(&change.new[i])))
            .collect::<Vec<_>>()
            .join(", ");

        let where_clause = change.pk.iter()
            .enumerate()
            .map(|(i, v)| format!("pk{} = {}", i, quote_value(v)))
            .collect::<Vec<_>>()
            .join(" AND ");

        format!("UPDATE \"{}\" SET {} WHERE {};", self.name, set_clause, where_clause)
    }
}

fn quote_value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Blob(b) => format!("X'{}'", hex::encode(b)),
    }
}
```

### CLI Entry Point
```rust
/// Run sqldiff CLI
pub fn sqldiff_main(args: &[String]) -> Result<i32> {
    let config = parse_args(args)?;

    let source = Connection::open(&config.source)?;
    let target = Connection::open(&config.target)?;

    let mut output = String::new();

    if config.transaction {
        output.push_str("BEGIN TRANSACTION;\n");
    }

    // Get tables to compare
    let tables = if config.tables.is_empty() {
        get_tables(&source)?
    } else {
        config.tables.clone()
    };

    // Schema diff
    let schema_changes = diff_schema(&source, &target)?;
    for change in &schema_changes {
        match change {
            SchemaChange::Created(sql) => output.push_str(&format!("{};\n", sql)),
            SchemaChange::Dropped(name) => output.push_str(&format!("DROP TABLE \"{}\";\n", name)),
            _ => {}
        }
    }

    // Data diff
    if !config.schema_only {
        for table in &tables {
            if config.exclude_tables.contains(table) {
                continue;
            }

            let diff = diff_table(&source, &target, table, &config)?;
            output.push_str(&diff.to_sql());
        }
    }

    if config.transaction {
        output.push_str("COMMIT;\n");
    }

    print!("{}", output);
    Ok(0)
}
```

## Acceptance Criteria
- [ ] Schema comparison (tables, indexes)
- [ ] Data comparison with primary keys
- [ ] Deleted row detection
- [ ] Inserted row detection
- [ ] Changed row detection
- [ ] SQL output generation
- [ ] Summary output mode
- [ ] JSON output mode
- [ ] Table filtering
- [ ] Transaction wrapper
- [ ] WITHOUT ROWID table support
- [ ] CLI argument parsing

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `sqldiff.test` - SQL diff tool tests
