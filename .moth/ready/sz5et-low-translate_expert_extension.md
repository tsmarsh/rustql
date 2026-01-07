# Translate Expert Extension

## Overview
Translate SQLite expert extension - query optimization advisor that suggests indexes.

## Source Reference
- `sqlite3/ext/expert/sqlite3expert.c` - Expert implementation
- `sqlite3/ext/expert/sqlite3expert.h` - Expert API header

## Design Fidelity
- SQLite's "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite's observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Expert Handle
```rust
/// SQLite Expert - index recommendation engine
pub struct SqliteExpert {
    /// Database connection
    db: Connection,
    /// Candidate indexes
    candidates: Vec<CandidateIndex>,
    /// Analyzed queries
    queries: Vec<AnalyzedQuery>,
    /// Recommendations
    recommendations: Vec<IndexRecommendation>,
}

/// Candidate index for evaluation
#[derive(Debug, Clone)]
pub struct CandidateIndex {
    /// Table name
    table: String,
    /// Columns in index
    columns: Vec<IndexColumn>,
    /// Is unique
    is_unique: bool,
    /// Estimated usefulness score
    score: f64,
}

#[derive(Debug, Clone)]
pub struct IndexColumn {
    /// Column name
    name: String,
    /// Is descending
    desc: bool,
    /// Is part of WHERE clause
    is_where: bool,
    /// Is part of ORDER BY
    is_order: bool,
}

/// Analyzed SQL query
#[derive(Debug)]
pub struct AnalyzedQuery {
    /// Original SQL
    sql: String,
    /// Tables referenced
    tables: Vec<String>,
    /// WHERE clause columns
    where_columns: Vec<ColumnRef>,
    /// ORDER BY columns
    order_columns: Vec<ColumnRef>,
    /// GROUP BY columns
    group_columns: Vec<ColumnRef>,
    /// Estimated cost without new indexes
    base_cost: f64,
}

#[derive(Debug, Clone)]
pub struct ColumnRef {
    pub table: String,
    pub column: String,
    pub op: Option<CompareOp>,
}

#[derive(Debug, Clone, Copy)]
pub enum CompareOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    In,
    Between,
}

/// Final index recommendation
#[derive(Debug)]
pub struct IndexRecommendation {
    /// CREATE INDEX statement
    pub sql: String,
    /// Tables affected
    pub tables: Vec<String>,
    /// Estimated improvement factor
    pub improvement: f64,
    /// Queries that benefit
    pub benefits_queries: Vec<usize>,
}
```

### Query Analyzer
```rust
/// Analyze query to extract column usage
pub struct QueryAnalyzer {
    /// Parser for SQL
    parser: SqlParser,
}

impl QueryAnalyzer {
    pub fn new() -> Self {
        Self {
            parser: SqlParser::new(),
        }
    }

    pub fn analyze(&self, sql: &str) -> Result<AnalyzedQuery> {
        let ast = self.parser.parse(sql)?;

        let mut query = AnalyzedQuery {
            sql: sql.to_string(),
            tables: Vec::new(),
            where_columns: Vec::new(),
            order_columns: Vec::new(),
            group_columns: Vec::new(),
            base_cost: 0.0,
        };

        // Extract table references
        self.extract_tables(&ast, &mut query.tables);

        // Extract WHERE clause columns
        if let Some(where_clause) = ast.where_clause() {
            self.extract_where_columns(where_clause, &mut query.where_columns);
        }

        // Extract ORDER BY columns
        if let Some(order_by) = ast.order_by() {
            self.extract_order_columns(order_by, &mut query.order_columns);
        }

        // Extract GROUP BY columns
        if let Some(group_by) = ast.group_by() {
            self.extract_group_columns(group_by, &mut query.group_columns);
        }

        Ok(query)
    }

    fn extract_where_columns(&self, expr: &Expr, columns: &mut Vec<ColumnRef>) {
        match expr {
            Expr::Binary { left, op, right } => {
                // Check for column comparison
                if let Expr::Column(col) = left.as_ref() {
                    columns.push(ColumnRef {
                        table: col.table.clone().unwrap_or_default(),
                        column: col.name.clone(),
                        op: Some(self.convert_op(op)),
                    });
                }

                // Recurse for AND/OR
                if matches!(op, BinaryOp::And | BinaryOp::Or) {
                    self.extract_where_columns(left, columns);
                    self.extract_where_columns(right, columns);
                }
            }
            Expr::In { expr, list } => {
                if let Expr::Column(col) = expr.as_ref() {
                    columns.push(ColumnRef {
                        table: col.table.clone().unwrap_or_default(),
                        column: col.name.clone(),
                        op: Some(CompareOp::In),
                    });
                }
            }
            Expr::Between { expr, low, high } => {
                if let Expr::Column(col) = expr.as_ref() {
                    columns.push(ColumnRef {
                        table: col.table.clone().unwrap_or_default(),
                        column: col.name.clone(),
                        op: Some(CompareOp::Between),
                    });
                }
            }
            _ => {}
        }
    }

    fn convert_op(&self, op: &BinaryOp) -> CompareOp {
        match op {
            BinaryOp::Eq => CompareOp::Eq,
            BinaryOp::Lt => CompareOp::Lt,
            BinaryOp::Le => CompareOp::Le,
            BinaryOp::Gt => CompareOp::Gt,
            BinaryOp::Ge => CompareOp::Ge,
            BinaryOp::Like => CompareOp::Like,
            _ => CompareOp::Eq,
        }
    }
}
```

## Expert Operations

### Index Candidate Generation
```rust
impl SqliteExpert {
    pub fn new(db: Connection) -> Self {
        Self {
            db,
            candidates: Vec::new(),
            queries: Vec::new(),
            recommendations: Vec::new(),
        }
    }

    /// Add SQL query for analysis
    pub fn sql(&mut self, sql: &str) -> Result<()> {
        let analyzer = QueryAnalyzer::new();
        let query = analyzer.analyze(sql)?;

        // Estimate base cost
        let base_cost = self.estimate_cost(&query)?;
        let mut query = query;
        query.base_cost = base_cost;

        self.queries.push(query);
        Ok(())
    }

    /// Generate candidate indexes from analyzed queries
    fn generate_candidates(&mut self) -> Result<()> {
        for query in &self.queries {
            // Generate candidates from WHERE columns
            for table in &query.tables {
                let where_cols: Vec<_> = query.where_columns.iter()
                    .filter(|c| c.table.is_empty() || c.table == *table)
                    .collect();

                // Single column indexes
                for col in &where_cols {
                    self.add_candidate(CandidateIndex {
                        table: table.clone(),
                        columns: vec![IndexColumn {
                            name: col.column.clone(),
                            desc: false,
                            is_where: true,
                            is_order: false,
                        }],
                        is_unique: false,
                        score: 0.0,
                    });
                }

                // Multi-column indexes (equality columns first)
                let eq_cols: Vec<_> = where_cols.iter()
                    .filter(|c| matches!(c.op, Some(CompareOp::Eq)))
                    .collect();

                let range_cols: Vec<_> = where_cols.iter()
                    .filter(|c| !matches!(c.op, Some(CompareOp::Eq)))
                    .collect();

                if !eq_cols.is_empty() {
                    let mut columns: Vec<IndexColumn> = eq_cols.iter()
                        .map(|c| IndexColumn {
                            name: c.column.clone(),
                            desc: false,
                            is_where: true,
                            is_order: false,
                        })
                        .collect();

                    // Add first range column
                    if let Some(range_col) = range_cols.first() {
                        columns.push(IndexColumn {
                            name: range_col.column.clone(),
                            desc: false,
                            is_where: true,
                            is_order: false,
                        });
                    }

                    self.add_candidate(CandidateIndex {
                        table: table.clone(),
                        columns,
                        is_unique: false,
                        score: 0.0,
                    });
                }

                // Covering indexes (include ORDER BY)
                let order_cols: Vec<_> = query.order_columns.iter()
                    .filter(|c| c.table.is_empty() || c.table == *table)
                    .collect();

                if !order_cols.is_empty() && !eq_cols.is_empty() {
                    let mut columns: Vec<IndexColumn> = eq_cols.iter()
                        .map(|c| IndexColumn {
                            name: c.column.clone(),
                            desc: false,
                            is_where: true,
                            is_order: false,
                        })
                        .collect();

                    for order_col in &order_cols {
                        columns.push(IndexColumn {
                            name: order_col.column.clone(),
                            desc: false,
                            is_where: false,
                            is_order: true,
                        });
                    }

                    self.add_candidate(CandidateIndex {
                        table: table.clone(),
                        columns,
                        is_unique: false,
                        score: 0.0,
                    });
                }
            }
        }

        Ok(())
    }

    fn add_candidate(&mut self, candidate: CandidateIndex) {
        // Check for duplicates
        let exists = self.candidates.iter().any(|c| {
            c.table == candidate.table &&
            c.columns.len() == candidate.columns.len() &&
            c.columns.iter().zip(&candidate.columns)
                .all(|(a, b)| a.name == b.name && a.desc == b.desc)
        });

        if !exists {
            self.candidates.push(candidate);
        }
    }
}
```

### Index Evaluation
```rust
impl SqliteExpert {
    /// Analyze all candidates and generate recommendations
    pub fn analyze(&mut self) -> Result<()> {
        // Generate candidates
        self.generate_candidates()?;

        // Score each candidate
        for candidate in &mut self.candidates {
            candidate.score = self.score_candidate(candidate)?;
        }

        // Sort by score
        self.candidates.sort_by(|a, b| {
            b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal)
        });

        // Generate recommendations
        self.generate_recommendations()?;

        Ok(())
    }

    fn score_candidate(&self, candidate: &CandidateIndex) -> Result<f64> {
        let mut total_score = 0.0;

        // Create hypothetical index
        let index_sql = self.candidate_to_sql(candidate);

        // Try with hypothetical index
        for query in &self.queries {
            // Check if index is useful for this query
            let tables_overlap = query.tables.contains(&candidate.table);
            if !tables_overlap {
                continue;
            }

            // Check column overlap
            let useful_cols = candidate.columns.iter()
                .filter(|c| {
                    query.where_columns.iter().any(|w| w.column == c.name) ||
                    query.order_columns.iter().any(|o| o.column == c.name)
                })
                .count();

            if useful_cols > 0 {
                // Estimate improvement
                let col_factor = useful_cols as f64 / candidate.columns.len() as f64;
                let improvement = query.base_cost * col_factor * 0.5;
                total_score += improvement;
            }
        }

        Ok(total_score)
    }

    fn generate_recommendations(&mut self) -> Result<()> {
        // Take top candidates
        let mut selected: Vec<&CandidateIndex> = Vec::new();
        let mut covered_queries: HashSet<usize> = HashSet::new();

        for candidate in &self.candidates {
            if candidate.score < 1.0 {
                continue;
            }

            // Check which queries benefit
            let benefits: Vec<usize> = self.queries.iter().enumerate()
                .filter(|(_, q)| q.tables.contains(&candidate.table))
                .map(|(i, _)| i)
                .collect();

            // Skip if all queries already covered
            if benefits.iter().all(|i| covered_queries.contains(i)) {
                continue;
            }

            selected.push(candidate);
            covered_queries.extend(benefits.iter());

            // Limit recommendations
            if selected.len() >= 5 {
                break;
            }
        }

        // Generate SQL statements
        for candidate in selected {
            let sql = self.candidate_to_sql(candidate);
            let benefits: Vec<usize> = self.queries.iter().enumerate()
                .filter(|(_, q)| q.tables.contains(&candidate.table))
                .map(|(i, _)| i)
                .collect();

            self.recommendations.push(IndexRecommendation {
                sql,
                tables: vec![candidate.table.clone()],
                improvement: candidate.score,
                benefits_queries: benefits,
            });
        }

        Ok(())
    }

    fn candidate_to_sql(&self, candidate: &CandidateIndex) -> String {
        let cols: Vec<String> = candidate.columns.iter()
            .map(|c| {
                if c.desc {
                    format!("\"{}\" DESC", c.name)
                } else {
                    format!("\"{}\"", c.name)
                }
            })
            .collect();

        let index_name = format!(
            "idx_{}_{}",
            candidate.table,
            candidate.columns.iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join("_")
        );

        format!(
            "CREATE INDEX \"{}\" ON \"{}\" ({})",
            index_name,
            candidate.table,
            cols.join(", ")
        )
    }
}
```

### Report Generation
```rust
impl SqliteExpert {
    /// Get number of recommendations
    pub fn count(&self) -> usize {
        self.recommendations.len()
    }

    /// Get recommendation by index
    pub fn report(&self, idx: usize) -> Option<&IndexRecommendation> {
        self.recommendations.get(idx)
    }

    /// Generate full report
    pub fn full_report(&self) -> String {
        let mut report = String::new();

        report.push_str("=== SQLite Expert Index Recommendations ===\n\n");

        if self.recommendations.is_empty() {
            report.push_str("No index recommendations.\n");
            return report;
        }

        for (i, rec) in self.recommendations.iter().enumerate() {
            report.push_str(&format!("Recommendation #{}:\n", i + 1));
            report.push_str(&format!("  SQL: {}\n", rec.sql));
            report.push_str(&format!("  Estimated improvement: {:.1}%\n",
                rec.improvement * 100.0));
            report.push_str(&format!("  Benefits {} queries\n\n",
                rec.benefits_queries.len()));
        }

        report
    }

    /// Destroy expert and free resources
    pub fn destroy(self) {
        // Rust handles cleanup automatically
    }
}
```

## Acceptance Criteria
- [ ] Expert handle creation
- [ ] SQL query analysis
- [ ] WHERE clause column extraction
- [ ] ORDER BY column extraction
- [ ] GROUP BY column extraction
- [ ] Single-column index candidates
- [ ] Multi-column index candidates
- [ ] Covering index candidates
- [ ] Index scoring algorithm
- [ ] Recommendation generation
- [ ] CREATE INDEX SQL generation
- [ ] Report generation
- [ ] Cost estimation

## TCL Tests That Should Pass
After completion, the following SQLite TCL test files should pass:
- `expert1.test` - Basic expert functionality
- `expert2.test` - Index recommendations
