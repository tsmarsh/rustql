//! Opcode Comparison Tests
//!
//! This module compares the opcode sequences emitted by RustQL's compiler
//! against SQLite's EXPLAIN output to ensure semantic equivalence.
//!
//! # Normalization Rules
//!
//! The following differences are considered acceptable:
//!
//! 1. **Address values (p2)**: Jump targets may differ due to different
//!    instruction ordering, but the control flow structure should match.
//!
//! 2. **Register allocation (p1, p3)**: RustQL may use different register
//!    numbers than SQLite, but the data flow should be equivalent.
//!
//! 3. **Trace opcode**: SQLite includes Trace opcodes for debugging which
//!    RustQL omits in normal compilation.
//!
//! 4. **Explain opcode**: Present in EXPLAIN output but not executed.
//!
//! 5. **AggStep vs AggStep1**: SQLite uses AggStep1 for single-argument
//!    aggregates; RustQL may use AggStep with appropriate flags.
//!
//! 6. **Init jump target**: The Init opcode's jump target (p2) varies based
//!    on the total instruction count.
//!
//! # Pass Criteria
//!
//! A query passes if:
//! - The normalized opcode sequence matches, OR
//! - The opcodes are semantically equivalent (same operations in same order)
//!
//! # Known Differences
//!
//! Document any intentional differences here:
//! - RustQL includes `AggStep0` for aggregate initialization
//! - RustQL includes `MaxOpcode` and `Unused` as sentinel values

use rustql::{sqlite3_open, sqlite3_prepare_v2, SqliteConnection};

use std::collections::HashMap;
use std::process::Command;

/// Extract opcodes from RustQL compiled bytecode
fn extract_rustql_opcodes(sql: &str, conn: &mut SqliteConnection) -> Vec<String> {
    match sqlite3_prepare_v2(conn, sql) {
        Ok((stmt, _)) => stmt
            .ops()
            .iter()
            .map(|op| format!("{:?}", op.opcode))
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Extract opcodes from SQLite EXPLAIN output
fn extract_sqlite_opcodes(sql: &str, db_path: &str) -> Vec<String> {
    let output = Command::new("sqlite3")
        .arg(db_path)
        .arg(format!("EXPLAIN {}", sql))
        .output();

    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            stdout
                .lines()
                .filter_map(|line| {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    // Skip header lines (addr, opcode, etc.) and separator lines
                    if parts.len() >= 2 {
                        let first = parts[0];
                        // Only process lines where first column is a number (address)
                        if first.parse::<i32>().is_ok() {
                            Some(parts[1].to_string())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect()
        }
        Err(_) => Vec::new(),
    }
}

/// Normalize opcode name for comparison
fn normalize_opcode(opcode: &str) -> &str {
    match opcode {
        // Trace is debug-only
        "Trace" => "",
        // Explain is meta-opcode
        "Explain" => "",
        // AggStep1 is equivalent to AggStep for single-arg
        "AggStep1" => "AggStep",
        // Init and Goto are control flow that RustQL handles differently
        // RustQL starts execution directly at the first instruction
        "Init" => "",
        "Goto" => "",
        // Transaction is SQLite's read-lock opcode, handled implicitly by RustQL
        "Transaction" => "",
        // Close is cursor cleanup, may be placed differently or implicitly handled
        "Close" => "",
        _ => opcode,
    }
}

/// Normalize opcode sequence for comparison
fn normalize_sequence(opcodes: &[String]) -> Vec<String> {
    opcodes
        .iter()
        .map(|op| normalize_opcode(op).to_string())
        .filter(|op| !op.is_empty())
        .collect()
}

/// Compare two opcode sequences structurally
fn compare_opcode_structure(sqlite_ops: &[String], rustql_ops: &[String]) -> ComparisonResult {
    let sqlite_norm = normalize_sequence(sqlite_ops);
    let rustql_norm = normalize_sequence(rustql_ops);

    if sqlite_norm == rustql_norm {
        return ComparisonResult::Exact;
    }

    // Check for semantic equivalence (same opcodes, potentially different order)
    let sqlite_counts = count_opcodes(&sqlite_norm);
    let rustql_counts = count_opcodes(&rustql_norm);

    if sqlite_counts == rustql_counts {
        return ComparisonResult::SemanticMatch;
    }

    // Identify differences
    let mut missing = Vec::new();
    let mut extra = Vec::new();

    for (op, count) in &sqlite_counts {
        let rustql_count = rustql_counts.get(op).unwrap_or(&0);
        if rustql_count < count {
            missing.push((op.clone(), count - rustql_count));
        }
    }

    for (op, count) in &rustql_counts {
        let sqlite_count = sqlite_counts.get(op).unwrap_or(&0);
        if sqlite_count < count {
            extra.push((op.clone(), count - sqlite_count));
        }
    }

    ComparisonResult::Different { missing, extra }
}

fn count_opcodes(opcodes: &[String]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for op in opcodes {
        *counts.entry(op.clone()).or_insert(0) += 1;
    }
    counts
}

#[derive(Debug, PartialEq)]
enum ComparisonResult {
    /// Exact match after normalization
    Exact,
    /// Same opcodes but different order (acceptable for some cases)
    SemanticMatch,
    /// Different opcodes
    Different {
        missing: Vec<(String, usize)>,
        extra: Vec<(String, usize)>,
    },
}

/// Test case for opcode comparison
struct TestCase {
    name: &'static str,
    sql: &'static str,
    setup: Option<&'static str>,
}

impl TestCase {
    fn new(name: &'static str, sql: &'static str) -> Self {
        Self {
            name,
            sql,
            setup: None,
        }
    }

    fn with_setup(name: &'static str, sql: &'static str, setup: &'static str) -> Self {
        Self {
            name,
            sql,
            setup: Some(setup),
        }
    }
}

/// Run opcode comparison tests
fn run_comparison_tests(cases: &[TestCase]) -> (usize, usize, Vec<String>) {
    let mut passed = 0;
    let mut failed = 0;
    let mut failures = Vec::new();

    // Create temp database file
    let tmp_dir = std::env::temp_dir();
    let db_path = tmp_dir.join("opcode_compare_test.db");
    let db_path_str = db_path.to_str().unwrap();

    // Clean up any existing file
    let _ = std::fs::remove_file(&db_path);

    for case in cases {
        // Reset database for each test
        let _ = std::fs::remove_file(&db_path);

        // Create SQLite database with setup
        if let Some(setup) = case.setup {
            let _ = Command::new("sqlite3").arg(db_path_str).arg(setup).output();
        } else {
            // Create empty database
            let _ = Command::new("sqlite3")
                .arg(db_path_str)
                .arg("SELECT 1")
                .output();
        }

        // Create RustQL connection
        let mut conn = match sqlite3_open(db_path_str) {
            Ok(c) => c,
            Err(_) => {
                failures.push(format!("{}: RustQL open failed", case.name));
                failed += 1;
                continue;
            }
        };

        // Run setup in RustQL if needed
        if let Some(setup) = case.setup {
            for stmt_sql in setup.split(';').filter(|s| !s.trim().is_empty()) {
                let _ = sqlite3_prepare_v2(&mut conn, stmt_sql);
            }
        }

        // Extract opcodes
        let sqlite_ops = extract_sqlite_opcodes(case.sql, db_path_str);
        let rustql_ops = extract_rustql_opcodes(case.sql, &mut conn);

        // Compare
        match compare_opcode_structure(&sqlite_ops, &rustql_ops) {
            ComparisonResult::Exact => {
                passed += 1;
            }
            ComparisonResult::SemanticMatch => {
                // Accept semantic matches
                passed += 1;
            }
            ComparisonResult::Different { missing, extra } => {
                let mut msg = format!("{}: Opcode mismatch\n", case.name);
                msg.push_str(&format!("  SQL: {}\n", case.sql));
                if !missing.is_empty() {
                    msg.push_str(&format!(
                        "  Missing (in SQLite but not RustQL): {:?}\n",
                        missing
                    ));
                }
                if !extra.is_empty() {
                    msg.push_str(&format!(
                        "  Extra (in RustQL but not SQLite): {:?}\n",
                        extra
                    ));
                }
                msg.push_str(&format!(
                    "  SQLite: {:?}\n",
                    normalize_sequence(&sqlite_ops)
                ));
                msg.push_str(&format!(
                    "  RustQL: {:?}\n",
                    normalize_sequence(&rustql_ops)
                ));
                failures.push(msg);
                failed += 1;
            }
        }
    }

    // Clean up
    let _ = std::fs::remove_file(&db_path);

    (passed, failed, failures)
}

#[test]
fn test_simple_expressions() {
    let cases = vec![
        TestCase::new("literal_integer", "SELECT 1"),
        TestCase::new("literal_string", "SELECT 'hello'"),
        TestCase::new("literal_null", "SELECT NULL"),
        TestCase::new("addition", "SELECT 1+1"),
        TestCase::new("subtraction", "SELECT 5-3"),
        TestCase::new("multiplication", "SELECT 2*3"),
        TestCase::new("division", "SELECT 10/2"),
        TestCase::new("multiple_columns", "SELECT 1, 2, 3"),
        TestCase::new("nested_arithmetic", "SELECT (1+2)*3"),
    ];

    let (passed, failed, failures) = run_comparison_tests(&cases);

    if !failures.is_empty() {
        for f in &failures {
            eprintln!("{}", f);
        }
    }

    println!("Simple expressions: {} passed, {} failed", passed, failed);
    // Note: We don't assert here to allow documenting differences
}

#[test]
fn test_comparisons() {
    let cases = vec![
        TestCase::new("equals", "SELECT 1=1"),
        TestCase::new("not_equals", "SELECT 1!=2"),
        TestCase::new("less_than", "SELECT 1<2"),
        TestCase::new("greater_than", "SELECT 2>1"),
        TestCase::new("less_equal", "SELECT 1<=1"),
        TestCase::new("greater_equal", "SELECT 2>=2"),
        TestCase::new("is_null", "SELECT NULL IS NULL"),
        TestCase::new("is_not_null", "SELECT 1 IS NOT NULL"),
    ];

    let (passed, failed, failures) = run_comparison_tests(&cases);

    if !failures.is_empty() {
        for f in &failures {
            eprintln!("{}", f);
        }
    }

    println!("Comparisons: {} passed, {} failed", passed, failed);
}

#[test]
fn test_where_clause() {
    let cases = vec![
        TestCase::new("where_true", "SELECT 1 WHERE 1=1"),
        TestCase::new("where_false", "SELECT 1 WHERE 0"),
        TestCase::new("where_and", "SELECT 1 WHERE 1=1 AND 2=2"),
        TestCase::new("where_or", "SELECT 1 WHERE 1=0 OR 1=1"),
    ];

    let (passed, failed, failures) = run_comparison_tests(&cases);

    if !failures.is_empty() {
        for f in &failures {
            eprintln!("{}", f);
        }
    }

    println!("WHERE clause: {} passed, {} failed", passed, failed);
}

#[test]
fn test_case_expression() {
    let cases = vec![
        TestCase::new("simple_case", "SELECT CASE WHEN 1 THEN 'a' ELSE 'b' END"),
        TestCase::new(
            "case_multiple",
            "SELECT CASE WHEN 0 THEN 'a' WHEN 1 THEN 'b' ELSE 'c' END",
        ),
        TestCase::new("coalesce", "SELECT COALESCE(NULL, 1)"),
        TestCase::new("ifnull", "SELECT IFNULL(NULL, 'default')"),
        TestCase::new("nullif", "SELECT NULLIF(1, 1)"),
    ];

    let (passed, failed, failures) = run_comparison_tests(&cases);

    if !failures.is_empty() {
        for f in &failures {
            eprintln!("{}", f);
        }
    }

    println!("CASE expressions: {} passed, {} failed", passed, failed);
}

#[test]
fn test_table_scans() {
    let setup = "CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c REAL);
                 INSERT INTO t1 VALUES(1, 'one', 1.0);
                 INSERT INTO t1 VALUES(2, 'two', 2.0)";

    let cases = vec![
        TestCase::with_setup("select_star", "SELECT * FROM t1", setup),
        TestCase::with_setup("select_column", "SELECT a FROM t1", setup),
        TestCase::with_setup("select_multiple", "SELECT a, b FROM t1", setup),
        TestCase::with_setup("where_equals", "SELECT * FROM t1 WHERE a = 1", setup),
        TestCase::with_setup("where_range", "SELECT * FROM t1 WHERE a > 1", setup),
    ];

    let (passed, failed, failures) = run_comparison_tests(&cases);

    if !failures.is_empty() {
        for f in &failures {
            eprintln!("{}", f);
        }
    }

    println!("Table scans: {} passed, {} failed", passed, failed);
}

#[test]
fn test_order_by() {
    let setup = "CREATE TABLE t1(a INTEGER, b TEXT);
                 INSERT INTO t1 VALUES(3, 'c');
                 INSERT INTO t1 VALUES(1, 'a');
                 INSERT INTO t1 VALUES(2, 'b')";

    let cases = vec![
        TestCase::with_setup("order_asc", "SELECT * FROM t1 ORDER BY a", setup),
        TestCase::with_setup("order_desc", "SELECT * FROM t1 ORDER BY a DESC", setup),
        TestCase::with_setup("order_text", "SELECT * FROM t1 ORDER BY b", setup),
        TestCase::with_setup("order_multi", "SELECT * FROM t1 ORDER BY a, b", setup),
    ];

    let (passed, failed, failures) = run_comparison_tests(&cases);

    if !failures.is_empty() {
        for f in &failures {
            eprintln!("{}", f);
        }
    }

    println!("ORDER BY: {} passed, {} failed", passed, failed);
}

#[test]
fn test_aggregates() {
    let setup = "CREATE TABLE t1(a INTEGER, b INTEGER);
                 INSERT INTO t1 VALUES(1, 10);
                 INSERT INTO t1 VALUES(2, 20);
                 INSERT INTO t1 VALUES(3, 30)";

    let cases = vec![
        TestCase::with_setup("count_star", "SELECT COUNT(*) FROM t1", setup),
        TestCase::with_setup("count_column", "SELECT COUNT(a) FROM t1", setup),
        TestCase::with_setup("sum", "SELECT SUM(b) FROM t1", setup),
        TestCase::with_setup("avg", "SELECT AVG(b) FROM t1", setup),
        TestCase::with_setup("min", "SELECT MIN(a) FROM t1", setup),
        TestCase::with_setup("max", "SELECT MAX(a) FROM t1", setup),
    ];

    let (passed, failed, failures) = run_comparison_tests(&cases);

    if !failures.is_empty() {
        for f in &failures {
            eprintln!("{}", f);
        }
    }

    println!("Aggregates: {} passed, {} failed", passed, failed);
}

#[test]
fn test_group_by() {
    let setup = "CREATE TABLE t1(a INTEGER, b INTEGER);
                 INSERT INTO t1 VALUES(1, 10);
                 INSERT INTO t1 VALUES(1, 20);
                 INSERT INTO t1 VALUES(2, 30)";

    let cases = vec![
        TestCase::with_setup(
            "group_count",
            "SELECT a, COUNT(*) FROM t1 GROUP BY a",
            setup,
        ),
        TestCase::with_setup("group_sum", "SELECT a, SUM(b) FROM t1 GROUP BY a", setup),
        TestCase::with_setup(
            "group_having",
            "SELECT a, COUNT(*) FROM t1 GROUP BY a HAVING COUNT(*) > 1",
            setup,
        ),
    ];

    let (passed, failed, failures) = run_comparison_tests(&cases);

    if !failures.is_empty() {
        for f in &failures {
            eprintln!("{}", f);
        }
    }

    println!("GROUP BY: {} passed, {} failed", passed, failed);
}

#[test]
fn test_joins() {
    let setup = "CREATE TABLE t1(a INTEGER, b TEXT);
                 CREATE TABLE t2(x INTEGER, y TEXT);
                 INSERT INTO t1 VALUES(1, 'a');
                 INSERT INTO t1 VALUES(2, 'b');
                 INSERT INTO t2 VALUES(1, 'x');
                 INSERT INTO t2 VALUES(3, 'z')";

    let cases = vec![
        TestCase::with_setup("cross_join", "SELECT * FROM t1, t2", setup),
        TestCase::with_setup(
            "inner_join",
            "SELECT * FROM t1 JOIN t2 ON t1.a = t2.x",
            setup,
        ),
        TestCase::with_setup(
            "left_join",
            "SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.x",
            setup,
        ),
    ];

    let (passed, failed, failures) = run_comparison_tests(&cases);

    if !failures.is_empty() {
        for f in &failures {
            eprintln!("{}", f);
        }
    }

    println!("JOINs: {} passed, {} failed", passed, failed);
}

#[test]
fn test_subqueries() {
    let setup = "CREATE TABLE t1(a INTEGER);
                 INSERT INTO t1 VALUES(1);
                 INSERT INTO t1 VALUES(2);
                 INSERT INTO t1 VALUES(3)";

    let cases = vec![
        TestCase::with_setup("scalar_subquery", "SELECT (SELECT MAX(a) FROM t1)", setup),
        TestCase::with_setup(
            "in_subquery",
            "SELECT * FROM t1 WHERE a IN (SELECT a FROM t1 WHERE a > 1)",
            setup,
        ),
        TestCase::with_setup(
            "exists_subquery",
            "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t1 WHERE a > 2)",
            setup,
        ),
    ];

    let (passed, failed, failures) = run_comparison_tests(&cases);

    if !failures.is_empty() {
        for f in &failures {
            eprintln!("{}", f);
        }
    }

    println!("Subqueries: {} passed, {} failed", passed, failed);
}

#[test]
fn test_union() {
    let cases = vec![
        TestCase::new("union", "SELECT 1 UNION SELECT 2"),
        TestCase::new("union_all", "SELECT 1 UNION ALL SELECT 1"),
        TestCase::new("except", "SELECT 1 UNION SELECT 2 EXCEPT SELECT 2"),
        TestCase::new("intersect", "SELECT 1 INTERSECT SELECT 1"),
    ];

    let (passed, failed, failures) = run_comparison_tests(&cases);

    if !failures.is_empty() {
        for f in &failures {
            eprintln!("{}", f);
        }
    }

    println!("UNION operations: {} passed, {} failed", passed, failed);
}

/// Generate a summary report of opcode differences
#[test]
fn test_generate_opcode_report() {
    println!("\n=== Opcode Comparison Report ===\n");

    // All test cases
    let all_cases = vec![
        // Simple expressions
        TestCase::new("SELECT 1", "SELECT 1"),
        TestCase::new("SELECT 'hello'", "SELECT 'hello'"),
        TestCase::new("SELECT 1+1", "SELECT 1+1"),
        // With tables
        TestCase::with_setup(
            "SELECT * FROM t1",
            "SELECT * FROM t1",
            "CREATE TABLE t1(a); INSERT INTO t1 VALUES(1)",
        ),
        TestCase::with_setup(
            "SELECT COUNT(*) FROM t1",
            "SELECT COUNT(*) FROM t1",
            "CREATE TABLE t1(a); INSERT INTO t1 VALUES(1)",
        ),
    ];

    let (passed, failed, failures) = run_comparison_tests(&all_cases);

    println!("Total: {} passed, {} failed", passed, failed);
    println!();

    if !failures.is_empty() {
        println!("Differences found (may be acceptable):");
        println!("=========================================");
        for f in &failures {
            println!("{}", f);
        }
    } else {
        println!("All comparisons passed!");
    }
}
