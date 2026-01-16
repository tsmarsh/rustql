//! SQLite Compatibility Test Suite
//!
//! Run with: cargo test --test sqlite_compat_test
//!
//! This test suite runs SQLite's TCL tests against rustql to measure
//! compatibility and track progress.

mod sqlite_compat;

use sqlite_compat::runner::{SuiteStats, TestRunner};
use sqlite_compat::rustql_adapter::RustqlTestDb;
use sqlite_compat::{analyze_test_suite, load_test_file, PRIORITY_TEST_FILES, SQLITE_TEST_DIR};
use std::path::Path;

/// Analyze the SQLite test suite (without running)
#[test]
fn test_analyze_sqlite_suite() {
    let test_dir = Path::new(SQLITE_TEST_DIR);
    if !test_dir.exists() {
        println!("SQLite test directory not found at: {}", SQLITE_TEST_DIR);
        println!("Skipping test suite analysis");
        return;
    }

    match analyze_test_suite(test_dir) {
        Ok(analysis) => {
            analysis.print_summary();
        }
        Err(e) => {
            println!("Failed to analyze test suite: {}", e);
        }
    }
}

/// Run a single priority test file
fn run_test_file(file_name: &str) -> Option<sqlite_compat::runner::TestFileStats> {
    let test_dir = Path::new(SQLITE_TEST_DIR);
    let path = test_dir.join(file_name);

    if !path.exists() {
        println!("Test file not found: {}", path.display());
        return None;
    }

    let parsed = match load_test_file(&path) {
        Ok(p) => p,
        Err(e) => {
            println!("Failed to parse {}: {}", file_name, e);
            return None;
        }
    };

    println!(
        "\nRunning {} ({} tests, {} setup commands)",
        file_name,
        parsed.tests.len(),
        parsed.setup_commands.len()
    );

    // Create a fresh database for each test file
    let db_path = format!(
        "/tmp/rustql_sqlite_compat_{}.db",
        file_name.replace('.', "_")
    );
    let db = match RustqlTestDb::new(&db_path) {
        Ok(db) => db,
        Err(e) => {
            println!("Failed to create database: {}", e);
            return None;
        }
    };

    let mut runner = TestRunner::new(db);
    runner.set_verbose(true);

    let (stats, results) = runner.run_file(&parsed);

    // Print detailed results for failed tests
    let failed: Vec<_> = results.iter().filter(|r| !r.passed && !r.skipped).collect();
    if !failed.is_empty() {
        println!("\nFailed tests:");
        for result in failed.iter().take(10) {
            println!("  {} (line {})", result.name, result.line);
            if let Some(ref err) = result.error {
                println!("    Error: {}", err);
            } else {
                println!("    Expected: {:?}", result.expected);
                println!("    Actual:   {:?}", result.actual);
            }
        }
        if failed.len() > 10 {
            println!("  ... and {} more", failed.len() - 10);
        }
    }

    let skipped: Vec<_> = results.iter().filter(|r| r.skipped).collect();
    if !skipped.is_empty() {
        println!("\nSkipped {} tests (unimplemented features)", skipped.len());
    }

    println!(
        "\n{}: {} total, {} passed, {} failed, {} skipped ({:.1}%)",
        file_name,
        stats.total,
        stats.passed,
        stats.failed,
        stats.skipped,
        stats.pass_rate()
    );

    Some(stats)
}

/// Run all priority test files and report overall progress
/// This test is ignored by default because it runs the full SQLite test suite
/// and takes a long time. Run with: cargo test --ignored test_sqlite_compatibility_progress
#[test]
#[ignore]
fn test_sqlite_compatibility_progress() {
    let test_dir = Path::new(SQLITE_TEST_DIR);
    if !test_dir.exists() {
        println!("SQLite test directory not found at: {}", SQLITE_TEST_DIR);
        println!("Skipping compatibility tests");
        return;
    }

    println!("\n{}", "=".repeat(60));
    println!("SQLite Compatibility Test Suite");
    println!("{}", "=".repeat(60));

    let mut suite_stats = SuiteStats::default();

    for file_name in PRIORITY_TEST_FILES {
        if let Some(stats) = run_test_file(file_name) {
            suite_stats.add_file(stats);
        }
    }

    suite_stats.print_summary();

    // Don't fail the test - this is for measuring progress
    println!("\nNote: This test measures compatibility progress, not pass/fail.");
}

/// Test individual test files (for debugging)
#[test]
fn test_table_basic() {
    if let Some(stats) = run_test_file("table.test") {
        println!("table.test: {:.1}% pass rate", stats.pass_rate());
    }
}

#[test]
fn test_select1_basic() {
    if let Some(stats) = run_test_file("select1.test") {
        println!("select1.test: {:.1}% pass rate", stats.pass_rate());
    }
}

/// Quick smoke test with simple SQL
#[test]
fn test_basic_sql_execution() {
    let db = match RustqlTestDb::new("/tmp/rustql_smoke_test.db") {
        Ok(db) => db,
        Err(e) => {
            println!("Failed to create database: {}", e);
            return;
        }
    };

    let mut db = db;

    // Test basic operations
    let tests = [
        ("CREATE TABLE", "CREATE TABLE t1(a, b)", vec![]),
        ("INSERT", "INSERT INTO t1 VALUES(1, 2)", vec![]),
        ("SELECT", "SELECT * FROM t1", vec!["1", "2"]),
        // Test COALESCE function
        (
            "CREATE t2",
            "CREATE TABLE t2(a INTEGER PRIMARY KEY, b, c, d)",
            vec![],
        ),
        (
            "INSERT t2-1",
            "INSERT INTO t2 VALUES(1, null, null, null)",
            vec![],
        ),
        ("INSERT t2-2", "INSERT INTO t2 VALUES(2, 2, 99, 99)", vec![]),
        (
            "COALESCE",
            "SELECT coalesce(b,c,d) FROM t2 ORDER BY a",
            vec!["{}", "2"],
        ),
    ];

    for (name, sql, expected) in &tests {
        match db.exec_sql(sql) {
            Ok(result) => {
                let expected_strs: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
                if result == expected_strs {
                    println!("PASS: {}", name);
                } else {
                    println!("FAIL: {} - expected {:?}, got {:?}", name, expected, result);
                }
            }
            Err(e) => {
                println!("ERROR: {} - {}", name, e);
            }
        }
    }
}

/// Test coalesce function with full data from coalesce.test
#[test]
fn test_coalesce_full() {
    let db = match RustqlTestDb::new("/tmp/rustql_coalesce_test.db") {
        Ok(db) => db,
        Err(e) => {
            println!("Failed to create database: {}", e);
            return;
        }
    };
    let mut db = db;

    // Setup from coalesce.test
    let setup = r#"
        CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c, d);
        INSERT INTO t1 VALUES(1, null, null, null);
        INSERT INTO t1 VALUES(2, 2, 99, 99);
        INSERT INTO t1 VALUES(3, null, 3, 99);
        INSERT INTO t1 VALUES(4, null, null, 4);
        INSERT INTO t1 VALUES(5, null, null, null);
        INSERT INTO t1 VALUES(6, 22, 99, 99);
        INSERT INTO t1 VALUES(7, null, 33, 99);
        INSERT INTO t1 VALUES(8, null, null, 44);
    "#;

    match db.exec_sql(setup) {
        Ok(_) => println!("Setup OK"),
        Err(e) => {
            println!("Setup ERROR: {}", e);
            return;
        }
    }

    // Test coalesce-1.0: SELECT coalesce(b,c,d) FROM t1 ORDER BY a
    match db.exec_sql("SELECT coalesce(b,c,d) FROM t1 ORDER BY a") {
        Ok(result) => {
            let expected = vec!["{}", "2", "3", "4", "{}", "22", "33", "44"];
            println!("coalesce-1.0:");
            println!("  Expected: {:?}", expected);
            println!("  Got:      {:?}", result);
            if result == expected {
                println!("  PASS");
            } else {
                println!("  FAIL");
            }
        }
        Err(e) => println!("Query ERROR: {}", e),
    }

    // Also check raw data
    match db.exec_sql("SELECT a, b, c, d FROM t1 ORDER BY a") {
        Ok(result) => {
            println!("\nRaw data (a, b, c, d):");
            println!("  {:?}", result);
        }
        Err(e) => println!("Raw query ERROR: {}", e),
    }
}

/// Generate and print a progress report
#[test]
fn test_progress_report() {
    let test_dir = Path::new(SQLITE_TEST_DIR);

    let report = sqlite_compat::progress::generate_progress_report(test_dir);
    report.print_report();
}

// Re-export for use by other test modules
pub use sqlite_compat::runner::TestDatabase;

#[test]
fn test_where_clause_filtering() {
    let db = match RustqlTestDb::new("/tmp/rustql_where_test.db") {
        Ok(db) => db,
        Err(e) => {
            println!("Failed to create database: {}", e);
            return;
        }
    };
    let mut db = db;

    // Setup
    let _ = db.exec_sql("CREATE TABLE t1(w INT, x INT, y INT)");
    let _ = db.exec_sql("INSERT INTO t1 VALUES(1, 10, 100)");
    let _ = db.exec_sql("INSERT INTO t1 VALUES(2, 20, 200)");
    let _ = db.exec_sql("INSERT INTO t1 VALUES(3, 30, 300)");
    let _ = db.exec_sql("INSERT INTO t1 VALUES(4, 40, 400)");

    // Test WHERE clause
    match db.exec_sql("SELECT w, x, y FROM t1 WHERE w = 2") {
        Ok(result) => {
            println!("WHERE test result: {:?}", result);
            if result == vec!["2", "20", "200"] {
                println!("PASS: WHERE clause filtering works!");
            } else {
                println!("FAIL: Expected [2, 20, 200], got {:?}", result);
            }
        }
        Err(e) => println!("ERROR: {}", e),
    }

    // Test aggregate with WHERE
    match db.exec_sql("SELECT COUNT(*) FROM t1 WHERE w > 2") {
        Ok(result) => {
            println!("COUNT with WHERE result: {:?}", result);
            if result == vec!["2"] {
                println!("PASS: COUNT with WHERE works!");
            } else {
                println!("FAIL: Expected [2], got {:?}", result);
            }
        }
        Err(e) => println!("ERROR: {}", e),
    }
}

#[test]
fn test_count_aggregate() {
    let db = match RustqlTestDb::new("/tmp/rustql_count_test.db") {
        Ok(db) => db,
        Err(e) => {
            println!("Failed to create database: {}", e);
            return;
        }
    };
    let mut db = db;

    // Setup - create table with 2 rows
    let _ = db.exec_sql("CREATE TABLE test1(f1 INT, f2 INT)");
    let _ = db.exec_sql("INSERT INTO test1 VALUES(11, 22)");
    let _ = db.exec_sql("INSERT INTO test1 VALUES(33, 44)");

    // Check row count
    match db.exec_sql("SELECT * FROM test1") {
        Ok(result) => println!("All rows: {:?}", result),
        Err(e) => println!("ERROR: {}", e),
    }

    // Test COUNT(*)
    match db.exec_sql("SELECT COUNT(*) FROM test1") {
        Ok(result) => {
            println!("COUNT(*) result: {:?}", result);
            if result == vec!["2"] {
                println!("PASS: COUNT(*) returns 2");
            } else {
                println!("FAIL: Expected [2], got {:?}", result);
            }
        }
        Err(e) => println!("ERROR: {}", e),
    }

    // Test COUNT(f1)
    match db.exec_sql("SELECT COUNT(f1) FROM test1") {
        Ok(result) => {
            println!("COUNT(f1) result: {:?}", result);
            if result == vec!["2"] {
                println!("PASS: COUNT(f1) returns 2");
            } else {
                println!("FAIL: Expected [2], got {:?}", result);
            }
        }
        Err(e) => println!("ERROR: {}", e),
    }

    // Test MAX
    match db.exec_sql("SELECT MAX(f1) FROM test1") {
        Ok(result) => {
            println!("MAX(f1) result: {:?}", result);
            if result == vec!["33"] {
                println!("PASS: MAX(f1) returns 33");
            } else {
                println!("FAIL: Expected [33], got {:?}", result);
            }
        }
        Err(e) => println!("ERROR: {}", e),
    }
}

#[test]
fn test_distinct() {
    let db = match RustqlTestDb::new("/tmp/rustql_distinct_test.db") {
        Ok(db) => db,
        Err(e) => {
            println!("Failed to create database: {}", e);
            return;
        }
    };
    let mut db = db;

    // Setup - table with duplicate values
    let _ = db.exec_sql("CREATE TABLE t1(a INTEGER, b TEXT)");
    let _ = db.exec_sql("INSERT INTO t1 VALUES(1, 'one')");
    let _ = db.exec_sql("INSERT INTO t1 VALUES(2, 'two')");
    let _ = db.exec_sql("INSERT INTO t1 VALUES(1, 'one')");
    let _ = db.exec_sql("INSERT INTO t1 VALUES(3, 'three')");
    let _ = db.exec_sql("INSERT INTO t1 VALUES(2, 'two')");

    // Test without DISTINCT
    println!("Without DISTINCT:");
    match db.exec_sql("SELECT a FROM t1 ORDER BY a") {
        Ok(result) => {
            println!("  Result: {:?}", result);
            println!("  Expected: [1, 1, 2, 2, 3]");
        }
        Err(e) => println!("  ERROR: {}", e),
    }

    // Test with DISTINCT
    println!("\nWith DISTINCT:");
    match db.exec_sql("SELECT DISTINCT a FROM t1 ORDER BY a") {
        Ok(result) => {
            println!("  Result: {:?}", result);
            if result == vec!["1", "2", "3"] {
                println!("  PASS: DISTINCT works!");
            } else {
                println!("  FAIL: Expected [1, 2, 3], got {:?}", result);
            }
        }
        Err(e) => println!("  ERROR: {}", e),
    }
}

#[test]
fn test_scalar_min_max() {
    let db = match RustqlTestDb::new("/tmp/rustql_scalar_minmax_test.db") {
        Ok(db) => db,
        Err(e) => {
            println!("Failed to create database: {}", e);
            return;
        }
    };
    let mut db = db;

    // Setup
    let _ = db.exec_sql("CREATE TABLE test1(f1 INT, f2 INT)");
    let _ = db.exec_sql("INSERT INTO test1 VALUES(11, 22)");

    // Test scalar min
    println!("Testing scalar min(f1, f2):");
    match db.exec_sql("SELECT min(f1, f2) FROM test1") {
        Ok(result) => {
            println!("  Result: {:?}", result);
            if result == vec!["11"] {
                println!("  PASS: min(11, 22) = 11");
            } else {
                println!("  FAIL: Expected [11], got {:?}", result);
            }
        }
        Err(e) => println!("  ERROR: {}", e),
    }

    // Test scalar max
    println!("\nTesting scalar max(f1, f2):");
    match db.exec_sql("SELECT max(f1, f2) FROM test1") {
        Ok(result) => {
            println!("  Result: {:?}", result);
            if result == vec!["22"] {
                println!("  PASS: max(11, 22) = 22");
            } else {
                println!("  FAIL: Expected [22], got {:?}", result);
            }
        }
        Err(e) => println!("  ERROR: {}", e),
    }

    // Test both together
    println!("\nTesting combined: SELECT *, min(f1,f2), max(f1,f2) FROM test1");
    match db.exec_sql("SELECT *, min(f1,f2), max(f1,f2) FROM test1") {
        Ok(result) => {
            println!("  Result: {:?}", result);
            if result == vec!["11", "22", "11", "22"] {
                println!("  PASS: Combined scalar min/max works!");
            } else {
                println!("  FAIL: Expected [11, 22, 11, 22], got {:?}", result);
            }
        }
        Err(e) => println!("  ERROR: {}", e),
    }
}

#[test]
fn test_value_one_bug() {
    let db = match RustqlTestDb::new("/tmp/rustql_value_one_test.db") {
        Ok(db) => db,
        Err(e) => {
            println!("Failed to create database: {}", e);
            return;
        }
    };
    let mut db = db;

    // Test various small integer values
    let _ = db.exec_sql("CREATE TABLE t(a INT)");

    let test_values = vec!["0", "1", "2", "-1", "10"];
    for val in test_values {
        let _ = db.exec_sql(&format!("DELETE FROM t"));
        let _ = db.exec_sql(&format!("INSERT INTO t VALUES({})", val));

        println!("\nTesting value {}:", val);
        match db.exec_sql("SELECT a, typeof(a) FROM t") {
            Ok(result) => {
                println!("  Result: {:?}", result);
                let expected_val = val;
                let expected_type = "integer";
                if result.len() >= 2 && result[0] == expected_val && result[1] == expected_type {
                    println!("  PASS!");
                } else {
                    println!("  FAIL: Expected [{}, integer], got {:?}", val, result);
                }
            }
            Err(e) => println!("  ERROR: {}", e),
        }
    }
}
