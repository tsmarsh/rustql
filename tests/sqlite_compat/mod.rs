//! SQLite compatibility test suite.
//!
//! This module provides infrastructure for running SQLite's TCL test suite
//! against rustql to measure compatibility and track progress.

pub mod progress;
pub mod runner;
pub mod rustql_adapter;
pub mod tcl_parser;

use std::fs;
use std::path::Path;

pub use runner::{SuiteStats, TestDatabase, TestFileStats, TestResult, TestRunner};
pub use tcl_parser::{parse_tcl_test_file, ParsedTestFile};

/// Default path to SQLite test files
pub const SQLITE_TEST_DIR: &str = "sqlite3/test";

/// List of priority test files to run (in order of importance)
pub const PRIORITY_TEST_FILES: &[&str] = &[
    "table.test",    // CREATE TABLE basics
    "select1.test",  // SELECT basics
    "insert.test",   // INSERT basics
    "update.test",   // UPDATE basics
    "delete.test",   // DELETE basics
    "expr.test",     // Expression evaluation
    "types.test",    // Type system
    "index.test",    // Index operations
    "join.test",     // JOIN operations
    "where.test",    // WHERE clause
    "orderby.test",  // ORDER BY
    "distinct.test", // DISTINCT
    "limit.test",    // LIMIT/OFFSET
    "null.test",     // NULL handling
    "coalesce.test", // COALESCE function
    "func.test",     // SQL functions
    "aggfunc.test",  // Aggregate functions
    "subquery.test", // Subqueries
    "view.test",     // Views
    "trigger.test",  // Triggers
    "trans.test",    // Transactions
    "fkey.test",     // Foreign keys
];

/// Load and parse a test file
pub fn load_test_file(path: &Path) -> Result<ParsedTestFile, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;

    Ok(parse_tcl_test_file(
        &content,
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown"),
    ))
}

/// List available test files
pub fn list_test_files(test_dir: &Path) -> Result<Vec<String>, String> {
    let mut files = Vec::new();

    if !test_dir.exists() {
        return Err(format!("Test directory not found: {}", test_dir.display()));
    }

    for entry in fs::read_dir(test_dir).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();

        if path.extension().and_then(|e| e.to_str()) == Some("test") {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                files.push(name.to_string());
            }
        }
    }

    files.sort();
    Ok(files)
}

/// Get test file statistics without running tests
pub fn analyze_test_file(path: &Path) -> Result<TestFileAnalysis, String> {
    let parsed = load_test_file(path)?;

    Ok(TestFileAnalysis {
        file: parsed.path.clone(),
        test_count: parsed.tests.len(),
        setup_count: parsed.setup_commands.len(),
        variable_count: parsed.variables.len(),
    })
}

/// Analysis of a test file (without running)
#[derive(Debug)]
pub struct TestFileAnalysis {
    pub file: String,
    pub test_count: usize,
    pub setup_count: usize,
    pub variable_count: usize,
}

/// Summary of all available tests
pub fn analyze_test_suite(test_dir: &Path) -> Result<TestSuiteAnalysis, String> {
    let files = list_test_files(test_dir)?;
    let mut analyses = Vec::new();
    let mut total_tests = 0;
    let mut errors = Vec::new();

    for file in &files {
        let path = test_dir.join(file);
        match analyze_test_file(&path) {
            Ok(analysis) => {
                total_tests += analysis.test_count;
                analyses.push(analysis);
            }
            Err(e) => {
                errors.push(format!("{}: {}", file, e));
            }
        }
    }

    Ok(TestSuiteAnalysis {
        file_count: analyses.len(),
        total_tests,
        analyses,
        parse_errors: errors,
    })
}

/// Analysis of the full test suite
#[derive(Debug)]
pub struct TestSuiteAnalysis {
    pub file_count: usize,
    pub total_tests: usize,
    pub analyses: Vec<TestFileAnalysis>,
    pub parse_errors: Vec<String>,
}

impl TestSuiteAnalysis {
    pub fn print_summary(&self) {
        println!("\nSQLite Test Suite Analysis");
        println!("{}", "=".repeat(60));
        println!("Total test files: {}", self.file_count);
        println!("Total test cases: {}", self.total_tests);
        println!("Parse errors: {}", self.parse_errors.len());

        if !self.parse_errors.is_empty() {
            println!("\nParse errors:");
            for err in &self.parse_errors {
                println!("  - {}", err);
            }
        }

        println!("\nTop 20 files by test count:");
        let mut sorted: Vec<_> = self.analyses.iter().collect();
        sorted.sort_by(|a, b| b.test_count.cmp(&a.test_count));

        for analysis in sorted.iter().take(20) {
            println!("  {:40} {:>5} tests", analysis.file, analysis.test_count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_parse_priority_file() {
        let test_dir = PathBuf::from(SQLITE_TEST_DIR);
        if !test_dir.exists() {
            // Skip if test dir not available
            return;
        }

        let path = test_dir.join("select1.test");
        if path.exists() {
            let parsed = load_test_file(&path).unwrap();
            assert!(!parsed.tests.is_empty(), "select1.test should have tests");
            println!("Parsed {} tests from select1.test", parsed.tests.len());
        }
    }
}
