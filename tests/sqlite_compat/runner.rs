//! Test runner for SQLite compatibility tests.
//!
//! Executes parsed TCL tests against rustql and tracks results.

use super::tcl_parser::{extract_sql_from_script, ParsedTestFile, SetupCommand, TestCase};
use std::collections::HashMap;

/// Result of running a single test
#[derive(Debug, Clone)]
pub struct TestResult {
    pub name: String,
    pub passed: bool,
    pub expected: String,
    pub actual: String,
    pub error: Option<String>,
    pub line: usize,
    /// Test was skipped (feature not implemented)
    pub skipped: bool,
    pub skip_reason: Option<String>,
}

/// Statistics for a test file
#[derive(Debug, Default, Clone)]
pub struct TestFileStats {
    pub file: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub errors: usize,
}

impl TestFileStats {
    pub fn pass_rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.passed as f64 / self.total as f64) * 100.0
        }
    }
}

/// Overall test suite statistics
#[derive(Debug, Default)]
pub struct SuiteStats {
    pub files: Vec<TestFileStats>,
    pub total_tests: usize,
    pub total_passed: usize,
    pub total_failed: usize,
    pub total_skipped: usize,
    pub total_errors: usize,
}

impl SuiteStats {
    pub fn add_file(&mut self, stats: TestFileStats) {
        self.total_tests += stats.total;
        self.total_passed += stats.passed;
        self.total_failed += stats.failed;
        self.total_skipped += stats.skipped;
        self.total_errors += stats.errors;
        self.files.push(stats);
    }

    pub fn pass_rate(&self) -> f64 {
        if self.total_tests == 0 {
            0.0
        } else {
            (self.total_passed as f64 / self.total_tests as f64) * 100.0
        }
    }

    pub fn print_summary(&self) {
        println!("\n{}", "=".repeat(60));
        println!("SQLite Compatibility Test Summary");
        println!("{}\n", "=".repeat(60));

        println!("Per-file results:");
        println!(
            "{:<40} {:>6} {:>6} {:>6} {:>6} {:>7}",
            "File", "Total", "Pass", "Fail", "Skip", "Rate"
        );
        println!(
            "{:-<40} {:-<6} {:-<6} {:-<6} {:-<6} {:-<7}",
            "", "", "", "", "", ""
        );

        for stats in &self.files {
            println!(
                "{:<40} {:>6} {:>6} {:>6} {:>6} {:>6.1}%",
                stats.file,
                stats.total,
                stats.passed,
                stats.failed,
                stats.skipped,
                stats.pass_rate()
            );
        }

        println!("\n{}", "-".repeat(60));
        println!(
            "Overall: {} tests, {} passed, {} failed, {} skipped",
            self.total_tests, self.total_passed, self.total_failed, self.total_skipped
        );
        println!("Pass rate: {:.1}%", self.pass_rate());
        println!("{}\n", "=".repeat(60));
    }
}

/// Database connection abstraction for testing
pub trait TestDatabase {
    /// Execute SQL and return results as a list of strings
    fn exec_sql(&mut self, sql: &str) -> Result<Vec<String>, String>;

    /// Execute SQL expecting an error, return (error_code, message) or Ok(results)
    fn catch_sql(&mut self, sql: &str) -> Result<CatchResult, String>;

    /// Close and reopen the database
    fn reopen(&mut self) -> Result<(), String>;

    /// Get the database path
    fn path(&self) -> &str;
}

/// Result of catchsql
#[derive(Debug)]
pub enum CatchResult {
    /// SQL succeeded: {0 result}
    Success(Vec<String>),
    /// SQL failed: {1 error_message}
    Error(String),
}

impl CatchResult {
    pub fn to_tcl_result(&self) -> String {
        match self {
            CatchResult::Success(vals) => format!("0 {}", vals.join(" ")),
            CatchResult::Error(msg) => format!("1 {{{}}}", msg),
        }
    }
}

/// Test runner
pub struct TestRunner<D: TestDatabase> {
    db: D,
    variables: HashMap<String, String>,
    verbose: bool,
}

impl<D: TestDatabase> TestRunner<D> {
    pub fn new(db: D) -> Self {
        Self {
            db,
            variables: HashMap::new(),
            verbose: false,
        }
    }

    pub fn set_verbose(&mut self, verbose: bool) {
        self.verbose = verbose;
    }

    /// Run all tests from a parsed test file
    pub fn run_file(&mut self, parsed: &ParsedTestFile) -> (TestFileStats, Vec<TestResult>) {
        let mut stats = TestFileStats {
            file: parsed.path.clone(),
            ..Default::default()
        };
        let mut results = Vec::new();

        // Merge variables
        for (k, v) in &parsed.variables {
            self.variables.insert(k.clone(), v.clone());
        }

        // Track setup command index
        let mut setup_idx = 0;

        for test in &parsed.tests {
            // Run any setup commands that come before this test
            while setup_idx < parsed.setup_commands.len()
                && parsed.setup_commands[setup_idx].line < test.line
            {
                let setup = &parsed.setup_commands[setup_idx];
                if let Err(e) = self.run_setup(setup) {
                    if self.verbose {
                        eprintln!("Setup error at line {}: {}", setup.line, e);
                    }
                }
                setup_idx += 1;
            }

            // Run the test
            let result = self.run_test(test);
            stats.total += 1;

            if result.skipped {
                stats.skipped += 1;
            } else if result.passed {
                stats.passed += 1;
            } else if result.error.is_some() {
                stats.errors += 1;
                stats.failed += 1;
            } else {
                stats.failed += 1;
            }

            if self.verbose && !result.passed && !result.skipped {
                eprintln!(
                    "FAIL {}: expected {:?}, got {:?}",
                    result.name, result.expected, result.actual
                );
                if let Some(ref err) = result.error {
                    eprintln!("  Error: {}", err);
                }
            }

            results.push(result);
        }

        // Run remaining setup commands
        while setup_idx < parsed.setup_commands.len() {
            let setup = &parsed.setup_commands[setup_idx];
            if let Err(e) = self.run_setup(setup) {
                if self.verbose {
                    eprintln!("Setup error at line {}: {}", setup.line, e);
                }
            }
            setup_idx += 1;
        }

        (stats, results)
    }

    /// Run a setup command
    fn run_setup(&mut self, setup: &SetupCommand) -> Result<(), String> {
        // Substitute variables
        let sql = self.substitute_variables(&setup.sql);

        // Execute SQL
        self.db.exec_sql(&sql).map(|_| ())
    }

    /// Run a single test case
    fn run_test(&mut self, test: &TestCase) -> TestResult {
        // Check for skip conditions
        if let Some(reason) = self.should_skip(test) {
            return TestResult {
                name: test.name.clone(),
                passed: false,
                expected: test.expected.clone(),
                actual: String::new(),
                error: None,
                line: test.line,
                skipped: true,
                skip_reason: Some(reason),
            };
        }

        // Extract SQL commands from script
        let commands = extract_sql_from_script(&test.script);

        let mut actual_results: Vec<String> = Vec::new();
        let mut error: Option<String> = None;

        for cmd in &commands {
            let sql = self.substitute_variables(&cmd.sql);

            if cmd.expects_error {
                match self.db.catch_sql(&sql) {
                    Ok(CatchResult::Success(vals)) => {
                        actual_results.push("0".to_string());
                        actual_results.extend(vals);
                    }
                    Ok(CatchResult::Error(msg)) => {
                        actual_results.push("1".to_string());
                        actual_results.push(msg);
                    }
                    Err(e) => {
                        error = Some(e);
                        break;
                    }
                }
            } else {
                match self.db.exec_sql(&sql) {
                    Ok(vals) => {
                        actual_results.extend(vals);
                    }
                    Err(e) => {
                        error = Some(e);
                        break;
                    }
                }
            }
        }

        let actual = actual_results.join(" ");
        let expected = self.normalize_expected(&test.expected);
        let passed = error.is_none() && self.compare_results(&actual, &expected);

        TestResult {
            name: test.name.clone(),
            passed,
            expected,
            actual,
            error,
            line: test.line,
            skipped: false,
            skip_reason: None,
        }
    }

    /// Check if a test should be skipped
    fn should_skip(&self, test: &TestCase) -> Option<String> {
        // Skip tests that use features not yet implemented
        let unimplemented = [
            "ifcapable",
            "db close",
            "sqlite3 ",
            "file delete",
            "file exists",
            "btree_",
            "sqlite3_",
        ];

        for feature in &unimplemented {
            if test.script.contains(feature) {
                return Some(format!("Uses unimplemented feature: {}", feature));
            }
        }

        None
    }

    /// Substitute variables in a string
    fn substitute_variables(&self, s: &str) -> String {
        let mut result = s.to_string();
        for (k, v) in &self.variables {
            result = result.replace(&format!("${}", k), v);
            result = result.replace(&format!("$::{}", k), v);
        }
        result
    }

    /// Normalize expected result for comparison
    fn normalize_expected(&self, expected: &str) -> String {
        // Handle TCL list format: {item1} {item2} -> item1 item2
        let mut result = expected.to_string();

        // Remove outer braces
        result = result.trim().to_string();
        if result.starts_with('{') && result.ends_with('}') {
            // Check if it's a single braced item
            let inner = &result[1..result.len() - 1];
            if !inner.contains('{') {
                result = inner.to_string();
            }
        }

        // Normalize whitespace
        result.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    /// Compare actual and expected results
    fn compare_results(&self, actual: &str, expected: &str) -> bool {
        let actual_norm: Vec<&str> = actual.split_whitespace().collect();
        let expected_norm: Vec<&str> = expected.split_whitespace().collect();
        actual_norm == expected_norm
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockDb {
        results: Vec<Vec<String>>,
        call_count: usize,
    }

    impl TestDatabase for MockDb {
        fn exec_sql(&mut self, _sql: &str) -> Result<Vec<String>, String> {
            if self.call_count < self.results.len() {
                let result = self.results[self.call_count].clone();
                self.call_count += 1;
                Ok(result)
            } else {
                Ok(vec![])
            }
        }

        fn catch_sql(&mut self, sql: &str) -> Result<CatchResult, String> {
            match self.exec_sql(sql) {
                Ok(vals) => Ok(CatchResult::Success(vals)),
                Err(e) => Ok(CatchResult::Error(e)),
            }
        }

        fn reopen(&mut self) -> Result<(), String> {
            Ok(())
        }

        fn path(&self) -> &str {
            "test.db"
        }
    }

    #[test]
    fn test_stats_pass_rate() {
        let stats = TestFileStats {
            file: "test.test".to_string(),
            total: 10,
            passed: 8,
            failed: 2,
            skipped: 0,
            errors: 0,
        };
        assert!((stats.pass_rate() - 80.0).abs() < 0.001);
    }

    #[test]
    fn test_catch_result_format() {
        let success = CatchResult::Success(vec!["1".to_string(), "2".to_string()]);
        assert_eq!(success.to_tcl_result(), "0 1 2");

        let error = CatchResult::Error("no such table".to_string());
        assert_eq!(error.to_tcl_result(), "1 {no such table}");
    }
}
