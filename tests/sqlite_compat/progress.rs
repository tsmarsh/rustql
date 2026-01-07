//! Progress tracking for SQLite compatibility.
//!
//! This module provides a simple way to track and report compatibility progress.

use std::fs;
use std::path::Path;

/// Compatibility progress report
#[derive(Debug, Default)]
pub struct ProgressReport {
    pub total_test_files: usize,
    pub total_test_cases: usize,
    pub parsed_test_files: usize,
    pub parse_errors: usize,

    // Execution results (when available)
    pub tests_run: usize,
    pub tests_passed: usize,
    pub tests_failed: usize,
    pub tests_skipped: usize,
}

impl ProgressReport {
    pub fn compatibility_percentage(&self) -> f64 {
        if self.tests_run == 0 {
            0.0
        } else {
            (self.tests_passed as f64 / self.tests_run as f64) * 100.0
        }
    }

    pub fn print_report(&self) {
        println!("\n╔══════════════════════════════════════════════════════════╗");
        println!("║         RustQL - SQLite Compatibility Progress           ║");
        println!("╠══════════════════════════════════════════════════════════╣");
        println!("║                                                          ║");
        println!("║  Test Suite Coverage:                                    ║");
        println!(
            "║    Test files:    {:>6}                                 ║",
            self.total_test_files
        );
        println!(
            "║    Test cases:    {:>6}                                 ║",
            self.total_test_cases
        );
        println!(
            "║    Parse errors:  {:>6}                                 ║",
            self.parse_errors
        );
        println!("║                                                          ║");

        if self.tests_run > 0 {
            println!("║  Execution Results:                                      ║");
            println!(
                "║    Tests run:     {:>6}                                 ║",
                self.tests_run
            );
            println!(
                "║    Passed:        {:>6} ({:>5.1}%)                       ║",
                self.tests_passed,
                self.compatibility_percentage()
            );
            println!(
                "║    Failed:        {:>6}                                 ║",
                self.tests_failed
            );
            println!(
                "║    Skipped:       {:>6}                                 ║",
                self.tests_skipped
            );
            println!("║                                                          ║");
            println!("║  ┌──────────────────────────────────────────────────┐   ║");
            println!(
                "║  │ Compatibility: {:>5.1}%                           │   ║",
                self.compatibility_percentage()
            );
            println!("║  └──────────────────────────────────────────────────┘   ║");
        } else {
            println!("║  Execution: Not yet available                          ║");
            println!("║  (SELECT queries require VDBE completion)              ║");
        }

        println!("║                                                          ║");
        println!("╚══════════════════════════════════════════════════════════╝\n");
    }
}

/// Generate a progress report from analyzing the test suite
pub fn generate_progress_report(test_dir: &Path) -> ProgressReport {
    let mut report = ProgressReport::default();

    if !test_dir.exists() {
        return report;
    }

    // Count test files
    if let Ok(entries) = fs::read_dir(test_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("test") {
                report.total_test_files += 1;

                // Parse and count tests
                if let Ok(content) = fs::read_to_string(&path) {
                    let parsed = super::tcl_parser::parse_tcl_test_file(
                        &content,
                        path.file_name().and_then(|n| n.to_str()).unwrap_or(""),
                    );
                    report.total_test_cases += parsed.tests.len();
                    report.parsed_test_files += 1;
                } else {
                    report.parse_errors += 1;
                }
            }
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_report() {
        let report = ProgressReport {
            total_test_files: 1174,
            total_test_cases: 20875,
            parsed_test_files: 1174,
            parse_errors: 0,
            tests_run: 100,
            tests_passed: 25,
            tests_failed: 50,
            tests_skipped: 25,
        };

        assert!((report.compatibility_percentage() - 25.0).abs() < 0.01);
    }
}
