//! TCL test file parser for SQLite test suite.
//!
//! Parses SQLite's TCL-based test files and extracts test cases.

use std::collections::HashMap;

/// A single test case from a TCL test file.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TestCase {
    /// Test name (e.g., "select1-1.1")
    pub name: String,
    /// The TCL script to execute
    pub script: String,
    /// Expected result
    pub expected: String,
    /// Line number in source file
    pub line: usize,
    /// Whether this is a catchsql test (expects error)
    pub expects_error: bool,
}

/// A setup command (SQL executed outside of test cases)
#[derive(Debug, Clone)]
pub struct SetupCommand {
    /// The SQL to execute
    pub sql: String,
    /// Line number in source file
    pub line: usize,
}

/// Parsed content from a TCL test file.
#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct ParsedTestFile {
    /// File path
    pub path: String,
    /// Test cases
    pub tests: Vec<TestCase>,
    /// Setup commands (SQL executed between tests)
    pub setup_commands: Vec<SetupCommand>,
    /// Variables defined in the file
    pub variables: HashMap<String, String>,
    /// Parse errors/warnings
    pub warnings: Vec<String>,
}

/// Parser state
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq)]
enum ParserState {
    Normal,
    InDoTest,
    InExecSql,
    InCatchSql,
    InBraces(usize), // depth
}

/// Parse a TCL test file and extract test cases.
pub fn parse_tcl_test_file(content: &str, path: &str) -> ParsedTestFile {
    let mut result = ParsedTestFile {
        path: path.to_string(),
        ..Default::default()
    };

    let lines: Vec<&str> = content.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim();
        let line_num = i + 1;

        // Skip comments and empty lines
        if line.is_empty() || line.starts_with('#') {
            i += 1;
            continue;
        }

        // Handle variable assignment: set varname value
        if line.starts_with("set ") && !line.contains('[') {
            if let Some((name, value)) = parse_set_command(line) {
                result.variables.insert(name, value);
            }
            i += 1;
            continue;
        }

        // Handle do_test
        if line.starts_with("do_test ") || line.starts_with("do_execsql_test ") {
            let (test, consumed) = parse_do_test(&lines, i);
            if let Some(test) = test {
                result.tests.push(test);
            }
            i += consumed;
            continue;
        }

        // Handle standalone execsql (setup commands)
        if line.starts_with("execsql ") || line.starts_with("execsql{") {
            let (sql, consumed) = parse_execsql(&lines, i);
            if let Some(sql) = sql {
                result.setup_commands.push(SetupCommand {
                    sql,
                    line: line_num,
                });
            }
            i += consumed;
            continue;
        }

        // Handle catchsql outside of do_test
        if line.starts_with("catchsql ") || line.starts_with("catchsql{") {
            let (sql, consumed) = parse_catchsql(&lines, i);
            if let Some(sql) = sql {
                result.setup_commands.push(SetupCommand {
                    sql,
                    line: line_num,
                });
            }
            i += consumed;
            continue;
        }

        i += 1;
    }

    result
}

/// Parse a set command: set varname value
fn parse_set_command(line: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = line.splitn(3, ' ').collect();
    if parts.len() >= 3 && parts[0] == "set" {
        let name = parts[1].to_string();
        let value = parts[2..].join(" ");
        // Remove surrounding braces or quotes
        let value = value.trim_matches(|c| c == '{' || c == '}' || c == '"');
        Some((name, value.to_string()))
    } else {
        None
    }
}

/// Parse a do_test block
fn parse_do_test(lines: &[&str], start: usize) -> (Option<TestCase>, usize) {
    let first_line = lines[start].trim();

    // Extract test name
    let is_execsql_test = first_line.starts_with("do_execsql_test ");
    let prefix = if is_execsql_test {
        "do_execsql_test "
    } else {
        "do_test "
    };

    let after_prefix = &first_line[prefix.len()..];
    let name = extract_test_name(after_prefix);

    // Find the script (first braced block)
    let (script, script_end) = extract_braced_block(lines, start);
    if script.is_none() {
        return (None, 1);
    }
    let script = script.unwrap();

    // Find the expected result (second braced block) - may be on same line
    let (expected, expected_end) = find_next_braced_block(lines, script_end);
    let expected = expected.unwrap_or_default();

    // Determine if this expects an error
    let expects_error = script.contains("catchsql") || expected.starts_with("1 ");

    // For do_execsql_test, the script IS the SQL
    let script = if is_execsql_test {
        format!("execsql {{{}}}", script)
    } else {
        script
    };

    let test = TestCase {
        name,
        script,
        expected,
        line: start + 1,
        expects_error,
    };

    (Some(test), expected_end.saturating_sub(start).max(1))
}

/// Extract a test name from the line after do_test
fn extract_test_name(s: &str) -> String {
    let s = s.trim();
    // Name ends at first space or brace
    let end = s
        .find(|c: char| c.is_whitespace() || c == '{')
        .unwrap_or(s.len());
    s[..end].to_string()
}

/// Extract a braced block starting from a given line
/// Returns (content, next_line_to_search)
fn extract_braced_block(lines: &[&str], start: usize) -> (Option<String>, usize) {
    let mut i = start;
    let mut content = String::new();
    let mut depth = 0;
    let mut started = false;
    let char_pos = 0; // Track position within line for same-line blocks

    while i < lines.len() {
        let line = lines[i];
        let chars: Vec<char> = line.chars().collect();
        let start_pos = if i == start { char_pos } else { 0 };

        let mut j = start_pos;
        while j < chars.len() {
            let ch = chars[j];
            if ch == '{' {
                if started {
                    content.push(ch);
                }
                depth += 1;
                started = true;
            } else if ch == '}' {
                depth -= 1;
                if depth == 0 {
                    // Found end of block - return next position to search
                    // The next search should start right after this '}'
                    return (Some(content.trim().to_string()), i);
                }
                content.push(ch);
            } else if started && depth > 0 {
                content.push(ch);
            }
            j += 1;
        }

        if started && depth > 0 {
            content.push('\n');
        }

        i += 1;
    }

    (None, i)
}

/// Find next braced block, potentially on same line after previous block
fn find_next_braced_block(lines: &[&str], start_line: usize) -> (Option<String>, usize) {
    let mut i = start_line;

    while i < lines.len() {
        let line = lines[i];

        // Look for opening brace
        if let Some(brace_pos) = line.find('{') {
            // Check if this is after a closing brace (same line)
            let before_brace = &line[..brace_pos];
            if !before_brace.contains('{') || before_brace.matches('}').count() > 0 {
                // Found a new opening brace, extract the block
                return extract_braced_block_from_pos(lines, i, brace_pos);
            }
        }

        i += 1;
    }

    (None, i)
}

/// Extract braced block starting from specific position in line
fn extract_braced_block_from_pos(
    lines: &[&str],
    line_idx: usize,
    char_pos: usize,
) -> (Option<String>, usize) {
    let mut i = line_idx;
    let mut content = String::new();
    let mut depth = 0;
    let mut started = false;
    let mut first_line = true;

    while i < lines.len() {
        let line = lines[i];
        let chars: Vec<char> = line.chars().collect();
        let start_pos = if first_line { char_pos } else { 0 };
        first_line = false;

        let mut j = start_pos;
        while j < chars.len() {
            let ch = chars[j];
            if ch == '{' {
                if started {
                    content.push(ch);
                }
                depth += 1;
                started = true;
            } else if ch == '}' {
                depth -= 1;
                if depth == 0 {
                    return (Some(content.trim().to_string()), i + 1);
                }
                content.push(ch);
            } else if started && depth > 0 {
                content.push(ch);
            }
            j += 1;
        }

        if started && depth > 0 {
            content.push('\n');
        }

        i += 1;
    }

    (None, i)
}

/// Parse an execsql command
fn parse_execsql(lines: &[&str], start: usize) -> (Option<String>, usize) {
    let (block, end) = extract_braced_block(lines, start);
    (block, end.saturating_sub(start).max(1))
}

/// Parse a catchsql command
fn parse_catchsql(lines: &[&str], start: usize) -> (Option<String>, usize) {
    let (block, end) = extract_braced_block(lines, start);
    (block, end.saturating_sub(start).max(1))
}

/// Extract SQL from a test script.
/// Handles execsql {SQL} and catchsql {SQL} patterns.
pub fn extract_sql_from_script(script: &str) -> Vec<SqlCommand> {
    let mut commands = Vec::new();
    let mut i = 0;
    let chars: Vec<char> = script.chars().collect();

    while i < chars.len() {
        // Skip whitespace
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }

        if i >= chars.len() {
            break;
        }

        // Look for execsql or catchsql
        let remaining: String = chars[i..].iter().collect();

        if remaining.starts_with("execsql") {
            i += 7;
            // Skip whitespace
            while i < chars.len() && chars[i].is_whitespace() {
                i += 1;
            }
            // Extract braced content
            if i < chars.len() && chars[i] == '{' {
                let (sql, new_i) = extract_braced_content(&chars, i);
                commands.push(SqlCommand {
                    sql,
                    expects_error: false,
                });
                i = new_i;
            }
        } else if remaining.starts_with("catchsql") {
            i += 8;
            // Skip whitespace
            while i < chars.len() && chars[i].is_whitespace() {
                i += 1;
            }
            // Extract braced content
            if i < chars.len() && chars[i] == '{' {
                let (sql, new_i) = extract_braced_content(&chars, i);
                commands.push(SqlCommand {
                    sql,
                    expects_error: true,
                });
                i = new_i;
            }
        } else if remaining.starts_with("db eval") {
            // Handle "db eval {SQL}" pattern used in many tests
            i += 7;
            // Skip whitespace
            while i < chars.len() && chars[i].is_whitespace() {
                i += 1;
            }
            // Extract braced content
            if i < chars.len() && chars[i] == '{' {
                let (sql, new_i) = extract_braced_content(&chars, i);
                commands.push(SqlCommand {
                    sql,
                    expects_error: false,
                });
                i = new_i;
            }
        } else if remaining.starts_with("catch") && !remaining.starts_with("catchsql") {
            // Handle "catch {execsql {SQL}}" or "set v [catch {execsql {SQL}} msg]" pattern
            // Skip to the opening brace
            i += 5;
            while i < chars.len() && chars[i] != '{' {
                i += 1;
            }
            if i < chars.len() && chars[i] == '{' {
                // Extract the inner content
                let (inner, new_i) = extract_braced_content(&chars, i);
                i = new_i;
                // Now parse the inner content for execsql
                let inner_chars: Vec<char> = inner.chars().collect();
                let inner_remaining: String = inner_chars.iter().collect();
                if inner_remaining.starts_with("execsql") {
                    let mut j = 7;
                    while j < inner_chars.len() && inner_chars[j].is_whitespace() {
                        j += 1;
                    }
                    if j < inner_chars.len() && inner_chars[j] == '{' {
                        let (sql, _) = extract_braced_content(&inner_chars, j);
                        commands.push(SqlCommand {
                            sql,
                            expects_error: true,
                        });
                    }
                }
            }
        } else {
            i += 1;
        }
    }

    commands
}

/// A SQL command extracted from a test script
#[derive(Debug, Clone)]
pub struct SqlCommand {
    pub sql: String,
    pub expects_error: bool,
}

/// Extract content from braces
fn extract_braced_content(chars: &[char], start: usize) -> (String, usize) {
    let mut content = String::new();
    let mut depth = 0;
    let mut i = start;

    while i < chars.len() {
        let ch = chars[i];
        if ch == '{' {
            depth += 1;
            if depth > 1 {
                content.push(ch);
            }
        } else if ch == '}' {
            depth -= 1;
            if depth == 0 {
                return (content.trim().to_string(), i + 1);
            }
            content.push(ch);
        } else if depth > 0 {
            content.push(ch);
        }
        i += 1;
    }

    (content.trim().to_string(), i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_do_test() {
        // Note: Expected on separate line (common format in SQLite tests)
        let content = r#"
do_test select1-1.1 {
  execsql {SELECT * FROM test1}
} {
  11 22
}
"#;
        let parsed = parse_tcl_test_file(content, "test.test");
        assert_eq!(parsed.tests.len(), 1);
        assert_eq!(parsed.tests[0].name, "select1-1.1");
        assert_eq!(parsed.tests[0].expected, "11 22");
    }

    #[test]
    fn test_parse_execsql_test() {
        let content = r#"
do_execsql_test select1-1.4 {
  SELECT f1 FROM test1
} {11}
"#;
        let parsed = parse_tcl_test_file(content, "test.test");
        assert_eq!(parsed.tests.len(), 1);
        assert_eq!(parsed.tests[0].name, "select1-1.4");
    }

    #[test]
    fn test_parse_setup_command() {
        let content = r#"
execsql {CREATE TABLE test1(f1 int, f2 int)}
execsql {INSERT INTO test1(f1,f2) VALUES(11,22)}
"#;
        let parsed = parse_tcl_test_file(content, "test.test");
        assert_eq!(parsed.setup_commands.len(), 2);
        assert!(parsed.setup_commands[0].sql.contains("CREATE TABLE"));
    }

    #[test]
    fn test_extract_sql() {
        let script = "execsql {SELECT * FROM test1}";
        let commands = extract_sql_from_script(script);
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].sql, "SELECT * FROM test1");
        assert!(!commands[0].expects_error);
    }

    #[test]
    fn test_extract_catchsql() {
        let script = "catchsql {SELECT * FROM nonexistent}";
        let commands = extract_sql_from_script(script);
        assert_eq!(commands.len(), 1);
        assert!(commands[0].expects_error);
    }
}
