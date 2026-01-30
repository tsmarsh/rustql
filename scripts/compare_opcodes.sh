#!/bin/bash
# Compare RustQL and SQLite opcode emission for a query corpus
#
# This script runs queries through both SQLite EXPLAIN and RustQL's bytecode
# compiler, then compares the opcode sequences with normalization rules for
# acceptable differences.
#
# NOTE: This script requires RustQL's EXPLAIN statement to return bytecode rows.
# Currently, EXPLAIN is parsed but bytecode output is not yet implemented in
# the CLI. Use the Rust tests in tests/opcode_comparison.rs for programmatic
# comparison which accesses bytecode directly.
#
# Usage: ./scripts/compare_opcodes.sh [--verbose] [--query "SQL"]

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

VERBOSE=false
SINGLE_QUERY=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --query|-q)
            SINGLE_QUERY="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Build release if needed
if [ ! -f "$PROJECT_ROOT/target/release/rustql" ]; then
    echo "Building rustql..."
    cargo build --release --manifest-path "$PROJECT_ROOT/Cargo.toml"
fi

# Create temp directory for test databases
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# Test queries corpus
QUERIES=(
    "SELECT 1"
    "SELECT 1+1"
    "SELECT 1, 2, 3"
    "SELECT 'hello'"
    "SELECT NULL"
    "SELECT 1 WHERE 1=1"
    "SELECT 1 WHERE 0"
    "SELECT 1 UNION SELECT 2"
    "SELECT 1 ORDER BY 1"
    "SELECT CASE WHEN 1 THEN 'a' ELSE 'b' END"
)

# Schema for table tests
SCHEMA="CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c REAL);
INSERT INTO t1 VALUES(1, 'one', 1.0);
INSERT INTO t1 VALUES(2, 'two', 2.0);
INSERT INTO t1 VALUES(3, 'three', 3.0);"

TABLE_QUERIES=(
    "SELECT * FROM t1"
    "SELECT a FROM t1"
    "SELECT a, b FROM t1 WHERE a = 1"
    "SELECT * FROM t1 WHERE a > 1"
    "SELECT * FROM t1 ORDER BY b"
    "SELECT COUNT(*) FROM t1"
    "SELECT SUM(c) FROM t1"
    "SELECT a, COUNT(*) FROM t1 GROUP BY a"
)

# Extract opcodes from SQLite EXPLAIN output
extract_sqlite_opcodes() {
    local sql="$1"
    local db="$2"
    sqlite3 "$db" "EXPLAIN $sql" 2>/dev/null | awk '{print $2}' | grep -v '^$' || true
}

# Extract opcodes from RustQL (using internal test helper)
# For now, we'll create a separate test binary for this
extract_rustql_opcodes() {
    local sql="$1"
    local db="$2"
    # Use the test binary which outputs opcodes
    # Note: This will be implemented in Rust tests
    "$PROJECT_ROOT/target/release/rustql" "$db" "EXPLAIN $sql" 2>/dev/null | awk '{print $2}' | grep -v '^$' | grep -v '^opcode$' || true
}

# Normalize opcode sequence (remove acceptable differences)
normalize_opcodes() {
    local opcodes="$1"
    echo "$opcodes" | \
        # Remove Trace (debug-only in SQLite)
        grep -v '^Trace$' | \
        # Remove Explain (handled differently)
        grep -v '^Explain$' | \
        # Normalize Init jump targets (address-dependent)
        sed 's/^Init$/Init/' | \
        # Remove comments column if present
        awk '{print $1}'
}

# Compare two opcode sequences
compare_opcodes() {
    local sqlite_ops="$1"
    local rustql_ops="$2"
    local query="$3"

    local sqlite_norm=$(normalize_opcodes "$sqlite_ops")
    local rustql_norm=$(normalize_opcodes "$rustql_ops")

    if [ "$sqlite_norm" = "$rustql_norm" ]; then
        return 0
    else
        return 1
    fi
}

echo "=== RustQL/SQLite Opcode Comparison ==="
echo ""

# If single query mode
if [ -n "$SINGLE_QUERY" ]; then
    echo "Query: $SINGLE_QUERY"
    echo ""

    # Create test database
    DB="$TMPDIR/test.db"
    echo "$SCHEMA" | sqlite3 "$DB"

    echo "SQLite opcodes:"
    extract_sqlite_opcodes "$SINGLE_QUERY" "$DB"
    echo ""

    echo "RustQL opcodes:"
    extract_rustql_opcodes "$SINGLE_QUERY" "$DB"
    echo ""

    exit 0
fi

# Run corpus tests
PASSED=0
FAILED=0
SKIPPED=0

# Test simple queries (no schema)
echo "Testing simple queries..."
DB="$TMPDIR/simple.db"
sqlite3 "$DB" "SELECT 1" > /dev/null  # Create empty DB

for query in "${QUERIES[@]}"; do
    sqlite_ops=$(extract_sqlite_opcodes "$query" "$DB")
    rustql_ops=$(extract_rustql_opcodes "$query" "$DB")

    if [ -z "$sqlite_ops" ]; then
        if $VERBOSE; then
            echo "  SKIP: $query (SQLite error)"
        fi
        ((SKIPPED++))
        continue
    fi

    if compare_opcodes "$sqlite_ops" "$rustql_ops" "$query"; then
        if $VERBOSE; then
            echo "  PASS: $query"
        fi
        ((PASSED++))
    else
        echo "  FAIL: $query"
        if $VERBOSE; then
            echo "    SQLite: $(echo "$sqlite_ops" | tr '\n' ' ')"
            echo "    RustQL: $(echo "$rustql_ops" | tr '\n' ' ')"
        fi
        ((FAILED++))
    fi
done

# Test table queries
echo ""
echo "Testing table queries..."
DB="$TMPDIR/tables.db"
echo "$SCHEMA" | sqlite3 "$DB"

for query in "${TABLE_QUERIES[@]}"; do
    sqlite_ops=$(extract_sqlite_opcodes "$query" "$DB")
    rustql_ops=$(extract_rustql_opcodes "$query" "$DB")

    if [ -z "$sqlite_ops" ]; then
        if $VERBOSE; then
            echo "  SKIP: $query (SQLite error)"
        fi
        ((SKIPPED++))
        continue
    fi

    if compare_opcodes "$sqlite_ops" "$rustql_ops" "$query"; then
        if $VERBOSE; then
            echo "  PASS: $query"
        fi
        ((PASSED++))
    else
        echo "  FAIL: $query"
        if $VERBOSE; then
            echo "    SQLite: $(echo "$sqlite_ops" | tr '\n' ' ')"
            echo "    RustQL: $(echo "$rustql_ops" | tr '\n' ' ')"
        fi
        ((FAILED++))
    fi
done

echo ""
echo "=== Results ==="
echo "Passed:  $PASSED"
echo "Failed:  $FAILED"
echo "Skipped: $SKIPPED"
echo ""

if [ $FAILED -eq 0 ]; then
    echo "All opcode comparisons passed!"
    exit 0
else
    echo "Some opcode comparisons failed."
    exit 1
fi
