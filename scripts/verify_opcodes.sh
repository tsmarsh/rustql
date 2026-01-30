#!/bin/bash
# Verify RustQL opcode inventory matches SQLite's vdbe.c opcodes
#
# This script extracts opcodes from both SQLite and RustQL and compares them.
# It accounts for:
# - OP_Noop and OP_Explain being handled by default: case in SQLite
# - AggStep0, MaxOpcode, Unused being RustQL-specific

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== SQLite/RustQL Opcode Parity Check ==="
echo ""

# Extract SQLite opcodes from vdbe.c case statements
sqlite_opcodes=$(grep -E "^case OP_" "$PROJECT_ROOT/sqlite3/src/vdbe.c" | sed 's/case OP_//' | sed 's/:.*//' | sort | uniq)
# Add Noop and Explain which are handled by default: case
sqlite_opcodes=$(echo -e "$sqlite_opcodes\nNoop\nExplain" | sort | uniq)

# Extract RustQL opcodes from ops.rs enum (handles both "Name," and "Name = N,")
# Filter out impl blocks and other non-opcode lines
rustql_opcodes=$(grep -E "^\s+[A-Z][a-zA-Z0-9]+\s*[,=]" "$PROJECT_ROOT/src/vdbe/ops.rs" | grep -v "impl\|Self\|{" | sed 's/[,=].*//' | sed 's/^\s*//' | sed 's/\s*$//' | sort | uniq)

# Known RustQL-specific opcodes (acceptable differences)
known_extra="AggStep0
MaxOpcode
Unused"

# Find missing opcodes (in SQLite but not in RustQL)
missing=$(comm -23 <(echo "$sqlite_opcodes") <(echo "$rustql_opcodes"))

# Find extra opcodes (in RustQL but not in SQLite), excluding known extras
extra_raw=$(comm -13 <(echo "$sqlite_opcodes") <(echo "$rustql_opcodes"))
extra=$(comm -23 <(echo "$extra_raw") <(echo "$known_extra"))

echo "SQLite opcodes: $(echo "$sqlite_opcodes" | wc -l | tr -d ' ')"
echo "RustQL opcodes: $(echo "$rustql_opcodes" | wc -l | tr -d ' ')"
echo ""

if [ -n "$missing" ]; then
    echo "❌ MISSING OPCODES (in SQLite but not RustQL):"
    echo "$missing" | sed 's/^/  - /'
    echo ""
    exit 1
fi

if [ -n "$extra" ]; then
    echo "⚠️  EXTRA OPCODES (in RustQL but not SQLite, excluding known):"
    echo "$extra" | sed 's/^/  - /'
    echo ""
fi

echo "✅ Opcode parity check passed!"
echo ""
echo "Known RustQL-specific opcodes (acceptable):"
echo "$known_extra" | sed 's/^/  - /'
