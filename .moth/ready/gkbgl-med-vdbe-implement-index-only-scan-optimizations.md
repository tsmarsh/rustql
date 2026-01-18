# VDBE: Implement index-only scan optimizations

## Problem
When all needed columns are in an index, SQLite avoids accessing the table. RustQL always reads from table.

## SQLite Behavior
- Covering index scan skips table lookup
- EXPLAIN shows "COVERING INDEX" in query plan

## Required Changes
1. Detect when index contains all needed columns
2. Use alt_cursor/alt_map for index-only access
3. Skip table seek when possible

## Files
- `src/executor/wherecode.rs` - Index selection
- `src/vdbe/engine.rs` - Column opcode
