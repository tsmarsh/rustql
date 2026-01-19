# VDBE: Add per-connection memory accounting

## Problem
No tracking of memory usage per connection. SQLite tracks this for soft/hard heap limits.

## SQLite Features
- `sqlite3_memory_used()` - Current usage
- `sqlite3_memory_highwater()` - Peak usage
- Soft/hard heap limits

## Required Changes
1. Add memory counter to connection
2. Track allocations in Mem, cursors, sorters
3. Implement memory limit checking
