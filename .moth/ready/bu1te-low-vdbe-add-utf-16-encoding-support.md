# VDBE: Add UTF-16 encoding support

## Problem
RustQL only supports UTF-8. SQLite supports UTF-8, UTF-16LE, UTF-16BE.

## SQLite Features
- PRAGMA encoding to set database encoding
- Automatic conversion between encodings
- UTF-16 optimized comparisons

## Required Changes
1. Add encoding flag to connection/database
2. Convert strings on storage/retrieval
3. Handle UTF-16 in collation functions
