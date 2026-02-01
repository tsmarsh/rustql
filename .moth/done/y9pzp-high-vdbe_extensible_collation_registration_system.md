# VDBE: Extensible collation registration system

## Problem
Only BINARY, NOCASE, RTRIM collations supported. SQLite allows custom collation registration.

## SQLite API
```c
sqlite3_create_collation(db, "MYORDER", SQLITE_UTF8, NULL, myCompare);
```

## Current State
Collations hardcoded in `src/api/connection.rs`

## Required Changes
1. Collation registry in connection
2. API for registering custom collations
3. Collation needed callback support
