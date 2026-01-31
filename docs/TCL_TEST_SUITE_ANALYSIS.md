# SQLite TCL Test Suite Analysis

This document maps the interdependencies of the SQLite TCL test suite and the features required by each test. It serves as a reference for understanding test requirements and prioritizing implementation work.

## Table of Contents

1. [Test Suite Overview](#test-suite-overview)
2. [Feature Capability System](#feature-capability-system)
3. [RustQL Feature Configuration](#rustql-feature-configuration)
4. [Test Categories and Dependencies](#test-categories-and-dependencies)
5. [Inter-Test Dependencies](#inter-test-dependencies)
6. [Test Execution Patterns](#test-execution-patterns)
7. [Feature-to-Test Mapping](#feature-to-test-mapping)

---

## Test Suite Overview

The SQLite TCL test suite consists of **1,176 test files** in `sqlite3/test/`. The RustQL Makefile currently runs a subset of **47 tests** that are most relevant to core SQL functionality.

### Test File Categories

| Category | Count | Description |
|----------|-------|-------------|
| `tkt*.test` | 82 | Ticket/bug fix regression tests |
| `select*.test` | 9 | SELECT statement tests |
| `where*.test` | 9 | WHERE clause optimization tests |
| `window*.test` | 9 | Window function tests |
| `wal*.test` | 9 | Write-Ahead Logging tests |
| `vtab*.test` | 9 | Virtual table tests |
| `trigger*.test` | 9 | Trigger tests |
| `join*.test` | 9 | JOIN operation tests |
| `index*.test` | 9 | Index tests |
| `func*.test` | 9 | Function tests |
| `corrupt*.test` | 9 | Corruption handling tests |
| `collate*.test` | 9 | Collation tests |
| `fts*.test` | 20+ | Full-text search tests |
| `json*.test` | 10 | JSON function tests |
| Other | 900+ | Various specialized tests |

### Tests Currently Run by RustQL

```
select1 select2 select3 select4 select5 select6 select7
insert insert2 insert3
update delete
expr
where where2 where3
join join2 join3
subquery
trigger trigger2
view
index index2
null
types types2 types3
cast coalesce between distinct limit
orderby1
func func2 func3
date printf
like like2 glob
attach vacuum
pragma pragma2
trans trans2
savepoint
collate1 collate2 collate3
```

---

## Feature Capability System

SQLite tests use `ifcapable` to conditionally run tests based on compile-time features. The syntax is:

```tcl
ifcapable feature {
    # Code runs if feature is enabled
}

ifcapable !feature {
    # Code runs if feature is disabled
}

ifcapable {feature1 && feature2} {
    # Code runs if both features are enabled
}
```

### Most Commonly Required Features

| Feature | Usage Count | Description |
|---------|-------------|-------------|
| `subquery` | 109+ | Subquery support |
| `compound` | 47+ | UNION/INTERSECT/EXCEPT |
| `tempdb` | 52 | Temporary database support |
| `view` | 40+ | VIEW support |
| `trigger` | 24+ | TRIGGER support |
| `altertable` | 38 | ALTER TABLE support |
| `attach` | 35 | ATTACH DATABASE support |
| `vtab` | 58 | Virtual table support |
| `fts3` | 15+ | FTS3 full-text search |
| `fts5` | 12 | FTS5 full-text search |
| `floatingpoint` | 27 | Floating-point support |
| `explain` | 14+ | EXPLAIN support |
| `conflict` | 12 | ON CONFLICT support |
| `bloblit` | 12 | Blob literal (X'...') support |
| `autovacuum` | 12 | Auto-vacuum support |
| `wal` | 11+ | Write-ahead logging |
| `utf16` | 28+ | UTF-16 encoding support |
| `rtree` | 12 | R-Tree extension |
| `icu` | 9 | ICU collation support |
| `stat4` | 21 | SQLITE_STAT4 statistics |
| `vacuum` | 17 | VACUUM support |
| `schema_pragmas` | 16 | Schema PRAGMAs |
| `pager_pragmas` | 14 | Pager PRAGMAs |
| `integrityck` | 9+ | PRAGMA integrity_check |
| `shared_cache` | 8+ | Shared cache mode |
| `incrblob` | 8+ | Incremental blob I/O |
| `memorymanage` | 10 | Memory management |
| `tclvar` | 9 | TCL variable binding |
| `datetime` | varies | Date/time functions |
| `cast` | varies | CAST expressions |

---

## RustQL Feature Configuration

RustQL configures SQLite capabilities in `scripts/run_sqlite_test.tcl`:

### Enabled Features (value = 1)

```
autovacuum          compound            trigger             view
subquery            memorydb            attach              progress
vacuum              tempdb              integrityck         conflict
schema              foreignkey          datetime            pager_pragmas
utf16               tcl                 windowfunc          json/json1
fts3                fts5                rtree               wal
lookaside           threadsafe          shared_cache        stat4
secure_delete       cursorhint          diskio              explain
bloblit             check               authorization       columncount
complete            hexlit              like                or_opt
reindex             trace               pragma              floatingpoint
autoinc             maxexpr             cast                altertable
schema_pragmas      like_opt            between_opt         schema_version
default_cache_size  encoding            wsd                 oversize_cell_check
savepoint           system_malloc
```

### Disabled Features (value = 0)

```
incrblob            builtin_test        memdebug           lock_proxy_pragmas
long_double         mem5                casesensitivelike  debug
update_delete_limit hidden_columns      crashtest          tclvar
icu                 deprecated          direct_read        legacyformat
configslower        rowid32             cursorhints        uri
analyze             autoindex           cte                memorymanage
like_match_blobs    vtab                auth               worker_threads
load_ext            tempdb_in_memory    default_temp_store localtime
malloc_usable_size  mmap_size           offset_sql_func    pagecache_overflow_stats
preupdate           session             snapshot           sorter_reference_size
stat3               unlock_notify       userauth           win32heap
yytrackmaxstackdepth
```

### Key Missing Features

These disabled features affect many tests:

| Feature | Impact | Notes |
|---------|--------|-------|
| `vtab` | High | Virtual tables not implemented |
| `cte` | Medium | Common Table Expressions (WITH clause) |
| `analyze` | Medium | ANALYZE statement |
| `autoindex` | Low | Automatic index creation |
| `tclvar` | Low | TCL variable binding ($var) |
| `incrblob` | Low | Incremental blob I/O |

---

## Test Categories and Dependencies

### SELECT Tests

| Test | Features Required | Tables Created | Notes |
|------|-------------------|----------------|-------|
| `select1` | compound, subquery | test1, test2, test3, t4 | Core SELECT, ORDER BY, LIMIT |
| `select2` | tclvar | t1 | More SELECT variations |
| `select3` | (none) | t1, t2 | Aggregate functions |
| `select4` | compound, subquery | test1, test2 | UNION, INTERSECT, EXCEPT |
| `select5` | (none) | t1, t2 | GROUP BY, HAVING |
| `select6` | subquery, explain, view | t1 | Subquery optimization |
| `select7` | compound, tempdb, view, subquery | t1, t2 | Complex queries |

### INSERT Tests

| Test | Features Required | Tables Created | Notes |
|------|-------------------|----------------|-------|
| `insert` | reindex, subquery, tempdb, explain, conflict, compound | t1, test1 | Basic INSERT |
| `insert2` | explain, compound, subquery, tempdb, fts3 | t1, t2 | INSERT variations |
| `insert3` | trigger, compound, bloblit | t1, t2 | INSERT with triggers |

### UPDATE/DELETE Tests

| Test | Features Required | Tables Created | Notes |
|------|-------------------|----------------|-------|
| `update` | subquery, trigger, altertable | t1 | UPDATE statement |
| `delete` | explain, trigger | t1, t2 | DELETE statement |

### WHERE Tests

| Test | Features Required | Tables Created | Notes |
|------|-------------------|----------------|-------|
| `where` | subquery, vtab | t1, t2, t3 | WHERE clause basics |
| `where2` | tclvar, subquery, compound, explain, or_opt | t1 | WHERE optimization |
| `where3` | explain, cursorhints | t1, t2, t3 | More WHERE tests |

### JOIN Tests

| Test | Features Required | Tables Created | Notes |
|------|-------------------|----------------|-------|
| `join` | subquery, view, compound | t1, t2 | Basic JOINs |
| `join2` | subquery | t1, t2 | JOIN variations |
| `join3` | subquery, compound | t1, t2, t3 | Complex JOINs |

### Expression Tests

| Test | Features Required | Tables Created | Notes |
|------|-------------------|----------------|-------|
| `expr` | floatingpoint, cast, subquery, datetime | t1 | Expression evaluation |

### Type/Cast Tests

| Test | Features Required | Tables Created | Notes |
|------|-------------------|----------------|-------|
| `types` | (varies) | t1 | Type affinity |
| `types2` | (varies) | t1 | More type tests |
| `types3` | (varies) | t1 | Type coercion |
| `cast` | cast | t1 | CAST expressions |

### Function Tests

| Test | Features Required | Tables Created | Notes |
|------|-------------------|----------------|-------|
| `func` | floatingpoint | t1 | Core functions |
| `func2` | (varies) | t1 | More functions |
| `func3` | windowfunc | t1 | Window functions |
| `date` | datetime | t1 | Date/time functions |
| `printf` | (none) | (none) | printf function |

### String Pattern Tests

| Test | Features Required | Tables Created | Notes |
|------|-------------------|----------------|-------|
| `like` | like, like_opt | t1 | LIKE operator |
| `like2` | like | t1 | More LIKE tests |
| `glob` | (none) | t1 | GLOB operator |

### Transaction Tests

| Test | Features Required | Tables Created | Notes |
|------|-------------------|----------------|-------|
| `trans` | (none) | t1, t2 | BEGIN/COMMIT/ROLLBACK |
| `trans2` | (none) | t1 | Transaction edge cases |
| `savepoint` | savepoint | t1 | SAVEPOINT support |

### Schema Tests

| Test | Features Required | Tables Created | Notes |
|------|-------------------|----------------|-------|
| `view` | view, trigger | t1, v1 | VIEW support |
| `trigger` | trigger | t1, log | TRIGGER basics |
| `trigger2` | trigger, subquery | t1, log | More trigger tests |
| `index` | (none) | t1, t2 | INDEX creation |
| `index2` | (none) | t1 | INDEX variations |
| `attach` | attach | t1 | ATTACH DATABASE |
| `vacuum` | vacuum | t1 | VACUUM statement |

### PRAGMA Tests

| Test | Features Required | Tables Created | Notes |
|------|-------------------|----------------|-------|
| `pragma` | pragma, schema_pragmas | t1 | Basic PRAGMAs |
| `pragma2` | pager_pragmas | t1 | More PRAGMAs |

### Collation Tests

| Test | Features Required | Tables Created | Notes |
|------|-------------------|----------------|-------|
| `collate1` | (none) | t1 | COLLATE basics |
| `collate2` | (none) | t1, t2 | COLLATE variations |
| `collate3` | (none) | t1 | More collation |

---

## Inter-Test Dependencies

### Database State Dependencies

Tests within the same file often depend on tables created by earlier tests:

```
select1:
  - test1 created in select1-1.0, used through select1-1.x
  - test2 created in select1-2.1, used through select1-2.x
  - reset_db called periodically to clear state

insert:
  - t1 created early, modified throughout
  - test1 created for specific test groups
```

### Reset Points

Tests use `reset_db` to clear database state. This creates logical test sections:

```tcl
# Group 1: tests on original schema
do_test test-1.1 { ... }
do_test test-1.2 { ... }

reset_db  # Clear everything

# Group 2: tests on fresh schema
do_test test-2.1 { ... }
```

### Feature Dependency Graph

```
compound ─────┬───> select1, select4, select6, select7
              ├───> insert, insert2, insert3
              ├───> join, join3
              └───> where2

subquery ─────┬───> select1, select4, select6, select7
              ├───> insert, insert2, update
              ├───> where, where2
              ├───> join, join2, join3
              ├───> expr
              └───> trigger2

view ─────────┬───> select6, select7
              ├───> join
              └───> view (primary)

trigger ──────┬───> insert3, update, delete
              ├───> trigger, trigger2
              └───> view

tempdb ───────┬───> select7
              ├───> insert, insert2
              └───> (many tests)

explain ──────┬───> insert, insert2, delete
              ├───> select6
              └───> where2, where3

floatingpoint ┬───> expr, func
              └───> (math operations)
```

---

## Test Execution Patterns

### Standard Test Structure

```tcl
# 1. Setup
set testdir [file dirname $argv0]
source $testdir/tester.tcl

# 2. Feature checks
ifcapable !required_feature {
    finish_test
    return
}

# 3. Test groups
do_test testname-1.1 {
    execsql {
        CREATE TABLE t1(a, b, c);
        INSERT INTO t1 VALUES(1, 2, 3);
    }
} {}

do_test testname-1.2 {
    execsql {SELECT * FROM t1}
} {1 2 3}

# 4. Conditional tests
ifcapable compound {
    do_test testname-2.1 {
        execsql {SELECT 1 UNION SELECT 2}
    } {1 2}
}

# 5. Reset for new group
reset_db

# 6. More tests...

# 7. Cleanup
finish_test
```

### Test Result Formats

| Function | Result Format |
|----------|---------------|
| `do_test` | TCL list of values |
| `do_execsql_test` | Space-separated values |
| `do_catchsql_test` | `{error_code error_message}` |
| `execsql2` | `{col1 val1 col2 val2 ...}` |

---

## Feature-to-Test Mapping

### High-Priority Features (affect many tests)

| Feature | Tests Affected | Status |
|---------|----------------|--------|
| `compound` | select1,4,6,7 insert1,2,3 join,3 where2 | Enabled |
| `subquery` | select1,4,6,7 insert1,2 update where1,2 join1,2,3 expr trigger2 | Enabled |
| `view` | select6,7 join view | Enabled |
| `trigger` | insert3 update delete trigger1,2 view | Enabled |
| `explain` | insert1,2 delete select6 where2,3 | Enabled |
| `floatingpoint` | expr func | Enabled |

### Medium-Priority Features

| Feature | Tests Affected | Status |
|---------|----------------|--------|
| `tempdb` | select7 insert1,2 | Enabled |
| `altertable` | update | Enabled |
| `conflict` | insert | Enabled |
| `bloblit` | insert3 | Enabled |
| `datetime` | expr date | Enabled |
| `cast` | expr cast | Enabled |
| `savepoint` | savepoint | Enabled |

### Low-Priority Features (currently disabled)

| Feature | Tests Affected | Notes |
|---------|----------------|-------|
| `vtab` | where (skipped sections) | Virtual tables |
| `tclvar` | select2 where2 | $variable binding |
| `cte` | (would affect WITH clause) | Not implemented |
| `analyze` | (ANALYZE stmt) | Not implemented |
| `incrblob` | (blob I/O tests) | Not implemented |

---

## Recommendations

### For Improving Test Pass Rate

1. **Focus on core features first**: compound, subquery, view, trigger
2. **Fix error message formatting**: Many tests fail on message differences
3. **Implement missing pragmas**: table_info for views, etc.
4. **View flattening optimization**: Not required for correctness but affects some tests

### For Adding New Tests

1. Check feature requirements with `ifcapable`
2. Look for `reset_db` boundaries to understand state dependencies
3. Verify RustQL has the feature enabled in `sqlite_options`

### Test Execution Order

Tests within a file should be run in order. Tests across files are independent (each gets fresh database).

---

## Appendix: Complete sqlite_options Reference

```tcl
# Enabled in RustQL
autovacuum=1 compound=1 trigger=1 view=1 subquery=1 memorydb=1
attach=1 progress=1 vacuum=1 tempdb=1 integrityck=1 conflict=1
schema=1 foreignkey=1 datetime=1 pager_pragmas=1 utf16=1 tcl=1
windowfunc=1 json=1 json1=1 fts3=1 fts5=1 rtree=1 wal=1
lookaside=1 threadsafe=1 shared_cache=1 stat4=1 secure_delete=1
cursorhint=1 diskio=1 explain=1 bloblit=1 check=1 authorization=1
columncount=1 complete=1 hexlit=1 like=1 or_opt=1 reindex=1
trace=1 pragma=1 floatingpoint=1 autoinc=1 maxexpr=1 cast=1
altertable=1 schema_pragmas=1 like_opt=1 between_opt=1
schema_version=1 default_cache_size=1 encoding=1 wsd=1
oversize_cell_check=1 savepoint=1 system_malloc=1

# Disabled in RustQL
incrblob=0 builtin_test=0 memdebug=0 lock_proxy_pragmas=0
long_double=0 mem5=0 casesensitivelike=0 debug=0
update_delete_limit=0 hidden_columns=0 crashtest=0 tclvar=0
icu=0 deprecated=0 direct_read=0 legacyformat=0 configslower=0
rowid32=0 cursorhints=0 uri=0 analyze=0 autoindex=0 cte=0
memorymanage=0 like_match_blobs=0 vtab=0 auth=0 worker_threads=0
load_ext=0 tempdb_in_memory=0 default_temp_store=0 localtime=0
malloc_usable_size=0 mmap_size=0 offset_sql_func=0
pagecache_overflow_stats=0 preupdate=0 session=0 snapshot=0
sorter_reference_size=0 stat3=0 unlock_notify=0 userauth=0
win32heap=0 yytrackmaxstackdepth=0
```
