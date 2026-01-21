# Fix Compound SELECT Parsing

Parser and executor issues with UNION/EXCEPT/INTERSECT queries.

## Issues Identified
- `near "UNION": syntax error` - Multiple test failures
- `near "INTERSECT": syntax error` - select4 tests
- `near "EXCEPT": syntax error` - select4 tests  
- `near "VALUES": syntax error` - VALUES clause in compounds

## Test Failures
- select4-1.3, select4-1.4: Basic compound parsing
- select4-2.3, select4-2.4: UNION with ORDER BY
- select4-3.3: EXCEPT queries
- select4-4.3, select4-4.4: INTERSECT queries
- select4-11.16: Complex compound queries

## Root Causes
1. Parser may not handle all compound operator positions
2. ORDER BY in subqueries within compounds may fail
3. VALUES clause not fully supported in compound context

## Files to Investigate
- `src/parser/parser.rs` - Compound SELECT parsing
- `src/executor/select.rs` - Compound query execution
- `src/parser/ast.rs` - CompoundOp enum

## Test Commands
```bash
make test-select4
```
