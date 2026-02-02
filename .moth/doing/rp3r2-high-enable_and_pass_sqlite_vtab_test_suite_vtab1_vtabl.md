# Enable And Pass SQLite Vtab Test Suite (vtab1)

## Progress

**Current Status**: 99/223 tests passing (44.4%)
**Starting Point**: ~58/223 tests passing (~26%)

## Changes Made

### Echo Module Transaction Callbacks
- Added xBegin, xSync, xCommit, xRollback callbacks with logging
- These are called at transaction boundaries for virtual tables

### xBestIndex/xFilter Logging
- Added xBestIndex callback logging with generated SQL
- Added xFilter callback logging with SQL and constraint values
- Log constraint values after SQL for test verification

### Constraint Value Binding
- Replace ? placeholders with actual values in filter SQL execution
- Use explicit column names instead of * in generated SQL
- Properly escape string values

### VDBE Transaction Support
- Added vtab_needs_sync flag to track vtab writes
- Set flag in VCreate on successful virtual table creation
- Call vtab sync_all/commit_all at Halt in auto-commit mode

### Column-to-Column Comparison Fix
- Skip pushing down WHERE constraints comparing two columns from same vtab
- Expressions like `a<b` are now evaluated post-filter by VDBE

## Known Remaining Issues

### VUpdate Not Implemented
- INSERT/UPDATE/DELETE on virtual tables not working
- VUpdate opcode handler is empty
- INSERT uses regular OpenWrite/Insert instead of VOpen/VUpdate

### Missing Modules
- echo_v2 module not registered
- wholenumber module not registered
- fts4 module (tests reference it but we only have fts3)

### MATCH Operator
- MATCH on non-FTS tables should error, currently doesn't
- Echo module converts MATCH to LIKE which is incorrect

### Test Infrastructure
- Some tests have TCL variable interpolation issues
- State leakage between tests (leftover tables)
- SQLITE_ERROR/SQLITE_DONE constants return 0 instead of strings

## Next Steps

1. Implement VUpdate opcode handler for vtab INSERT/UPDATE/DELETE
2. Fix INSERT compiler to emit VOpen/VUpdate for virtual tables
3. Consider registering echo_v2 variant if needed for tests
