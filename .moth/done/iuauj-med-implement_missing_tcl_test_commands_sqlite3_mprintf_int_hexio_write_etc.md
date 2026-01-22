# Implement Missing TCL Test Commands

Missing TCL commands needed to run the SQLite test suite.

## High Priority (blocking many tests)
- `sqlite3_mprintf_int` (116 occurrences) - Format integers for testing
- `sqlite3_mprintf_double` (18 occurrences) - Format doubles for testing
- `sqlite3_txn_state` (12 occurrences) - Query transaction state

## Medium Priority
- `hexio_write` (3 occurrences) - Low-level hex I/O for corruption tests
- `sqlite3_release_memory` (2 occurrences) - Memory management
- `breakpoint` (2 occurrences) - Test breakpoint command

## Low Priority (test infrastructure)
- `tcl_variable_type` (4 occurrences)
- `working_64bit_int` (2 occurrences)
- `clang_sanitize_address` (2 occurrences)
- `do_faultsim_test` (2 occurrences)

## Notes
These are TCL extension commands in `src/tcl_ext.rs` that wrap SQLite C API
functions for testing. Implementation should follow existing patterns like
`sqlite3_prepare_v2`.
