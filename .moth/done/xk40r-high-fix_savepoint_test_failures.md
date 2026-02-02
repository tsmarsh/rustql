Fix savepoint test failures: transaction/savepoint state handling, locking semantics, incrblob/incr_vacuum support (or appropriate gating), and Tcl channel integration regressions. Use sqlite3/test/savepoint.test as the source of truth. Update tests and engine to match SQLite behavior; no shortcut normalization.

## Current Status (2026-02-02)

**Tests passing: 74/89 (83.1%)**

### Categories of Failures

1. **Multi-client tests (savepoint-14.*)** - 5+ tests
   - Require `do_multiclient_test` / `code2_chan` Tcl infrastructure
   - RustQL's Tcl extension doesn't implement multi-client test channels
   - These tests verify concurrent access behavior which needs separate infrastructure

2. **Database corruption in savepoint-10.2.*** - 3 tests
   - "database disk image is malformed" errors during complex rollback scenarios
   - Involves attached databases (aux1, aux2) with savepoints
   - Savepoint rollback may not properly restore page state across attached DBs
   - Tests: savepoint-10.2.9, 10.2.10, 10.2.12

3. **Lock state mismatches** - several tests
   - Expected vs actual lock states differ (e.g., "reserved" vs "shared")
   - May be related to attached database lock coordination

4. **Missing tables after rollback** - several tests
   - Tables t2, t3 not found after rollback scenarios
   - Schema state may not be properly rolled back

### Analysis

The savepoint test covers:
- Basic savepoint/release/rollback
- Nested savepoints
- Savepoints with attached databases
- Multi-client concurrent savepoint access
- Large transaction rollback

Most failures are in complex scenarios involving:
- Attached databases with savepoints
- Multi-client concurrent access (infra issue)
- Lock state coordination across multiple databases

### Next Steps (if continuing)

1. Investigate savepoint-10.2.* corruption - trace btree page operations during rollback
2. Add Tcl multiclient infrastructure (or skip those tests)
3. Fix lock state tracking for attached databases

### Acceptance Criteria

All savepoint tests should pass (excluding those requiring unavailable infrastructure).
```bash
make test-savepoint
```
