# Fix Nested Transaction Error Handling

Issues with transaction state management.

## Errors Identified
- `cannot start a transaction within a transaction` (73 occurrences)
- `cannot commit - no transaction is active` (9 occurrences)

## Problem
Tests expect certain transaction behaviors that aren't matching SQLite:
1. BEGIN within BEGIN should error appropriately
2. COMMIT/ROLLBACK without active transaction
3. Savepoint handling within transactions

## SQLite Behavior
- BEGIN when already in transaction: "cannot start a transaction within a transaction"
- COMMIT when not in transaction: "cannot commit - no transaction is active"
- ROLLBACK when not in transaction: no-op (silent success)
- Savepoints provide nested transaction-like behavior

## Investigation Areas
- `src/vdbe/engine.rs` - Transaction opcode handling
- `src/storage/btree.rs` - Transaction state tracking
- `src/api/connection.rs` - Autocommit mode handling

## Test Files
- trans.test, trans2.test
- savepoint.test

## Notes
Some failures may be from test setup issues (shared database state)
rather than actual transaction bugs.
