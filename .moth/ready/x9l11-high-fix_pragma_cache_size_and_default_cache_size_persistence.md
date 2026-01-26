# Fix PRAGMA cache_size and default_cache_size Persistence

The pragma tests show cache_size and default_cache_size are not working correctly.

## Current Behavior

```
pragma-1.1 expected: [0 0 2]
pragma-1.1 got:      [0 0 1]

pragma-1.2 expected: [1234 0 0]
pragma-1.2 got:      [1234 1234 0]
```

The third value appears to be schema_version or similar.

## Issues Identified

1. **cache_size not resetting** - After setting cache_size, it persists when it shouldn't
2. **default_cache_size** - SQLite's deprecated pragma, may need special handling
3. **schema_version** - Appears to be returning wrong values (1 instead of 2)

## Required Behavior

- `PRAGMA cache_size` - Returns current cache size in pages (or -kibibytes if negative)
- `PRAGMA cache_size = N` - Sets cache size for this connection only (not persisted)
- `PRAGMA default_cache_size` - Deprecated, but should still work
- Cache size changes should NOT affect other connections
- Reopening database should reset to default

## Affected Tests

- pragma-1.1 through pragma-1.18 (20+ tests)

## Files to Modify

- `src/pragma.rs` - cache_size and default_cache_size handling
- `src/pager.rs` - actual cache size implementation
