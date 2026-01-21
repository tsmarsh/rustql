# Implement Missing Pragmas

SQLite pragmas needed for test compatibility.

## Missing Pragmas (by frequency)
- `default_cache_size` (20 occurrences) - Legacy cache size pragma
- `case_sensitive_like` (17 occurrences) - Control LIKE case sensitivity
- `freelist_count` (10 occurrences) - Query number of free pages
- `lock_status` (7 occurrences) - Query lock state of databases
- `incremental_vacuum` (3 occurrences) - Incremental vacuum support
- `cache_spill` (3 occurrences) - Control cache spilling behavior
- `writable_schema` (2 occurrences) - Allow schema modifications
- `automatic_index` (1 occurrence) - Control automatic index creation
- `default_synchronous` (1 occurrence) - Legacy synchronous pragma

## Implementation Location
Pragmas are handled in `src/executor/pragma.rs`. Each pragma needs:
1. Parser support in the pragma handler
2. Implementation logic (get/set values)
3. Integration with btree/pager for storage-related pragmas

## Notes
Some pragmas like `default_cache_size` are deprecated in favor of
`cache_size` but are still used in older tests.
