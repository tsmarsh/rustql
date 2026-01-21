# Implement Missing DB Methods

TCL db command methods needed for test compatibility.

## Missing Methods
- `db collation_needed` (3 occurrences) - Register collation callback
- `db incrblob` (2 occurrences) - Incremental BLOB I/O
- `db config` (2 occurrences) - Database configuration options
- `db null` (6 occurrences) - Set null representation
- `db func` (1 occurrence) - Register custom SQL function

## Implementation Location
These are methods on the `db` TCL command in `src/tcl_ext.rs`.
Each method needs to be added to the `db_cmd` match statement.

## Details

### collation_needed
Register a callback invoked when an unknown collation is requested.
```tcl
db collation_needed callback_proc
```

### incrblob
Open a BLOB for incremental I/O.
```tcl
set channel [db incrblob ?-readonly? table column rowid]
```

### config
Query/set database configuration.
```tcl
db config ?option? ?value?
```

### null
Set the string representation for NULL values.
```tcl
db nullvalue ?string?
```

## Notes
`incrblob` is complex as it requires creating a TCL channel for streaming
BLOB access. May need to stub initially.
