# Implement WITH RECURSIVE Common Table Expressions

## Problem

Recursive CTEs (Common Table Expressions) using `WITH RECURSIVE` are not
implemented. The parser may accept the syntax but execution fails with
"no such table" errors when the CTE references itself.

## Failing Tests

- `delete-11.0`, `delete-11.1` in `sqlite3/test/delete.test`
- Likely many tests in `with1.test`, `with2.test`, `with3.test`

## Test Case Analysis

```sql
-- delete-11.0: Generate sequence 1-20 using recursive CTE
CREATE TABLE t11(a INTEGER PRIMARY KEY, b INT);
WITH RECURSIVE cnt(x) AS (
    VALUES(1)                           -- Base case: start with 1
    UNION ALL
    SELECT x+1 FROM cnt WHERE x<20      -- Recursive case: increment until 20
)
INSERT INTO t11(a,b) SELECT x, (x*17)%100 FROM cnt;

-- Expected result: 20 rows with a=1..20, b=(a*17)%100
-- Current result: Error "no such table: cnt"
```

## How Recursive CTEs Work

A recursive CTE has two parts connected by UNION ALL:

1. **Base case** (anchor): A non-recursive query that provides initial rows
2. **Recursive case**: A query that references the CTE itself

Execution algorithm:
```
1. Execute base case, store results in working table W
2. Copy W to result table R
3. While W is not empty:
   a. Execute recursive case using W as the CTE
   b. Store results in temporary table T
   c. Clear W
   d. Move T contents to W
   e. Append W to R
4. Return R
```

## Current State

Looking at the codebase:
- `src/parser/ast.rs` has `WithClause` and `Cte` structures
- Non-recursive CTEs may work (treated as named subqueries)
- Recursive execution loop is not implemented

## Discovered Bug (2026-01-27)

**WITH RECURSIVE causes infinite loop / timeout**

The `compile_recursive_cte` function in `src/executor/select/mod.rs` (line 544)
has been implemented, but executing any WITH RECURSIVE query hits the 100M
instruction limit and aborts with "query aborted".

**Reproduction:**
```sql
WITH RECURSIVE cnt(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x<5)
SELECT x FROM cnt;
-- Times out / aborts
```

**Root cause investigation needed:**
1. Check if `queue_cursor` is properly being cleared between iterations
2. Verify the Rewind/Next loop terminates when queue is empty
3. Check if `next_cursor` rows are being transferred to `queue_cursor` correctly
4. Verify WHERE clause `x<5` is being evaluated (termination condition)

**Key code location:** `compile_recursive_cte()` at line 544-712 and
`emit_recursive_cte_process_cursor()` at line 715-800 in `src/executor/select/mod.rs`

## Implementation Plan

### Phase 1: Parser Updates

Ensure `WITH RECURSIVE` is properly parsed in `src/parser/grammar.rs`:

```rust
// In WithClause
pub struct WithClause {
    pub recursive: bool,  // true if WITH RECURSIVE
    pub ctes: Vec<Cte>,
}
```

### Phase 2: Recursive CTE Detection

In `src/executor/select.rs`, detect recursive CTEs:

```rust
fn is_recursive_cte(cte: &Cte) -> bool {
    // A CTE is recursive if its body references itself
    match &cte.body {
        SelectBody::Compound { op: CompoundOp::UnionAll, left, right } => {
            // Check if right side references cte.name
            references_table(&right, &cte.name)
        }
        _ => false
    }
}
```

### Phase 3: Recursive Execution

Add recursive CTE execution in `src/executor/select.rs`:

```rust
fn execute_recursive_cte(
    &mut self,
    cte: &Cte,
    schema: &Schema,
) -> Result<Vec<Row>> {
    let mut result = Vec::new();

    // Extract base and recursive cases from UNION ALL
    let (base_query, recursive_query) = split_union_all(&cte.body)?;

    // Step 1: Execute base case
    let mut working_set = self.execute_select(&base_query, schema)?;
    result.extend(working_set.clone());

    // Step 2: Iterative execution
    let max_iterations = 1000; // Prevent infinite loops
    for _ in 0..max_iterations {
        if working_set.is_empty() {
            break;
        }

        // Create temporary "table" from working set for recursive query
        let temp_cte = create_temp_table(&cte.name, &working_set);

        // Execute recursive case with temp_cte available
        let new_rows = self.execute_select_with_cte(
            &recursive_query,
            schema,
            &temp_cte
        )?;

        if new_rows.is_empty() {
            break;
        }

        result.extend(new_rows.clone());
        working_set = new_rows;
    }

    Ok(result)
}
```

### Phase 4: VDBE Implementation

For better performance, implement at VDBE level using opcodes:

```
; Recursive CTE execution
OpenEphemeral 0 N       ; Working table
OpenEphemeral 1 N       ; Result table
OpenEphemeral 2 N       ; Temp table

; Execute base case into working table
<base case bytecode writing to cursor 0>

; Copy working to result
Rewind 0 done
loop:
  RowData 0 r1
  Insert 1 r1 r2        ; Copy to result
  Next 0 loop

; Recursive loop
recursive:
  Rewind 0 done         ; If working empty, done

  ; Execute recursive case reading from cursor 0, writing to cursor 2
  <recursive case bytecode>

  ; Clear working, move temp to working
  Clear 0
  Rewind 2 recursive    ; If temp empty, done
copy_loop:
  RowData 2 r1
  Insert 0 r1 r2        ; Move to working
  Insert 1 r1 r3        ; Also to result
  Next 2 copy_loop
  Clear 2
  Goto recursive

done:
  ; Result is in cursor 1
```

## Files to Modify

1. `src/parser/grammar.rs` - Ensure WITH RECURSIVE parsing
2. `src/parser/ast.rs` - Add `recursive` flag to WithClause
3. `src/executor/select.rs` - Add recursive CTE execution
4. `src/vdbe/engine/mod.rs` - Support CTE table references in subprograms

## Testing

```bash
make test-delete  # Should pass delete-11.0, delete-11.1
tclsh sqlite3/test/with1.test  # Full CTE test suite
```

## Edge Cases

1. **Infinite recursion**: Limit iterations (SQLite uses 1000 by default)
2. **Multiple recursive references**: Error - only one reference allowed
3. **Mutual recursion**: Multiple CTEs referencing each other
4. **UNION vs UNION ALL**: UNION removes duplicates (affects termination)

## References

- SQLite docs: https://sqlite.org/lang_with.html
- SQLite source: `select.c` function `sqlite3Select()` CTE handling
- Test files: `sqlite3/test/with1.test`, `with2.test`, `with3.test`
