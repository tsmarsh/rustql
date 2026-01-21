# Implement Query Hint Functions

SQLite query optimizer hint functions.

## Missing Functions
- `LIKELY(X)` (7 occurrences) - Hint that X is probably true
- `UNLIKELY(X)` (7 occurrences) - Hint that X is probably false  
- `LIKELIHOOD(X, P)` (8 occurrences) - Hint that X has probability P of being true

## Behavior
These functions are no-ops that return their first argument unchanged.
They exist to provide hints to the query optimizer about expected
boolean outcomes. Since RustQL doesn't have the same query optimizer
as SQLite, these can be implemented as simple pass-through functions.

## Implementation
```rust
// In src/functions/scalar.rs
fn func_likely(args: Vec<Value>) -> Value {
    args.into_iter().next().unwrap_or(Value::Null)
}

fn func_unlikely(args: Vec<Value>) -> Value {
    args.into_iter().next().unwrap_or(Value::Null)
}

fn func_likelihood(args: Vec<Value>) -> Value {
    // Ignore probability argument, just return first arg
    args.into_iter().next().unwrap_or(Value::Null)
}
```

## Notes
Low priority since these don't affect query correctness, only optimizer hints.
