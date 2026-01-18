# VDBE: Implement cursor column caching optimization

## Problem
VdbeCursor has `cached_columns` field but it's unused. Repeated column access re-decodes the record each time.

## Current State
```rust
pub struct VdbeCursor {
    cached_columns: Option<Vec<Mem>>,  // Unused
    // ...
}
```

## Required Changes
1. Cache decoded columns on first access
2. Invalidate cache on cursor movement
3. Return cached value for repeated Column opcode
