# Implement SeekResult Optimization for Insert

## Problem

The `seekResult` parameter in insert() is currently ignored (btree.rs ~2520). SQLite uses this hint to avoid redundant cursor positioning when the cursor is already at or near the insertion point.

This optimization significantly improves bulk insert performance.

## SQLite3 Reference

### Key Files
- `sqlite3/src/btree.c` - sqlite3BtreeInsert() (~9370)
- `sqlite3/src/vdbe.c` - OP_Insert, OP_IdxInsert usage

### seekResult Parameter (btree.c ~9390)
```c
/*
** seekResult parameter values:
**   0 - cursor is not positioned, do a full seek
**  -1 - cursor is positioned at a key less than pX->nKey
**  +1 - cursor is positioned at a key greater than pX->nKey
**
** If seekResult!=0, the cursor is already near the insertion point.
** We can skip the initial seek operation.
*/
int sqlite3BtreeInsert(
  BtCursor *pCur,
  const BtreePayload *pX,
  int flags,
  int seekResult   /* Result of prior sqlite3BtreeSeek() or 0 */
)
```

### How SQLite Uses seekResult (btree.c ~9420)
```c
if( seekResult==0 ){
  /* No hint, do full seek */
  rc = btreeMoveto(pCur, pX->pKey, pX->nKey, 0, &seekResult);
}else{
  /* Cursor already positioned, verify it's still valid */
  if( seekResult<0 && pCur->idx>0 ){
    /* Cursor at key < target, might need to advance */
    moveNext = 1;
  }
}
```

### VDBE Usage Pattern (vdbe.c OP_Insert)
```c
/* From OP_SeekGE or similar: */
seekResult = pC->seekResult;  /* -1, 0, or +1 from prior seek */

/* Pass to insert: */
sqlite3BtreeInsert(pCur, &x, flags, seekResult);
```

### Performance Impact
Without seekResult optimization:
- Every insert does O(log n) seek
- 1M inserts = 1M seeks = ~20M page reads

With seekResult optimization:
- Sequential inserts: cursor already at right place
- Only verify position, skip seek
- ~1 page read per insert for sequential data

## Current Rust Implementation

```rust
// src/storage/btree.rs line ~2520
pub fn insert(
    &mut self,
    cursor: &mut BtCursor,
    key: i64,
    data: &[u8],
    flags: BtreeInsertFlags,
    _seek_result: i32,  // IGNORED!
) -> Result<()> {
    // Always does full seek via cursor positioning
    // Never uses seek_result hint
}
```

## Required Changes

### 1. Store seekResult in Cursor
```rust
pub struct BtCursor {
    // ... existing fields
    pub seek_result: i32,  // -1, 0, or +1 from last seek
}
```

### 2. Update Seek Operations to Set seek_result
```rust
impl BtCursor {
    pub fn seek_ge(&mut self, key: i64) -> Result<bool> {
        // ... existing seek logic
        self.seek_result = if found { 0 } else if cursor_key < key { -1 } else { 1 };
        Ok(found)
    }
}
```

### 3. Modify Insert to Use seek_result
```rust
pub fn insert(
    &mut self,
    cursor: &mut BtCursor,
    key: i64,
    data: &[u8],
    flags: BtreeInsertFlags,
    seek_result: i32,
) -> Result<()> {
    if seek_result == 0 {
        // No hint or exact match - do full positioning
        self.position_for_insert(cursor, key)?;
    } else if seek_result < 0 {
        // Cursor at key < target - might need to advance
        // Verify and advance if needed
        while cursor.is_valid() && cursor.key()? < key {
            cursor.next()?;
        }
    } else {
        // seek_result > 0: cursor at key > target
        // Position is already correct for insert before current
    }

    // Now insert at cursor position
    self.insert_at_cursor(cursor, key, data)?;
    Ok(())
}
```

## Unit Tests Required

### Test 1: Sequential insert with hint
```rust
#[test]
fn test_seek_result_sequential_insert() {
    let mut btree = setup_btree();
    let mut cursor = btree.cursor();

    // Insert 1-1000 sequentially
    let mut seek_result = 0;  // First insert needs full seek

    for i in 1..=1000i64 {
        btree.insert(&mut cursor, i, &i.to_le_bytes(), BtreeInsertFlags::empty(), seek_result).unwrap();
        seek_result = -1;  // Cursor now at key < next key
    }

    // Verify all inserted
    assert_eq!(btree.count().unwrap(), 1000);
}
```

### Test 2: Reverse insert with hint
```rust
#[test]
fn test_seek_result_reverse_insert() {
    let mut btree = setup_btree();
    let mut cursor = btree.cursor();

    // Insert 1000-1 in reverse
    let mut seek_result = 0;

    for i in (1..=1000i64).rev() {
        btree.insert(&mut cursor, i, &i.to_le_bytes(), BtreeInsertFlags::empty(), seek_result).unwrap();
        seek_result = 1;  // Cursor now at key > next key (since we're going backwards)
    }

    assert_eq!(btree.count().unwrap(), 1000);
}
```

### Test 3: Random insert ignores hint
```rust
#[test]
fn test_seek_result_random_insert() {
    let mut btree = setup_btree();
    let mut cursor = btree.cursor();

    // Random order - hint should be 0 (no hint)
    let keys: Vec<i64> = vec![500, 100, 900, 250, 750];

    for key in keys {
        btree.insert(&mut cursor, key, &[], BtreeInsertFlags::empty(), 0).unwrap();
    }

    // Should work even with 0 hint
    assert_eq!(btree.count().unwrap(), 5);
}
```

### Test 4: Hint correctness verification
```rust
#[test]
fn test_seek_result_verification() {
    let mut btree = setup_btree();
    let mut cursor = btree.cursor();

    // Insert some data
    btree.insert(&mut cursor, 100, &[], BtreeInsertFlags::empty(), 0).unwrap();
    btree.insert(&mut cursor, 200, &[], BtreeInsertFlags::empty(), 0).unwrap();

    // Position cursor at 100
    cursor.seek_ge(100).unwrap();
    assert_eq!(cursor.seek_result, 0);  // Exact match

    // Position cursor between 100 and 200
    cursor.seek_ge(150).unwrap();
    assert_eq!(cursor.seek_result, 1);  // Cursor at 200 > 150

    // Position cursor before 100
    cursor.seek_ge(50).unwrap();
    assert_eq!(cursor.seek_result, 1);  // Cursor at 100 > 50
}
```

### Test 5: Performance comparison
```rust
#[test]
fn test_seek_result_performance() {
    let mut btree_with_hint = setup_btree();
    let mut btree_no_hint = setup_btree();

    let count = 10000i64;

    // With hint
    let start = Instant::now();
    let mut cursor = btree_with_hint.cursor();
    let mut seek_result = 0;
    for i in 1..=count {
        btree_with_hint.insert(&mut cursor, i, &[], BtreeInsertFlags::empty(), seek_result).unwrap();
        seek_result = -1;
    }
    let with_hint_time = start.elapsed();

    // Without hint (always 0)
    let start = Instant::now();
    let mut cursor = btree_no_hint.cursor();
    for i in 1..=count {
        btree_no_hint.insert(&mut cursor, i, &[], BtreeInsertFlags::empty(), 0).unwrap();
    }
    let no_hint_time = start.elapsed();

    // With hint should be significantly faster for sequential
    println!("With hint: {:?}, Without: {:?}", with_hint_time, no_hint_time);
    // Not asserting exact speedup, but with hint should be faster
}
```

## Acceptance Criteria

- [ ] seek_result parameter is used (not ignored)
- [ ] Cursor stores seek_result from seek operations
- [ ] Sequential inserts skip redundant seeks when hint provided
- [ ] Correctness maintained regardless of hint value
- [ ] Performance improvement measurable for sequential inserts
- [ ] All unit tests pass
