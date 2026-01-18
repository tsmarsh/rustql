# Implement KeyInfo and Key Comparison with Collation Support

## Problem

The current `KeyInfo` struct is a stub (btree.rs lines 318-324) with only an `encoding` field. SQLite's KeyInfo contains collation sequences, sort orders, and column count - essential for multi-column index comparisons and ORDER BY operations.

Without proper key comparison, indexes cannot be used for complex WHERE clauses or sorting.

## SQLite3 Reference

### Key Files
- `sqlite3/src/sqliteInt.h` - KeyInfo struct definition (~2400)
- `sqlite3/src/vdbeaux.c` - sqlite3VdbeRecordCompare() (~4800)
- `sqlite3/src/vdbemem.c` - sqlite3MemCompare() (~1100)

### KeyInfo Structure (sqliteInt.h ~2400)
```c
struct KeyInfo {
  u32 nRef;           /* Number of references to this KeyInfo */
  u8 enc;             /* Text encoding - SQLITE_UTF8, etc */
  u16 nKeyField;      /* Number of key columns in the index */
  u16 nAllField;      /* Total columns including rowid */
  sqlite3 *db;        /* Database connection */
  u8 *aSortFlags;     /* Sort order for each column (ASC/DESC/NULLS FIRST) */
  CollSeq *aColl[1];  /* Collating sequence for each column */
};
```

### Comparison Functions
- `sqlite3VdbeRecordCompare()` - Main record comparison (vdbeaux.c ~4800)
- `sqlite3VdbeRecordCompareWithSkip()` - Skip first N fields
- `sqlite3MemCompare()` - Compare two Mem values (vdbemem.c ~1100)

### Collation Sequences (sqliteInt.h ~1800)
```c
struct CollSeq {
  char *zName;          /* Name of collation (BINARY, NOCASE, RTRIM) */
  u8 enc;               /* Text encoding */
  void *pUser;          /* User data for xCmp */
  int (*xCmp)(void*,int,const void*,int,const void*); /* Comparison function */
  void (*xDel)(void*);  /* Destructor for pUser */
};
```

### Built-in Collations
- `BINARY` - memcmp() byte comparison (default)
- `NOCASE` - Case-insensitive for ASCII
- `RTRIM` - Ignore trailing spaces

## Current Rust Implementation

```rust
// src/storage/btree.rs lines 318-324
pub struct KeyInfo {
    pub encoding: u8,
}

pub struct UnpackedRecord {
    pub key: Vec<u8>,
}
```

This is insufficient - no collation, no column info, no comparison function.

## Required Changes

### 1. Expand KeyInfo Structure
```rust
pub struct KeyInfo {
    pub encoding: u8,
    pub n_key_field: u16,      // Number of key columns
    pub n_all_field: u16,      // Total columns including rowid
    pub sort_flags: Vec<u8>,   // ASC=0, DESC=1, NULLS_FIRST, etc
    pub collations: Vec<CollSeq>,
}
```

### 2. Implement CollSeq
```rust
pub enum CollSeq {
    Binary,
    NoCase,
    RTrim,
    Custom { name: String, cmp: fn(&[u8], &[u8]) -> Ordering },
}
```

### 3. Implement Comparison
```rust
impl KeyInfo {
    pub fn compare(&self, left: &[u8], right: &[u8]) -> Ordering {
        // Unpack records, compare field-by-field using collations
    }
}
```

## Unit Tests Required

### Test 1: Binary collation (default)
```rust
#[test]
fn test_binary_collation() {
    let ki = KeyInfo::new(1, vec![CollSeq::Binary]);

    assert_eq!(ki.compare(b"abc", b"abd"), Ordering::Less);
    assert_eq!(ki.compare(b"ABC", b"abc"), Ordering::Less); // A < a in ASCII
    assert_eq!(ki.compare(b"abc", b"abc"), Ordering::Equal);
}
```

### Test 2: NOCASE collation
```rust
#[test]
fn test_nocase_collation() {
    let ki = KeyInfo::new(1, vec![CollSeq::NoCase]);

    assert_eq!(ki.compare(b"ABC", b"abc"), Ordering::Equal);
    assert_eq!(ki.compare(b"Hello", b"HELLO"), Ordering::Equal);
    assert_eq!(ki.compare(b"abc", b"abd"), Ordering::Less);
}
```

### Test 3: Multi-column comparison
```rust
#[test]
fn test_multi_column_compare() {
    // Compare (name COLLATE NOCASE, age BINARY)
    let ki = KeyInfo::new(2, vec![CollSeq::NoCase, CollSeq::Binary]);

    // Same name different age
    let rec1 = encode_record(&["John", 25]);
    let rec2 = encode_record(&["JOHN", 30]);
    assert_eq!(ki.compare(&rec1, &rec2), Ordering::Less); // 25 < 30
}
```

### Test 4: DESC sort order
```rust
#[test]
fn test_desc_sort_order() {
    let mut ki = KeyInfo::new(1, vec![CollSeq::Binary]);
    ki.sort_flags = vec![SORT_DESC];

    // With DESC, larger values come first
    assert_eq!(ki.compare(b"z", b"a"), Ordering::Less); // Reversed!
}
```

### Test 5: NULL handling
```rust
#[test]
fn test_null_comparison() {
    let ki = KeyInfo::new(1, vec![CollSeq::Binary]);

    // NULL compares less than any value by default
    let null_rec = encode_record(&[Value::Null]);
    let int_rec = encode_record(&[Value::Int(1)]);
    assert_eq!(ki.compare(&null_rec, &int_rec), Ordering::Less);
}
```

### Test 6: Index seek with collation
```rust
#[test]
fn test_index_seek_nocase() {
    let db = setup_db();
    db.execute("CREATE TABLE t(name TEXT COLLATE NOCASE)").unwrap();
    db.execute("CREATE INDEX i ON t(name)").unwrap();
    db.execute("INSERT INTO t VALUES('Alice'),('BOB'),('charlie')").unwrap();

    // Should find BOB using lowercase search
    let result = db.query("SELECT * FROM t WHERE name = 'bob'").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["name"], "BOB");
}
```

## Acceptance Criteria

- [ ] KeyInfo contains collation sequences and sort flags
- [ ] BINARY, NOCASE, RTRIM collations implemented
- [ ] Multi-column comparison works correctly
- [ ] DESC sort order inverts comparison
- [ ] NULL handling matches SQLite (NULL < any value)
- [ ] Index operations use KeyInfo for comparisons
- [ ] All unit tests pass
