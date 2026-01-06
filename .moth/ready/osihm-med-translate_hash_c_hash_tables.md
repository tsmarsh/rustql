# Translate hash.c - Hash Tables

## Overview
Translate hash table implementation used throughout SQLite for name lookups and symbol tables.

## Source Reference
- `sqlite3/src/hash.c` - ~350 lines
- `sqlite3/src/hash.h` - ~100 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Hash Table
```rust
/// Hash table with string keys
pub struct Hash<V> {
    /// Number of entries
    count: usize,
    /// Hash buckets
    buckets: Vec<Option<Box<HashEntry<V>>>>,
    /// Number of buckets (always power of 2)
    n_buckets: usize,
}

struct HashEntry<V> {
    /// Key
    key: String,
    /// Value
    value: V,
    /// Next entry in chain
    next: Option<Box<HashEntry<V>>>,
}
```

### Hash Element Iterator
```rust
pub struct HashIter<'a, V> {
    hash: &'a Hash<V>,
    bucket_idx: usize,
    current: Option<&'a HashEntry<V>>,
}

impl<'a, V> Iterator for HashIter<'a, V> {
    type Item = (&'a str, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        // Follow chain or move to next bucket
        loop {
            if let Some(entry) = self.current {
                self.current = entry.next.as_ref().map(|b| b.as_ref());
                return Some((&entry.key, &entry.value));
            }

            // Move to next bucket
            while self.bucket_idx < self.hash.n_buckets {
                if let Some(ref entry) = self.hash.buckets[self.bucket_idx] {
                    self.bucket_idx += 1;
                    self.current = Some(entry.as_ref());
                    break;
                }
                self.bucket_idx += 1;
            }

            if self.current.is_none() {
                return None;
            }
        }
    }
}
```

## Hash Implementation

### Core Operations
```rust
impl<V> Hash<V> {
    /// Create a new hash table
    pub fn new() -> Self {
        Self {
            count: 0,
            buckets: Vec::new(),
            n_buckets: 0,
        }
    }

    /// Create with initial capacity
    pub fn with_capacity(capacity: usize) -> Self {
        let n_buckets = capacity.next_power_of_two();
        Self {
            count: 0,
            buckets: (0..n_buckets).map(|_| None).collect(),
            n_buckets,
        }
    }

    /// Number of entries
    pub fn len(&self) -> usize {
        self.count
    }

    /// Is empty?
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Clear all entries
    pub fn clear(&mut self) {
        self.buckets.iter_mut().for_each(|b| *b = None);
        self.count = 0;
    }

    /// Insert a key-value pair
    pub fn insert(&mut self, key: String, value: V) -> Option<V> {
        // Resize if needed
        if self.count >= self.n_buckets {
            self.resize();
        }

        let hash = self.hash_key(&key);
        let bucket_idx = hash & (self.n_buckets - 1);

        // Check if key exists
        let mut current = &mut self.buckets[bucket_idx];
        while let Some(ref mut entry) = current {
            if entry.key == key {
                // Replace value
                return Some(std::mem::replace(&mut entry.value, value));
            }
            current = &mut entry.next;
        }

        // Insert new entry at head
        let new_entry = Box::new(HashEntry {
            key,
            value,
            next: self.buckets[bucket_idx].take(),
        });
        self.buckets[bucket_idx] = Some(new_entry);
        self.count += 1;

        None
    }

    /// Get value by key
    pub fn get(&self, key: &str) -> Option<&V> {
        if self.n_buckets == 0 {
            return None;
        }

        let hash = self.hash_key(key);
        let bucket_idx = hash & (self.n_buckets - 1);

        let mut current = self.buckets[bucket_idx].as_ref();
        while let Some(entry) = current {
            if entry.key == key {
                return Some(&entry.value);
            }
            current = entry.next.as_ref();
        }

        None
    }

    /// Get mutable value by key
    pub fn get_mut(&mut self, key: &str) -> Option<&mut V> {
        if self.n_buckets == 0 {
            return None;
        }

        let hash = self.hash_key(key);
        let bucket_idx = hash & (self.n_buckets - 1);

        let mut current = self.buckets[bucket_idx].as_mut();
        while let Some(entry) = current {
            if entry.key == key {
                return Some(&mut entry.value);
            }
            current = entry.next.as_mut();
        }

        None
    }

    /// Remove by key
    pub fn remove(&mut self, key: &str) -> Option<V> {
        if self.n_buckets == 0 {
            return None;
        }

        let hash = self.hash_key(key);
        let bucket_idx = hash & (self.n_buckets - 1);

        // Special case: first entry
        if let Some(ref entry) = self.buckets[bucket_idx] {
            if entry.key == key {
                let removed = self.buckets[bucket_idx].take().unwrap();
                self.buckets[bucket_idx] = removed.next;
                self.count -= 1;
                return Some(removed.value);
            }
        }

        // Search chain
        let mut current = &mut self.buckets[bucket_idx];
        while let Some(ref mut entry) = current {
            if let Some(ref next) = entry.next {
                if next.key == key {
                    let removed = entry.next.take().unwrap();
                    entry.next = removed.next;
                    self.count -= 1;
                    return Some(removed.value);
                }
            }
            current = &mut entry.next;
        }

        None
    }

    /// Check if key exists
    pub fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    /// Iterate over entries
    pub fn iter(&self) -> HashIter<V> {
        HashIter {
            hash: self,
            bucket_idx: 0,
            current: None,
        }
    }
}
```

### Hash Function
```rust
impl<V> Hash<V> {
    /// SQLite-compatible hash function for strings
    fn hash_key(&self, key: &str) -> usize {
        let mut h: u32 = 0;

        for &byte in key.as_bytes() {
            // Case-insensitive hash for ASCII
            let c = if byte >= b'A' && byte <= b'Z' {
                byte + 32
            } else {
                byte
            };

            h = (h << 3) ^ h ^ (c as u32);
        }

        h as usize
    }

    /// Resize the hash table
    fn resize(&mut self) {
        let new_size = if self.n_buckets == 0 {
            8
        } else {
            self.n_buckets * 2
        };

        let old_buckets = std::mem::replace(
            &mut self.buckets,
            (0..new_size).map(|_| None).collect()
        );
        self.n_buckets = new_size;
        self.count = 0;

        // Rehash all entries
        for bucket in old_buckets {
            let mut current = bucket;
            while let Some(mut entry) = current {
                current = entry.next.take();
                self.insert(entry.key, entry.value);
            }
        }
    }
}
```

## Case-Insensitive Hash

```rust
/// Case-insensitive string hash table
pub struct HashNoCase<V> {
    inner: Hash<V>,
}

impl<V> HashNoCase<V> {
    pub fn new() -> Self {
        Self { inner: Hash::new() }
    }

    pub fn insert(&mut self, key: &str, value: V) -> Option<V> {
        self.inner.insert(key.to_lowercase(), value)
    }

    pub fn get(&self, key: &str) -> Option<&V> {
        self.inner.get(&key.to_lowercase())
    }

    pub fn remove(&mut self, key: &str) -> Option<V> {
        self.inner.remove(&key.to_lowercase())
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.inner.contains_key(&key.to_lowercase())
    }
}
```

## Integration with Schema

```rust
/// Schema uses hash tables for name lookups
pub struct Schema {
    /// Tables by name
    pub tables: Hash<Arc<Table>>,
    /// Indexes by name
    pub indexes: Hash<Arc<Index>>,
    /// Triggers by name
    pub triggers: Hash<Arc<Trigger>>,
    /// Views by name
    pub views: Hash<Arc<View>>,
    /// Collations by name
    pub collations: HashNoCase<Arc<Collation>>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            tables: Hash::new(),
            indexes: Hash::new(),
            triggers: Hash::new(),
            views: Hash::new(),
            collations: HashNoCase::new(),
        }
    }

    pub fn find_table(&self, name: &str) -> Option<&Arc<Table>> {
        self.tables.get(name)
    }

    pub fn add_table(&mut self, table: Table) {
        self.tables.insert(table.name.clone(), Arc::new(table));
    }
}
```

## Acceptance Criteria
- [ ] Hash table with string keys
- [ ] Insert/get/remove operations
- [ ] Case-insensitive variant
- [ ] Chained collision resolution
- [ ] Automatic resizing
- [ ] Iterator implementation
- [ ] SQLite-compatible hash function
- [ ] O(1) average lookup time
- [ ] Clear operation
- [ ] Length/empty checks
- [ ] Integration with Schema
