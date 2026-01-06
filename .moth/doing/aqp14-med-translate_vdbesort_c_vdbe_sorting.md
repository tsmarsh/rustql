# Translate vdbesort.c - VDBE Sorting

## Overview
Translate the external merge sort implementation used for ORDER BY clauses when the data doesn't fit in memory.

## Source Reference
- `sqlite3/src/vdbesort.c` - 2,796 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### VdbeSorter
Main sorter object:
```rust
pub struct VdbeSorter {
    /// Key comparison info
    key_info: Arc<KeyInfo>,

    /// Memory limit before spilling to disk
    mem_limit: i64,

    /// Current memory usage
    mem_used: i64,

    /// In-memory records
    records: Vec<SorterRecord>,

    /// Temporary files for external sort
    temp_files: Vec<TempFile>,

    /// Merge state
    merge: Option<MergeEngine>,

    /// Number of PMAs (sorted runs)
    n_pma: i32,

    /// Sort state
    state: SorterState,

    /// Database connection
    db: Arc<Connection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SorterState {
    Building,    // Adding records
    Sorted,      // In-memory sort complete
    Merging,     // External merge in progress
}
```

### SorterRecord
Single record being sorted:
```rust
pub struct SorterRecord {
    /// Serialized key
    key: Vec<u8>,

    /// Optional data payload
    data: Option<Vec<u8>>,

    /// Key for comparison (parsed)
    parsed_key: Option<Vec<Mem>>,
}
```

### MergeEngine
Manages multi-way merge:
```rust
pub struct MergeEngine {
    /// Iterators over sorted runs
    iterators: Vec<PmaReader>,

    /// Tournament tree for k-way merge
    /// Index of iterator with smallest key at each level
    tree: Vec<usize>,

    /// Result of last comparison
    last_cmp: Ordering,
}

pub struct PmaReader {
    /// File handle
    file: Box<dyn VfsFile>,

    /// Current offset in file
    offset: i64,

    /// End offset
    end_offset: i64,

    /// Buffer for reading
    buffer: Vec<u8>,

    /// Current record (if any)
    current: Option<SorterRecord>,
}
```

## Key Functions

### Initialization

```rust
impl VdbeSorter {
    pub fn new(db: Arc<Connection>, key_info: Arc<KeyInfo>) -> Self {
        let mem_limit = db.cache_size() * db.page_size() as i64;

        VdbeSorter {
            key_info,
            mem_limit,
            mem_used: 0,
            records: Vec::new(),
            temp_files: Vec::new(),
            merge: None,
            n_pma: 0,
            state: SorterState::Building,
            db,
        }
    }
}
```

### Adding Records

```rust
impl VdbeSorter {
    /// Add a record to be sorted
    pub fn write(&mut self, key: &[u8], data: Option<&[u8]>) -> Result<()> {
        let record = SorterRecord {
            key: key.to_vec(),
            data: data.map(|d| d.to_vec()),
            parsed_key: None,
        };

        let size = record.key.len() + record.data.as_ref().map_or(0, |d| d.len());
        self.mem_used += size as i64;
        self.records.push(record);

        // Check if we need to spill to disk
        if self.mem_used > self.mem_limit {
            self.spill_to_disk()?;
        }

        Ok(())
    }

    /// Flush in-memory records to a sorted run on disk
    fn spill_to_disk(&mut self) -> Result<()> {
        // Sort in-memory records
        self.sort_in_memory();

        // Create temp file
        let mut temp = self.db.vfs().open_temp()?;

        // Write sorted records as PMA
        for record in &self.records {
            self.write_record(&mut temp, record)?;
        }

        self.temp_files.push(temp);
        self.n_pma += 1;

        // Clear memory
        self.records.clear();
        self.mem_used = 0;

        Ok(())
    }
}
```

### Sorting

```rust
impl VdbeSorter {
    /// Sort records in memory
    fn sort_in_memory(&mut self) {
        let key_info = &self.key_info;

        self.records.sort_by(|a, b| {
            Self::compare_records(a, b, key_info)
        });
    }

    /// Compare two records
    fn compare_records(
        a: &SorterRecord,
        b: &SorterRecord,
        key_info: &KeyInfo,
    ) -> Ordering {
        // Parse keys if not already parsed
        let key_a = Self::parse_key(&a.key, key_info);
        let key_b = Self::parse_key(&b.key, key_info);

        // Compare field by field
        for i in 0..key_info.n_key_field as usize {
            let coll = &key_info.collations[i];
            let order = &key_info.sort_order[i];

            let cmp = key_a[i].compare(&key_b[i], Some(coll));

            if cmp != Ordering::Equal {
                return if *order {
                    cmp.reverse() // DESC
                } else {
                    cmp // ASC
                };
            }
        }

        Ordering::Equal
    }

    fn parse_key(key: &[u8], key_info: &KeyInfo) -> Vec<Mem> {
        let (types, header_end) = Vdbe::decode_record_header(key).unwrap();
        let mut fields = Vec::new();
        let mut offset = header_end;

        for st in types {
            let (mem, size) = Mem::deserialize(&key[offset..], st);
            fields.push(mem);
            offset += size;
        }

        fields
    }
}
```

### Rewind and Iteration

```rust
impl VdbeSorter {
    /// Prepare for reading sorted results
    pub fn rewind(&mut self) -> Result<()> {
        match self.state {
            SorterState::Building => {
                if self.temp_files.is_empty() {
                    // All in memory - just sort
                    self.sort_in_memory();
                    self.state = SorterState::Sorted;
                } else {
                    // Need to merge temp files with in-memory records
                    if !self.records.is_empty() {
                        self.spill_to_disk()?;
                    }
                    self.start_merge()?;
                    self.state = SorterState::Merging;
                }
            }
            _ => {
                // Already prepared
            }
        }

        Ok(())
    }

    /// Get next record
    pub fn next(&mut self) -> Result<Option<&SorterRecord>> {
        match self.state {
            SorterState::Sorted => {
                // In-memory iteration
                // ... (maintain index)
                Ok(self.records.get(self.current_idx))
            }
            SorterState::Merging => {
                // External merge iteration
                if let Some(ref mut merge) = self.merge {
                    merge.next()
                } else {
                    Ok(None)
                }
            }
            SorterState::Building => {
                Err(Error::new(ErrorCode::Misuse))
            }
        }
    }
}
```

### External Merge

```rust
impl MergeEngine {
    /// Initialize merge engine with PMA readers
    pub fn new(readers: Vec<PmaReader>) -> Result<Self> {
        let n = readers.len();
        let tree_size = n * 2;

        let mut engine = MergeEngine {
            iterators: readers,
            tree: vec![0; tree_size],
            last_cmp: Ordering::Equal,
        };

        // Build initial tournament tree
        engine.build_tree()?;

        Ok(engine)
    }

    /// Build tournament tree for k-way merge
    fn build_tree(&mut self) -> Result<()> {
        let n = self.iterators.len();

        // Initialize leaves
        for i in 0..n {
            self.tree[n + i] = i;
        }

        // Build tree bottom-up
        for i in (1..n).rev() {
            let left = self.tree[i * 2];
            let right = self.tree[i * 2 + 1];
            self.tree[i] = self.winner(left, right)?;
        }

        Ok(())
    }

    /// Compare two iterators and return winner (smaller key)
    fn winner(&self, a: usize, b: usize) -> Result<usize> {
        let rec_a = self.iterators[a].current.as_ref();
        let rec_b = self.iterators[b].current.as_ref();

        match (rec_a, rec_b) {
            (None, None) => Ok(a),
            (Some(_), None) => Ok(a),
            (None, Some(_)) => Ok(b),
            (Some(ra), Some(rb)) => {
                // Compare records using KeyInfo ordering and return index of smaller.
                // Keep collation/NULL/ASC-DESC semantics in sync with `compare_records`.
                Ok(a)
            }
        }
    }

    /// Get next record from merge
    pub fn next(&mut self) -> Result<Option<&SorterRecord>> {
        // Get winner from tree root
        let winner = self.tree[1];

        // Advance winner iterator
        self.iterators[winner].advance()?;

        // Replay tree from winner position
        self.replay_tree(winner)?;

        Ok(self.iterators[winner].current.as_ref())
    }
}
```

## PMA File Format

```
Each PMA (Packed Memory Array) consists of:

+-------------------+
| Record 1 length   | (varint)
+-------------------+
| Record 1 data     |
+-------------------+
| Record 2 length   | (varint)
+-------------------+
| Record 2 data     |
+-------------------+
| ...               |
+-------------------+
| 0 (end marker)    |
+-------------------+
```

## Acceptance Criteria
- [ ] VdbeSorter struct with state management
- [ ] write() to add records
- [ ] In-memory sorting
- [ ] Spill to disk when memory limit exceeded
- [ ] PMA file format read/write
- [ ] MergeEngine for k-way merge
- [ ] Tournament tree implementation
- [ ] rewind() and next() iteration
- [ ] Proper comparison with KeyInfo
- [ ] Temp file cleanup
