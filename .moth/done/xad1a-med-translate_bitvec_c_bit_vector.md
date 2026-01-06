# Translate bitvec.c - Bit Vector

## Overview
Translate the bit vector implementation used for tracking page usage, journaling status, and other boolean-per-page tracking throughout SQLite.

## Source Reference
- `sqlite3/src/bitvec.c` - 495 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Bitvec
Efficient bit vector that handles both small and large sets:
```rust
/// A bitmap that can efficiently store bits for page numbers
/// Uses a hybrid approach:
/// - For small bitmaps: direct bit storage
/// - For large sparse bitmaps: hash table of set bits
pub struct Bitvec {
    /// Maximum bit index (size of the bitvec)
    size: u32,

    /// Number of bits currently set
    n_set: u32,

    /// Divisor for hashing (0 = use direct bitmap)
    divisor: u32,

    /// Storage: either bitmap or hash table
    storage: BitvecStorage,
}

enum BitvecStorage {
    /// Direct bitmap for small sizes
    Bitmap(Vec<u8>),

    /// Hash table for large sparse sets
    /// Stores (sub_bitmap_index, sub_bitmap)
    Hash {
        table: Vec<Option<Box<Bitvec>>>,
        n_hash: u32,
    },

    /// Array of set values for very small counts
    Array(Vec<u32>),
}
```

## Constants

```rust
/// Size threshold for using bitmap vs hash
const BITVEC_SZ: usize = 512;

/// Number of bits in direct bitmap
const BITVEC_NBIT: u32 = BITVEC_SZ as u32 * 8;

/// Size of hash table
const BITVEC_NINT: usize = BITVEC_SZ / 4;

/// Entries in pointer array
const BITVEC_NPTR: usize = BITVEC_SZ / std::mem::size_of::<*mut Bitvec>();

/// Maximum array elements before switching
const BITVEC_MXHASH: u32 = BITVEC_NINT as u32 / 2;
```

## Key Functions

### Creation and Destruction
```rust
impl Bitvec {
    /// Create a new bitvec that can store bits 1..=size
    pub fn new(size: u32) -> Self {
        if size <= BITVEC_NBIT {
            // Small: use direct bitmap
            Bitvec {
                size,
                n_set: 0,
                divisor: 0,
                storage: BitvecStorage::Bitmap(vec![0u8; BITVEC_SZ]),
            }
        } else {
            // Large: use hash table of sub-bitvecs
            let divisor = (size + BITVEC_NPTR as u32 - 1) / BITVEC_NPTR as u32;
            Bitvec {
                size,
                n_set: 0,
                divisor,
                storage: BitvecStorage::Hash {
                    table: vec![None; BITVEC_NPTR],
                    n_hash: 0,
                },
            }
        }
    }
}
```

### Bit Operations
```rust
impl Bitvec {
    /// Check if bit i is set (1-indexed)
    pub fn test(&self, i: u32) -> bool {
        if i == 0 || i > self.size {
            return false;
        }

        match &self.storage {
            BitvecStorage::Bitmap(bits) => {
                let idx = (i - 1) as usize;
                let byte = idx / 8;
                let bit = idx % 8;
                bits[byte] & (1 << bit) != 0
            }
            BitvecStorage::Hash { table, .. } => {
                let h = ((i - 1) / self.divisor) as usize;
                if let Some(sub) = &table[h] {
                    let sub_idx = (i - 1) % self.divisor + 1;
                    sub.test(sub_idx)
                } else {
                    false
                }
            }
            BitvecStorage::Array(arr) => {
                arr.contains(&i)
            }
        }
    }

    /// Set bit i (1-indexed)
    pub fn set(&mut self, i: u32) -> Result<()> {
        if i == 0 || i > self.size {
            return Err(Error::new(ErrorCode::Error));
        }

        if self.test(i) {
            return Ok(()); // Already set
        }

        match &mut self.storage {
            BitvecStorage::Bitmap(bits) => {
                let idx = (i - 1) as usize;
                let byte = idx / 8;
                let bit = idx % 8;
                bits[byte] |= 1 << bit;
                self.n_set += 1;
            }
            BitvecStorage::Hash { table, n_hash } => {
                let h = ((i - 1) / self.divisor) as usize;
                if table[h].is_none() {
                    table[h] = Some(Box::new(Bitvec::new(self.divisor)));
                    *n_hash += 1;
                }
                let sub_idx = (i - 1) % self.divisor + 1;
                table[h].as_mut().unwrap().set(sub_idx)?;
                self.n_set += 1;
            }
            BitvecStorage::Array(arr) => {
                if arr.len() < BITVEC_MXHASH as usize {
                    arr.push(i);
                    self.n_set += 1;
                } else {
                    // Convert to hash table
                    self.convert_to_hash()?;
                    return self.set(i);
                }
            }
        }

        Ok(())
    }

    /// Clear bit i (1-indexed)
    pub fn clear(&mut self, i: u32) {
        if i == 0 || i > self.size {
            return;
        }

        match &mut self.storage {
            BitvecStorage::Bitmap(bits) => {
                let idx = (i - 1) as usize;
                let byte = idx / 8;
                let bit = idx % 8;
                if bits[byte] & (1 << bit) != 0 {
                    bits[byte] &= !(1 << bit);
                    self.n_set -= 1;
                }
            }
            BitvecStorage::Hash { table, .. } => {
                let h = ((i - 1) / self.divisor) as usize;
                if let Some(sub) = &mut table[h] {
                    let sub_idx = (i - 1) % self.divisor + 1;
                    if sub.test(sub_idx) {
                        sub.clear(sub_idx);
                        self.n_set -= 1;
                    }
                }
            }
            BitvecStorage::Array(arr) => {
                if let Some(pos) = arr.iter().position(|&x| x == i) {
                    arr.swap_remove(pos);
                    self.n_set -= 1;
                }
            }
        }
    }

    /// Clear all bits
    pub fn clear_all(&mut self) {
        match &mut self.storage {
            BitvecStorage::Bitmap(bits) => bits.fill(0),
            BitvecStorage::Hash { table, n_hash } => {
                *table = vec![None; BITVEC_NPTR];
                *n_hash = 0;
            }
            BitvecStorage::Array(arr) => arr.clear(),
        }
        self.n_set = 0;
    }
}
```

### Utility Functions
```rust
impl Bitvec {
    /// Get number of bits set
    pub fn count(&self) -> u32 {
        self.n_set
    }

    /// Get size of bitvec
    pub fn size(&self) -> u32 {
        self.size
    }

    /// Convert array storage to hash table
    fn convert_to_hash(&mut self) -> Result<()> {
        if let BitvecStorage::Array(arr) = &self.storage {
            let items: Vec<u32> = arr.clone();
            let divisor = (self.size + BITVEC_NPTR as u32 - 1) / BITVEC_NPTR as u32;

            self.divisor = divisor;
            self.storage = BitvecStorage::Hash {
                table: vec![None; BITVEC_NPTR],
                n_hash: 0,
            };
            self.n_set = 0;

            for i in items {
                self.set(i)?;
            }
        }
        Ok(())
    }
}
```

## Usage in SQLite

The Bitvec is used throughout SQLite for:

1. **Journal tracking**: Track which pages are in the rollback journal
2. **Page allocation**: Track which pages are in use
3. **Checkpoint**: Track which WAL frames have been checkpointed
4. **Vacuum**: Track pages during database compaction

```rust
// Example: Track journaled pages
impl Pager {
    fn journal_page(&mut self, pgno: Pgno) -> Result<()> {
        if !self.journaled_pages.test(pgno) {
            self.write_to_journal(pgno)?;
            self.journaled_pages.set(pgno)?;
        }
        Ok(())
    }
}
```

## Acceptance Criteria
- [ ] Bitvec struct with hybrid storage
- [ ] new() creates appropriate storage type
- [ ] test() checks if bit is set
- [ ] set() sets a bit
- [ ] clear() clears a bit
- [ ] clear_all() resets entire bitvec
- [ ] count() returns set bit count
- [ ] Automatic conversion from array to hash
- [ ] Memory efficient for sparse sets
