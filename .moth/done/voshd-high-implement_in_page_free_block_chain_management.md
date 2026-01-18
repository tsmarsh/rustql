# Implement In-Page Free Block Chain Management

## Problem

SQLite manages deleted cell space within pages using a free block chain. This allows space reuse without page compaction. The Rust implementation lacks this, leading to:
- Wasted space within pages
- More frequent page splits
- Degraded performance over time

## SQLite3 Reference

### Key Files
- `sqlite3/src/btree.c` - Free block management functions
- `sqlite3/src/btreeInt.h` - Page structure definitions

### Page Header Layout (btreeInt.h ~150)
```
Offset  Size  Description
------  ----  -----------
0       1     Page type flags
1       2     Offset to first free block (0 if none)
3       2     Number of cells on this page
5       2     Offset to first byte of cell content area
7       1     Number of fragmented free bytes
8       4     Right-most child page (interior pages only)
```

### Free Block Structure (btreeInt.h ~180)
```
Each free block:
- Bytes 0-1: Offset to next free block (0 if last)
- Bytes 2-3: Size of this free block (including header)

Minimum free block size: 4 bytes (for the header)
Blocks smaller than 4 bytes become "fragmented" bytes.
```

### Key Functions

#### allocateSpace() (btree.c ~1470)
```c
/*
** Allocate nByte bytes of space within page pPage.
** Return offset to the space, or 0 if not enough room.
**
** First tries to find a free block large enough.
** Falls back to cell content area if no suitable block.
*/
static int allocateSpace(MemPage *pPage, int nByte, int *pIdx){
  int hdr = pPage->hdrOffset;
  int top = get2byte(&pPage->aData[hdr+5]);  /* Cell content area start */
  int pc = get2byte(&pPage->aData[hdr+1]);   /* First free block */
  int pbegin = pc;

  /* Search free block chain for suitable block */
  while( pc>0 ){
    int next = get2byte(&pPage->aData[pc]);
    int size = get2byte(&pPage->aData[pc+2]);
    if( size>=nByte ){
      /* Found a block big enough */
      if( size<nByte+4 ){
        /* Too small to split - use whole block */
        memcpy(&pPage->aData[pbegin], &pPage->aData[pc], 2);  /* Unlink */
        pPage->nFree -= size;
      }else{
        /* Split block */
        put2byte(&pPage->aData[pc+2], size-nByte);
        pc += size - nByte;
      }
      return pc;
    }
    pbegin = pc;
    pc = next;
  }

  /* No suitable free block - allocate from top of cell content area */
  // ...
}
```

#### freeSpace() (btree.c ~1570)
```c
/*
** Free nByte bytes starting at offset start.
** Adds freed space to free block chain.
** Coalesces adjacent free blocks.
*/
static int freeSpace(MemPage *pPage, u16 start, u16 size){
  int hdr = pPage->hdrOffset;
  int iPtr = hdr + 1;  /* Pointer to first free block */
  int pc = get2byte(&pPage->aData[iPtr]);

  /* Find insertion point in sorted chain */
  while( pc>0 && pc<start ){
    iPtr = pc;
    pc = get2byte(&pPage->aData[iPtr]);
  }

  /* Check if we can coalesce with previous block */
  if( iPtr!=hdr+1 ){
    int prevSize = get2byte(&pPage->aData[iPtr+2]);
    if( iPtr + prevSize == start ){
      /* Coalesce with previous */
      size += prevSize;
      start = iPtr;
      iPtr = /* find previous pointer */;
    }
  }

  /* Check if we can coalesce with next block */
  if( pc>0 && start+size==pc ){
    size += get2byte(&pPage->aData[pc+2]);
    pc = get2byte(&pPage->aData[pc]);
  }

  /* Insert into chain */
  if( size<4 ){
    /* Too small for free block, count as fragments */
    pPage->aData[hdr+7] += size;
  }else{
    put2byte(&pPage->aData[iPtr], start);
    put2byte(&pPage->aData[start], pc);
    put2byte(&pPage->aData[start+2], size);
  }

  pPage->nFree += size;
  return SQLITE_OK;
}
```

#### defragmentPage() (btree.c ~1680)
```c
/*
** Defragment the page by moving all cells to the end,
** consolidating all free space.
** Called when free block chain is too fragmented.
*/
static int defragmentPage(MemPage *pPage, int nMaxFrag){
  // Move all cells to contiguous space at end of page
  // Clear free block chain
  // Set single free area
}
```

## Current Rust Implementation

The Rust implementation doesn't track free blocks within pages. When a cell is deleted:
- The cell pointer is removed
- Space is not reclaimed for reuse
- Eventually requires page defragmentation or split

## Required Changes

### 1. Add Free Block Tracking to MemPage
```rust
pub struct MemPage {
    // ... existing fields
    first_free_block: u16,  // Offset to first free block, 0 if none
    n_free: u16,            // Total free bytes on page
    n_frag: u8,             // Fragmented bytes (too small for free block)
}
```

### 2. Implement allocate_space()
```rust
impl MemPage {
    /// Allocate n_byte bytes within page.
    /// Returns offset to allocated space, or None if not enough room.
    pub fn allocate_space(&mut self, n_byte: usize) -> Option<u16> {
        // 1. Search free block chain for suitable block
        // 2. If found, split or use whole block
        // 3. If not found, allocate from cell content area
        // 4. Update n_free
    }
}
```

### 3. Implement free_space()
```rust
impl MemPage {
    /// Free space at given offset.
    /// Adds to free block chain, coalescing if possible.
    pub fn free_space(&mut self, offset: u16, size: u16) {
        // 1. Find insertion point in sorted chain
        // 2. Try coalescing with previous block
        // 3. Try coalescing with next block
        // 4. If size < 4, count as fragments
        // 5. Otherwise insert into chain
        // 6. Update n_free
    }
}
```

### 4. Implement defragment_page()
```rust
impl MemPage {
    /// Consolidate all free space by moving cells together.
    pub fn defragment(&mut self) {
        // 1. Collect all cells
        // 2. Move to contiguous area at end
        // 3. Clear free block chain
        // 4. Update cell pointers
    }
}
```

## Unit Tests Required

### Test 1: Basic free block allocation
```rust
#[test]
fn test_free_block_allocate() {
    let mut page = create_test_page();

    // Insert and delete to create free block
    let offset1 = page.allocate_space(100).unwrap();
    page.free_space(offset1, 100);

    // Should reuse freed space
    let offset2 = page.allocate_space(50).unwrap();
    assert_eq!(offset2, offset1);  // Reuses same location

    // Remaining space should still be available
    let offset3 = page.allocate_space(40).unwrap();
    assert!(offset3 > offset2);  // Uses remaining of split block
}
```

### Test 2: Free block coalescing
```rust
#[test]
fn test_free_block_coalesce() {
    let mut page = create_test_page();

    // Allocate three adjacent blocks
    let off1 = page.allocate_space(100).unwrap();
    let off2 = page.allocate_space(100).unwrap();
    let off3 = page.allocate_space(100).unwrap();

    // Free middle, then adjacent blocks
    page.free_space(off2, 100);
    page.free_space(off1, 100);  // Should coalesce with off2
    page.free_space(off3, 100);  // Should coalesce into single 300-byte block

    // Should be able to allocate 280 bytes (single coalesced block)
    let big = page.allocate_space(280).unwrap();
    assert!(big.is_some());
}
```

### Test 3: Fragmented bytes handling
```rust
#[test]
fn test_fragment_bytes() {
    let mut page = create_test_page();

    // Allocate and free space too small for free block
    let off = page.allocate_space(100).unwrap();
    page.free_space(off, 3);  // Less than 4 bytes

    // Should be counted as fragments
    assert_eq!(page.n_frag, 3);

    // Free block chain should be empty
    assert_eq!(page.first_free_block, 0);
}
```

### Test 4: Free block chain ordering
```rust
#[test]
fn test_free_block_chain_sorted() {
    let mut page = create_test_page();

    // Allocate multiple blocks
    let offsets: Vec<u16> = (0..5).map(|_| page.allocate_space(50).unwrap()).collect();

    // Free in random order
    page.free_space(offsets[2], 50);
    page.free_space(offsets[0], 50);
    page.free_space(offsets[4], 50);

    // Chain should be in offset order
    let chain = page.get_free_block_chain();
    for i in 1..chain.len() {
        assert!(chain[i-1] < chain[i], "Free block chain should be sorted");
    }
}
```

### Test 5: Defragmentation
```rust
#[test]
fn test_defragment_page() {
    let mut page = create_test_page();

    // Create fragmented page
    let offsets: Vec<u16> = (0..10).map(|_| page.allocate_space(100).unwrap()).collect();

    // Free alternating cells
    for i in (0..10).step_by(2) {
        page.free_space(offsets[i], 100);
    }

    // Page is now fragmented
    let chain_before = page.get_free_block_chain();
    assert!(chain_before.len() > 1);

    // Defragment
    page.defragment();

    // Should have single contiguous free area
    assert_eq!(page.first_free_block, 0);  // No free block chain
    // All free space consolidated at top of cell area
}
```

### Test 6: SQLite compatibility
```rust
#[test]
fn test_free_block_sqlite_compatible() {
    // Create page with SQLite, inspect free block structure
    // Create equivalent page with rustql
    // Compare byte-for-byte layout
}
```

### Test 7: Integration with delete
```rust
#[test]
fn test_delete_frees_space() {
    let mut btree = setup_btree();
    let mut cursor = btree.cursor();

    // Insert rows
    for i in 1..=100i64 {
        btree.insert(&mut cursor, i, &[0u8; 100], BtreeInsertFlags::empty(), 0).unwrap();
    }

    let page = btree.get_page(2).unwrap();
    let free_before = page.n_free;

    // Delete some rows
    for i in (1..=100i64).step_by(2) {
        cursor.seek_ge(i).unwrap();
        btree.delete(&mut cursor, 0).unwrap();
    }

    // Free space should have increased
    let page = btree.get_page(2).unwrap();
    assert!(page.n_free > free_before);

    // New inserts should reuse freed space
    btree.insert(&mut cursor, 200, &[0u8; 100], BtreeInsertFlags::empty(), 0).unwrap();
    // Should not have triggered page split
}
```

## Acceptance Criteria

- [ ] Page header stores first_free_block offset at byte 1-2
- [ ] Free blocks form sorted linked list within page
- [ ] allocate_space() searches free chain before using content area
- [ ] free_space() coalesces adjacent blocks
- [ ] Small freed spaces (<4 bytes) tracked as fragments
- [ ] defragment_page() consolidates all free space
- [ ] Delete operation properly frees cell space
- [ ] Free block structure matches SQLite byte layout
- [ ] All unit tests pass
