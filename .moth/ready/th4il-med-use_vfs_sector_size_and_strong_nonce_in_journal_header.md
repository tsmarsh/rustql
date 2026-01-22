# Use VFS sector size and strong nonce in journal header

## Problem
Journal headers are written with a hard-coded sector size (4096) and a time-based nonce. SQLite uses `xSectorSize()` and `sqlite3_randomness()` to populate these fields. Hardcoding and weak randomness break invariants around atomic writes and journal corruption detection.

Code refs:
- `src/storage/pager.rs:1180` (journal header uses sector size 4096)
- `src/storage/pager.rs:343` (time-based nonce)

## SQLite Behavior
SQLite writes the actual sector size from the VFS and uses strong randomness for the journal nonce. This is required to detect stale journals and to honor the atomic-write optimization safely.

## Expected Fix
- Use `VfsFile::sector_size()` when writing journal headers.
- Replace `rand_nonce()` with VFS randomness (`Vfs::randomness`) or a stronger RNG.
- Ensure checksum and header validation uses the updated nonce.

## Concrete Tests (Rust)
Add a test in `tests/pager_journal_header.rs` with a fake VFS/VfsFile:

```rust
// Fake VFS file returns sector size 512
let vfs = FakeVfs::new().with_sector_size(512);
let mut pager = Pager::open_with_vfs("test.db", vfs, ...)?;

pager.begin()?;
pager.write_page(...)?;

// Read first 28 bytes from -journal file
let header = read_journal_header("test.db-journal")?;
assert_eq!(header.sector_size, 512);

// Ensure nonce != 0 and changes between runs
```

## Success Criteria
- Journal header uses actual VFS sector size.
- Nonce is randomized (non-deterministic across runs).
- Tests validate header contents.
