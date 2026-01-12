//! VDBE Sorting Operations
//!
//! This module implements external merge sort for ORDER BY clauses when
//! the data doesn't fit in memory. It corresponds to SQLite's vdbesort.c.
//!
//! The sorter uses a multi-phase approach:
//! 1. Accumulate records in memory until limit is reached
//! 2. Sort in-memory records and write to temp file as a "PMA" (sorted run)
//! 3. When reading, merge all PMAs using a tournament tree

use std::cmp::Ordering;
use std::sync::Arc;

use crate::error::{Error, ErrorCode, Result};
use crate::vdbe::auxdata::{decode_record_header, get_varint, put_varint, SerialType};
use crate::vdbe::mem::Mem;
use crate::vdbe::ops::KeyInfo;

// ============================================================================
// Constants
// ============================================================================

/// Default memory limit (1MB)
const DEFAULT_MEM_LIMIT: i64 = 1024 * 1024;

/// Maximum PMAs to merge at once
const MAX_MERGE_COUNT: usize = 16;

// ============================================================================
// Sorter State
// ============================================================================

/// State of the sorter
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SorterState {
    /// Adding records
    #[default]
    Building,
    /// In-memory sort complete
    Sorted,
    /// External merge in progress
    Merging,
}

// ============================================================================
// Sorter Record
// ============================================================================

/// A single record being sorted
#[derive(Debug, Clone)]
pub struct SorterRecord {
    /// Serialized key (record format)
    key: Vec<u8>,
    /// Optional data payload
    data: Option<Vec<u8>>,
}

impl SorterRecord {
    /// Create a new sorter record
    pub fn new(key: Vec<u8>, data: Option<Vec<u8>>) -> Self {
        Self { key, data }
    }

    /// Get the key bytes
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Get the data bytes (if any)
    pub fn data(&self) -> Option<&[u8]> {
        self.data.as_deref()
    }

    /// Get the total size of this record
    pub fn size(&self) -> usize {
        self.key.len() + self.data.as_ref().map_or(0, |d| d.len())
    }
}

// ============================================================================
// VdbeSorter
// ============================================================================

/// Main sorter object for ORDER BY implementation
///
/// This corresponds to SQLite's VdbeSorter structure.
pub struct VdbeSorter {
    /// Key comparison info
    key_info: Arc<KeyInfo>,
    /// Memory limit before spilling to disk
    mem_limit: i64,
    /// Current memory usage
    mem_used: i64,
    /// In-memory records
    records: Vec<SorterRecord>,
    /// PMAs (sorted runs) stored as Vec<u8> for simplicity
    pmas: Vec<Vec<u8>>,
    /// Current index for iteration (in-memory)
    current_idx: usize,
    /// Merge engine for external merge
    merge: Option<MergeEngine>,
    /// Sort state
    state: SorterState,
}

impl VdbeSorter {
    /// Create a new sorter with the given key info
    pub fn new(key_info: Arc<KeyInfo>) -> Self {
        Self {
            key_info,
            mem_limit: DEFAULT_MEM_LIMIT,
            mem_used: 0,
            records: Vec::new(),
            pmas: Vec::new(),
            current_idx: 0,
            merge: None,
            state: SorterState::Building,
        }
    }

    /// Create a sorter with custom memory limit
    pub fn with_mem_limit(key_info: Arc<KeyInfo>, mem_limit: i64) -> Self {
        let mut sorter = Self::new(key_info);
        sorter.mem_limit = mem_limit;
        sorter
    }

    /// Get the current state
    pub fn state(&self) -> SorterState {
        self.state
    }

    /// Add a record to be sorted
    pub fn write(&mut self, key: &[u8], data: Option<&[u8]>) -> Result<()> {
        if self.state != SorterState::Building {
            return Err(Error::with_message(
                ErrorCode::Misuse,
                "cannot write to sorter after rewind",
            ));
        }

        let record = SorterRecord::new(key.to_vec(), data.map(|d| d.to_vec()));

        let size = record.size();
        self.mem_used += size as i64;
        self.records.push(record);

        // Check if we need to spill to disk
        if self.mem_used > self.mem_limit {
            self.spill_to_disk()?;
        }

        Ok(())
    }

    /// Sort in-memory records
    fn sort_in_memory(&mut self) {
        let key_info = Arc::clone(&self.key_info);

        self.records
            .sort_by(|a, b| compare_records(&a.key, &b.key, &key_info));
    }

    /// Flush in-memory records to a PMA (sorted run)
    fn spill_to_disk(&mut self) -> Result<()> {
        if self.records.is_empty() {
            return Ok(());
        }

        // Sort in-memory records
        self.sort_in_memory();

        // Create PMA buffer
        let mut pma = Vec::new();

        // Write each record
        for record in &self.records {
            // Write key length as varint
            let key_len = record.key.len() as u64;
            put_varint(&mut pma, key_len);

            // Write key data
            pma.extend_from_slice(&record.key);

            // Write data length as varint (0 if none)
            let data_len = record.data.as_ref().map_or(0, |d| d.len()) as u64;
            put_varint(&mut pma, data_len);

            // Write data if present
            if let Some(data) = &record.data {
                pma.extend_from_slice(data);
            }
        }

        // Write end marker (0)
        pma.push(0);

        self.pmas.push(pma);

        // Clear memory
        self.records.clear();
        self.mem_used = 0;

        Ok(())
    }

    /// Prepare for reading sorted results
    pub fn rewind(&mut self) -> Result<()> {
        match self.state {
            SorterState::Building => {
                if self.pmas.is_empty() {
                    // All in memory - just sort
                    self.sort_in_memory();
                    self.current_idx = 0;
                    self.state = SorterState::Sorted;
                } else {
                    // Need to merge PMAs with any remaining in-memory records
                    if !self.records.is_empty() {
                        self.spill_to_disk()?;
                    }
                    self.start_merge()?;
                    self.state = SorterState::Merging;
                }
            }
            SorterState::Sorted => {
                // Reset to beginning
                self.current_idx = 0;
            }
            SorterState::Merging => {
                // Restart merge - recreate merge engine
                self.start_merge()?;
            }
        }

        Ok(())
    }

    /// Start the merge process
    fn start_merge(&mut self) -> Result<()> {
        if self.pmas.is_empty() {
            return Ok(());
        }

        // Create PMA readers
        let mut readers: Vec<PmaReader> = Vec::new();
        for pma in &self.pmas {
            let reader = PmaReader::new(pma.clone())?;
            readers.push(reader);
        }

        // Create merge engine
        let merge = MergeEngine::new(readers, Arc::clone(&self.key_info))?;
        self.merge = Some(merge);

        Ok(())
    }

    /// Check if there are more records
    pub fn eof(&self) -> bool {
        match self.state {
            SorterState::Building => true,
            SorterState::Sorted => self.current_idx >= self.records.len(),
            SorterState::Merging => self.merge.as_ref().is_none_or(|m| m.eof()),
        }
    }

    /// Get current record key
    pub fn key(&self) -> Option<&[u8]> {
        match self.state {
            SorterState::Building => None,
            SorterState::Sorted => self.records.get(self.current_idx).map(|r| r.key()),
            SorterState::Merging => self.merge.as_ref().and_then(|m| m.key()),
        }
    }

    /// Get current record data
    pub fn data(&self) -> Option<&[u8]> {
        match self.state {
            SorterState::Building => None,
            SorterState::Sorted => self.records.get(self.current_idx).and_then(|r| r.data()),
            SorterState::Merging => self.merge.as_ref().and_then(|m| m.data()),
        }
    }

    /// Advance to next record
    pub fn next(&mut self) -> Result<()> {
        match self.state {
            SorterState::Building => {
                return Err(Error::with_message(
                    ErrorCode::Misuse,
                    "must call rewind before next",
                ));
            }
            SorterState::Sorted => {
                if self.current_idx < self.records.len() {
                    self.current_idx += 1;
                }
            }
            SorterState::Merging => {
                if let Some(ref mut merge) = self.merge {
                    merge.next()?;
                }
            }
        }

        Ok(())
    }

    /// Get the number of records (only valid for in-memory sort)
    pub fn count(&self) -> usize {
        match self.state {
            SorterState::Sorted => self.records.len(),
            _ => 0,
        }
    }

    /// Reset the sorter for reuse
    pub fn reset(&mut self) {
        self.records.clear();
        self.pmas.clear();
        self.mem_used = 0;
        self.current_idx = 0;
        self.merge = None;
        self.state = SorterState::Building;
    }
}

// ============================================================================
// PMA Reader
// ============================================================================

/// Reader for a single PMA (sorted run)
struct PmaReader {
    /// PMA data
    data: Vec<u8>,
    /// Current position in data
    pos: usize,
    /// Current record key
    current_key: Option<Vec<u8>>,
    /// Current record data
    current_data: Option<Vec<u8>>,
    /// Whether we've reached the end
    at_eof: bool,
}

impl PmaReader {
    /// Create a new PMA reader
    fn new(data: Vec<u8>) -> Result<Self> {
        let mut reader = Self {
            data,
            pos: 0,
            current_key: None,
            current_data: None,
            at_eof: false,
        };

        // Read first record
        reader.read_next()?;

        Ok(reader)
    }

    /// Read the next record from the PMA
    fn read_next(&mut self) -> Result<()> {
        if self.pos >= self.data.len() {
            self.at_eof = true;
            self.current_key = None;
            self.current_data = None;
            return Ok(());
        }

        // Read key length
        let (key_len, consumed) = get_varint(&self.data[self.pos..]);
        self.pos += consumed;

        if key_len == 0 {
            // End marker
            self.at_eof = true;
            self.current_key = None;
            self.current_data = None;
            return Ok(());
        }

        // Read key
        let key_len = key_len as usize;
        if self.pos + key_len > self.data.len() {
            return Err(Error::with_message(ErrorCode::Corrupt, "truncated PMA key"));
        }
        let key = self.data[self.pos..self.pos + key_len].to_vec();
        self.pos += key_len;

        // Read data length
        let (data_len, consumed) = get_varint(&self.data[self.pos..]);
        self.pos += consumed;

        // Read data
        let data = if data_len > 0 {
            let data_len = data_len as usize;
            if self.pos + data_len > self.data.len() {
                return Err(Error::with_message(
                    ErrorCode::Corrupt,
                    "truncated PMA data",
                ));
            }
            let data = self.data[self.pos..self.pos + data_len].to_vec();
            self.pos += data_len;
            Some(data)
        } else {
            None
        };

        self.current_key = Some(key);
        self.current_data = data;

        Ok(())
    }

    /// Check if at end of file
    fn eof(&self) -> bool {
        self.at_eof
    }

    /// Get current key
    fn key(&self) -> Option<&[u8]> {
        self.current_key.as_deref()
    }

    /// Get current data
    fn data(&self) -> Option<&[u8]> {
        self.current_data.as_deref()
    }

    /// Advance to next record
    fn advance(&mut self) -> Result<()> {
        self.read_next()
    }
}

// ============================================================================
// Merge Engine
// ============================================================================

/// Engine for k-way merge of sorted PMAs
struct MergeEngine {
    /// Readers for each PMA
    readers: Vec<PmaReader>,
    /// Key info for comparisons
    key_info: Arc<KeyInfo>,
    /// Tournament tree - tree[i] is the index of the winning reader at position i
    /// tree[1] is the overall winner
    tree: Vec<usize>,
    /// Number of readers
    n_readers: usize,
}

impl MergeEngine {
    /// Create a new merge engine
    fn new(readers: Vec<PmaReader>, key_info: Arc<KeyInfo>) -> Result<Self> {
        let n = readers.len();
        if n == 0 {
            return Ok(Self {
                readers,
                key_info,
                tree: Vec::new(),
                n_readers: 0,
            });
        }

        // Tree size: 2*n elements (1-indexed, index 0 unused)
        let tree = vec![0usize; 2 * n];

        let mut engine = Self {
            readers,
            key_info,
            tree,
            n_readers: n,
        };

        engine.build_tree()?;

        Ok(engine)
    }

    /// Build the tournament tree
    fn build_tree(&mut self) -> Result<()> {
        let n = self.n_readers;
        if n == 0 {
            return Ok(());
        }

        // Initialize leaves (positions n to 2n-1)
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

    /// Compare two readers and return the index of the winner (smaller key)
    fn winner(&self, a: usize, b: usize) -> Result<usize> {
        let key_a = self.readers.get(a).and_then(|r| r.key());
        let key_b = self.readers.get(b).and_then(|r| r.key());

        match (key_a, key_b) {
            (None, None) => Ok(a),
            (Some(_), None) => Ok(a),
            (None, Some(_)) => Ok(b),
            (Some(ka), Some(kb)) => {
                let cmp = compare_records(ka, kb, &self.key_info);
                if cmp == Ordering::Greater {
                    Ok(b)
                } else {
                    Ok(a)
                }
            }
        }
    }

    /// Replay the tree after advancing a reader
    fn replay_tree(&mut self, changed: usize) -> Result<()> {
        let n = self.n_readers;
        if n == 0 {
            return Ok(());
        }

        // Start at the leaf position for this reader
        let mut pos = n + changed;

        // Replay up to the root
        while pos > 1 {
            let sibling = if pos.is_multiple_of(2) {
                pos + 1
            } else {
                pos - 1
            };
            let parent = pos / 2;

            let left = self.tree[pos];
            let right = if sibling < self.tree.len() {
                self.tree[sibling]
            } else {
                left
            };

            // Ensure we compare in consistent order
            let (a, b) = if pos.is_multiple_of(2) {
                (left, right)
            } else {
                (right, left)
            };
            self.tree[parent] = self.winner(a, b)?;

            pos = parent;
        }

        Ok(())
    }

    /// Check if all readers are exhausted
    fn eof(&self) -> bool {
        if self.n_readers == 0 {
            return true;
        }
        // Check if the winner has no more data
        let winner = self.tree.get(1).copied().unwrap_or(0);
        self.readers.get(winner).is_none_or(|r| r.eof())
    }

    /// Get the current winner's key
    fn key(&self) -> Option<&[u8]> {
        if self.n_readers == 0 {
            return None;
        }
        let winner = self.tree.get(1).copied()?;
        self.readers.get(winner).and_then(|r| r.key())
    }

    /// Get the current winner's data
    fn data(&self) -> Option<&[u8]> {
        if self.n_readers == 0 {
            return None;
        }
        let winner = self.tree.get(1).copied()?;
        self.readers.get(winner).and_then(|r| r.data())
    }

    /// Advance to the next record
    fn next(&mut self) -> Result<()> {
        if self.n_readers == 0 {
            return Ok(());
        }

        // Get current winner
        let winner = self.tree.get(1).copied().unwrap_or(0);

        // Advance that reader
        if let Some(reader) = self.readers.get_mut(winner) {
            reader.advance()?;
        }

        // Replay tree
        self.replay_tree(winner)?;

        Ok(())
    }
}

// ============================================================================
// Record Comparison
// ============================================================================

/// Compare two record keys using KeyInfo
fn compare_records(key_a: &[u8], key_b: &[u8], key_info: &KeyInfo) -> Ordering {
    // Parse keys
    let fields_a = parse_record(key_a);
    let fields_b = parse_record(key_b);

    // Compare field by field
    let n_fields = (key_info.n_key_field as usize)
        .min(fields_a.len())
        .min(fields_b.len());

    for i in 0..n_fields {
        let desc = key_info.sort_orders.get(i).copied().unwrap_or(false);
        let coll_name = key_info
            .collations
            .get(i)
            .map(|s| s.as_str())
            .unwrap_or("BINARY");

        let cmp = compare_mem(&fields_a[i], &fields_b[i], coll_name);

        if cmp != Ordering::Equal {
            return if desc { cmp.reverse() } else { cmp };
        }
    }

    // Compare by number of fields if all compared fields are equal
    fields_a.len().cmp(&fields_b.len())
}

/// Parse a record into Mem values
fn parse_record(data: &[u8]) -> Vec<Mem> {
    if data.is_empty() {
        return Vec::new();
    }

    // Decode record header
    let result = decode_record_header(data);
    if result.is_err() {
        return Vec::new();
    }

    let (types, header_end) = result.unwrap();
    let mut fields = Vec::new();
    let mut offset = header_end;

    for st in types {
        // For types with 0-size data (Zero, One, Null), we still need to create the Mem
        let remaining = if offset < data.len() {
            &data[offset..]
        } else {
            &[][..]
        };

        let (mem, size) = deserialize_mem(remaining, &st);
        fields.push(mem);
        offset += size;
    }

    fields
}

/// Deserialize a Mem from data with given serial type
fn deserialize_mem(data: &[u8], st: &SerialType) -> (Mem, usize) {
    match st {
        SerialType::Null => (Mem::new(), 0),
        SerialType::Int8 => {
            if data.is_empty() {
                return (Mem::from_int(0), 0);
            }
            let val = data[0] as i8 as i64;
            (Mem::from_int(val), 1)
        }
        SerialType::Int16 => {
            if data.len() < 2 {
                return (Mem::from_int(0), 0);
            }
            let val = i16::from_be_bytes([data[0], data[1]]) as i64;
            (Mem::from_int(val), 2)
        }
        SerialType::Int24 => {
            if data.len() < 3 {
                return (Mem::from_int(0), 0);
            }
            let sign_extend = if data[0] & 0x80 != 0 { 0xFF } else { 0x00 };
            let val = i32::from_be_bytes([sign_extend, data[0], data[1], data[2]]) as i64;
            (Mem::from_int(val), 3)
        }
        SerialType::Int32 => {
            if data.len() < 4 {
                return (Mem::from_int(0), 0);
            }
            let val = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as i64;
            (Mem::from_int(val), 4)
        }
        SerialType::Int48 => {
            if data.len() < 6 {
                return (Mem::from_int(0), 0);
            }
            let sign_extend = if data[0] & 0x80 != 0 {
                [0xFF, 0xFF]
            } else {
                [0x00, 0x00]
            };
            let val = i64::from_be_bytes([
                sign_extend[0],
                sign_extend[1],
                data[0],
                data[1],
                data[2],
                data[3],
                data[4],
                data[5],
            ]);
            (Mem::from_int(val), 6)
        }
        SerialType::Int64 => {
            if data.len() < 8 {
                return (Mem::from_int(0), 0);
            }
            let val = i64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            (Mem::from_int(val), 8)
        }
        SerialType::Float64 => {
            if data.len() < 8 {
                return (Mem::from_real(0.0), 0);
            }
            let val = f64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            (Mem::from_real(val), 8)
        }
        SerialType::Zero => (Mem::from_int(0), 0),
        SerialType::One => (Mem::from_int(1), 0),
        SerialType::Text(len) => {
            let len = *len as usize;
            if data.len() < len {
                return (Mem::from_str(""), 0);
            }
            let s = String::from_utf8_lossy(&data[..len]).into_owned();
            (Mem::from_str(&s), len)
        }
        SerialType::Blob(len) => {
            let len = *len as usize;
            if data.len() < len {
                return (Mem::from_blob(&[]), 0);
            }
            (Mem::from_blob(&data[..len]), len)
        }
        SerialType::Reserved(_) => (Mem::new(), 0),
    }
}

/// Compare two Mem values with collation
fn compare_mem(a: &Mem, b: &Mem, coll_name: &str) -> Ordering {
    // Use Mem's built-in comparison
    // For text types, apply collation
    if a.is_str() && b.is_str() {
        let sa = a.to_str();
        let sb = b.to_str();

        match coll_name.to_uppercase().as_str() {
            "NOCASE" => sa.to_ascii_lowercase().cmp(&sb.to_ascii_lowercase()),
            "RTRIM" => sa.trim_end().cmp(sb.trim_end()),
            _ => sa.cmp(&sb), // BINARY or default
        }
    } else {
        a.compare(b)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_int_record(value: i64) -> Vec<u8> {
        // Simple record format: header + int
        let mut record = Vec::new();

        // Header: header_size (1 byte) + serial type for int
        let serial_type = if value == 0 {
            8u8 // Zero
        } else if value == 1 {
            9u8 // One
        } else if value >= -128 && value <= 127 {
            1u8 // Int8
        } else if value >= -32768 && value <= 32767 {
            2u8 // Int16
        } else if value >= -2147483648 && value <= 2147483647 {
            4u8 // Int32
        } else {
            6u8 // Int64
        };

        // Header size
        record.push(2);
        // Serial type
        record.push(serial_type);

        // Data
        match serial_type {
            8 | 9 => {} // Zero/One have no data
            1 => record.push(value as u8),
            2 => record.extend(&(value as i16).to_be_bytes()),
            4 => record.extend(&(value as i32).to_be_bytes()),
            6 => record.extend(&value.to_be_bytes()),
            _ => {}
        }

        record
    }

    #[test]
    fn test_sorter_in_memory() {
        let key_info = Arc::new(KeyInfo::new(1));
        let mut sorter = VdbeSorter::new(key_info);

        // Add records in unsorted order
        sorter.write(&make_int_record(30), None).unwrap();
        sorter.write(&make_int_record(10), None).unwrap();
        sorter.write(&make_int_record(20), None).unwrap();

        // Rewind to start iteration
        sorter.rewind().unwrap();
        assert_eq!(sorter.state(), SorterState::Sorted);

        // Read in sorted order
        assert!(!sorter.eof());

        // First should be 10
        let key = sorter.key().unwrap();
        let fields = parse_record(key);
        assert_eq!(fields[0].to_int(), 10);

        sorter.next().unwrap();
        let key = sorter.key().unwrap();
        let fields = parse_record(key);
        assert_eq!(fields[0].to_int(), 20);

        sorter.next().unwrap();
        let key = sorter.key().unwrap();
        let fields = parse_record(key);
        assert_eq!(fields[0].to_int(), 30);

        sorter.next().unwrap();
        assert!(sorter.eof());
    }

    #[test]
    fn test_sorter_with_spill() {
        let key_info = Arc::new(KeyInfo::new(1));
        // Very small memory limit to force spilling
        let mut sorter = VdbeSorter::with_mem_limit(key_info, 50);

        // Add enough records to trigger spill
        for i in (0..20).rev() {
            sorter.write(&make_int_record(i), None).unwrap();
        }

        sorter.rewind().unwrap();
        assert_eq!(sorter.state(), SorterState::Merging);

        // Verify sorted order
        let mut prev = i64::MIN;
        let mut count = 0;
        while !sorter.eof() {
            let key = sorter.key().unwrap();
            let fields = parse_record(key);
            if fields.is_empty() {
                // Skip records that couldn't be parsed
                sorter.next().unwrap();
                continue;
            }
            let val = fields[0].to_int();
            assert!(val >= prev, "out of order: {} < {}", val, prev);
            prev = val;
            count += 1;
            sorter.next().unwrap();
        }
        assert_eq!(count, 20, "should have 20 records");
    }

    #[test]
    fn test_sorter_descending() {
        let mut key_info = KeyInfo::new(1);
        key_info.sort_orders = vec![true]; // DESC
        let key_info = Arc::new(key_info);

        let mut sorter = VdbeSorter::new(key_info);

        sorter.write(&make_int_record(10), None).unwrap();
        sorter.write(&make_int_record(30), None).unwrap();
        sorter.write(&make_int_record(20), None).unwrap();

        sorter.rewind().unwrap();

        // Should be descending: 30, 20, 10
        let key = sorter.key().unwrap();
        let fields = parse_record(key);
        assert_eq!(fields[0].to_int(), 30);

        sorter.next().unwrap();
        let key = sorter.key().unwrap();
        let fields = parse_record(key);
        assert_eq!(fields[0].to_int(), 20);

        sorter.next().unwrap();
        let key = sorter.key().unwrap();
        let fields = parse_record(key);
        assert_eq!(fields[0].to_int(), 10);
    }

    #[test]
    fn test_sorter_reset() {
        let key_info = Arc::new(KeyInfo::new(1));
        let mut sorter = VdbeSorter::new(key_info);

        sorter.write(&make_int_record(10), None).unwrap();
        sorter.rewind().unwrap();
        assert_eq!(sorter.count(), 1);

        sorter.reset();
        assert_eq!(sorter.state(), SorterState::Building);

        sorter.write(&make_int_record(20), None).unwrap();
        sorter.write(&make_int_record(30), None).unwrap();
        sorter.rewind().unwrap();
        assert_eq!(sorter.count(), 2);
    }

    #[test]
    fn test_sorter_with_data() {
        let key_info = Arc::new(KeyInfo::new(1));
        let mut sorter = VdbeSorter::new(key_info);

        sorter.write(&make_int_record(20), Some(b"data20")).unwrap();
        sorter.write(&make_int_record(10), Some(b"data10")).unwrap();

        sorter.rewind().unwrap();

        // First record should be key=10, data=data10
        assert_eq!(sorter.data(), Some(b"data10".as_slice()));
        sorter.next().unwrap();
        assert_eq!(sorter.data(), Some(b"data20".as_slice()));
    }

    #[test]
    fn test_pma_reader() {
        // Create a simple PMA
        let mut pma = Vec::new();

        // Record 1: key = [1, 2], data = [3, 4]
        put_varint(&mut pma, 2); // key len
        pma.extend_from_slice(&[1, 2]);
        put_varint(&mut pma, 2); // data len
        pma.extend_from_slice(&[3, 4]);

        // Record 2: key = [5], no data
        put_varint(&mut pma, 1); // key len
        pma.push(5);
        put_varint(&mut pma, 0); // data len

        // End marker
        pma.push(0);

        let mut reader = PmaReader::new(pma).unwrap();

        assert!(!reader.eof());
        assert_eq!(reader.key(), Some(&[1u8, 2][..]));
        assert_eq!(reader.data(), Some(&[3u8, 4][..]));

        reader.advance().unwrap();
        assert!(!reader.eof());
        assert_eq!(reader.key(), Some(&[5u8][..]));
        assert_eq!(reader.data(), None);

        reader.advance().unwrap();
        assert!(reader.eof());
    }

    #[test]
    fn test_compare_records() {
        let key_info = KeyInfo::new(1);

        let rec1 = make_int_record(10);
        let rec2 = make_int_record(20);
        let rec3 = make_int_record(10);

        assert_eq!(compare_records(&rec1, &rec2, &key_info), Ordering::Less);
        assert_eq!(compare_records(&rec2, &rec1, &key_info), Ordering::Greater);
        assert_eq!(compare_records(&rec1, &rec3, &key_info), Ordering::Equal);
    }
}
