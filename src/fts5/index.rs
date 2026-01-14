use crate::error::{Error, ErrorCode, Result};
use crate::vdbe::auxdata::{get_varint, put_varint};

pub const FTS5_OPT_WORK_UNIT: usize = 1000;
pub const FTS5_WORK_UNIT: usize = 64;
pub const FTS5_MIN_DLIDX_SIZE: usize = 4;
pub const FTS5_MAIN_PREFIX: u8 = b'0';
pub const FTS5_MAX_LEVEL: usize = 64;
pub const FTS5_MAX_TOKEN_SIZE: usize = 32768;
pub const FTS5_MAX_SEGMENT: usize = 2000;
pub const FTS5_STRUCTURE_V2: [u8; 4] = [0xff, 0x00, 0x00, 0x01];

#[derive(Debug, Clone, Default)]
pub struct Fts5Buffer {
    data: Vec<u8>,
}

impl Fts5Buffer {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn append_blob(&mut self, bytes: &[u8]) {
        if !bytes.is_empty() {
            self.data.extend_from_slice(bytes);
        }
    }

    pub fn append_varint(&mut self, value: u64) {
        put_varint(&mut self.data, value);
    }
}

pub fn fts5_put_u32_be(out: &mut [u8], value: u32) {
    out[0] = ((value >> 24) & 0xff) as u8;
    out[1] = ((value >> 16) & 0xff) as u8;
    out[2] = ((value >> 8) & 0xff) as u8;
    out[3] = (value & 0xff) as u8;
}

pub fn fts5_get_u32_be(input: &[u8]) -> u32 {
    ((input[0] as u32) << 24)
        | ((input[1] as u32) << 16)
        | ((input[2] as u32) << 8)
        | (input[3] as u32)
}

fn fts5_get_varint(buf: &[u8], offset: &mut usize) -> Result<u64> {
    if *offset >= buf.len() {
        return Err(Error::with_message(
            ErrorCode::Corrupt,
            "unexpected end of buffer",
        ));
    }
    let (value, consumed) = get_varint(&buf[*offset..]);
    if consumed == 0 {
        return Err(Error::with_message(ErrorCode::Corrupt, "invalid varint"));
    }
    *offset += consumed;
    Ok(value)
}

fn fts5_get_varint32(buf: &[u8], offset: &mut usize) -> Result<u32> {
    let value = fts5_get_varint(buf, offset)?;
    u32::try_from(value).map_err(|_| Error::with_message(ErrorCode::Corrupt, "varint overflow"))
}

pub fn fts5_poslist_next64(data: &[u8], offset: &mut usize, current: &mut i64) -> Result<bool> {
    if *offset >= data.len() {
        *current = -1;
        return Ok(true);
    }

    let mut i = *offset;
    let mut ioff = *current;
    let val = fts5_get_varint32(data, &mut i)?;

    if val <= 1 {
        if val == 0 {
            *offset = i;
            return Ok(false);
        }
        let hi = fts5_get_varint32(data, &mut i)?;
        ioff = (hi as i64) << 32;
        let lo = fts5_get_varint32(data, &mut i)?;
        if lo < 2 {
            *current = -1;
            return Ok(true);
        }
        *current = ioff + ((lo - 2) & 0x7fff_ffff) as i64;
    } else {
        let hi = (ioff as i64) & ((0x7fff_ffff_i64) << 32);
        let lo = (ioff as i64 + (val as i64 - 2)) & 0x7fff_ffff;
        *current = hi + lo;
    }

    *offset = i;
    Ok(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fts5Position {
    pub column: i32,
    pub offset: i32,
}

#[derive(Debug, Clone)]
pub struct Fts5DoclistEntry {
    pub rowid: i64,
    pub positions: Vec<Fts5Position>,
    pub deleted: bool,
}

#[derive(Debug, Clone)]
pub struct Fts5StructureSegment {
    pub segid: i32,
    pub pgno_first: i32,
    pub pgno_last: i32,
    pub origin1: u64,
    pub origin2: u64,
    pub n_pg_tombstone: i32,
    pub n_entry_tombstone: u64,
    pub n_entry: u64,
}

#[derive(Debug, Clone)]
pub struct Fts5StructureLevel {
    pub n_merge: i32,
    pub segments: Vec<Fts5StructureSegment>,
}

#[derive(Debug, Clone)]
pub struct Fts5Structure {
    pub cookie: u32,
    pub n_write_counter: u64,
    pub n_origin_counter: u64,
    pub levels: Vec<Fts5StructureLevel>,
}

pub fn decode_structure(data: &[u8]) -> Result<Fts5Structure> {
    if data.len() < 4 {
        return Err(Error::with_message(
            ErrorCode::Corrupt,
            "structure record too short",
        ));
    }

    let cookie = fts5_get_u32_be(&data[0..4]);
    let mut offset = 4usize;
    let mut is_v2 = false;
    if data.len() >= 8 && data[offset..offset + 4] == FTS5_STRUCTURE_V2 {
        offset += 4;
        is_v2 = true;
    }

    let n_level = fts5_get_varint32(data, &mut offset)? as usize;
    let mut n_segment = fts5_get_varint32(data, &mut offset)? as usize;
    if n_level > FTS5_MAX_SEGMENT || n_segment > FTS5_MAX_SEGMENT {
        return Err(Error::with_message(
            ErrorCode::Corrupt,
            "structure size overflow",
        ));
    }

    let n_write_counter = fts5_get_varint(data, &mut offset)?;
    let mut levels: Vec<Fts5StructureLevel> = Vec::with_capacity(n_level);
    let mut max_origin = 0u64;

    for level_idx in 0..n_level {
        let n_merge = fts5_get_varint32(data, &mut offset)? as i32;
        let n_total = fts5_get_varint32(data, &mut offset)? as usize;
        if n_total < n_merge as usize {
            return Err(Error::with_message(
                ErrorCode::Corrupt,
                "merge count exceeds segment count",
            ));
        }
        if n_total > n_segment {
            return Err(Error::with_message(
                ErrorCode::Corrupt,
                "segment count underflow",
            ));
        }
        n_segment -= n_total;

        let mut segments = Vec::with_capacity(n_total);
        for _ in 0..n_total {
            let segid = fts5_get_varint32(data, &mut offset)? as i32;
            let pgno_first = fts5_get_varint32(data, &mut offset)? as i32;
            let pgno_last = fts5_get_varint32(data, &mut offset)? as i32;
            if pgno_last < pgno_first {
                return Err(Error::with_message(
                    ErrorCode::Corrupt,
                    "invalid segment page range",
                ));
            }

            let mut segment = Fts5StructureSegment {
                segid,
                pgno_first,
                pgno_last,
                origin1: 0,
                origin2: 0,
                n_pg_tombstone: 0,
                n_entry_tombstone: 0,
                n_entry: 0,
            };

            if is_v2 {
                segment.origin1 = fts5_get_varint(data, &mut offset)?;
                segment.origin2 = fts5_get_varint(data, &mut offset)?;
                segment.n_pg_tombstone = fts5_get_varint32(data, &mut offset)? as i32;
                segment.n_entry_tombstone = fts5_get_varint(data, &mut offset)?;
                segment.n_entry = fts5_get_varint(data, &mut offset)?;
                if segment.origin2 > max_origin {
                    max_origin = segment.origin2;
                }
            }

            segments.push(segment);
        }

        if level_idx + 1 == n_level && n_merge > 0 {
            return Err(Error::with_message(
                ErrorCode::Corrupt,
                "merge segments on final level",
            ));
        }
        if level_idx > 0 && levels[level_idx - 1].n_merge > 0 && n_total == 0 {
            return Err(Error::with_message(ErrorCode::Corrupt, "empty merge level"));
        }

        levels.push(Fts5StructureLevel { n_merge, segments });
    }

    if n_segment != 0 {
        return Err(Error::with_message(
            ErrorCode::Corrupt,
            "segment count mismatch",
        ));
    }

    let n_origin_counter = if is_v2 { max_origin + 1 } else { 0 };
    Ok(Fts5Structure {
        cookie,
        n_write_counter,
        n_origin_counter,
        levels,
    })
}

pub fn encode_structure(structure: &Fts5Structure) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&structure.cookie.to_be_bytes());
    let is_v2 = structure.n_origin_counter > 0;
    if is_v2 {
        buf.extend_from_slice(&FTS5_STRUCTURE_V2);
    }

    let n_level = structure.levels.len() as u64;
    let n_segment: u64 = structure
        .levels
        .iter()
        .map(|level| level.segments.len() as u64)
        .sum();
    put_varint(&mut buf, n_level);
    put_varint(&mut buf, n_segment);
    put_varint(&mut buf, structure.n_write_counter);

    for level in &structure.levels {
        put_varint(&mut buf, level.n_merge.max(0) as u64);
        put_varint(&mut buf, level.segments.len() as u64);
        for seg in &level.segments {
            put_varint(&mut buf, seg.segid as u64);
            put_varint(&mut buf, seg.pgno_first as u64);
            put_varint(&mut buf, seg.pgno_last as u64);
            if is_v2 {
                put_varint(&mut buf, seg.origin1);
                put_varint(&mut buf, seg.origin2);
                put_varint(&mut buf, seg.n_pg_tombstone.max(0) as u64);
                put_varint(&mut buf, seg.n_entry_tombstone);
                put_varint(&mut buf, seg.n_entry);
            }
        }
    }

    buf
}

pub fn encode_leaf_header(first_rowid_offset: u16, footer_offset: u16) -> [u8; 4] {
    [
        ((first_rowid_offset >> 8) & 0xff) as u8,
        (first_rowid_offset & 0xff) as u8,
        ((footer_offset >> 8) & 0xff) as u8,
        (footer_offset & 0xff) as u8,
    ]
}

pub fn decode_leaf_header(data: &[u8]) -> Result<(u16, u16)> {
    if data.len() < 4 {
        return Err(Error::with_message(
            ErrorCode::Corrupt,
            "leaf header too short",
        ));
    }
    let first = ((data[0] as u16) << 8) | data[1] as u16;
    let footer = ((data[2] as u16) << 8) | data[3] as u16;
    Ok((first, footer))
}

pub fn encode_leaf_footer(offsets: &[usize]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut prev = 0usize;
    for offset in offsets {
        if *offset < prev {
            return Err(Error::with_message(
                ErrorCode::Corrupt,
                "leaf footer offsets not ordered",
            ));
        }
        let delta = (*offset - prev) as u64;
        put_varint(&mut buf, delta);
        prev = *offset;
    }
    Ok(buf)
}

pub fn decode_leaf_footer(
    data: &[u8],
    footer_offset: usize,
    footer_len: usize,
) -> Result<Vec<usize>> {
    if footer_offset > data.len() {
        return Err(Error::with_message(
            ErrorCode::Corrupt,
            "leaf footer offset out of bounds",
        ));
    }
    let footer_end = footer_offset
        .checked_add(footer_len)
        .ok_or_else(|| Error::with_message(ErrorCode::Corrupt, "leaf footer size overflow"))?;
    if footer_end > data.len() {
        return Err(Error::with_message(
            ErrorCode::Corrupt,
            "leaf footer out of bounds",
        ));
    }
    let mut offsets = Vec::new();
    let mut cursor = footer_offset;
    let mut prev = 0usize;
    while cursor < footer_end {
        let (value, consumed) = get_varint(&data[cursor..footer_end]);
        if consumed == 0 {
            return Err(Error::with_message(
                ErrorCode::Corrupt,
                "invalid leaf footer varint",
            ));
        }
        cursor += consumed;
        let next = prev + value as usize;
        offsets.push(next);
        prev = next;
    }
    Ok(offsets)
}

#[derive(Debug, Clone)]
pub struct Fts5Segment {
    terms: std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Fts5Segment {
    pub fn new() -> Self {
        Self {
            terms: std::collections::BTreeMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    pub fn add_entry(&mut self, term: &[u8], entry: Fts5DoclistEntry) -> Result<()> {
        let doclist = self.terms.entry(term.to_vec()).or_default();
        if doclist.is_empty() {
            *doclist = encode_doclist(&[entry])?;
            return Ok(());
        }

        let mut entries = decode_doclist(doclist)?;
        entries.push(entry);
        normalize_doclist_entries(&mut entries);
        *doclist = encode_doclist(&entries)?;
        Ok(())
    }

    fn term_doclists(&self) -> impl Iterator<Item = (&Vec<u8>, &Vec<u8>)> {
        self.terms.iter()
    }
}

#[derive(Debug, Clone)]
pub struct Fts5IndexConfig {
    pub merge_threshold: usize,
}

impl Default for Fts5IndexConfig {
    fn default() -> Self {
        Self { merge_threshold: 8 }
    }
}

#[derive(Debug, Clone)]
pub struct Fts5Index {
    config: Fts5IndexConfig,
    levels: Vec<Vec<Fts5Segment>>,
    pending: Fts5Segment,
}

impl Fts5Index {
    pub fn new(config: Fts5IndexConfig) -> Self {
        Self {
            config,
            levels: vec![Vec::new()],
            pending: Fts5Segment::new(),
        }
    }

    pub fn insert_term(&mut self, term: &[u8], entry: Fts5DoclistEntry) -> Result<()> {
        self.pending.add_entry(term, entry)
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let segment = std::mem::replace(&mut self.pending, Fts5Segment::new());
        if self.levels.is_empty() {
            self.levels.push(Vec::new());
        }
        self.levels[0].push(segment);
        if self.levels[0].len() > self.config.merge_threshold {
            self.merge_level(0)?;
        }
        Ok(())
    }

    pub fn lookup_term(&self, term: &[u8]) -> Result<Vec<Fts5DoclistEntry>> {
        let mut doclists = Vec::new();
        for level in &self.levels {
            for segment in level {
                if let Some(doclist) = segment.terms.get(term) {
                    doclists.push(doclist.clone());
                }
            }
        }
        if let Some(doclist) = self.pending.terms.get(term) {
            doclists.push(doclist.clone());
        }
        if doclists.is_empty() {
            return Ok(Vec::new());
        }
        merge_doclists(&doclists)
    }

    pub fn lookup_prefix(&self, prefix: &[u8]) -> Result<Vec<Fts5DoclistEntry>> {
        let mut doclists = Vec::new();
        for level in &self.levels {
            for segment in level {
                for (term, doclist) in segment.term_doclists() {
                    if term.starts_with(prefix) {
                        doclists.push(doclist.clone());
                    }
                }
            }
        }
        for (term, doclist) in self.pending.term_doclists() {
            if term.starts_with(prefix) {
                doclists.push(doclist.clone());
            }
        }
        if doclists.is_empty() {
            return Ok(Vec::new());
        }
        merge_doclists(&doclists)
    }

    fn merge_level(&mut self, level: usize) -> Result<()> {
        if level >= self.levels.len() {
            return Ok(());
        }
        if self.levels[level].len() < 2 {
            return Ok(());
        }

        let segments = std::mem::take(&mut self.levels[level]);
        let merged = merge_segments(segments)?;
        let next_level = level + 1;
        if self.levels.len() <= next_level {
            self.levels.push(Vec::new());
        }
        self.levels[next_level].push(merged);
        Ok(())
    }
}

fn merge_segments(segments: Vec<Fts5Segment>) -> Result<Fts5Segment> {
    use std::collections::BTreeMap;

    let mut term_map: BTreeMap<Vec<u8>, Vec<Vec<u8>>> = BTreeMap::new();
    for segment in segments {
        for (term, doclist) in segment.term_doclists() {
            term_map
                .entry(term.clone())
                .or_default()
                .push(doclist.clone());
        }
    }

    let mut merged = Fts5Segment::new();
    for (term, doclists) in term_map {
        let entries = merge_doclists(&doclists)?;
        let encoded = encode_doclist(&entries)?;
        merged.terms.insert(term, encoded);
    }
    Ok(merged)
}

fn merge_doclists(doclists: &[Vec<u8>]) -> Result<Vec<Fts5DoclistEntry>> {
    let mut all_entries = Vec::new();
    for doclist in doclists {
        all_entries.extend(decode_doclist(doclist)?);
    }
    all_entries.sort_by_key(|entry| entry.rowid);

    let mut merged: Vec<Fts5DoclistEntry> = Vec::new();
    for entry in all_entries {
        if let Some(last) = merged.last_mut() {
            if last.rowid == entry.rowid {
                last.deleted |= entry.deleted;
                last.positions.extend(entry.positions);
                normalize_positions(&mut last.positions);
                continue;
            }
        }
        merged.push(entry);
    }
    Ok(merged)
}

fn normalize_doclist_entries(entries: &mut Vec<Fts5DoclistEntry>) {
    entries.sort_by_key(|entry| entry.rowid);
    let mut out: Vec<Fts5DoclistEntry> = Vec::new();
    for entry in entries.drain(..) {
        if let Some(last) = out.last_mut() {
            if last.rowid == entry.rowid {
                last.deleted |= entry.deleted;
                last.positions.extend(entry.positions);
                normalize_positions(&mut last.positions);
                continue;
            }
        }
        let mut entry = entry;
        normalize_positions(&mut entry.positions);
        out.push(entry);
    }
    *entries = out;
}

fn normalize_positions(positions: &mut Vec<Fts5Position>) {
    positions.sort_by(|a, b| (a.column, a.offset).cmp(&(b.column, b.offset)));
    positions.dedup_by(|a, b| a.column == b.column && a.offset == b.offset);
}

fn encode_poslist(positions: &[Fts5Position]) -> Result<Vec<u8>> {
    use std::collections::BTreeMap;

    let mut by_column: BTreeMap<i32, Vec<i32>> = BTreeMap::new();
    for pos in positions {
        if pos.column < 0 || pos.offset < 0 {
            return Err(Error::with_message(
                ErrorCode::Corrupt,
                "negative column or offset in poslist",
            ));
        }
        by_column.entry(pos.column).or_default().push(pos.offset);
    }

    let mut out = Vec::new();
    for (idx, (column, mut offsets)) in by_column.into_iter().enumerate() {
        offsets.sort_unstable();
        if idx > 0 || column != 0 {
            put_varint(&mut out, 1);
            put_varint(&mut out, column as u64);
        }

        let mut prev = 0i32;
        for (i, offset) in offsets.into_iter().enumerate() {
            let delta = if i == 0 { offset } else { offset - prev };
            if delta < 0 {
                return Err(Error::with_message(
                    ErrorCode::Corrupt,
                    "non-monotonic offsets in poslist",
                ));
            }
            let value = delta as u64 + 2;
            put_varint(&mut out, value);
            prev = offset;
        }
    }

    Ok(out)
}

fn decode_poslist(data: &[u8]) -> Result<(Vec<Fts5Position>, bool, usize)> {
    let (header, header_len) = get_varint(data);
    let deleted = (header & 0x01) != 0;
    let size = (header >> 1) as usize;
    let end = header_len + size;
    if end > data.len() {
        return Err(Error::with_message(
            ErrorCode::Corrupt,
            "poslist size overflow",
        ));
    }

    let mut positions = Vec::new();
    let mut offset = header_len;
    let mut column = 0i32;
    let mut prev_offset = 0i32;

    while offset < end {
        let value = fts5_get_varint(data, &mut offset)?;

        if value == 1 {
            let col = fts5_get_varint(data, &mut offset)?;
            column = i32::try_from(col)
                .map_err(|_| Error::with_message(ErrorCode::Corrupt, "column overflow"))?;
            prev_offset = 0;
            continue;
        }

        if value < 2 {
            return Err(Error::with_message(
                ErrorCode::Corrupt,
                "invalid poslist value",
            ));
        }

        let delta = i32::try_from(value - 2)
            .map_err(|_| Error::with_message(ErrorCode::Corrupt, "poslist offset overflow"))?;
        let pos = prev_offset + delta;
        positions.push(Fts5Position {
            column,
            offset: pos,
        });
        prev_offset = pos;
    }

    Ok((positions, deleted, end))
}

pub fn encode_doclist(entries: &[Fts5DoclistEntry]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut prev_rowid = 0i64;

    for (i, entry) in entries.iter().enumerate() {
        if entry.rowid < 0 {
            return Err(Error::with_message(
                ErrorCode::Corrupt,
                "doclist rowid must be non-negative",
            ));
        }
        if i > 0 && entry.rowid <= prev_rowid {
            return Err(Error::with_message(
                ErrorCode::Corrupt,
                "doclist rowids must be strictly increasing",
            ));
        }
        let delta = if i == 0 {
            entry.rowid as u64
        } else {
            (entry.rowid - prev_rowid) as u64
        };
        put_varint(&mut out, delta);

        let poslist = encode_poslist(&entry.positions)?;
        let header = ((poslist.len() as u64) << 1) | if entry.deleted { 1 } else { 0 };
        put_varint(&mut out, header);
        out.extend_from_slice(&poslist);

        prev_rowid = entry.rowid;
    }

    Ok(out)
}

pub fn decode_doclist(data: &[u8]) -> Result<Vec<Fts5DoclistEntry>> {
    let mut entries = Vec::new();
    let mut offset = 0usize;
    let mut rowid = 0i64;
    let mut first = true;

    while offset < data.len() {
        let value = fts5_get_varint(data, &mut offset)?;
        let delta = i64::try_from(value)
            .map_err(|_| Error::with_message(ErrorCode::Corrupt, "doclist rowid overflow"))?;
        if first {
            rowid = delta;
            first = false;
        } else {
            rowid += delta;
        }

        let (positions, deleted, end) = decode_poslist(&data[offset..])?;
        offset += end;

        entries.push(Fts5DoclistEntry {
            rowid,
            positions,
            deleted,
        });
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_doclist, decode_structure, encode_doclist, encode_structure, Fts5DoclistEntry,
        Fts5Position, Fts5Structure, Fts5StructureLevel, Fts5StructureSegment,
    };

    #[test]
    fn doclist_roundtrip() {
        let entries = vec![
            Fts5DoclistEntry {
                rowid: 10,
                deleted: false,
                positions: vec![
                    Fts5Position {
                        column: 0,
                        offset: 1,
                    },
                    Fts5Position {
                        column: 0,
                        offset: 3,
                    },
                    Fts5Position {
                        column: 1,
                        offset: 2,
                    },
                ],
            },
            Fts5DoclistEntry {
                rowid: 25,
                deleted: true,
                positions: vec![Fts5Position {
                    column: 0,
                    offset: 0,
                }],
            },
        ];

        let encoded = encode_doclist(&entries).expect("encode doclist");
        let decoded = decode_doclist(&encoded).expect("decode doclist");
        assert_eq!(decoded.len(), entries.len());
        assert_eq!(decoded[0].rowid, 10);
        assert_eq!(decoded[0].deleted, false);
        assert_eq!(decoded[0].positions.len(), 3);
        assert_eq!(decoded[1].rowid, 25);
        assert_eq!(decoded[1].deleted, true);
    }

    #[test]
    fn structure_roundtrip() {
        let structure = Fts5Structure {
            cookie: 123,
            n_write_counter: 42,
            n_origin_counter: 1,
            levels: vec![Fts5StructureLevel {
                n_merge: 0,
                segments: vec![Fts5StructureSegment {
                    segid: 1,
                    pgno_first: 2,
                    pgno_last: 3,
                    origin1: 10,
                    origin2: 11,
                    n_pg_tombstone: 0,
                    n_entry_tombstone: 0,
                    n_entry: 7,
                }],
            }],
        };

        let encoded = encode_structure(&structure);
        let decoded = decode_structure(&encoded).expect("decode structure");
        assert_eq!(decoded.cookie, 123);
        assert_eq!(decoded.n_write_counter, 42);
        assert_eq!(decoded.levels.len(), 1);
        assert_eq!(decoded.levels[0].segments.len(), 1);
        assert_eq!(decoded.levels[0].segments[0].segid, 1);
    }

    #[test]
    fn leaf_footer_roundtrip() {
        let offsets = vec![4usize, 12usize, 25usize];
        let encoded = super::encode_leaf_footer(&offsets).expect("encode footer");
        let mut page = vec![0u8; 40];
        let footer_offset = 30usize;
        page[footer_offset..footer_offset + encoded.len()].copy_from_slice(&encoded);
        let decoded =
            super::decode_leaf_footer(&page, footer_offset, encoded.len()).expect("decode footer");
        assert_eq!(decoded, offsets);
    }

    #[test]
    fn index_lookup_merge() {
        let mut index = super::Fts5Index::new(super::Fts5IndexConfig { merge_threshold: 1 });
        index
            .insert_term(
                b"alpha",
                Fts5DoclistEntry {
                    rowid: 1,
                    deleted: false,
                    positions: vec![Fts5Position {
                        column: 0,
                        offset: 0,
                    }],
                },
            )
            .expect("insert term");
        index.flush().expect("flush");
        index
            .insert_term(
                b"alpha",
                Fts5DoclistEntry {
                    rowid: 2,
                    deleted: false,
                    positions: vec![Fts5Position {
                        column: 0,
                        offset: 1,
                    }],
                },
            )
            .expect("insert term");
        index.flush().expect("flush");

        let entries = index.lookup_term(b"alpha").expect("lookup term");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].rowid, 1);
        assert_eq!(entries[1].rowid, 2);
    }
}
