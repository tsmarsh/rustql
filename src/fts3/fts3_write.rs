use std::collections::HashMap;

use super::fts3::{
    fts3_put_varint_u64, fts3_varint_len, Fts3Doclist, Fts3DoclistEntry, Fts3Position,
};

#[derive(Default)]
pub struct PendingTerms {
    terms: HashMap<Vec<u8>, Vec<Fts3DoclistEntry>>,
}

impl PendingTerms {
    pub fn new() -> Self {
        Self {
            terms: HashMap::new(),
        }
    }

    pub fn add(&mut self, term: &str, rowid: i64, column: i32, offset: i32) {
        let entry = self.terms.entry(term.as_bytes().to_vec()).or_default();
        if let Some(last) = entry.last_mut() {
            if last.rowid == rowid {
                last.positions.push(Fts3Position { column, offset });
                return;
            }
        }
        entry.push(Fts3DoclistEntry {
            rowid,
            positions: vec![Fts3Position { column, offset }],
        });
    }

    pub fn add_delete(&mut self, term: &str, rowid: i64) {
        let entry = self.terms.entry(term.as_bytes().to_vec()).or_default();
        entry.push(Fts3DoclistEntry {
            rowid,
            positions: Vec::new(),
        });
    }

    pub fn into_sorted_doclists(self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut terms: Vec<_> = self.terms.into_iter().collect();
        terms.sort_by(|a, b| a.0.cmp(&b.0));

        terms
            .into_iter()
            .map(|(term, entries)| {
                let doclist = Fts3Doclist::encode(&entries);
                (term, doclist.data)
            })
            .collect()
    }
}

#[derive(Default)]
pub struct LeafNode {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl LeafNode {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub fn add_term(&mut self, term: &[u8], doclist: &[u8]) {
        self.entries
            .push((term.to_vec(), doclist.to_vec()));
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn encoded_len(&self) -> usize {
        if self.entries.is_empty() {
            return 0;
        }

        let mut len = fts3_varint_len(0);
        let (first_term, first_doclist) = &self.entries[0];
        len += fts3_varint_len(first_term.len() as u64);
        len += first_term.len();
        len += fts3_varint_len(first_doclist.len() as u64);
        len += first_doclist.len();

        let mut prev_term = first_term.as_slice();
        for (term, doclist) in self.entries.iter().skip(1) {
            let prefix = shared_prefix_len(prev_term, term);
            let suffix_len = term.len() - prefix;
            len += fts3_varint_len(prefix as u64);
            len += fts3_varint_len(suffix_len as u64);
            len += suffix_len;
            len += fts3_varint_len(doclist.len() as u64);
            len += doclist.len();
            prev_term = term;
        }

        len
    }

    pub fn encoded_len_with(&self, term: &[u8], doclist: &[u8]) -> usize {
        if self.entries.is_empty() {
            let mut len = fts3_varint_len(0);
            len += fts3_varint_len(term.len() as u64);
            len += term.len();
            len += fts3_varint_len(doclist.len() as u64);
            len += doclist.len();
            return len;
        }

        let mut len = self.encoded_len();
        let prev_term = &self.entries.last().expect("non-empty entries").0;
        let prefix = shared_prefix_len(prev_term, term);
        let suffix_len = term.len() - prefix;
        len += fts3_varint_len(prefix as u64);
        len += fts3_varint_len(suffix_len as u64);
        len += suffix_len;
        len += fts3_varint_len(doclist.len() as u64);
        len += doclist.len();
        len
    }

    pub fn encode(&self) -> Vec<u8> {
        if self.entries.is_empty() {
            return Vec::new();
        }

        let mut buf = Vec::with_capacity(self.encoded_len());
        fts3_put_varint_u64(&mut buf, 0);

        let (first_term, first_doclist) = &self.entries[0];
        fts3_put_varint_u64(&mut buf, first_term.len() as u64);
        buf.extend_from_slice(first_term);
        fts3_put_varint_u64(&mut buf, first_doclist.len() as u64);
        buf.extend_from_slice(first_doclist);

        let mut prev_term = first_term.as_slice();
        for (term, doclist) in self.entries.iter().skip(1) {
            let prefix = shared_prefix_len(prev_term, term);
            let suffix = &term[prefix..];
            fts3_put_varint_u64(&mut buf, prefix as u64);
            fts3_put_varint_u64(&mut buf, suffix.len() as u64);
            buf.extend_from_slice(suffix);
            fts3_put_varint_u64(&mut buf, doclist.len() as u64);
            buf.extend_from_slice(doclist);
            prev_term = term;
        }

        buf
    }
}

fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let mut n = 0;
    while n < a.len() && n < b.len() {
        if a[n] != b[n] {
            break;
        }
        n += 1;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_node_encode() {
        let mut node = LeafNode::new();
        node.add_term(b"alpha", b"doc1");
        node.add_term(b"alpine", b"doc2");
        let encoded = node.encode();
        assert!(!encoded.is_empty());
        assert!(encoded.len() >= node.encoded_len());
    }
}
