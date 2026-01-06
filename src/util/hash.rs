//! Hash table

#[derive(Clone, Copy, Default, Debug)]
struct HashBucket {
    count: u32,
    chain: Option<usize>,
}

pub struct HashElem<T> {
    next: Option<usize>,
    prev: Option<usize>,
    data: T,
    key: String,
    h: u32,
}

pub struct Hash<T> {
    htsize: usize,
    count: usize,
    first: Option<usize>,
    ht: Vec<HashBucket>,
    elements: Vec<Option<HashElem<T>>>,
}

impl<T> Hash<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.first = None;
        self.count = 0;
        self.htsize = 0;
        self.ht.clear();
        self.elements.clear();
    }

    pub fn find(&self, key: &str) -> Option<&T> {
        let (elem_idx, _) = self.find_elem_with_hash(key);
        elem_idx.and_then(|idx| self.elements[idx].as_ref().map(|elem| &elem.data))
    }

    pub fn insert(&mut self, key: &str, data: Option<T>) -> Option<T> {
        let (elem_idx, h) = self.find_elem_with_hash(key);
        if let Some(idx) = elem_idx {
            if data.is_none() {
                return self.remove_element(idx);
            }
            if let Some(elem) = self.elements[idx].as_mut() {
                let new_data = data.expect("data missing after check");
                let old_data = std::mem::replace(&mut elem.data, new_data);
                elem.key = key.to_string();
                return Some(old_data);
            }
        }

        if data.is_none() {
            return None;
        }
        let new_elem = HashElem {
            next: None,
            prev: None,
            data: data.unwrap(),
            key: key.to_string(),
            h,
        };
        let new_idx = self.push_elem(new_elem);
        self.count += 1;
        if self.count >= 5 && self.count > 2 * self.htsize {
            self.rehash(self.count * 3);
        }
        let bucket_idx = if self.ht.is_empty() {
            None
        } else {
            Some((h as usize) % self.htsize)
        };
        self.insert_element(new_idx, bucket_idx);
        None
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn first(&self) -> Option<usize> {
        self.first
    }

    pub fn next(&self, elem_idx: usize) -> Option<usize> {
        self.elements
            .get(elem_idx)
            .and_then(|elem| elem.as_ref().and_then(|elem| elem.next))
    }

    pub fn data(&self, elem_idx: usize) -> Option<&T> {
        self.elements
            .get(elem_idx)
            .and_then(|elem| elem.as_ref().map(|elem| &elem.data))
    }

    pub fn key(&self, elem_idx: usize) -> Option<&str> {
        self.elements
            .get(elem_idx)
            .and_then(|elem| elem.as_ref().map(|elem| elem.key.as_str()))
    }

    fn push_elem(&mut self, elem: HashElem<T>) -> usize {
        if let Some((idx, slot)) = self
            .elements
            .iter_mut()
            .enumerate()
            .find(|(_, slot)| slot.is_none())
        {
            *slot = Some(elem);
            return idx;
        }
        self.elements.push(Some(elem));
        self.elements.len() - 1
    }

    fn rehash(&mut self, new_size: usize) -> bool {
        if new_size == 0 || new_size == self.htsize {
            return false;
        }
        let indices = self.indices_in_order();
        self.first = None;
        self.htsize = new_size;
        self.ht = vec![HashBucket::default(); new_size];
        for idx in indices {
            let bucket_idx = {
                let elem = self.elements[idx].as_ref().expect("element missing");
                (elem.h as usize) % self.htsize
            };
            self.insert_element(idx, Some(bucket_idx));
        }
        true
    }

    fn indices_in_order(&self) -> Vec<usize> {
        let mut indices = Vec::with_capacity(self.count);
        let mut current = self.first;
        while let Some(idx) = current {
            indices.push(idx);
            current = self.elements[idx].as_ref().and_then(|elem| elem.next);
        }
        indices
    }

    fn find_elem_with_hash(&self, key: &str) -> (Option<usize>, u32) {
        let h = str_hash(key);
        let (mut elem_idx, mut remaining) = if self.ht.is_empty() {
            (self.first, self.count as u32)
        } else {
            let entry = &self.ht[(h as usize) % self.htsize];
            (entry.chain, entry.count)
        };
        while remaining > 0 {
            let idx = match elem_idx {
                Some(idx) => idx,
                None => break,
            };
            let elem = self.elements[idx].as_ref().expect("element missing");
            if elem.h == h && str_icmp(&elem.key, key) {
                return (Some(idx), h);
            }
            elem_idx = elem.next;
            remaining -= 1;
        }
        (None, h)
    }

    fn insert_element(&mut self, elem_idx: usize, bucket_idx: Option<usize>) {
        let mut head = None;
        if let Some(bucket_idx) = bucket_idx {
            let entry = &mut self.ht[bucket_idx];
            head = if entry.count > 0 { entry.chain } else { None };
            entry.count += 1;
            entry.chain = Some(elem_idx);
        }
        if let Some(head_idx) = head {
            let head_prev = self.elements[head_idx].as_ref().and_then(|elem| elem.prev);
            {
                let elem = self.elements[elem_idx].as_mut().expect("element missing");
                elem.next = Some(head_idx);
                elem.prev = head_prev;
            }
            self.elements[head_idx]
                .as_mut()
                .expect("element missing")
                .prev = Some(elem_idx);
            if let Some(prev_idx) = head_prev {
                self.elements[prev_idx]
                    .as_mut()
                    .expect("element missing")
                    .next = Some(elem_idx);
            } else {
                self.first = Some(elem_idx);
            }
        } else {
            let old_first = self.first;
            {
                let elem = self.elements[elem_idx].as_mut().expect("element missing");
                elem.next = old_first;
                elem.prev = None;
            }
            if let Some(old_first_idx) = old_first {
                self.elements[old_first_idx]
                    .as_mut()
                    .expect("element missing")
                    .prev = Some(elem_idx);
            }
            self.first = Some(elem_idx);
        }
    }

    fn remove_element(&mut self, elem_idx: usize) -> Option<T> {
        let (prev, next, h) = {
            let elem = self.elements[elem_idx].as_ref().expect("element missing");
            (elem.prev, elem.next, elem.h)
        };
        if let Some(prev_idx) = prev {
            self.elements[prev_idx]
                .as_mut()
                .expect("element missing")
                .next = next;
        } else {
            self.first = next;
        }
        if let Some(next_idx) = next {
            self.elements[next_idx]
                .as_mut()
                .expect("element missing")
                .prev = prev;
        }
        if !self.ht.is_empty() {
            let entry = &mut self.ht[(h as usize) % self.htsize];
            if entry.chain == Some(elem_idx) {
                entry.chain = next;
            }
            entry.count -= 1;
        }
        self.count -= 1;
        let data = self.elements[elem_idx].take().map(|elem| elem.data);
        if self.count == 0 {
            self.clear();
        }
        data
    }
}

impl<T> Default for Hash<T> {
    fn default() -> Self {
        Self {
            htsize: 0,
            count: 0,
            first: None,
            ht: Vec::new(),
            elements: Vec::new(),
        }
    }
}

fn str_hash(key: &str) -> u32 {
    let mut h: u32 = 0;
    for byte in key.as_bytes() {
        h = h.wrapping_add((byte & 0xdf) as u32);
        h = h.wrapping_mul(0x9e3779b1);
    }
    h
}

fn str_icmp(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

#[cfg(test)]
mod tests {
    use super::Hash;

    #[test]
    fn insert_find_remove() {
        let mut hash = Hash::new();
        assert_eq!(hash.insert("Alpha", Some(1)), None);
        assert_eq!(hash.find("ALPHA"), Some(&1));
        assert_eq!(hash.insert("Alpha", Some(2)), Some(1));
        assert_eq!(hash.find("alpha"), Some(&2));
        assert_eq!(hash.insert("Alpha", None), Some(2));
        assert_eq!(hash.find("alpha"), None);
        assert_eq!(hash.count(), 0);
    }

    #[test]
    fn rehash_keeps_entries() {
        let mut hash = Hash::new();
        for idx in 0..10 {
            let key = format!("k{}", idx);
            assert_eq!(hash.insert(&key, Some(idx)), None);
        }
        for idx in 0..10 {
            let key = format!("K{}", idx);
            assert_eq!(hash.find(&key), Some(&idx));
        }
    }
}
