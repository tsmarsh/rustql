//! Bit vector

#[derive(Clone, Debug, Default)]
pub struct BitVec {
    bits: Vec<u8>,
    len: usize,
}

impl BitVec {
    pub fn new(len: usize) -> Self {
        let byte_len = (len + 7) / 8;
        Self {
            bits: vec![0; byte_len],
            len,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn get(&self, idx: usize) -> bool {
        if idx >= self.len {
            return false;
        }
        let byte = self.bits[idx / 8];
        let mask = 1u8 << (idx % 8);
        (byte & mask) != 0
    }

    pub fn set(&mut self, idx: usize, value: bool) {
        if idx >= self.len {
            return;
        }
        let byte_idx = idx / 8;
        let mask = 1u8 << (idx % 8);
        if value {
            self.bits[byte_idx] |= mask;
        } else {
            self.bits[byte_idx] &= !mask;
        }
    }
}
