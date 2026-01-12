//! Bit vector (bitvec.c translation).

use std::mem::size_of;

use crate::error::ErrorCode;

const BITVEC_SZ: usize = 512;
const BITVEC_USIZE: usize =
    ((BITVEC_SZ - (3 * size_of::<u32>())) / size_of::<usize>()) * size_of::<usize>();
const BITVEC_NELEM: usize = BITVEC_USIZE / size_of::<u8>();
const BITVEC_NBIT: u32 = (BITVEC_NELEM * 8) as u32;
const BITVEC_NINT: usize = BITVEC_USIZE / size_of::<u32>();
const BITVEC_MXHASH: usize = BITVEC_NINT / 2;
const BITVEC_NPTR: usize = BITVEC_USIZE / size_of::<usize>();

#[derive(Debug)]
enum BitvecRepr {
    Bitmap(Vec<u8>),
    Hash(Vec<u32>),
    Sub(Vec<Option<Box<BitVec>>>),
}

/// Bit vector supporting sparse and dense representations.
#[derive(Debug)]
pub struct BitVec {
    size: u32,
    n_set: u32,
    divisor: u32,
    repr: BitvecRepr,
}

impl BitVec {
    pub fn new(size: u32) -> Self {
        let repr = if size <= BITVEC_NBIT {
            BitvecRepr::Bitmap(vec![0u8; BITVEC_NELEM])
        } else {
            BitvecRepr::Hash(vec![0u32; BITVEC_NINT])
        };
        Self {
            size,
            n_set: 0,
            divisor: 0,
            repr,
        }
    }

    pub fn size(&self) -> u32 {
        self.size
    }

    /// Test a 1-based bit index.
    pub fn test(&self, i: u32) -> bool {
        if i == 0 || i > self.size {
            return false;
        }
        let mut idx = i - 1;
        let mut current = self;
        loop {
            if current.divisor == 0 {
                break;
            }
            let bin = (idx / current.divisor) as usize;
            idx %= current.divisor;
            match &current.repr {
                BitvecRepr::Sub(subs) => {
                    if let Some(sub) = subs.get(bin).and_then(|v| v.as_ref()) {
                        current = sub;
                    } else {
                        return false;
                    }
                }
                _ => return false,
            }
        }

        match &current.repr {
            BitvecRepr::Bitmap(bits) => {
                let byte = (idx / 8) as usize;
                let bit = (idx & 7) as u8;
                bits.get(byte).is_some_and(|v| (v & (1 << bit)) != 0)
            }
            BitvecRepr::Hash(hash) => {
                let mut h = (idx as usize) % BITVEC_NINT;
                let target = idx + 1;
                loop {
                    let entry = hash[h];
                    if entry == 0 {
                        return false;
                    }
                    if entry == target {
                        return true;
                    }
                    h = (h + 1) % BITVEC_NINT;
                }
            }
            BitvecRepr::Sub(_) => false,
        }
    }

    /// Set a 1-based bit index.
    pub fn set(&mut self, i: u32) -> ErrorCode {
        if i == 0 || i > self.size {
            return ErrorCode::Range;
        }
        let mut idx = i - 1;
        let mut current: *mut BitVec = self;
        unsafe {
            while (*current).size > BITVEC_NBIT && (*current).divisor != 0 {
                let bin = (idx / (*current).divisor) as usize;
                idx %= (*current).divisor;
                let sub = match &mut (*current).repr {
                    BitvecRepr::Sub(subs) => subs,
                    _ => return ErrorCode::Corrupt,
                };
                if sub[bin].is_none() {
                    sub[bin] = Some(Box::new(BitVec::new((*current).divisor)));
                }
                current = sub[bin].as_mut().unwrap().as_mut();
            }
            match &mut (*current).repr {
                BitvecRepr::Bitmap(bits) => {
                    let byte = (idx / 8) as usize;
                    let bit = (idx & 7) as u8;
                    bits[byte] |= 1 << bit;
                    ErrorCode::Ok
                }
                BitvecRepr::Hash(hash) => {
                    let mut h = (idx as usize) % BITVEC_NINT;
                    let target = idx + 1;
                    if hash[h] == 0 {
                        if (*current).n_set < (BITVEC_NINT as u32 - 1) {
                            hash[h] = target;
                            (*current).n_set += 1;
                            return ErrorCode::Ok;
                        }
                        return (*current).rehash_and_set(target);
                    }
                    loop {
                        if hash[h] == target {
                            return ErrorCode::Ok;
                        }
                        h = (h + 1) % BITVEC_NINT;
                        if hash[h] == 0 {
                            break;
                        }
                    }
                    if (*current).n_set as usize >= BITVEC_MXHASH {
                        return (*current).rehash_and_set(target);
                    }
                    hash[h] = target;
                    (*current).n_set += 1;
                    ErrorCode::Ok
                }
                BitvecRepr::Sub(_) => ErrorCode::Corrupt,
            }
        }
    }

    /// Clear a 1-based bit index.
    pub fn clear(&mut self, i: u32) {
        if i == 0 || i > self.size {
            return;
        }
        let mut idx = i - 1;
        let mut current = self;
        while current.divisor != 0 {
            let bin = (idx / current.divisor) as usize;
            idx %= current.divisor;
            match &mut current.repr {
                BitvecRepr::Sub(subs) => {
                    if let Some(sub) = subs.get_mut(bin).and_then(|v| v.as_mut()) {
                        current = sub;
                    } else {
                        return;
                    }
                }
                _ => return,
            }
        }

        match &mut current.repr {
            BitvecRepr::Bitmap(bits) => {
                let byte = (idx / 8) as usize;
                let bit = (idx & 7) as u8;
                if let Some(v) = bits.get_mut(byte) {
                    *v &= !(1 << bit);
                }
            }
            BitvecRepr::Hash(hash) => {
                let mut values = Vec::with_capacity(hash.len());
                for &entry in hash.iter() {
                    if entry != 0 && entry != idx + 1 {
                        values.push(entry);
                    }
                }
                for entry in hash.iter_mut() {
                    *entry = 0;
                }
                current.n_set = 0;
                for entry in values {
                    let mut h = ((entry - 1) as usize) % BITVEC_NINT;
                    while hash[h] != 0 {
                        h = (h + 1) % BITVEC_NINT;
                    }
                    hash[h] = entry;
                    current.n_set += 1;
                }
            }
            BitvecRepr::Sub(_) => {}
        }
    }

    fn rehash_and_set(&mut self, value: u32) -> ErrorCode {
        let mut entries = Vec::with_capacity(BITVEC_NINT);
        if let BitvecRepr::Hash(hash) = &self.repr {
            for &entry in hash.iter() {
                if entry != 0 {
                    entries.push(entry);
                }
            }
        }

        self.repr = BitvecRepr::Sub((0..BITVEC_NPTR).map(|_| None).collect());
        self.n_set = 0;
        self.divisor = self.size / BITVEC_NPTR as u32;
        if !self.size.is_multiple_of(BITVEC_NPTR as u32) {
            self.divisor += 1;
        }
        if self.divisor < BITVEC_NBIT {
            self.divisor = BITVEC_NBIT;
        }

        let rc = self.set(value);
        if rc != ErrorCode::Ok {
            return rc;
        }
        for entry in entries {
            let rc = self.set(entry);
            if rc != ErrorCode::Ok {
                return rc;
            }
        }
        ErrorCode::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitvec_basic() {
        let mut bv = BitVec::new(100);
        assert!(!bv.test(1));
        assert_eq!(bv.set(1), ErrorCode::Ok);
        assert!(bv.test(1));
        bv.clear(1);
        assert!(!bv.test(1));
    }

    #[test]
    fn test_bitvec_hash() {
        let mut bv = BitVec::new(BITVEC_NBIT + 100);
        assert_eq!(bv.set(1), ErrorCode::Ok);
        assert_eq!(bv.set(BITVEC_NBIT + 50), ErrorCode::Ok);
        assert!(bv.test(1));
        assert!(bv.test(BITVEC_NBIT + 50));
    }
}
