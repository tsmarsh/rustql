//! MD5 Implementation (public domain, ported from SQLite's test_md5.c)
//!
//! This module provides MD5 hashing support for the TCL test suite.

use std::ffi::c_void;
use std::os::raw::c_int;

use super::ffi::{Tcl_Interp, Tcl_Obj, TCL_ERROR, TCL_OK};
use super::helpers::{obj_to_string, set_result_string};

/// MD5 context for hashing
struct Md5Context {
    buf: [u32; 4],
    bits: [u32; 2],
    input: [u8; 64],
}

impl Md5Context {
    fn new() -> Self {
        Self {
            buf: [0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476],
            bits: [0, 0],
            input: [0; 64],
        }
    }

    fn update(&mut self, data: &[u8]) {
        let mut t = self.bits[0];
        self.bits[0] = t.wrapping_add((data.len() as u32) << 3);
        if self.bits[0] < t {
            self.bits[1] = self.bits[1].wrapping_add(1);
        }
        self.bits[1] = self.bits[1].wrapping_add((data.len() as u32) >> 29);

        t = ((t >> 3) & 0x3f) as u32;

        let mut data_idx = 0usize;
        if t != 0 {
            let p = t as usize;
            t = 64 - t;
            if data.len() < t as usize {
                self.input[p..p + data.len()].copy_from_slice(data);
                return;
            }
            self.input[p..p + t as usize].copy_from_slice(&data[..t as usize]);
            self.transform();
            data_idx = t as usize;
        }

        while data_idx + 64 <= data.len() {
            self.input.copy_from_slice(&data[data_idx..data_idx + 64]);
            self.transform();
            data_idx += 64;
        }

        let remaining = data.len() - data_idx;
        self.input[..remaining].copy_from_slice(&data[data_idx..]);
    }

    fn finalize(mut self) -> [u8; 16] {
        let mut count = ((self.bits[0] >> 3) & 0x3f) as usize;

        self.input[count] = 0x80;
        count += 1;

        if count > 56 {
            for i in count..64 {
                self.input[i] = 0;
            }
            self.transform();
            count = 0;
        }

        for i in count..56 {
            self.input[i] = 0;
        }

        // Append length in bits
        self.input[56..60].copy_from_slice(&self.bits[0].to_le_bytes());
        self.input[60..64].copy_from_slice(&self.bits[1].to_le_bytes());
        self.transform();

        let mut digest = [0u8; 16];
        for (i, &word) in self.buf.iter().enumerate() {
            digest[i * 4..(i + 1) * 4].copy_from_slice(&word.to_le_bytes());
        }
        digest
    }

    fn transform(&mut self) {
        let mut input_words = [0u32; 16];
        for i in 0..16 {
            input_words[i] = u32::from_le_bytes([
                self.input[i * 4],
                self.input[i * 4 + 1],
                self.input[i * 4 + 2],
                self.input[i * 4 + 3],
            ]);
        }

        let mut a = self.buf[0];
        let mut b = self.buf[1];
        let mut c = self.buf[2];
        let mut d = self.buf[3];

        macro_rules! f1 {
            ($x:expr, $y:expr, $z:expr) => {
                $z ^ ($x & ($y ^ $z))
            };
        }
        macro_rules! f2 {
            ($x:expr, $y:expr, $z:expr) => {
                f1!($z, $x, $y)
            };
        }
        macro_rules! f3 {
            ($x:expr, $y:expr, $z:expr) => {
                $x ^ $y ^ $z
            };
        }
        macro_rules! f4 {
            ($x:expr, $y:expr, $z:expr) => {
                $y ^ ($x | !$z)
            };
        }

        macro_rules! md5step {
            ($f:ident, $w:expr, $x:expr, $y:expr, $z:expr, $data:expr, $s:expr) => {
                $w = $w.wrapping_add($f!($x, $y, $z)).wrapping_add($data);
                $w = ($w << $s) | ($w >> (32 - $s));
                $w = $w.wrapping_add($x);
            };
        }

        md5step!(f1, a, b, c, d, input_words[0].wrapping_add(0xd76aa478), 7);
        md5step!(f1, d, a, b, c, input_words[1].wrapping_add(0xe8c7b756), 12);
        md5step!(f1, c, d, a, b, input_words[2].wrapping_add(0x242070db), 17);
        md5step!(f1, b, c, d, a, input_words[3].wrapping_add(0xc1bdceee), 22);
        md5step!(f1, a, b, c, d, input_words[4].wrapping_add(0xf57c0faf), 7);
        md5step!(f1, d, a, b, c, input_words[5].wrapping_add(0x4787c62a), 12);
        md5step!(f1, c, d, a, b, input_words[6].wrapping_add(0xa8304613), 17);
        md5step!(f1, b, c, d, a, input_words[7].wrapping_add(0xfd469501), 22);
        md5step!(f1, a, b, c, d, input_words[8].wrapping_add(0x698098d8), 7);
        md5step!(f1, d, a, b, c, input_words[9].wrapping_add(0x8b44f7af), 12);
        md5step!(f1, c, d, a, b, input_words[10].wrapping_add(0xffff5bb1), 17);
        md5step!(f1, b, c, d, a, input_words[11].wrapping_add(0x895cd7be), 22);
        md5step!(f1, a, b, c, d, input_words[12].wrapping_add(0x6b901122), 7);
        md5step!(f1, d, a, b, c, input_words[13].wrapping_add(0xfd987193), 12);
        md5step!(f1, c, d, a, b, input_words[14].wrapping_add(0xa679438e), 17);
        md5step!(f1, b, c, d, a, input_words[15].wrapping_add(0x49b40821), 22);

        md5step!(f2, a, b, c, d, input_words[1].wrapping_add(0xf61e2562), 5);
        md5step!(f2, d, a, b, c, input_words[6].wrapping_add(0xc040b340), 9);
        md5step!(f2, c, d, a, b, input_words[11].wrapping_add(0x265e5a51), 14);
        md5step!(f2, b, c, d, a, input_words[0].wrapping_add(0xe9b6c7aa), 20);
        md5step!(f2, a, b, c, d, input_words[5].wrapping_add(0xd62f105d), 5);
        md5step!(f2, d, a, b, c, input_words[10].wrapping_add(0x02441453), 9);
        md5step!(f2, c, d, a, b, input_words[15].wrapping_add(0xd8a1e681), 14);
        md5step!(f2, b, c, d, a, input_words[4].wrapping_add(0xe7d3fbc8), 20);
        md5step!(f2, a, b, c, d, input_words[9].wrapping_add(0x21e1cde6), 5);
        md5step!(f2, d, a, b, c, input_words[14].wrapping_add(0xc33707d6), 9);
        md5step!(f2, c, d, a, b, input_words[3].wrapping_add(0xf4d50d87), 14);
        md5step!(f2, b, c, d, a, input_words[8].wrapping_add(0x455a14ed), 20);
        md5step!(f2, a, b, c, d, input_words[13].wrapping_add(0xa9e3e905), 5);
        md5step!(f2, d, a, b, c, input_words[2].wrapping_add(0xfcefa3f8), 9);
        md5step!(f2, c, d, a, b, input_words[7].wrapping_add(0x676f02d9), 14);
        md5step!(f2, b, c, d, a, input_words[12].wrapping_add(0x8d2a4c8a), 20);

        md5step!(f3, a, b, c, d, input_words[5].wrapping_add(0xfffa3942), 4);
        md5step!(f3, d, a, b, c, input_words[8].wrapping_add(0x8771f681), 11);
        md5step!(f3, c, d, a, b, input_words[11].wrapping_add(0x6d9d6122), 16);
        md5step!(f3, b, c, d, a, input_words[14].wrapping_add(0xfde5380c), 23);
        md5step!(f3, a, b, c, d, input_words[1].wrapping_add(0xa4beea44), 4);
        md5step!(f3, d, a, b, c, input_words[4].wrapping_add(0x4bdecfa9), 11);
        md5step!(f3, c, d, a, b, input_words[7].wrapping_add(0xf6bb4b60), 16);
        md5step!(f3, b, c, d, a, input_words[10].wrapping_add(0xbebfbc70), 23);
        md5step!(f3, a, b, c, d, input_words[13].wrapping_add(0x289b7ec6), 4);
        md5step!(f3, d, a, b, c, input_words[0].wrapping_add(0xeaa127fa), 11);
        md5step!(f3, c, d, a, b, input_words[3].wrapping_add(0xd4ef3085), 16);
        md5step!(f3, b, c, d, a, input_words[6].wrapping_add(0x04881d05), 23);
        md5step!(f3, a, b, c, d, input_words[9].wrapping_add(0xd9d4d039), 4);
        md5step!(f3, d, a, b, c, input_words[12].wrapping_add(0xe6db99e5), 11);
        md5step!(f3, c, d, a, b, input_words[15].wrapping_add(0x1fa27cf8), 16);
        md5step!(f3, b, c, d, a, input_words[2].wrapping_add(0xc4ac5665), 23);

        md5step!(f4, a, b, c, d, input_words[0].wrapping_add(0xf4292244), 6);
        md5step!(f4, d, a, b, c, input_words[7].wrapping_add(0x432aff97), 10);
        md5step!(f4, c, d, a, b, input_words[14].wrapping_add(0xab9423a7), 15);
        md5step!(f4, b, c, d, a, input_words[5].wrapping_add(0xfc93a039), 21);
        md5step!(f4, a, b, c, d, input_words[12].wrapping_add(0x655b59c3), 6);
        md5step!(f4, d, a, b, c, input_words[3].wrapping_add(0x8f0ccc92), 10);
        md5step!(f4, c, d, a, b, input_words[10].wrapping_add(0xffeff47d), 15);
        md5step!(f4, b, c, d, a, input_words[1].wrapping_add(0x85845dd1), 21);
        md5step!(f4, a, b, c, d, input_words[8].wrapping_add(0x6fa87e4f), 6);
        md5step!(f4, d, a, b, c, input_words[15].wrapping_add(0xfe2ce6e0), 10);
        md5step!(f4, c, d, a, b, input_words[6].wrapping_add(0xa3014314), 15);
        md5step!(f4, b, c, d, a, input_words[13].wrapping_add(0x4e0811a1), 21);
        md5step!(f4, a, b, c, d, input_words[4].wrapping_add(0xf7537e82), 6);
        md5step!(f4, d, a, b, c, input_words[11].wrapping_add(0xbd3af235), 10);
        md5step!(f4, c, d, a, b, input_words[2].wrapping_add(0x2ad7d2bb), 15);
        md5step!(f4, b, c, d, a, input_words[9].wrapping_add(0xeb86d391), 21);

        self.buf[0] = self.buf[0].wrapping_add(a);
        self.buf[1] = self.buf[1].wrapping_add(b);
        self.buf[2] = self.buf[2].wrapping_add(c);
        self.buf[3] = self.buf[3].wrapping_add(d);
    }
}

/// Compute MD5 hash and return hex string
pub fn md5_hash(data: &[u8]) -> String {
    let mut ctx = Md5Context::new();
    ctx.update(data);
    let digest = ctx.finalize();
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

/// TCL md5 command - compute MD5 hash of a string
/// Usage: md5 STRING
pub unsafe extern "C" fn md5_cmd(
    _client_data: *mut c_void,
    interp: *mut Tcl_Interp,
    objc: c_int,
    objv: *const *mut Tcl_Obj,
) -> c_int {
    if objc != 2 {
        set_result_string(interp, "wrong # args: should be \"md5 STRING\"");
        return TCL_ERROR;
    }

    let input = obj_to_string(*objv.offset(1));
    let hash = md5_hash(input.as_bytes());
    set_result_string(interp, &hash);
    TCL_OK
}
