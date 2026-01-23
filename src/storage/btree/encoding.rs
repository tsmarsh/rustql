//! Varint encoding/decoding utilities for B-tree

use crate::error::{Error, ErrorCode, Result};

/// Read a varint at the given offset, returns (value, bytes_consumed)
pub fn read_varint_at(data: &[u8], start: usize) -> (u64, usize) {
    if start >= data.len() {
        return (0, 0);
    }

    let mut value: u64 = 0;
    let mut bytes = 0;

    for i in 0..9 {
        if start + i >= data.len() {
            break;
        }
        let b = data[start + i];
        if i < 8 {
            value = (value << 7) | (b & 0x7f) as u64;
            bytes += 1;
            if b & 0x80 == 0 {
                break;
            }
        } else {
            // 9th byte uses all 8 bits
            value = (value << 8) | b as u64;
            bytes += 1;
        }
    }

    (value, bytes)
}

/// Read a varint from the given offset in a byte slice
pub fn read_varint(data: &[u8], offset: usize) -> Result<(u64, usize)> {
    if offset >= data.len() {
        return Err(Error::new(ErrorCode::Corrupt));
    }
    let (value, consumed) = read_varint_at(data, offset);
    if consumed == 0 {
        return Err(Error::new(ErrorCode::Corrupt));
    }
    Ok((value, consumed))
}

/// Read a 32-bit varint
pub fn read_varint32(data: &[u8], offset: usize) -> Result<(u32, usize)> {
    let (val, consumed) = read_varint(data, offset)?;
    Ok((val as u32, consumed))
}

/// Public API: get a varint from data
pub fn get_varint(data: &[u8]) -> Result<(u64, usize)> {
    read_varint(data, 0)
}

/// Public API: get a 32-bit varint from data
pub fn get_varint32(data: &[u8]) -> Result<(u32, usize)> {
    read_varint32(data, 0)
}

/// Write a varint value to a buffer, returns bytes written
pub fn put_varint(buf: &mut [u8], value: u64) -> usize {
    put_varint_at(buf, value)
}

/// Calculate the number of bytes needed to encode a varint
pub fn varint_len(value: u64) -> usize {
    if value <= 0x7f {
        1
    } else if value <= 0x3fff {
        2
    } else if value <= 0x1f_ffff {
        3
    } else if value <= 0x0fff_ffff {
        4
    } else if value <= 0x07_ffff_ffff {
        5
    } else if value <= 0x03ff_ffff_ffff {
        6
    } else if value <= 0x01_ffff_ffff_ffff {
        7
    } else if value <= 0x00ff_ffff_ffff_ffff {
        8
    } else {
        9
    }
}

/// Write a varint at the start of a buffer, returns bytes written
pub(crate) fn put_varint_at(buf: &mut [u8], value: u64) -> usize {
    let len = varint_len(value);
    if buf.len() < len {
        return 0;
    }
    if len == 9 {
        buf[8] = (value & 0xFF) as u8;
        let mut v = value >> 8;
        for i in (0..8).rev() {
            buf[i] = ((v & 0x7F) | 0x80) as u8;
            v >>= 7;
        }
    } else {
        let mut v = value;
        for i in (0..len).rev() {
            if i == len - 1 {
                buf[i] = (v & 0x7F) as u8;
            } else {
                buf[i] = ((v & 0x7F) | 0x80) as u8;
            }
            v >>= 7;
        }
    }
    len
}

/// Write a varint value to a Vec
pub fn write_varint(value: u64, out: &mut Vec<u8>) {
    let len = varint_len(value);
    let start = out.len();
    out.resize(start + len, 0);
    put_varint_at(&mut out[start..], value);
}

/// Read a u16 from a byte slice at the given offset
pub fn read_u16(data: &[u8], offset: usize) -> Option<u16> {
    if offset + 2 > data.len() {
        return None;
    }
    Some(u16::from_be_bytes([data[offset], data[offset + 1]]))
}

/// Read a u32 from a byte slice at the given offset
pub fn read_u32(data: &[u8], offset: usize) -> Option<u32> {
    if offset + 4 > data.len() {
        return None;
    }
    Some(u32::from_be_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]))
}

/// Write a u32 to a byte slice at the given offset
pub fn write_u32(data: &mut [u8], offset: usize, value: u32) -> Result<()> {
    if offset + 4 > data.len() {
        return Err(Error::new(ErrorCode::Corrupt));
    }
    let bytes = value.to_be_bytes();
    data[offset] = bytes[0];
    data[offset + 1] = bytes[1];
    data[offset + 2] = bytes[2];
    data[offset + 3] = bytes[3];
    Ok(())
}

/// Write a u16 to a byte slice at the given offset
pub fn write_u16(data: &mut [u8], offset: usize, value: u16) -> Result<()> {
    if offset + 2 > data.len() {
        return Err(Error::new(ErrorCode::Corrupt));
    }
    let bytes = value.to_be_bytes();
    data[offset] = bytes[0];
    data[offset + 1] = bytes[1];
    Ok(())
}
