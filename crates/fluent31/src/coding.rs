//! Little-endian fixed-width and LEB128 varint primitives shared by every
//! on-disk format in the engine.

use crate::error::{corrupt, Result};

pub fn put_u32(dst: &mut Vec<u8>, v: u32) {
    dst.extend_from_slice(&v.to_le_bytes());
}

pub fn put_u64(dst: &mut Vec<u8>, v: u64) {
    dst.extend_from_slice(&v.to_le_bytes());
}

pub fn put_uvarint(dst: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        dst.push((v as u8) | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
}

pub fn put_len_prefixed(dst: &mut Vec<u8>, bytes: &[u8]) {
    put_uvarint(dst, bytes.len() as u64);
    dst.extend_from_slice(bytes);
}

/// Cursor over a byte slice with checked reads; every decoder in the engine
/// funnels through this so truncation always surfaces as `Corruption`.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(corrupt("short read while decoding"));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    pub fn uvarint(&mut self) -> Result<u64> {
        let mut v: u64 = 0;
        let mut shift = 0u32;
        loop {
            let b = self.u8()?;
            if shift == 63 && b > 1 {
                return Err(corrupt("uvarint overflow"));
            }
            v |= u64::from(b & 0x7f) << shift;
            if b < 0x80 {
                return Ok(v);
            }
            shift += 7;
            if shift > 63 {
                return Err(corrupt("uvarint too long"));
            }
        }
    }

    pub fn len_prefixed(&mut self) -> Result<&'a [u8]> {
        let n = self.uvarint()?;
        if n > self.remaining() as u64 {
            return Err(corrupt("length prefix beyond buffer"));
        }
        self.bytes(n as usize)
    }
}

pub fn crc32(data: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(data);
    h.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        let vals = [
            0u64,
            1,
            127,
            128,
            300,
            16383,
            16384,
            u32::MAX as u64,
            u64::MAX / 2,
            u64::MAX,
        ];
        for &v in &vals {
            let mut b = Vec::new();
            put_uvarint(&mut b, v);
            let mut r = Reader::new(&b);
            assert_eq!(r.uvarint().unwrap(), v);
            assert!(r.is_empty());
        }
    }

    #[test]
    fn varint_truncated_is_corruption() {
        let mut b = Vec::new();
        put_uvarint(&mut b, u64::MAX);
        b.pop();
        let mut r = Reader::new(&b);
        assert!(r.uvarint().is_err());
    }

    #[test]
    fn len_prefixed_roundtrip() {
        let mut b = Vec::new();
        put_len_prefixed(&mut b, b"hello");
        put_len_prefixed(&mut b, b"");
        let mut r = Reader::new(&b);
        assert_eq!(r.len_prefixed().unwrap(), b"hello");
        assert_eq!(r.len_prefixed().unwrap(), b"");
    }
}
