//! Byte encodings shared by both engines: LEB128 varints, fixed-width
//! little-endian integers, length-prefixed slices and the CRC-32 checksum.

use crate::error::{Error, Result};

pub fn put_varint32(dst: &mut Vec<u8>, v: u32) {
    put_varint64(dst, v as u64);
}

pub fn put_varint64(dst: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        dst.push((v as u8) | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
}

pub fn varint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

/// Decodes a varint at the front of `src`, returning the value and the number
/// of bytes it used.
pub fn get_varint64(src: &[u8]) -> Result<(u64, usize)> {
    let mut v: u64 = 0;
    for (i, &b) in src.iter().enumerate().take(10) {
        v |= ((b & 0x7f) as u64) << (7 * i);
        if b < 0x80 {
            return Ok((v, i + 1));
        }
    }
    Err(Error::corruption("truncated or overlong varint"))
}

pub fn get_varint32(src: &[u8]) -> Result<(u32, usize)> {
    let (v, n) = get_varint64(src)?;
    if v > u32::MAX as u64 {
        return Err(Error::corruption("varint32 out of range"));
    }
    Ok((v as u32, n))
}

pub fn put_length_prefixed(dst: &mut Vec<u8>, s: &[u8]) {
    put_varint32(dst, s.len() as u32);
    dst.extend_from_slice(s);
}

/// Reads a varint32 length and that many bytes from the front of `src`.
pub fn get_length_prefixed(src: &[u8]) -> Result<(&[u8], usize)> {
    let (len, n) = get_varint32(src)?;
    let end = n
        .checked_add(len as usize)
        .ok_or_else(|| Error::corruption("slice length overflow"))?;
    if end > src.len() {
        return Err(Error::corruption(
            "length-prefixed slice runs past its buffer",
        ));
    }
    Ok((&src[n..end], end))
}

/// A cursor over an encoded buffer. Every getter fails with `Corruption`
/// instead of panicking when the buffer is short.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    pub fn remaining(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    pub fn varint32(&mut self) -> Result<u32> {
        let (v, n) = get_varint32(&self.buf[self.pos..])?;
        self.pos += n;
        Ok(v)
    }

    pub fn varint64(&mut self) -> Result<u64> {
        let (v, n) = get_varint64(&self.buf[self.pos..])?;
        self.pos += n;
        Ok(v)
    }

    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let (s, n) = get_length_prefixed(&self.buf[self.pos..])?;
        self.pos += n;
        Ok(s)
    }

    pub fn u8(&mut self) -> Result<u8> {
        let b = *self
            .buf
            .get(self.pos)
            .ok_or_else(|| Error::corruption("buffer ends before a byte field"))?;
        self.pos += 1;
        Ok(b)
    }

    pub fn fixed64(&mut self) -> Result<u64> {
        let end = self.pos + 8;
        if end > self.buf.len() {
            return Err(Error::corruption("buffer ends before a fixed64 field"));
        }
        let v = u64::from_le_bytes(self.buf[self.pos..end].try_into().unwrap());
        self.pos = end;
        Ok(v)
    }

    pub fn fixed32(&mut self) -> Result<u32> {
        let end = self.pos + 4;
        if end > self.buf.len() {
            return Err(Error::corruption("buffer ends before a fixed32 field"));
        }
        let v = u32::from_le_bytes(self.buf[self.pos..end].try_into().unwrap());
        self.pos = end;
        Ok(v)
    }
}

pub fn get_u16(b: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([b[off], b[off + 1]])
}

pub fn get_u32(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(b[off..off + 4].try_into().unwrap())
}

pub fn get_u64(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().unwrap())
}

pub fn set_u16(b: &mut [u8], off: usize, v: u16) {
    b[off..off + 2].copy_from_slice(&v.to_le_bytes());
}

pub fn set_u32(b: &mut [u8], off: usize, v: u32) {
    b[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

pub fn set_u64(b: &mut [u8], off: usize, v: u64) {
    b[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

/// CRC-32 (IEEE polynomial, the zlib variant) through crc32fast, which uses
/// the ARMv8 CRC instructions on Apple Silicon.
pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

pub fn crc32_parts(parts: &[&[u8]]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    for p in parts {
        h.update(p);
    }
    h.finalize()
}

/// MurmurHash64A by Austin Appleby (public domain, from the SMHasher
/// repository). The LSM bloom filters and the cache shard picker use it.
pub fn hash64(data: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;
    let mut h = seed ^ (data.len() as u64).wrapping_mul(M);
    let (chunks, tail) = data.as_chunks::<8>();
    for c in chunks {
        let mut k = u64::from_le_bytes(*c);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }
    if !tail.is_empty() {
        let mut t = [0u8; 8];
        t[..tail.len()].copy_from_slice(tail);
        h ^= u64::from_le_bytes(t);
        h = h.wrapping_mul(M);
    }
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_round_trip() {
        for v in [0u64, 1, 127, 128, 300, 1 << 32, u64::MAX] {
            let mut b = Vec::new();
            put_varint64(&mut b, v);
            assert_eq!(b.len(), varint_len(v));
            assert_eq!(get_varint64(&b).unwrap(), (v, b.len()));
        }
        assert!(get_varint64(&[0x80, 0x80]).is_err());
        assert!(get_varint32(&[0xff, 0xff, 0xff, 0xff, 0x7f]).is_err());
    }

    #[test]
    fn length_prefixed_rejects_short_buffers() {
        let mut b = Vec::new();
        put_length_prefixed(&mut b, b"hello");
        assert_eq!(get_length_prefixed(&b).unwrap(), (&b"hello"[..], 6));
        assert!(get_length_prefixed(&b[..4]).is_err());
    }

    #[test]
    fn hash_differs_by_seed_and_input() {
        assert_ne!(hash64(b"a", 0), hash64(b"b", 0));
        assert_ne!(hash64(b"a", 0), hash64(b"a", 1));
        assert_eq!(hash64(b"abcdefghij", 7), hash64(b"abcdefghij", 7));
    }
}
