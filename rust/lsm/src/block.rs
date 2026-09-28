//! Blocks in the format of LevelDB's `doc/table_format.md`: each entry
//! stores the length of the key prefix it shares with the previous entry and
//! only the rest of the key; every `interval` entries a restart point stores
//! a full key, and the block ends with the array of restart offsets. An entry
//! is `[shared varint32][unshared varint32][value length varint32][unshared
//! key bytes][value]`; the block ends with `restart offsets (u32 LE each)`
//! and `restart count (u32 LE)`. Keys are internal keys.

use std::cmp::Ordering;
use std::sync::Arc;

use nilengine_api::coding::{get_varint32, put_varint32};
use nilengine_api::{Error, Result};

use crate::key::cmp_internal;

pub struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    counter: usize,
    interval: usize,
    last_key: Vec<u8>,
    entries: usize,
}

impl BlockBuilder {
    pub fn new(interval: usize) -> Self {
        BlockBuilder {
            buf: Vec::new(),
            restarts: vec![0],
            counter: 0,
            interval: interval.max(1),
            last_key: Vec::new(),
            entries: 0,
        }
    }

    /// Adds an entry; keys must arrive in increasing internal-key order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let mut shared = 0;
        if self.counter < self.interval {
            let max = self.last_key.len().min(key.len());
            while shared < max && self.last_key[shared] == key[shared] {
                shared += 1;
            }
        } else {
            self.restarts.push(self.buf.len() as u32);
            self.counter = 0;
        }
        put_varint32(&mut self.buf, shared as u32);
        put_varint32(&mut self.buf, (key.len() - shared) as u32);
        put_varint32(&mut self.buf, value.len() as u32);
        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(value);
        self.last_key.truncate(shared);
        self.last_key.extend_from_slice(&key[shared..]);
        self.counter += 1;
        self.entries += 1;
    }

    pub fn size_estimate(&self) -> usize {
        self.buf.len() + self.restarts.len() * 4 + 4
    }

    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    pub fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    /// Appends the restart array and returns the finished block. Call
    /// `reset` before adding again.
    pub fn finish(&mut self) -> &[u8] {
        for r in &self.restarts {
            self.buf.extend_from_slice(&r.to_le_bytes());
        }
        self.buf
            .extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        &self.buf
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.last_key.clear();
        self.entries = 0;
    }
}

pub struct Block {
    data: Vec<u8>,
    restarts_off: usize,
    num_restarts: usize,
}

impl Block {
    pub fn new(data: Vec<u8>) -> Result<Block> {
        if data.len() < 4 {
            return Err(Error::corruption("block shorter than its restart count"));
        }
        let n = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap()) as usize;
        let max = (data.len() - 4) / 4;
        if n == 0 || n > max {
            return Err(Error::corruption(format!(
                "block claims {n} restart points"
            )));
        }
        let restarts_off = data.len() - 4 - 4 * n;
        let b = Block {
            data,
            restarts_off,
            num_restarts: n,
        };
        for i in 0..n {
            if b.restart_point(i) > restarts_off {
                return Err(Error::corruption("block restart point past its entries"));
            }
        }
        Ok(b)
    }

    fn restart_point(&self, i: usize) -> usize {
        let off = self.restarts_off + 4 * i;
        u32::from_le_bytes(self.data[off..off + 4].try_into().unwrap()) as usize
    }

    /// Bytes held, for cache accounting.
    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn iter(self: Arc<Self>) -> BlockIter {
        let end = self.restarts_off;
        BlockIter {
            block: self,
            current: end,
            next: end,
            restart_index: 0,
            key: Vec::new(),
            value: (0, 0),
            err: None,
        }
    }
}

/// `(shared, unshared, value length, header length)` of the entry at `off`.
fn decode_entry(data: &[u8], off: usize, limit: usize) -> Result<(usize, usize, usize, usize)> {
    let p = &data[off..limit];
    let (shared, a) = get_varint32(p)?;
    let (unshared, b) = get_varint32(&p[a..])?;
    let (vlen, c) = get_varint32(&p[a + b..])?;
    let header = a + b + c;
    if header + unshared as usize + vlen as usize > p.len() {
        return Err(Error::corruption("block entry runs past the block"));
    }
    Ok((shared as usize, unshared as usize, vlen as usize, header))
}

pub struct BlockIter {
    block: Arc<Block>,
    /// Offset of the current entry; `restarts_off` when invalid.
    current: usize,
    next: usize,
    restart_index: usize,
    key: Vec<u8>,
    value: (usize, usize),
    err: Option<Error>,
}

impl BlockIter {
    pub fn valid(&self) -> bool {
        self.err.is_none() && self.current < self.block.restarts_off
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value.0..self.value.1]
    }

    pub fn status(&self) -> Result<()> {
        match &self.err {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    fn invalidate(&mut self) {
        self.current = self.block.restarts_off;
        self.next = self.block.restarts_off;
    }

    fn corrupt(&mut self, msg: &str) {
        self.err = Some(Error::corruption(msg));
        self.invalidate();
    }

    fn seek_to_restart(&mut self, i: usize) {
        self.key.clear();
        self.restart_index = i;
        self.next = self.block.restart_point(i);
    }

    fn parse_next(&mut self) -> bool {
        self.current = self.next;
        let limit = self.block.restarts_off;
        if self.current >= limit {
            self.invalidate();
            return false;
        }
        let (shared, unshared, vlen, header) =
            match decode_entry(&self.block.data, self.current, limit) {
                Ok(e) => e,
                Err(_) => {
                    self.corrupt("undecodable block entry");
                    return false;
                }
            };
        if shared > self.key.len() {
            self.corrupt("block entry shares more key bytes than the previous key has");
            return false;
        }
        let kstart = self.current + header;
        self.key.truncate(shared);
        self.key
            .extend_from_slice(&self.block.data[kstart..kstart + unshared]);
        if self.key.len() < 8 {
            self.corrupt("block key shorter than an internal key trailer");
            return false;
        }
        self.value = (kstart + unshared, kstart + unshared + vlen);
        self.next = self.value.1;
        while self.restart_index + 1 < self.block.num_restarts
            && self.block.restart_point(self.restart_index + 1) <= self.current
        {
            self.restart_index += 1;
        }
        true
    }

    pub fn seek_to_first(&mut self) {
        self.err = None;
        self.seek_to_restart(0);
        self.parse_next();
    }

    pub fn seek_to_last(&mut self) {
        self.err = None;
        self.seek_to_restart(self.block.num_restarts - 1);
        while self.parse_next() && self.next < self.block.restarts_off {}
    }

    /// First entry with key >= `target`.
    pub fn seek(&mut self, target: &[u8]) {
        self.err = None;
        let (mut left, mut right) = (0usize, self.block.num_restarts - 1);
        while left < right {
            let mid = (left + right).div_ceil(2);
            let off = self.block.restart_point(mid);
            let (shared, unshared, _, header) =
                match decode_entry(&self.block.data, off, self.block.restarts_off) {
                    Ok(e) => e,
                    Err(_) => {
                        self.corrupt("undecodable restart entry");
                        return;
                    }
                };
            if shared != 0 || unshared < 8 {
                self.corrupt("restart entry without a full key");
                return;
            }
            let k = &self.block.data[off + header..off + header + unshared];
            if cmp_internal(k, target) == Ordering::Less {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        self.seek_to_restart(left);
        while self.parse_next() {
            if cmp_internal(&self.key, target) != Ordering::Less {
                return;
            }
        }
    }

    /// Last entry with key <= `target`.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        self.seek(target);
        if !self.valid() {
            if self.err.is_none() {
                self.seek_to_last();
            }
            return;
        }
        if cmp_internal(&self.key, target) == Ordering::Greater {
            self.prev();
        }
    }

    pub fn next(&mut self) {
        self.parse_next();
    }

    pub fn prev(&mut self) {
        let original = self.current;
        while self.block.restart_point(self.restart_index) >= original {
            if self.restart_index == 0 {
                self.invalidate();
                return;
            }
            self.restart_index -= 1;
        }
        self.seek_to_restart(self.restart_index);
        while self.parse_next() {
            if self.next >= original {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{KIND_PUT, make_internal, user_key};

    fn build(n: usize, interval: usize) -> Arc<Block> {
        let mut b = BlockBuilder::new(interval);
        for i in 0..n {
            b.add(
                &make_internal(format!("key{i:04}").as_bytes(), 1, KIND_PUT),
                format!("v{i}").as_bytes(),
            );
        }
        Arc::new(Block::new(b.finish().to_vec()).unwrap())
    }

    #[test]
    fn seek_next_prev() {
        for interval in [1, 3, 16] {
            let blk = build(50, interval);
            let mut it = blk.clone().iter();
            it.seek_to_first();
            let mut n = 0;
            while it.valid() {
                assert_eq!(user_key(it.key()), format!("key{n:04}").as_bytes());
                assert_eq!(it.value(), format!("v{n}").as_bytes());
                it.next();
                n += 1;
            }
            assert_eq!(n, 50);
            it.seek_to_last();
            for i in (0..50).rev() {
                assert!(it.valid());
                assert_eq!(user_key(it.key()), format!("key{i:04}").as_bytes());
                it.prev();
            }
            assert!(!it.valid());
            it.seek(&make_internal(b"key0025x", 9, KIND_PUT));
            assert_eq!(user_key(it.key()), b"key0026");
            it.seek_for_prev(&make_internal(b"key0025x", 9, KIND_PUT));
            assert_eq!(user_key(it.key()), b"key0025");
            it.seek_for_prev(&make_internal(b"a", 9, KIND_PUT));
            assert!(!it.valid());
            it.seek(&make_internal(b"z", 9, KIND_PUT));
            assert!(!it.valid());
            it.seek_for_prev(&make_internal(b"z", 9, KIND_PUT));
            assert_eq!(user_key(it.key()), b"key0049");
        }
    }

    #[test]
    fn empty_block() {
        let mut b = BlockBuilder::new(16);
        let blk = Arc::new(Block::new(b.finish().to_vec()).unwrap());
        let mut it = blk.iter();
        it.seek_to_first();
        assert!(!it.valid());
        it.seek_to_last();
        assert!(!it.valid());
    }
}
