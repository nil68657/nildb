//! The memtable: a skip list (Pugh, "Skip Lists: A Probabilistic Alternative
//! to Balanced Trees", CACM 1990) of internal keys whose user part is
//! `column family id (4 bytes, big-endian) ++ user key`, so one list holds
//! every column family in (family, key, newest-first) order.
//!
//! The concurrency scheme follows LevelDB's `skiplist.h`: one writer inserts
//! at a time (the arena mutex), readers walk the list without locks, nodes
//! are never removed, and a node is published with a release store after its
//! fields and lower links are written. Nodes live in an arena that is freed
//! only when the last `Arc<MemTable>` goes away.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::ptr;
use std::sync::atomic::{self, AtomicBool, AtomicPtr, AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex};

use nilengine_api::{Error, Result};

use crate::iter::InternalIterator;
use crate::key::{KIND_DELETE, KIND_SEEK, MAX_SEQ, cmp_internal, parse_trailer, trailer};
use crate::rangedel::{Fragments, RangeTombstone};

const MAX_HEIGHT: usize = 12;
const CHUNK_BYTES: usize = 256 << 10;
const NODE_HEADER: usize = 16;

/// Node layout in the arena: this header, then `height` next pointers, then
/// the key bytes, then the value bytes.
#[repr(C)]
struct Node {
    key_len: u32,
    value_len: u32,
    height: u32,
    _pad: u32,
}

impl Node {
    unsafe fn tower(n: *const Node) -> *const AtomicPtr<Node> {
        unsafe { (n as *const u8).add(NODE_HEADER) as *const AtomicPtr<Node> }
    }

    unsafe fn next(n: *const Node, level: usize) -> *mut Node {
        unsafe { (*Self::tower(n).add(level)).load(atomic::Ordering::Acquire) }
    }

    unsafe fn set_next(n: *const Node, level: usize, x: *mut Node) {
        unsafe { (*Self::tower(n).add(level)).store(x, atomic::Ordering::Release) }
    }

    unsafe fn key<'a>(n: *const Node) -> &'a [u8] {
        unsafe {
            let h = (*n).height as usize;
            let p = (n as *const u8).add(NODE_HEADER + 8 * h);
            std::slice::from_raw_parts(p, (*n).key_len as usize)
        }
    }

    unsafe fn value<'a>(n: *const Node) -> &'a [u8] {
        unsafe {
            let h = (*n).height as usize;
            let p = (n as *const u8).add(NODE_HEADER + 8 * h + (*n).key_len as usize);
            std::slice::from_raw_parts(p, (*n).value_len as usize)
        }
    }
}

struct Arena {
    chunks: Vec<Vec<u64>>,
    cur: *mut u8,
    left: usize,
    rng: u64,
    counts: HashMap<u32, (u64, u64)>,
}

impl Arena {
    /// Returns 8-byte aligned, zeroed memory that lives as long as the arena.
    fn alloc(&mut self, size: usize) -> *mut u8 {
        let size = (size + 7) & !7;
        if size > CHUNK_BYTES / 4 {
            let mut v = vec![0u64; size / 8];
            let p = v.as_mut_ptr() as *mut u8;
            self.chunks.push(v);
            return p;
        }
        if size > self.left {
            let mut v = vec![0u64; CHUNK_BYTES / 8];
            self.cur = v.as_mut_ptr() as *mut u8;
            self.left = CHUNK_BYTES;
            self.chunks.push(v);
        }
        let p = self.cur;
        // SAFETY: size <= left, so the result stays inside the current chunk.
        self.cur = unsafe { self.cur.add(size) };
        self.left -= size;
        p
    }

    /// Height with P(h + 1) = P(h) / 4, as in LevelDB.
    fn random_height(&mut self) -> usize {
        let mut h = 1;
        while h < MAX_HEIGHT {
            self.rng ^= self.rng << 13;
            self.rng ^= self.rng >> 7;
            self.rng ^= self.rng << 17;
            if !self.rng.is_multiple_of(4) {
                break;
            }
            h += 1;
        }
        h
    }
}

struct Tombstones {
    list: Vec<(u32, RangeTombstone)>,
    cache: HashMap<u32, Arc<Fragments>>,
}

pub struct MemTable {
    arena: Mutex<Arena>,
    head: *mut Node,
    max_height: AtomicUsize,
    usage: AtomicUsize,
    entries: AtomicU64,
    max_seq: AtomicU64,
    tombs: Mutex<Tombstones>,
    has_tombs: AtomicBool,
    /// Number of the log that replaced this memtable's log when it became
    /// immutable; the flush records it as the manifest's log number.
    next_wal_number: AtomicU64,
}

// SAFETY: the raw pointers point into the arena owned by the MemTable. Nodes
// are immutable after publication except for their next pointers, which are
// atomics; inserts are serialized by the arena mutex.
unsafe impl Send for MemTable {}
unsafe impl Sync for MemTable {}

impl MemTable {
    pub fn new(seed: u64) -> MemTable {
        let mut arena = Arena {
            chunks: Vec::new(),
            cur: ptr::null_mut(),
            left: 0,
            rng: 0x2545_f491_4f6c_dd1d ^ seed.wrapping_mul(0x9e37_79b9),
            counts: HashMap::new(),
        };
        let head = arena.alloc(NODE_HEADER + 8 * MAX_HEIGHT) as *mut Node;
        // SAFETY: freshly allocated, zeroed and large enough for the header.
        unsafe {
            (*head).height = MAX_HEIGHT as u32;
        }
        MemTable {
            arena: Mutex::new(arena),
            head,
            max_height: AtomicUsize::new(1),
            usage: AtomicUsize::new(0),
            entries: AtomicU64::new(0),
            max_seq: AtomicU64::new(0),
            tombs: Mutex::new(Tombstones {
                list: Vec::new(),
                cache: HashMap::new(),
            }),
            has_tombs: AtomicBool::new(false),
            next_wal_number: AtomicU64::new(0),
        }
    }

    /// Bytes of keys, values and node overhead inserted so far.
    pub fn usage(&self) -> usize {
        self.usage.load(atomic::Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.load(atomic::Ordering::Acquire) == 0
            && !self.has_tombs.load(atomic::Ordering::Acquire)
    }

    pub fn max_seq(&self) -> u64 {
        self.max_seq.load(atomic::Ordering::Acquire)
    }

    pub fn set_next_wal_number(&self, n: u64) {
        self.next_wal_number.store(n, atomic::Ordering::Release);
    }

    pub fn next_wal_number(&self) -> u64 {
        self.next_wal_number.load(atomic::Ordering::Acquire)
    }

    /// Puts minus deletes recorded for column family `cf`.
    pub fn live_estimate(&self, cf: u32) -> i64 {
        let a = self.arena.lock().unwrap();
        a.counts.get(&cf).map_or(0, |&(p, d)| p as i64 - d as i64)
    }

    pub fn add(&self, cf: u32, user_key: &[u8], seq: u64, kind: u8, value: &[u8]) {
        let klen = 4 + user_key.len() + 8;
        let mut a = self.arena.lock().unwrap();
        let height = a.random_height();
        let size = NODE_HEADER + 8 * height + klen + value.len();
        let n = a.alloc(size) as *mut Node;
        // SAFETY: `n` points to `size` zeroed bytes; the key and value copies
        // stay inside them. The node is unreachable until the release stores
        // below link it in.
        unsafe {
            (*n).key_len = klen as u32;
            (*n).value_len = value.len() as u32;
            (*n).height = height as u32;
            let kp = (n as *mut u8).add(NODE_HEADER + 8 * height);
            ptr::copy_nonoverlapping(cf.to_be_bytes().as_ptr(), kp, 4);
            ptr::copy_nonoverlapping(user_key.as_ptr(), kp.add(4), user_key.len());
            let t = trailer(seq, kind).to_le_bytes();
            ptr::copy_nonoverlapping(t.as_ptr(), kp.add(4 + user_key.len()), 8);
            ptr::copy_nonoverlapping(value.as_ptr(), kp.add(klen), value.len());

            let key = Node::key(n);
            let mut prev = [ptr::null_mut::<Node>(); MAX_HEIGHT];
            self.find_ge(key, Some(&mut prev));
            let cur = self.max_height.load(atomic::Ordering::Relaxed);
            if height > cur {
                for p in prev.iter_mut().take(height).skip(cur) {
                    *p = self.head;
                }
                self.max_height.store(height, atomic::Ordering::Relaxed);
            }
            for (level, &p) in prev.iter().enumerate().take(height) {
                (*Node::tower(n).add(level)).store(Node::next(p, level), atomic::Ordering::Relaxed);
                Node::set_next(p, level, n);
            }
        }
        let c = a.counts.entry(cf).or_insert((0, 0));
        if kind == KIND_DELETE {
            c.1 += 1;
        } else {
            c.0 += 1;
        }
        drop(a);
        self.usage.fetch_add(size, atomic::Ordering::Relaxed);
        self.max_seq.fetch_max(seq, atomic::Ordering::AcqRel);
        self.entries.fetch_add(1, atomic::Ordering::Release);
    }

    /// First node with key >= `key`, filling `prev` with the rightmost node
    /// before it on every level.
    unsafe fn find_ge(
        &self,
        key: &[u8],
        mut prev: Option<&mut [*mut Node; MAX_HEIGHT]>,
    ) -> *mut Node {
        let mut x = self.head;
        let mut level = self.max_height.load(atomic::Ordering::Relaxed) - 1;
        loop {
            // SAFETY: x is the head or a published node.
            let next = unsafe { Node::next(x, level) };
            if !next.is_null() && cmp_internal(unsafe { Node::key(next) }, key) == Ordering::Less {
                x = next;
            } else {
                if let Some(p) = &mut prev {
                    p[level] = x;
                }
                if level == 0 {
                    return next;
                }
                level -= 1;
            }
        }
    }

    /// Last node with key < `key`, or the head.
    unsafe fn find_lt(&self, key: &[u8]) -> *mut Node {
        let mut x = self.head;
        let mut level = self.max_height.load(atomic::Ordering::Relaxed) - 1;
        loop {
            let next = unsafe { Node::next(x, level) };
            if !next.is_null() && cmp_internal(unsafe { Node::key(next) }, key) == Ordering::Less {
                x = next;
            } else if level == 0 {
                return x;
            } else {
                level -= 1;
            }
        }
    }

    /// Newest version of `user_key` in `cf` with sequence <= `seq`:
    /// `(sequence, kind, value)`. `buf` is scratch space for the target key.
    pub fn get<'a>(
        &'a self,
        cf: u32,
        user_key: &[u8],
        seq: u64,
        buf: &mut Vec<u8>,
    ) -> Option<(u64, u8, &'a [u8])> {
        buf.clear();
        buf.extend_from_slice(&cf.to_be_bytes());
        buf.extend_from_slice(user_key);
        buf.extend_from_slice(&trailer(seq, KIND_SEEK).to_le_bytes());
        // SAFETY: nodes returned by find_ge live as long as `self`.
        unsafe {
            let n = self.find_ge(buf, None);
            if n.is_null() {
                return None;
            }
            let k = Node::key(n);
            if k.len() != buf.len() || k[..k.len() - 8] != buf[..buf.len() - 8] {
                return None;
            }
            let (s, kind) = parse_trailer(k);
            Some((s, kind, Node::value(n)))
        }
    }

    /// Calls `f(cf, internal key, value)` for every entry in order.
    pub fn for_each(&self, mut f: impl FnMut(u32, &[u8], &[u8]) -> Result<()>) -> Result<()> {
        // SAFETY: walking published nodes of a list that outlives the loop.
        unsafe {
            let mut n = Node::next(self.head, 0);
            while !n.is_null() {
                let k = Node::key(n);
                let cf = u32::from_be_bytes(k[..4].try_into().unwrap());
                f(cf, &k[4..], Node::value(n))?;
                n = Node::next(n, 0);
            }
        }
        Ok(())
    }

    pub fn add_range_tombstone(&self, cf: u32, start: &[u8], end: &[u8], seq: u64) {
        let mut t = self.tombs.lock().unwrap();
        t.list.push((
            cf,
            RangeTombstone {
                start: start.to_vec(),
                end: end.to_vec(),
                seq,
            },
        ));
        t.cache.remove(&cf);
        drop(t);
        self.usage
            .fetch_add(start.len() + end.len() + 32, atomic::Ordering::Relaxed);
        self.max_seq.fetch_max(seq, atomic::Ordering::AcqRel);
        self.has_tombs.store(true, atomic::Ordering::Release);
    }

    /// Fragmented range tombstones of `cf`, or `None` when it has none.
    pub fn fragments(&self, cf: u32) -> Option<Arc<Fragments>> {
        if !self.has_tombs.load(atomic::Ordering::Acquire) {
            return None;
        }
        let mut t = self.tombs.lock().unwrap();
        if let Some(f) = t.cache.get(&cf) {
            return (!f.is_empty()).then(|| f.clone());
        }
        let list: Vec<RangeTombstone> = t
            .list
            .iter()
            .filter(|(c, _)| *c == cf)
            .map(|(_, r)| r.clone())
            .collect();
        let f = Arc::new(Fragments::build(&list));
        t.cache.insert(cf, f.clone());
        (!f.is_empty()).then_some(f)
    }

    pub fn tombstones(&self, cf: u32) -> Vec<RangeTombstone> {
        if !self.has_tombs.load(atomic::Ordering::Acquire) {
            return Vec::new();
        }
        let t = self.tombs.lock().unwrap();
        t.list
            .iter()
            .filter(|(c, _)| *c == cf)
            .map(|(_, r)| r.clone())
            .collect()
    }

    pub fn all_tombstones(&self) -> Vec<(u32, RangeTombstone)> {
        self.tombs.lock().unwrap().list.clone()
    }
}

/// Iterates one column family of a memtable. Keys are internal keys without
/// the family prefix.
pub struct MemIter {
    mem: Arc<MemTable>,
    prefix: [u8; 4],
    node: *mut Node,
    buf: Vec<u8>,
}

// SAFETY: the iterator holds an Arc to the memtable its node pointer points
// into, and only reads published nodes.
unsafe impl Send for MemIter {}

impl MemIter {
    pub fn new(mem: Arc<MemTable>, cf: u32) -> MemIter {
        MemIter {
            mem,
            prefix: cf.to_be_bytes(),
            node: ptr::null_mut(),
            buf: Vec::new(),
        }
    }

    fn target(&mut self, prefix: [u8; 4], ikey: &[u8]) {
        self.buf.clear();
        self.buf.extend_from_slice(&prefix);
        self.buf.extend_from_slice(ikey);
    }

    /// Keeps the position only if it is a real node of this family.
    fn settle(&mut self, n: *mut Node) {
        // SAFETY: n is null, the head or a published node.
        self.node =
            if n.is_null() || n == self.mem.head || unsafe { Node::key(n)[..4] != self.prefix } {
                ptr::null_mut()
            } else {
                n
            };
    }
}

impl InternalIterator for MemIter {
    fn valid(&self) -> bool {
        !self.node.is_null()
    }

    fn seek_to_first(&mut self) {
        let mut t = Vec::with_capacity(8);
        t.extend_from_slice(&trailer(MAX_SEQ, KIND_SEEK).to_le_bytes());
        self.seek(&t);
    }

    fn seek_to_last(&mut self) {
        let next = u32::from_be_bytes(self.prefix).wrapping_add(1);
        self.target(
            next.to_be_bytes(),
            &trailer(MAX_SEQ, KIND_SEEK).to_le_bytes(),
        );
        // SAFETY: see settle.
        let n = if next == 0 {
            unsafe {
                let mut x = self.mem.head;
                let mut level = self.mem.max_height.load(atomic::Ordering::Relaxed) - 1;
                loop {
                    let nx = Node::next(x, level);
                    if !nx.is_null() {
                        x = nx;
                    } else if level == 0 {
                        break x;
                    } else {
                        level -= 1;
                    }
                }
            }
        } else {
            unsafe { self.mem.find_lt(&self.buf) }
        };
        self.settle(n);
    }

    fn seek(&mut self, target: &[u8]) {
        self.target(self.prefix, target);
        let n = unsafe { self.mem.find_ge(&self.buf, None) };
        self.settle(n);
    }

    fn seek_for_prev(&mut self, target: &[u8]) {
        self.target(self.prefix, target);
        // SAFETY: see settle.
        let n = unsafe {
            let ge = self.mem.find_ge(&self.buf, None);
            if !ge.is_null() && Node::key(ge) == self.buf.as_slice() {
                ge
            } else {
                self.mem.find_lt(&self.buf)
            }
        };
        self.settle(n);
    }

    fn next(&mut self) {
        let n = unsafe { Node::next(self.node, 0) };
        self.settle(n);
    }

    fn prev(&mut self) {
        let key = unsafe { Node::key(self.node) };
        let n = unsafe { self.mem.find_lt(key) };
        self.settle(n);
    }

    fn key(&self) -> &[u8] {
        unsafe { &Node::key(self.node)[4..] }
    }

    fn value(&self) -> &[u8] {
        unsafe { Node::value(self.node) }
    }

    fn status(&self) -> Result<()> {
        Ok(())
    }
}

/// Checks that a batch's column family ids exist before replay inserts it.
pub fn check_cf(cf: u32, num_cfs: usize) -> Result<()> {
    if (cf as usize) < num_cfs {
        Ok(())
    } else {
        Err(Error::corruption(format!(
            "log record names column family {cf}, which does not exist"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{KIND_PUT, make_internal, user_key};

    #[test]
    fn get_returns_newest_visible_version() {
        let m = MemTable::new(1);
        m.add(0, b"k", 1, KIND_PUT, b"v1");
        m.add(0, b"k", 3, KIND_PUT, b"v3");
        m.add(0, b"k", 5, KIND_DELETE, b"");
        m.add(1, b"k", 2, KIND_PUT, b"other");
        let mut buf = Vec::new();
        assert_eq!(m.get(0, b"k", 2, &mut buf), Some((1, KIND_PUT, &b"v1"[..])));
        assert_eq!(m.get(0, b"k", 4, &mut buf), Some((3, KIND_PUT, &b"v3"[..])));
        assert_eq!(m.get(0, b"k", 9, &mut buf).map(|x| x.1), Some(KIND_DELETE));
        assert_eq!(
            m.get(1, b"k", 9, &mut buf),
            Some((2, KIND_PUT, &b"other"[..]))
        );
        assert_eq!(m.get(0, b"j", 9, &mut buf), None);
        assert_eq!(m.get(2, b"k", 9, &mut buf), None);
    }

    #[test]
    fn iterator_stays_in_its_family() {
        let m = Arc::new(MemTable::new(1));
        for (i, k) in [b"a", b"b", b"c"].iter().enumerate() {
            m.add(1, *k, i as u64 + 1, KIND_PUT, b"x");
            m.add(0, *k, i as u64 + 10, KIND_PUT, b"y");
            m.add(2, *k, i as u64 + 20, KIND_PUT, b"z");
        }
        let mut it = MemIter::new(m, 1);
        it.seek_to_first();
        let mut keys = Vec::new();
        while it.valid() {
            keys.push(user_key(it.key()).to_vec());
            it.next();
        }
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        it.seek_to_last();
        assert_eq!(user_key(it.key()), b"c");
        it.prev();
        assert_eq!(user_key(it.key()), b"b");
        it.seek_for_prev(&make_internal(b"bb", 0, KIND_DELETE));
        assert_eq!(user_key(it.key()), b"b");
        it.seek(&make_internal(b"d", MAX_SEQ, KIND_SEEK));
        assert!(!it.valid());
    }
}
