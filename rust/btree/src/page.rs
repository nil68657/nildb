//! Page formats. Every page starts with a 40-byte header:
//!
//! ```text
//! 0..4    CRC-32 of bytes 4..page_size
//! 4       type: 1 meta, 2 branch, 3 leaf, 4 overflow
//! 5       flags (zero)
//! 6..8    cell count (u16)
//! 8..16   id of the transaction that wrote the page
//! 16..24  the page's own number
//! 24..32  aux1: branch leftmost child page; overflow next page (0 ends)
//! 32..40  aux2: branch leftmost child txn; overflow bytes in this page
//! ```
//!
//! Leaf and branch pages are slotted: a u16 offset per cell follows the
//! header in key order, and cells fill the page from the end. A leaf cell is
//! `[flags u8][key len u16][value len u32][key][value]`, or with flag bit 0
//! `[..][key][overflow page u64][overflow txn u64]` when the value lives in
//! an overflow chain. A branch cell is `[key len u16][child page u64][child
//! txn u64][key]`; its child holds keys at or above the cell's key and below
//! the next cell's key, and the leftmost child (in the header) holds keys
//! below the first cell's key. Every child reference carries the id of the
//! transaction that wrote the child, so recovery can tell a page that reached
//! the disk from an older page left at the same number (btrfs stores
//! generation numbers in its tree pointers for the same reason).

use nilengine_api::coding::{crc32, get_u16, get_u32, get_u64, set_u16, set_u32, set_u64};
use nilengine_api::{Error, Result};

pub const HEADER: usize = 40;
pub const TYPE_META: u8 = 1;
pub const TYPE_BRANCH: u8 = 2;
pub const TYPE_LEAF: u8 = 3;
pub const TYPE_OVERFLOW: u8 = 4;
const LEAF_CELL: usize = 7;
const BRANCH_CELL: usize = 18;
const FLAG_OVERFLOW: u8 = 1;
pub const META_MAGIC: u64 = u64::from_le_bytes(*b"NILBPTRE");
pub const FORMAT_VERSION: u32 = 1;

pub fn page_type(p: &[u8]) -> u8 {
    p[4]
}

pub fn count(p: &[u8]) -> usize {
    get_u16(p, 6) as usize
}

pub fn page_txn(p: &[u8]) -> u64 {
    get_u64(p, 8)
}

pub fn page_no(p: &[u8]) -> u64 {
    get_u64(p, 16)
}

pub fn aux1(p: &[u8]) -> u64 {
    get_u64(p, 24)
}

pub fn aux2(p: &[u8]) -> u64 {
    get_u64(p, 32)
}

pub fn init_header(
    p: &mut [u8],
    ty: u8,
    count: usize,
    txn: u64,
    page_no: u64,
    aux1: u64,
    aux2: u64,
) {
    p[4] = ty;
    p[5] = 0;
    set_u16(p, 6, count as u16);
    set_u64(p, 8, txn);
    set_u64(p, 16, page_no);
    set_u64(p, 24, aux1);
    set_u64(p, 32, aux2);
}

pub fn seal(p: &mut [u8]) {
    let crc = crc32(&p[4..]);
    set_u32(p, 0, crc);
}

pub fn checksum_ok(p: &[u8]) -> bool {
    get_u32(p, 0) == crc32(&p[4..])
}

/// Bytes a cell may use, so that four cells always fit in a page and a
/// split always yields two pages that fit.
pub fn max_cell(page_size: usize) -> usize {
    (page_size - HEADER) / 4
}

/// Longest key: a branch cell or a leaf cell with an overflow reference must
/// fit in `max_cell`.
pub fn max_key_len(page_size: usize) -> usize {
    max_cell(page_size) - 2 - LEAF_CELL - 16 - 8
}

/// Payload bytes of one overflow page.
pub fn overflow_capacity(page_size: usize) -> usize {
    page_size - HEADER
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChildRef {
    pub page: u64,
    pub txn: u64,
}

#[derive(Clone, Debug)]
pub enum LeafVal {
    Inline(Vec<u8>),
    Overflow { page: u64, txn: u64, len: u32 },
}

#[derive(Clone, Debug)]
pub struct LeafCell {
    pub key: Vec<u8>,
    pub val: LeafVal,
}

impl LeafCell {
    /// Bytes the cell uses in a page, slot included.
    pub fn size(&self) -> usize {
        2 + LEAF_CELL
            + self.key.len()
            + match &self.val {
                LeafVal::Inline(v) => v.len(),
                LeafVal::Overflow { .. } => 16,
            }
    }
}

pub fn inline_cell_size(key_len: usize, val_len: usize) -> usize {
    2 + LEAF_CELL + key_len + val_len
}

pub fn branch_cell_size(key_len: usize) -> usize {
    2 + BRANCH_CELL + key_len
}

/// A leaf or branch page parsed for modification.
#[derive(Clone, Debug)]
pub enum Node {
    Leaf(Vec<LeafCell>),
    Branch {
        first: ChildRef,
        cells: Vec<(Vec<u8>, ChildRef)>,
    },
}

impl Node {
    pub fn size(&self) -> usize {
        HEADER
            + match self {
                Node::Leaf(cells) => cells.iter().map(LeafCell::size).sum::<usize>(),
                Node::Branch { cells, .. } => {
                    cells.iter().map(|(k, _)| branch_cell_size(k.len())).sum()
                }
            }
    }

    /// A leaf without cells, or a branch whose last child was removed.
    pub fn is_empty(&self) -> bool {
        match self {
            Node::Leaf(c) => c.is_empty(),
            Node::Branch { first, cells } => cells.is_empty() && first.page == 0,
        }
    }

    /// Parses a page that passed [`validate`].
    pub fn parse(p: &[u8]) -> Node {
        let n = count(p);
        if page_type(p) == TYPE_LEAF {
            let cells = (0..n)
                .map(|i| {
                    let key = leaf_key(p, i).to_vec();
                    let val = match leaf_val(p, i) {
                        ValRef::Inline(v) => LeafVal::Inline(v.to_vec()),
                        ValRef::Overflow { page, txn, len } => LeafVal::Overflow { page, txn, len },
                    };
                    LeafCell { key, val }
                })
                .collect();
            Node::Leaf(cells)
        } else {
            let first = branch_child(p, 0);
            let cells = (0..n)
                .map(|i| (branch_key(p, i).to_vec(), branch_child(p, i + 1)))
                .collect();
            Node::Branch { first, cells }
        }
    }

    /// Serializes the node into a sealed page.
    pub fn write(&self, page_size: usize, txn: u64, page: u64) -> Vec<u8> {
        let mut p = vec![0u8; page_size];
        let mut end = page_size;
        match self {
            Node::Leaf(cells) => {
                for (i, c) in cells.iter().enumerate() {
                    let body = c.size() - 2;
                    end -= body;
                    let o = end;
                    let (flags, vlen) = match &c.val {
                        LeafVal::Inline(v) => (0, v.len() as u32),
                        LeafVal::Overflow { len, .. } => (FLAG_OVERFLOW, *len),
                    };
                    p[o] = flags;
                    set_u16(&mut p, o + 1, c.key.len() as u16);
                    set_u32(&mut p, o + 3, vlen);
                    let k = o + LEAF_CELL;
                    p[k..k + c.key.len()].copy_from_slice(&c.key);
                    let v = k + c.key.len();
                    match &c.val {
                        LeafVal::Inline(val) => p[v..v + val.len()].copy_from_slice(val),
                        LeafVal::Overflow { page, txn, .. } => {
                            set_u64(&mut p, v, *page);
                            set_u64(&mut p, v + 8, *txn);
                        }
                    }
                    set_u16(&mut p, HEADER + 2 * i, o as u16);
                }
                debug_assert!(HEADER + 2 * cells.len() <= end, "leaf overflows its page");
                init_header(&mut p, TYPE_LEAF, cells.len(), txn, page, 0, 0);
            }
            Node::Branch { first, cells } => {
                for (i, (key, child)) in cells.iter().enumerate() {
                    end -= BRANCH_CELL + key.len();
                    let o = end;
                    set_u16(&mut p, o, key.len() as u16);
                    set_u64(&mut p, o + 2, child.page);
                    set_u64(&mut p, o + 10, child.txn);
                    p[o + BRANCH_CELL..o + BRANCH_CELL + key.len()].copy_from_slice(key);
                    set_u16(&mut p, HEADER + 2 * i, o as u16);
                }
                debug_assert!(HEADER + 2 * cells.len() <= end, "branch overflows its page");
                init_header(
                    &mut p,
                    TYPE_BRANCH,
                    cells.len(),
                    txn,
                    page,
                    first.page,
                    first.txn,
                );
            }
        }
        seal(&mut p);
        p
    }
}

/// Checks the header and every slot of a leaf or branch page read from disk,
/// so the accessors below can index without bounds errors.
pub fn validate(p: &[u8], expect_page: u64) -> Result<()> {
    let bad = |what: &str| Err(Error::corruption(format!("page {expect_page}: {what}")));
    if !checksum_ok(p) {
        return bad("checksum mismatch");
    }
    if page_no(p) != expect_page {
        return bad(&format!("header names page {}", page_no(p)));
    }
    let ty = page_type(p);
    if ty != TYPE_LEAF && ty != TYPE_BRANCH {
        return if ty == TYPE_OVERFLOW || ty == TYPE_META {
            Ok(())
        } else {
            bad(&format!("unknown page type {ty}"))
        };
    }
    let n = count(p);
    let slots_end = HEADER + 2 * n;
    if slots_end > p.len() {
        return bad("slot array past the page end");
    }
    for i in 0..n {
        let o = get_u16(p, HEADER + 2 * i) as usize;
        if o < slots_end {
            return bad("cell overlaps the slot array");
        }
        let len = if ty == TYPE_LEAF {
            if o + LEAF_CELL > p.len() {
                return bad("leaf cell header past the page end");
            }
            let klen = get_u16(p, o + 1) as usize;
            let vlen = if p[o] & FLAG_OVERFLOW != 0 {
                16
            } else {
                get_u32(p, o + 3) as usize
            };
            LEAF_CELL + klen + vlen
        } else {
            if o + BRANCH_CELL > p.len() {
                return bad("branch cell header past the page end");
            }
            BRANCH_CELL + get_u16(p, o) as usize
        };
        if o + len > p.len() {
            return bad("cell past the page end");
        }
    }
    Ok(())
}

fn slot(p: &[u8], i: usize) -> usize {
    get_u16(p, HEADER + 2 * i) as usize
}

pub fn leaf_key(p: &[u8], i: usize) -> &[u8] {
    let o = slot(p, i);
    let klen = get_u16(p, o + 1) as usize;
    &p[o + LEAF_CELL..o + LEAF_CELL + klen]
}

pub enum ValRef<'a> {
    Inline(&'a [u8]),
    Overflow { page: u64, txn: u64, len: u32 },
}

pub fn leaf_val(p: &[u8], i: usize) -> ValRef<'_> {
    let o = slot(p, i);
    let klen = get_u16(p, o + 1) as usize;
    let vlen = get_u32(p, o + 3);
    let v = o + LEAF_CELL + klen;
    if p[o] & FLAG_OVERFLOW != 0 {
        ValRef::Overflow {
            page: get_u64(p, v),
            txn: get_u64(p, v + 8),
            len: vlen,
        }
    } else {
        ValRef::Inline(&p[v..v + vlen as usize])
    }
}

pub fn branch_key(p: &[u8], i: usize) -> &[u8] {
    let o = slot(p, i);
    let klen = get_u16(p, o) as usize;
    &p[o + BRANCH_CELL..o + BRANCH_CELL + klen]
}

/// Child `i` of a branch, 0 being the leftmost.
pub fn branch_child(p: &[u8], i: usize) -> ChildRef {
    if i == 0 {
        return ChildRef {
            page: aux1(p),
            txn: aux2(p),
        };
    }
    let o = slot(p, i - 1);
    ChildRef {
        page: get_u64(p, o + 2),
        txn: get_u64(p, o + 10),
    }
}

/// Index of the child of a branch page that covers `key`.
pub fn branch_find(p: &[u8], key: &[u8]) -> usize {
    let (mut lo, mut hi) = (0, count(p));
    while lo < hi {
        let mid = (lo + hi) / 2;
        if branch_key(p, mid) <= key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// First cell of a leaf page with key >= `key` (or > `key` when `strict`).
pub fn leaf_search(p: &[u8], key: &[u8], strict: bool) -> usize {
    let (mut lo, mut hi) = (0, count(p));
    while lo < hi {
        let mid = (lo + hi) / 2;
        let k = leaf_key(p, mid);
        if k < key || (strict && k == key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// A column family as the meta page records it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaCf {
    pub name: String,
    pub root: u64,
    pub root_txn: u64,
    pub keys: u64,
}

/// The meta page: after the header, `magic u64, version u32, page size u32,
/// page count u64, cf count u32`, then per column family `name length u8,
/// name, root page u64, root txn u64, key count u64`. The header's txn is
/// the id of the commit the meta describes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Meta {
    pub txn: u64,
    pub page_size: u32,
    pub num_pages: u64,
    pub cfs: Vec<MetaCf>,
}

impl Meta {
    pub fn encoded_len(&self) -> usize {
        HEADER
            + 28
            + self
                .cfs
                .iter()
                .map(|c| 1 + c.name.len() + 24)
                .sum::<usize>()
    }

    pub fn encode(&self, slot: u64) -> Result<Vec<u8>> {
        let ps = self.page_size as usize;
        if self.encoded_len() > ps {
            return Err(Error::invalid(format!(
                "{} column families do not fit in a {ps}-byte meta page",
                self.cfs.len()
            )));
        }
        let mut p = vec![0u8; ps];
        set_u64(&mut p, HEADER, META_MAGIC);
        set_u32(&mut p, HEADER + 8, FORMAT_VERSION);
        set_u32(&mut p, HEADER + 12, self.page_size);
        set_u64(&mut p, HEADER + 16, self.num_pages);
        set_u32(&mut p, HEADER + 24, self.cfs.len() as u32);
        let mut o = HEADER + 28;
        for c in &self.cfs {
            p[o] = c.name.len() as u8;
            p[o + 1..o + 1 + c.name.len()].copy_from_slice(c.name.as_bytes());
            o += 1 + c.name.len();
            set_u64(&mut p, o, c.root);
            set_u64(&mut p, o + 8, c.root_txn);
            set_u64(&mut p, o + 16, c.keys);
            o += 24;
        }
        init_header(&mut p, TYPE_META, 0, self.txn, slot, 0, 0);
        seal(&mut p);
        Ok(p)
    }

    /// Decodes a meta page; `None` when it is torn, blank or not a meta page.
    pub fn decode(p: &[u8], slot: u64) -> Option<Meta> {
        if p.len() < HEADER + 28
            || !checksum_ok(p)
            || page_type(p) != TYPE_META
            || page_no(p) != slot
        {
            return None;
        }
        if get_u64(p, HEADER) != META_MAGIC || get_u32(p, HEADER + 8) != FORMAT_VERSION {
            return None;
        }
        let page_size = get_u32(p, HEADER + 12);
        if page_size as usize != p.len() {
            return None;
        }
        let num_pages = get_u64(p, HEADER + 16);
        let n = get_u32(p, HEADER + 24) as usize;
        let mut o = HEADER + 28;
        let mut cfs = Vec::with_capacity(n);
        for _ in 0..n {
            let len = *p.get(o)? as usize;
            let name = std::str::from_utf8(p.get(o + 1..o + 1 + len)?)
                .ok()?
                .to_string();
            o += 1 + len;
            if o + 24 > p.len() {
                return None;
            }
            cfs.push(MetaCf {
                name,
                root: get_u64(p, o),
                root_txn: get_u64(p, o + 8),
                keys: get_u64(p, o + 16),
            });
            o += 24;
        }
        Some(Meta {
            txn: page_txn(p),
            page_size,
            num_pages,
            cfs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_round_trip() {
        let n = Node::Leaf(vec![
            LeafCell {
                key: b"a".to_vec(),
                val: LeafVal::Inline(b"1".to_vec()),
            },
            LeafCell {
                key: b"b".to_vec(),
                val: LeafVal::Overflow {
                    page: 9,
                    txn: 3,
                    len: 70000,
                },
            },
            LeafCell {
                key: Vec::new(),
                val: LeafVal::Inline(Vec::new()),
            },
        ]);
        let p = n.write(4096, 5, 12);
        validate(&p, 12).unwrap();
        assert_eq!(page_txn(&p), 5);
        assert_eq!(leaf_key(&p, 1), b"b");
        assert!(matches!(
            leaf_val(&p, 1),
            ValRef::Overflow {
                page: 9,
                txn: 3,
                len: 70000
            }
        ));
        assert_eq!(Node::parse(&p).size(), n.size());
        assert!(validate(&p, 13).is_err());
        let mut bad = p.clone();
        bad[100] ^= 1;
        assert!(validate(&bad, 12).is_err());
    }

    #[test]
    fn branch_routing() {
        let c = |p| ChildRef { page: p, txn: 1 };
        let n = Node::Branch {
            first: c(10),
            cells: vec![(b"g".to_vec(), c(11)), (b"p".to_vec(), c(12))],
        };
        let p = n.write(4096, 1, 3);
        validate(&p, 3).unwrap();
        assert_eq!(branch_child(&p, branch_find(&p, b"a")).page, 10);
        assert_eq!(branch_child(&p, branch_find(&p, b"g")).page, 11);
        assert_eq!(branch_child(&p, branch_find(&p, b"o")).page, 11);
        assert_eq!(branch_child(&p, branch_find(&p, b"p")).page, 12);
        assert_eq!(branch_child(&p, branch_find(&p, b"z")).page, 12);
    }

    #[test]
    fn meta_round_trip_and_torn_detection() {
        let m = Meta {
            txn: 42,
            page_size: 4096,
            num_pages: 100,
            cfs: vec![MetaCf {
                name: "default".into(),
                root: 7,
                root_txn: 40,
                keys: 1234,
            }],
        };
        let mut p = m.encode(1).unwrap();
        assert_eq!(Meta::decode(&p, 1), Some(m));
        assert_eq!(Meta::decode(&p, 0), None);
        p[3000] ^= 1;
        assert_eq!(Meta::decode(&p, 1), None);
    }
}
