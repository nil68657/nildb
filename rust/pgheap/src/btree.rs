//! The B-tree index of one column family or TOAST relation, after
//! PostgreSQL's nbtree (src/backend/access/nbtree/README): Lehman and Yao's
//! B-link tree, where every page but the rightmost of its level carries a
//! high key and a right link. A reader holds one page lock at a time and
//! moves right when its key is at or above a page's high key, so it finds
//! its way past a split that happened after it read the parent.
//!
//! Entries are (key, tid) pairs sorted by key, then tid, so every entry is
//! unique (PostgreSQL 12's heap-TID tiebreaker) and a key may have several
//! entries, one per HOT chain. Pages are never merged or deleted.
//!
//! Layout: block 0 is the meta page (magic u32, version u32, root u32,
//! level u32 after the page header). Other pages keep 16 bytes of special
//! space: left sibling u32, right sibling u32, level u32 (0 for leaves),
//! flags u16, unused u16. A leaf item is `[tid block u32][tid offset u16]
//! [key length u16][key]`; an internal item is `[child u32]` followed by the
//! same fields for its separator. On a page with a right sibling, item 1 is
//! the high key; on an internal page, the first data item's key counts as
//! minus infinity.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering as AtOrd};

use nilengine_api::coding::{get_u16, get_u32, set_u16, set_u32};
use nilengine_api::{Error, Result};

use crate::bufmgr::{BufferPool, FORK_MAIN, Locked, Mode};
use crate::heap::Tid;
use crate::page::{self, BLCKSZ, HEADER, ITEM_ID};
use crate::wal::{
    PageTag, RM_BTREE_INSERT, RM_BTREE_NEWROOT, RM_BTREE_SPLIT, RM_BTREE_VACUUM, RecBlock,
};

const SPECIAL: usize = 16;
const SP: usize = BLCKSZ - SPECIAL;
const BTP_ROOT: u16 = 2;
const META_MAGIC: u32 = 0x4e42_5452; // "NBTR"
const META_VERSION: u32 = 1;
/// Longest key an entry may carry: four entries of this size fit on a page,
/// so a split always leaves each half room for its high key.
pub const MAX_KEY_LEN: usize = 2000;
const LEAF_FIXED: usize = 8;
const PIVOT_FIXED: usize = 12;

fn prev(p: &[u8]) -> u32 {
    get_u32(p, SP)
}

fn next(p: &[u8]) -> u32 {
    get_u32(p, SP + 4)
}

fn level(p: &[u8]) -> u32 {
    get_u32(p, SP + 8)
}

fn bflags(p: &[u8]) -> u16 {
    get_u16(p, SP + 12)
}

fn set_prev(p: &mut [u8], v: u32) {
    set_u32(p, SP, v)
}

fn set_next(p: &mut [u8], v: u32) {
    set_u32(p, SP + 4, v)
}

fn set_level(p: &mut [u8], v: u32) {
    set_u32(p, SP + 8, v)
}

fn set_bflags(p: &mut [u8], v: u16) {
    set_u16(p, SP + 12, v)
}

fn is_leaf(p: &[u8]) -> bool {
    level(p) == 0
}

fn is_rightmost(p: &[u8]) -> bool {
    next(p) == 0
}

fn first_data(p: &[u8]) -> u16 {
    if is_rightmost(p) { 1 } else { 2 }
}

fn init_page(p: &mut [u8], lvl: u32, flags: u16) {
    page::init(p, page::KIND_BTREE, SPECIAL);
    set_level(p, lvl);
    set_bflags(p, flags);
}

fn leaf_item(key: &[u8], tid: Tid) -> Vec<u8> {
    let mut v = Vec::with_capacity(LEAF_FIXED + key.len());
    v.extend_from_slice(&tid.block.to_le_bytes());
    v.extend_from_slice(&tid.off.to_le_bytes());
    v.extend_from_slice(&(key.len() as u16).to_le_bytes());
    v.extend_from_slice(key);
    v
}

fn pivot_item(child: u32, key: &[u8], tid: Tid) -> Vec<u8> {
    let mut v = Vec::with_capacity(PIVOT_FIXED + key.len());
    v.extend_from_slice(&child.to_le_bytes());
    v.extend_from_slice(&leaf_item(key, tid));
    v
}

fn parse_leaf(it: &[u8]) -> (&[u8], Tid) {
    if it.len() < LEAF_FIXED {
        return (&[], Tid::MIN);
    }
    let tid = Tid {
        block: get_u32(it, 0),
        off: get_u16(it, 4),
    };
    let n = get_u16(it, 6) as usize;
    (it.get(LEAF_FIXED..LEAF_FIXED + n).unwrap_or(&[]), tid)
}

fn pivot_child(it: &[u8]) -> u32 {
    if it.len() < PIVOT_FIXED {
        return 0;
    }
    get_u32(it, 0)
}

/// Sort key of item `off`: the separator of an internal item, the entry
/// itself on a leaf.
fn item_sk(p: &[u8], off: u16) -> (&[u8], Tid) {
    let it = page::item(p, off);
    if is_leaf(p) {
        parse_leaf(it)
    } else {
        parse_leaf(it.get(4..).unwrap_or(&[]))
    }
}

/// A search key: a (key, tid) position, or past every entry.
#[derive(Clone, Copy)]
enum Sk<'a> {
    At(&'a [u8], Tid),
    PlusInf,
}

fn cmp_sk(sk: Sk<'_>, key: &[u8], tid: Tid) -> Ordering {
    match sk {
        Sk::At(k, t) => k.cmp(key).then(t.cmp(&tid)),
        Sk::PlusInf => Ordering::Greater,
    }
}

/// Should a search for `sk` move right from this page?
fn beyond_high_key(p: &[u8], sk: Sk<'_>) -> bool {
    if is_rightmost(p) {
        return false;
    }
    let (k, t) = item_sk(p, 1);
    cmp_sk(sk, k, t) != Ordering::Less
}

/// First data offset of a leaf whose entry is at or after `sk`.
fn lower_bound(p: &[u8], sk: Sk<'_>) -> u16 {
    let (mut lo, mut hi) = (first_data(p), page::max_offset(p) + 1);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (k, t) = item_sk(p, mid);
        if cmp_sk(sk, k, t) == Ordering::Greater {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Child of an internal page to descend for `sk`: the last data item whose
/// separator is at or below it (the first data item is minus infinity).
fn find_child(p: &[u8], sk: Sk<'_>) -> u32 {
    let fd = first_data(p);
    let (mut lo, mut hi) = (fd + 1, page::max_offset(p) + 1);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (k, t) = item_sk(p, mid);
        if cmp_sk(sk, k, t) != Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    pivot_child(page::item(p, lo - 1))
}

/// All entries of one key found by a scan.
pub struct Group {
    pub key: Vec<u8>,
    pub tids: Vec<Tid>,
}

fn push_entry(out: &mut Vec<Group>, key: &[u8], tid: Tid, max_keys: usize) -> bool {
    match out.last_mut() {
        Some(g) if g.key == key => g.tids.push(tid),
        _ => {
            if out.len() >= max_keys {
                return false;
            }
            out.push(Group {
                key: key.to_vec(),
                tids: vec![tid],
            });
        }
    }
    true
}

fn corrupt(rel: u32, what: &str) -> Error {
    Error::corruption(format!("index relation {rel}: {what}"))
}

/// One B-tree. The root and level are cached from the meta page.
pub struct Index {
    pub rel: u32,
    root: AtomicU32,
    level: AtomicU32,
}

impl Index {
    pub fn new(rel: u32) -> Index {
        Index {
            rel,
            root: AtomicU32::new(0),
            level: AtomicU32::new(0),
        }
    }

    fn tag(&self, block: u32) -> PageTag {
        PageTag {
            rel: self.rel,
            fork: FORK_MAIN,
            block,
        }
    }

    /// Reads the root from the meta page (after open and recovery).
    pub fn load_meta(&self, pool: &BufferPool) -> Result<()> {
        if pool.nblocks(self.rel, FORK_MAIN)? == 0 {
            self.root.store(0, AtOrd::Release);
            return Ok(());
        }
        let b = pool.get(self.tag(0), Mode::Read)?;
        let g = b.read();
        let p = &g[..];
        if page::is_new(p) {
            self.root.store(0, AtOrd::Release);
            return Ok(());
        }
        if get_u32(p, HEADER) != META_MAGIC {
            return Err(corrupt(self.rel, "bad meta page"));
        }
        self.root.store(get_u32(p, HEADER + 8), AtOrd::Release);
        self.level.store(get_u32(p, HEADER + 12), AtOrd::Release);
        Ok(())
    }

    pub fn depth(&self) -> u32 {
        if self.root.load(AtOrd::Acquire) == 0 {
            0
        } else {
            self.level.load(AtOrd::Acquire) + 1
        }
    }

    /// The leaf where `sk` belongs, or `None` for an empty index. With
    /// `stack`, the internal pages passed on the way down are recorded.
    fn find_leaf(
        &self,
        pool: &BufferPool,
        sk: Sk<'_>,
        mut stack: Option<&mut Vec<u32>>,
    ) -> Result<Option<u32>> {
        let mut blk = self.root.load(AtOrd::Acquire);
        if blk == 0 {
            return Ok(None);
        }
        for _ in 0..1_000_000 {
            let b = pool.get(self.tag(blk), Mode::Read)?;
            let g = b.read();
            let p = &g[..];
            if page::kind(p) != page::KIND_BTREE {
                return Err(corrupt(self.rel, "descent reached a non-index page"));
            }
            if beyond_high_key(p, sk) {
                blk = next(p);
                continue;
            }
            if is_leaf(p) {
                return Ok(Some(blk));
            }
            if let Some(s) = stack.as_deref_mut() {
                s.push(blk);
            }
            blk = find_child(p, sk);
            if blk == 0 {
                return Err(corrupt(self.rel, "internal page without a child"));
            }
        }
        Err(corrupt(self.rel, "descent does not end"))
    }

    /// Entries with key at or after `start` (after, when not `inclusive`),
    /// grouped by key in ascending order, up to `max_keys` keys. The flag
    /// is true when the scan reached the end of the index.
    pub fn scan_forward(
        &self,
        pool: &BufferPool,
        start: &[u8],
        inclusive: bool,
        max_keys: usize,
    ) -> Result<(Vec<Group>, bool)> {
        let sk = Sk::At(start, if inclusive { Tid::MIN } else { Tid::MAX });
        let Some(mut blk) = self.find_leaf(pool, sk, None)? else {
            return Ok((Vec::new(), true));
        };
        let mut out = Vec::new();
        let mut first = true;
        loop {
            let b = pool.get(self.tag(blk), Mode::Read)?;
            let g = b.read();
            let p = &g[..];
            if first && beyond_high_key(p, sk) {
                blk = next(p);
                continue;
            }
            let from = if first {
                lower_bound(p, sk)
            } else {
                first_data(p)
            };
            first = false;
            for off in from..=page::max_offset(p) {
                let (k, t) = parse_leaf(page::item(p, off));
                if !push_entry(&mut out, k, t, max_keys) {
                    return Ok((out, false));
                }
            }
            let nx = next(p);
            if nx == 0 {
                return Ok((out, true));
            }
            blk = nx;
        }
    }

    /// Entries with key at or before `start` (before, when not
    /// `inclusive`; `None` is past every key), grouped by key in descending
    /// order, up to `max_keys` keys. The flag is true when the scan reached
    /// the start of the index.
    pub fn scan_backward(
        &self,
        pool: &BufferPool,
        start: Option<&[u8]>,
        inclusive: bool,
        max_keys: usize,
    ) -> Result<(Vec<Group>, bool)> {
        let sk = match start {
            None => Sk::PlusInf,
            Some(s) => Sk::At(s, if inclusive { Tid::MAX } else { Tid::MIN }),
        };
        let Some(mut blk) = self.find_leaf(pool, sk, None)? else {
            return Ok((Vec::new(), true));
        };
        let mut out = Vec::new();
        let mut first = true;
        let mut expect_next: Option<u32> = None;
        loop {
            let b = pool.get(self.tag(blk), Mode::Read)?;
            let g = b.read();
            let p = &g[..];
            if let Some(cur) = expect_next {
                // The left sibling may have split since `cur` named it; move
                // right to the page that links to `cur`.
                if next(p) != cur {
                    blk = next(p);
                    if blk == 0 {
                        return Err(corrupt(self.rel, "broken left link"));
                    }
                    continue;
                }
            } else if first && beyond_high_key(p, sk) {
                blk = next(p);
                continue;
            }
            let end = if first {
                lower_bound(p, sk)
            } else {
                page::max_offset(p) + 1
            };
            first = false;
            for off in (first_data(p)..end).rev() {
                let (k, t) = parse_leaf(page::item(p, off));
                if !push_entry(&mut out, k, t, max_keys) {
                    return Ok((out, false));
                }
            }
            let pv = prev(p);
            if pv == 0 {
                return Ok((out, true));
            }
            expect_next = Some(blk);
            blk = pv;
        }
    }

    /// Every tid stored under `key`.
    pub fn lookup(&self, pool: &BufferPool, key: &[u8]) -> Result<Vec<Tid>> {
        let (groups, _) = self.scan_forward(pool, key, true, 1)?;
        Ok(groups
            .into_iter()
            .next()
            .filter(|g| g.key == key)
            .map(|g| g.tids)
            .unwrap_or_default())
    }

    /// Inserts `(key, tid)`. The caller holds the writer lock, so no other
    /// thread changes the tree while readers may be walking it. A leaf
    /// insert logs the item; a split logs every page it changed as a full
    /// image in one record, so recovery never sees half a split.
    pub fn insert(&self, pool: &BufferPool, key: &[u8], tid: Tid) -> Result<()> {
        if self.root.load(AtOrd::Acquire) == 0 {
            return self.create(pool, key, tid);
        }
        let sk = Sk::At(key, tid);
        let mut stack = Vec::new();
        let leaf = self
            .find_leaf(pool, sk, Some(&mut stack))?
            .ok_or_else(|| corrupt(self.rel, "root vanished"))?;
        let item = leaf_item(key, tid);
        let mut lp = pool.get(self.tag(leaf), Mode::Read)?.lock();
        let pos = lower_bound(lp.page(), sk);
        if page::insert_item_at(lp.page_mut(), &item, pos) {
            let mut main = Vec::with_capacity(2 + item.len());
            main.extend_from_slice(&pos.to_le_bytes());
            main.extend_from_slice(&item);
            let (_, end) = pool.wal().insert(
                RM_BTREE_INSERT,
                0,
                &[RecBlock {
                    tag: lp.tag(),
                    page: lp.page(),
                    force_image: false,
                    standard: true,
                }],
                &main,
            );
            page::set_lsn(lp.page_mut(), end);
            lp.mark_dirty();
            return Ok(());
        }
        let mut changed: Vec<Locked<'_>> = Vec::new();
        let (mut left, mut right, mut sep) = self.split(pool, lp, pos, item, &mut changed)?;
        let new_root = loop {
            let Some(parent) = stack.pop() else {
                break Some(self.new_root(pool, left, right, sep, &mut changed)?);
            };
            let mut pp = pool.get(self.tag(parent), Mode::Read)?.lock();
            let p = pp.page();
            let pos = (first_data(p)..=page::max_offset(p))
                .find(|&o| pivot_child(page::item(p, o)) == left)
                .ok_or_else(|| corrupt(self.rel, "parent lost a downlink"))?
                + 1;
            if page::insert_item_at(pp.page_mut(), &sep, pos) {
                changed.push(pp);
                break None;
            }
            (left, right, sep) = self.split(pool, pp, pos, sep, &mut changed)?;
        };
        let blocks: Vec<RecBlock<'_>> = changed
            .iter()
            .map(|l| RecBlock {
                tag: l.tag(),
                page: l.page(),
                force_image: true,
                standard: true,
            })
            .collect();
        let rm = if new_root.is_some() {
            RM_BTREE_NEWROOT
        } else {
            RM_BTREE_SPLIT
        };
        let (_, end) = pool.wal().insert(rm, 0, &blocks, &[]);
        drop(blocks);
        for l in changed.iter_mut() {
            page::set_lsn(l.page_mut(), end);
            l.mark_dirty();
        }
        drop(changed);
        if let Some((root, lvl)) = new_root {
            self.level.store(lvl, AtOrd::Release);
            self.root.store(root, AtOrd::Release);
        }
        Ok(())
    }

    /// First insert: the meta page (block 0) and a root leaf (block 1).
    fn create(&self, pool: &BufferPool, key: &[u8], tid: Tid) -> Result<()> {
        let m = pool.extend(self.rel, FORK_MAIN)?;
        let r = pool.extend(self.rel, FORK_MAIN)?;
        if m != 0 {
            return Err(corrupt(self.rel, "meta page is not block 0"));
        }
        let mut mp = pool.get(self.tag(m), Mode::Zero)?.lock();
        let mut rp = pool.get(self.tag(r), Mode::Zero)?.lock();
        write_meta(mp.page_mut(), r, 0);
        init_page(rp.page_mut(), 0, BTP_ROOT);
        if !page::insert_item_at(rp.page_mut(), &leaf_item(key, tid), 1) {
            return Err(Error::invalid("index entry does not fit on a page"));
        }
        let (_, end) = pool.wal().insert(
            RM_BTREE_NEWROOT,
            0,
            &[
                RecBlock {
                    tag: mp.tag(),
                    page: mp.page(),
                    force_image: true,
                    standard: true,
                },
                RecBlock {
                    tag: rp.tag(),
                    page: rp.page(),
                    force_image: true,
                    standard: true,
                },
            ],
            &[],
        );
        for l in [&mut mp, &mut rp] {
            page::set_lsn(l.page_mut(), end);
            l.mark_dirty();
        }
        drop(rp);
        drop(mp);
        self.level.store(0, AtOrd::Release);
        self.root.store(r, AtOrd::Release);
        Ok(())
    }

    /// Splits the full page `lp` while inserting `new_item` at `pos`. The
    /// upper part moves to a new right sibling; the left page's new high key
    /// is the right page's first key. Returns the left and right blocks and
    /// the separator (an internal item pointing at the right page).
    fn split<'a>(
        &self,
        pool: &'a BufferPool,
        mut lp: Locked<'a>,
        pos: u16,
        new_item: Vec<u8>,
        changed: &mut Vec<Locked<'a>>,
    ) -> Result<(u32, u32, Vec<u8>)> {
        let lblk = lp.tag().block;
        let p = lp.page();
        let leaf = is_leaf(p);
        let fd = first_data(p);
        let max = page::max_offset(p);
        let mut items: Vec<Vec<u8>> = (fd..=max).map(|o| page::item(p, o).to_vec()).collect();
        let ins = (pos - fd) as usize;
        items.insert(ins, new_item);
        let old_hk = (!is_rightmost(p)).then(|| page::item(p, 1).to_vec());
        let old_next = next(p);
        let old_prev = prev(p);
        let lvl = level(p);
        let flags = bflags(p) & !BTP_ROOT;
        let k = choose_split(
            &items,
            ins,
            old_hk.as_ref().map_or(0, Vec::len),
            old_next == 0,
        );

        let rblk = pool.extend(self.rel, FORK_MAIN)?;
        let mut rp = pool.get(self.tag(rblk), Mode::Zero)?.lock();
        init_page(rp.page_mut(), lvl, flags);
        set_prev(rp.page_mut(), lblk);
        set_next(rp.page_mut(), old_next);
        let mut right: Vec<&[u8]> = Vec::with_capacity(items.len() - k + 1);
        if let Some(h) = &old_hk {
            right.push(h);
        }
        right.extend(items[k..].iter().map(Vec::as_slice));
        if !page::set_index_items(rp.page_mut(), &right) {
            return Err(corrupt(self.rel, "right half of a split does not fit"));
        }

        // The separator and the left page's new high key both come from the
        // right page's first data item.
        let (sep_key, sep_tid) = if leaf {
            parse_leaf(&items[k])
        } else {
            parse_leaf(&items[k][4..])
        };
        let sep = pivot_item(rblk, sep_key, sep_tid);
        let hk = if leaf {
            leaf_item(sep_key, sep_tid)
        } else {
            pivot_item(0, sep_key, sep_tid)
        };
        let mut left: Vec<&[u8]> = Vec::with_capacity(k + 1);
        left.push(&hk);
        left.extend(items[..k].iter().map(Vec::as_slice));
        let lpage = lp.page_mut();
        init_page(lpage, lvl, flags);
        set_prev(lpage, old_prev);
        set_next(lpage, rblk);
        if !page::set_index_items(lpage, &left) {
            return Err(corrupt(self.rel, "left half of a split does not fit"));
        }
        if old_next != 0 {
            let mut sp = pool.get(self.tag(old_next), Mode::Read)?.lock();
            set_prev(sp.page_mut(), rblk);
            changed.push(sp);
        }
        changed.push(lp);
        changed.push(rp);
        Ok((lblk, rblk, sep))
    }

    /// Adds a root above `left` and the page `sep` points at, after the old
    /// root split. Returns the new root and level.
    fn new_root<'a>(
        &self,
        pool: &'a BufferPool,
        left: u32,
        _right: u32,
        sep: Vec<u8>,
        changed: &mut Vec<Locked<'a>>,
    ) -> Result<(u32, u32)> {
        let lvl = self.level.load(AtOrd::Acquire) + 1;
        let rblk = pool.extend(self.rel, FORK_MAIN)?;
        let mut rp = pool.get(self.tag(rblk), Mode::Zero)?.lock();
        init_page(rp.page_mut(), lvl, BTP_ROOT);
        let minus_inf = pivot_item(left, &[], Tid::MIN);
        if !page::set_index_items(rp.page_mut(), &[&minus_inf, &sep]) {
            return Err(corrupt(self.rel, "new root does not fit"));
        }
        let mut mp = pool.get(self.tag(0), Mode::Read)?.lock();
        write_meta(mp.page_mut(), rblk, lvl);
        changed.push(rp);
        changed.push(mp);
        Ok((rblk, lvl))
    }

    /// Removes every leaf entry whose tid is in `dead` (VACUUM's index
    /// pass), one logged record per changed leaf. Returns the entries
    /// removed.
    pub fn bulk_delete(&self, pool: &BufferPool, dead: &HashSet<Tid>) -> Result<u64> {
        if dead.is_empty() {
            return Ok(0);
        }
        let Some(mut blk) = self.find_leaf(pool, Sk::At(&[], Tid::MIN), None)? else {
            return Ok(0);
        };
        let mut removed = 0u64;
        loop {
            let mut lp = pool.get(self.tag(blk), Mode::Read)?.lock();
            let p = lp.page();
            let offs: Vec<u16> = (first_data(p)..=page::max_offset(p))
                .filter(|&o| dead.contains(&parse_leaf(page::item(p, o)).1))
                .collect();
            let nx = next(p);
            if !offs.is_empty() {
                page::delete_index_items(lp.page_mut(), &offs);
                let mut main = Vec::with_capacity(2 + 2 * offs.len());
                main.extend_from_slice(&(offs.len() as u16).to_le_bytes());
                for o in &offs {
                    main.extend_from_slice(&o.to_le_bytes());
                }
                let (_, end) = pool.wal().insert(
                    RM_BTREE_VACUUM,
                    0,
                    &[RecBlock {
                        tag: lp.tag(),
                        page: lp.page(),
                        force_image: false,
                        standard: true,
                    }],
                    &main,
                );
                page::set_lsn(lp.page_mut(), end);
                lp.mark_dirty();
                removed += offs.len() as u64;
            }
            drop(lp);
            if nx == 0 {
                return Ok(removed);
            }
            blk = nx;
        }
    }

    /// Entries in the leaves (for statistics and tests).
    pub fn count_entries(&self, pool: &BufferPool) -> Result<u64> {
        let Some(mut blk) = self.find_leaf(pool, Sk::At(&[], Tid::MIN), None)? else {
            return Ok(0);
        };
        let mut n = 0u64;
        loop {
            let b = pool.get(self.tag(blk), Mode::Read)?;
            let g = b.read();
            let p = &g[..];
            n += (page::max_offset(p) + 1 - first_data(p)) as u64;
            let nx = next(p);
            if nx == 0 {
                return Ok(n);
            }
            blk = nx;
        }
    }
}

fn write_meta(p: &mut [u8], root: u32, lvl: u32) {
    if page::kind(p) != page::KIND_BTREE_META {
        page::init_raw(p, page::KIND_BTREE_META);
    }
    set_u32(p, HEADER, META_MAGIC);
    set_u32(p, HEADER + 4, META_VERSION);
    set_u32(p, HEADER + 8, root);
    set_u32(p, HEADER + 12, lvl);
}

/// Picks how many items stay on the left. Both halves must fit, the left
/// with its new high key (a copy of the first right item). A split caused by
/// an insert at the end of the rightmost page leaves the left page 90% full,
/// as PostgreSQL does for rightmost leaf splits; any other split balances
/// the bytes.
fn choose_split(items: &[Vec<u8>], ins: usize, old_hk: usize, rightmost: bool) -> usize {
    let cap = BLCKSZ - HEADER - SPECIAL;
    let sizes: Vec<usize> = items.iter().map(|i| i.len() + ITEM_ID).collect();
    let total: usize = sizes.iter().sum();
    let pct = if rightmost && ins + 1 == items.len() {
        90
    } else {
        50
    };
    let target = total * pct / 100;
    let hk_extra = if old_hk > 0 { old_hk + ITEM_ID } else { 0 };
    let mut best = (usize::MAX, items.len() / 2);
    let mut left = 0;
    for k in 1..items.len() {
        left += sizes[k - 1];
        let right = total - left + hk_extra;
        if left + sizes[k] > cap || right > cap {
            continue;
        }
        let score = left.abs_diff(target);
        if score < best.0 {
            best = (score, k);
        }
    }
    best.1.clamp(1, items.len() - 1)
}

/// Redo of a leaf insert: the item at its logged position.
pub fn redo_insert(p: &mut [u8], main: &[u8]) -> Result<()> {
    if main.len() < 2 {
        return Err(Error::corruption("short B-tree insert record"));
    }
    let pos = get_u16(main, 0);
    if !page::insert_item_at(p, &main[2..], pos) {
        return Err(Error::corruption("B-tree insert does not replay"));
    }
    Ok(())
}

/// Redo of VACUUM's entry removal.
pub fn redo_vacuum(p: &mut [u8], main: &[u8]) -> Result<()> {
    if main.len() < 2 {
        return Err(Error::corruption("short B-tree vacuum record"));
    }
    let n = get_u16(main, 0) as usize;
    if main.len() < 2 + 2 * n {
        return Err(Error::corruption("short B-tree vacuum record"));
    }
    let offs: Vec<u16> = (0..n).map(|i| get_u16(main, 2 + 2 * i)).collect();
    page::delete_index_items(p, &offs);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_point_respects_capacity_and_rightmost_packing() {
        // A full page of 78 items of 100 bytes plus the one that did not fit.
        let items: Vec<Vec<u8>> = (0..79).map(|i| vec![i as u8; 100]).collect();
        let k = choose_split(&items, 78, 0, true);
        assert!(
            (69..=73).contains(&k),
            "rightmost append packs the left page to 90%, got {k}"
        );
        let k = choose_split(&items, 40, 0, false);
        assert!((38..=41).contains(&k), "middle insert balances, got {k}");
        let big: Vec<Vec<u8>> = (0..4)
            .map(|_| vec![0u8; MAX_KEY_LEN + PIVOT_FIXED])
            .collect();
        let k = choose_split(&big, 1, MAX_KEY_LEN + PIVOT_FIXED, false);
        assert!((1..4).contains(&k));
    }
}
