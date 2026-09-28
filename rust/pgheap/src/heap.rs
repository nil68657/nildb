//! Heap tuples and heap pages, after PostgreSQL's heapam (htup_details.h,
//! src/backend/access/heap/README.HOT).
//!
//! Tuple layout, little-endian: xmin u64, xmax u64, t_ctid (block u32,
//! offset u16), t_infomask2 u16, t_infomask u16, key length u16, value
//! length u32, then the key and the value. With HEAP_HASEXTERNAL the value
//! is a 16-byte TOAST pointer instead. PostgreSQL's header is 23 bytes with
//! 32-bit xids; this one is 32.
//!
//! A version chain links each version to its successor through t_ctid. A
//! HOT chain is the part of it that stays on one page: the old version
//! carries HEAP_HOT_UPDATED and the new one HEAP_ONLY_TUPLE, and only the
//! chain's root has an index entry.

use nilengine_api::coding::{get_u16, get_u32, get_u64, set_u16, set_u32, set_u64};
use nilengine_api::{Error, Result};

use crate::bufmgr::{Buf, BufferPool, FORK_MAIN, Mode};
use crate::page::{self, BLCKSZ, HEADER, ITEM_ID, LP_NORMAL, LP_REDIRECT, PD_ALL_VISIBLE};
use crate::wal::PageTag;
use crate::xact::{Hint, Htsv, View, VisCtx};

pub const TUPLE_HEADER: usize = 32;
/// Line pointers a heap page can hold (PostgreSQL's MaxHeapTuplesPerPage).
pub const MAX_HEAP_TUPLES: usize = (BLCKSZ - HEADER) / (TUPLE_HEADER + ITEM_ID);

const T_XMIN: usize = 0;
const T_XMAX: usize = 8;
const T_CTID_BLOCK: usize = 16;
const T_CTID_OFF: usize = 20;
const T_INFOMASK2: usize = 22;
const T_INFOMASK: usize = 24;
const T_KEYLEN: usize = 26;
const T_VALLEN: usize = 28;

// t_infomask2
pub const HEAP_HOT_UPDATED: u16 = 0x4000;
pub const HEAP_ONLY_TUPLE: u16 = 0x8000;
// t_infomask
pub const HEAP_HASEXTERNAL: u16 = 0x0004;
pub const HEAP_XMIN_COMMITTED: u16 = 0x0100;
pub const HEAP_XMIN_INVALID: u16 = 0x0200;
pub const HEAP_XMAX_COMMITTED: u16 = 0x0400;
pub const HEAP_XMAX_INVALID: u16 = 0x0800;
pub const HEAP_UPDATED: u16 = 0x2000;

/// A tuple id: block and line pointer offset.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Tid {
    pub block: u32,
    pub off: u16,
}

impl Tid {
    pub const MIN: Tid = Tid { block: 0, off: 0 };
    pub const MAX: Tid = Tid {
        block: u32::MAX,
        off: u16::MAX,
    };
}

pub fn xmin(t: &[u8]) -> u64 {
    get_u64(t, T_XMIN)
}

pub fn xmax(t: &[u8]) -> u64 {
    get_u64(t, T_XMAX)
}

pub fn ctid(t: &[u8]) -> Tid {
    Tid {
        block: get_u32(t, T_CTID_BLOCK),
        off: get_u16(t, T_CTID_OFF),
    }
}

fn set_ctid(t: &mut [u8], tid: Tid) {
    set_u32(t, T_CTID_BLOCK, tid.block);
    set_u16(t, T_CTID_OFF, tid.off);
}

pub fn infomask(t: &[u8]) -> u16 {
    get_u16(t, T_INFOMASK)
}

fn set_infomask(t: &mut [u8], v: u16) {
    set_u16(t, T_INFOMASK, v)
}

pub fn infomask2(t: &[u8]) -> u16 {
    get_u16(t, T_INFOMASK2)
}

fn set_infomask2(t: &mut [u8], v: u16) {
    set_u16(t, T_INFOMASK2, v)
}

pub fn is_external(t: &[u8]) -> bool {
    infomask(t) & HEAP_HASEXTERNAL != 0
}

pub fn key(t: &[u8]) -> &[u8] {
    let k = get_u16(t, T_KEYLEN) as usize;
    t.get(TUPLE_HEADER..TUPLE_HEADER + k).unwrap_or(&[])
}

/// The value bytes stored in the tuple: the value itself, or the TOAST
/// pointer when `is_external`.
pub fn value(t: &[u8]) -> &[u8] {
    let k = get_u16(t, T_KEYLEN) as usize;
    let v = get_u32(t, T_VALLEN) as usize;
    t.get(TUPLE_HEADER + k..TUPLE_HEADER + k + v).unwrap_or(&[])
}

/// Checks that a tuple's lengths match its item length.
pub fn check(t: &[u8]) -> Result<()> {
    if t.len() < TUPLE_HEADER
        || TUPLE_HEADER + get_u16(t, T_KEYLEN) as usize + get_u32(t, T_VALLEN) as usize != t.len()
    {
        return Err(Error::corruption(format!(
            "heap tuple of {} bytes has inconsistent lengths",
            t.len()
        )));
    }
    Ok(())
}

/// Builds a new tuple inserted by `xmin`; `flags2` goes into t_infomask2.
/// t_ctid is filled in when the tuple is placed.
pub fn build(
    xmin: u64,
    key: &[u8],
    value: &[u8],
    external: bool,
    flags2: u16,
    updated: bool,
) -> Vec<u8> {
    let mut t = vec![0u8; TUPLE_HEADER + key.len() + value.len()];
    set_u64(&mut t, T_XMIN, xmin);
    let mut mask = HEAP_XMAX_INVALID;
    if external {
        mask |= HEAP_HASEXTERNAL;
    }
    if updated {
        mask |= HEAP_UPDATED;
    }
    set_infomask(&mut t, mask);
    set_infomask2(&mut t, flags2);
    set_u16(&mut t, T_KEYLEN, key.len() as u16);
    set_u32(&mut t, T_VALLEN, value.len() as u32);
    t[TUPLE_HEADER..TUPLE_HEADER + key.len()].copy_from_slice(key);
    t[TUPLE_HEADER + key.len()..].copy_from_slice(value);
    t
}

/// Marks a new version as a heap-only tuple (reached through a HOT chain,
/// with no index entry of its own).
pub fn set_heap_only(t: &mut [u8]) {
    set_infomask2(t, infomask2(t) | HEAP_ONLY_TUPLE);
}

pub fn init_page(p: &mut [u8]) {
    page::init(p, page::KIND_HEAP, 0);
}

fn clear_all_visible(p: &mut [u8]) {
    let f = page::flags(p);
    if f & PD_ALL_VISIBLE != 0 {
        page::set_flags(p, f & !PD_ALL_VISIBLE);
    }
}

/// Remembers that the page may hold a prunable tuple once `xid` is old
/// enough (PostgreSQL's PageSetPrunable).
fn set_prunable(p: &mut [u8], xid: u64) {
    let cur = page::prune_xid(p);
    if cur == 0 || xid < cur {
        page::set_prune_xid(p, xid);
    }
}

/// Places a tuple on a page (at `at` during redo) and points its t_ctid at
/// itself. Returns the offset, or `None` when it does not fit.
pub fn place(p: &mut [u8], block: u32, t: &[u8], at: Option<u16>) -> Option<u16> {
    let off = page::add_item(p, t, at, MAX_HEAP_TUPLES)?;
    set_ctid(page::item_mut(p, off), Tid { block, off });
    clear_all_visible(p);
    Some(off)
}

/// Marks a version deleted by `xid` (PostgreSQL's heap_delete page change).
pub fn mark_deleted(p: &mut [u8], block: u32, off: u16, xid: u64) {
    let t = page::item_mut(p, off);
    set_u64(t, T_XMAX, xid);
    set_infomask(t, infomask(t) & !(HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID));
    set_infomask2(t, infomask2(t) & !HEAP_HOT_UPDATED);
    set_ctid(t, Tid { block, off });
    set_prunable(p, xid);
    clear_all_visible(p);
}

/// Marks a version replaced by the version at `new` (PostgreSQL's
/// heap_update change to the old tuple); `hot` when `new` is a heap-only
/// tuple on the same page.
pub fn mark_updated(p: &mut [u8], off: u16, xid: u64, new: Tid, hot: bool) {
    let t = page::item_mut(p, off);
    set_u64(t, T_XMAX, xid);
    set_infomask(t, infomask(t) & !(HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID));
    let m2 = infomask2(t);
    set_infomask2(
        t,
        if hot {
            m2 | HEAP_HOT_UPDATED
        } else {
            m2 & !HEAP_HOT_UPDATED
        },
    );
    set_ctid(t, new);
    set_prunable(p, xid);
    clear_all_visible(p);
}

/// Line pointer changes made by one prune (PostgreSQL's PruneState lists).
#[derive(Default, Debug)]
pub struct PruneChanges {
    pub redirected: Vec<(u16, u16)>,
    pub dead: Vec<u16>,
    pub unused: Vec<u16>,
    pub prune_xid: u64,
}

impl PruneChanges {
    pub fn is_empty(&self) -> bool {
        self.redirected.is_empty() && self.dead.is_empty() && self.unused.is_empty()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(
            14 + 4 * self.redirected.len() + 2 * (self.dead.len() + self.unused.len()),
        );
        b.extend_from_slice(&(self.redirected.len() as u16).to_le_bytes());
        b.extend_from_slice(&(self.dead.len() as u16).to_le_bytes());
        b.extend_from_slice(&(self.unused.len() as u16).to_le_bytes());
        b.extend_from_slice(&self.prune_xid.to_le_bytes());
        for (f, t) in &self.redirected {
            b.extend_from_slice(&f.to_le_bytes());
            b.extend_from_slice(&t.to_le_bytes());
        }
        for o in self.dead.iter().chain(&self.unused) {
            b.extend_from_slice(&o.to_le_bytes());
        }
        b
    }

    pub fn decode(b: &[u8]) -> Result<PruneChanges> {
        let bad = || Error::corruption("malformed prune record");
        if b.len() < 14 {
            return Err(bad());
        }
        let (nr, nd, nu) = (
            get_u16(b, 0) as usize,
            get_u16(b, 2) as usize,
            get_u16(b, 4) as usize,
        );
        if b.len() != 14 + 4 * nr + 2 * (nd + nu) {
            return Err(bad());
        }
        let mut pos = 14;
        let mut rd = |n: usize| -> Vec<u16> {
            let v = (0..n).map(|i| get_u16(b, pos + 2 * i)).collect();
            pos += 2 * n;
            v
        };
        let pairs = rd(2 * nr);
        let dead = rd(nd);
        let unused = rd(nu);
        Ok(PruneChanges {
            redirected: pairs.chunks(2).map(|c| (c[0], c[1])).collect(),
            dead,
            unused,
            prune_xid: get_u64(b, 6),
        })
    }
}

/// Decides what pruning can remove from a page (PostgreSQL's
/// heap_page_prune and heap_prune_chain). For each HOT chain, the members
/// that are dead to every snapshot and precede every surviving member go:
/// heap-only members become unused, and the root line pointer becomes a
/// redirect to the first survivor, or dead when nothing survives (an index
/// entry still points at it until VACUUM). A dead heap-only tuple that no
/// longer chains anywhere, such as the new version of an aborted update,
/// becomes unused. Returns the changes and the number of tuples whose
/// storage they free.
pub fn plan_prune(
    p: &[u8],
    block: u32,
    vis: &VisCtx<'_>,
    horizon: u64,
    own: Option<u64>,
) -> Result<(PruneChanges, u64)> {
    let max = page::max_offset(p);
    let mut marked = vec![false; max as usize + 1];
    let mut ch = PruneChanges::default();
    let mut removed = 0u64;
    let note_xmax = |ch: &mut PruneChanges, x: u64| {
        if ch.prune_xid == 0 || x < ch.prune_xid {
            ch.prune_xid = x;
        }
    };
    for root in 1..=max {
        if marked[root as usize] {
            continue;
        }
        let id = page::item_id(p, root);
        let redirect = match id.flags {
            LP_NORMAL => false,
            LP_REDIRECT => true,
            _ => continue,
        };
        if !redirect {
            let t = page::item(p, root);
            check(t)?;
            if infomask2(t) & HEAP_ONLY_TUPLE != 0 {
                // Reached from its chain's root, unless it chains nowhere.
                if vis.vacuum_status(t, horizon, own)? == Htsv::Dead
                    && infomask2(t) & HEAP_HOT_UPDATED == 0
                {
                    ch.unused.push(root);
                    marked[root as usize] = true;
                    removed += 1;
                }
                continue;
            }
        }
        // chain[0] is the root line pointer; the rest are tuples.
        let mut chain: Vec<u16> = vec![root];
        let mut latest_dead: Option<usize> = None;
        let mut off = if redirect { id.off } else { root };
        let mut prior_xmax: Option<u64> = None;
        loop {
            if off == 0 || off > max || (off != root && marked[off as usize]) {
                break;
            }
            let oid = page::item_id(p, off);
            if oid.flags != LP_NORMAL {
                break;
            }
            let t = page::item(p, off);
            check(t)?;
            if prior_xmax.is_some_and(|x| xmin(t) != x) {
                break;
            }
            if off != root && infomask2(t) & HEAP_ONLY_TUPLE == 0 {
                break;
            }
            if off != root {
                chain.push(off);
            }
            let st = vis.vacuum_status(t, horizon, own)?;
            match st {
                Htsv::Dead => latest_dead = Some(chain.len() - 1),
                Htsv::RecentlyDead | Htsv::DeleteInProgress => note_xmax(&mut ch, xmax(t)),
                Htsv::Live | Htsv::InsertInProgress => {}
            }
            if !matches!(st, Htsv::Dead | Htsv::RecentlyDead) {
                break;
            }
            if infomask2(t) & HEAP_HOT_UPDATED == 0 {
                break;
            }
            let next = ctid(t);
            if next.block != block {
                break;
            }
            prior_xmax = Some(xmax(t));
            off = next.off;
        }
        for &o in &chain {
            marked[o as usize] = true;
        }
        match latest_dead {
            Some(ld) => {
                for &o in &chain[1..=ld] {
                    ch.unused.push(o);
                    removed += 1;
                }
                if !redirect {
                    // The root tuple itself is dead and its storage goes.
                    removed += 1;
                }
                if ld + 1 == chain.len() {
                    ch.dead.push(root);
                } else {
                    ch.redirected.push((root, chain[ld + 1]));
                }
            }
            None if redirect && chain.len() < 2 => ch.dead.push(root),
            None => {}
        }
    }
    Ok((ch, removed))
}

/// Applies a prune's line pointer changes and repacks the page
/// (PostgreSQL's heap_page_prune_execute).
pub fn apply_prune(p: &mut [u8], c: &PruneChanges) {
    for &(from, to) in &c.redirected {
        page::set_item_id(
            p,
            from,
            page::ItemId {
                off: to,
                flags: LP_REDIRECT,
                len: 0,
            },
        );
    }
    for &o in &c.dead {
        page::set_item_id(
            p,
            o,
            page::ItemId {
                off: 0,
                flags: page::LP_DEAD,
                len: 0,
            },
        );
    }
    for &o in &c.unused {
        page::set_item_id(
            p,
            o,
            page::ItemId {
                off: 0,
                flags: page::LP_UNUSED,
                len: 0,
            },
        );
    }
    page::repair_fragmentation(p);
    page::set_prune_xid(p, c.prune_xid);
}

/// VACUUM's second heap pass on one page: dead line pointers whose index
/// entries are gone become unused, and trailing unused ones are dropped.
pub fn apply_vacuum(p: &mut [u8], offs: &[u16]) {
    for &o in offs {
        page::set_item_id(
            p,
            o,
            page::ItemId {
                off: 0,
                flags: page::LP_UNUSED,
                len: 0,
            },
        );
    }
    page::truncate_line_pointers(p);
    page::repair_fragmentation(p);
}

/// Sets the hint bits that still apply. Returns whether any bit changed.
pub fn apply_hints(p: &mut [u8], hints: &[Hint]) -> bool {
    let max = page::max_offset(p);
    let mut changed = false;
    for h in hints {
        if h.off == 0 || h.off > max || page::item_id(p, h.off).flags != LP_NORMAL {
            continue;
        }
        let t = page::item_mut(p, h.off);
        if t.len() < TUPLE_HEADER {
            continue;
        }
        let cur = if h.xmax { xmax(t) } else { xmin(t) };
        let mask = infomask(t);
        if cur == h.xid && mask & h.bit == 0 {
            set_infomask(t, mask | h.bit);
            changed = true;
        }
    }
    changed
}

/// Sets hint bits if the page's exclusive lock is free right now; hints
/// are optional, so a busy page goes without. A page not logged since the
/// last checkpoint's redo pointer first gets a full-page image.
pub fn set_hints(pool: &BufferPool, b: &Buf<'_>, hints: &[Hint]) {
    if hints.is_empty() {
        return;
    }
    let Some(mut g) = b.try_write() else {
        return;
    };
    let p = &mut g[..];
    if !apply_hints(p, hints) {
        return;
    }
    if let Some(end) = pool.wal().log_hint_image(b.tag(), p) {
        page::set_lsn(p, end);
    }
    b.mark_dirty();
}

/// A version found by a chain walk: where it lives and a copy of it.
pub struct Found {
    pub tid: Tid,
    pub tuple: Vec<u8>,
}

/// Walks the HOT chain rooted at `root` and returns the first member
/// visible in `view` (PostgreSQL's heap_hot_search_buffer). A redirect line
/// pointer may start the chain; each later member must have the previous
/// member's xmax as its xmin, which rejects a line pointer reused after
/// VACUUM. Members must carry `key_want`.
pub fn fetch_chain(
    pool: &BufferPool,
    rel: u32,
    root: Tid,
    key_want: &[u8],
    vis: &VisCtx<'_>,
    view: &View<'_>,
) -> Result<Option<Found>> {
    if root.block >= pool.nblocks(rel, FORK_MAIN)? {
        return Ok(None);
    }
    let tag = PageTag {
        rel,
        fork: FORK_MAIN,
        block: root.block,
    };
    let b = pool.get(tag, Mode::Read)?;
    let mut hints = Vec::new();
    let found = {
        let g = b.read();
        walk_chain(&g[..], root, key_want, vis, view, &mut hints)?
    };
    set_hints(pool, &b, &hints);
    Ok(found)
}

fn walk_chain(
    p: &[u8],
    root: Tid,
    key_want: &[u8],
    vis: &VisCtx<'_>,
    view: &View<'_>,
    hints: &mut Vec<Hint>,
) -> Result<Option<Found>> {
    if page::is_new(p) {
        return Ok(None);
    }
    let max = page::max_offset(p);
    let mut off = root.off;
    let mut prev_xmax: Option<u64> = None;
    for step in 0..=MAX_HEAP_TUPLES {
        if off == 0 || off > max {
            return Ok(None);
        }
        let id = page::item_id(p, off);
        if id.flags == LP_REDIRECT {
            if step != 0 {
                return Ok(None);
            }
            off = id.off;
            continue;
        }
        if id.flags != LP_NORMAL {
            return Ok(None);
        }
        let t = page::item(p, off);
        check(t)?;
        if prev_xmax.is_some_and(|x| xmin(t) != x) || key(t) != key_want {
            return Ok(None);
        }
        if vis.visible(view, off, t, hints)? {
            return Ok(Some(Found {
                tid: Tid {
                    block: root.block,
                    off,
                },
                tuple: t.to_vec(),
            }));
        }
        if infomask2(t) & HEAP_HOT_UPDATED == 0 {
            return Ok(None);
        }
        let next = ctid(t);
        if next.block != root.block {
            return Ok(None);
        }
        prev_xmax = Some(xmax(t));
        off = next.off;
    }
    Ok(None)
}
