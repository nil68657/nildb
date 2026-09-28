//! The free-space map: one byte per heap page holding its free space in
//! 32-byte steps (PostgreSQL's FSM categories), 8,160 heap pages per map
//! page in the relation's `_fsm` fork.
//!
//! Like PostgreSQL's, the map is a hint and is not WAL-logged. VACUUM and
//! pruning record what they free; an insert that finds a page fuller than
//! the map claimed records the real figure and looks again. PostgreSQL keeps
//! a tree of maximums in each map page; this map is a flat array scanned
//! from a per-relation starting point, which is enough at NilDB's table
//! sizes (a 1 GiB heap has a 16-page map).

use std::sync::atomic::{AtomicU32, Ordering};

use nilengine_api::Result;

use crate::bufmgr::{BufferPool, FORK_FSM, Mode};
use crate::page::{self, BLCKSZ, HEADER};
use crate::wal::PageTag;

const SLOTS: u32 = (BLCKSZ - HEADER) as u32;

fn tag(rel: u32, block: u32) -> PageTag {
    PageTag {
        rel,
        fork: FORK_FSM,
        block,
    }
}

/// Records that heap page `block` has `free` bytes available.
pub fn record(pool: &BufferPool, rel: u32, block: u32, free: usize) -> Result<()> {
    let (fb, slot) = (block / SLOTS, (block % SLOTS) as usize);
    let cat = (free / 32).min(255) as u8;
    if cat == 0 && fb >= pool.nblocks(rel, FORK_FSM)? {
        return Ok(());
    }
    pool.ensure_nblocks(rel, FORK_FSM, fb + 1)?;
    let mut l = pool.get(tag(rel, fb), Mode::Read)?.lock();
    if page::is_new(l.page()) {
        if cat == 0 {
            return Ok(());
        }
        page::init_raw(l.page_mut(), page::KIND_FSM);
    }
    if l.page()[HEADER + slot] != cat {
        l.page_mut()[HEADER + slot] = cat;
        l.mark_dirty();
    }
    Ok(())
}

/// A heap page below `nblocks` that the map says has `bytes` free, searched
/// from `hint` onward and then from the start, skipping `skip`.
pub fn search(
    pool: &BufferPool,
    rel: u32,
    bytes: usize,
    nblocks: u32,
    hint: &AtomicU32,
    skip: Option<u32>,
) -> Result<Option<u32>> {
    let need = bytes.div_ceil(32).clamp(1, 255) as u8;
    let pages = pool.nblocks(rel, FORK_FSM)?;
    let start = hint.load(Ordering::Relaxed).min(nblocks);
    for (lo, hi) in [(start, nblocks), (0, start)] {
        let mut blk = lo;
        while blk < hi {
            let fb = blk / SLOTS;
            if fb >= pages {
                break;
            }
            let b = pool.get(tag(rel, fb), Mode::Read)?;
            let g = b.read();
            let p = &g[..];
            let end = hi.min((fb + 1) * SLOTS);
            if !page::is_new(p) {
                for cand in blk..end {
                    if p[HEADER + (cand % SLOTS) as usize] >= need && Some(cand) != skip {
                        hint.store(cand, Ordering::Relaxed);
                        return Ok(Some(cand));
                    }
                }
            }
            blk = end;
        }
    }
    Ok(None)
}
