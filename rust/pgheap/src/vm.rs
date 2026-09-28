//! The visibility map: one bit per heap page, set when every tuple on the
//! page is visible to every snapshot, 65,280 heap pages per map page in the
//! relation's `_vm` fork. VACUUM skips pages whose bit is set. A heap page
//! with its bit set also carries PD_ALL_VISIBLE, so a writer changing the
//! page sees the flag and clears both.
//!
//! PostgreSQL WAL-logs setting and clearing the bit. Here the map is not
//! logged; instead open empties every visibility map after a crash, and the
//! next VACUUM scans every page and rebuilds it. The bits are only used to
//! skip work, so an empty map costs time, never correctness.

use nilengine_api::Result;

use crate::bufmgr::{BufferPool, FORK_VM, Mode};
use crate::page::{self, BLCKSZ, HEADER};
use crate::wal::PageTag;

const BITS: u32 = ((BLCKSZ - HEADER) * 8) as u32;

fn at(block: u32) -> (u32, usize, u8) {
    let i = block % BITS;
    (block / BITS, HEADER + (i / 8) as usize, 1 << (i % 8))
}

fn tag(rel: u32, block: u32) -> PageTag {
    PageTag {
        rel,
        fork: FORK_VM,
        block,
    }
}

pub fn is_set(pool: &BufferPool, rel: u32, block: u32) -> Result<bool> {
    let (vb, byte, bit) = at(block);
    if vb >= pool.nblocks(rel, FORK_VM)? {
        return Ok(false);
    }
    let b = pool.get(tag(rel, vb), Mode::Read)?;
    let g = b.read();
    Ok(!page::is_new(&g[..]) && g[byte] & bit != 0)
}

pub fn set(pool: &BufferPool, rel: u32, block: u32, on: bool) -> Result<()> {
    let (vb, byte, bit) = at(block);
    if !on && vb >= pool.nblocks(rel, FORK_VM)? {
        return Ok(());
    }
    pool.ensure_nblocks(rel, FORK_VM, vb + 1)?;
    let mut l = pool.get(tag(rel, vb), Mode::Read)?.lock();
    if page::is_new(l.page()) {
        if !on {
            return Ok(());
        }
        page::init_raw(l.page_mut(), page::KIND_VM);
    }
    let cur = l.page()[byte];
    let new = if on { cur | bit } else { cur & !bit };
    if new != cur {
        l.page_mut()[byte] = new;
        l.mark_dirty();
    }
    Ok(())
}

/// Heap pages marked all-visible (statistics).
pub fn count(pool: &BufferPool, rel: u32) -> Result<u64> {
    let mut n = 0u64;
    for vb in 0..pool.nblocks(rel, FORK_VM)? {
        let b = pool.get(tag(rel, vb), Mode::Read)?;
        let g = b.read();
        if !page::is_new(&g[..]) {
            n += g[HEADER..]
                .iter()
                .map(|b| b.count_ones() as u64)
                .sum::<u64>();
        }
    }
    Ok(n)
}
