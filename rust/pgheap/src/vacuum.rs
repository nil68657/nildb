//! VACUUM and autovacuum, after PostgreSQL's lazy vacuum (docs:
//! routine-vacuuming.html; vacuumlazy.c).
//!
//! VACUUM of one heap makes three passes. The first prunes every page the
//! visibility map does not mark all-visible and collects the dead line
//! pointers pruning leaves behind. The second removes the index entries
//! that point at them. The third marks those line pointers unused, records
//! each page's free space in the free-space map and sets the visibility-map
//! bit of pages left all-visible. A version is dead once the transaction
//! that deleted it committed before the oldest registered snapshot's xmin,
//! so one long-held snapshot keeps every later dead version (bloat).
//!
//! VACUUM takes the writer lock for at most 64 heap pages at a time, and for
//! the whole index pass, so writers wait for one chunk rather than the whole
//! run. Autovacuum starts VACUUM on a heap once its dead tuples exceed
//! `autovacuum_threshold + autovacuum_scale_percent% * live tuples`.

use std::collections::HashSet;
use std::sync::MutexGuard;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use nilengine_api::{Error, Result};

use crate::btree::Index;
use crate::bufmgr::{FORK_MAIN, Locked, Mode};
use crate::db::{HeapRel, Inner, rec_block};
use crate::fsm;
use crate::heap::{self, MAX_HEAP_TUPLES, Tid};
use crate::page::{self, LP_DEAD, LP_NORMAL, PD_ALL_VISIBLE};
use crate::vm;
use crate::wal::{RM_HEAP_PRUNE, RM_HEAP_VACUUM};
use crate::xact::{Htsv, VisCtx};

const CHUNK: usize = 64;
/// Dead line pointers collected before an index pass (PostgreSQL bounds
/// them with maintenance_work_mem).
const MAX_DEAD: usize = 1 << 20;

/// What one VACUUM of one heap found and did.
#[derive(Default, Debug, Clone, Copy)]
pub struct VacuumStats {
    pub pages_scanned: u64,
    pub pages_skipped: u64,
    /// Tuples whose storage pruning freed.
    pub tuples_removed: u64,
    /// Dead line pointers made reusable.
    pub items_freed: u64,
    pub index_entries_removed: u64,
    pub live: u64,
    pub recently_dead: u64,
}

struct PageScan {
    dead_items: Vec<u16>,
    live: u64,
    recently_dead: u64,
    all_visible: bool,
}

fn scan_page(p: &[u8], vis: &VisCtx<'_>, horizon: u64) -> Result<PageScan> {
    let mut s = PageScan {
        dead_items: Vec::new(),
        live: 0,
        recently_dead: 0,
        all_visible: true,
    };
    for off in 1..=page::max_offset(p) {
        let id = page::item_id(p, off);
        match id.flags {
            LP_DEAD => s.dead_items.push(off),
            LP_NORMAL => {
                let t = page::item(p, off);
                heap::check(t)?;
                match vis.vacuum_status(t, horizon, None)? {
                    Htsv::Live => {
                        s.live += 1;
                        if heap::xmin(t) >= horizon {
                            s.all_visible = false;
                        }
                    }
                    Htsv::RecentlyDead => {
                        s.recently_dead += 1;
                        s.all_visible = false;
                    }
                    _ => s.all_visible = false,
                }
            }
            _ => {}
        }
    }
    Ok(s)
}

impl Inner {
    fn lock_writer(&self) -> Result<MutexGuard<'_, Option<Error>>> {
        let w = self.writer.lock().unwrap();
        self.check_open()?;
        if let Some(e) = &*w {
            return Err(e.clone());
        }
        Ok(w)
    }

    /// Prunes a locked heap page and logs the change. Returns the tuples
    /// whose storage it freed.
    pub(crate) fn prune_locked(
        &self,
        h: &HeapRel,
        lp: &mut Locked<'_>,
        vis: &VisCtx<'_>,
        horizon: u64,
        own: Option<u64>,
    ) -> Result<u64> {
        let (ch, removed) = heap::plan_prune(lp.page(), lp.tag().block, vis, horizon, own)?;
        if ch.is_empty() {
            // Only the hint of when pruning could next help changed; store it
            // the way hint bits are stored.
            if page::prune_xid(lp.page()) != ch.prune_xid {
                page::set_prune_xid(lp.page_mut(), ch.prune_xid);
                if let Some(end) = self.wal.log_hint_image(lp.tag(), lp.page()) {
                    page::set_lsn(lp.page_mut(), end);
                }
                lp.mark_dirty();
            }
            return Ok(0);
        }
        heap::apply_prune(lp.page_mut(), &ch);
        let (_, end) = self
            .wal
            .insert(RM_HEAP_PRUNE, 0, &[rec_block(lp)], &ch.encode());
        page::set_lsn(lp.page_mut(), end);
        lp.mark_dirty();
        h.stats.dead.fetch_sub(removed as i64, Ordering::Relaxed);
        h.stats.pruned.fetch_add(removed, Ordering::Relaxed);
        Ok(removed)
    }

    fn mark_all_visible(&self, h: &HeapRel, lp: &mut Locked<'_>) -> Result<()> {
        let f = page::flags(lp.page());
        if f & PD_ALL_VISIBLE == 0 {
            page::set_flags(lp.page_mut(), f | PD_ALL_VISIBLE);
            if let Some(end) = self.wal.log_hint_image(lp.tag(), lp.page()) {
                page::set_lsn(lp.page_mut(), end);
            }
            lp.mark_dirty();
        }
        vm::set(&self.pool, h.rel, lp.tag().block, true)
    }

    /// VACUUM of a column family: its heap, then its TOAST heap.
    pub(crate) fn vacuum(&self, icf: usize, auto: bool) -> Result<[VacuumStats; 2]> {
        let r = &self.rels[icf];
        Ok([
            self.vacuum_rel(&r.heap, &r.index, auto)?,
            self.vacuum_rel(&r.toast, &r.toast_index, auto)?,
        ])
    }

    pub(crate) fn vacuum_rel(&self, h: &HeapRel, idx: &Index, auto: bool) -> Result<VacuumStats> {
        let pool = &self.pool;
        let nblocks = pool.nblocks(h.rel, FORK_MAIN)?;
        let mut st = VacuumStats::default();
        let mut dead: Vec<Tid> = Vec::new();
        let mut blk = 0u32;
        while blk < nblocks {
            let end = (blk + CHUNK as u32).min(nblocks);
            {
                let _w = self.lock_writer()?;
                let horizon = self.xact.oldest_xmin();
                let vis = self.vis_ctx();
                for b in blk..end {
                    if vm::is_set(pool, h.rel, b)? {
                        st.pages_skipped += 1;
                        continue;
                    }
                    let mut lp = pool.get(h.tag(b), Mode::Read)?.lock();
                    if page::is_new(lp.page()) {
                        continue;
                    }
                    st.pages_scanned += 1;
                    st.tuples_removed += self.prune_locked(h, &mut lp, &vis, horizon, None)?;
                    let s = scan_page(lp.page(), &vis, horizon)?;
                    st.live += s.live;
                    st.recently_dead += s.recently_dead;
                    dead.extend(s.dead_items.iter().map(|&off| Tid { block: b, off }));
                    if s.dead_items.is_empty() && s.all_visible {
                        self.mark_all_visible(h, &mut lp)?;
                    }
                    let free = page::heap_free_space(lp.page(), MAX_HEAP_TUPLES);
                    fsm::record(pool, h.rel, b, free)?;
                }
            }
            blk = end;
            if dead.len() >= MAX_DEAD {
                self.vacuum_dead(h, idx, &mut dead, &mut st)?;
            }
        }
        self.vacuum_dead(h, idx, &mut dead, &mut st)?;
        let s = &h.stats;
        if auto {
            s.autovacuum_count.fetch_add(1, Ordering::Relaxed);
        } else {
            s.vacuum_count.fetch_add(1, Ordering::Relaxed);
        }
        s.vacuum_removed
            .fetch_add(st.tuples_removed, Ordering::Relaxed);
        // PostgreSQL sets n_dead_tup to what VACUUM had to leave behind, and
        // n_live_tup to what it counted when it saw every page.
        s.dead.store(st.recently_dead as i64, Ordering::Relaxed);
        if st.pages_skipped == 0 {
            s.live.store(st.live as i64, Ordering::Relaxed);
        }
        Ok(st)
    }

    /// The index pass and the second heap pass for `dead`.
    fn vacuum_dead(
        &self,
        h: &HeapRel,
        idx: &Index,
        dead: &mut Vec<Tid>,
        st: &mut VacuumStats,
    ) -> Result<()> {
        if dead.is_empty() {
            return Ok(());
        }
        let set: HashSet<Tid> = dead.iter().copied().collect();
        {
            let _w = self.lock_writer()?;
            st.index_entries_removed += idx.bulk_delete(&self.pool, &set)?;
        }
        dead.sort_unstable();
        let groups: Vec<&[Tid]> = dead.chunk_by(|a, b| a.block == b.block).collect();
        for chunk in groups.chunks(CHUNK) {
            let _w = self.lock_writer()?;
            let horizon = self.xact.oldest_xmin();
            let vis = self.vis_ctx();
            for g in chunk {
                let b = g[0].block;
                let mut lp = self.pool.get(h.tag(b), Mode::Read)?.lock();
                let p = lp.page();
                let max = page::max_offset(p);
                let offs: Vec<u16> = g
                    .iter()
                    .map(|t| t.off)
                    .filter(|&o| o <= max && page::item_id(p, o).flags == LP_DEAD)
                    .collect();
                if offs.is_empty() {
                    continue;
                }
                heap::apply_vacuum(lp.page_mut(), &offs);
                let mut main = Vec::with_capacity(2 + 2 * offs.len());
                main.extend_from_slice(&(offs.len() as u16).to_le_bytes());
                for o in &offs {
                    main.extend_from_slice(&o.to_le_bytes());
                }
                let (_, end) = self.wal.insert(RM_HEAP_VACUUM, 0, &[rec_block(&lp)], &main);
                page::set_lsn(lp.page_mut(), end);
                lp.mark_dirty();
                st.items_freed += offs.len() as u64;
                let s = scan_page(lp.page(), &vis, horizon)?;
                if s.dead_items.is_empty() && s.all_visible {
                    self.mark_all_visible(h, &mut lp)?;
                }
                let free = page::heap_free_space(lp.page(), MAX_HEAP_TUPLES);
                fsm::record(&self.pool, h.rel, b, free)?;
            }
        }
        dead.clear();
        Ok(())
    }

    /// Autovacuum: every `autovacuum_naptime_ms`, VACUUM each heap whose dead
    /// tuples passed its threshold.
    pub(crate) fn autovacuum(&self) {
        let nap = Duration::from_millis(self.opts.autovacuum_naptime_ms.max(1));
        loop {
            let started = Instant::now();
            {
                let mut st = self.bg.state.lock().unwrap();
                while !st.stop && started.elapsed() < nap {
                    let left = nap.saturating_sub(started.elapsed());
                    st = self.bg.cv.wait_timeout(st, left).unwrap().0;
                }
                if st.stop {
                    return;
                }
            }
            for r in &self.rels {
                for (h, idx) in [(&r.heap, &r.index), (&r.toast, &r.toast_index)] {
                    let dead = h.stats.dead.load(Ordering::Relaxed).max(0) as u64;
                    let live = h.stats.live.load(Ordering::Relaxed).max(0) as u64;
                    let limit = self.opts.autovacuum_threshold
                        + self.opts.autovacuum_scale_percent * live / 100;
                    if dead <= limit {
                        continue;
                    }
                    if let Err(e) = self.vacuum_rel(h, idx, true) {
                        if !matches!(e, Error::Closed) && !self.halted() {
                            self.poison(e);
                        }
                        return;
                    }
                }
            }
        }
    }
}
