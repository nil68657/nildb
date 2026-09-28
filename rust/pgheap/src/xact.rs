//! Transactions, the commit log and snapshots, after PostgreSQL's MVCC
//! chapter (docs: mvcc.html), src/backend/access/transam/README and
//! heapam_visibility.c.
//!
//! Each write batch is one transaction with a 64-bit xid. Xids never wrap
//! around, so tuples never need freezing. The commit log (pg_xact) keeps
//! two bits per xid, 32,640 xids per 8 KiB page (after the page header), in
//! buffer-pool pages that commit and abort records log. One writer runs at a
//! time, so at most one xid is ever in progress.

use std::cell::Cell;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use nilengine_api::Result;

use crate::bufmgr::{BufferPool, CLOG_REL, FORK_MAIN, Mode};
use crate::heap::{
    self, HEAP_XMAX_COMMITTED, HEAP_XMAX_INVALID, HEAP_XMIN_COMMITTED, HEAP_XMIN_INVALID,
};
use crate::page::{BLCKSZ, HEADER};
use crate::wal::{Lsn, PageTag};

/// First xid handed out (PostgreSQL's FirstNormalTransactionId); 0 means
/// "no xid".
pub const FIRST_XID: u64 = 3;
pub const XIDS_PER_PAGE: u64 = ((BLCKSZ - HEADER) * 4) as u64;

pub const XID_IN_PROGRESS: u8 = 0;
pub const XID_COMMITTED: u8 = 1;
pub const XID_ABORTED: u8 = 2;

pub fn clog_tag(xid: u64) -> PageTag {
    PageTag {
        rel: CLOG_REL,
        fork: FORK_MAIN,
        block: (xid / XIDS_PER_PAGE) as u32,
    }
}

fn clog_pos(xid: u64) -> (usize, u32) {
    let i = (xid % XIDS_PER_PAGE) as usize;
    (HEADER + i / 4, (i % 4) as u32 * 2)
}

pub fn clog_get(p: &[u8], xid: u64) -> u8 {
    let (b, s) = clog_pos(xid);
    (p[b] >> s) & 3
}

pub fn clog_set(p: &mut [u8], xid: u64, status: u8) {
    let (b, s) = clog_pos(xid);
    p[b] = (p[b] & !(3 << s)) | (status << s);
}

/// A snapshot: every xid below `xmin` has finished, every xid at or above
/// `xmax` is invisible, and `running` was in progress when it was taken.
/// `seq` is the last committed xid at that moment.
#[derive(Clone, Debug)]
pub struct SnapData {
    pub xmin: u64,
    pub xmax: u64,
    pub running: Option<u64>,
    pub seq: u64,
}

struct ProcState {
    next_xid: u64,
    running: Option<u64>,
    latest_committed: u64,
    /// xmin of every registered snapshot, with a count.
    snaps: BTreeMap<u64, usize>,
}

/// Transaction state shared by the writer, readers and VACUUM
/// (PostgreSQL's ProcArray plus the xid counter).
pub struct Xact {
    procs: Mutex<ProcState>,
    /// Every xid at or below this has its commit or abort record flushed,
    /// so a committed hint bit on it can reach disk safely.
    durable_xid: AtomicU64,
    pending: Mutex<VecDeque<(Lsn, u64)>>,
    pub user_snapshots: AtomicUsize,
}

impl Xact {
    pub fn new(next_xid: u64, latest_committed: u64) -> Xact {
        Xact {
            procs: Mutex::new(ProcState {
                next_xid,
                running: None,
                latest_committed,
                snaps: BTreeMap::new(),
            }),
            durable_xid: AtomicU64::new(next_xid.saturating_sub(1)),
            pending: Mutex::new(VecDeque::new()),
            user_snapshots: AtomicUsize::new(0),
        }
    }

    /// Starts a transaction (the caller holds the writer lock).
    pub fn assign_xid(&self) -> u64 {
        let mut p = self.procs.lock().unwrap();
        let xid = p.next_xid;
        p.next_xid += 1;
        p.running = Some(xid);
        xid
    }

    /// Ends the running transaction after its commit-log bits are set, so
    /// any snapshot that no longer lists it finds its fate in the log.
    pub fn end_xact(&self, xid: u64, committed: bool) {
        let mut p = self.procs.lock().unwrap();
        if p.running == Some(xid) {
            p.running = None;
        }
        if committed {
            p.latest_committed = p.latest_committed.max(xid);
        }
    }

    pub fn take_snapshot(&self) -> SnapData {
        let mut p = self.procs.lock().unwrap();
        let xmax = p.next_xid;
        let xmin = p.running.unwrap_or(xmax);
        *p.snaps.entry(xmin).or_insert(0) += 1;
        SnapData {
            xmin,
            xmax,
            running: p.running,
            seq: p.latest_committed,
        }
    }

    pub fn release_snapshot(&self, xmin: u64) {
        let mut p = self.procs.lock().unwrap();
        if let Some(c) = p.snaps.get_mut(&xmin) {
            *c -= 1;
            if *c == 0 {
                p.snaps.remove(&xmin);
            }
        }
    }

    /// The oldest xmin any snapshot or the running transaction still needs:
    /// a version deleted by a transaction that committed below it is dead to
    /// everyone (PostgreSQL's OldestXmin).
    pub fn oldest_xmin(&self) -> u64 {
        let p = self.procs.lock().unwrap();
        let mut h = p.running.unwrap_or(p.next_xid);
        if let Some((&x, _)) = p.snaps.first_key_value() {
            h = h.min(x);
        }
        h
    }

    pub fn next_xid(&self) -> u64 {
        self.procs.lock().unwrap().next_xid
    }

    pub fn latest_committed(&self) -> u64 {
        self.procs.lock().unwrap().latest_committed
    }

    pub fn registered_snapshots(&self) -> usize {
        self.procs.lock().unwrap().snaps.values().sum()
    }

    /// Records the end LSN of a commit or abort record that is not flushed
    /// yet.
    pub fn note_unflushed(&self, lsn: Lsn, xid: u64) {
        self.pending.lock().unwrap().push_back((lsn, xid));
    }

    /// Marks `xid` durable at once (its record was flushed with the commit).
    pub fn note_durable(&self, xid: u64) {
        self.durable_xid.fetch_max(xid, Ordering::AcqRel);
    }

    /// The highest xid whose commit or abort record is flushed, given the
    /// WAL's flushed LSN.
    pub fn durable_xid(&self, flushed: Lsn) -> u64 {
        let d = self.durable_xid.load(Ordering::Acquire);
        let mut q = self.pending.lock().unwrap();
        if q.front().is_none_or(|&(lsn, _)| lsn > flushed) {
            return d;
        }
        let mut best = d;
        while let Some(&(lsn, xid)) = q.front() {
            if lsn > flushed {
                break;
            }
            best = best.max(xid);
            q.pop_front();
        }
        drop(q);
        self.durable_xid.fetch_max(best, Ordering::AcqRel);
        best
    }
}

/// Reads an xid's commit-log status. Pages past the end of the log hold no
/// finished xid.
pub fn xid_status(pool: &BufferPool, xid: u64) -> Result<u8> {
    let tag = clog_tag(xid);
    if tag.block >= pool.nblocks(CLOG_REL, FORK_MAIN)? {
        return Ok(XID_IN_PROGRESS);
    }
    let b = pool.get(tag, Mode::Read)?;
    let g = b.read();
    Ok(clog_get(&g[..], xid))
}

/// Whose view a visibility check takes.
pub enum View<'s> {
    Snap(&'s SnapData),
    /// The writer sees every committed version plus its own changes; every
    /// other xid has finished because it is the only writer.
    Writer(Option<u64>),
}

/// A hint bit to set once the page can be locked exclusively: `bit` in the
/// tuple at `off`, valid while the tuple's xmin (or xmax) still equals
/// `xid`.
#[derive(Clone, Copy, Debug)]
pub struct Hint {
    pub off: u16,
    pub xmax: bool,
    pub xid: u64,
    pub bit: u16,
}

/// PostgreSQL's HeapTupleSatisfiesVacuum results.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Htsv {
    Dead,
    RecentlyDead,
    Live,
    InsertInProgress,
    DeleteInProgress,
}

/// Visibility checks with a one-entry commit-log cache.
pub struct VisCtx<'a> {
    pub pool: &'a BufferPool,
    /// Committed hint bits are set only for xids at or below this.
    pub durable: u64,
    cache: Cell<(u64, u8)>,
}

impl<'a> VisCtx<'a> {
    pub fn new(pool: &'a BufferPool, durable: u64) -> Self {
        VisCtx {
            pool,
            durable,
            cache: Cell::new((0, 0)),
        }
    }

    /// Whether a finished xid committed. A crashed transaction left "in
    /// progress" in the log counts as aborted.
    fn committed(&self, xid: u64) -> Result<bool> {
        let (cx, cs) = self.cache.get();
        let s = if cx == xid {
            cs
        } else {
            let s = xid_status(self.pool, xid)?;
            self.cache.set((xid, s));
            s
        };
        Ok(s == XID_COMMITTED)
    }

    fn hint(&self, hints: &mut Vec<Hint>, off: u16, xmax: bool, xid: u64, committed: bool) {
        let bit = match (xmax, committed) {
            (false, true) => HEAP_XMIN_COMMITTED,
            (false, false) => HEAP_XMIN_INVALID,
            (true, true) => HEAP_XMAX_COMMITTED,
            (true, false) => HEAP_XMAX_INVALID,
        };
        // A committed hint must not reach disk before the commit record
        // does (PostgreSQL's SetHintBits checks the same).
        if committed && xid > self.durable {
            return;
        }
        hints.push(Hint {
            off,
            xmax,
            xid,
            bit,
        });
    }

    /// Is the tuple `t` at `off` visible in `view`? Hint bits learned from
    /// the commit log go to `hints`.
    pub fn visible(
        &self,
        view: &View<'_>,
        off: u16,
        t: &[u8],
        hints: &mut Vec<Hint>,
    ) -> Result<bool> {
        let mask = heap::infomask(t);
        if mask & HEAP_XMIN_INVALID != 0 {
            return Ok(false);
        }
        let xmin = heap::xmin(t);
        let inserted = match view {
            View::Snap(s) => {
                if xmin >= s.xmax || s.running == Some(xmin) {
                    false
                } else if mask & HEAP_XMIN_COMMITTED != 0 {
                    true
                } else {
                    let c = self.committed(xmin)?;
                    self.hint(hints, off, false, xmin, c);
                    c
                }
            }
            View::Writer(own) => {
                if *own == Some(xmin) || mask & HEAP_XMIN_COMMITTED != 0 {
                    true
                } else {
                    let c = self.committed(xmin)?;
                    self.hint(hints, off, false, xmin, c);
                    c
                }
            }
        };
        if !inserted {
            return Ok(false);
        }
        let xmax = heap::xmax(t);
        if xmax == 0 || mask & HEAP_XMAX_INVALID != 0 {
            return Ok(true);
        }
        let deleted = match view {
            View::Snap(s) => {
                if xmax >= s.xmax || s.running == Some(xmax) {
                    false
                } else if mask & HEAP_XMAX_COMMITTED != 0 {
                    true
                } else {
                    let c = self.committed(xmax)?;
                    self.hint(hints, off, true, xmax, c);
                    c
                }
            }
            View::Writer(own) => {
                if *own == Some(xmax) || mask & HEAP_XMAX_COMMITTED != 0 {
                    true
                } else {
                    let c = self.committed(xmax)?;
                    self.hint(hints, off, true, xmax, c);
                    c
                }
            }
        };
        Ok(!deleted)
    }

    /// PostgreSQL's HeapTupleSatisfiesVacuum: can the tuple go? `horizon` is
    /// the oldest xmin; `own` is the running transaction, if any.
    pub fn vacuum_status(&self, t: &[u8], horizon: u64, own: Option<u64>) -> Result<Htsv> {
        let mask = heap::infomask(t);
        if mask & HEAP_XMIN_INVALID != 0 {
            return Ok(Htsv::Dead);
        }
        let xmin = heap::xmin(t);
        if own == Some(xmin) {
            return Ok(Htsv::InsertInProgress);
        }
        if mask & HEAP_XMIN_COMMITTED == 0 && !self.committed(xmin)? {
            return Ok(Htsv::Dead);
        }
        let xmax = heap::xmax(t);
        if xmax == 0 || mask & HEAP_XMAX_INVALID != 0 {
            return Ok(Htsv::Live);
        }
        if own == Some(xmax) {
            return Ok(Htsv::DeleteInProgress);
        }
        if mask & HEAP_XMAX_COMMITTED == 0 && !self.committed(xmax)? {
            return Ok(Htsv::Live);
        }
        Ok(if xmax < horizon {
            Htsv::Dead
        } else {
            Htsv::RecentlyDead
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clog_bits_and_snapshots() {
        let mut p = vec![0u8; BLCKSZ];
        clog_set(&mut p, 5, XID_COMMITTED);
        clog_set(&mut p, 6, XID_ABORTED);
        clog_set(&mut p, 7, XID_COMMITTED);
        assert_eq!(clog_get(&p, 5), XID_COMMITTED);
        assert_eq!(clog_get(&p, 6), XID_ABORTED);
        assert_eq!(clog_get(&p, 8), XID_IN_PROGRESS);
        clog_set(&mut p, 6, XID_COMMITTED);
        assert_eq!(clog_get(&p, 6), XID_COMMITTED);
        assert_eq!(clog_get(&p, 5), XID_COMMITTED);

        let x = Xact::new(FIRST_XID, 0);
        let s0 = x.take_snapshot();
        assert_eq!((s0.xmin, s0.xmax, s0.running), (3, 3, None));
        let a = x.assign_xid();
        let s1 = x.take_snapshot();
        assert_eq!((s1.xmin, s1.xmax, s1.running), (a, a + 1, Some(a)));
        assert_eq!(x.oldest_xmin(), 3);
        x.end_xact(a, true);
        x.release_snapshot(s0.xmin);
        x.release_snapshot(s1.xmin);
        assert_eq!(x.oldest_xmin(), a + 1);
        assert_eq!(x.latest_committed(), a);
    }
}
