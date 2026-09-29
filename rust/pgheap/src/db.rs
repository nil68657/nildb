//! The pgheap engine: open and crash recovery, the writer, commits,
//! checkpoints, TOAST, snapshots and properties.
//!
//! Each column family owns four relations: a heap, its B-tree index, a
//! TOAST heap for values too large to keep inline, and the TOAST heap's
//! index. Column family n (in catalog order) uses relations 16 + 4n to
//! 19 + 4n; the commit log is relation 1.
//!
//! One engine-level writer lock serializes write batches, as NilDB's server
//! already locks the keys a command touches. Readers never take it: they
//! read committed versions through snapshots and hold page locks only while
//! copying data out.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use nilengine_api::coding::{get_u32, get_u64};
use nilengine_api::{
    BatchOp, CfId, CrashMode, DbIterator, Engine, EngineKind, Error, IoContext, IterOptions,
    Options, Result, Snapshot, WriteBatch, WriteOptions, checkpoint, i64add,
};

use crate::btree::{Index, MAX_KEY_LEN};
use crate::bufmgr::{BufferPool, CLOG_REL, FORK_MAIN, FORK_VM, Locked, Mode};
use crate::control::{
    Catalog, Control, ControlFile, STATE_IN_PRODUCTION, STATE_SHUTDOWN, take_stats, write_stats,
};
use crate::file::sync_dir;
use crate::fsm;
use crate::heap::{self, Found, MAX_HEAP_TUPLES, TUPLE_HEADER, Tid};
use crate::iter::PgIter;
use crate::page::{self, BLCKSZ};
use crate::redo;
use crate::vm;
use crate::wal::{
    FIRST_LSN, Lsn, PageTag, RM_ABORT, RM_CHECKPOINT, RM_COMMIT, RM_HEAP_DELETE,
    RM_HEAP_HOT_UPDATE, RM_HEAP_INSERT, RM_HEAP_UPDATE, RecBlock, Wal, WalReader,
};
use crate::xact::{
    FIRST_XID, SnapData, View, VisCtx, XID_ABORTED, XID_COMMITTED, Xact, clog_set, clog_tag,
};

/// Longest value the engine accepts (PostgreSQL's TOAST limit).
pub const MAX_VALUE_LEN: usize = 1 << 30;
/// A tuple longer than this moves its value to the TOAST heap
/// (PostgreSQL's TOAST_TUPLE_THRESHOLD at 8 KiB pages).
pub const TOAST_THRESHOLD: usize = 2032;
/// Bytes of value per TOAST chunk, sized so four chunk tuples fill a page
/// (PostgreSQL uses 1996 with its smaller tuple header).
pub const TOAST_CHUNK: usize = (BLCKSZ - page::HEADER) / 4 - page::ITEM_ID - TUPLE_HEADER - 12;
const TOAST_PTR_LEN: usize = 16;
const FIRST_REL: u32 = 16;
/// A checkpoint also runs this long after the last one if anything was
/// logged since (PostgreSQL's checkpoint_timeout default).
const CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(300);
/// PostgreSQL's wal_writer_delay default.
const WAL_WRITER_DELAY: Duration = Duration::from_millis(200);

const OPEN: u8 = 0;
const CLOSED: u8 = 1;

static NEXT_DB_ID: AtomicU64 = AtomicU64::new(1);

/// Counters in the style of pg_stat_user_tables for one heap.
#[derive(Default)]
pub struct RelStats {
    pub ins: AtomicU64,
    pub upd: AtomicU64,
    pub hot_upd: AtomicU64,
    pub del: AtomicU64,
    pub live: AtomicI64,
    pub dead: AtomicI64,
    pub live_bytes: AtomicI64,
    pub vacuum_count: AtomicU64,
    pub autovacuum_count: AtomicU64,
    pub pruned: AtomicU64,
    pub vacuum_removed: AtomicU64,
}

impl RelStats {
    fn row(&self) -> Vec<u64> {
        vec![
            self.ins.load(Ordering::Relaxed),
            self.upd.load(Ordering::Relaxed),
            self.hot_upd.load(Ordering::Relaxed),
            self.del.load(Ordering::Relaxed),
            self.live.load(Ordering::Relaxed) as u64,
            self.dead.load(Ordering::Relaxed) as u64,
            self.live_bytes.load(Ordering::Relaxed) as u64,
            self.vacuum_count.load(Ordering::Relaxed),
            self.autovacuum_count.load(Ordering::Relaxed),
            self.pruned.load(Ordering::Relaxed),
            self.vacuum_removed.load(Ordering::Relaxed),
        ]
    }

    fn load_row(&self, r: &[u64]) {
        let g = |i: usize| r.get(i).copied().unwrap_or(0);
        self.ins.store(g(0), Ordering::Relaxed);
        self.upd.store(g(1), Ordering::Relaxed);
        self.hot_upd.store(g(2), Ordering::Relaxed);
        self.del.store(g(3), Ordering::Relaxed);
        self.live.store(g(4) as i64, Ordering::Relaxed);
        self.dead.store(g(5) as i64, Ordering::Relaxed);
        self.live_bytes.store(g(6) as i64, Ordering::Relaxed);
        self.vacuum_count.store(g(7), Ordering::Relaxed);
        self.autovacuum_count.store(g(8), Ordering::Relaxed);
        self.pruned.store(g(9), Ordering::Relaxed);
        self.vacuum_removed.store(g(10), Ordering::Relaxed);
    }
}

/// A heap relation, the block its inserts try first (PostgreSQL's
/// rd_targblock) and where its free-space map search starts.
pub struct HeapRel {
    pub rel: u32,
    pub target: AtomicU32,
    pub fsm_hint: AtomicU32,
    pub stats: RelStats,
}

impl HeapRel {
    fn new(rel: u32) -> HeapRel {
        HeapRel {
            rel,
            target: AtomicU32::new(u32::MAX),
            fsm_hint: AtomicU32::new(0),
            stats: RelStats::default(),
        }
    }

    pub(crate) fn tag(&self, block: u32) -> PageTag {
        PageTag {
            rel: self.rel,
            fork: FORK_MAIN,
            block,
        }
    }
}

/// The four relations of one column family.
pub struct CfRels {
    pub name: String,
    pub heap: HeapRel,
    pub index: Index,
    pub toast: HeapRel,
    pub toast_index: Index,
}

impl CfRels {
    fn new(name: &str, pos: usize) -> CfRels {
        let base = FIRST_REL + 4 * pos as u32;
        CfRels {
            name: name.to_string(),
            heap: HeapRel::new(base),
            index: Index::new(base + 1),
            toast: HeapRel::new(base + 2),
            toast_index: Index::new(base + 3),
        }
    }
}

/// A registered snapshot. Dropping it (with the last iterator using it)
/// releases its xmin, which lets VACUUM move past it.
pub struct PgSnapshot {
    pub data: SnapData,
    db: u64,
    xact: Arc<Xact>,
    user: bool,
}

impl Drop for PgSnapshot {
    fn drop(&mut self) {
        self.xact.release_snapshot(self.data.xmin);
        if self.user {
            self.xact.user_snapshots.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[derive(Default)]
pub(crate) struct BgState {
    checkpoint: bool,
    pub(crate) stop: bool,
}

/// The checkpointer and autovacuum threads and what wakes them.
#[derive(Default)]
pub(crate) struct Bg {
    pub(crate) state: Mutex<BgState>,
    pub(crate) cv: Condvar,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

pub struct Inner {
    id: u64,
    dir: PathBuf,
    pub(crate) opts: Options,
    cf_names: Vec<String>,
    /// `CfId` to catalog position.
    cf_map: Vec<usize>,
    pub(crate) rels: Vec<CfRels>,
    pub(crate) pool: BufferPool,
    pub(crate) wal: Arc<Wal>,
    pub(crate) xact: Arc<Xact>,
    /// The writer lock; holds the error that stopped writes, if any.
    pub(crate) writer: Mutex<Option<Error>>,
    ckpt: Mutex<()>,
    control: Mutex<ControlFile>,
    state: AtomicU8,
    /// Set by `crash`: background work stops before its next write.
    halt: AtomicBool,
    /// Tests only: the checkpoint step at which `crash_during_checkpoint`
    /// stops a checkpoint (0: none).
    ckpt_stop: AtomicU8,
    lock_file: Mutex<Option<File>>,
    pub(crate) bg: Bg,
    redo_hint: AtomicU64,
    last_ckpt_end: AtomicU64,
    checkpoints: AtomicU64,
}

pub struct PgHeapEngine {
    pub(crate) inner: Arc<Inner>,
}

fn lock_dir(dir: &Path) -> Result<File> {
    let lock_path = dir.join("LOCK");
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .ctx(|| format!("open {}", lock_path.display()))?;
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(TryLockError::WouldBlock) => Err(Error::Busy(format!(
            "{} is locked by another open",
            dir.display()
        ))),
        Err(TryLockError::Error(e)) => Err(Error::io(format!("lock {}", lock_path.display()), &e)),
    }
}

fn checkpoint_body(redo: Lsn, next_xid: u64, latest: u64, shutdown: bool) -> Vec<u8> {
    let mut b = Vec::with_capacity(25);
    b.extend_from_slice(&redo.to_le_bytes());
    b.extend_from_slice(&next_xid.to_le_bytes());
    b.extend_from_slice(&latest.to_le_bytes());
    b.push(shutdown as u8);
    b
}

/// Creates the directories, the first checkpoint record, an empty catalog
/// and, last, `pg_control`, whose presence marks a database.
fn init_database(dir: &Path, ctl_path: &Path) -> Result<ControlFile> {
    for sub in ["base", "pg_xact", "pg_wal"] {
        let p = dir.join(sub);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).ctx(|| format!("create {}", p.display()))?;
    }
    let _ = take_stats(dir);
    let wal = Wal::open(&dir.join("pg_wal"), FIRST_LSN, 0, FIRST_LSN)?;
    let (start, end) = wal.insert(
        RM_CHECKPOINT,
        0,
        &[],
        &checkpoint_body(FIRST_LSN, FIRST_XID, 0, true),
    );
    wal.flush(end)?;
    drop(wal);
    Catalog::default().write(dir)?;
    let c = ControlFile::create(
        ctl_path,
        Control {
            seq: 0,
            state: STATE_SHUTDOWN,
            checkpoint: start,
            redo: FIRST_LSN,
            next_xid: FIRST_XID,
            latest_committed: 0,
        },
    )?;
    sync_dir(dir)?;
    Ok(c)
}

/// First recovery pass: where the valid log ends, checking that the
/// checkpoint record `pg_control` names is there.
fn find_wal_end(wal_dir: &Path, ctl: &Control) -> Result<(Lsn, Lsn)> {
    let mut r = WalReader::new(wal_dir, ctl.redo);
    let mut found = false;
    while let Some(rec) = r.next_record()? {
        if rec.start == ctl.checkpoint && rec.rmid == RM_CHECKPOINT {
            found = true;
        }
    }
    if !found {
        return Err(Error::corruption(format!(
            "the checkpoint record at {} is missing from the WAL",
            ctl.checkpoint
        )));
    }
    Ok((r.lsn, r.prev.unwrap_or(0)))
}

/// Second pass: replays every record from the redo pointer. Returns the
/// next xid, the last committed xid and the number of records replayed.
fn replay(pool: &BufferPool, wal_dir: &Path, ctl: &Control) -> Result<(u64, u64, u64)> {
    let mut r = WalReader::new(wal_dir, ctl.redo);
    let (mut next_xid, mut latest, mut n) = (ctl.next_xid, ctl.latest_committed, 0u64);
    while let Some(rec) = r.next_record()? {
        if rec.xid != 0 {
            next_xid = next_xid.max(rec.xid + 1);
        }
        if rec.rmid == RM_COMMIT {
            latest = latest.max(rec.xid);
        }
        if rec.rmid != RM_CHECKPOINT {
            n += 1;
        }
        redo::apply(pool, &rec)?;
    }
    Ok((next_xid, latest, n))
}

/// Opens (creating if allowed) a pgheap database in `dir`; `CfId` n names
/// `cfs[n]`.
pub fn open(dir: &Path, cfs: &[&str], opts: &Options) -> Result<PgHeapEngine> {
    if cfs.is_empty() {
        return Err(Error::invalid("open needs at least one column family"));
    }
    for (i, a) in cfs.iter().enumerate() {
        if cfs[..i].contains(a) {
            return Err(Error::invalid(format!("column family {a:?} listed twice")));
        }
        if a.len() > 255 {
            return Err(Error::invalid(format!(
                "column family name of {} bytes; the limit is 255",
                a.len()
            )));
        }
    }
    if !dir.exists() {
        if !opts.create_if_missing {
            return Err(Error::NotFound(format!("{} does not exist", dir.display())));
        }
        fs::create_dir_all(dir).ctx(|| format!("create {}", dir.display()))?;
    }
    let lock_file = lock_dir(dir)?;
    let ctl_path = dir.join("pg_control");
    let control = match ControlFile::open(&ctl_path)? {
        Some(c) => c,
        None if opts.create_if_missing => init_database(dir, &ctl_path)?,
        None => {
            return Err(Error::NotFound(format!(
                "{} holds no database",
                dir.display()
            )));
        }
    };
    let ctl = control.get().clone();
    let mut catalog = Catalog::read(dir)?;
    let mut cf_map = Vec::with_capacity(cfs.len());
    let mut added = false;
    for name in cfs {
        match catalog.cfs.iter().position(|n| n == name) {
            Some(i) => cf_map.push(i),
            None if opts.create_if_missing => {
                cf_map.push(catalog.cfs.len());
                catalog.cfs.push(name.to_string());
                added = true;
            }
            None => {
                return Err(Error::NotFound(format!(
                    "column family {name:?} does not exist"
                )));
            }
        }
    }
    if added {
        catalog.write(dir)?;
    }

    let wal_dir = dir.join("pg_wal");
    let (end, prev) = find_wal_end(&wal_dir, &ctl)?;
    let wal = Arc::new(Wal::open(&wal_dir, end, prev, ctl.redo)?);
    let pool = BufferPool::new(
        &dir.join("base"),
        &dir.join("pg_xact"),
        opts.cache_bytes / BLCKSZ,
        opts.track_unsynced_writes,
        wal.clone(),
    );
    let (next_xid, latest, replayed) = replay(&pool, &wal_dir, &ctl)?;
    let crashed = ctl.state != STATE_SHUTDOWN;

    let rels: Vec<CfRels> = catalog
        .cfs
        .iter()
        .enumerate()
        .map(|(i, n)| CfRels::new(n, i))
        .collect();
    for r in &rels {
        r.index.load_meta(&pool)?;
        r.toast_index.load_meta(&pool)?;
        for h in [&r.heap, &r.toast] {
            let n = pool.nblocks(h.rel, FORK_MAIN)?;
            h.target
                .store(if n == 0 { u32::MAX } else { n - 1 }, Ordering::Relaxed);
            if crashed {
                // The visibility map is not WAL-logged, so after a crash it
                // may claim a page all-visible that is not; start it over.
                pool.truncate_fork(h.rel, FORK_VM)?;
            }
        }
    }
    let saved = take_stats(dir);
    if !crashed && let Some(rows) = saved {
        for (i, r) in rels.iter().enumerate() {
            if let (Some(h), Some(t)) = (rows.get(2 * i), rows.get(2 * i + 1)) {
                r.heap.stats.load_row(h);
                r.toast.stats.load_row(t);
            }
        }
    }

    let inner = Arc::new(Inner {
        id: NEXT_DB_ID.fetch_add(1, Ordering::Relaxed),
        dir: dir.to_path_buf(),
        opts: opts.clone(),
        cf_names: cfs.iter().map(|s| s.to_string()).collect(),
        cf_map,
        rels,
        pool,
        wal,
        xact: Arc::new(Xact::new(next_xid, latest)),
        writer: Mutex::new(None),
        ckpt: Mutex::new(()),
        control: Mutex::new(control),
        state: AtomicU8::new(OPEN),
        halt: AtomicBool::new(false),
        ckpt_stop: AtomicU8::new(0),
        lock_file: Mutex::new(Some(lock_file)),
        bg: Bg::default(),
        redo_hint: AtomicU64::new(ctl.redo),
        last_ckpt_end: AtomicU64::new(end),
        checkpoints: AtomicU64::new(0),
    });
    if crashed || replayed > 0 {
        // End-of-recovery checkpoint: the replayed pages become durable and
        // the next recovery starts here.
        inner.checkpoint(false)?;
    } else {
        let mut c = inner.control.lock().unwrap();
        let cur = c.get().clone();
        c.write(Control {
            state: STATE_IN_PRODUCTION,
            ..cur
        })?;
    }
    inner.start_bg();
    Ok(PgHeapEngine { inner })
}

fn toast_ptr(value_id: u64, raw: u32, chunks: u32) -> Vec<u8> {
    let mut b = Vec::with_capacity(TOAST_PTR_LEN);
    b.extend_from_slice(&value_id.to_le_bytes());
    b.extend_from_slice(&raw.to_le_bytes());
    b.extend_from_slice(&chunks.to_le_bytes());
    b
}

fn parse_toast_ptr(b: &[u8]) -> Result<(u64, u32, u32)> {
    if b.len() != TOAST_PTR_LEN {
        return Err(Error::corruption(format!(
            "TOAST pointer of {} bytes",
            b.len()
        )));
    }
    Ok((get_u64(b, 0), get_u32(b, 8), get_u32(b, 12)))
}

/// Key of a TOAST chunk: the value id and chunk number, big-endian so the
/// chunks of one value sort together and in order.
fn chunk_key(value_id: u64, seq: u32) -> [u8; 12] {
    let mut k = [0u8; 12];
    k[..8].copy_from_slice(&value_id.to_be_bytes());
    k[8..].copy_from_slice(&seq.to_be_bytes());
    k
}

pub(crate) fn rec_block<'p>(l: &'p Locked<'_>) -> RecBlock<'p> {
    RecBlock {
        tag: l.tag(),
        page: l.page(),
        force_image: false,
        standard: true,
    }
}

impl Inner {
    pub(crate) fn check_open(&self) -> Result<()> {
        if self.state.load(Ordering::Acquire) == OPEN {
            Ok(())
        } else {
            Err(Error::Closed)
        }
    }

    pub(crate) fn halted(&self) -> bool {
        self.halt.load(Ordering::Acquire)
    }

    fn icf(&self, cf: CfId) -> Result<usize> {
        self.cf_map.get(cf as usize).copied().ok_or_else(|| {
            Error::invalid(format!(
                "column family {cf} does not exist ({} are open)",
                self.cf_map.len()
            ))
        })
    }

    pub(crate) fn vis_ctx(&self) -> VisCtx<'_> {
        VisCtx::new(&self.pool, self.xact.durable_xid(self.wal.flushed_lsn()))
    }

    fn new_snapshot(&self, user: bool) -> PgSnapshot {
        let data = self.xact.take_snapshot();
        if user {
            self.xact.user_snapshots.fetch_add(1, Ordering::Relaxed);
        }
        PgSnapshot {
            data,
            db: self.id,
            xact: self.xact.clone(),
            user,
        }
    }

    fn user_snapshot<'s>(&self, s: &'s Snapshot) -> Result<&'s PgSnapshot> {
        let p = s
            .downcast::<PgSnapshot>()
            .ok_or_else(|| Error::invalid("snapshot comes from another engine kind"))?;
        if p.db != self.id {
            return Err(Error::invalid("snapshot comes from another database"));
        }
        Ok(p)
    }

    /// The value of the first version among the chains `tids` that `view`
    /// sees.
    pub(crate) fn resolve(
        &self,
        icf: usize,
        key: &[u8],
        tids: &[Tid],
        vis: &VisCtx<'_>,
        view: &View<'_>,
    ) -> Result<Option<Vec<u8>>> {
        let rel = self.rels[icf].heap.rel;
        for &tid in tids {
            if let Some(f) = heap::fetch_chain(&self.pool, rel, tid, key, vis, view)? {
                return Ok(Some(self.detoast(icf, &f.tuple)?));
            }
        }
        Ok(None)
    }

    fn read_key(&self, icf: usize, key: &[u8], snap: &SnapData) -> Result<Option<Vec<u8>>> {
        let tids = self.rels[icf].index.lookup(&self.pool, key)?;
        if tids.is_empty() {
            return Ok(None);
        }
        let vis = self.vis_ctx();
        self.resolve(icf, key, &tids, &vis, &View::Snap(snap))
    }

    /// A tuple's value, reassembled from its TOAST chunks when stored out of
    /// line. Chunks are read without a visibility check (PostgreSQL's
    /// SnapshotToast): a chunk lives as long as some snapshot can see the
    /// version that points at it.
    pub(crate) fn detoast(&self, icf: usize, t: &[u8]) -> Result<Vec<u8>> {
        if !heap::is_external(t) {
            return Ok(heap::value(t).to_vec());
        }
        let (value_id, raw, chunks) = parse_toast_ptr(heap::value(t))?;
        let r = &self.rels[icf];
        let (groups, _) = r.toast_index.scan_forward(
            &self.pool,
            &chunk_key(value_id, 0),
            true,
            chunks as usize,
        )?;
        let mut out = Vec::with_capacity(raw as usize);
        let mut seq = 0u32;
        for g in &groups {
            if g.key.len() != 12 || g.key[..8] != value_id.to_be_bytes() {
                break;
            }
            let n = u32::from_be_bytes(g.key[8..12].try_into().expect("12-byte key"));
            if n != seq {
                break;
            }
            out.extend_from_slice(&self.read_chunk(r.toast.rel, &g.key, &g.tids)?);
            seq += 1;
        }
        if seq != chunks || out.len() != raw as usize {
            return Err(Error::corruption(format!(
                "TOAST value {value_id:#x} has {seq} of {chunks} chunks, {} of {raw} bytes",
                out.len()
            )));
        }
        Ok(out)
    }

    fn read_chunk(&self, rel: u32, key: &[u8], tids: &[Tid]) -> Result<Vec<u8>> {
        for tid in tids {
            if tid.block >= self.pool.nblocks(rel, FORK_MAIN)? {
                continue;
            }
            let b = self.pool.get(
                PageTag {
                    rel,
                    fork: FORK_MAIN,
                    block: tid.block,
                },
                Mode::Read,
            )?;
            let g = b.read();
            let p = &g[..];
            if page::is_new(p)
                || tid.off == 0
                || tid.off > page::max_offset(p)
                || page::item_id(p, tid.off).flags != page::LP_NORMAL
            {
                continue;
            }
            let t = page::item(p, tid.off);
            heap::check(t)?;
            if heap::key(t) == key {
                return Ok(heap::value(t).to_vec());
            }
        }
        Err(Error::corruption("TOAST chunk has no live tuple"))
    }

    pub(crate) fn poison(&self, e: Error) {
        let mut w = self.writer.lock().unwrap();
        if w.is_none() {
            *w = Some(e);
        }
    }

    fn write(&self, batch: &WriteBatch, opts: WriteOptions) -> Result<()> {
        self.check_open()?;
        batch.validate(self.cf_map.len(), MAX_KEY_LEN, MAX_VALUE_LEN)?;
        let mut w = self.writer.lock().unwrap();
        self.check_open()?;
        if let Some(e) = &*w {
            return Err(e.clone());
        }
        if batch.is_empty() {
            if opts.sync {
                self.wal.flush(self.wal.insert_lsn())?;
            }
            return Ok(());
        }
        let mut txn = Txn::new(self);
        let res = batch.iter().try_for_each(|op| txn.apply(op));
        let outcome = match res {
            Ok(()) if txn.xid.is_some() => self.finish(&txn, true, opts.sync),
            Ok(()) => {
                if opts.sync {
                    self.wal.flush(self.wal.insert_lsn())
                } else {
                    Ok(())
                }
            }
            Err(e) if txn.logical => {
                // A merge into a value that is not a counter: the batch
                // aborts and its tuples stay behind, dead, for VACUUM.
                if txn.xid.is_some()
                    && let Err(fatal) = self.finish(&txn, false, false)
                {
                    *w = Some(fatal.clone());
                    return Err(fatal);
                }
                return Err(e);
            }
            Err(e) => Err(e),
        };
        if let Err(e) = &outcome {
            *w = Some(e.clone());
        }
        drop(w);
        if outcome.is_ok()
            && self
                .wal
                .insert_lsn()
                .saturating_sub(self.redo_hint.load(Ordering::Acquire))
                >= self.opts.checkpoint_wal_bytes
        {
            self.request_checkpoint();
        }
        outcome
    }

    /// Commits or aborts `txn`: sets its commit-log bits under a commit or
    /// abort record, flushes or writes the WAL, then takes the xid out of
    /// the running set so new snapshots see the outcome.
    fn finish(&self, txn: &Txn<'_>, commit: bool, sync: bool) -> Result<()> {
        let xid = txn.xid.expect("finish needs an xid");
        let tag = clog_tag(xid);
        self.pool
            .ensure_nblocks(CLOG_REL, FORK_MAIN, tag.block + 1)?;
        let end = {
            let mut cp = self.pool.get(tag, Mode::Read)?.lock();
            if page::is_new(cp.page()) {
                page::init_raw(cp.page_mut(), page::KIND_CLOG);
            }
            let status = if commit { XID_COMMITTED } else { XID_ABORTED };
            clog_set(cp.page_mut(), xid, status);
            let rm = if commit { RM_COMMIT } else { RM_ABORT };
            let (_, end) = self.wal.insert(
                rm,
                xid,
                &[RecBlock {
                    tag,
                    page: cp.page(),
                    force_image: false,
                    standard: false,
                }],
                &[],
            );
            page::set_lsn(cp.page_mut(), end);
            cp.mark_dirty();
            end
        };
        if sync {
            self.wal.flush(end)?;
            self.xact.note_durable(xid);
        } else {
            self.wal.write()?;
            self.xact.note_unflushed(end, xid);
        }
        self.xact.end_xact(xid, commit);
        for (i, d) in txn.deltas.iter().enumerate() {
            let (h, t) = (&self.rels[i].heap.stats, &self.rels[i].toast.stats);
            if commit {
                h.ins.fetch_add(d.ins, Ordering::Relaxed);
                h.upd.fetch_add(d.upd, Ordering::Relaxed);
                h.hot_upd.fetch_add(d.hot, Ordering::Relaxed);
                h.del.fetch_add(d.del, Ordering::Relaxed);
                h.live.fetch_add(d.live, Ordering::Relaxed);
                h.dead.fetch_add(d.dead, Ordering::Relaxed);
                h.live_bytes.fetch_add(d.bytes, Ordering::Relaxed);
                t.ins.fetch_add(d.t_new, Ordering::Relaxed);
                t.live.fetch_add(d.t_live, Ordering::Relaxed);
                t.dead.fetch_add(d.t_dead, Ordering::Relaxed);
                t.live_bytes.fetch_add(d.t_bytes, Ordering::Relaxed);
            } else {
                h.dead.fetch_add(d.h_new as i64, Ordering::Relaxed);
                t.dead.fetch_add(d.t_new as i64, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// A checkpoint (PostgreSQL's CreateCheckPoint): move the redo pointer
    /// to the end of the log, write every dirty buffer (each after the WAL
    /// up to its LSN), sync the relation files, log a checkpoint record and
    /// point `pg_control` at it. Writers keep running; a page they change
    /// after the redo pointer logs a full-page image first.
    pub(crate) fn checkpoint(&self, shutdown: bool) -> Result<()> {
        let _g = self.ckpt.lock().unwrap();
        self.checkpoint_locked(shutdown)
    }

    /// The body of `checkpoint`; the caller holds the checkpoint lock.
    fn checkpoint_locked(&self, shutdown: bool) -> Result<()> {
        // `crash` models a power loss, which stops a running checkpoint
        // between two writes instead of letting it finish and sync.
        let halted = || self.halt.load(Ordering::Acquire);
        let stop_at = self.ckpt_stop.load(Ordering::Acquire);
        let step = |n: u8| {
            if halted() || stop_at == n {
                Err(Error::Closed)
            } else {
                Ok(())
            }
        };
        if halted() {
            return Err(Error::Closed);
        }
        let redo = self.wal.begin_checkpoint();
        self.redo_hint.store(redo, Ordering::Release);
        let next_xid = self.xact.next_xid();
        let latest = self.xact.latest_committed();
        step(1)?;
        self.pool.flush_all(&halted)?;
        step(2)?;
        self.pool.sync_all()?;
        step(3)?;
        let (start, end) = self.wal.insert(
            RM_CHECKPOINT,
            0,
            &[],
            &checkpoint_body(redo, next_xid, latest, shutdown),
        );
        self.wal.flush(end)?;
        step(4)?;
        let state = if shutdown {
            STATE_SHUTDOWN
        } else {
            STATE_IN_PRODUCTION
        };
        self.control.lock().unwrap().write(Control {
            seq: 0,
            state,
            checkpoint: start,
            redo,
            next_xid,
            latest_committed: latest,
        })?;
        self.wal.remove_before(redo)?;
        self.last_ckpt_end.store(end, Ordering::Release);
        self.checkpoints.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn request_checkpoint(&self) {
        let mut st = self.bg.state.lock().unwrap();
        st.checkpoint = true;
        self.bg.cv.notify_all();
    }

    /// Writes an openable copy into `dir`, a base backup in PostgreSQL's
    /// terms. It runs a checkpoint and, still holding the checkpoint lock so
    /// that no later checkpoint moves the redo pointer or removes WAL,
    /// copies `pg_control`, the catalog, the relation files and the commit
    /// log, then the WAL. Writers keep running, so a page can change while
    /// its file is copied. Every change after the redo pointer is in the
    /// WAL, which is copied last, and the first change to each page carries
    /// a full-page image. The copied `pg_control` says the database was
    /// running, so opening the copy replays the WAL from the redo pointer,
    /// restores any page caught half-written from its image, and keeps
    /// exactly the transactions whose commit records the copy holds.
    fn base_backup(&self, dir: &Path) -> Result<()> {
        self.check_open()?;
        let tmp = checkpoint::begin(dir)?;
        let res = self
            .copy_files(&tmp)
            .and_then(|()| checkpoint::finish(&tmp, dir));
        if res.is_err() {
            checkpoint::abandon(&tmp);
        }
        res
    }

    fn copy_files(&self, tmp: &Path) -> Result<()> {
        let _g = self.ckpt.lock().unwrap();
        self.checkpoint_locked(false)?;
        for name in ["pg_control", "catalog"] {
            checkpoint::copy_synced(&self.dir.join(name), &tmp.join(name))?;
        }
        for sub in ["base", "pg_xact", "pg_wal"] {
            let (src, dst) = (self.dir.join(sub), tmp.join(sub));
            fs::create_dir(&dst).ctx(|| format!("create {}", dst.display()))?;
            if sub == "pg_wal" {
                self.wal.write()?;
            }
            let mut names = Vec::new();
            for e in fs::read_dir(&src).ctx(|| format!("list {}", src.display()))? {
                let e = e.ctx(|| format!("list {}", src.display()))?;
                if e.file_type()
                    .ctx(|| format!("stat {}", e.path().display()))?
                    .is_file()
                {
                    names.push(e.file_name());
                }
            }
            names.sort();
            for n in names {
                checkpoint::copy_synced(&src.join(&n), &dst.join(&n))?;
            }
        }
        Ok(())
    }

    /// For each range: 0 when the index holds no entry in it; otherwise the
    /// share of index entries between the two ends, from one descent per
    /// end, times the bytes of the column family's heap and TOAST heap.
    fn approximate_sizes(&self, cf: CfId, ranges: &[(&[u8], &[u8])]) -> Result<Vec<u64>> {
        self.check_open()?;
        let r = &self.rels[self.icf(cf)?];
        let bytes = (self.pool.fork_bytes(r.heap.rel, FORK_MAIN)
            + self.pool.fork_bytes(r.toast.rel, FORK_MAIN)) as f64;
        ranges
            .iter()
            .map(|&(start, limit)| {
                if start >= limit || !r.index.has_entries(&self.pool, start, limit)? {
                    return Ok(0);
                }
                let fa = r.index.rank_fraction(&self.pool, start)?;
                let fb = r.index.rank_fraction(&self.pool, limit)?;
                Ok((((fb - fa).max(0.0) * bytes) as u64).max(1))
            })
            .collect()
    }

    fn start_bg(self: &Arc<Self>) {
        let me = self.clone();
        let h = std::thread::Builder::new()
            .name("pgheap-checkpointer".into())
            .spawn(move || me.checkpointer())
            .expect("spawn the checkpointer thread");
        self.bg.threads.lock().unwrap().push(h);
        let me = self.clone();
        let h = std::thread::Builder::new()
            .name("pgheap-walwriter".into())
            .spawn(move || me.wal_writer())
            .expect("spawn the WAL writer thread");
        self.bg.threads.lock().unwrap().push(h);
        if self.opts.autovacuum {
            let me = self.clone();
            let h = std::thread::Builder::new()
                .name("pgheap-autovacuum".into())
                .spawn(move || me.autovacuum())
                .expect("spawn the autovacuum thread");
            self.bg.threads.lock().unwrap().push(h);
        }
    }

    /// The checkpointer: runs a checkpoint when a writer asks (the WAL grew
    /// by `checkpoint_wal_bytes`) or after `CHECKPOINT_TIMEOUT` with new WAL.
    fn checkpointer(&self) {
        let mut last = Instant::now();
        loop {
            let mut st = self.bg.state.lock().unwrap();
            loop {
                if st.stop {
                    return;
                }
                let timed_out = last.elapsed() >= CHECKPOINT_TIMEOUT
                    && self.wal.insert_lsn() > self.last_ckpt_end.load(Ordering::Acquire);
                if st.checkpoint || timed_out {
                    break;
                }
                st = self
                    .bg
                    .cv
                    .wait_timeout(st, Duration::from_millis(500))
                    .unwrap()
                    .0;
            }
            st.checkpoint = false;
            drop(st);
            if let Err(e) = self.checkpoint(false) {
                if !self.halt.load(Ordering::Acquire) {
                    self.poison(e);
                }
                return;
            }
            last = Instant::now();
        }
    }

    /// The WAL writer (PostgreSQL's walwriter): syncs the WAL every
    /// `WAL_WRITER_DELAY`, so unsynced commits reach disk within that time
    /// and a dirty page chosen for eviction rarely waits for a WAL sync.
    fn wal_writer(&self) {
        loop {
            {
                let st = self.bg.state.lock().unwrap();
                if st.stop {
                    return;
                }
                let st = self.bg.cv.wait_timeout(st, WAL_WRITER_DELAY).unwrap().0;
                if st.stop {
                    return;
                }
            }
            let target = self.wal.insert_lsn();
            if target > self.wal.flushed_lsn()
                && let Err(e) = self.wal.flush(target)
            {
                if !matches!(e, Error::Closed) && !self.halted() {
                    self.poison(e);
                }
                return;
            }
        }
    }

    fn stop_bg(&self) {
        {
            let mut st = self.bg.state.lock().unwrap();
            st.stop = true;
            self.bg.cv.notify_all();
        }
        let threads = std::mem::take(&mut *self.bg.threads.lock().unwrap());
        for t in threads {
            let _ = t.join();
        }
    }

    fn save_stats(&self) -> Result<()> {
        let rows: Vec<Vec<u64>> = self
            .rels
            .iter()
            .flat_map(|r| [r.heap.stats.row(), r.toast.stats.row()])
            .collect();
        write_stats(&self.dir, &rows)
    }

    fn close(&self) -> Result<()> {
        if self
            .state
            .compare_exchange(OPEN, CLOSED, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        self.stop_bg();
        let w = self.writer.lock().unwrap();
        let res = if w.is_none() {
            self.checkpoint(true).and_then(|()| self.save_stats())
        } else {
            Ok(())
        };
        drop(w);
        *self.lock_file.lock().unwrap() = None;
        res
    }

    fn crash(&self, mode: CrashMode) -> Result<()> {
        if self
            .state
            .compare_exchange(OPEN, CLOSED, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::Closed);
        }
        self.halt.store(true, Ordering::Release);
        self.wal.halt();
        self.stop_bg();
        let w = self.writer.lock().unwrap();
        let res = self.wal.crash(mode).and_then(|()| self.pool.crash(mode));
        drop(w);
        *self.lock_file.lock().unwrap() = None;
        res
    }

    fn property(&self, cf: CfId, name: &str) -> Option<String> {
        let icf = self.icf(cf).ok()?;
        let r = &self.rels[icf];
        let s = &r.heap.stats;
        let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let v = match name {
            "rocksdb.estimate-num-keys" | "nil.pgheap.n-live-tup" => {
                s.live.load(Ordering::Relaxed).max(0).to_string()
            }
            "rocksdb.num-snapshots" => self.xact.user_snapshots.load(Ordering::Relaxed).to_string(),
            "rocksdb.block-cache-usage" => (self.pool.used_frames() * BLCKSZ).to_string(),
            "rocksdb.block-cache-capacity" => (self.pool.num_frames() * BLCKSZ).to_string(),
            "nil.latest-sequence" => self.xact.latest_committed().to_string(),
            "nil.pgheap.n-dead-tup" => s.dead.load(Ordering::Relaxed).max(0).to_string(),
            "nil.pgheap.n-tup-ins" => ld(&s.ins).to_string(),
            "nil.pgheap.n-tup-upd" => ld(&s.upd).to_string(),
            "nil.pgheap.n-tup-hot-upd" => ld(&s.hot_upd).to_string(),
            "nil.pgheap.n-tup-del" => ld(&s.del).to_string(),
            "nil.pgheap.heap-bytes" => self.pool.fork_bytes(r.heap.rel, FORK_MAIN).to_string(),
            "nil.pgheap.index-bytes" => self.pool.fork_bytes(r.index.rel, FORK_MAIN).to_string(),
            "nil.pgheap.toast-bytes" => (self.pool.fork_bytes(r.toast.rel, FORK_MAIN)
                + self.pool.fork_bytes(r.toast_index.rel, FORK_MAIN))
            .to_string(),
            "nil.pgheap.wal-bytes" => ld(&self.wal.stats.bytes).to_string(),
            "nil.pgheap.checkpoints" => ld(&self.checkpoints).to_string(),
            "nil.pgheap.next-xid" => self.xact.next_xid().to_string(),
            "nil.pgheap.index-depth" => r.index.depth().to_string(),
            "nil.pgheap.index-entries" => r.index.count_entries(&self.pool).ok()?.to_string(),
            "nil.pgheap.vacuum-count" => ld(&s.vacuum_count).to_string(),
            "nil.pgheap.autovacuum-count" => ld(&s.autovacuum_count).to_string(),
            "nil.pgheap.pruned" => ld(&s.pruned).to_string(),
            "nil.pgheap.live-bytes" => s.live_bytes.load(Ordering::Relaxed).max(0).to_string(),
            "nil.pgheap.bloat-pct" => format!("{:.1}", self.bloat_pct(icf)),
            "nil.pgheap.all-visible-pages" => vm::count(&self.pool, r.heap.rel).ok()?.to_string(),
            "nil.pgheap.toast-n-live-tup" => r.toast.stats.live.load(Ordering::Relaxed).to_string(),
            "nil.pgheap.toast-n-dead-tup" => r.toast.stats.dead.load(Ordering::Relaxed).to_string(),
            "nil.pgheap.wal-disk-bytes" => self.wal.disk_bytes().to_string(),
            "nil.pgheap.buffer-hit-ratio" => format!("{:.4}", self.hit_ratio()),
            "nil.pgheap.oldest-xmin" => self.xact.oldest_xmin().to_string(),
            "nil.pgheap.registered-snapshots" => self.xact.registered_snapshots().to_string(),
            "rocksdb.stats" | "nil.stats" => self.stats_text(icf),
            _ => return None,
        };
        Some(v)
    }

    /// Share of the heap not taken by live tuple bytes: dead versions,
    /// free space, fillfactor reserve and page overhead.
    fn bloat_pct(&self, icf: usize) -> f64 {
        let r = &self.rels[icf];
        let heap = self.pool.fork_bytes(r.heap.rel, FORK_MAIN) as f64;
        if heap == 0.0 {
            return 0.0;
        }
        let live = r.heap.stats.live_bytes.load(Ordering::Relaxed).max(0) as f64;
        (100.0 * (heap - live) / heap).max(0.0)
    }

    fn hit_ratio(&self) -> f64 {
        let p = &self.pool.stats;
        let hits = p.hits.load(Ordering::Relaxed) as f64;
        let reads = p.reads.load(Ordering::Relaxed) as f64;
        if hits + reads == 0.0 {
            1.0
        } else {
            hits / (hits + reads)
        }
    }

    fn stats_text(&self, icf: usize) -> String {
        let r = &self.rels[icf];
        let s = &r.heap.stats;
        let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let p = &self.pool.stats;
        format!(
            "column family {}: live {}, dead {}, inserted {}, updated {} ({} HOT), deleted {}\n\
             vacuum {} (autovacuum {}), pruned {} tuples, bloat {:.1}%\n\
             heap {} bytes, index {} bytes (depth {}), toast {} bytes\n\
             wal {} bytes in {} records, {} full-page images, {} flushes; checkpoints {}\n\
             buffers {} of {} used ({} dirty), hit ratio {:.4}, {} reads, {} writes\n",
            r.name,
            s.live.load(Ordering::Relaxed),
            s.dead.load(Ordering::Relaxed),
            ld(&s.ins),
            ld(&s.upd),
            ld(&s.hot_upd),
            ld(&s.del),
            ld(&s.vacuum_count),
            ld(&s.autovacuum_count),
            ld(&s.pruned),
            self.bloat_pct(icf),
            self.pool.fork_bytes(r.heap.rel, FORK_MAIN),
            self.pool.fork_bytes(r.index.rel, FORK_MAIN),
            r.index.depth(),
            self.pool.fork_bytes(r.toast.rel, FORK_MAIN)
                + self.pool.fork_bytes(r.toast_index.rel, FORK_MAIN),
            ld(&self.wal.stats.bytes),
            ld(&self.wal.stats.records),
            ld(&self.wal.stats.fpis),
            ld(&self.wal.stats.flushes),
            ld(&self.checkpoints),
            self.pool.used_frames(),
            self.pool.num_frames(),
            self.pool.dirty_frames(),
            self.hit_ratio(),
            ld(&p.reads),
            ld(&p.writes),
        )
    }
}

impl PgHeapEngine {
    /// Tests only: applies `batch` and logs it, flushed, without committing,
    /// as a transaction still running at a crash would leave it. The engine
    /// must be crashed next.
    #[doc(hidden)]
    pub fn test_write_uncommitted(&self, batch: &WriteBatch) -> Result<()> {
        let i = &self.inner;
        i.check_open()?;
        batch.validate(i.cf_map.len(), MAX_KEY_LEN, MAX_VALUE_LEN)?;
        let _w = i.writer.lock().unwrap();
        let mut txn = Txn::new(i);
        batch.iter().try_for_each(|op| txn.apply(op))?;
        i.wal.flush(i.wal.insert_lsn())
    }

    /// Tests only: runs a checkpoint that stops before step `step` (1: after
    /// moving the redo pointer, 2: after writing the buffers, 3: after
    /// syncing the relation files, 4: after logging the checkpoint record,
    /// before `pg_control` names it), then crashes with `mode`.
    #[doc(hidden)]
    pub fn test_crash_during_checkpoint(&self, step: u8, mode: CrashMode) -> Result<()> {
        let i = &self.inner;
        i.check_open()?;
        i.ckpt_stop.store(step, Ordering::Release);
        let r = i.checkpoint(false);
        i.ckpt_stop.store(0, Ordering::Release);
        if !matches!(r, Err(Error::Closed)) {
            return Err(Error::invalid(format!(
                "checkpoint did not stop at step {step}: {r:?}"
            )));
        }
        i.crash(mode)
    }

    /// Tests only: the main-fork file of column family `cf`'s heap, or of
    /// its TOAST heap.
    #[doc(hidden)]
    pub fn test_heap_path(&self, cf: CfId, toast: bool) -> Result<PathBuf> {
        let i = &self.inner;
        let r = &i.rels[i.icf(cf)?];
        let rel = if toast { r.toast.rel } else { r.heap.rel };
        Ok(i.dir.join("base").join(rel.to_string()))
    }
}

/// Per-column-family counter changes of one transaction, applied at commit.
#[derive(Default, Clone, Copy)]
struct Delta {
    ins: u64,
    upd: u64,
    hot: u64,
    del: u64,
    live: i64,
    dead: i64,
    bytes: i64,
    /// Heap tuples this transaction created; dead if it aborts.
    h_new: u64,
    t_new: u64,
    t_live: i64,
    t_dead: i64,
    t_bytes: i64,
}

/// One write batch in progress. The xid is assigned at the first change,
/// so a batch that changes nothing commits nothing.
struct Txn<'a> {
    e: &'a Inner,
    xid: Option<u64>,
    toast_seq: u32,
    deltas: Vec<Delta>,
    /// The batch failed on its own data (a bad merge), not on I/O.
    logical: bool,
}

impl<'a> Txn<'a> {
    fn new(e: &'a Inner) -> Txn<'a> {
        Txn {
            e,
            xid: None,
            toast_seq: 0,
            deltas: vec![Delta::default(); e.rels.len()],
            logical: false,
        }
    }

    fn xid(&mut self) -> u64 {
        match self.xid {
            Some(x) => x,
            None => {
                let x = self.e.xact.assign_xid();
                self.xid = Some(x);
                x
            }
        }
    }

    fn apply(&mut self, op: BatchOp<'_>) -> Result<()> {
        let icf = self.e.cf_map[op.cf() as usize];
        match op {
            BatchOp::Put { key, value, .. } => {
                let old = self.find_live(icf, key)?;
                self.write_version(icf, key, value, old)
            }
            BatchOp::Delete { key, .. } => {
                if let Some(f) = self.find_live(icf, key)? {
                    self.delete_found(icf, f)?;
                }
                Ok(())
            }
            BatchOp::DeleteRange { start, end, .. } => self.delete_range(icf, start, end),
            BatchOp::Merge { key, operand, .. } => {
                let old = self.find_live(icf, key)?;
                let cur = match &old {
                    Some(f) => Some(self.e.detoast(icf, &f.tuple)?),
                    None => None,
                };
                let v = match i64add(cur.as_deref(), operand) {
                    Ok(v) => v,
                    Err(e) => {
                        self.logical = true;
                        return Err(e);
                    }
                };
                self.write_version(icf, key, &v, old)
            }
        }
    }

    /// The version of `key` the writer sees: committed and not deleted, or
    /// written by this transaction.
    fn find_live(&self, icf: usize, key: &[u8]) -> Result<Option<Found>> {
        let e = self.e;
        let r = &e.rels[icf];
        let tids = r.index.lookup(&e.pool, key)?;
        if tids.is_empty() {
            return Ok(None);
        }
        let vis = e.vis_ctx();
        let view = View::Writer(self.xid);
        for tid in tids {
            if let Some(f) = heap::fetch_chain(&e.pool, r.heap.rel, tid, key, &vis, &view)? {
                return Ok(Some(f));
            }
        }
        Ok(None)
    }

    fn write_version(
        &mut self,
        icf: usize,
        key: &[u8],
        value: &[u8],
        old: Option<Found>,
    ) -> Result<()> {
        let xid = self.xid();
        let (stored, external) = self.maybe_toast(icf, key, value)?;
        let e = self.e;
        let r = &e.rels[icf];
        match old {
            None => {
                let t = heap::build(xid, key, &stored, external, 0, false);
                let tid = self.heap_insert(&r.heap, &t, xid)?;
                r.index.insert(&e.pool, key, tid)?;
                let d = &mut self.deltas[icf];
                d.ins += 1;
                d.live += 1;
                d.bytes += t.len() as i64;
                d.h_new += 1;
            }
            Some(f) => {
                let t = heap::build(xid, key, &stored, external, 0, true);
                let new_len = t.len() as i64;
                let (tid, hot) = self.heap_update(&r.heap, f.tid, t, xid)?;
                if !hot {
                    r.index.insert(&e.pool, key, tid)?;
                }
                if heap::is_external(&f.tuple) {
                    self.toast_delete(icf, heap::value(&f.tuple))?;
                }
                let d = &mut self.deltas[icf];
                d.upd += 1;
                d.hot += hot as u64;
                d.dead += 1;
                d.bytes += new_len - f.tuple.len() as i64;
                d.h_new += 1;
            }
        }
        Ok(())
    }

    fn delete_found(&mut self, icf: usize, f: Found) -> Result<()> {
        let xid = self.xid();
        let e = self.e;
        self.heap_delete(&e.rels[icf].heap, f.tid, xid)?;
        if heap::is_external(&f.tuple) {
            self.toast_delete(icf, heap::value(&f.tuple))?;
        }
        let d = &mut self.deltas[icf];
        d.del += 1;
        d.live -= 1;
        d.dead += 1;
        d.bytes -= f.tuple.len() as i64;
        Ok(())
    }

    /// Deletes every live key in `[start, end)` one version at a time:
    /// PostgreSQL has no range tombstone, so this costs O(keys in range).
    fn delete_range(&mut self, icf: usize, start: &[u8], end: &[u8]) -> Result<()> {
        if start >= end {
            return Ok(());
        }
        let e = self.e;
        let r = &e.rels[icf];
        let mut from = start.to_vec();
        let mut inclusive = true;
        loop {
            let (groups, done) = r.index.scan_forward(&e.pool, &from, inclusive, 256)?;
            let mut stop = done;
            for g in &groups {
                if g.key.as_slice() >= end {
                    stop = true;
                    break;
                }
                let vis = e.vis_ctx();
                let view = View::Writer(self.xid);
                let mut live = None;
                for &tid in &g.tids {
                    if let Some(f) =
                        heap::fetch_chain(&e.pool, r.heap.rel, tid, &g.key, &vis, &view)?
                    {
                        live = Some(f);
                        break;
                    }
                }
                if let Some(f) = live {
                    self.delete_found(icf, f)?;
                }
            }
            match groups.last() {
                Some(g) if !stop => {
                    from = g.key.clone();
                    inclusive = false;
                }
                _ => return Ok(()),
            }
        }
    }

    /// Stores a value that would make the tuple longer than
    /// `TOAST_THRESHOLD` in the TOAST heap, one chunk tuple per
    /// `TOAST_CHUNK` bytes, and returns the pointer to keep inline.
    fn maybe_toast(&mut self, icf: usize, key: &[u8], value: &[u8]) -> Result<(Vec<u8>, bool)> {
        if TUPLE_HEADER + key.len() + value.len() <= TOAST_THRESHOLD || value.len() <= TOAST_PTR_LEN
        {
            return Ok((value.to_vec(), false));
        }
        let xid = self.xid();
        let seq = self.toast_seq;
        if seq >= 1 << 24 {
            return Err(Error::invalid(
                "a batch may store at most 16,777,216 values out of line",
            ));
        }
        self.toast_seq += 1;
        // Unique for ever: xids are never reused.
        let value_id = (xid << 24) | seq as u64;
        let e = self.e;
        let r = &e.rels[icf];
        let mut chunks = 0u32;
        for (i, c) in value.chunks(TOAST_CHUNK).enumerate() {
            let k = chunk_key(value_id, i as u32);
            let t = heap::build(xid, &k, c, false, 0, false);
            let tid = self.heap_insert(&r.toast, &t, xid)?;
            r.toast_index.insert(&e.pool, &k, tid)?;
            let d = &mut self.deltas[icf];
            d.t_new += 1;
            d.t_live += 1;
            d.t_bytes += t.len() as i64;
            chunks += 1;
        }
        Ok((toast_ptr(value_id, value.len() as u32, chunks), true))
    }

    /// Marks the chunks of a value deleted with this transaction's xid, as
    /// PostgreSQL's toast_delete_datum does when their tuple is updated or
    /// deleted.
    fn toast_delete(&mut self, icf: usize, ptr: &[u8]) -> Result<()> {
        let (value_id, _, chunks) = parse_toast_ptr(ptr)?;
        let xid = self.xid();
        let e = self.e;
        let r = &e.rels[icf];
        let (groups, _) =
            r.toast_index
                .scan_forward(&e.pool, &chunk_key(value_id, 0), true, chunks as usize)?;
        let vis = e.vis_ctx();
        let view = View::Writer(Some(xid));
        for g in groups {
            if g.key.len() != 12 || g.key[..8] != value_id.to_be_bytes() {
                break;
            }
            for tid in g.tids {
                if let Some(f) = heap::fetch_chain(&e.pool, r.toast.rel, tid, &g.key, &vis, &view)?
                {
                    self.heap_delete(&r.toast, f.tid, xid)?;
                    let d = &mut self.deltas[icf];
                    d.t_live -= 1;
                    d.t_dead += 1;
                    d.t_bytes -= f.tuple.len() as i64;
                    break;
                }
            }
        }
        Ok(())
    }

    /// Opportunistic pruning (PostgreSQL's heap_page_prune_opt): when a page
    /// the writer wants to use is short of `need` bytes and its prune hint
    /// says some deleted version may now be dead to every snapshot, prune it
    /// first.
    fn maybe_prune(&self, rel: &HeapRel, lp: &mut Locked<'_>, need: usize) -> Result<()> {
        let px = page::prune_xid(lp.page());
        if px == 0 || page::heap_free_space(lp.page(), MAX_HEAP_TUPLES) >= need {
            return Ok(());
        }
        let horizon = self.e.xact.oldest_xmin();
        if px >= horizon {
            return Ok(());
        }
        let vis = self.e.vis_ctx();
        self.e.prune_locked(rel, lp, &vis, horizon, self.xid)?;
        Ok(())
    }

    /// A page of `rel` with room for a tuple of `len` bytes plus the space
    /// fillfactor keeps free, locked exclusively: the insert target, a page
    /// the free-space map offers, or a new block at the end.
    fn page_for(&self, rel: &HeapRel, len: usize, avoid: Option<u32>) -> Result<Locked<'a>> {
        let pool = &self.e.pool;
        let fill = self.e.opts.fillfactor.clamp(10, 100);
        let need = len + BLCKSZ * (100 - fill) / 100;
        let nblocks = pool.nblocks(rel.rel, FORK_MAIN)?;
        let t = rel.target.load(Ordering::Relaxed);
        let usable = |lp: &mut Locked<'_>| -> Result<bool> {
            if page::is_new(lp.page()) {
                return Ok(false);
            }
            self.maybe_prune(rel, lp, need)?;
            let free = page::heap_free_space(lp.page(), MAX_HEAP_TUPLES);
            if free >= need {
                return Ok(true);
            }
            fsm::record(pool, rel.rel, lp.tag().block, free)?;
            Ok(false)
        };
        if t != u32::MAX && Some(t) != avoid && t < nblocks {
            let mut lp = pool.get(rel.tag(t), Mode::Read)?.lock();
            if usable(&mut lp)? {
                return Ok(lp);
            }
        }
        for _ in 0..8 {
            let Some(b) = fsm::search(pool, rel.rel, need, nblocks, &rel.fsm_hint, avoid)? else {
                break;
            };
            let mut lp = pool.get(rel.tag(b), Mode::Read)?.lock();
            if usable(&mut lp)? {
                rel.target.store(b, Ordering::Relaxed);
                return Ok(lp);
            }
            if page::is_new(lp.page()) {
                fsm::record(pool, rel.rel, b, 0)?;
            }
        }
        let b = pool.extend(rel.rel, FORK_MAIN)?;
        let mut lp = pool.get(rel.tag(b), Mode::Zero)?.lock();
        heap::init_page(lp.page_mut());
        rel.target.store(b, Ordering::Relaxed);
        Ok(lp)
    }

    /// Clears the visibility-map bit of a page that was all-visible before
    /// this change.
    fn clear_vm(&self, rel: &HeapRel, block: u32, was_all_visible: bool) -> Result<()> {
        if was_all_visible {
            vm::set(&self.e.pool, rel.rel, block, false)?;
        }
        Ok(())
    }

    fn heap_insert(&self, rel: &HeapRel, t: &[u8], xid: u64) -> Result<Tid> {
        let mut lp = self.page_for(rel, t.len(), None)?;
        let block = lp.tag().block;
        let was = all_visible(lp.page());
        let off = heap::place(lp.page_mut(), block, t, None)
            .ok_or_else(|| Error::corruption("heap page lost its free space"))?;
        let mut main = Vec::with_capacity(3 + t.len());
        main.extend_from_slice(&off.to_le_bytes());
        main.push(0);
        main.extend_from_slice(page::item(lp.page(), off));
        let (_, end) = self
            .e
            .wal
            .insert(RM_HEAP_INSERT, xid, &[rec_block(&lp)], &main);
        page::set_lsn(lp.page_mut(), end);
        lp.mark_dirty();
        drop(lp);
        self.clear_vm(rel, block, was)?;
        Ok(Tid { block, off })
    }

    /// Writes the new version `t` of the tuple at `old`. When it fits on
    /// the same page it becomes a heap-only tuple and the update is HOT: no
    /// index entry, the old version marked HEAP_HOT_UPDATED. Otherwise it
    /// goes to another page and the caller adds an index entry. Returns the
    /// new tid and whether the update was HOT.
    fn heap_update(
        &self,
        rel: &HeapRel,
        old: Tid,
        mut t: Vec<u8>,
        xid: u64,
    ) -> Result<(Tid, bool)> {
        let pool = &self.e.pool;
        let wal = &self.e.wal;
        let mut op = pool.get(rel.tag(old.block), Mode::Read)?.lock();
        let old_was = all_visible(op.page());
        self.maybe_prune(rel, &mut op, t.len())?;
        if page::heap_free_space(op.page(), MAX_HEAP_TUPLES) >= t.len() {
            heap::set_heap_only(&mut t);
            let off = heap::place(op.page_mut(), old.block, &t, None)
                .ok_or_else(|| Error::corruption("heap page lost its free space"))?;
            let new = Tid {
                block: old.block,
                off,
            };
            heap::mark_updated(op.page_mut(), old.off, xid, new, true);
            let main = update_body(old.off, off, xid, page::item(op.page(), off));
            let (_, end) = wal.insert(RM_HEAP_HOT_UPDATE, xid, &[rec_block(&op)], &main);
            page::set_lsn(op.page_mut(), end);
            op.mark_dirty();
            drop(op);
            self.clear_vm(rel, old.block, old_was)?;
            return Ok((new, true));
        }
        let mut np = self.page_for(rel, t.len(), Some(old.block))?;
        let nb = np.tag().block;
        let new_was = all_visible(np.page());
        let off = heap::place(np.page_mut(), nb, &t, None)
            .ok_or_else(|| Error::corruption("heap page lost its free space"))?;
        let new = Tid { block: nb, off };
        heap::mark_updated(op.page_mut(), old.off, xid, new, false);
        let main = update_body(old.off, off, xid, page::item(np.page(), off));
        let (_, end) = wal.insert(
            RM_HEAP_UPDATE,
            xid,
            &[rec_block(&np), rec_block(&op)],
            &main,
        );
        for l in [&mut np, &mut op] {
            page::set_lsn(l.page_mut(), end);
            l.mark_dirty();
        }
        drop(np);
        drop(op);
        self.clear_vm(rel, nb, new_was)?;
        self.clear_vm(rel, old.block, old_was)?;
        Ok((new, false))
    }

    fn heap_delete(&self, rel: &HeapRel, tid: Tid, xid: u64) -> Result<()> {
        let mut lp = self.e.pool.get(rel.tag(tid.block), Mode::Read)?.lock();
        let was = all_visible(lp.page());
        heap::mark_deleted(lp.page_mut(), tid.block, tid.off, xid);
        let mut main = Vec::with_capacity(10);
        main.extend_from_slice(&tid.off.to_le_bytes());
        main.extend_from_slice(&xid.to_le_bytes());
        let (_, end) = self
            .e
            .wal
            .insert(RM_HEAP_DELETE, xid, &[rec_block(&lp)], &main);
        page::set_lsn(lp.page_mut(), end);
        lp.mark_dirty();
        drop(lp);
        self.clear_vm(rel, tid.block, was)
    }
}

fn all_visible(p: &[u8]) -> bool {
    page::flags(p) & page::PD_ALL_VISIBLE != 0
}

fn update_body(old_off: u16, new_off: u16, xid: u64, tuple: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(12 + tuple.len());
    b.extend_from_slice(&old_off.to_le_bytes());
    b.extend_from_slice(&new_off.to_le_bytes());
    b.extend_from_slice(&xid.to_le_bytes());
    b.extend_from_slice(tuple);
    b
}

impl Engine for PgHeapEngine {
    fn kind(&self) -> EngineKind {
        EngineKind::PgHeap
    }

    fn column_families(&self) -> Vec<String> {
        self.inner.cf_names.clone()
    }

    fn max_key_len(&self) -> usize {
        MAX_KEY_LEN
    }

    fn max_value_len(&self) -> usize {
        MAX_VALUE_LEN
    }

    fn write(&self, batch: &WriteBatch, opts: WriteOptions) -> Result<()> {
        self.inner.write(batch, opts)
    }

    fn get(&self, cf: CfId, key: &[u8], snapshot: Option<&Snapshot>) -> Result<Option<Vec<u8>>> {
        let i = &self.inner;
        i.check_open()?;
        let icf = i.icf(cf)?;
        match snapshot {
            Some(s) => i.read_key(icf, key, &i.user_snapshot(s)?.data),
            None => {
                let s = i.new_snapshot(false);
                i.read_key(icf, key, &s.data)
            }
        }
    }

    fn multi_get(
        &self,
        cf: CfId,
        keys: &[&[u8]],
        snapshot: Option<&Snapshot>,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let i = &self.inner;
        i.check_open()?;
        let icf = i.icf(cf)?;
        let own;
        let data = match snapshot {
            Some(s) => &i.user_snapshot(s)?.data,
            None => {
                own = i.new_snapshot(false);
                &own.data
            }
        };
        keys.iter().map(|k| i.read_key(icf, k, data)).collect()
    }

    fn snapshot(&self) -> Result<Snapshot> {
        let i = &self.inner;
        i.check_open()?;
        let s = i.new_snapshot(true);
        Ok(Snapshot::new(s.data.seq, Arc::new(s)))
    }

    fn iter(&self, cf: CfId, opts: IterOptions) -> Result<Box<dyn DbIterator>> {
        let i = &self.inner;
        i.check_open()?;
        let icf = i.icf(cf)?;
        let snap = match opts.snapshot {
            Some(s) => {
                i.user_snapshot(&s)?;
                s
            }
            None => {
                let s = i.new_snapshot(false);
                Snapshot::new(s.data.seq, Arc::new(s))
            }
        };
        Ok(Box::new(PgIter::new(
            self.inner.clone(),
            icf,
            snap,
            opts.lower_bound,
            opts.upper_bound,
        )))
    }

    fn flush(&self) -> Result<()> {
        let i = &self.inner;
        i.check_open()?;
        if let Some(e) = &*i.writer.lock().unwrap() {
            return Err(e.clone());
        }
        i.checkpoint(false)
    }

    fn flush_wal(&self, sync: bool) -> Result<()> {
        let i = &self.inner;
        i.check_open()?;
        if let Some(e) = &*i.writer.lock().unwrap() {
            return Err(e.clone());
        }
        if sync {
            i.wal.flush(i.wal.insert_lsn())
        } else {
            i.wal.write()
        }
    }

    /// Runs VACUUM on the whole column family (heap and TOAST heap); the
    /// range is ignored, as PostgreSQL's VACUUM works on whole relations.
    fn compact_range(&self, cf: CfId, _start: Option<&[u8]>, _end: Option<&[u8]>) -> Result<()> {
        let i = &self.inner;
        i.check_open()?;
        let icf = i.icf(cf)?;
        i.vacuum(icf, false).map(|_| ())
    }

    fn property(&self, cf: CfId, name: &str) -> Option<String> {
        self.inner.property(cf, name)
    }

    fn latest_sequence(&self) -> u64 {
        self.inner.xact.latest_committed()
    }

    fn checkpoint(&self, dir: &Path) -> Result<()> {
        self.inner.base_backup(dir)
    }

    fn approximate_sizes(&self, cf: CfId, ranges: &[(&[u8], &[u8])]) -> Result<Vec<u64>> {
        self.inner.approximate_sizes(cf, ranges)
    }

    fn close(&self) -> Result<()> {
        self.inner.close()
    }

    fn crash(&self, mode: CrashMode) -> Result<()> {
        self.inner.crash(mode)
    }
}

impl Drop for PgHeapEngine {
    fn drop(&mut self) {
        let _ = self.inner.close();
    }
}
