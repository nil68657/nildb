//! The B+ tree engine: commits, the two meta pages, snapshots, free-page
//! reuse and recovery.
//!
//! A commit writes the transaction's new pages, then the meta page naming
//! the new roots. It writes the meta into the slot that does not hold the
//! last durable meta, so a torn or lost meta write leaves the durable one
//! intact. A synced commit syncs before and after the meta write (LMDB's
//! order). An unsynced commit syncs nothing; to keep the last durable tree
//! recoverable after a power loss, pages freed since the last sync are not
//! reused until the next sync, and the engine syncs on its own once
//! `max_unsynced_free_pages` pages are waiting.
//!
//! Open reads both meta pages and walks the tree of the newer one, checking
//! every page's checksum and the writer transaction its parent expects. If a
//! page is missing or stale it falls back to the older meta. The walk also
//! yields the free pages: every page below the high-water mark that the
//! chosen tree does not reach.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use nilengine_api::{
    BatchOp, CfId, CrashMode, DbIterator, Engine, EngineKind, Error, IoContext, IterOptions,
    Options, Result, Snapshot, WriteBatch, WriteOptions, checkpoint, i64add,
};

use crate::cursor::Cursor;
use crate::page::{
    HEADER, META_MAGIC, Meta, MetaCf, TYPE_BRANCH, branch_child, max_key_len, page_type,
};
use crate::pager::Pager;
use crate::tree::{self, Allocator, ReaderTable, Root, TreeState, TxnOutput, WriteTxn};

pub const DATA_FILE: &str = "data.nbt";
/// Longest value the B+ tree accepts.
pub const MAX_VALUE_LEN: usize = 1 << 30;

const OPEN: u8 = 0;
const CLOSED: u8 = 1;

static NEXT_DB_ID: AtomicU64 = AtomicU64::new(1);

struct BtSnapshot {
    state: Arc<TreeState>,
    db: u64,
    count: Arc<AtomicUsize>,
}

impl Drop for BtSnapshot {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

struct Writer {
    alloc: Allocator,
    durable_txn: u64,
    /// Meta slot holding the last durable meta.
    durable_slot: u64,
    /// Meta slot written by the last commit.
    last_slot: u64,
    unsynced_freed: usize,
    /// Column family names in meta order.
    cf_names: Vec<String>,
    poisoned: Option<Error>,
}

#[derive(Default)]
struct Stats {
    commits: AtomicU64,
    synced_commits: AtomicU64,
    auto_syncs: AtomicU64,
    pages_written: AtomicU64,
}

struct Inner {
    id: u64,
    opts: Options,
    page_size: usize,
    cf_names: Vec<String>,
    /// `CfId` to meta position.
    cf_map: Vec<usize>,
    lock_file: Mutex<Option<File>>,
    pager: Arc<Pager>,
    writer: Mutex<Writer>,
    current: RwLock<Arc<TreeState>>,
    readers: Arc<ReaderTable>,
    snapshots: Arc<AtomicUsize>,
    state: AtomicU8,
    stats: Stats,
}

pub struct BTreeEngine {
    inner: Arc<Inner>,
}

fn sync_dir(dir: &Path) -> Result<()> {
    let d = File::open(dir).ctx(|| format!("open directory {}", dir.display()))?;
    match d.sync_all() {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::InvalidInput || e.raw_os_error() == Some(45) => {
            Ok(())
        }
        Err(e) => Err(Error::io(format!("sync directory {}", dir.display()), &e)),
    }
}

/// Finds the page size (recorded in the meta pages) and decodes both
/// metas. Tries the size the first meta claims, then every legal size, so a
/// torn first meta does not hide the second.
fn read_metas(pager: &mut Pager) -> Result<(usize, Vec<(u64, Meta)>)> {
    pager.set_page_size(512);
    let head = pager.read_raw(0)?;
    let mut sizes = Vec::new();
    if u64::from_le_bytes(head[HEADER..HEADER + 8].try_into().unwrap()) == META_MAGIC {
        let claimed =
            u32::from_le_bytes(head[HEADER + 12..HEADER + 16].try_into().unwrap()) as usize;
        if (512..=65536).contains(&claimed) && claimed.is_power_of_two() {
            sizes.push(claimed);
        }
    }
    sizes.extend((9..=16).map(|s| 1usize << s));
    sizes.dedup();
    for ps in sizes {
        pager.set_page_size(ps);
        let mut metas = Vec::new();
        for slot in 0..2 {
            let p = pager.read_raw(slot)?;
            if let Some(m) = Meta::decode(&p, slot)
                && m.page_size as usize == ps
            {
                metas.push((slot, m));
            }
        }
        if !metas.is_empty() {
            return Ok((ps, metas));
        }
    }
    Err(Error::corruption("no intact meta page"))
}

/// Opens (creating if allowed) a B+ tree database in `dir`; `CfId` n names
/// `cfs[n]`.
pub fn open(dir: &Path, cfs: &[&str], opts: &Options) -> Result<BTreeEngine> {
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
    let lock_path = dir.join("LOCK");
    let lock_file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .ctx(|| format!("open {}", lock_path.display()))?;
    match lock_file.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            return Err(Error::Busy(format!(
                "{} is locked by another open",
                dir.display()
            )));
        }
        Err(TryLockError::Error(e)) => {
            return Err(Error::io(format!("lock {}", lock_path.display()), &e));
        }
    }

    let path = dir.join(DATA_FILE);
    let exists = fs::metadata(&path).map(|m| m.len() > 0).unwrap_or(false);
    if !exists && !opts.create_if_missing {
        return Err(Error::NotFound(format!(
            "{} holds no database",
            dir.display()
        )));
    }
    let mut pager = Pager::open(
        &path,
        opts.page_size,
        opts.cache_bytes,
        opts.track_unsynced_writes,
        true,
    )?;
    if !exists {
        let meta = Meta {
            txn: 1,
            page_size: opts.page_size as u32,
            num_pages: 2,
            cfs: cfs
                .iter()
                .map(|n| MetaCf {
                    name: n.to_string(),
                    root: 0,
                    root_txn: 0,
                    keys: 0,
                })
                .collect(),
        };
        pager.write_pages(
            Vec::new(),
            vec![(0, meta.encode(0)?), (1, vec![0u8; opts.page_size])],
        )?;
        pager.sync()?;
        sync_dir(dir)?;
    }

    let (page_size, mut metas) = read_metas(&mut pager)?;
    metas.sort_by_key(|m| std::cmp::Reverse(m.1.txn));
    let mut chosen = None;
    let mut last_err = None;
    for (slot, m) in metas {
        let mut seen = vec![false; m.num_pages as usize];
        let mut roots = Vec::with_capacity(m.cfs.len());
        let walk = m.cfs.iter().try_for_each(|c| {
            let root = Root {
                page: c.root,
                txn: c.root_txn,
                keys: 0,
            };
            let keys = tree::verify_tree(&pager, &root, m.num_pages, &mut seen)?;
            roots.push(Root { keys, ..root });
            Ok::<(), Error>(())
        });
        match walk {
            Ok(()) => {
                chosen = Some((slot, m, roots, seen));
                break;
            }
            Err(e) => last_err = Some(e),
        }
    }
    let Some((slot, meta, mut roots, seen)) = chosen else {
        return Err(
            last_err.unwrap_or_else(|| Error::corruption("no meta page describes an intact tree"))
        );
    };
    let ready: Vec<u64> = (2..meta.num_pages).filter(|&p| !seen[p as usize]).collect();
    // The chosen tree may have reached the file only through the page cache
    // of the operating system; make it durable before building on it.
    pager.sync()?;
    let mut alloc = Allocator::new(ready, meta.num_pages);

    let mut cf_names_meta: Vec<String> = meta.cfs.iter().map(|c| c.name.clone()).collect();
    let mut cf_map = Vec::with_capacity(cfs.len());
    let mut added = false;
    for name in cfs {
        match cf_names_meta.iter().position(|n| n == name) {
            Some(i) => cf_map.push(i),
            None if opts.create_if_missing => {
                cf_map.push(cf_names_meta.len());
                cf_names_meta.push(name.to_string());
                roots.push(Root::default());
                added = true;
            }
            None => {
                return Err(Error::NotFound(format!(
                    "column family {name:?} does not exist"
                )));
            }
        }
    }
    let mut txn = meta.txn;
    let mut durable_slot = slot;
    if added {
        txn += 1;
        durable_slot = slot ^ 1;
        let m = Meta {
            txn,
            page_size: page_size as u32,
            num_pages: alloc.num_pages,
            cfs: meta_cfs(&cf_names_meta, &roots),
        };
        pager.write_pages(Vec::new(), vec![(durable_slot, m.encode(durable_slot)?)])?;
        pager.sync()?;
    }
    alloc.release_up_to(u64::MAX);
    let readers = Arc::new(ReaderTable::default());
    let state = TreeState::new(txn, roots, alloc.num_pages, &readers);
    let inner = Inner {
        id: NEXT_DB_ID.fetch_add(1, Ordering::Relaxed),
        opts: opts.clone(),
        page_size,
        cf_names: cfs.iter().map(|s| s.to_string()).collect(),
        cf_map,
        lock_file: Mutex::new(Some(lock_file)),
        pager: Arc::new(pager),
        writer: Mutex::new(Writer {
            alloc,
            durable_txn: txn,
            durable_slot,
            last_slot: durable_slot,
            unsynced_freed: 0,
            cf_names: cf_names_meta,
            poisoned: None,
        }),
        current: RwLock::new(state),
        readers,
        snapshots: Arc::new(AtomicUsize::new(0)),
        state: AtomicU8::new(OPEN),
        stats: Stats::default(),
    };
    Ok(BTreeEngine {
        inner: Arc::new(inner),
    })
}

fn meta_cfs(names: &[String], roots: &[Root]) -> Vec<MetaCf> {
    names
        .iter()
        .zip(roots)
        .map(|(n, r)| MetaCf {
            name: n.clone(),
            root: r.page,
            root_txn: r.txn,
            keys: r.keys,
        })
        .collect()
}

impl Inner {
    fn check_open(&self) -> Result<()> {
        if self.state.load(Ordering::Acquire) == OPEN {
            Ok(())
        } else {
            Err(Error::Closed)
        }
    }

    fn current(&self) -> Arc<TreeState> {
        self.current.read().unwrap().clone()
    }

    fn icf(&self, cf: CfId) -> Result<usize> {
        self.cf_map.get(cf as usize).copied().ok_or_else(|| {
            Error::invalid(format!(
                "column family {cf} does not exist ({} are open)",
                self.cf_map.len()
            ))
        })
    }

    fn state_for(&self, snap: Option<&Snapshot>) -> Result<Arc<TreeState>> {
        match snap {
            None => Ok(self.current()),
            Some(s) => {
                let b = s
                    .downcast::<BtSnapshot>()
                    .ok_or_else(|| Error::invalid("snapshot comes from another engine kind"))?;
                if b.db != self.id {
                    return Err(Error::invalid("snapshot comes from another database"));
                }
                Ok(b.state.clone())
            }
        }
    }

    fn apply(&self, tx: &mut WriteTxn<'_>, batch: &WriteBatch) -> Result<()> {
        for op in batch.iter() {
            let cf = self.cf_map[op.cf() as usize];
            match op {
                BatchOp::Put { key, value, .. } => tx.put(cf, key, value)?,
                BatchOp::Delete { key, .. } => {
                    tx.delete(cf, key)?;
                }
                BatchOp::DeleteRange { start, end, .. } => tx.delete_range(cf, start, end)?,
                BatchOp::Merge { key, operand, .. } => {
                    let old = tx.get(cf, key, true)?;
                    let v = i64add(old.as_deref(), operand)?;
                    tx.put(cf, key, &v)?;
                }
            }
        }
        Ok(())
    }

    fn write(&self, batch: &WriteBatch, opts: WriteOptions) -> Result<()> {
        self.check_open()?;
        batch.validate(
            self.cf_map.len(),
            max_key_len(self.page_size),
            MAX_VALUE_LEN,
        )?;
        if batch.is_empty() {
            return Ok(());
        }
        let mut guard = self.writer.lock().unwrap();
        self.check_open()?;
        let w = &mut *guard;
        if let Some(e) = &w.poisoned {
            return Err(e.clone());
        }
        let base = self.current();
        let limit = w.durable_txn.min(self.readers.oldest().unwrap_or(u64::MAX));
        w.alloc.release_up_to(limit);
        let txn = base.txn + 1;
        let mut tx = WriteTxn::new(&self.pager, txn, base.roots.clone(), &mut w.alloc);
        if let Err(e) = self.apply(&mut tx, batch) {
            tx.abort();
            return Err(e);
        }
        let out = tx.finish();
        drop(base);
        if let Err(e) = self.commit(w, txn, out, opts.sync) {
            w.poisoned = Some(e.clone());
            return Err(e);
        }
        Ok(())
    }

    fn commit(&self, w: &mut Writer, txn: u64, out: TxnOutput, sync: bool) -> Result<()> {
        let TxnOutput {
            pages,
            overflow,
            freed,
            roots,
        } = out;
        let written = (pages.len() + overflow.len() + 1) as u64;
        self.pager.write_pages(pages, overflow)?;
        if sync {
            self.pager.sync()?;
        }
        let slot = w.durable_slot ^ 1;
        let meta = Meta {
            txn,
            page_size: self.page_size as u32,
            num_pages: w.alloc.num_pages,
            cfs: meta_cfs(&w.cf_names, &roots),
        };
        self.pager
            .write_pages(Vec::new(), vec![(slot, meta.encode(slot)?)])?;
        w.last_slot = slot;
        if sync {
            self.pager.sync()?;
            w.durable_txn = txn;
            w.durable_slot = slot;
            w.unsynced_freed = 0;
            self.stats.synced_commits.fetch_add(1, Ordering::Relaxed);
        } else {
            w.unsynced_freed += freed.len();
        }
        if !freed.is_empty() {
            w.alloc_free(txn, freed);
        }
        let state = TreeState::new(txn, roots, w.alloc.num_pages, &self.readers);
        *self.current.write().unwrap() = state;
        self.stats.commits.fetch_add(1, Ordering::Relaxed);
        self.stats
            .pages_written
            .fetch_add(written, Ordering::Relaxed);
        if !sync && w.unsynced_freed >= self.opts.max_unsynced_free_pages {
            self.sync_now(w, txn)?;
            self.stats.auto_syncs.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn sync_now(&self, w: &mut Writer, txn: u64) -> Result<()> {
        self.pager.sync()?;
        w.durable_txn = txn;
        w.durable_slot = w.last_slot;
        w.unsynced_freed = 0;
        Ok(())
    }

    fn flush_wal(&self, sync: bool) -> Result<()> {
        self.check_open()?;
        if !sync {
            return Ok(());
        }
        let mut guard = self.writer.lock().unwrap();
        let w = &mut *guard;
        if let Some(e) = &w.poisoned {
            return Err(e.clone());
        }
        let txn = self.current().txn;
        if w.durable_txn < txn {
            self.sync_now(w, txn)?;
        }
        Ok(())
    }

    /// Writes an openable copy into `dir`. The tree current under the writer
    /// lock is held for the whole copy, which keeps every page it reaches
    /// from being freed and reused. The data file is copied (cloned on
    /// APFS); later commits may have written other pages and new meta pages
    /// meanwhile, so the copy is cut to the held tree's page count and both
    /// of its meta pages are rewritten to name the held tree's roots.
    fn checkpoint(&self, dir: &Path) -> Result<()> {
        self.check_open()?;
        let tmp = checkpoint::begin(dir)?;
        let res = self
            .checkpoint_into(&tmp)
            .and_then(|()| checkpoint::finish(&tmp, dir));
        if res.is_err() {
            checkpoint::abandon(&tmp);
        }
        res
    }

    fn checkpoint_into(&self, tmp: &Path) -> Result<()> {
        let (state, names) = {
            let w = self.writer.lock().unwrap();
            if let Some(e) = &w.poisoned {
                return Err(e.clone());
            }
            (self.current(), w.cf_names.clone())
        };
        let dst = data_path(tmp);
        checkpoint::copy_synced(self.pager.path(), &dst)?;
        let meta = Meta {
            txn: state.txn,
            page_size: self.page_size as u32,
            num_pages: state.num_pages,
            cfs: meta_cfs(&names, &state.roots),
        };
        let ps = self.page_size as u64;
        let (m0, m1) = (meta.encode(0)?, meta.encode(1)?);
        let f = OpenOptions::new()
            .write(true)
            .open(&dst)
            .ctx(|| format!("open {}", dst.display()))?;
        f.set_len(state.num_pages * ps)
            .and_then(|()| f.write_all_at(&m0, 0))
            .and_then(|()| f.write_all_at(&m1, ps))
            .and_then(|()| f.sync_data())
            .ctx(|| format!("write the meta pages of {}", dst.display()))?;
        drop(state);
        Ok(())
    }

    /// For each range: 0 when a cursor finds no key in it; otherwise the
    /// share of the tree's keys between the two ends from `rank_estimate`,
    /// times the key count and the average entry size of the two leaves the
    /// descents reached.
    fn approximate_sizes(&self, cf: CfId, ranges: &[(&[u8], &[u8])]) -> Result<Vec<u64>> {
        self.check_open()?;
        let icf = self.icf(cf)?;
        let state = self.current();
        let root = state.roots[icf];
        ranges
            .iter()
            .map(|&(start, limit)| {
                if start >= limit || root.keys == 0 {
                    return Ok(0);
                }
                let mut c = Cursor::new(
                    self.pager.clone(),
                    state.clone(),
                    icf,
                    Some(start.to_vec()),
                    Some(limit.to_vec()),
                    false,
                );
                c.seek_to_first();
                c.status()?;
                if !c.valid() {
                    return Ok(0);
                }
                let (fa, ba) = tree::rank_estimate(&self.pager, &root, start)?;
                let (fb, bb) = tree::rank_estimate(&self.pager, &root, limit)?;
                let avg = if ba > 0.0 && bb > 0.0 {
                    (ba + bb) / 2.0
                } else {
                    ba.max(bb)
                };
                let est = (fb - fa).max(0.0) * root.keys as f64 * avg;
                Ok((est as u64).max(1))
            })
            .collect()
    }

    fn depth(&self, root: &Root) -> Result<usize> {
        if root.page == 0 {
            return Ok(0);
        }
        let mut r = root.child();
        let mut d = 1;
        loop {
            let p = self.pager.read(r.page, r.txn, false)?;
            if page_type(&p) != TYPE_BRANCH {
                return Ok(d);
            }
            r = branch_child(&p, 0);
            d += 1;
        }
    }

    fn property(&self, cf: CfId, name: &str) -> Option<String> {
        let icf = self.icf(cf).ok()?;
        let state = self.current();
        let root = state.roots[icf];
        let v = match name {
            "rocksdb.estimate-num-keys" => root.keys.to_string(),
            "rocksdb.num-snapshots" => self.snapshots.load(Ordering::Relaxed).to_string(),
            "rocksdb.block-cache-usage" => self.pager.cache_usage().to_string(),
            "rocksdb.block-cache-capacity" => self.pager.cache_capacity().to_string(),
            "nil.latest-sequence" | "nil.btree.txn" => state.txn.to_string(),
            "nil.btree.durable-txn" => self.writer.lock().unwrap().durable_txn.to_string(),
            "nil.btree.page-size" => self.page_size.to_string(),
            "nil.btree.num-pages" => state.num_pages.to_string(),
            "nil.btree.free-pages" => self.writer.lock().unwrap().alloc.ready_count().to_string(),
            "nil.btree.pending-free-pages" => self
                .writer
                .lock()
                .unwrap()
                .alloc
                .pending_count()
                .to_string(),
            "nil.btree.file-size" => self.pager.file_len().ok()?.to_string(),
            "nil.btree.depth" => self.depth(&root).ok()?.to_string(),
            "rocksdb.stats" | "nil.stats" => {
                let w = self.writer.lock().unwrap();
                let s = &self.stats;
                format!(
                    "column family {}: {} keys, depth {}\npages {} of {} bytes, {} free, {} waiting for reuse\ncommits {} ({} synced), automatic syncs {}, pages written {}\n",
                    self.cf_names[cf as usize],
                    root.keys,
                    self.depth(&root).unwrap_or(0),
                    state.num_pages,
                    self.page_size,
                    w.alloc.ready_count(),
                    w.alloc.pending_count(),
                    s.commits.load(Ordering::Relaxed),
                    s.synced_commits.load(Ordering::Relaxed),
                    s.auto_syncs.load(Ordering::Relaxed),
                    s.pages_written.load(Ordering::Relaxed),
                )
            }
            _ => return None,
        };
        Some(v)
    }

    fn close(&self) -> Result<()> {
        if self
            .state
            .compare_exchange(OPEN, CLOSED, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let w = self.writer.lock().unwrap();
        let res = if w.poisoned.is_none() {
            self.pager.sync()
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
        let w = self.writer.lock().unwrap();
        let res = self.pager.crash(mode);
        drop(w);
        *self.lock_file.lock().unwrap() = None;
        res
    }
}

impl Writer {
    fn alloc_free(&mut self, txn: u64, pages: Vec<u64>) {
        self.alloc.free_at(txn, pages);
    }
}

impl Engine for BTreeEngine {
    fn kind(&self) -> EngineKind {
        EngineKind::BTree
    }

    fn column_families(&self) -> Vec<String> {
        self.inner.cf_names.clone()
    }

    fn max_key_len(&self) -> usize {
        max_key_len(self.inner.page_size)
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
        let state = i.state_for(snapshot)?;
        tree::get(&i.pager, &state.roots[icf], key)
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
        let state = i.state_for(snapshot)?;
        keys.iter()
            .map(|k| tree::get(&i.pager, &state.roots[icf], k))
            .collect()
    }

    fn snapshot(&self) -> Result<Snapshot> {
        let i = &self.inner;
        i.check_open()?;
        let state = i.current();
        i.snapshots.fetch_add(1, Ordering::Relaxed);
        Ok(Snapshot::new(
            state.txn,
            Arc::new(BtSnapshot {
                state,
                db: i.id,
                count: i.snapshots.clone(),
            }),
        ))
    }

    fn iter(&self, cf: CfId, opts: IterOptions) -> Result<Box<dyn DbIterator>> {
        let i = &self.inner;
        i.check_open()?;
        let icf = i.icf(cf)?;
        let state = i.state_for(opts.snapshot.as_ref())?;
        Ok(Box::new(Cursor::new(
            i.pager.clone(),
            state,
            icf,
            opts.lower_bound,
            opts.upper_bound,
            opts.fill_cache,
        )))
    }

    fn flush(&self) -> Result<()> {
        self.inner.flush_wal(true)
    }

    fn flush_wal(&self, sync: bool) -> Result<()> {
        self.inner.flush_wal(sync)
    }

    fn compact_range(&self, cf: CfId, _start: Option<&[u8]>, _end: Option<&[u8]>) -> Result<()> {
        self.inner.check_open()?;
        self.inner.icf(cf)?;
        Ok(())
    }

    fn property(&self, cf: CfId, name: &str) -> Option<String> {
        self.inner.property(cf, name)
    }

    fn latest_sequence(&self) -> u64 {
        self.inner.current().txn
    }

    fn checkpoint(&self, dir: &Path) -> Result<()> {
        self.inner.checkpoint(dir)
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

impl Drop for BTreeEngine {
    fn drop(&mut self) {
        let _ = self.inner.close();
    }
}

/// Where the engine keeps its data file inside `dir`.
pub fn data_path(dir: &Path) -> PathBuf {
    dir.join(DATA_FILE)
}

impl BTreeEngine {
    /// Tests only: pages written since the last sync.
    #[doc(hidden)]
    pub fn unsynced_pages(&self) -> Vec<u64> {
        self.inner.pager.unsynced_pages()
    }

    /// Tests only: stops the engine like `crash`, losing the unsynced writes
    /// to the pages `lose` selects and keeping the others.
    #[doc(hidden)]
    pub fn crash_pages(&self, lose: impl Fn(u64) -> bool) -> Result<()> {
        let i = &self.inner;
        if i.state
            .compare_exchange(OPEN, CLOSED, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::Closed);
        }
        let w = i.writer.lock().unwrap();
        let res = i.pager.crash_pages(lose);
        drop(w);
        *i.lock_file.lock().unwrap() = None;
        res
    }
}
