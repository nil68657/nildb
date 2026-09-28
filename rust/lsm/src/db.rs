//! The LSM engine: the write path (log, then memtable), reads through
//! memtables and levels, snapshots, the background flush and compaction
//! threads, and recovery from the manifest plus the logs.
//!
//! Lock order: `writer`, then `vs`, then `sv` (write). Readers take only the
//! `sv` read lock. Background threads never take `writer`.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use nilengine_api::{
    BatchOp, CfId, CrashMode, DbIterator, Engine, EngineKind, Error, IoContext, IterOptions,
    Options, Result, Snapshot, WriteBatch, WriteOptions, i64add,
};

use crate::compaction::{self, Compaction};
use crate::filter;
use crate::iter::{DbIter, InternalIterator, LevelIter, MergingIterator};
use crate::key::{KIND_DELETE, KIND_PUT, KIND_SEEK, make_internal};
use crate::log::{LogEnd, LogWriter, read_records};
use crate::memtable::{MemIter, MemTable, check_cf};
use crate::rangedel::{Fragments, RangeTombstone};
use crate::table::{BlockCache, TableBuilder, TableIter};
use crate::version::{FileData, Version, VersionEdit, VersionSet, log_path, sync_dir, table_path};

/// Longest key the LSM accepts.
pub const MAX_KEY_LEN: usize = 1 << 24;
/// Longest value the LSM accepts.
pub const MAX_VALUE_LEN: usize = 1 << 30;

const OPEN: u8 = 0;
const CLOSED: u8 = 1;

static NEXT_DB_ID: AtomicU64 = AtomicU64::new(1);

/// The memtables and version a read uses, swapped as one unit.
pub struct SuperVersion {
    pub mem: Arc<MemTable>,
    /// Immutable memtables waiting for a flush, newest first.
    pub imms: Vec<Arc<MemTable>>,
    pub version: Arc<Version>,
}

#[derive(Default)]
struct SnapshotList {
    seqs: Mutex<BTreeMap<u64, usize>>,
}

impl SnapshotList {
    fn acquire(&self, seq: u64) {
        *self.seqs.lock().unwrap().entry(seq).or_insert(0) += 1;
    }

    fn release(&self, seq: u64) {
        let mut m = self.seqs.lock().unwrap();
        if let Some(n) = m.get_mut(&seq) {
            *n -= 1;
            if *n == 0 {
                m.remove(&seq);
            }
        }
    }

    fn sorted(&self) -> Vec<u64> {
        self.seqs.lock().unwrap().keys().copied().collect()
    }

    fn count(&self) -> usize {
        self.seqs.lock().unwrap().values().sum()
    }

    fn oldest(&self) -> Option<u64> {
        self.seqs.lock().unwrap().keys().next().copied()
    }
}

struct LsmSnapshot {
    seq: u64,
    list: Arc<SnapshotList>,
    db: u64,
}

impl Drop for LsmSnapshot {
    fn drop(&mut self) {
        self.list.release(self.seq);
    }
}

#[derive(Default)]
struct Stats {
    flushes: AtomicU64,
    flush_bytes: AtomicU64,
    compactions: AtomicU64,
    trivial_moves: AtomicU64,
    compaction_read: AtomicU64,
    compaction_written: AtomicU64,
    stalls: AtomicU64,
}

struct Writer {
    wal: Option<LogWriter>,
}

struct Inner {
    dir: PathBuf,
    opts: Options,
    id: u64,
    cf_names: Vec<String>,
    /// `CfId` (position in the open call) to internal column family id.
    cf_map: Vec<u32>,
    identity: bool,
    lock_file: Mutex<Option<File>>,
    writer: Mutex<Writer>,
    sv: RwLock<Arc<SuperVersion>>,
    vs: Mutex<VersionSet>,
    /// Sequence of the last write visible to readers.
    last_seq: AtomicU64,
    snapshots: Arc<SnapshotList>,
    /// Shutdown flag for the background threads.
    bg: Mutex<bool>,
    bg_cv: Condvar,
    compaction_lock: Mutex<()>,
    cancel: AtomicBool,
    state: AtomicU8,
    bg_error: Mutex<Option<Error>>,
    cache: Arc<BlockCache>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    stats: Stats,
}

pub struct LsmEngine {
    inner: Arc<Inner>,
}

enum FileKind {
    Log(u64),
    Table(u64),
    Manifest(u64),
    Temp,
    Other,
}

fn parse_name(name: &str) -> FileKind {
    if let Some(n) = name.strip_suffix(".log").and_then(|n| n.parse().ok()) {
        FileKind::Log(n)
    } else if let Some(n) = name.strip_suffix(".sst").and_then(|n| n.parse().ok()) {
        FileKind::Table(n)
    } else if let Some(n) = name.strip_prefix("MANIFEST-").and_then(|n| n.parse().ok()) {
        FileKind::Manifest(n)
    } else if name.ends_with(".tmp") {
        FileKind::Temp
    } else {
        FileKind::Other
    }
}

fn list_dir(dir: &Path) -> Result<Vec<(FileKind, PathBuf)>> {
    let mut out = Vec::new();
    for e in fs::read_dir(dir).ctx(|| format!("list {}", dir.display()))? {
        let e = e.ctx(|| format!("list {}", dir.display()))?;
        if let Some(name) = e.file_name().to_str() {
            out.push((parse_name(name), e.path()));
        }
    }
    Ok(out)
}

/// Writes a memtable into one level-0 table per column family.
fn build_tables(
    mem: &MemTable,
    dir: &Path,
    opts: &Options,
    next_number: &mut dyn FnMut() -> u64,
) -> Result<Vec<(u32, FileData)>> {
    let mut out: Vec<(u32, FileData)> = Vec::new();
    let mut cur: Option<(u32, u64, TableBuilder)> = None;
    let res = mem.for_each(|cf, ikey, value| {
        if cur.as_ref().is_none_or(|(c, _, _)| *c != cf) {
            if let Some((c, num, b)) = cur.take() {
                out.push((c, FileData::from_props(num, b.finish()?)));
            }
            let num = next_number();
            cur = Some((cf, num, TableBuilder::create(&table_path(dir, num), opts)?));
        }
        cur.as_mut().unwrap().2.add(ikey, value)
    });
    let res = res.and_then(|()| {
        if let Some((c, num, b)) = cur.take() {
            out.push((c, FileData::from_props(num, b.finish()?)));
        }
        Ok(())
    });
    if let Err(e) = res {
        if let Some((_, _, b)) = cur.take() {
            b.abandon();
        }
        for (_, f) in &out {
            let _ = fs::remove_file(table_path(dir, f.number));
        }
        return Err(e);
    }
    Ok(out)
}

/// Inserts one log record (`[base sequence u64 LE][batch]`) into `mem`.
fn replay_record(mem: &MemTable, rec: &[u8], num_cfs: usize, max_seq: &mut u64) -> Result<()> {
    if rec.len() < 8 {
        return Err(Error::corruption(
            "log record shorter than its sequence number",
        ));
    }
    let base = u64::from_le_bytes(rec[..8].try_into().unwrap());
    let batch = WriteBatch::from_data(&rec[8..])?;
    for op in batch.iter() {
        check_cf(op.cf(), num_cfs)?;
        if matches!(op, BatchOp::Merge { .. }) {
            return Err(Error::corruption("log record holds an unresolved merge"));
        }
    }
    let mut seq = base;
    for op in batch.iter() {
        match op {
            BatchOp::Put { cf, key, value } => mem.add(cf, key, seq, KIND_PUT, value),
            BatchOp::Delete { cf, key } => mem.add(cf, key, seq, KIND_DELETE, b""),
            BatchOp::DeleteRange { cf, start, end } => {
                if start < end {
                    mem.add_range_tombstone(cf, start, end, seq);
                }
            }
            BatchOp::Merge { .. } => unreachable!(),
        }
        seq += 1;
    }
    if seq > base {
        *max_seq = (*max_seq).max(seq - 1);
    }
    Ok(())
}

/// The latest state of `key` among the operations already in `b`: `Some`
/// when one of them decides it.
fn lookup_in_batch(b: &WriteBatch, cf: u32, key: &[u8]) -> Option<Option<Vec<u8>>> {
    let mut r = None;
    for op in b.iter() {
        match op {
            BatchOp::Put {
                cf: c,
                key: k,
                value,
            } if c == cf && k == key => r = Some(Some(value.to_vec())),
            BatchOp::Delete { cf: c, key: k } if c == cf && k == key => r = Some(None),
            BatchOp::DeleteRange { cf: c, start, end } if c == cf && start <= key && key < end => {
                r = Some(None)
            }
            _ => {}
        }
    }
    r
}

/// Opens (creating if allowed) an LSM database in `dir` with the column
/// families `cfs`; `CfId` n names `cfs[n]`.
pub fn open(dir: &Path, cfs: &[&str], opts: &Options) -> Result<LsmEngine> {
    if cfs.is_empty() {
        return Err(Error::invalid("open needs at least one column family"));
    }
    for (i, a) in cfs.iter().enumerate() {
        if cfs[..i].contains(a) {
            return Err(Error::invalid(format!("column family {a:?} listed twice")));
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

    let cache = Arc::new(BlockCache::new(opts.cache_bytes));
    let num_levels = opts.num_levels.max(2);
    let mut vs = match VersionSet::recover(dir, cache.clone(), num_levels)? {
        Some(vs) => vs,
        None if opts.create_if_missing => VersionSet::new_empty(dir, cache.clone(), num_levels),
        None => {
            return Err(Error::NotFound(format!(
                "{} holds no database",
                dir.display()
            )));
        }
    };

    let mut cf_edit = VersionEdit::default();
    let mut cf_map = Vec::with_capacity(cfs.len());
    let mut next_id = vs.cf_names.len() as u32;
    for name in cfs {
        match vs.cf_id(name) {
            Some(id) => cf_map.push(id),
            None if opts.create_if_missing => {
                cf_edit.new_cfs.push((next_id, name.to_string()));
                cf_map.push(next_id);
                next_id += 1;
            }
            None => {
                return Err(Error::NotFound(format!(
                    "column family {name:?} does not exist"
                )));
            }
        }
    }
    vs.apply_in_memory(&cf_edit)?;

    let entries = list_dir(dir)?;
    let max_seen = entries
        .iter()
        .filter_map(|(k, _)| match k {
            FileKind::Log(n) | FileKind::Table(n) | FileKind::Manifest(n) => Some(*n),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    vs.next_file_number = vs.next_file_number.max(max_seen + 1);

    // Replay every log the manifest has not flushed, stopping everything at
    // the first damaged record.
    let mut logs: Vec<u64> = entries
        .iter()
        .filter_map(|(k, _)| match k {
            FileKind::Log(n) if *n >= vs.log_number => Some(*n),
            _ => None,
        })
        .collect();
    logs.sort_unstable();
    let recovered = MemTable::new(0);
    let mut max_seq = vs.last_sequence;
    let num_cfs = vs.cf_names.len();
    for n in &logs {
        let end = read_records(&log_path(dir, *n), |rec| {
            replay_record(&recovered, rec, num_cfs, &mut max_seq)
        })?;
        if end != LogEnd::Clean {
            break;
        }
    }

    let mut edit = VersionEdit::default();
    if !recovered.is_empty() {
        for (cf, fd) in build_tables(&recovered, dir, opts, &mut || vs.new_file_number())? {
            edit.new_files.push((cf, 0, fd));
        }
        for (cf, t) in recovered.all_tombstones() {
            edit.new_tombstones.push((cf, t));
        }
    }
    let wal_number = vs.new_file_number();
    edit.log_number = Some(wal_number);
    vs.last_sequence = max_seq;
    vs.apply_in_memory(&edit)?;
    vs.write_new_manifest()?;
    let wal = LogWriter::create(&log_path(dir, wal_number))?;
    sync_dir(dir)?;

    let live = vs.current.live_files();
    for (kind, path) in list_dir(dir)? {
        let dead = match kind {
            FileKind::Log(n) => n < wal_number,
            FileKind::Table(n) => !live.contains(&n),
            FileKind::Manifest(n) => n != vs.manifest_number,
            FileKind::Temp => true,
            FileKind::Other => false,
        };
        if dead {
            let _ = fs::remove_file(path);
        }
    }

    let identity = cf_map.iter().enumerate().all(|(i, &c)| i as u32 == c);
    let sv = SuperVersion {
        mem: Arc::new(MemTable::new(wal_number)),
        imms: Vec::new(),
        version: vs.current.clone(),
    };
    let inner = Arc::new(Inner {
        dir: dir.to_path_buf(),
        opts: opts.clone(),
        id: NEXT_DB_ID.fetch_add(1, Ordering::Relaxed),
        cf_names: cfs.iter().map(|s| s.to_string()).collect(),
        cf_map,
        identity,
        lock_file: Mutex::new(Some(lock_file)),
        writer: Mutex::new(Writer { wal: Some(wal) }),
        sv: RwLock::new(Arc::new(sv)),
        vs: Mutex::new(vs),
        last_seq: AtomicU64::new(max_seq),
        snapshots: Arc::new(SnapshotList::default()),
        bg: Mutex::new(false),
        bg_cv: Condvar::new(),
        compaction_lock: Mutex::new(()),
        cancel: AtomicBool::new(false),
        state: AtomicU8::new(OPEN),
        bg_error: Mutex::new(None),
        cache,
        threads: Mutex::new(Vec::new()),
        stats: Stats::default(),
    });
    let f = inner.clone();
    let flush = thread::Builder::new()
        .name("nil-lsm-flush".into())
        .spawn(move || flush_loop(f))
        .ctx(|| "spawn flush thread")?;
    let c = inner.clone();
    let compact = thread::Builder::new()
        .name("nil-lsm-compact".into())
        .spawn(move || compaction_loop(c))
        .ctx(|| "spawn compaction thread")?;
    inner.threads.lock().unwrap().extend([flush, compact]);
    Ok(LsmEngine { inner })
}

fn flush_loop(inner: Arc<Inner>) {
    loop {
        let imm = {
            let mut shutdown = inner.bg.lock().unwrap();
            loop {
                if *shutdown {
                    return;
                }
                if inner.bg_error.lock().unwrap().is_none()
                    && let Some(m) = inner.sv().imms.last().cloned()
                {
                    break m;
                }
                shutdown = inner
                    .bg_cv
                    .wait_timeout(shutdown, Duration::from_millis(200))
                    .unwrap()
                    .0;
            }
        };
        if let Err(e) = inner.flush_imm(&imm) {
            inner.set_bg_error(e);
        }
        inner.bg_cv.notify_all();
    }
}

fn compaction_loop(inner: Arc<Inner>) {
    loop {
        {
            let mut shutdown = inner.bg.lock().unwrap();
            loop {
                if *shutdown {
                    return;
                }
                if inner.bg_error.lock().unwrap().is_none()
                    && compaction::needs_compaction(&inner.sv().version, &inner.opts)
                {
                    break;
                }
                shutdown = inner
                    .bg_cv
                    .wait_timeout(shutdown, Duration::from_millis(200))
                    .unwrap()
                    .0;
            }
        }
        let _g = inner.compaction_lock.lock().unwrap();
        let c = {
            let mut vs = inner.vs.lock().unwrap();
            let v = vs.current.clone();
            compaction::pick_auto(&v, &inner.opts, &mut vs.compact_pointers)
        };
        if let Some(c) = c
            && let Err(e) = inner.run_compaction(c)
        {
            inner.set_bg_error(e);
        }
        inner.bg_cv.notify_all();
    }
}

impl Inner {
    fn check_open(&self) -> Result<()> {
        if self.state.load(Ordering::Acquire) == OPEN {
            Ok(())
        } else {
            Err(Error::Closed)
        }
    }

    fn check_bg(&self) -> Result<()> {
        match &*self.bg_error.lock().unwrap() {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    fn set_bg_error(&self, e: Error) {
        let mut g = self.bg_error.lock().unwrap();
        if g.is_none() {
            *g = Some(e);
        }
        drop(g);
        self.bg_cv.notify_all();
    }

    fn sv(&self) -> Arc<SuperVersion> {
        self.sv.read().unwrap().clone()
    }

    /// Installs a super version with the current version of `vs`; the
    /// caller holds the `vs` lock.
    fn install_sv(&self, vs: &VersionSet, mem: Arc<MemTable>, imms: Vec<Arc<MemTable>>) {
        *self.sv.write().unwrap() = Arc::new(SuperVersion {
            mem,
            imms,
            version: vs.current.clone(),
        });
    }

    fn icf(&self, cf: CfId) -> Result<u32> {
        self.cf_map.get(cf as usize).copied().ok_or_else(|| {
            Error::invalid(format!(
                "column family {cf} does not exist ({} are open)",
                self.cf_map.len()
            ))
        })
    }

    fn read_seq(&self, snap: Option<&Snapshot>) -> Result<u64> {
        match snap {
            None => Ok(self.last_seq.load(Ordering::Acquire)),
            Some(s) => {
                let inner = s
                    .downcast::<LsmSnapshot>()
                    .ok_or_else(|| Error::invalid("snapshot comes from another engine kind"))?;
                if inner.db != self.id {
                    return Err(Error::invalid("snapshot comes from another database"));
                }
                Ok(inner.seq)
            }
        }
    }

    fn get_internal(
        &self,
        sv: &SuperVersion,
        icf: u32,
        key: &[u8],
        seq: u64,
    ) -> Result<Option<Vec<u8>>> {
        let mut buf = Vec::with_capacity(key.len() + 12);
        let mut found: Option<(u64, u8, Cow<'_, [u8]>)> = None;
        for m in std::iter::once(&sv.mem).chain(sv.imms.iter()) {
            if let Some((s, kind, v)) = m.get(icf, key, seq, &mut buf) {
                found = Some((s, kind, Cow::Borrowed(v)));
                break;
            }
        }
        let cfs = &sv.version.cfs[icf as usize];
        if found.is_none() {
            let target = make_internal(key, seq, KIND_SEEK);
            let h = filter::key_hash(key);
            'levels: for (level, files) in cfs.levels.iter().enumerate() {
                if level == 0 {
                    for f in files.iter().rev() {
                        if f.contains_user(key)
                            && let Some((s, kind, v)) = f.table()?.get(&target, key, h)?
                        {
                            found = Some((s, kind, Cow::Owned(v)));
                            break 'levels;
                        }
                    }
                } else {
                    let i = files.partition_point(|f| f.largest_user() < key);
                    if let Some(f) = files.get(i)
                        && f.smallest_user() <= key
                        && let Some((s, kind, v)) = f.table()?.get(&target, key, h)?
                    {
                        found = Some((s, kind, Cow::Owned(v)));
                        break 'levels;
                    }
                }
            }
        }
        let Some((s, kind, v)) = found else {
            return Ok(None);
        };
        if kind == KIND_DELETE {
            return Ok(None);
        }
        let mut cover = cfs.frags.max_covering(key, seq);
        for m in std::iter::once(&sv.mem).chain(sv.imms.iter()) {
            if let Some(f) = m.fragments(icf) {
                cover = cover.max(f.max_covering(key, seq));
            }
        }
        if cover > s {
            return Ok(None);
        }
        Ok(Some(v.into_owned()))
    }

    /// Maps column families to internal ids and turns merges into puts,
    /// reading the current value from earlier operations of the batch or
    /// from the database at `latest`.
    fn prepare(&self, batch: &WriteBatch, sv: &SuperVersion, latest: u64) -> Result<WriteBatch> {
        let mut out = WriteBatch::with_capacity(batch.size());
        for op in batch.iter() {
            let icf = self.cf_map[op.cf() as usize];
            match op {
                BatchOp::Put { key, value, .. } => out.put(icf, key, value),
                BatchOp::Delete { key, .. } => out.delete(icf, key),
                BatchOp::DeleteRange { start, end, .. } => out.delete_range(icf, start, end),
                BatchOp::Merge { key, operand, .. } => {
                    let existing = match lookup_in_batch(&out, icf, key) {
                        Some(v) => v,
                        None => self.get_internal(sv, icf, key, latest)?,
                    };
                    let v = i64add(existing.as_deref(), operand)?;
                    out.put(icf, key, &v);
                }
            }
        }
        Ok(out)
    }

    fn write(&self, batch: &WriteBatch, opts: WriteOptions) -> Result<()> {
        self.check_open()?;
        batch.validate(self.cf_map.len(), MAX_KEY_LEN, MAX_VALUE_LEN)?;
        if batch.is_empty() {
            return Ok(());
        }
        let mut w = self.writer.lock().unwrap();
        self.check_open()?;
        self.check_bg()?;
        self.make_room(&mut w)?;
        let sv = self.sv();
        let latest = self.last_seq.load(Ordering::Acquire);
        let base = latest + 1;
        let prepared: Cow<'_, WriteBatch> = if self.identity && !batch.has_merges() {
            Cow::Borrowed(batch)
        } else {
            Cow::Owned(self.prepare(batch, &sv, latest)?)
        };
        let wal = w.wal.as_mut().ok_or(Error::Closed)?;
        let logged = wal
            .add_record(&[&base.to_le_bytes(), prepared.data()])
            .and_then(|()| if opts.sync { wal.sync() } else { Ok(()) });
        if let Err(e) = logged {
            // A partial record may sit at the log tail; later records would
            // be lost behind it, so the engine refuses further writes.
            self.set_bg_error(e.clone());
            return Err(e);
        }
        let mut seq = base;
        for op in prepared.iter() {
            match op {
                BatchOp::Put { cf, key, value } => sv.mem.add(cf, key, seq, KIND_PUT, value),
                BatchOp::Delete { cf, key } => sv.mem.add(cf, key, seq, KIND_DELETE, b""),
                BatchOp::DeleteRange { cf, start, end } => {
                    if start < end {
                        sv.mem.add_range_tombstone(cf, start, end, seq);
                    }
                }
                BatchOp::Merge { .. } => unreachable!("prepare resolves merges"),
            }
            seq += 1;
        }
        self.last_seq.store(seq - 1, Ordering::Release);
        Ok(())
    }

    fn wait_bg(&self, ms: u64) {
        self.bg_cv.notify_all();
        let g = self.bg.lock().unwrap();
        let _ = self
            .bg_cv
            .wait_timeout(g, Duration::from_millis(ms))
            .unwrap();
    }

    fn make_room(&self, w: &mut Writer) -> Result<()> {
        let mut stalled = false;
        loop {
            self.check_bg()?;
            self.check_open()?;
            let sv = self.sv();
            let l0 = sv
                .version
                .cfs
                .iter()
                .map(|c| c.levels[0].len())
                .max()
                .unwrap_or(0);
            let full = sv.mem.usage() >= self.opts.write_buffer_size;
            if l0 >= self.opts.level0_stop_writes_trigger
                || (full && sv.imms.len() >= self.opts.max_immutable_memtables)
            {
                if !stalled {
                    self.stats.stalls.fetch_add(1, Ordering::Relaxed);
                    stalled = true;
                }
                self.wait_bg(50);
                continue;
            }
            if !full {
                return Ok(());
            }
            return self.switch_memtable(w);
        }
    }

    /// Makes the memtable immutable and starts a new memtable and log.
    fn switch_memtable(&self, w: &mut Writer) -> Result<()> {
        let number = self.vs.lock().unwrap().new_file_number();
        let new_wal = LogWriter::create(&log_path(&self.dir, number))?;
        let synced = match w.wal.as_mut() {
            Some(old) => old.sync(),
            None => Err(Error::Closed),
        };
        let synced = synced.and_then(|()| sync_dir(&self.dir));
        if let Err(e) = synced {
            self.set_bg_error(e.clone());
            return Err(e);
        }
        {
            let vs = self.vs.lock().unwrap();
            let cur = self.sv();
            cur.mem.set_next_wal_number(number);
            let mut imms = Vec::with_capacity(cur.imms.len() + 1);
            imms.push(cur.mem.clone());
            imms.extend(cur.imms.iter().cloned());
            self.install_sv(&vs, Arc::new(MemTable::new(number)), imms);
        }
        w.wal = Some(new_wal);
        self.bg_cv.notify_all();
        Ok(())
    }

    fn flush_imm(&self, imm: &Arc<MemTable>) -> Result<()> {
        let files = build_tables(imm, &self.dir, &self.opts, &mut || {
            self.vs.lock().unwrap().new_file_number()
        })?;
        let mut edit = VersionEdit::default();
        let mut bytes = 0;
        for (cf, fd) in files {
            bytes += fd.size;
            edit.new_files.push((cf, 0, fd));
        }
        for (cf, t) in imm.all_tombstones() {
            edit.new_tombstones.push((cf, t));
        }
        let next_log = imm.next_wal_number();
        edit.log_number = Some(next_log);
        {
            let mut vs = self.vs.lock().unwrap();
            vs.last_sequence = vs.last_sequence.max(imm.max_seq());
            vs.log_and_apply(edit)?;
            let cur = self.sv();
            let imms = cur
                .imms
                .iter()
                .filter(|m| !Arc::ptr_eq(m, imm))
                .cloned()
                .collect();
            self.install_sv(&vs, cur.mem.clone(), imms);
        }
        self.stats.flushes.fetch_add(1, Ordering::Relaxed);
        self.stats.flush_bytes.fetch_add(bytes, Ordering::Relaxed);
        if let Ok(entries) = list_dir(&self.dir) {
            for (kind, path) in entries {
                if let FileKind::Log(n) = kind
                    && n < next_log
                {
                    let _ = fs::remove_file(path);
                }
            }
        }
        Ok(())
    }

    /// Applies `edit`, then drops range tombstones of `cf` that no file
    /// needs any more, all under one `vs` lock.
    fn install(&self, edit: VersionEdit, cf: usize) -> Result<()> {
        let mut vs = self.vs.lock().unwrap();
        vs.log_and_apply(edit)?;
        let dead = compaction::obsolete_tombstones(&vs.current, cf);
        if !dead.is_empty() {
            let gc = VersionEdit {
                deleted_tombstones: dead.into_iter().map(|s| (cf as u32, s)).collect(),
                ..VersionEdit::default()
            };
            vs.log_and_apply(gc)?;
        }
        let cur = self.sv();
        self.install_sv(&vs, cur.mem.clone(), cur.imms.clone());
        Ok(())
    }

    fn run_compaction(&self, c: Compaction) -> Result<()> {
        if c.is_trivial_move() {
            let f = &c.inputs[0];
            let mut edit = VersionEdit::default();
            edit.deleted_files
                .push((c.cf as u32, c.level as u32, f.number()));
            edit.new_files
                .push((c.cf as u32, c.output_level as u32, f.d.clone()));
            self.install(edit, c.cf)?;
            self.stats.trivial_moves.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let snapshots = self.snapshots.sorted();
        let out = compaction::run(
            &c,
            &snapshots,
            &self.opts,
            &self.dir,
            &mut || self.vs.lock().unwrap().new_file_number(),
            &self.cancel,
            &self.cache,
        )?;
        let Some(out) = out else {
            return Ok(());
        };
        self.install(c.edit(&out.files), c.cf)?;
        self.stats.compactions.fetch_add(1, Ordering::Relaxed);
        self.stats
            .compaction_read
            .fetch_add(out.bytes_read, Ordering::Relaxed);
        self.stats
            .compaction_written
            .fetch_add(out.bytes_written, Ordering::Relaxed);
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        self.check_open()?;
        {
            let mut w = self.writer.lock().unwrap();
            self.check_open()?;
            self.check_bg()?;
            if !self.sv().mem.is_empty() {
                self.switch_memtable(&mut w)?;
            }
        }
        loop {
            self.check_bg()?;
            if self.sv().imms.is_empty() {
                return Ok(());
            }
            self.check_open()?;
            self.wait_bg(20);
        }
    }

    fn compact_range(&self, cf: CfId, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        self.check_open()?;
        let icf = self.icf(cf)? as usize;
        self.flush()?;
        let _g = self.compaction_lock.lock().unwrap();
        self.check_bg()?;
        let bottom = self.sv().version.deepest_level(icf).max(1);
        for level in 0..bottom {
            let v = self.sv().version.clone();
            if let Some(c) = compaction::pick_manual(&v, icf, level, start, end) {
                self.run_compaction(c)?;
            }
        }
        let v = self.sv().version.clone();
        if let Some(c) = compaction::pick_bottom(&v, icf, bottom, start, end) {
            self.run_compaction(c)?;
        }
        Ok(())
    }

    fn iter(&self, cf: CfId, opts: IterOptions) -> Result<Box<dyn DbIterator>> {
        self.check_open()?;
        let icf = self.icf(cf)?;
        let seq = self.read_seq(opts.snapshot.as_ref())?;
        let sv = self.sv();
        let (lo, hi) = (opts.lower_bound.as_deref(), opts.upper_bound.as_deref());
        let mut children: Vec<Box<dyn InternalIterator>> =
            vec![Box::new(MemIter::new(sv.mem.clone(), icf))];
        for m in &sv.imms {
            children.push(Box::new(MemIter::new(m.clone(), icf)));
        }
        let cfs = &sv.version.cfs[icf as usize];
        for f in cfs.levels[0].iter().rev() {
            if f.overlaps(lo, hi) {
                children.push(Box::new(TableIter::new(f.table()?, opts.fill_cache)));
            }
        }
        for files in cfs.levels.iter().skip(1) {
            let files: Vec<_> = files
                .iter()
                .filter(|f| f.overlaps(lo, hi))
                .cloned()
                .collect();
            if !files.is_empty() {
                children.push(Box::new(LevelIter::new(files, opts.fill_cache)));
            }
        }
        let mut ts: Vec<RangeTombstone> = cfs
            .tombstones
            .iter()
            .filter(|t| t.seq <= seq)
            .cloned()
            .collect();
        for m in std::iter::once(&sv.mem).chain(sv.imms.iter()) {
            ts.extend(m.tombstones(icf).into_iter().filter(|t| t.seq <= seq));
        }
        let frags = (!ts.is_empty()).then(|| Fragments::build(&ts));
        let merged = MergingIterator::new(children);
        Ok(Box::new(DbIter::new(
            merged,
            seq,
            frags,
            opts.lower_bound,
            opts.upper_bound,
            Box::new(sv),
        )))
    }

    fn property(&self, cf: CfId, name: &str) -> Option<String> {
        let icf = self.icf(cf).ok()? as usize;
        let sv = self.sv();
        let c = &sv.version.cfs[icf];
        let mems = || std::iter::once(&sv.mem).chain(sv.imms.iter());
        let v = match name {
            "rocksdb.estimate-num-keys" => {
                let files: i64 = c
                    .levels
                    .iter()
                    .flatten()
                    .map(|f| f.d.entries as i64 - 2 * f.d.deletions as i64)
                    .sum();
                let mem: i64 = mems().map(|m| m.live_estimate(icf as u32)).sum();
                (files + mem).max(0).to_string()
            }
            "rocksdb.num-snapshots" => self.snapshots.count().to_string(),
            "rocksdb.oldest-snapshot-sequence" => self.snapshots.oldest().unwrap_or(0).to_string(),
            "rocksdb.num-immutable-mem-table" => sv.imms.len().to_string(),
            "rocksdb.cur-size-active-mem-table" => sv.mem.usage().to_string(),
            "rocksdb.cur-size-all-mem-tables" => {
                mems().map(|m| m.usage()).sum::<usize>().to_string()
            }
            "rocksdb.total-sst-files-size" => c
                .levels
                .iter()
                .flatten()
                .map(|f| f.d.size)
                .sum::<u64>()
                .to_string(),
            "rocksdb.estimate-pending-compaction-bytes" => {
                compaction::pending_bytes(&sv.version, icf, &self.opts).to_string()
            }
            "rocksdb.block-cache-usage" => self.cache.usage().to_string(),
            "rocksdb.block-cache-capacity" => self.cache.capacity().to_string(),
            "nil.latest-sequence" => self.last_seq.load(Ordering::Acquire).to_string(),
            "nil.num-range-tombstones" => (c.tombstones.len()
                + mems()
                    .map(|m| m.tombstones(icf as u32).len())
                    .sum::<usize>())
            .to_string(),
            "rocksdb.stats" | "nil.stats" => self.stats_text(&sv, icf),
            _ => {
                let level: usize = name
                    .strip_prefix("rocksdb.num-files-at-level")?
                    .parse()
                    .ok()?;
                c.levels.get(level)?.len().to_string()
            }
        };
        Some(v)
    }

    fn stats_text(&self, sv: &SuperVersion, icf: usize) -> String {
        let s = &self.stats;
        let mut out = format!("column family {}\n", self.cf_names_internal(icf));
        for (level, files) in sv.version.cfs[icf].levels.iter().enumerate() {
            let bytes: u64 = files.iter().map(|f| f.d.size).sum();
            out.push_str(&format!(
                "level {level}: {} files, {bytes} bytes\n",
                files.len()
            ));
        }
        out.push_str(&format!(
            "flushes {} ({} bytes), compactions {} (read {} bytes, wrote {} bytes), trivial moves {}, write stalls {}\n",
            s.flushes.load(Ordering::Relaxed),
            s.flush_bytes.load(Ordering::Relaxed),
            s.compactions.load(Ordering::Relaxed),
            s.compaction_read.load(Ordering::Relaxed),
            s.compaction_written.load(Ordering::Relaxed),
            s.trivial_moves.load(Ordering::Relaxed),
            s.stalls.load(Ordering::Relaxed),
        ));
        out
    }

    fn cf_names_internal(&self, icf: usize) -> String {
        self.cf_map
            .iter()
            .position(|&c| c as usize == icf)
            .map(|i| self.cf_names[i].clone())
            .unwrap_or_else(|| format!("#{icf}"))
    }

    fn stop_threads(&self) {
        self.cancel.store(true, Ordering::Release);
        *self.bg.lock().unwrap() = true;
        self.bg_cv.notify_all();
        let handles = std::mem::take(&mut *self.threads.lock().unwrap());
        for h in handles {
            let _ = h.join();
        }
    }

    fn close(&self) -> Result<()> {
        if self
            .state
            .compare_exchange(OPEN, CLOSED, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        self.stop_threads();
        let res = {
            let mut w = self.writer.lock().unwrap();
            let r = match w.wal.as_mut() {
                Some(wal) => wal.sync(),
                None => Ok(()),
            };
            w.wal = None;
            r
        };
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
        self.stop_threads();
        let wal = self.writer.lock().unwrap().wal.take();
        let res = match wal {
            Some(w) => w.crash(mode),
            None => Ok(()),
        };
        *self.lock_file.lock().unwrap() = None;
        res
    }
}

impl Engine for LsmEngine {
    fn kind(&self) -> EngineKind {
        EngineKind::Lsm
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
        let seq = i.read_seq(snapshot)?;
        let sv = i.sv();
        i.get_internal(&sv, icf, key, seq)
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
        let seq = i.read_seq(snapshot)?;
        let sv = i.sv();
        keys.iter()
            .map(|k| i.get_internal(&sv, icf, k, seq))
            .collect()
    }

    fn snapshot(&self) -> Result<Snapshot> {
        let i = &self.inner;
        i.check_open()?;
        let seq = i.last_seq.load(Ordering::Acquire);
        i.snapshots.acquire(seq);
        Ok(Snapshot::new(
            seq,
            Arc::new(LsmSnapshot {
                seq,
                list: i.snapshots.clone(),
                db: i.id,
            }),
        ))
    }

    fn iter(&self, cf: CfId, opts: IterOptions) -> Result<Box<dyn DbIterator>> {
        self.inner.iter(cf, opts)
    }

    fn flush(&self) -> Result<()> {
        self.inner.flush()
    }

    fn flush_wal(&self, sync: bool) -> Result<()> {
        let i = &self.inner;
        i.check_open()?;
        if !sync {
            return Ok(());
        }
        let mut w = i.writer.lock().unwrap();
        match w.wal.as_mut() {
            Some(wal) => wal.sync(),
            None => Err(Error::Closed),
        }
    }

    fn compact_range(&self, cf: CfId, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        self.inner.compact_range(cf, start, end)
    }

    fn property(&self, cf: CfId, name: &str) -> Option<String> {
        self.inner.property(cf, name)
    }

    fn latest_sequence(&self) -> u64 {
        self.inner.last_seq.load(Ordering::Acquire)
    }

    fn close(&self) -> Result<()> {
        self.inner.close()
    }

    fn crash(&self, mode: CrashMode) -> Result<()> {
        self.inner.crash(mode)
    }
}

impl Drop for LsmEngine {
    fn drop(&mut self) {
        let _ = self.inner.close();
    }
}
