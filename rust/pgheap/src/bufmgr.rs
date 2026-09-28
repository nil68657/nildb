//! The buffer pool, after PostgreSQL's shared buffers
//! (src/backend/storage/buffer/README): a fixed array of 8 KiB frames, a
//! table from page tags to frames split into shards, pin counts, dirty
//! flags and clock-sweep eviction with usage counts capped at 5.
//!
//! A pinned frame keeps its page. Readers and writers lock a frame's
//! contents with its `RwLock`. Writing a dirty page first flushes the WAL up
//! to the page's LSN (WAL before data), then writes a checksummed copy. A
//! per-frame I/O lock keeps two writers of the same frame in order, so an
//! older copy never lands after a newer one.

use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use nilengine_api::cache::FxHasher;
use nilengine_api::{CrashMode, Error, Result};

use crate::file::DataFile;
use crate::page::{self, BLCKSZ, PageBuf};
use crate::wal::{PageTag, Wal};

pub const FORK_MAIN: u8 = 0;
pub const FORK_FSM: u8 = 1;
pub const FORK_VM: u8 = 2;
/// The commit log is relation 1; column families start at 16.
pub const CLOG_REL: u32 = 1;

const SHARDS: usize = 64;
const MAX_USAGE: u32 = 5;
/// Fewer frames than this could run out during a B-tree split, which pins
/// about a dozen pages at once.
pub const MIN_FRAMES: usize = 32;

pub type PageBox = Box<PageBuf>;
type FxBuild = BuildHasherDefault<FxHasher>;

struct Frame {
    tag: Mutex<Option<PageTag>>,
    data: RwLock<PageBox>,
    pins: AtomicU32,
    usage: AtomicU32,
    dirty: AtomicBool,
    valid: AtomicBool,
    io: Mutex<()>,
}

/// One relation fork's file and its length in blocks.
pub struct RelFile {
    pub file: DataFile,
    pub nblocks: AtomicU32,
}

/// How `get` fills a frame that does not hold the page yet.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Read the page from its file (zeros past the end).
    Read,
    /// Start from zeros: a new block, or one redo overwrites with an image.
    Zero,
}

#[derive(Default)]
pub struct PoolStats {
    pub hits: AtomicU64,
    pub reads: AtomicU64,
    pub writes: AtomicU64,
}

pub struct BufferPool {
    frames: Box<[Frame]>,
    shards: Box<[Mutex<HashMap<PageTag, usize, FxBuild>>]>,
    hand: AtomicUsize,
    files: RwLock<HashMap<(u32, u8), Arc<RelFile>>>,
    base: PathBuf,
    clog_dir: PathBuf,
    track: bool,
    wal: Arc<Wal>,
    pub stats: PoolStats,
}

/// A pinned page. Dropping it unpins the frame; lock guards taken from it
/// must be dropped first.
pub struct Buf<'a> {
    pool: &'a BufferPool,
    idx: usize,
    tag: PageTag,
}

impl<'a> Buf<'a> {
    pub fn tag(&self) -> PageTag {
        self.tag
    }

    pub fn read(&self) -> RwLockReadGuard<'a, PageBox> {
        self.pool.frames[self.idx].data.read().unwrap()
    }

    pub fn try_write(&self) -> Option<RwLockWriteGuard<'a, PageBox>> {
        self.pool.frames[self.idx].data.try_write().ok()
    }

    /// Marks the page dirty; call while holding the exclusive lock.
    pub fn mark_dirty(&self) {
        self.pool.frames[self.idx]
            .dirty
            .store(true, Ordering::Release);
    }

    /// Takes the exclusive lock and keeps the pin with it.
    pub fn lock(self) -> Locked<'a> {
        let g = self.pool.frames[self.idx].data.write().unwrap();
        Locked { g, b: self }
    }
}

impl Drop for Buf<'_> {
    fn drop(&mut self) {
        self.pool.frames[self.idx]
            .pins
            .fetch_sub(1, Ordering::AcqRel);
    }
}

/// A page held under its exclusive lock. Field order matters: the guard
/// drops before the pin.
pub struct Locked<'a> {
    g: RwLockWriteGuard<'a, PageBox>,
    b: Buf<'a>,
}

impl Locked<'_> {
    pub fn tag(&self) -> PageTag {
        self.b.tag
    }

    pub fn page(&self) -> &[u8] {
        &self.g[..]
    }

    pub fn page_mut(&mut self) -> &mut [u8] {
        &mut self.g[..]
    }

    pub fn mark_dirty(&self) {
        self.b.mark_dirty()
    }
}

fn shard_of(tag: &PageTag) -> usize {
    let h = (tag.rel as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ (tag.block as u64).wrapping_mul(0xc2b2_ae3d_27d4_eb4f)
        ^ tag.fork as u64;
    ((h ^ (h >> 29)) as usize) % SHARDS
}

impl BufferPool {
    pub fn new(base: &Path, clog_dir: &Path, frames: usize, track: bool, wal: Arc<Wal>) -> Self {
        let n = frames.max(MIN_FRAMES);
        let frames = (0..n)
            .map(|_| Frame {
                tag: Mutex::new(None),
                data: RwLock::new(Box::new([0u8; BLCKSZ])),
                pins: AtomicU32::new(0),
                usage: AtomicU32::new(0),
                dirty: AtomicBool::new(false),
                valid: AtomicBool::new(false),
                io: Mutex::new(()),
            })
            .collect();
        BufferPool {
            frames,
            shards: (0..SHARDS)
                .map(|_| Mutex::new(HashMap::default()))
                .collect(),
            hand: AtomicUsize::new(0),
            files: RwLock::new(HashMap::new()),
            base: base.to_path_buf(),
            clog_dir: clog_dir.to_path_buf(),
            track,
            wal,
            stats: PoolStats::default(),
        }
    }

    pub fn num_frames(&self) -> usize {
        self.frames.len()
    }

    pub fn wal(&self) -> &Wal {
        &self.wal
    }

    fn path_of(&self, rel: u32, fork: u8) -> PathBuf {
        if rel == CLOG_REL {
            return self.clog_dir.join("0000");
        }
        let suffix = match fork {
            FORK_FSM => "_fsm",
            FORK_VM => "_vm",
            _ => "",
        };
        self.base.join(format!("{rel}{suffix}"))
    }

    /// The file of one relation fork, opened (and created) on first use.
    pub fn rel_file(&self, rel: u32, fork: u8) -> Result<Arc<RelFile>> {
        if let Some(f) = self.files.read().unwrap().get(&(rel, fork)) {
            return Ok(f.clone());
        }
        let mut files = self.files.write().unwrap();
        if let Some(f) = files.get(&(rel, fork)) {
            return Ok(f.clone());
        }
        let file = DataFile::open(&self.path_of(rel, fork), self.track)?;
        let n = file.len()?.div_ceil(BLCKSZ as u64) as u32;
        let rf = Arc::new(RelFile {
            file,
            nblocks: AtomicU32::new(n),
        });
        files.insert((rel, fork), rf.clone());
        Ok(rf)
    }

    pub fn nblocks(&self, rel: u32, fork: u8) -> Result<u32> {
        Ok(self.rel_file(rel, fork)?.nblocks.load(Ordering::Acquire))
    }

    /// Reserves the next block of a fork; the caller creates it with
    /// `Mode::Zero`.
    pub fn extend(&self, rel: u32, fork: u8) -> Result<u32> {
        Ok(self
            .rel_file(rel, fork)?
            .nblocks
            .fetch_add(1, Ordering::AcqRel))
    }

    /// Grows a fork's block count to at least `n` (redo and new commit-log
    /// pages).
    pub fn ensure_nblocks(&self, rel: u32, fork: u8, n: u32) -> Result<()> {
        self.rel_file(rel, fork)?
            .nblocks
            .fetch_max(n, Ordering::AcqRel);
        Ok(())
    }

    /// Empties a fork on disk. Only called at open, before any of its pages
    /// is cached.
    pub fn truncate_fork(&self, rel: u32, fork: u8) -> Result<()> {
        let f = self.rel_file(rel, fork)?;
        f.file.truncate()?;
        f.nblocks.store(0, Ordering::Release);
        Ok(())
    }

    /// Pins the page `tag`, loading it if needed.
    pub fn get(&self, tag: PageTag, mode: Mode) -> Result<Buf<'_>> {
        let mut spins = 0u32;
        loop {
            {
                let map = self.shards[shard_of(&tag)].lock().unwrap();
                if let Some(&i) = map.get(&tag) {
                    let f = &self.frames[i];
                    f.pins.fetch_add(1, Ordering::AcqRel);
                    drop(map);
                    let u = f.usage.load(Ordering::Relaxed);
                    if u < MAX_USAGE {
                        f.usage.store(u + 1, Ordering::Relaxed);
                    }
                    // A frame still loading holds its write lock; wait for
                    // it and check the load worked.
                    let ok = f.valid.load(Ordering::Acquire) || {
                        let _g = f.data.read().unwrap();
                        f.valid.load(Ordering::Acquire)
                    };
                    if ok {
                        self.stats.hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(Buf {
                            pool: self,
                            idx: i,
                            tag,
                        });
                    }
                    f.pins.fetch_sub(1, Ordering::AcqRel);
                    continue;
                }
            }
            let Some(v) = self.claim_victim() else {
                spins += 1;
                if spins > 2000 {
                    return Err(Error::Io {
                        context: "buffer pool".into(),
                        kind: std::io::ErrorKind::OutOfMemory,
                        message: format!("all {} frames are pinned", self.frames.len()),
                    });
                }
                std::thread::yield_now();
                continue;
            };
            if let Some(buf) = self.remap(v, tag, mode)? {
                return Ok(buf);
            }
        }
    }

    /// Moves the claimed victim frame `v` to `tag` and loads it. Returns
    /// `None` (with the claim released) when someone else loaded `tag` or
    /// used the victim meanwhile.
    fn remap(&self, v: usize, tag: PageTag, mode: Mode) -> Result<Option<Buf<'_>>> {
        let f = &self.frames[v];
        let release = || {
            f.pins.fetch_sub(1, Ordering::AcqRel);
        };
        if f.dirty.load(Ordering::Acquire)
            && let Err(e) = self.flush_frame(v)
        {
            release();
            return Err(e);
        }
        let old = *f.tag.lock().unwrap();
        let new_shard = shard_of(&tag);
        let old_shard = old.map(|t| shard_of(&t));
        // Lock both shards in index order.
        let lo = old_shard.map_or(new_shard, |s| s.min(new_shard));
        let hi = old_shard.map_or(new_shard, |s| s.max(new_shard));
        let mut g_lo = self.shards[lo].lock().unwrap();
        let mut g_hi = (hi != lo).then(|| self.shards[hi].lock().unwrap());
        let (new_map, old_map) = match g_hi.as_deref_mut() {
            None => (&mut *g_lo, None),
            Some(h) if new_shard == hi => (h, Some(&mut *g_lo)),
            Some(h) => (&mut *g_lo, Some(h)),
        };
        let usable = !new_map.contains_key(&tag)
            && f.pins.load(Ordering::Acquire) == 1
            && !f.dirty.load(Ordering::Acquire);
        let guard = if usable {
            f.data.try_write().ok()
        } else {
            None
        };
        let Some(mut w) = guard else {
            drop(g_hi);
            drop(g_lo);
            release();
            return Ok(None);
        };
        if let Some(o) = old {
            match old_map {
                Some(m) => m.remove(&o),
                None => new_map.remove(&o),
            };
        }
        new_map.insert(tag, v);
        *f.tag.lock().unwrap() = Some(tag);
        f.valid.store(false, Ordering::Release);
        f.usage.store(1, Ordering::Relaxed);
        drop(g_hi);
        drop(g_lo);
        let loaded = match mode {
            Mode::Zero => {
                w.fill(0);
                Ok(())
            }
            Mode::Read => self.load(tag, &mut w[..]),
        };
        if let Err(e) = loaded {
            let mut m = self.shards[new_shard].lock().unwrap();
            if m.get(&tag) == Some(&v) {
                m.remove(&tag);
            }
            *f.tag.lock().unwrap() = None;
            drop(m);
            drop(w);
            release();
            return Err(e);
        }
        f.valid.store(true, Ordering::Release);
        drop(w);
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        Ok(Some(Buf {
            pool: self,
            idx: v,
            tag,
        }))
    }

    fn load(&self, tag: PageTag, buf: &mut [u8]) -> Result<()> {
        let rf = self.rel_file(tag.rel, tag.fork)?;
        rf.file.read_block(tag.block, buf)?;
        if !page::verify(buf, tag.block) {
            if tag.fork != FORK_MAIN {
                // The free-space and visibility maps are hints; PostgreSQL
                // also zeroes their pages on a read error.
                buf.fill(0);
                return Ok(());
            }
            return Err(Error::corruption(format!(
                "checksum mismatch in block {} of relation {} fork {}",
                tag.block, tag.rel, tag.fork
            )));
        }
        Ok(())
    }

    /// Clock sweep: a frame with no pins and a usage count of zero, pinned
    /// for the caller. The mapping is checked again under the shard lock
    /// before the frame is reused.
    fn claim_victim(&self) -> Option<usize> {
        let n = self.frames.len();
        for _ in 0..n * (MAX_USAGE as usize + 1) {
            let i = self.hand.fetch_add(1, Ordering::Relaxed) % n;
            let f = &self.frames[i];
            if f.pins.load(Ordering::Acquire) != 0 {
                continue;
            }
            let u = f.usage.load(Ordering::Relaxed);
            if u > 0 {
                f.usage.store(u - 1, Ordering::Relaxed);
                continue;
            }
            if f.pins
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(i);
            }
        }
        None
    }

    /// Writes one dirty frame: WAL first, then a checksummed copy.
    fn flush_frame(&self, i: usize) -> Result<()> {
        let f = &self.frames[i];
        let _io = f.io.lock().unwrap();
        let (tag, mut copy) = {
            let g = f.data.read().unwrap();
            if !f.dirty.load(Ordering::Acquire) || !f.valid.load(Ordering::Acquire) {
                return Ok(());
            }
            let Some(tag) = *f.tag.lock().unwrap() else {
                return Ok(());
            };
            f.dirty.store(false, Ordering::Release);
            (tag, Box::new(**g))
        };
        let res = (|| {
            self.wal.flush(page::lsn(&copy[..]))?;
            page::set_checksum(&mut copy[..], tag.block);
            self.rel_file(tag.rel, tag.fork)?
                .file
                .write_block(tag.block, &copy[..])
        })();
        match res {
            Ok(()) => {
                self.stats.writes.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                f.dirty.store(true, Ordering::Release);
                Err(e)
            }
        }
    }

    /// Writes every dirty frame (a checkpoint's buffer flush), giving up
    /// with `Error::Closed` as soon as `halted` returns true.
    pub fn flush_all(&self, halted: &dyn Fn() -> bool) -> Result<()> {
        for i in 0..self.frames.len() {
            let f = &self.frames[i];
            if !f.dirty.load(Ordering::Acquire) {
                continue;
            }
            if halted() {
                return Err(Error::Closed);
            }
            f.pins.fetch_add(1, Ordering::AcqRel);
            let r = self.flush_frame(i);
            f.pins.fetch_sub(1, Ordering::AcqRel);
            r?;
        }
        Ok(())
    }

    /// Syncs every relation file written since its last sync.
    pub fn sync_all(&self) -> Result<()> {
        let files: Vec<Arc<RelFile>> = self.files.read().unwrap().values().cloned().collect();
        for f in files {
            f.file.sync()?;
        }
        Ok(())
    }

    /// Frames holding a page.
    pub fn used_frames(&self) -> usize {
        self.frames
            .iter()
            .filter(|f| f.valid.load(Ordering::Relaxed))
            .count()
    }

    pub fn dirty_frames(&self) -> usize {
        self.frames
            .iter()
            .filter(|f| f.dirty.load(Ordering::Relaxed))
            .count()
    }

    /// Bytes of one fork, counting blocks not yet written.
    pub fn fork_bytes(&self, rel: u32, fork: u8) -> u64 {
        self.nblocks(rel, fork).unwrap_or(0) as u64 * BLCKSZ as u64
    }

    /// Tests only: undoes, keeps or tears every unsynced page write.
    pub fn crash(&self, mode: CrashMode) -> Result<()> {
        let files: Vec<Arc<RelFile>> = self.files.read().unwrap().values().cloned().collect();
        for f in files {
            f.file.crash(mode)?;
        }
        Ok(())
    }
}
