//! Relation files and WAL segments. With `Options::track_unsynced_writes`
//! each file remembers what it held at its last sync, so `Engine::crash` can
//! undo, keep or tear every later write the way a power loss would. Relation
//! files are written in whole 8 KiB pages; WAL segments are append-only.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use nilengine_api::{CrashMode, Error, IoContext, Result};

use crate::page::BLCKSZ;

/// xorshift64 (Marsaglia, "Xorshift RNGs", JSS 2003), the generator the
/// other engines' crash hooks use.
pub struct XorShift(u64);

impl XorShift {
    pub fn new(seed: u64) -> XorShift {
        XorShift(seed | 1)
    }

    pub fn next(&mut self) -> u64 {
        let mut r = self.0;
        r ^= r << 13;
        r ^= r >> 7;
        r ^= r << 17;
        self.0 = r;
        r
    }
}

/// A salt that gives each file its own crash choices for one seed.
fn path_salt(path: &Path) -> u64 {
    let name = path
        .file_name()
        .map(|n| n.as_encoded_bytes())
        .unwrap_or(&[]);
    nilengine_api::coding::hash64(name, 0x5eed)
}

struct PageTracker {
    synced_len: u64,
    /// Content at the last sync of every block written since, or `None`
    /// for a block that lay beyond the synced end of the file.
    pre: HashMap<u32, Option<Box<[u8]>>>,
}

/// One fork of one relation: a file of 8 KiB pages.
pub struct DataFile {
    file: File,
    path: PathBuf,
    /// Set by every write and cleared by a sync, so a checkpoint syncs only
    /// the files it wrote.
    dirty: AtomicBool,
    tracker: Option<Mutex<PageTracker>>,
}

impl DataFile {
    pub fn open(path: &Path, track: bool) -> Result<DataFile> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .ctx(|| format!("open {}", path.display()))?;
        let len = file
            .metadata()
            .ctx(|| format!("stat {}", path.display()))?
            .len();
        Ok(DataFile {
            file,
            path: path.to_path_buf(),
            dirty: AtomicBool::new(false),
            tracker: track.then(|| {
                Mutex::new(PageTracker {
                    synced_len: len,
                    pre: HashMap::new(),
                })
            }),
        })
    }

    pub fn len(&self) -> Result<u64> {
        Ok(self
            .file
            .metadata()
            .ctx(|| format!("stat {}", self.path.display()))?
            .len())
    }

    /// Reads one block; bytes past the end of the file read as zeros.
    pub fn read_block(&self, block: u32, buf: &mut [u8]) -> Result<()> {
        let off = block as u64 * BLCKSZ as u64;
        let mut done = 0;
        while done < buf.len() {
            let n = self
                .file
                .read_at(&mut buf[done..], off + done as u64)
                .ctx(|| format!("read block {block} of {}", self.path.display()))?;
            if n == 0 {
                break;
            }
            done += n;
        }
        buf[done..].fill(0);
        Ok(())
    }

    pub fn write_block(&self, block: u32, data: &[u8]) -> Result<()> {
        let off = block as u64 * BLCKSZ as u64;
        let write = || {
            self.file
                .write_all_at(data, off)
                .ctx(|| format!("write block {block} of {}", self.path.display()))
        };
        match &self.tracker {
            None => write()?,
            Some(t) => {
                let mut t = t.lock().unwrap();
                if !t.pre.contains_key(&block) {
                    let pre = if off + BLCKSZ as u64 <= t.synced_len {
                        let mut b = vec![0u8; BLCKSZ].into_boxed_slice();
                        self.read_block(block, &mut b)?;
                        Some(b)
                    } else {
                        None
                    };
                    t.pre.insert(block, pre);
                }
                write()?;
            }
        }
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }

    /// Syncs the file if anything was written since the last sync
    /// (`F_FULLFSYNC` on macOS through `File::sync_data`).
    pub fn sync(&self) -> Result<()> {
        let guard = self.tracker.as_ref().map(|t| t.lock().unwrap());
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        if let Err(e) = self.file.sync_data() {
            self.dirty.store(true, Ordering::Release);
            return Err(Error::io(format!("sync {}", self.path.display()), &e));
        }
        if let Some(mut t) = guard {
            t.synced_len = self.len()?;
            t.pre.clear();
        }
        Ok(())
    }

    /// Empties the file and syncs it.
    pub fn truncate(&self) -> Result<()> {
        let guard = self.tracker.as_ref().map(|t| t.lock().unwrap());
        self.file
            .set_len(0)
            .and_then(|_| self.file.sync_data())
            .ctx(|| format!("truncate {}", self.path.display()))?;
        if let Some(mut t) = guard {
            t.synced_len = 0;
            t.pre.clear();
        }
        Ok(())
    }

    /// Undoes, keeps or tears each block written since the last sync, then
    /// cuts the file after the last block that survived.
    pub fn crash(&self, mode: CrashMode) -> Result<()> {
        let Some(t) = &self.tracker else {
            return Err(Error::Unsupported(
                "crash needs Options::track_unsynced_writes".into(),
            ));
        };
        let t = t.lock().unwrap();
        let ps = BLCKSZ as u64;
        let mut blocks: Vec<(&u32, &Option<Box<[u8]>>)> = t.pre.iter().collect();
        blocks.sort_unstable_by_key(|(n, _)| **n);
        let write = |n: u32, data: &[u8]| {
            self.file
                .write_all_at(data, n as u64 * ps)
                .ctx(|| format!("crash block {n} of {}", self.path.display()))
        };
        let zeros = vec![0u8; BLCKSZ];
        let mut keep_end = t.synced_len;
        let (partial, mut rng) = match mode {
            CrashMode::DropUnsynced => (false, XorShift::new(1)),
            CrashMode::PartialUnsynced { seed } => {
                (true, XorShift::new(seed ^ path_salt(&self.path)))
            }
        };
        for (&n, pre) in blocks {
            let old = pre.as_deref().unwrap_or(&zeros);
            let choice = if partial { rng.next() % 3 } else { 1 };
            match choice {
                // keep the new content
                0 => keep_end = keep_end.max((n as u64 + 1) * ps),
                // lose the write
                1 => write(n, old)?,
                // tear it at a 512-byte boundary
                _ => {
                    let sectors = BLCKSZ / 512;
                    let cut = 512 * (1 + (rng.next() as usize) % (sectors - 1));
                    let mut now = vec![0u8; BLCKSZ];
                    self.read_block(n, &mut now)?;
                    now[cut..].copy_from_slice(&old[cut..]);
                    write(n, &now)?;
                    keep_end = keep_end.max((n as u64 + 1) * ps);
                }
            }
        }
        self.file
            .set_len(keep_end)
            .ctx(|| format!("truncate {}", self.path.display()))
    }
}

/// One WAL segment file, written strictly in order.
pub struct WalFile {
    pub file: Arc<File>,
    pub path: PathBuf,
    /// Bytes written.
    pub len: u64,
    /// Bytes known to be on stable storage.
    pub synced: u64,
}

impl WalFile {
    /// Creates the segment, dropping any older file of the same name.
    pub fn create(path: &Path) -> Result<WalFile> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .ctx(|| format!("create {}", path.display()))?;
        Ok(WalFile {
            file: Arc::new(file),
            path: path.to_path_buf(),
            len: 0,
            synced: 0,
        })
    }

    /// Opens an existing segment to append at `len`, cutting anything after
    /// it, and syncs the cut.
    pub fn reopen(path: &Path, len: u64) -> Result<WalFile> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .ctx(|| format!("open {}", path.display()))?;
        file.set_len(len)
            .and_then(|_| file.sync_data())
            .ctx(|| format!("truncate {}", path.display()))?;
        Ok(WalFile {
            file: Arc::new(file),
            path: path.to_path_buf(),
            len,
            synced: len,
        })
    }

    pub fn write_at(&mut self, off: u64, data: &[u8]) -> Result<()> {
        self.file
            .write_all_at(data, off)
            .ctx(|| format!("append to {}", self.path.display()))?;
        self.len = self.len.max(off + data.len() as u64);
        Ok(())
    }

    /// Simulates power loss: bytes written after the last sync are dropped,
    /// or with `PartialUnsynced` a random prefix of them survives and one
    /// 512-byte sector inside that prefix may be zeroed (the LSM log's model).
    pub fn crash(&self, mode: CrashMode) -> Result<()> {
        let keep = match mode {
            CrashMode::DropUnsynced => self.synced,
            CrashMode::PartialUnsynced { seed } => {
                let mut rng = XorShift::new(seed ^ path_salt(&self.path) ^ 0x6a09_e667_f3bc_c909);
                let unsynced = self.len - self.synced;
                let keep = self.synced + rng.next() % (unsynced + 1);
                if keep > self.synced && rng.next().is_multiple_of(4) {
                    let first = self.synced / 512;
                    let last = (keep - 1) / 512;
                    let sector = first + rng.next() % (last - first + 1);
                    let start = (sector * 512).max(self.synced);
                    let end = ((sector + 1) * 512).min(keep);
                    let zeros = vec![0u8; (end - start) as usize];
                    self.file
                        .write_all_at(&zeros, start)
                        .ctx(|| format!("zero sector of {}", self.path.display()))?;
                }
                keep
            }
        };
        self.file
            .set_len(keep)
            .ctx(|| format!("truncate {}", self.path.display()))
    }
}

/// Syncs a directory so new and renamed files in it survive a crash.
pub fn sync_dir(dir: &Path) -> Result<()> {
    let d = File::open(dir).ctx(|| format!("open directory {}", dir.display()))?;
    match d.sync_all() {
        Ok(()) => Ok(()),
        // Some file systems refuse fsync on a directory descriptor.
        Err(e) if e.kind() == std::io::ErrorKind::InvalidInput || e.raw_os_error() == Some(45) => {
            Ok(())
        }
        Err(e) => Err(Error::io(format!("sync directory {}", dir.display()), &e)),
    }
}

/// Replaces `dir/name` with `data` atomically: a temporary file, a sync, a
/// rename and a directory sync.
pub fn write_atomic(dir: &Path, name: &str, data: &[u8]) -> Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    let f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .ctx(|| format!("create {}", tmp.display()))?;
    f.write_all_at(data, 0)
        .and_then(|_| f.sync_data())
        .ctx(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, dir.join(name)).ctx(|| format!("rename {} to {name}", tmp.display()))?;
    sync_dir(dir)
}
