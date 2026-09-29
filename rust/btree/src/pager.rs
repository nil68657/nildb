//! The data file: page reads through an LRU page cache, coalesced page
//! writes, syncs, and for tests a record of every page written since the
//! last sync, which `crash` uses to undo or tear those writes the way a power
//! loss would.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use nilengine_api::cache::LruCache;
use nilengine_api::{CrashMode, Error, IoContext, Result};

use crate::page::{page_txn, validate};

pub type Page = Arc<Vec<u8>>;

struct Tracker {
    synced_len: u64,
    /// Content at the last sync of every page written since, or `None` for a
    /// page that lay beyond the synced end of the file.
    pre: HashMap<u64, Option<Vec<u8>>>,
}

pub struct Pager {
    file: File,
    path: PathBuf,
    page_size: usize,
    cache: LruCache<u64, Vec<u8>>,
    tracker: Option<Mutex<Tracker>>,
}

impl Pager {
    pub fn open(
        path: &Path,
        page_size: usize,
        cache_bytes: usize,
        track: bool,
        create: bool,
    ) -> Result<Pager> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(path)
            .ctx(|| format!("open {}", path.display()))?;
        let len = file
            .metadata()
            .ctx(|| format!("stat {}", path.display()))?
            .len();
        Ok(Pager {
            file,
            path: path.to_path_buf(),
            page_size,
            cache: LruCache::new(cache_bytes),
            tracker: track.then(|| {
                Mutex::new(Tracker {
                    synced_len: len,
                    pre: HashMap::new(),
                })
            }),
        })
    }

    /// Changes the page size before any page is read; open learns it from
    /// the meta page.
    pub fn set_page_size(&mut self, page_size: usize) {
        self.page_size = page_size;
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file_len(&self) -> Result<u64> {
        Ok(self
            .file
            .metadata()
            .ctx(|| format!("stat {}", self.path.display()))?
            .len())
    }

    /// Reads a page without any check. Short reads past the end of the file
    /// come back zero-filled.
    pub fn read_raw(&self, page: u64) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; self.page_size];
        let off = page * self.page_size as u64;
        let mut done = 0;
        while done < buf.len() {
            let n = self
                .file
                .read_at(&mut buf[done..], off + done as u64)
                .ctx(|| format!("read page {page} of {}", self.path.display()))?;
            if n == 0 {
                break;
            }
            done += n;
        }
        Ok(buf)
    }

    /// Reads a page that must have been written by transaction `txn`.
    pub fn read(&self, page: u64, txn: u64, fill: bool) -> Result<Page> {
        if let Some(p) = self.cache.get(&page)
            && page_txn(&p) == txn
        {
            return Ok(p);
        }
        let buf = self.read_raw(page)?;
        validate(&buf, page)?;
        if page_txn(&buf) != txn {
            return Err(Error::corruption(format!(
                "page {page} was written by transaction {}, its parent expects {txn}",
                page_txn(&buf)
            )));
        }
        let p = Arc::new(buf);
        if fill {
            self.cache.insert(page, p.clone(), self.page_size);
        }
        Ok(p)
    }

    /// Writes whole pages, coalescing runs of consecutive page numbers into
    /// one write call. Pages in `cached` also go into the page cache; pages
    /// in `uncached` leave it.
    pub fn write_pages(
        &self,
        mut cached: Vec<(u64, Vec<u8>)>,
        mut uncached: Vec<(u64, Vec<u8>)>,
    ) -> Result<()> {
        let ps = self.page_size as u64;
        if let Some(t) = &self.tracker {
            let mut t = t.lock().unwrap();
            for (n, _) in cached.iter().chain(uncached.iter()) {
                if !t.pre.contains_key(n) {
                    let pre = if (n + 1) * ps <= t.synced_len {
                        Some(self.read_raw(*n)?)
                    } else {
                        None
                    };
                    t.pre.insert(*n, pre);
                }
            }
        }
        for list in [&mut cached, &mut uncached] {
            list.sort_unstable_by_key(|(n, _)| *n);
            let mut i = 0;
            while i < list.len() {
                let mut j = i + 1;
                while j < list.len() && list[j].0 == list[j - 1].0 + 1 && j - i < 256 {
                    j += 1;
                }
                let off = list[i].0 * ps;
                let res = if j - i == 1 {
                    self.file.write_all_at(&list[i].1, off)
                } else {
                    let mut buf = Vec::with_capacity((j - i) * self.page_size);
                    for (_, data) in &list[i..j] {
                        buf.extend_from_slice(data);
                    }
                    self.file.write_all_at(&buf, off)
                };
                res.ctx(|| format!("write page {} of {}", list[i].0, self.path.display()))?;
                i = j;
            }
        }
        for (n, _) in &uncached {
            self.cache.remove(n);
        }
        for (n, data) in cached {
            self.cache.insert(n, Arc::new(data), self.page_size);
        }
        Ok(())
    }

    /// Syncs the file (`F_FULLFSYNC` on macOS through `File::sync_data`).
    pub fn sync(&self) -> Result<()> {
        self.file
            .sync_data()
            .ctx(|| format!("sync {}", self.path.display()))?;
        if let Some(t) = &self.tracker {
            let len = self.file_len()?;
            let mut t = t.lock().unwrap();
            t.synced_len = len;
            t.pre.clear();
        }
        Ok(())
    }

    pub fn cache_usage(&self) -> usize {
        self.cache.usage()
    }

    pub fn cache_capacity(&self) -> usize {
        self.cache.capacity()
    }

    /// Pages written since the last sync (tests only).
    pub fn unsynced_pages(&self) -> Vec<u64> {
        let mut v: Vec<u64> = self
            .tracker
            .as_ref()
            .map(|t| t.lock().unwrap().pre.keys().copied().collect())
            .unwrap_or_default();
        v.sort_unstable();
        v
    }

    /// Reverts the unsynced writes to the pages for which `lose` is true and
    /// keeps the rest (tests only).
    pub fn crash_pages(&self, lose: impl Fn(u64) -> bool) -> Result<()> {
        let Some(t) = &self.tracker else {
            return Err(Error::Unsupported(
                "crash needs Options::track_unsynced_writes".into(),
            ));
        };
        let t = t.lock().unwrap();
        let zeros = vec![0u8; self.page_size];
        for (&n, pre) in t.pre.iter() {
            if lose(n) {
                let old = pre.as_deref().unwrap_or(&zeros);
                self.file
                    .write_all_at(old, n * self.page_size as u64)
                    .ctx(|| format!("crash page {n} of {}", self.path.display()))?;
            }
        }
        Ok(())
    }

    /// Undoes, keeps or tears each page written since the last sync.
    pub fn crash(&self, mode: CrashMode) -> Result<()> {
        let Some(t) = &self.tracker else {
            return Err(Error::Unsupported(
                "crash needs Options::track_unsynced_writes".into(),
            ));
        };
        let t = t.lock().unwrap();
        let ps = self.page_size as u64;
        let mut pages: Vec<(&u64, &Option<Vec<u8>>)> = t.pre.iter().collect();
        pages.sort_unstable_by_key(|(n, _)| **n);
        let write = |n: u64, data: &[u8]| {
            self.file
                .write_all_at(data, n * ps)
                .ctx(|| format!("crash page {n} of {}", self.path.display()))
        };
        let zeros = vec![0u8; self.page_size];
        let mut keep_end = t.synced_len;
        let (partial, mut r) = match mode {
            CrashMode::DropUnsynced => (false, 1),
            CrashMode::PartialUnsynced { seed } => (true, seed ^ 0xbb67_ae85_84ca_a73b | 1),
        };
        let mut next = || {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            r
        };
        for (&n, pre) in pages {
            let old = pre.as_deref().unwrap_or(&zeros);
            let choice = if partial { next() % 3 } else { 1 };
            match choice {
                // keep the new content
                0 => keep_end = keep_end.max((n + 1) * ps),
                // lose the write
                1 => write(n, old)?,
                // tear it at a 512-byte boundary
                _ => {
                    let sectors = self.page_size / 512;
                    let cut = 512 * (1 + (next() as usize) % (sectors.max(2) - 1));
                    let mut now = self.read_raw(n)?;
                    now[cut..].copy_from_slice(&old[cut..]);
                    write(n, &now)?;
                    keep_end = keep_end.max((n + 1) * ps);
                }
            }
        }
        self.file
            .set_len(keep_end)
            .ctx(|| format!("truncate {}", self.path.display()))?;
        self.cache.remove(&0);
        Ok(())
    }
}
