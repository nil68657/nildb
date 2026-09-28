//! The write-ahead log, after PostgreSQL's (src/backend/access/transam/README).
//!
//! The log is one byte stream addressed by LSN (a byte position), cut into
//! 16 MiB segment files `pg_wal/<segment number as 16 hex digits>`. A record
//! may span two segments. Record layout, little-endian:
//!
//! ```text
//! header (32 bytes): total length u32, CRC-32 u32, previous record's LSN u64,
//!                    xid u64, record type u8, info u8, block count u8,
//!                    unused u8, main-data length u32
//! per block (12 bytes): relation u32, fork u8, flags u8, block u32, unused u16
//!     with flag 1 a full-page image follows: hole offset u16, hole length
//!     u16, then the page without the hole
//! main data
//! ```
//!
//! The CRC covers the length field and every byte after the CRC field.
//! Readers stop at the first record that is short, fails its CRC or does not
//! link back to the record before it. A page modified for the first time
//! after the redo pointer of the latest checkpoint is logged with a
//! full-page image of its new contents, which is what repairs a torn page
//! write during recovery.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use nilengine_api::coding::{crc32_parts, get_u16, get_u32, get_u64, set_u32, set_u64};
use nilengine_api::{CrashMode, Error, IoContext, Result};

use crate::file::{WalFile, sync_dir};
use crate::page::{self, BLCKSZ, PageBuf};

pub type Lsn = u64;

pub const SEG_SIZE: u64 = 16 << 20;
/// LSN of the first record of a new database; 0 means "no LSN".
pub const FIRST_LSN: Lsn = 16;
const REC_HEADER: usize = 32;
const BLOCK_HEADER: usize = 12;
const MAX_RECORD: usize = 64 << 20;
const BKP_HAS_IMAGE: u8 = 1;

pub const RM_CHECKPOINT: u8 = 1;
pub const RM_FPI_FOR_HINT: u8 = 2;
pub const RM_COMMIT: u8 = 3;
pub const RM_ABORT: u8 = 4;
pub const RM_HEAP_INSERT: u8 = 10;
pub const RM_HEAP_DELETE: u8 = 11;
pub const RM_HEAP_UPDATE: u8 = 12;
pub const RM_HEAP_HOT_UPDATE: u8 = 13;
pub const RM_HEAP_PRUNE: u8 = 14;
pub const RM_HEAP_VACUUM: u8 = 15;
pub const RM_BTREE_INSERT: u8 = 20;
pub const RM_BTREE_SPLIT: u8 = 21;
pub const RM_BTREE_NEWROOT: u8 = 22;
pub const RM_BTREE_VACUUM: u8 = 23;

/// A page address: relation number, fork and block.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageTag {
    pub rel: u32,
    pub fork: u8,
    pub block: u32,
}

/// A page a record changes, with its contents after the change.
pub struct RecBlock<'p> {
    pub tag: PageTag,
    pub page: &'p [u8],
    /// Log an image even if the page was already logged since the redo
    /// pointer (B-tree splits log their pages whole).
    pub force_image: bool,
    /// The page has pd_lower/pd_upper, so the image can skip the hole.
    pub standard: bool,
}

pub struct DecodedBlock {
    pub tag: PageTag,
    pub image: Option<Box<PageBuf>>,
}

pub struct Record {
    pub start: Lsn,
    pub end: Lsn,
    pub xid: u64,
    pub rmid: u8,
    pub blocks: Vec<DecodedBlock>,
    pub main: Vec<u8>,
}

struct State {
    /// End of the last record.
    insert: Lsn,
    /// Start of the last record.
    prev: Lsn,
    /// Record bytes from `buf_start` to `insert` not yet handed to the
    /// operating system.
    buf: Vec<u8>,
    buf_start: Lsn,
    segs: BTreeMap<u64, WalFile>,
    /// Redo pointer of the latest checkpoint: a page whose LSN is at or
    /// below it gets a full-page image with its next record.
    redo: Lsn,
    failed: Option<Error>,
}

#[derive(Default)]
pub struct WalStats {
    pub records: AtomicU64,
    pub fpis: AtomicU64,
    pub bytes: AtomicU64,
    pub flushes: AtomicU64,
}

pub struct Wal {
    dir: PathBuf,
    state: Mutex<State>,
    flush_lock: Mutex<()>,
    flushed: AtomicU64,
    insert_hint: AtomicU64,
    /// Set by `Engine::crash`: no sync may start after the simulated power
    /// loss.
    halted: AtomicBool,
    pub stats: WalStats,
}

fn seg_path(dir: &Path, seg: u64) -> PathBuf {
    dir.join(format!("{seg:016X}"))
}

fn list_segments(dir: &Path) -> Result<Vec<u64>> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(dir).ctx(|| format!("list {}", dir.display()))? {
        let e = e.ctx(|| format!("list {}", dir.display()))?;
        if let Some(n) = e
            .file_name()
            .to_str()
            .filter(|s| s.len() == 16)
            .and_then(|s| u64::from_str_radix(s, 16).ok())
        {
            v.push(n);
        }
    }
    v.sort_unstable();
    Ok(v)
}

impl Wal {
    /// Opens the log for appending at `end`, the end of the last valid
    /// record found by recovery (`prev` is that record's start). The segment
    /// holding `end` is cut there and every later segment deleted, so no
    /// stale bytes from before a crash can follow new records.
    pub fn open(dir: &Path, end: Lsn, prev: Lsn, redo: Lsn) -> Result<Wal> {
        std::fs::create_dir_all(dir).ctx(|| format!("create {}", dir.display()))?;
        let seg = end / SEG_SIZE;
        let mut segs = BTreeMap::new();
        for n in list_segments(dir)? {
            if n > seg || (n == seg && end.is_multiple_of(SEG_SIZE)) {
                let p = seg_path(dir, n);
                std::fs::remove_file(&p).ctx(|| format!("remove {}", p.display()))?;
            }
        }
        if !end.is_multiple_of(SEG_SIZE) {
            let path = seg_path(dir, seg);
            let f = if path.exists() {
                WalFile::reopen(&path, end % SEG_SIZE)?
            } else {
                WalFile::create(&path)?
            };
            segs.insert(seg, f);
        }
        sync_dir(dir)?;
        Ok(Wal {
            dir: dir.to_path_buf(),
            state: Mutex::new(State {
                insert: end,
                prev,
                buf: Vec::with_capacity(1 << 20),
                buf_start: end,
                segs,
                redo,
                failed: None,
            }),
            flush_lock: Mutex::new(()),
            flushed: AtomicU64::new(end),
            insert_hint: AtomicU64::new(end),
            halted: AtomicBool::new(false),
            stats: WalStats::default(),
        })
    }

    /// Appends a record and returns its start and end LSN. Pages in `blocks`
    /// get a full-page image when their LSN is at or below the redo pointer;
    /// the caller then sets each page's LSN to the returned end while it
    /// still holds the page's exclusive lock.
    pub fn insert(&self, rmid: u8, xid: u64, blocks: &[RecBlock<'_>], main: &[u8]) -> (Lsn, Lsn) {
        let st = self.state.lock().unwrap();
        self.insert_locked(st, rmid, xid, blocks, main)
    }

    /// Logs an image of a page that only gained hint bits, if its LSN is at
    /// or below the redo pointer (PostgreSQL's XLogSaveBufferForHint, which
    /// runs when data checksums are on). A torn write of such a page would
    /// otherwise fail its checksum with no image to repair it. Returns the
    /// record's end LSN when one was written.
    pub fn log_hint_image(&self, tag: PageTag, page: &[u8]) -> Option<Lsn> {
        let st = self.state.lock().unwrap();
        if page::lsn(page) > st.redo {
            return None;
        }
        let blocks = [RecBlock {
            tag,
            page,
            force_image: true,
            standard: true,
        }];
        Some(self.insert_locked(st, RM_FPI_FOR_HINT, 0, &blocks, &[]).1)
    }

    fn insert_locked(
        &self,
        mut st: std::sync::MutexGuard<'_, State>,
        rmid: u8,
        xid: u64,
        blocks: &[RecBlock<'_>],
        main: &[u8],
    ) -> (Lsn, Lsn) {
        let start = st.insert;
        let prev = st.prev;
        let redo = st.redo;
        let base = st.buf.len();
        let buf = &mut st.buf;
        buf.resize(base + REC_HEADER, 0);
        let mut fpis = 0;
        for b in blocks {
            let image = b.force_image || page::lsn(b.page) <= redo;
            buf.extend_from_slice(&b.tag.rel.to_le_bytes());
            buf.push(b.tag.fork);
            buf.push(if image { BKP_HAS_IMAGE } else { 0 });
            buf.extend_from_slice(&b.tag.block.to_le_bytes());
            buf.extend_from_slice(&[0, 0]);
            if image {
                let (off, len) = if b.standard {
                    page::hole(b.page)
                } else {
                    (0, 0)
                };
                buf.extend_from_slice(&(off as u16).to_le_bytes());
                buf.extend_from_slice(&(len as u16).to_le_bytes());
                buf.extend_from_slice(&b.page[..off]);
                buf.extend_from_slice(&b.page[off + len..]);
                fpis += 1;
            }
        }
        buf.extend_from_slice(main);
        let total = buf.len() - base;
        let rec = &mut buf[base..];
        set_u32(rec, 0, total as u32);
        set_u64(rec, 8, prev);
        set_u64(rec, 16, xid);
        rec[24] = rmid;
        rec[26] = blocks.len() as u8;
        set_u32(rec, 28, main.len() as u32);
        let crc = crc32_parts(&[&rec[0..4], &rec[8..]]);
        set_u32(rec, 4, crc);
        st.prev = start;
        st.insert = start + total as u64;
        let end = st.insert;
        drop(st);
        self.insert_hint.store(end, Ordering::Release);
        self.stats.records.fetch_add(1, Ordering::Relaxed);
        self.stats.fpis.fetch_add(fpis, Ordering::Relaxed);
        self.stats.bytes.fetch_add(total as u64, Ordering::Relaxed);
        (start, end)
    }

    /// End of the last inserted record.
    pub fn insert_lsn(&self) -> Lsn {
        self.insert_hint.load(Ordering::Acquire)
    }

    pub fn flushed_lsn(&self) -> Lsn {
        self.flushed.load(Ordering::Acquire)
    }

    /// Starts a checkpoint: the redo pointer moves to the current insert
    /// position, so every page changed from now on logs a full-page image
    /// with its next record.
    pub fn begin_checkpoint(&self) -> Lsn {
        let mut st = self.state.lock().unwrap();
        st.redo = st.insert;
        st.redo
    }

    fn segment<'a>(&self, st: &'a mut State, seg: u64) -> Result<&'a mut WalFile> {
        match st.segs.entry(seg) {
            Entry::Occupied(o) => Ok(o.into_mut()),
            Entry::Vacant(v) => {
                let f = WalFile::create(&seg_path(&self.dir, seg))?;
                sync_dir(&self.dir)?;
                Ok(v.insert(f))
            }
        }
    }

    fn write_locked(&self, st: &mut State) -> Result<()> {
        if let Some(e) = &st.failed {
            return Err(e.clone());
        }
        if st.buf.is_empty() {
            return Ok(());
        }
        let mut buf = std::mem::take(&mut st.buf);
        let mut lsn = st.buf_start;
        let mut rest: &[u8] = &buf;
        let mut res = Ok(());
        while !rest.is_empty() {
            let seg = lsn / SEG_SIZE;
            let off = lsn % SEG_SIZE;
            let n = rest.len().min((SEG_SIZE - off) as usize);
            if let Err(e) = self
                .segment(st, seg)
                .and_then(|f| f.write_at(off, &rest[..n]))
            {
                res = Err(e);
                break;
            }
            lsn += n as u64;
            rest = &rest[n..];
        }
        if let Err(e) = res {
            // Later records would follow a gap; refuse everything from now on.
            st.failed = Some(e.clone());
            return Err(e);
        }
        st.buf_start = lsn;
        buf.clear();
        st.buf = buf;
        Ok(())
    }

    /// Hands every inserted record to the operating system, so a process
    /// crash keeps them.
    pub fn write(&self) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        self.write_locked(&mut st)
    }

    /// Makes every record up to `upto` durable. It writes and syncs
    /// everything inserted so far, so one sync serves many callers.
    pub fn flush(&self, upto: Lsn) -> Result<()> {
        if self.flushed.load(Ordering::Acquire) >= upto {
            return Ok(());
        }
        let _f = self.flush_lock.lock().unwrap();
        if self.flushed.load(Ordering::Acquire) >= upto {
            return Ok(());
        }
        if self.halted.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        type Pending = (u64, Arc<std::fs::File>, u64, PathBuf);
        let (target, files): (Lsn, Vec<Pending>) = {
            let mut st = self.state.lock().unwrap();
            self.write_locked(&mut st)?;
            let files = st
                .segs
                .iter()
                .filter(|(_, f)| f.synced < f.len)
                .map(|(n, f)| (*n, f.file.clone(), f.len, f.path.clone()))
                .collect();
            (st.buf_start, files)
        };
        for (_, f, _, path) in &files {
            f.sync_data().ctx(|| format!("sync {}", path.display()))?;
        }
        {
            let mut st = self.state.lock().unwrap();
            for (n, _, len, _) in files {
                if let Some(s) = st.segs.get_mut(&n) {
                    s.synced = s.synced.max(len);
                }
            }
        }
        self.flushed.store(target, Ordering::Release);
        self.stats.flushes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Deletes every segment that ends at or before `lsn` (after a
    /// checkpoint whose redo pointer is `lsn`).
    pub fn remove_before(&self, lsn: Lsn) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        let current = st.insert / SEG_SIZE;
        for n in list_segments(&self.dir)? {
            if (n + 1) * SEG_SIZE <= lsn && n < current {
                st.segs.remove(&n);
                let p = seg_path(&self.dir, n);
                std::fs::remove_file(&p).ctx(|| format!("remove {}", p.display()))?;
            }
        }
        Ok(())
    }

    /// Bytes of log on disk.
    pub fn disk_bytes(&self) -> u64 {
        list_segments(&self.dir)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|n| std::fs::metadata(seg_path(&self.dir, n)).ok())
            .map(|m| m.len())
            .sum()
    }

    /// Tests only: refuses every later sync, as a power loss would.
    pub fn halt(&self) {
        self.halted.store(true, Ordering::Release);
    }

    /// Tests only: drops, keeps or tears the unsynced tail of every segment.
    pub fn crash(&self, mode: CrashMode) -> Result<()> {
        let st = self.state.lock().unwrap();
        for f in st.segs.values() {
            f.crash(mode)?;
        }
        Ok(())
    }
}

/// Reads records in order from a start LSN.
pub struct WalReader {
    dir: PathBuf,
    seg: Option<(u64, Vec<u8>)>,
    pub lsn: Lsn,
    /// Start of the last record read.
    pub prev: Option<Lsn>,
}

impl WalReader {
    pub fn new(dir: &Path, start: Lsn) -> WalReader {
        WalReader {
            dir: dir.to_path_buf(),
            seg: None,
            lsn: start,
            prev: None,
        }
    }

    /// `len` bytes at `lsn`, or `None` if the log ends first.
    fn read(&mut self, lsn: Lsn, len: usize) -> Result<Option<Vec<u8>>> {
        let mut out = Vec::with_capacity(len);
        let mut pos = lsn;
        while out.len() < len {
            let seg = pos / SEG_SIZE;
            if self.seg.as_ref().map(|s| s.0) != Some(seg) {
                let path = seg_path(&self.dir, seg);
                match std::fs::read(&path) {
                    Ok(d) => self.seg = Some((seg, d)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(e) => return Err(Error::io(format!("read {}", path.display()), &e)),
                }
            }
            let data = &self.seg.as_ref().expect("segment loaded").1;
            let off = (pos % SEG_SIZE) as usize;
            if off >= data.len() {
                return Ok(None);
            }
            let n = (len - out.len()).min(data.len() - off);
            out.extend_from_slice(&data[off..off + n]);
            pos += n as u64;
        }
        Ok(Some(out))
    }

    /// The next intact record, or `None` at the end of the valid log.
    pub fn next_record(&mut self) -> Result<Option<Record>> {
        let start = self.lsn;
        let Some(h) = self.read(start, REC_HEADER)? else {
            return Ok(None);
        };
        let total = get_u32(&h, 0) as usize;
        if !(REC_HEADER..=MAX_RECORD).contains(&total) {
            return Ok(None);
        }
        let Some(rec) = self.read(start, total)? else {
            return Ok(None);
        };
        if crc32_parts(&[&rec[0..4], &rec[8..]]) != get_u32(&rec, 4) {
            return Ok(None);
        }
        let prev = get_u64(&rec, 8);
        if self.prev.is_some_and(|p| p != prev) {
            return Ok(None);
        }
        let decoded = decode(start, &rec)
            .ok_or_else(|| Error::corruption(format!("WAL record at {start} is malformed")))?;
        self.prev = Some(start);
        self.lsn = start + total as u64;
        Ok(Some(decoded))
    }
}

fn decode(start: Lsn, rec: &[u8]) -> Option<Record> {
    let xid = get_u64(rec, 16);
    let rmid = rec[24];
    let nblocks = rec[26] as usize;
    let main_len = get_u32(rec, 28) as usize;
    let mut pos = REC_HEADER;
    let mut blocks = Vec::with_capacity(nblocks);
    for _ in 0..nblocks {
        if pos + BLOCK_HEADER > rec.len() {
            return None;
        }
        let tag = PageTag {
            rel: get_u32(rec, pos),
            fork: rec[pos + 4],
            block: get_u32(rec, pos + 6),
        };
        let flags = rec[pos + 5];
        pos += BLOCK_HEADER;
        let image = if flags & BKP_HAS_IMAGE != 0 {
            if pos + 4 > rec.len() {
                return None;
            }
            let off = get_u16(rec, pos) as usize;
            let len = get_u16(rec, pos + 2) as usize;
            pos += 4;
            if off + len > BLCKSZ || pos + BLCKSZ - len > rec.len() {
                return None;
            }
            let mut img = Box::new([0u8; BLCKSZ]);
            img[..off].copy_from_slice(&rec[pos..pos + off]);
            img[off + len..].copy_from_slice(&rec[pos + off..pos + BLCKSZ - len]);
            pos += BLCKSZ - len;
            Some(img)
        } else {
            None
        };
        blocks.push(DecodedBlock { tag, image });
    }
    if pos + main_len != rec.len() {
        return None;
    }
    Some(Record {
        start,
        end: start + rec.len() as u64,
        xid,
        rmid,
        blocks,
        main: rec[pos..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("pgheap-wal-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn records_round_trip_and_torn_tail_stops_replay() {
        let dir = tmp("rt");
        let wal = Wal::open(&dir, FIRST_LSN, 0, FIRST_LSN).unwrap();
        let mut page = vec![0u8; BLCKSZ];
        page::init(&mut page, page::KIND_HEAP, 0);
        page::add_item(&mut page, b"tuple", None, 10).unwrap();
        let tag = PageTag {
            rel: 16,
            fork: 0,
            block: 3,
        };
        fn block(tag: PageTag, p: &[u8]) -> [RecBlock<'_>; 1] {
            [RecBlock {
                tag,
                page: p,
                force_image: false,
                standard: true,
            }]
        }
        let (s1, e1) = wal.insert(RM_HEAP_INSERT, 7, &block(tag, &page), b"main-1");
        page::set_lsn(&mut page, e1);
        let (s2, e2) = wal.insert(RM_HEAP_INSERT, 8, &block(tag, &page), b"main-2");
        assert_eq!(s2, e1);
        wal.flush(e2).unwrap();
        let (_, e3) = wal.insert(RM_COMMIT, 8, &[], &[]);
        wal.write().unwrap();
        drop(wal);

        let mut r = WalReader::new(&dir, s1);
        let a = r.next_record().unwrap().unwrap();
        assert_eq!(
            (a.xid, a.rmid, a.main.as_slice()),
            (7, RM_HEAP_INSERT, &b"main-1"[..])
        );
        let img = a.blocks[0]
            .image
            .as_ref()
            .expect("first change logs an image");
        assert_eq!(page::item(&img[..], 1), b"tuple");
        let b = r.next_record().unwrap().unwrap();
        assert!(b.blocks[0].image.is_none(), "LSN above the redo pointer");
        let c = r.next_record().unwrap().unwrap();
        assert_eq!((c.rmid, c.end), (RM_COMMIT, e3));
        assert!(r.next_record().unwrap().is_none());

        // Cut the commit record short: replay ends after the second record.
        let seg = seg_path(&dir, 0);
        let f = std::fs::OpenOptions::new().write(true).open(&seg).unwrap();
        f.set_len(e3 - 3).unwrap();
        let mut r = WalReader::new(&dir, s1);
        assert!(r.next_record().unwrap().is_some());
        assert!(r.next_record().unwrap().is_some());
        assert!(r.next_record().unwrap().is_none());
        assert_eq!(r.lsn, e2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
